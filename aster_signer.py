#!/usr/bin/env python3
"""
Aster ETHUSD1 hedge sidecar (Rust).

Drop-in replacement for the old lighter_signer.py — same NDJSON protocol on
stdin/stdout so the Rust Hedger code is unchanged.

Protocol:
  Request:  {"id": <u64>, "side": "BUY"|"SELL", "qty": <float>,
             "ref_price": <float>}
  Response: {"id": <u64>, "ok": <bool>, "qty": <float>, "px": <float>,
             "coi": <int|null>, "err": <str|null>}
  BBO push: {"bbo": true, "bid": <float>, "ask": <float>}

The first stdout line after startup is `{"ready": true}` (or
`{"ready": false, "err": "..."}` on failure). Rust waits for this.

Hedge mechanics:
  - Aster v3 EIP-712 signed REST (POST /fapi/v3/order) with type=MARKET.
  - Confirmation via Aster userDataStream WS (listenKey-based): wait for
    ORDER_TRADE_UPDATE events carrying x=TRADE / X=FILLED to compute
    executed_qty and avg_px.
  - BBO via public WS ethusd1@bookTicker, pushed to stdout as {"bbo":true,...}.

Env vars required:
  ASTER_USER           — user wallet address (lowercase ok)
  ASTER_SIGNER         — signer wallet address (lowercase ok)
  ASTER_PRIVATE_KEY    — hex (0x-prefixed or bare) signer private key
  ASTER_SYMBOL         — default "ETHUSD1"
  ASTER_REST_URL       — default "https://fapi.asterdex.com"
  ASTER_WS_URL         — default "wss://fstream.asterdex.com/ws"
  ASTER_QTY_DECIMALS   — default "3" (LOT_SIZE 0.001 → 3dp)
  ASTER_PRICE_DECIMALS — default "2" (TICK_SIZE 0.01 → 2dp; only used in logs)
  ASTER_FILL_TIMEOUT   — default "10.0" seconds
  ASTER_RECV_WINDOW    — default "50000" ms
"""

import asyncio
import json
import os
import sys
import time
import urllib.parse

import aiohttp
import websockets

from eth_account import Account
from eth_account.messages import encode_typed_data
from eth_utils import keccak


def log(msg):
    # All logs to stderr; stdout is reserved for NDJSON protocol.
    ts = time.strftime("%H:%M:%S")
    ms = int(time.time() * 1000) % 1000
    print(f"{ts}.{ms:03d} [aster_signer] {msg}", file=sys.stderr, flush=True)


# ─── CONFIG ─────────────────────────────────────────────────────────────────

ASTER_USER = os.environ["ASTER_USER"].lower()
ASTER_SIGNER = os.environ["ASTER_SIGNER"].lower()
ASTER_PRIVATE_KEY = os.environ["ASTER_PRIVATE_KEY"]
if not ASTER_PRIVATE_KEY.startswith("0x"):
    ASTER_PRIVATE_KEY = "0x" + ASTER_PRIVATE_KEY

ASTER_SYMBOL = os.environ.get("ASTER_SYMBOL", "ETHUSD1")
ASTER_REST_URL = os.environ.get("ASTER_REST_URL", "https://fapi.asterdex.com").rstrip("/")
ASTER_WS_URL = os.environ.get("ASTER_WS_URL", "wss://fstream.asterdex.com/ws").rstrip("/")
QTY_DECIMALS = int(os.environ.get("ASTER_QTY_DECIMALS", "3"))
PRICE_DECIMALS = int(os.environ.get("ASTER_PRICE_DECIMALS", "2"))
FILL_TIMEOUT = float(os.environ.get("ASTER_FILL_TIMEOUT", "10.0"))
RECV_WINDOW = int(os.environ.get("ASTER_RECV_WINDOW", "50000"))

# Aster v3 EIP-712 domain (matches the Rust V3Signer in the reference script).
EIP712_DOMAIN = {
    "name": "AsterSignTransaction",
    "version": "1",
    "chainId": 1666,
    "verifyingContract": "0x0000000000000000000000000000000000000000",
}
EIP712_TYPES = {
    "EIP712Domain": [
        {"name": "name", "type": "string"},
        {"name": "version", "type": "string"},
        {"name": "chainId", "type": "uint256"},
        {"name": "verifyingContract", "type": "address"},
    ],
    "Message": [{"name": "msg", "type": "string"}],
}

# ─── STATE ──────────────────────────────────────────────────────────────────

acct = Account.from_key(ASTER_PRIVATE_KEY)

http_session: aiohttp.ClientSession = None
nonce_last: int = 0

# Pending hedge orders awaiting fill confirmation from userDataStream.
# Keyed by clientOrderId we send.
pending_fills: dict = {}  # coi -> {"qty","side","future","filled","quote"}
coi_counter: int = 0

# WS state
user_ws: websockets.WebSocketClientProtocol = None
listen_key: str = None


def _next_coi() -> str:
    """Generate a unique client order id (string, <= 36 chars)."""
    global coi_counter
    coi_counter = (coi_counter + 1) & 0xFFFFFF
    return f"hb{int(time.time() * 1000):x}{coi_counter:06x}"


def _next_nonce() -> int:
    """Monotonic microsecond nonce."""
    global nonce_last
    now_us = int(time.time() * 1_000_000)
    nonce_last = max(now_us, nonce_last + 1)
    return nonce_last


# ─── SIGNING ────────────────────────────────────────────────────────────────

def sign_body(body: str) -> str:
    """
    Sign a urlencoded body using Aster v3 EIP-712: keccak(body) wrapped in
    Message(string msg). Returns body + '&signature=0x<sig>'.
    """
    msg_value = body  # the EIP-712 'msg' field is the urlencoded body string
    encoded = encode_typed_data(
        domain_data=EIP712_DOMAIN,
        message_types={"Message": EIP712_TYPES["Message"]},
        message_data={"msg": msg_value},
    )
    signed = acct.sign_message(encoded)
    sig_hex = signed.signature.hex()
    if not sig_hex.startswith("0x"):
        sig_hex = "0x" + sig_hex
    return f"{body}&signature={sig_hex}"


def build_signed(params: dict) -> str:
    """
    Build a urlencoded body from `params`, append auth fields
    (recvWindow, timestamp, nonce, user, signer), then sign.

    Order of fields matters because the signature is over the exact byte
    string we POST.
    """
    parts = []
    for k, v in params.items():
        parts.append(f"{k}={urllib.parse.quote(str(v), safe='')}")
    parts.append(f"recvWindow={RECV_WINDOW}")
    parts.append(f"timestamp={int(time.time() * 1000)}")
    parts.append(f"nonce={_next_nonce()}")
    parts.append(f"user={ASTER_USER}")
    parts.append(f"signer={ASTER_SIGNER}")
    body = "&".join(parts)
    return sign_body(body)


# ─── HTTP HELPERS ───────────────────────────────────────────────────────────

async def _post(path: str, body: str) -> tuple[int, str]:
    url = f"{ASTER_REST_URL}{path}"
    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
        "User-Agent": "PythonApp/1.0",
        "Accept": "application/json",
    }
    async with http_session.post(url, data=body, headers=headers) as r:
        txt = await r.text()
        return r.status, txt


async def _put(path: str, body: str) -> tuple[int, str]:
    url = f"{ASTER_REST_URL}{path}"
    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
        "User-Agent": "PythonApp/1.0",
    }
    async with http_session.put(url, data=body, headers=headers) as r:
        txt = await r.text()
        return r.status, txt


async def _get(path: str) -> tuple[int, str]:
    url = f"{ASTER_REST_URL}{path}"
    async with http_session.get(url) as r:
        txt = await r.text()
        return r.status, txt


# ─── LISTEN KEY / USER DATA STREAM ──────────────────────────────────────────

async def create_listen_key() -> str:
    body = build_signed({})
    status, txt = await _post("/fapi/v3/listenKey", body)
    if status != 200:
        raise RuntimeError(f"listenKey create failed: {status} {txt}")
    obj = json.loads(txt)
    return obj["listenKey"]


async def keepalive_listen_key() -> bool:
    body = build_signed({})
    status, _ = await _put("/fapi/v3/listenKey", body)
    return status == 200


# ─── BBO FEED (public WS) ───────────────────────────────────────────────────

async def bbo_loop():
    """Public bookTicker stream → push BBO updates to stdout."""
    stream = f"{ASTER_SYMBOL.lower()}@bookTicker"
    url = f"{ASTER_WS_URL}/{stream}"
    while True:
        try:
            async with websockets.connect(url, ping_interval=30, ping_timeout=15) as ws:
                log(f"bbo ws connected {stream}")
                async for raw in ws:
                    try:
                        m = json.loads(raw)
                    except Exception:
                        continue
                    # Aster fstream bookTicker is Binance-compatible:
                    # {"e":"bookTicker","u":...,"s":"ETHUSD1","b":"px","B":"sz","a":"px","A":"sz","T":...,"E":...}
                    try:
                        bid = float(m.get("b"))
                        ask = float(m.get("a"))
                    except (TypeError, ValueError):
                        continue
                    if bid <= 0 or ask <= 0:
                        continue
                    sys.stdout.write(json.dumps({"bbo": True, "bid": bid, "ask": ask}) + "\n")
                    sys.stdout.flush()
        except Exception as e:
            log(f"bbo ws err: {e}, reconnecting in 2s")
            await asyncio.sleep(2)


# ─── USER DATA STREAM (private WS, listenKey) ───────────────────────────────

async def user_ws_loop():
    """Connect to user data stream, dispatch fills to pending_fills futures."""
    global user_ws, listen_key
    while True:
        try:
            listen_key = await create_listen_key()
            log(f"listenKey ok: {listen_key[:10]}...")
        except Exception as e:
            log(f"listenKey err: {e}, retrying in 5s")
            await asyncio.sleep(5)
            continue

        # Keepalive task — every 30 minutes.
        async def _ka():
            while True:
                await asyncio.sleep(30 * 60)
                try:
                    ok = await keepalive_listen_key()
                    if not ok:
                        log("listenKey keepalive failed")
                        return
                except Exception as e:
                    log(f"keepalive err: {e}")
                    return
        ka_task = asyncio.create_task(_ka())

        url = f"{ASTER_WS_URL}/{listen_key}"
        try:
            async with websockets.connect(url, ping_interval=30, ping_timeout=15) as ws:
                user_ws = ws
                log("user ws connected")
                async for raw in ws:
                    try:
                        m = json.loads(raw)
                    except Exception:
                        continue
                    if isinstance(m, dict) and m.get("e") == "listenKeyExpired":
                        log("listenKey expired, reconnecting")
                        break
                    _handle_user_event(m)
        except Exception as e:
            log(f"user ws err: {e}")
        finally:
            user_ws = None
            ka_task.cancel()
            await asyncio.sleep(2)


def _handle_user_event(m: dict):
    """
    Aster v3 user stream is Binance-USDM compatible:
      {"e":"ORDER_TRADE_UPDATE", "o": {
          "s":"ETHUSD1","c":"<clientOrderId>","S":"BUY"/"SELL",
          "x":"TRADE"/"NEW"/...,"X":"FILLED"/"PARTIALLY_FILLED"/...,
          "l":"<lastFillQty>","L":"<lastFillPx>",
          "z":"<cumFilledQty>","ap":"<avgPx>", ...
      }}
    """
    e = m.get("e")
    if e != "ORDER_TRADE_UPDATE":
        return
    o = m.get("o") or {}
    coi = o.get("c")
    if not coi or coi not in pending_fills:
        return

    entry = pending_fills[coi]
    x = o.get("x", "")
    X = o.get("X", "")

    # Accumulate trade fills as they come. If only z/ap arrive (some venues
    # batch), fall back to those for the final state.
    try:
        last_qty = float(o.get("l", "0") or 0)
        last_px = float(o.get("L", "0") or 0)
        cum_qty = float(o.get("z", "0") or 0)
        avg_px = float(o.get("ap", "0") or 0)
    except (TypeError, ValueError):
        return

    if x == "TRADE" and last_qty > 0 and last_px > 0:
        entry["filled"] += last_qty
        entry["quote"] += last_qty * last_px

    # Terminal states: settle the future.
    terminal = X in ("FILLED", "CANCELED", "EXPIRED", "REJECTED")
    if not terminal:
        return

    # Prefer trade-by-trade accumulation; fall back to exchange-reported VWAP.
    if entry["filled"] > 0:
        avg = entry["quote"] / entry["filled"]
        filled = entry["filled"]
    elif cum_qty > 0 and avg_px > 0:
        filled = cum_qty
        avg = avg_px
    else:
        filled = 0.0
        avg = 0.0

    if not entry["future"].done():
        if filled > 0 and avg > 0:
            entry["future"].set_result((filled, avg, X))
        else:
            entry["future"].set_exception(RuntimeError(f"no fill (status={X})"))


# ─── ORDER RECONCILIATION ───────────────────────────────────────────────────

async def query_order_by_coi(coi: str) -> dict | None:
    """GET /fapi/v3/order?origClientOrderId=<coi> — returns the order dict
    if Aster has it, None if not found. Used after REST timeouts where the
    order may have placed despite the error."""
    try:
        body = build_signed({"symbol": ASTER_SYMBOL, "origClientOrderId": coi})
        url = f"{ASTER_REST_URL}/fapi/v3/order?{body}"
        async with http_session.get(url) as r:
            txt = await r.text()
            if r.status == 200:
                return json.loads(txt)
            # -2013 = order does not exist
            try:
                j = json.loads(txt)
                if j.get("code") == -2013:
                    return None
            except Exception:
                pass
            log(f"query_order coi={coi} unexpected status={r.status} body={txt[:200]}")
            return None
    except Exception as e:
        log(f"query_order coi={coi} exc: {e}")
        return None


# ─── MARKET ORDER ───────────────────────────────────────────────────────────

async def market_order(side: str, qty: float, ref_price: float):
    """Submit a MARKET order and await fill via userDataStream.
    Returns (executed_qty, avg_price, coi, err)."""
    if user_ws is None:
        return 0.0, 0.0, None, "user ws not connected"
    if qty <= 0:
        return 0.0, 0.0, None, f"qty {qty} <= 0"

    qty_str = f"{qty:.{QTY_DECIMALS}f}"
    coi = _next_coi()

    params = {
        "symbol": ASTER_SYMBOL,
        "side": side,
        "type": "MARKET",
        "quantity": qty_str,
        "newClientOrderId": coi,
        "newOrderRespType": "RESULT",
    }
    body = build_signed(params)

    # Register the fill waiter BEFORE posting so we don't miss a fast fill.
    fill_future = asyncio.get_event_loop().create_future()
    pending_fills[coi] = {
        "side": side,
        "qty": qty,
        "future": fill_future,
        "filled": 0.0,
        "quote": 0.0,
    }

    t0 = time.time()
    post_failed = False
    post_err = None
    try:
        status, txt = await _post("/fapi/v3/order", body)
    except Exception as e:
        post_failed = True
        post_err = f"post exc: {e}"
        status, txt = 0, ""

    # If POST itself errored, or returned a "timed out" 400, the order may
    # have actually placed on Aster's side. Query by coi to find out.
    timed_out_body = (status == 400 and "timed out" in (txt or "").lower())
    if post_failed or timed_out_body:
        log(f"post uncertain (post_failed={post_failed} timed_out_body={timed_out_body} "
            f"status={status}) — reconciling coi={coi}")
        # Small delay so Aster's side has a moment to register the order.
        await asyncio.sleep(0.5)
        existing = await query_order_by_coi(coi)
        if existing is None:
            # Order definitely never placed — safe to report failure for retry.
            pending_fills.pop(coi, None)
            err = post_err or f"timed out (status={status} body={txt[:200]})"
            log(f"reconcile coi={coi}: NOT FOUND — safe to retry; err={err}")
            return 0.0, 0.0, coi, err
        # Order did place. Walk the existing fields and either settle now or
        # fall through to WS wait.
        log(f"reconcile coi={coi}: FOUND status={existing.get('status')} "
            f"executedQty={existing.get('executedQty')} avgPrice={existing.get('avgPrice')}")
        ex_status = existing.get("status", "")
        try:
            ex_qty = float(existing.get("executedQty", "0") or 0)
            ex_avg = float(existing.get("avgPrice", "0") or 0)
        except (TypeError, ValueError):
            ex_qty, ex_avg = 0.0, 0.0
        if ex_status == "FILLED" and ex_qty > 0 and ex_avg > 0:
            pending_fills.pop(coi, None)
            ack_ms = int((time.time() - t0) * 1000)
            log(f"FILL(reconcile) {side} coi={coi} qty={ex_qty} @{ex_avg:.{PRICE_DECIMALS}f} after={ack_ms}ms")
            return ex_qty, ex_avg, coi, None
        # Order exists but not FILLED yet (NEW/PARTIALLY_FILLED) — fall through
        # to the WS-wait path below. Skip the rest of the response-parsing.
        status, txt = 200, "{}"
        ack = {}
    elif status >= 400:
        pending_fills.pop(coi, None)
        err_code, err_msg = None, None
        try:
            j = json.loads(txt)
            err_code = j.get("code")
            err_msg = j.get("msg")
        except Exception:
            pass
        log(f"order rejected status={status} code={err_code} msg={err_msg!r} body={txt[:500]!r} "
            f"req: symbol={ASTER_SYMBOL} side={side} qty={qty_str} coi={coi}")
        if err_code is not None or err_msg is not None:
            return 0.0, 0.0, coi, f"rejected code={err_code} msg={err_msg}"
        return 0.0, 0.0, coi, f"rejected status={status} body={txt[:200]}"
    else:
        # Normal success path: parse ack.
        try:
            ack = json.loads(txt)
        except Exception:
            pending_fills.pop(coi, None)
            return 0.0, 0.0, coi, f"bad ack: {txt[:200]}"

    ack_ms = int((time.time() - t0) * 1000)

    # If the ack itself reports FILLED with executed qty/avg price, settle
    # immediately — handles the case where the userDataStream event arrived
    # before us or is delayed.
    try:
        ack_status = ack.get("status", "")
        ack_exec_qty = float(ack.get("executedQty", "0") or 0)
        ack_avg_px = float(ack.get("avgPrice", "0") or ack.get("price", "0") or 0)
    except (TypeError, ValueError):
        ack_status, ack_exec_qty, ack_avg_px = "", 0.0, 0.0

    if ack_status == "FILLED" and ack_exec_qty > 0 and ack_avg_px > 0:
        pending_fills.pop(coi, None)
        log(f"FILL(ack) {side} coi={coi} qty={ack_exec_qty} @{ack_avg_px:.{PRICE_DECIMALS}f} ack={ack_ms}ms")
        return ack_exec_qty, ack_avg_px, coi, None

    # Otherwise wait for the userDataStream confirmation.
    try:
        executed_qty, avg_price, X = await asyncio.wait_for(fill_future, timeout=FILL_TIMEOUT)
    except asyncio.TimeoutError:
        entry = pending_fills.pop(coi, None)
        if entry and entry["filled"] > 0:
            executed_qty = entry["filled"]
            avg_price = entry["quote"] / executed_qty
            log(f"partial coi={coi} {executed_qty}@{avg_price:.{PRICE_DECIMALS}f}")
            X = "PARTIAL_TIMEOUT"
        else:
            return 0.0, 0.0, coi, "no fill (timeout)"
    except Exception as e:
        pending_fills.pop(coi, None)
        return 0.0, 0.0, coi, f"fill err: {e}"
    else:
        pending_fills.pop(coi, None)

    fill_ms = int((time.time() - t0) * 1000)
    log(
        f"FILL {side} coi={coi} qty={executed_qty} "
        f"@{avg_price:.{PRICE_DECIMALS}f} X={X} ack={ack_ms}ms fill={fill_ms}ms"
    )
    if executed_qty <= 0:
        return 0.0, 0.0, coi, f"zero fill (status={X})"
    return executed_qty, avg_price, coi, None


# ─── NDJSON PROTOCOL ────────────────────────────────────────────────────────

def write_response(obj):
    sys.stdout.write(json.dumps(obj) + "\n")
    sys.stdout.flush()


async def handle_request(req):
    rid = req.get("id")
    try:
        side = req["side"]
        qty = float(req["qty"])
        ref_price = float(req["ref_price"])
    except (KeyError, TypeError, ValueError) as e:
        write_response({"id": rid, "ok": False, "qty": 0.0, "px": 0.0,
                        "coi": None, "err": f"bad request: {e}"})
        return

    if side not in ("BUY", "SELL"):
        write_response({"id": rid, "ok": False, "qty": 0.0, "px": 0.0,
                        "coi": None, "err": f"bad side: {side}"})
        return

    executed_qty, avg_price, coi, err = await market_order(side, qty, ref_price)
    write_response({
        "id": rid,
        "ok": executed_qty > 0,
        "qty": executed_qty,
        "px": avg_price,
        "coi": coi,
        "err": err,
    })


async def stdin_loop():
    """Read NDJSON requests on stdin, dispatch concurrently."""
    loop = asyncio.get_event_loop()
    reader = asyncio.StreamReader()
    protocol = asyncio.StreamReaderProtocol(reader)
    await loop.connect_read_pipe(lambda: protocol, sys.stdin)

    while True:
        line = await reader.readline()
        if not line:
            log("stdin EOF, shutting down")
            return
        try:
            req = json.loads(line.decode().strip())
        except Exception as e:
            log(f"bad line: {e}")
            continue
        asyncio.create_task(handle_request(req))


# ─── MAIN ───────────────────────────────────────────────────────────────────

async def init():
    """Warmup: REST ping + cancel any open orders on the symbol."""
    global http_session
    http_session = aiohttp.ClientSession(
        timeout=aiohttp.ClientTimeout(total=10, connect=3),
    )
    # Ping
    try:
        status, _ = await _get("/fapi/v3/ping")
        log(f"ping {status}")
    except Exception as e:
        log(f"ping err: {e}")

    # Best-effort cancel-all at startup so we don't leave stale orders behind.
    try:
        body = build_signed({"symbol": ASTER_SYMBOL})
        # DELETE with a body: use a manual session request.
        url = f"{ASTER_REST_URL}/fapi/v3/allOpenOrders"
        headers = {
            "Content-Type": "application/x-www-form-urlencoded",
            "User-Agent": "PythonApp/1.0",
        }
        async with http_session.delete(url, data=body, headers=headers) as r:
            txt = await r.text()
            log(f"cancel_all {r.status}: {txt[:120]}")
    except Exception as e:
        log(f"cancel_all err: {e}")


async def main():
    try:
        await init()
    except Exception as e:
        write_response({"ready": False, "err": str(e)})
        log(f"init failed: {e}")
        return

    bbo_task = asyncio.create_task(bbo_loop())
    user_task = asyncio.create_task(user_ws_loop())

    # Wait for the user-data WS to connect before signalling ready, so the
    # first hedge doesn't race against the `user_ws is None` check.
    for _ in range(100):  # 10s
        if user_ws is not None:
            break
        await asyncio.sleep(0.1)

    if user_ws is None:
        write_response({"ready": False, "err": "user ws never connected"})
        log("user ws never connected")
        bbo_task.cancel()
        user_task.cancel()
        return

    write_response({"ready": True})
    log(f"ready (symbol={ASTER_SYMBOL} user={ASTER_USER[:10]}... signer={ASTER_SIGNER[:10]}...)")

    try:
        await stdin_loop()
    finally:
        bbo_task.cancel()
        user_task.cancel()
        if http_session is not None:
            await http_session.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass