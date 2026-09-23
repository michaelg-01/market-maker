#!/usr/bin/env python3
"""
Lighter hedge sidecar (Rust).

Protocol (NDJSON, line-delimited on stdin/stdout):

  Request:  {"id": <u64>, "side": "BUY"|"SELL", "qty": <float>,
             "ref_price": <float>}
  Response: {"id": <u64>, "ok": <bool>, "qty": <float>, "px": <float>,
             "coi": <int|null>, "err": <str|null>}

The first stdout line after startup is `{"ready": true}` (or
`{"ready": false, "err": "..."}` on failure) — Rust waits for this
before sending requests.

Env vars required:
  LIGHTER_BASE_URL
  LIGHTER_API_KEY_PRIVATE_KEY
  LIGHTER_ACCOUNT_INDEX
  LIGHTER_API_KEY_INDEX
  LIGHTER_MARKET_SYMBOL   (default "BTC")
  LIGHTER_MAX_SLIPPAGE    (default "0.01")
  LIGHTER_FILL_TIMEOUT    (default "10.0", seconds)
"""

import asyncio
import json
import os
import sys
import time

import lighter  # pip install git+https://github.com/elliottech/lighter-python.git
import websockets


def log(msg):
    # All logs to stderr so stdout is reserved for the NDJSON protocol.
    ts = time.strftime("%H:%M:%S")
    ms = int(time.time() * 1000) % 1000
    print(f"{ts}.{ms:03d} [lighter_signer] {msg}", file=sys.stderr, flush=True)


LIGHTER_BASE_URL = os.environ["LIGHTER_BASE_URL"]
LIGHTER_API_KEY_PRIVATE_KEY = os.environ["LIGHTER_API_KEY_PRIVATE_KEY"]
LIGHTER_ACCOUNT_INDEX = int(os.environ["LIGHTER_ACCOUNT_INDEX"])
LIGHTER_API_KEY_INDEX = int(os.environ["LIGHTER_API_KEY_INDEX"])
LIGHTER_MARKET_SYMBOL = os.environ.get("LIGHTER_MARKET_SYMBOL", "ETH")
LIGHTER_MAX_SLIPPAGE = float(os.environ.get("LIGHTER_MAX_SLIPPAGE", "0.01"))
LIGHTER_FILL_TIMEOUT = float(os.environ.get("LIGHTER_FILL_TIMEOUT", "10.0"))
LIGHTER_WS_URL = LIGHTER_BASE_URL.replace("http", "ws", 1).rstrip("/") + "/stream"

# Resolved at startup.
client: lighter.SignerClient = None
market_index: int = None
size_decimals: int = None
price_decimals: int = None
min_base_amount: int = None
next_nonce: dict = {}  # api_key_index -> next nonce
ws: websockets.WebSocketClientProtocol = None
ws_send_lock: asyncio.Lock = None
ws_pending: dict = {}  # req_id -> Future for sendtx acks
pending_fills: dict = {}  # coi -> {"qty","side","future","filled","quote"}
coi_counter: int = 0


def _next_coi():
    global coi_counter
    coi_counter = (coi_counter + 1) & 0xFFFF
    return ((int(time.time()) & 0x7FFFFFFF) << 16) | coi_counter


async def init():
    """Resolve market metadata + nonce, build signer client."""
    global client, market_index, size_decimals, price_decimals, min_base_amount

    api_client = lighter.ApiClient(
        configuration=lighter.Configuration(host=LIGHTER_BASE_URL)
    )
    try:
        order_api = lighter.OrderApi(api_client)
        books = await order_api.order_books()
        mi = None
        for ob in books.order_books or []:
            if (ob.symbol or "").upper() == LIGHTER_MARKET_SYMBOL.upper():
                mi = int(ob.market_id)
                break
        if mi is None:
            raise RuntimeError(f"market {LIGHTER_MARKET_SYMBOL} not found")

        details = await order_api.order_book_details(market_id=mi)
        target = None
        for d in details.order_book_details or []:
            if int(d.market_id) == mi:
                target = d
                break
        if target is None:
            raise RuntimeError(f"order_book_details missing for market_id={mi}")

        market_index = mi
        size_decimals = int(target.size_decimals)
        price_decimals = int(target.price_decimals)
        raw_min = getattr(target, "min_base_amount", None)
        min_base_amount = (
            max(1, int(round(float(raw_min) * (10 ** size_decimals))))
            if raw_min is not None
            else 1
        )
        log(
            f"market={LIGHTER_MARKET_SYMBOL} idx={market_index} "
            f"size_dec={size_decimals} price_dec={price_decimals} "
            f"min_base={min_base_amount}"
        )

        tx_api = lighter.TransactionApi(api_client)
        nn = await tx_api.next_nonce(
            account_index=LIGHTER_ACCOUNT_INDEX,
            api_key_index=LIGHTER_API_KEY_INDEX,
        )
        next_nonce[LIGHTER_API_KEY_INDEX] = int(nn.nonce)
        log(f"starting nonce={nn.nonce}")
    finally:
        await api_client.close()

    client = lighter.SignerClient(
        url=LIGHTER_BASE_URL,
        api_private_keys={LIGHTER_API_KEY_INDEX: LIGHTER_API_KEY_PRIVATE_KEY},
        account_index=LIGHTER_ACCOUNT_INDEX,
    )
    err = client.check_client()
    if err is not None:
        raise RuntimeError(f"check_client failed: {err}")
    log("signer client ready")


async def ws_loop():
    """Persistent WS connection: receive sendtx acks + trade fills."""
    global ws

    while True:
        try:
            async with websockets.connect(
                LIGHTER_WS_URL, ping_interval=30, ping_timeout=15
            ) as conn:
                ws = conn
                # Drop stale pending state from previous connection.
                for fut in list(ws_pending.values()):
                    if not fut.done():
                        fut.cancel()
                ws_pending.clear()
                for coi, entry in list(pending_fills.items()):
                    if not entry["future"].done():
                        entry["future"].cancel()
                pending_fills.clear()

                auth_token, err = client.create_auth_token_with_expiry(
                    deadline=28800, api_key_index=LIGHTER_API_KEY_INDEX
                )
                if err is not None:
                    log(f"auth token err: {err}")
                    await asyncio.sleep(2)
                    continue

                await conn.send(json.dumps({
                    "type": "subscribe",
                    "channel": f"account_all/{LIGHTER_ACCOUNT_INDEX}",
                    "auth": auth_token,
                }))
                await conn.send(json.dumps({
                    "type": "subscribe",
                    "channel": f"ticker/{market_index}",
                }))
                log("ws connected + subscribed")

                async for raw in conn:
                    try:
                        msg = json.loads(raw)
                    except Exception:
                        continue
                    mtype = msg.get("type", "")
                    if mtype == "ping":
                        await conn.send(json.dumps({"type": "pong"}))
                        continue
                    msg_id = msg.get("id") or (msg.get("data") or {}).get("id")
                    if msg_id and msg_id in ws_pending:
                        fut = ws_pending[msg_id]
                        if not fut.done():
                            fut.set_result(msg)
                        continue
                    if mtype in ("subscribed/account_all", "update/account_all"):
                        _handle_trades(msg)
                    elif mtype in ("subscribed/ticker", "update/ticker"):
                        _handle_ticker(msg)
        except Exception as e:
            log(f"ws err: {e}, reconnecting")
            ws = None
            await asyncio.sleep(2)


def _handle_ticker(msg):
    """Ticker channel: top-of-book only, no diffs."""
    tk = msg.get("ticker") or {}
    a = tk.get("a") or {}
    b = tk.get("b") or {}
    try:
        bid = float(b.get("price"))
        ask = float(a.get("price"))
    except (TypeError, ValueError):
        return
    if bid <= 0 or ask <= 0:
        return
    sys.stdout.write(json.dumps({"bbo": True, "bid": bid, "ask": ask}) + "\n")
    sys.stdout.flush()


def _handle_trades(msg):
    """Match incoming trades to pending fill waiters by client_order_index."""
    trades_by_market = msg.get("trades") or {}
    market_trades = trades_by_market.get(str(market_index), [])
    if not market_trades and isinstance(trades_by_market, list):
        market_trades = trades_by_market

    for tr in market_trades:
        ask_coi = tr.get("ask_client_id")
        bid_coi = tr.get("bid_client_id")
        match_coi = None
        if ask_coi is not None and int(ask_coi) in pending_fills:
            match_coi = int(ask_coi)
        elif bid_coi is not None and int(bid_coi) in pending_fills:
            match_coi = int(bid_coi)
        if match_coi is None:
            continue

        entry = pending_fills[match_coi]
        try:
            sz = float(tr.get("size", 0) or 0)
            px = float(tr.get("price", 0) or 0)
        except (TypeError, ValueError):
            continue
        if sz <= 0 or px <= 0:
            continue

        entry["filled"] += sz
        entry["quote"] += sz * px

        if entry["filled"] >= entry["qty"] - 1e-9 and not entry["future"].done():
            avg = entry["quote"] / entry["filled"]
            entry["future"].set_result((entry["filled"], avg))


async def market_order(side: str, qty: float, ref_price: float):
    """Sign, submit, await fill. Returns (qty, px, coi) or (0, 0, None)."""
    if ws is None:
        return 0.0, 0.0, None, "ws not connected"

    is_ask = side == "SELL"
    base_amount = int(round(qty * (10 ** size_decimals)))
    if base_amount < min_base_amount:
        return 0.0, 0.0, None, f"base_amount {base_amount} < min {min_base_amount}"

    if ref_price <= 0:
        return 0.0, 0.0, None, "no ref price"
    worst_price = (
        ref_price * (1 + LIGHTER_MAX_SLIPPAGE)
        if side == "BUY"
        else ref_price * (1 - LIGHTER_MAX_SLIPPAGE)
    )
    price_int = int(round(worst_price * (10 ** price_decimals)))

    coi = _next_coi()
    nonce = next_nonce[LIGHTER_API_KEY_INDEX]
    next_nonce[LIGHTER_API_KEY_INDEX] = nonce + 1

    try:
        tx_type, tx_info_json, _, err = client.sign_create_order(
            market_index=market_index,
            client_order_index=coi,
            base_amount=base_amount,
            price=price_int,
            is_ask=is_ask,
            order_type=client.ORDER_TYPE_MARKET,
            time_in_force=client.ORDER_TIME_IN_FORCE_IMMEDIATE_OR_CANCEL,
            reduce_only=False,
            trigger_price=0,
            order_expiry=client.DEFAULT_IOC_EXPIRY,
            nonce=nonce,
            api_key_index=LIGHTER_API_KEY_INDEX,
        )
    except Exception as e:
        next_nonce[LIGHTER_API_KEY_INDEX] = nonce
        return 0.0, 0.0, None, f"sign exc: {e}"

    if err is not None:
        next_nonce[LIGHTER_API_KEY_INDEX] = nonce
        return 0.0, 0.0, None, f"sign err: {err}"

    fill_future = asyncio.get_event_loop().create_future()
    pending_fills[coi] = {
        "side": side,
        "qty": qty,
        "future": fill_future,
        "filled": 0.0,
        "quote": 0.0,
    }

    req_id = f"hedge_{coi}"
    ack_fut = asyncio.get_event_loop().create_future()
    ws_pending[req_id] = ack_fut
    payload = json.dumps(
        {
            "type": "jsonapi/sendtx",
            "data": {
                "id": req_id,
                "tx_type": tx_type,
                "tx_info": json.loads(tx_info_json),
            },
        }
    )

    t0 = time.time()
    try:
        async with ws_send_lock:
            await ws.send(payload)
        ack = await asyncio.wait_for(ack_fut, timeout=LIGHTER_FILL_TIMEOUT)
    except asyncio.TimeoutError:
        pending_fills.pop(coi, None)
        return 0.0, 0.0, coi, "sendtx ack timeout"
    except Exception as e:
        pending_fills.pop(coi, None)
        return 0.0, 0.0, coi, f"send err: {e}"
    finally:
        ws_pending.pop(req_id, None)

    ack_ms = int((time.time() - t0) * 1000)
    if "error" in ack.get("type", "") or ack.get("code", 200) >= 400:
        pending_fills.pop(coi, None)
        return 0.0, 0.0, coi, f"sendtx rejected: {ack}"

    try:
        executed_qty, avg_price = await asyncio.wait_for(
            fill_future, timeout=LIGHTER_FILL_TIMEOUT
        )
    except asyncio.TimeoutError:
        entry = pending_fills.pop(coi, None)
        if entry and entry["filled"] > 0:
            executed_qty = entry["filled"]
            avg_price = entry["quote"] / executed_qty
            log(f"partial coi={coi} {executed_qty}@{avg_price:.2f}")
        else:
            return 0.0, 0.0, coi, "no fill (IOC rejected/unfilled)"
    else:
        pending_fills.pop(coi, None)

    fill_ms = int((time.time() - t0) * 1000)
    log(
        f"FILL {side} coi={coi} qty={executed_qty} @{avg_price:.2f} "
        f"ack={ack_ms}ms fill={fill_ms}ms"
    )
    if executed_qty <= 0:
        return 0.0, 0.0, coi, f"zero fill (ack={ack_ms}ms fill={fill_ms}ms ack_resp={ack})"
    return executed_qty, avg_price, coi, None


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
    write_response(
        {
            "id": rid,
            "ok": executed_qty > 0,
            "qty": executed_qty,
            "px": avg_price,
            "coi": coi,
            "err": err,
        }
    )


async def stdin_loop():
    """Read NDJSON requests on stdin, spawn one task per request so
    concurrent hedges (e.g. both bid + ask fill at once) don't serialize."""
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


async def main():
    global ws_send_lock
    ws_send_lock = asyncio.Lock()
    try:
        await init()
    except Exception as e:
        write_response({"ready": False, "err": str(e)})
        log(f"init failed: {e}")
        return

    ws_task = asyncio.create_task(ws_loop())
    # Wait for first WS connect before signalling ready, so the first hedge
    # doesn't race against the ws=None check.
    for _ in range(50):
        if ws is not None:
            break
        await asyncio.sleep(0.1)

    write_response({"ready": True})
    log("ready")

    try:
        await stdin_loop()
    finally:
        ws_task.cancel()
        if client is not None:
            await client.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass