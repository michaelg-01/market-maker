// Market maker on Binance Spot BTCU, over FIX-SBE.
//
// Protocol: FIX-SBE (schema spot-fixsbe-1_1.xml) — fastest path Binance offers.
//   - Order Entry session: NewOrderSingle / OrderCancelRequest, post-only LIMIT,
//     receives ExecutionReport for fills.
//   - Market Data session #1: BTCU book ticker + trades → FV + k estimator.
//   - Market Data session #2: BTCUSDT book ticker → σ sampling + jump-pause
//     (the role the Binance perp feed played in the Aster version; BTCUSDT is
//     the deeper/faster book so its mid is a cleaner volatility signal than the
//     thin BTCU book).
//
// Fair value = BTCU non-self mid (best bid/ask mid excluding our own quotes).
//

mod sbe;
mod fix;

use anyhow::Result;
use crossbeam_queue::ArrayQueue;
use fix::{FixSession, InboundMsg, SessionKind};
use parking_lot::Mutex as PlMutex;
use std::collections::HashMap;
use std::process::Stdio;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, ChildStdin, Command};
use tokio::sync::{oneshot, Mutex as AsyncMutex, Notify};

// ─── CONFIG ─────────────────────────────────────────────────────────────────

const SYMBOL_SPOT: &str = "ETHU";        // trading venue (Binance spot)
const SYMBOL_USDT: &str = "ETHUSDT";     // σ / jump-pause anchor (deeper book)

// Verify against InstrumentList / exchangeInfo.
const TICK_SIZE: f64    = 0.01;
const LOT_SIZE: f64     = 0.0001;
const MIN_NOTIONAL: f64 = 5.0;

// SBE decimal exponents for ETHU.
// TICK_SIZE 0.01  → price has 2 dp → exponent -2.
// LOT_SIZE 0.0001 → qty has 4 dp   → exponent -4.
const PRICE_EXPONENT: i8 = -2;
const QTY_EXPONENT: i8   = -4;

// Sizing & inventory
const ORDER_QTY: f64        = 0.15;
const MAX_INVENTORY: f64    = 0.30;
#[allow(dead_code)] const INVENTORY_TARGET: f64 = 0.0;

// Quoting mode: true = one side at a time (flips on full fill).
const SINGLE_SIDE_MODE: bool = true;

// A-S parameters (no longer used in quoting; σ/k kept for stats logging)
#[allow(dead_code)] const GAMMA: f64           = 0.5;
#[allow(dead_code)] const TAU_SECONDS: f64     = 60.0;
const SIGMA_HALFLIFE: f64  = 15.0;
const K_HALFLIFE: f64      = 60.0;
const SIGMA_FLOOR: f64     = 1e-5;
const SIGMA_SAMPLE_MS: u64 = 1000;
const K_FLOOR: f64         = 0.05;
const K_DEFAULT: f64       = 0.5;
const SIGMA_DEFAULT: f64   = 5e-5;

// Basis EWMA (BTCU mid - BTCUSDT mid). Logged only; not fed into quoting.
const BASIS_HALFLIFE: f64 = 30.0;

// BTCUSDT jump detector: pause quoting while |usdt_mid - usdt_ewma| > threshold.
const USDT_LEAD_HALFLIFE: f64     = 1.0;
const USDT_JUMP_THRESHOLD: f64    = 0.9;  // USD: pause above this
const USDT_UNPAUSE_THRESHOLD: f64 = 0.08;  // unpause below this (hysteresis)

// ─── ETHUSDT trade-flow impulse predictor ───────────────────────────────────
// Short-horizon FV-displacement estimate from the ETHUSDT trade tape:
//     impulse = β · sign(Q) · √|Q|
// where Q is the EWMA-decayed signed rolling trade qty (taker-buy +, taker-sell
// −). The √ shape and β fit come from the standard square-root market-impact
// law: aggressor flow tends to be informed (people buy when they think value
// is too low, sell when too high), so a burst of one-sided trades predicts the
// mid is about to move that way. Trades are also a faster, harder-to-spoof
// signal than the book.
//
// β must be FIT OFFLINE: regress realized N-second ETHUSDT mid returns on
// sign(Q)·√|Q| sampled at the same horizon, β = slope. The placeholder below
// is a guess — replace it with your fitted value. Units of `impulse` are then
// price (USD), directly comparable to the jump-detector thresholds.
const BETA_SQRT_SIGNAL: f64       = 1.0;   // β=1: impulse = sign(Q)·√|Q| (√ETH), tune thresholds from logs. 0.0 disables.
// EWMA half-life of the signed-qty accumulator (s). Sets the "rolling window":
// shorter → reacts faster, decays sooner. Pick ~ the return horizon β was fit
// against (e.g. 5s).
const SIGNED_QTY_HALFLIFE: f64    = 1.0;
// Per-trade debug: log every ETHUSDT trade folded into Q. Noisy — diagnostic only.
const SIGNED_QTY_DEBUG: bool      = false;
// Rate trigger: pause on a sharp ΔQ swing (onset of one-sided flow), detectable
// 1–2 trades into a burst — earlier than the level threshold. dQ_dt is ΔQ per
// second, EWMA-smoothed. dt is floored to avoid blow-up on same-ms trades.
const QRATE_HALFLIFE: f64         = 0.3;   // smoothing of dQ/dt (s)
const QRATE_DT_FLOOR: f64         = 0.002; // min dt for the rate divide (s)
const QRATE_PAUSE_THRESHOLD: f64  = 40.0;  // |dQ/dt| (ETH/s) to pause — tune from logs
const QRATE_UNPAUSE_THRESHOLD: f64 = 15.0; // unpause below this
// Pause when |impulse| exceeds this (USD), unpause below the lower bound.
const IMPULSE_PAUSE_THRESHOLD: f64   = 9.0;
const IMPULSE_UNPAUSE_THRESHOLD: f64 = 2.0;

// ─── ETHUSDT SPOT BBO size-imbalance trigger ────────────────────────────────
// ratio = max(bidSz,askSz) / min(bidSz,askSz) — direction-agnostic. A heavily
// one-sided top-of-book on the main market tends to precede the trade going
// that way; since we quote on ETHU we want to dodge those. Hard pause.
const BBO_IMB_PAUSE_RATIO: f64   = 100.0;   // pause when ratio exceeds this
const BBO_IMB_UNPAUSE_RATIO: f64 = 2.0;   // unpause when ratio drops below this
const BBO_IMB_MIN_SIZE: f64      = 1.01;  // ignore if either side below this (ETH)

// ─── ETHUSDT PERPETUAL triggers (fstream WS) ────────────────────────────────
// Perp leads spot. Same dev / imbalance / derivative shape as the spot signals.
// NOTE: depth@0ms / imbalance disabled — sub bookTicker only.
const PERP_WS_URL: &str = "wss://fstream.binance.com/stream?streams=ethusdt@bookTicker";
const PERP_EWMA_HALFLIFE: f64        = 1.0;   // perp mid EWMA half-life (s)
const PERP_JUMP_THRESHOLD: f64       = 1.8;   // |perp_mid - perp_ewma| pause (USD)
const PERP_UNPAUSE_THRESHOLD: f64    = 0.3;  // unpause below this

// Quote management
const REQUOTE_TICKS: f64  = 5.0;   // tick=0.01 → drift ≥ $0.15 to requote
const MIN_REQUOTE_MS: u64 = 0;
const MIN_EDGE_USDC: f64  = 0.25;
#[allow(dead_code)] const MAX_DELTA_USDC: f64 = 5.0;
const ADVERSE_FRAC: f64   = 0.75;
#[allow(dead_code)] const SKEW_AT_MAX_INV: f64 = 10.0;
const STEP_TICKS: f64     = 6.0;   // ticks BEHIND non-self BBO ($0.20)
const POST_CANCEL_COOLDOWN_MS: u64 = 20;
const STUCK_ORDER_TIMEOUT_MS: u64 = 45_000;

// FIX endpoints. Order Entry port 9000 (Full reports). Market Data on its own
// host/port. VERIFY against current Binance FIX docs before going live.
const FIX_OE_HOST: &str = "fix-oe.binance.com";
const FIX_OE_PORT: u16  = 9002;
const FIX_MD_HOST: &str = "fix-md.binance.com";
const FIX_MD_PORT: u16  = 9002;

// FIX session params
const HEARTBEAT_SECS: u32 = 30;
const RECV_WINDOW_US: u32 = 5_000_000; // 5s
// MDReqIDs — must be unique across subscriptions on a session.
const MDREQ_SPOT_BOOK: &str  = "spot-book";
const MDREQ_SPOT_TRADE: &str = "spot-trade";
const MDREQ_USDT_BOOK: &str  = "usdt-book";
const MDREQ_USDT_TRADE: &str = "usdt-trade";

// Lighter hedge (unchanged)
const LIGHTER_SIGNER_BIN: &str = "/home/ec2-user/ficc/lighter_signer.py";
const LIGHTER_REQ_TIMEOUT_SEC: u64 = 12;
const MIN_HEDGE_QTY: f64 = 0.005;
// Master switch for the Lighter hedger. When false: spot fills still flow
// into the pending_hedge_*_e6 accumulators (and net out as usual), but no
// hedge order is ever sent to Lighter and the fatal-on-failure path is
// inert. Flip back to true to re-enable hedging.
const HEDGING_ENABLED: bool = false;

// ─── STATE ──────────────────────────────────────────────────────────────────

struct State {
    // Books (price * 100 in atomics; sizes via separate e6 fields).
    usdt_bid_x100: AtomicI64, usdt_ask_x100: AtomicI64,
    spot_bid_x100: AtomicI64, spot_ask_x100: AtomicI64,
    spot_bid_sz_e6: AtomicU64, spot_ask_sz_e6: AtomicU64,

    // Basis EWMA (cents).
    basis_x100: AtomicI64,
    basis_n: AtomicU64,
    last_basis_ts_us: AtomicU64,

    // BTCUSDT mid EWMA (lead signal). x100.
    usdt_mid_ewma_x100: AtomicI64,
    last_usdt_ewma_ts_us: AtomicU64,

    // ETHUSDT signed rolling trade qty, EWMA-decayed. Stored e6 (qty * 1e6),
    // signed: taker-buy adds +, taker-sell adds −. Drives the β·√|Q| impulse
    // predictor. last_signed_qty_ts_us timestamps the most recent decay step.
    signed_qty_e6: AtomicI64,
    last_signed_qty_ts_us: AtomicU64,
    // EWMA-smoothed dQ/dt (ETH/s, signed). Stored e6. Drives the rate trigger.
    qrate_e6: AtomicI64,

    // ETHUSDT spot BBO sizes, e6 (qty * 1e6). Drives the BBO-imbalance trigger.
    usdt_bid_sz_e6: AtomicI64,
    usdt_ask_sz_e6: AtomicI64,

    // ─── ETHUSDT PERPETUAL (fstream.binance.com WS) ─────────────────────────
    // Perp leads spot (≈12x volume). Best bid/ask x100 from @bookTicker.
    perp_bid_x100: AtomicI64,
    perp_ask_x100: AtomicI64,
    // Perp mid EWMA x100 + its timestamp — perp jump detector ("perp dev").
    perp_mid_ewma_x100: AtomicI64,
    last_perp_ewma_ts_us: AtomicU64,

    // Inventory.
    inventory_e6: AtomicI64,
    realized_pnl_x1m: AtomicI64,
    cash_x1m: AtomicI64,
    avg_cost_x100: AtomicI64,
    fills_count: AtomicU64,

    // Active orders. id = Binance OrderID (i64); 0 = none.
    bid_id: AtomicI64,
    ask_id: AtomicI64,
    bid_px_x100: AtomicI64,
    ask_px_x100: AtomicI64,

    last_requote_buy_ms: AtomicU64,
    last_requote_sell_ms: AtomicU64,

    // Post-cancel cooldown: timestamp (ms) until which the side cannot place
    // a new order. Set when an ExecutionReport(CANCELED) lands; gives Binance
    // a moment to release the cancelled order's locked balance before the
    // next place is sent (otherwise -2010 "insufficient balance").
    post_cancel_buy_ms: AtomicU64,
    post_cancel_sell_ms: AtomicU64,

    // Last time we sent an order action (place/cancel) on this side. If no
    // ExecutionReport arrives within STUCK_ORDER_TIMEOUT_MS, the slot is
    // force-cleared in tick() (defends against lost ER, mismatched cid, etc).
    last_buy_action_ms: AtomicU64,
    last_sell_action_ms: AtomicU64,

    // Single-side mode: 0 = BUY, 1 = SELL.
    active_side: AtomicU64,

    halted: AtomicBool,
    rate_limit_until_ms: AtomicU64,
    was_paused: AtomicBool,
    pause_epoch: AtomicU64,

    // Lighter hedge state.
    lighter_inv_e6: AtomicI64,
    lighter_cash_x1m: AtomicI64,
    lighter_realized_x1m: AtomicI64,
    lighter_avg_cost_x100: AtomicI64,
    lighter_hedges: AtomicU64,
    pending_hedge_buy_e6: AtomicI64,
    pending_hedge_sell_e6: AtomicI64,
    lighter_bid_x100: AtomicI64,
    lighter_ask_x100: AtomicI64,

    // Cumulative filled qty on the current resting order, per side. Reset on
    // slot clear (terminal ER) and on side flip. Used to size the next place
    // as ORDER_QTY - cum_filled so partial-then-cancelled orders don't get
    // re-placed at full size.
    bid_cum_filled_e6: AtomicI64,
    ask_cum_filled_e6: AtomicI64,
}

impl State {
    const fn new() -> Self {
        Self {
            usdt_bid_x100: AtomicI64::new(0), usdt_ask_x100: AtomicI64::new(0),
            spot_bid_x100: AtomicI64::new(0), spot_ask_x100: AtomicI64::new(0),
            spot_bid_sz_e6: AtomicU64::new(0), spot_ask_sz_e6: AtomicU64::new(0),
            basis_x100: AtomicI64::new(0),
            basis_n: AtomicU64::new(0),
            last_basis_ts_us: AtomicU64::new(0),
            usdt_mid_ewma_x100: AtomicI64::new(0),
            last_usdt_ewma_ts_us: AtomicU64::new(0),
            signed_qty_e6: AtomicI64::new(0),
            last_signed_qty_ts_us: AtomicU64::new(0),
            qrate_e6: AtomicI64::new(0),
            usdt_bid_sz_e6: AtomicI64::new(0),
            usdt_ask_sz_e6: AtomicI64::new(0),
            perp_bid_x100: AtomicI64::new(0),
            perp_ask_x100: AtomicI64::new(0),
            perp_mid_ewma_x100: AtomicI64::new(0),
            last_perp_ewma_ts_us: AtomicU64::new(0),
            inventory_e6: AtomicI64::new(0),
            realized_pnl_x1m: AtomicI64::new(0),
            cash_x1m: AtomicI64::new(0),
            avg_cost_x100: AtomicI64::new(0),
            fills_count: AtomicU64::new(0),
            bid_id: AtomicI64::new(0), ask_id: AtomicI64::new(0),
            bid_px_x100: AtomicI64::new(0), ask_px_x100: AtomicI64::new(0),
            last_requote_buy_ms: AtomicU64::new(0),
            last_requote_sell_ms: AtomicU64::new(0),
            post_cancel_buy_ms: AtomicU64::new(0),
            post_cancel_sell_ms: AtomicU64::new(0),
            last_buy_action_ms: AtomicU64::new(0),
            last_sell_action_ms: AtomicU64::new(0),
            active_side: AtomicU64::new(0),
            halted: AtomicBool::new(false),
            rate_limit_until_ms: AtomicU64::new(0),
            was_paused: AtomicBool::new(false),
            pause_epoch: AtomicU64::new(0),
            lighter_inv_e6: AtomicI64::new(0),
            lighter_cash_x1m: AtomicI64::new(0),
            lighter_realized_x1m: AtomicI64::new(0),
            lighter_avg_cost_x100: AtomicI64::new(0),
            lighter_hedges: AtomicU64::new(0),
            pending_hedge_buy_e6: AtomicI64::new(0),
            pending_hedge_sell_e6: AtomicI64::new(0),
            lighter_bid_x100: AtomicI64::new(0),
            lighter_ask_x100: AtomicI64::new(0),
            bid_cum_filled_e6: AtomicI64::new(0),
            ask_cum_filled_e6: AtomicI64::new(0),
        }
    }
}
static STATE: State = State::new();

static WAKE: OnceLock<Arc<Notify>> = OnceLock::new();
fn wake() { if let Some(n) = WAKE.get() { n.notify_one(); } }

// Global OE handle so handle_exec_report can issue a cancel for dust-remainder
// partials without threading the Router through.
static ORDER_ENTRY_HANDLE: OnceLock<Arc<OrderEntry>> = OnceLock::new();

// σ², k_hat — guarded by parking_lot Mutex (rarely contended).
static SIGMA2: PlMutex<f64> = PlMutex::new(SIGMA_DEFAULT * SIGMA_DEFAULT);
static K_HAT: PlMutex<f64> = PlMutex::new(K_DEFAULT);
static EWMA_DIST: PlMutex<f64> = PlMutex::new(1.0 / K_DEFAULT);
static LAST_TRADE_TS: PlMutex<f64> = PlMutex::new(0.0);
static LAST_MID_SAMPLE: PlMutex<(f64, f64)> = PlMutex::new((0.0, 0.0));

// In-flight order tracker: clientOrderId -> ("BUY"|"SELL"). Added just before
// a NewOrderSingle goes out, removed when its ack returns. Pause-cancel drains
// this to fire cancels by OrigClOrdID for orders whose OrderID isn't known yet.
static INFLIGHT: PlMutex<Option<HashMap<String, (&'static str, u64)>>> = PlMutex::new(None);

fn inflight_init() { *INFLIGHT.lock() = Some(HashMap::with_capacity(8)); }
fn inflight_add(cid: &str, side: &'static str) {
    if let Some(m) = INFLIGHT.lock().as_mut() { m.insert(cid.to_string(), (side, now_ms())); }
}
fn inflight_remove(cid: &str) {
    if let Some(m) = INFLIGHT.lock().as_mut() { m.remove(cid); }
}
fn inflight_drain() -> Vec<(String, &'static str)> {
    INFLIGHT.lock().as_mut()
        .map(|m| m.drain().map(|(c, (s, _))| (c, s)).collect())
        .unwrap_or_default()
}
fn inflight_count(side: &str) -> usize {
    INFLIGHT.lock().as_ref()
        .map(|m| m.values().filter(|(s, _)| *s == side).count())
        .unwrap_or(0)
}
/// Drop inflight entries older than `max_age_ms` — defends against a cid whose
/// ExecutionReport never arrived, which would otherwise wedge inflight_count
/// for that side permanently. Returns dropped (cid, side) pairs.
fn inflight_evict_stale(max_age_ms: u64) -> Vec<(String, &'static str)> {
    let now = now_ms();
    let mut dropped = Vec::new();
    if let Some(m) = INFLIGHT.lock().as_mut() {
        m.retain(|cid, (side, ts)| {
            if now.saturating_sub(*ts) > max_age_ms {
                dropped.push((cid.clone(), *side));
                false
            } else { true }
        });
    }
    dropped
}

// Map clientOrderId -> side for orders we've placed, so an ExecutionReport that
// carries a ClOrdID (and OrderID) can be attributed even before the place ack
// lands. Kept small; entries removed on terminal status.
static CID_SIDE: PlMutex<Option<HashMap<String, &'static str>>> = PlMutex::new(None);
fn cid_side_init() { *CID_SIDE.lock() = Some(HashMap::with_capacity(16)); }
fn cid_side_add(cid: &str, side: &'static str) {
    if let Some(m) = CID_SIDE.lock().as_mut() { m.insert(cid.to_string(), side); }
}
fn cid_side_get(cid: &str) -> Option<&'static str> {
    CID_SIDE.lock().as_ref().and_then(|m| m.get(cid).copied())
}
fn cid_side_remove(cid: &str) {
    if let Some(m) = CID_SIDE.lock().as_mut() { m.remove(cid); }
}

/// Arm the post-cancel cooldown on a side. Called whenever we *send* a cancel
/// (not only when CANCELED comes back), because the order remains balance-locked
/// on Binance until the cancel actually clears.
fn arm_post_cancel_cooldown(side: &str) {
    let until = now_ms() + POST_CANCEL_COOLDOWN_MS;
    match side {
        "BUY"  => { STATE.post_cancel_buy_ms.store(until, Ordering::Relaxed); }
        "SELL" => { STATE.post_cancel_sell_ms.store(until, Ordering::Relaxed); }
        _ => {}
    }
}

// ─── CLOCK ──────────────────────────────────────────────────────────────────

static CLOCK: OnceLock<quanta::Clock> = OnceLock::new();
static WALL_START_MS: AtomicU64 = AtomicU64::new(0);
static QUANTA_START: OnceLock<quanta::Instant> = OnceLock::new();

#[inline(always)]
fn clock() -> &'static quanta::Clock { CLOCK.get_or_init(quanta::Clock::new) }

fn init_clock() {
    let wall = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64;
    WALL_START_MS.store(wall, Ordering::Relaxed);
    QUANTA_START.set(clock().now()).ok();
}

#[inline(always)]
fn now_ms() -> u64 {
    let q = clock().now();
    let start = *QUANTA_START.get().unwrap();
    let delta_ns = q.duration_since(start).as_nanos() as u64;
    WALL_START_MS.load(Ordering::Relaxed) + delta_ns / 1_000_000
}

#[inline(always)]
fn now_us() -> u64 {
    let q = clock().now();
    let start = *QUANTA_START.get().unwrap();
    let delta_ns = q.duration_since(start).as_nanos() as u64;
    WALL_START_MS.load(Ordering::Relaxed) * 1_000 + delta_ns / 1_000
}

#[inline(always)]
fn now_secs_f() -> f64 { now_us() as f64 / 1e6 }

fn ts_log() -> String {
    let n = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
    let s = n.as_secs(); let ms = n.subsec_millis();
    format!("{:02}:{:02}:{:02}.{:03}", (s/3600)%24, (s/60)%60, s%60, ms)
}
macro_rules! log { ($($arg:tt)*) => {{ println!("{} {}", ts_log(), format_args!($($arg)*)) }} }

// ─── COUNTERS ───────────────────────────────────────────────────────────────

static CID_COUNTER: AtomicU64 = AtomicU64::new(1);
fn next_cid_counter() -> u64 { CID_COUNTER.fetch_add(1, Ordering::Relaxed) }

static NOQUOTE_LOG_MS: AtomicU64 = AtomicU64::new(0);

// ─── DECIMAL <-> MANTISSA ───────────────────────────────────────────────────

#[inline]
fn price_to_mantissa(p: f64) -> i64 {
    // exponent -2 → multiply by 100, round to nearest.
    (p / 10f64.powi(PRICE_EXPONENT as i32)).round() as i64
}
#[inline]
fn qty_to_mantissa(q: f64) -> i64 {
    (q / 10f64.powi(QTY_EXPONENT as i32)).round() as i64
}

// ─── LIGHTER HEDGER (subprocess sidecar) — unchanged ────────────────────────

#[derive(Debug)]
struct HedgeResult {
    ok: bool,
    qty: f64,        // filled qty reported by the sidecar
    px: f64,
    err: Option<String>,
    req_qty: f64,    // qty originally requested — used to requeue on failure
}

struct Hedger {
    stdin: AsyncMutex<ChildStdin>,
    pending: AsyncMutex<HashMap<u64, oneshot::Sender<HedgeResult>>>,
    next_id: AtomicU64,
    _child: AsyncMutex<Child>,
}

impl Hedger {
    async fn spawn() -> Result<Arc<Self>> {
        let mut child = Command::new("python3.11")
            .arg(LIGHTER_SIGNER_BIN)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .kill_on_drop(true)
            .spawn()?;

        let stdin = child.stdin.take().ok_or_else(|| anyhow::anyhow!("no stdin"))?;
        let stdout = child.stdout.take().ok_or_else(|| anyhow::anyhow!("no stdout"))?;

        let hedger = Arc::new(Self {
            stdin: AsyncMutex::new(stdin),
            pending: AsyncMutex::new(HashMap::new()),
            next_id: AtomicU64::new(1),
            _child: AsyncMutex::new(child),
        });

        let mut reader = BufReader::new(stdout);
        let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
        loop {
            let mut line = String::new();
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() { anyhow::bail!("lighter_signer ready timeout"); }
            let n = tokio::time::timeout(remaining, reader.read_line(&mut line)).await??;
            if n == 0 { anyhow::bail!("lighter_signer eof before ready"); }
            let trimmed = line.trim();
            if trimmed.is_empty() { continue; }
            if trimmed.contains("\"ready\"") {
                if !trimmed.contains("true") {
                    anyhow::bail!("lighter_signer not ready: {}", trimmed);
                }
                break;
            }
            let bytes = trimmed.as_bytes();
            if memchr::memmem::find(bytes, b"\"bbo\"").is_some() {
                if let Some(bid) = parse_number_field(bytes, b"\"bid\":") {
                    if let Some(ask) = parse_number_field(bytes, b"\"ask\":") {
                        STATE.lighter_bid_x100.store((bid * 100.0) as i64, Ordering::Relaxed);
                        STATE.lighter_ask_x100.store((ask * 100.0) as i64, Ordering::Relaxed);
                    }
                }
            }
        }
        log!("[HEDGER] sidecar ready");

        let h2 = hedger.clone();
        tokio::spawn(async move {
            let mut reader = reader;
            let mut line = String::new();
            loop {
                line.clear();
                match reader.read_line(&mut line).await {
                    Ok(0) => { log!("[HEDGER] sidecar stdout closed"); break; }
                    Ok(_) => {
                        let bytes = line.trim().as_bytes();
                        if memchr::memmem::find(bytes, b"\"bbo\":true").is_some()
                           || memchr::memmem::find(bytes, b"\"bbo\": true").is_some() {
                            if let Some(bid) = parse_number_field(bytes, b"\"bid\":") {
                                if let Some(ask) = parse_number_field(bytes, b"\"ask\":") {
                                    STATE.lighter_bid_x100.store((bid * 100.0) as i64, Ordering::Relaxed);
                                    STATE.lighter_ask_x100.store((ask * 100.0) as i64, Ordering::Relaxed);
                                }
                            }
                            continue;
                        }
                        h2.dispatch_response(bytes).await;
                    }
                    Err(e) => { log!("[HEDGER] read err: {}", e); break; }
                }
            }
        });

        Ok(hedger)
    }

    async fn dispatch_response(&self, line: &[u8]) {
        let id = match extract_int_field(line, b"\"id\":") {
            Some(v) => v as u64,
            None => { log!("[HEDGER] no id in resp: {}", std::str::from_utf8(line).unwrap_or("?")); return; }
        };
        let ok = memchr::memmem::find(line, b"\"ok\":true").is_some()
              || memchr::memmem::find(line, b"\"ok\": true").is_some();
        let qty = parse_number_field(line, b"\"qty\":").unwrap_or(0.0);
        let px = parse_number_field(line, b"\"px\":").unwrap_or(0.0);
        let err = extract_str_field(line, b"\"err\":")
            .and_then(|s| std::str::from_utf8(s).ok())
            .map(|s| s.to_string());

        let sender = self.pending.lock().await.remove(&id);
        if let Some(tx) = sender {
            let _ = tx.send(HedgeResult { ok, qty, px, err, req_qty: 0.0 });
        } else {
            log!("[HEDGER] orphan response id={}", id);
        }
    }

    async fn hedge(&self, side: &str, qty: f64, ref_price: f64) -> HedgeResult {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = oneshot::channel();
        self.pending.lock().await.insert(id, tx);

        let req = format!(
            "{{\"id\":{},\"side\":\"{}\",\"qty\":{:.6},\"ref_price\":{:.2}}}\n",
            id, side, qty, ref_price
        );
        {
            let mut stdin = self.stdin.lock().await;
            if let Err(e) = stdin.write_all(req.as_bytes()).await {
                log!("[HEDGER] stdin write err: {}", e);
                self.pending.lock().await.remove(&id);
                return HedgeResult { ok: false, qty: 0.0, px: 0.0, err: Some(format!("write: {}", e)), req_qty: qty };
            }
            let _ = stdin.flush().await;
        }

        // Stamp req_qty on whatever result we return, so a failure can be
        // requeued by the caller. dispatch_response doesn't know req_qty.
        let mut result = match tokio::time::timeout(Duration::from_secs(LIGHTER_REQ_TIMEOUT_SEC), rx).await {
            Ok(Ok(r)) => r,
            Ok(Err(_)) => HedgeResult { ok: false, qty: 0.0, px: 0.0, err: Some("channel closed".into()), req_qty: qty },
            Err(_) => {
                self.pending.lock().await.remove(&id);
                HedgeResult { ok: false, qty: 0.0, px: 0.0, err: Some("timeout".into()), req_qty: qty }
            }
        };
        result.req_qty = qty;
        result
    }
}

/// Parse `"key":<number>` (unquoted JSON number).
fn parse_number_field(s: &[u8], key: &[u8]) -> Option<f64> {
    let i = memchr::memmem::find(s, key)? + key.len();
    let rest = &s[i..];
    let mut start = 0;
    while start < rest.len() && (rest[start] == b' ' || rest[start] == b'\t') { start += 1; }
    let rest = &rest[start..];
    let mut j = 0;
    while j < rest.len() {
        let c = rest[j];
        if c == b'-' || c == b'+' || c == b'.' || c == b'e' || c == b'E' || c.is_ascii_digit() {
            j += 1;
        } else { break; }
    }
    if j == 0 { return None; }
    std::str::from_utf8(&rest[..j]).ok()?.parse().ok()
}

/// Extract a JSON string field's value. `key` must be the bare key token
/// including quotes and colon, e.g. b"\"err\":". Tolerates whitespace after
/// the colon (json.dumps emits `"err": "..."`). Returns None if the value is
/// `null` or not a quoted string.
#[inline]
fn extract_str_field<'a>(s: &'a [u8], key: &[u8]) -> Option<&'a [u8]> {
    let i = memchr::memmem::find(s, key)? + key.len();
    let rest = &s[i..];
    let mut start = 0;
    while start < rest.len() && (rest[start] == b' ' || rest[start] == b'\t') { start += 1; }
    let rest = &rest[start..];
    // Value must be a quoted string; `null`, numbers, etc. → no string value.
    if rest.first() != Some(&b'"') { return None; }
    let rest = &rest[1..];
    let end = memchr::memchr(b'"', rest)?;
    Some(&rest[..end])
}

#[inline]
fn extract_int_field(s: &[u8], key: &[u8]) -> Option<i64> {
    let i = memchr::memmem::find(s, key)? + key.len();
    let rest = &s[i..];
    let mut start = 0;
    while start < rest.len() && (rest[start] == b' ' || rest[start] == b'\t') { start += 1; }
    let rest = &rest[start..];
    let mut j = 0;
    while j < rest.len() && (rest[j] == b'-' || rest[j].is_ascii_digit()) { j += 1; }
    if j == 0 { return None; }
    std::str::from_utf8(&rest[..j]).ok()?.parse().ok()
}

// ─── ORDER ENTRY (FIX-SBE) ──────────────────────────────────────────────────
//
// Thin wrapper over a FixSession running the Order Entry stream. Provides the
// same surface the old `Aster` struct did: place_limit / cancel / cancel_by_cid.
// All ops are one SBE frame on the wire; ExecutionReport arrives async on the
// read loop and is handled by handle_exec_report.

#[derive(Debug)]
struct OrderAck {
    cid: String,
    side: &'static str,
}

struct OrderEntry {
    session: PlMutex<Option<Arc<FixSession>>>,
}

impl OrderEntry {
    fn new() -> Self { Self { session: PlMutex::new(None) } }

    fn set_session(&self, s: Arc<FixSession>) { *self.session.lock() = Some(s); }
    fn set_session_none(&self) { *self.session.lock() = None; }
    fn session(&self) -> Option<Arc<FixSession>> { self.session.lock().clone() }

    /// Send a NewOrderSingle — LIMIT, post-only, GTC. Fire-and-forget: success
    /// here means the frame went out; the resting/rejected outcome arrives via
    /// ExecutionReport. Returns the cid on successful send.
    async fn place_limit(&self, side: &'static str, price: f64, qty: f64, cid: &str)
        -> Option<OrderAck>
    {
        let sess = self.session()?;
        let side_byte = if side == "BUY" { sbe::SIDE_BUY } else { sbe::SIDE_SELL };
        let price_m = price_to_mantissa(price);
        let qty_m = qty_to_mantissa(qty);
        let cid_b = cid.as_bytes().to_vec();
        let sym = SYMBOL_SPOT.as_bytes().to_vec();

        let t0 = clock().now();
        let res = sess.send_with_seq(move |seq, ts| {
            sbe::encode_new_order_limit_postonly(
                seq, ts, PRICE_EXPONENT, QTY_EXPONENT,
                qty_m, price_m, side_byte, &cid_b, &sym,
            )
        }).await;
        let dt_us = clock().now().duration_since(t0).as_micros();

        match res {
            Ok(seq) => {
                let sb = STATE.spot_bid_x100.load(Ordering::Relaxed) as f64 / 100.0;
                let sa = STATE.spot_ask_x100.load(Ordering::Relaxed) as f64 / 100.0;
                let ub = STATE.usdt_bid_x100.load(Ordering::Relaxed) as f64 / 100.0;
                let ua = STATE.usdt_ask_x100.load(Ordering::Relaxed) as f64 / 100.0;
                log!("[PLACE SEND] {} {:.5} @ {:.2} cid={} seq={} ({}us) | eth={:.2}/{:.2} usdt={:.2}/{:.2} | {}",
                     side, qty, price, cid, seq, dt_us, sb, sa, ub, ua, signal_suffix());
                Some(OrderAck { cid: cid.to_string(), side })
            }
            Err(e) => {
                log!("[PLACE ERR] {} {}", side, e);
                None
            }
        }
    }

    /// Cancel by Binance OrderID.
    async fn cancel(&self, order_id: i64) -> bool {
        let sess = match self.session() { Some(s) => s, None => return false };
        let cid = fix::gen_clordid('c', next_cid_counter());
        let cid_b = cid.into_bytes();
        let sym = SYMBOL_SPOT.as_bytes().to_vec();
        sess.send_with_seq(move |seq, ts| {
            sbe::encode_order_cancel(seq, ts, order_id, &cid_b, &[], &sym)
        }).await.is_ok()
    }

    /// Cancel by OrderID with caller-supplied cid (so caller can track it
    /// in the inflight map and correlate the inbound ExecutionReport).
    async fn cancel_with_cid(&self, order_id: i64, cid: &str) -> bool {
        let sess = match self.session() { Some(s) => s, None => return false };
        let cid_b = cid.as_bytes().to_vec();
        let sym = SYMBOL_SPOT.as_bytes().to_vec();
        sess.send_with_seq(move |seq, ts| {
            sbe::encode_order_cancel(seq, ts, order_id, &cid_b, &[], &sym)
        }).await.is_ok()
    }

    /// Cancel by OrigClOrdID — for in-flight orders whose OrderID is unknown.
    async fn cancel_by_cid(&self, orig_cid: &str) -> bool {
        let sess = match self.session() { Some(s) => s, None => return false };
        let cid = fix::gen_clordid('c', next_cid_counter());
        let cid_b = cid.into_bytes();
        let orig_b = orig_cid.as_bytes().to_vec();
        let sym = SYMBOL_SPOT.as_bytes().to_vec();
        sess.send_with_seq(move |seq, ts| {
            sbe::encode_order_cancel(seq, ts, sbe::NULL_I64, &cid_b, &orig_b, &sym)
        }).await.is_ok()
    }

    /// Cancel both known resting orders. FIX-SBE (in the messages we use) has
    /// no all-orders cancel, so issue individual cancels for whatever we hold.
    async fn cancel_all(&self) -> bool {
        let bid = STATE.bid_id.load(Ordering::Relaxed);
        let ask = STATE.ask_id.load(Ordering::Relaxed);
        let mut ok = true;
        if bid != 0 || ask != 0 {
            log!("[CANCEL SEND] bid={} ask={} | {}", bid, ask, signal_suffix());
        }
        if bid != 0 { ok &= self.cancel(bid).await; }
        if ask != 0 { ok &= self.cancel(ask).await; }
        ok
    }
}

// ─── TICK LOG (off-thread CSV writer) ───────────────────────────────────────

struct TickRow {
    ts_ms: u64,
    usdt_bid: f64, usdt_ask: f64,
    spot_bid: f64, spot_ask: f64,
    lighter_bid: f64, lighter_ask: f64,
    basis: f64, fv: f64,
    inventory: f64,
    bid_quote: f64, ask_quote: f64,
    sigma_bp: f64, k: f64,
    usdt_dev: f64,
    signed_qty: f64,
    impulse: f64,
    qrate: f64,
    perp_dev: f64,
    event: &'static str,
    side: &'static str,
    qty: f64, px: f64,
    realized_pnl: f64,
}

static TICK_QUEUE: OnceLock<Arc<ArrayQueue<TickRow>>> = OnceLock::new();
const TICK_LOG_PATH: &str = "ticks.csv";

fn init_tick_logger() {
    let q: Arc<ArrayQueue<TickRow>> = Arc::new(ArrayQueue::new(8192));
    TICK_QUEUE.set(q.clone()).ok();

    std::thread::spawn(move || {
        use std::fs::OpenOptions;
        use std::io::{BufWriter, Write};
        let exists = std::path::Path::new(TICK_LOG_PATH).exists()
            && std::fs::metadata(TICK_LOG_PATH).map(|m| m.len() > 0).unwrap_or(false);
        let f = OpenOptions::new().create(true).append(true).open(TICK_LOG_PATH).unwrap();
        let mut w = BufWriter::with_capacity(64 * 1024, f);
        if !exists {
            let _ = writeln!(w,
                "ts_ms,usdt_bid,usdt_ask,spot_bid,spot_ask,lighter_bid,lighter_ask,basis,fv,inventory,bid_quote,ask_quote,sigma_bp,k,usdt_dev,signed_qty,impulse,dQ_dt,perp_dev,event,side,qty,px,realized_pnl");
        }
        let mut last_flush = std::time::Instant::now();
        loop {
            let mut wrote = false;
            while let Some(r) = q.pop() {
                let qty_s = if r.qty > 0.0 { format!("{:.6}", r.qty) } else { String::new() };
                let px_s = if r.px > 0.0 { format!("{:.2}", r.px) } else { String::new() };
                let pnl_s = if r.realized_pnl != 0.0 { format!("{:.4}", r.realized_pnl) } else { String::new() };
                let _ = writeln!(w,
                    "{},{:.2},{:.2},{:.2},{:.2},{:.2},{:.2},{:.4},{:.4},{:.6},{:.2},{:.2},{:.4},{:.4},{:.4},{:.4},{:.4},{:.2},{:.2},{},{},{},{},{}",
                    r.ts_ms, r.usdt_bid, r.usdt_ask, r.spot_bid, r.spot_ask,
                    r.lighter_bid, r.lighter_ask,
                    r.basis, r.fv, r.inventory, r.bid_quote, r.ask_quote,
                    r.sigma_bp, r.k, r.usdt_dev, r.signed_qty, r.impulse, r.qrate, r.perp_dev, r.event, r.side, qty_s, px_s, pnl_s);
                wrote = true;
            }
            if wrote && last_flush.elapsed() >= Duration::from_millis(100) {
                let _ = w.flush();
                last_flush = std::time::Instant::now();
            }
            std::thread::sleep(Duration::from_millis(20));
        }
    });
}

#[inline]
fn write_tick(event: &'static str, side: &'static str, qty: f64, px: f64, realized_pnl: f64) {
    let t = now_ms();

    let ub = STATE.usdt_bid_x100.load(Ordering::Relaxed) as f64 / 100.0;
    let ua = STATE.usdt_ask_x100.load(Ordering::Relaxed) as f64 / 100.0;
    let sb = STATE.spot_bid_x100.load(Ordering::Relaxed) as f64 / 100.0;
    let sa = STATE.spot_ask_x100.load(Ordering::Relaxed) as f64 / 100.0;
    let lb = STATE.lighter_bid_x100.load(Ordering::Relaxed) as f64 / 100.0;
    let la = STATE.lighter_ask_x100.load(Ordering::Relaxed) as f64 / 100.0;
    let um = if ub > 0.0 && ua > 0.0 { (ub + ua) / 2.0 } else { 0.0 };
    let sm = if sb > 0.0 && sa > 0.0 { (sb + sa) / 2.0 } else { 0.0 };
    let basis = if sm > 0.0 && um > 0.0 { sm - um } else { 0.0 };
    let fv_ = fair_value();
    let inv = STATE.inventory_e6.load(Ordering::Relaxed) as f64 / 1e6;
    let bid_q = STATE.bid_px_x100.load(Ordering::Relaxed) as f64 / 100.0;
    let ask_q = STATE.ask_px_x100.load(Ordering::Relaxed) as f64 / 100.0;
    let sigma_bp = SIGMA2.lock().sqrt() * 1e4;
    let k = *K_HAT.lock();
    let realized_total = STATE.realized_pnl_x1m.load(Ordering::Relaxed) as f64 / 1e6;

    let usdt_ewma_x100 = STATE.usdt_mid_ewma_x100.load(Ordering::Relaxed);
    let usdt_dev = if ub > 0.0 && ua > 0.0 && usdt_ewma_x100 > 0 {
        let usdt_mid = (ub + ua) / 2.0;
        usdt_mid - usdt_ewma_x100 as f64 / 100.0
    } else { 0.0 };

    if let Some(q) = TICK_QUEUE.get() {
        let _ = q.push(TickRow {
            ts_ms: t,
            usdt_bid: ub, usdt_ask: ua, spot_bid: sb, spot_ask: sa,
            lighter_bid: lb, lighter_ask: la,
            basis, fv: fv_, inventory: inv,
            bid_quote: bid_q, ask_quote: ask_q,
            sigma_bp, k, usdt_dev,
            signed_qty: signed_qty_now(), impulse: impulse_signal(), qrate: qrate_now(),
            perp_dev: perp_dev(),
            event, side, qty, px,
            realized_pnl: if realized_pnl != 0.0 { realized_pnl } else { realized_total },
        });
    }
}

// ─── ESTIMATORS ─────────────────────────────────────────────────────────────

#[inline(always)]
fn ewma_alpha(dt: f64, hl: f64) -> f64 {
    if hl <= 0.0 || dt <= 0.0 { return 0.0; }
    1.0 - (-std::f64::consts::LN_2 * dt / hl).exp()
}

/// σ sampled off BTCUSDT mid (deeper/faster book → cleaner vol signal).
fn sample_sigma(usdt_mid: f64) {
    if usdt_mid <= 0.0 { return; }
    let ts = now_secs_f();
    let mut last = LAST_MID_SAMPLE.lock();
    let (last_mid, last_ts) = *last;
    if last_mid > 0.0 && last_ts > 0.0 {
        let dt = ts - last_ts;
        if dt > 0.0 {
            let r = (usdt_mid / last_mid).ln();
            let r_per_sec_sq = (r * r) / dt;
            let a = ewma_alpha(dt, SIGMA_HALFLIFE);
            let mut s2 = SIGMA2.lock();
            *s2 = (1.0 - a) * *s2 + a * r_per_sec_sq;
        }
    }
    *last = (usdt_mid, ts);
}

/// k estimator off BTCU trades.
fn update_k(price: f64, is_buyer_maker: bool, spot_mid: f64) {
    if spot_mid <= 0.0 { return; }
    let dist = if is_buyer_maker { spot_mid - price } else { price - spot_mid };
    if dist <= 0.0 { return; }
    let ts = now_secs_f();
    let mut last_ts = LAST_TRADE_TS.lock();
    let dt = if *last_ts > 0.0 { ts - *last_ts } else { 1.0 };
    *last_ts = ts;
    drop(last_ts);

    let a = ewma_alpha(dt, K_HALFLIFE);
    let mut ed = EWMA_DIST.lock();
    *ed = (1.0 - a) * *ed + a * dist;
    if *ed > 0.0 {
        *K_HAT.lock() = (1.0 / *ed).max(K_FLOOR);
    }
}

/// Basis EWMA: BTCU mid - BTCUSDT mid. Logged; not fed into quoting.
fn update_basis() {
    let ub = STATE.usdt_bid_x100.load(Ordering::Relaxed);
    let ua = STATE.usdt_ask_x100.load(Ordering::Relaxed);
    let sb = STATE.spot_bid_x100.load(Ordering::Relaxed);
    let sa = STATE.spot_ask_x100.load(Ordering::Relaxed);
    if ub == 0 || ua == 0 || sb == 0 || sa == 0 { return; }
    let usdt_mid = (ub + ua) as f64 / 200.0;
    let spot_mid = (sb + sa) as f64 / 200.0;
    let basis = spot_mid - usdt_mid;

    let now_u = now_us();
    let last = STATE.last_basis_ts_us.swap(now_u, Ordering::Relaxed);
    if last == 0 {
        STATE.basis_x100.store((basis * 100.0) as i64, Ordering::Relaxed);
    } else {
        let dt = (now_u - last) as f64 / 1e6;
        let a = ewma_alpha(dt, BASIS_HALFLIFE);
        let prev = STATE.basis_x100.load(Ordering::Relaxed) as f64 / 100.0;
        let nb = (1.0 - a) * prev + a * basis;
        STATE.basis_x100.store((nb * 100.0) as i64, Ordering::Relaxed);
    }
    STATE.basis_n.fetch_add(1, Ordering::Relaxed);
}

/// Fold one ETHUSDT trade into the signed rolling-qty accumulator. The
/// accumulator decays continuously toward 0 (EWMA with SIGNED_QTY_HALFLIFE),
/// then the trade's qty is added with a sign from the aggressor side:
/// taker-buy (+), taker-sell (−). aggressor_side absent → ignored.
fn update_signed_qty(qty: f64, aggressor_side: u8) {
    if qty <= 0.0 { return; }
    let signed = match aggressor_side {
        sbe::SIDE_BUY  =>  qty,
        sbe::SIDE_SELL => -qty,
        _ => return,
    };
    let now_u = now_us();
    let last = STATE.last_signed_qty_ts_us.swap(now_u, Ordering::Relaxed);
    let prev = STATE.signed_qty_e6.load(Ordering::Relaxed) as f64 / 1e6;
    let decayed = if last == 0 {
        0.0
    } else {
        let dt = (now_u - last) as f64 / 1e6;
        // EWMA decay: surviving fraction is (1 - alpha).
        prev * (1.0 - ewma_alpha(dt, SIGNED_QTY_HALFLIFE))
    };
    let updated = decayed + signed;
    STATE.signed_qty_e6.store((updated * 1e6) as i64, Ordering::Relaxed);

    // dQ/dt: ΔQ over this trade's interval, EWMA-smoothed. dt floored so
    // same-ms trades don't blow the divide. ΔQ uses the decayed prev so
    // pure decay between trades isn't counted as flow.
    if last != 0 {
        let dt = ((now_u - last) as f64 / 1e6).max(QRATE_DT_FLOOR);
        let inst_rate = (updated - decayed) / dt;   // = signed / dt
        let prev_rate = STATE.qrate_e6.load(Ordering::Relaxed) as f64 / 1e6;
        let a = ewma_alpha(dt, QRATE_HALFLIFE);
        let smoothed = prev_rate + a * (inst_rate - prev_rate);
        STATE.qrate_e6.store((smoothed * 1e6) as i64, Ordering::Relaxed);
    }
    if SIGNED_QTY_DEBUG {
        let dt = if last == 0 { 0.0 } else { (now_u - last) as f64 / 1e6 };
        log!("[SQTY] agg={} qty={:.4} signed={:+.4} dt={:.3}s decayed={:+.4} Q={:+.4}",
             aggressor_side as char, qty, signed, dt, decayed, updated);
    }
}

/// Decay the signed-qty accumulator to "now" without adding a trade, then read
/// it. Needed because the raw stored value is only correct as of the last
/// trade; the impulse predictor must see flow fade between trades.
fn signed_qty_now() -> f64 {
    let last = STATE.last_signed_qty_ts_us.load(Ordering::Relaxed);
    let prev = STATE.signed_qty_e6.load(Ordering::Relaxed) as f64 / 1e6;
    if last == 0 { return 0.0; }
    let dt = (now_us() - last) as f64 / 1e6;
    prev * (1.0 - ewma_alpha(dt, SIGNED_QTY_HALFLIFE))
}

/// Formatted "dev/Q/impulse" suffix shared by PLACE/CANCEL/FILL/STATS logs.
fn signal_suffix() -> String {
    let ub = STATE.usdt_bid_x100.load(Ordering::Relaxed);
    let ua = STATE.usdt_ask_x100.load(Ordering::Relaxed);
    let ewma_x100 = STATE.usdt_mid_ewma_x100.load(Ordering::Relaxed);
    let dev = if ub != 0 && ua != 0 && ewma_x100 != 0 {
        (ub + ua) as f64 / 200.0 - ewma_x100 as f64 / 100.0
    } else { 0.0 };
    format!("dev={:+.2} Q={:+.4} impulse={:+.3} dQ/dt={:+.1} pdev={:+.2} bboImb={:+.1}",
            dev, signed_qty_now(), impulse_signal(), qrate_now(), perp_dev(),
            bbo_imbalance_signed())
}

/// β·sign(Q)·√|Q| — predicted short-horizon ETHUSDT FV displacement (USD).
/// Positive → taker-buy pressure, mid expected to rise.
fn impulse_signal() -> f64 {
    if BETA_SQRT_SIGNAL == 0.0 { return 0.0; }
    let q = signed_qty_now();
    BETA_SQRT_SIGNAL * q.signum() * q.abs().sqrt()
}

/// Update BTCUSDT mid EWMA — call on each BTCUSDT book tick.
fn update_usdt_ewma() {
    let ub = STATE.usdt_bid_x100.load(Ordering::Relaxed);
    let ua = STATE.usdt_ask_x100.load(Ordering::Relaxed);
    if ub == 0 || ua == 0 { return; }
    let usdt_mid_x100 = (ub + ua) / 2;

    let now_u = now_us();
    let last = STATE.last_usdt_ewma_ts_us.swap(now_u, Ordering::Relaxed);
    if last == 0 {
        STATE.usdt_mid_ewma_x100.store(usdt_mid_x100, Ordering::Relaxed);
    } else {
        let dt = (now_u - last) as f64 / 1e6;
        let a = ewma_alpha(dt, USDT_LEAD_HALFLIFE);
        let prev = STATE.usdt_mid_ewma_x100.load(Ordering::Relaxed) as f64;
        let nb = (1.0 - a) * prev + a * usdt_mid_x100 as f64;
        STATE.usdt_mid_ewma_x100.store(nb as i64, Ordering::Relaxed);
    }
}

fn best_non_self_bid_x100() -> i64 { STATE.spot_bid_x100.load(Ordering::Relaxed) }
fn best_non_self_ask_x100() -> i64 { STATE.spot_ask_x100.load(Ordering::Relaxed) }

/// Mid of best non-self bid/ask. FV anchor.
fn spot_mid_no_self() -> f64 {
    let b = best_non_self_bid_x100();
    let a = best_non_self_ask_x100();
    if b == 0 || a == 0 { return 0.0; }
    (b + a) as f64 / 200.0
}

/// Fair value = BTCU non-self mid.
fn fair_value() -> f64 { spot_mid_no_self() }

/// True while ETHUSDT is moving sharply. Two independent triggers, OR'd:
///   1. Jump detector: |usdt_mid - usdt_ewma| over the book EWMA.
///   2. Impulse predictor: |β·sign(Q)·√|Q|| over the ETHUSDT trade tape.
/// Each has its own hysteresis (pause above the high bound, unpause only
/// below the low bound). was_paused holds the combined latch.
fn is_paused() -> bool {
    let was = STATE.was_paused.load(Ordering::Relaxed);

    // Trigger 1 — book jump detector.
    let jump = {
        let ub = STATE.usdt_bid_x100.load(Ordering::Relaxed);
        let ua = STATE.usdt_ask_x100.load(Ordering::Relaxed);
        let ewma_x100 = STATE.usdt_mid_ewma_x100.load(Ordering::Relaxed);
        if ub == 0 || ua == 0 || ewma_x100 == 0 {
            false
        } else {
            let usdt_mid = (ub + ua) as f64 / 200.0;
            let ewma = ewma_x100 as f64 / 100.0;
            let dev = (usdt_mid - ewma).abs();
            if was { dev > USDT_UNPAUSE_THRESHOLD } else { dev > USDT_JUMP_THRESHOLD }
        }
    };

    // Trigger 2 — trade-flow impulse predictor.
    let impulse_active = if BETA_SQRT_SIGNAL == 0.0 {
        false
    } else {
        let imp = impulse_signal().abs();
        if was { imp > IMPULSE_UNPAUSE_THRESHOLD } else { imp > IMPULSE_PAUSE_THRESHOLD }
    };

    // Trigger 3 — dQ/dt rate: catches the onset of a one-sided burst 1–2
    // trades in, before the impulse level threshold is crossed.
    let rate_active = {
        let r = qrate_now().abs();
        if was { r > QRATE_UNPAUSE_THRESHOLD } else { r > QRATE_PAUSE_THRESHOLD }
    };

    // Trigger 4 — perp jump: |perp_mid - perp_ewma|. Perp leads spot, so this
    // fires earlier than the spot jump detector.
    let perp_jump = {
        let d = perp_dev().abs();
        if d == 0.0 { false }
        else if was { d > PERP_UNPAUSE_THRESHOLD } else { d > PERP_JUMP_THRESHOLD }
    };

    // NOTE: ETHUSDT spot BBO imbalance is NOT a full-pause trigger — it is
    // handled directionally in pause_edge_update (cancels only the threatened
    // side). See the BBO-imbalance block there.
    jump || impulse_active || rate_active || perp_jump
}

/// EWMA-smoothed dQ/dt (ETH/s, signed). Read-only.
fn qrate_now() -> f64 {
    STATE.qrate_e6.load(Ordering::Relaxed) as f64 / 1e6
}

/// Signed BBO imbalance: + = bid-heavy, − = ask-heavy, magnitude is the ratio
/// max(bidSz,askSz)/min(...). e.g. ask 18 / bid 5 → -3.6. Returns 0.0 if either
/// side is below BBO_IMB_MIN_SIZE or not yet populated.
fn bbo_imbalance_signed() -> f64 {
    let b = STATE.usdt_bid_sz_e6.load(Ordering::Relaxed) as f64 / 1e6;
    let a = STATE.usdt_ask_sz_e6.load(Ordering::Relaxed) as f64 / 1e6;
    if b < BBO_IMB_MIN_SIZE || a < BBO_IMB_MIN_SIZE { return 0.0; }
    if b >= a { b / a } else { -(a / b) }
}

/// Read-only pause check; run_md_usdt is the sole writer of was_paused.
fn pause_edge_check() -> bool { STATE.was_paused.load(Ordering::Relaxed) }

/// Rising/falling-edge pause handler. Called from run_md_usdt on every ETHUSDT
/// book tick and trade batch. On the rising edge (not paused → paused) it
/// latches was_paused, bumps pause_epoch, and fires cancels for all resting
/// and in-flight orders exactly once per pause window. On the falling edge it
/// clears the latch. `src` is just a log tag ("book" | "trade").
fn pause_edge_update(oe: &Arc<OrderEntry>, src: &str) {
    let paused = is_paused();
    let was = STATE.was_paused.load(Ordering::Relaxed);
    if paused && !was {
        STATE.was_paused.store(true, Ordering::Relaxed);
        STATE.pause_epoch.fetch_add(1, Ordering::Relaxed);
        let bid = STATE.bid_id.load(Ordering::Relaxed);
        let ask = STATE.ask_id.load(Ordering::Relaxed);
        let inflight = inflight_drain();
        let had_work = bid != 0 || ask != 0 || !inflight.is_empty();
        if had_work {
            let cur_bid_px = STATE.bid_px_x100.load(Ordering::Relaxed) as f64 / 100.0;
            let cur_ask_px = STATE.ask_px_x100.load(Ordering::Relaxed) as f64 / 100.0;
            let oe2 = oe.clone();
            tokio::spawn(async move { let _ = oe2.cancel_all().await; });
            for (cid, side) in inflight {
                let oe3 = oe.clone();
                arm_post_cancel_cooldown(side);
                tokio::spawn(async move { let _ = oe3.cancel_by_cid(&cid).await; });
            }
            // Arm cooldowns; do NOT zero local state — wait for CANCELED reports.
            if bid != 0 { arm_post_cancel_cooldown("BUY"); }
            if ask != 0 { arm_post_cancel_cooldown("SELL"); }
            if bid != 0 { write_tick("PAUSE", "BUY", 0.0, cur_bid_px, 0.0); }
            if ask != 0 { write_tick("PAUSE", "SELL", 0.0, cur_ask_px, 0.0); }
        }
        // Always log the rising edge, regardless of whether there were orders
        // to cancel. Otherwise a silent latch can be followed by [FAST-PAUSE
        // resumed] with no matching pause line, which is confusing.
        log!("[FAST-PAUSE] paused from MD-USDT ({}) work={} | {}",
             src, had_work, signal_suffix());
    } else if !paused && was {
        STATE.was_paused.store(false, Ordering::Relaxed);
        log!("[FAST-PAUSE] resumed");
    }

    // ── Directional BBO-imbalance cancel ────────────────────────────────────
    // Separate from the global pause: a heavily one-sided ETHUSDT BBO predicts
    // the trade going that way, so cancel ONLY the resting order on the side
    // that would be run over — keep the other side working.
    //   ask-heavy (sellers, price likely ↓) → cancel our resting BUY
    //   bid-heavy (buyers,  price likely ↑) → cancel our resting SELL
    // The post-cancel cooldown doubles as a re-fire guard: once we cancel a
    // side it stays armed for POST_CANCEL_COOLDOWN_MS, so we don't re-send a
    // cancel every tick while waiting for the CANCELED report.
    {
        let sig = bbo_imbalance_signed();   // + bid-heavy, − ask-heavy
        let ratio = sig.abs();
        if ratio > BBO_IMB_PAUSE_RATIO {
            let now = now_ms();
            if sig < 0.0 {
                // ask-heavy → threatens our BUY
                let bid = STATE.bid_id.load(Ordering::Relaxed);
                let armed = now < STATE.post_cancel_buy_ms.load(Ordering::Relaxed);
                if bid != 0 && !armed {
                    arm_post_cancel_cooldown("BUY");
                    let oe2 = oe.clone();
                    tokio::spawn(async move { let _ = oe2.cancel(bid).await; });
                    let px = STATE.bid_px_x100.load(Ordering::Relaxed) as f64 / 100.0;
                    write_tick("BBO-CXL", "BUY", 0.0, px, 0.0);
                    log!("[BBO-CANCEL] BUY cancelled, ask-heavy ratio={:.1} | {}",
                         ratio, signal_suffix());
                }
            } else if sig > 0.0 {
                // bid-heavy → threatens our SELL
                let ask = STATE.ask_id.load(Ordering::Relaxed);
                let armed = now < STATE.post_cancel_sell_ms.load(Ordering::Relaxed);
                if ask != 0 && !armed {
                    arm_post_cancel_cooldown("SELL");
                    let oe2 = oe.clone();
                    tokio::spawn(async move { let _ = oe2.cancel(ask).await; });
                    let px = STATE.ask_px_x100.load(Ordering::Relaxed) as f64 / 100.0;
                    write_tick("BBO-CXL", "SELL", 0.0, px, 0.0);
                    log!("[BBO-CANCEL] SELL cancelled, bid-heavy ratio={:.1} | {}",
                         ratio, signal_suffix());
                }
            }
        }
    }
}

fn spot_mid() -> f64 {
    let sb = STATE.spot_bid_x100.load(Ordering::Relaxed);
    let sa = STATE.spot_ask_x100.load(Ordering::Relaxed);
    if sb == 0 || sa == 0 { return 0.0; }
    (sb + sa) as f64 / 200.0
}

/// True if BTCU BBO drifted more than REQUOTE_TICKS/2 on either side since the
/// snapshot — used to detect a place that straddled a book move.
#[inline]
fn bbo_moved_during_send(bid_at_send_x100: i64, ask_at_send_x100: i64) -> bool {
    if bid_at_send_x100 == 0 || ask_at_send_x100 == 0 { return false; }
    let now_bid = STATE.spot_bid_x100.load(Ordering::Relaxed);
    let now_ask = STATE.spot_ask_x100.load(Ordering::Relaxed);
    if now_bid == 0 || now_ask == 0 { return false; }
    let thresh_x100 = ((REQUOTE_TICKS * 0.5) * TICK_SIZE * 100.0) as i64;
    (now_bid - bid_at_send_x100).abs() > thresh_x100
        || (now_ask - ask_at_send_x100).abs() > thresh_x100
}

// ─── QUOTE COMPUTATION ──────────────────────────────────────────────────────

#[inline]
fn round_price(p: f64) -> f64 {
    ((p / TICK_SIZE).round() as i64) as f64 * TICK_SIZE
}
#[inline]
fn round_qty(q: f64) -> f64 {
    ((q / LOT_SIZE).round() as i64) as f64 * LOT_SIZE
}

/// Returns (bid_px, ask_px) or (0,0) if unsafe to quote.
fn compute_quotes() -> (f64, f64) {
    let fv = fair_value();
    if fv <= 0.0 { return (0.0, 0.0); }

    let nb = best_non_self_bid_x100();
    let na = best_non_self_ask_x100();
    if nb == 0 || na == 0 { return (0.0, 0.0); }
    let non_self_bid = nb as f64 / 100.0;
    let non_self_ask = na as f64 / 100.0;
    if non_self_bid >= non_self_ask { return (0.0, 0.0); }

    // Keep σ/k computed elsewhere for stats; quoting is now simple:
    // bid = mid - MIN_EDGE_USDC, ask = mid + MIN_EDGE_USDC, then clamp
    // behind non-self BBO by STEP_TICKS.
    // Volatility scaling: edge and step multiplied by 1.5^floor(sigma_bp).
    // sigma_bp < 1 → 1x, [1,2) → 1.5x, [2,3) → 2.25x, [3,4) → 3.375x, ...
    let sigma_bp = (*SIGMA2.lock()).max(0.0).sqrt() * 1e4;
    let vol_mult = 2_f64.powf(sigma_bp.floor());
    let edge = MIN_EDGE_USDC * vol_mult;
    let step = STEP_TICKS * vol_mult;

    let mid = fv;
    let mut bid_px = round_price(mid - edge);
    let mut ask_px = round_price(mid + edge);

    let behind_bid = round_price(non_self_bid - step * TICK_SIZE);
    let behind_ask = round_price(non_self_ask + step * TICK_SIZE);
    if bid_px > behind_bid { bid_px = behind_bid; }
    if ask_px < behind_ask { ask_px = behind_ask; }

    if bid_px >= ask_px { return (0.0, 0.0); }
    (bid_px, ask_px)
}

// ─── ORDER ROUTER ───────────────────────────────────────────────────────────
//
// Single mutex per side to serialize cancel-then-place. Cheap — ops are
// infrequent (~once per requote).

struct Router {
    oe: Arc<OrderEntry>,
    buy_lock: AsyncMutex<()>,
    sell_lock: AsyncMutex<()>,
}

impl Router {
    fn new(oe: Arc<OrderEntry>) -> Self {
        Self { oe, buy_lock: AsyncMutex::new(()), sell_lock: AsyncMutex::new(()) }
    }

    async fn reconcile(&self, side: &str, target_px: f64, qty: f64) {
        if STATE.halted.load(Ordering::Relaxed) { return; }
        if now_ms() < STATE.rate_limit_until_ms.load(Ordering::Relaxed) { return; }

        // Jump pause: cancel both sides once per pause window. Only the BUY
        // task fires the cancels to avoid duplicates.
        if pause_edge_check() {
            if side == "BUY" {
                let bid = STATE.bid_id.load(Ordering::Relaxed);
                let ask = STATE.ask_id.load(Ordering::Relaxed);
                if bid != 0 || ask != 0 {
                    let cur_bid_px = STATE.bid_px_x100.load(Ordering::Relaxed) as f64 / 100.0;
                    let cur_ask_px = STATE.ask_px_x100.load(Ordering::Relaxed) as f64 / 100.0;
                    let oe = self.oe.clone();
                    tokio::spawn(async move { let _ = oe.cancel_all().await; });
                    // Arm cooldowns; do NOT zero local state — wait for the
                    // CANCELED ExecutionReport to clear bid_id/ask_id/px.
                    if bid != 0 { arm_post_cancel_cooldown("BUY"); }
                    if ask != 0 { arm_post_cancel_cooldown("SELL"); }
                    if bid != 0 { write_tick("PAUSE", "BUY", qty, cur_bid_px, 0.0); }
                    if ask != 0 { write_tick("PAUSE", "SELL", qty, cur_ask_px, 0.0); }
                }
            }
            return;
        }

        let _g = match side {
            "BUY" => self.buy_lock.lock().await,
            _     => self.sell_lock.lock().await,
        };

        let (cur_id_atom, cur_px_atom, last_rq_atom) = match side {
            "BUY" => (&STATE.bid_id, &STATE.bid_px_x100, &STATE.last_requote_buy_ms),
            _     => (&STATE.ask_id, &STATE.ask_px_x100, &STATE.last_requote_sell_ms),
        };

        // Inventory caps: only allow the unwind side. In single-side mode also
        // force-flip active_side so the next tick quotes the unwind side.
        let inv = STATE.inventory_e6.load(Ordering::Relaxed) as f64 / 1e6;
        if side == "BUY" && inv >= MAX_INVENTORY {
            let id = cur_id_atom.load(Ordering::Relaxed);
            if id != 0 {
                let _ = self.oe.cancel(id).await;
                arm_post_cancel_cooldown("BUY");
            }
            if SINGLE_SIDE_MODE && STATE.active_side.swap(1, Ordering::Relaxed) != 1 {
                STATE.bid_cum_filled_e6.store(0, Ordering::Relaxed);
                STATE.ask_cum_filled_e6.store(0, Ordering::Relaxed);
                log!("[FLIP] inv cap +{:.6} → SELL", inv);
                wake();
            }
            return;
        }
        if side == "SELL" && inv <= -MAX_INVENTORY {
            let id = cur_id_atom.load(Ordering::Relaxed);
            if id != 0 {
                let _ = self.oe.cancel(id).await;
                arm_post_cancel_cooldown("SELL");
            }
            if SINGLE_SIDE_MODE && STATE.active_side.swap(0, Ordering::Relaxed) != 0 {
                STATE.bid_cum_filled_e6.store(0, Ordering::Relaxed);
                STATE.ask_cum_filled_e6.store(0, Ordering::Relaxed);
                log!("[FLIP] inv cap {:+.6} → BUY", inv);
                wake();
            }
            return;
        }

        let now_m = now_ms();
        if now_m.saturating_sub(last_rq_atom.load(Ordering::Relaxed)) < MIN_REQUOTE_MS {
            return;
        }

        // Post-cancel cooldown: a freshly cancelled order may still have its
        // funds locked on Binance for a brief window. Placing during that
        // window returns -2010 "insufficient balance".
        let cooldown_atom = match side {
            "BUY" => &STATE.post_cancel_buy_ms,
            _     => &STATE.post_cancel_sell_ms,
        };
        if now_m < cooldown_atom.load(Ordering::Relaxed) { return; }

        // BBO-imbalance gate: while the ETHUSDT BBO is heavily one-sided
        // against this side, don't re-quote it (it was just cancelled, or
        // would be immediately threatened). Hysteresis — blocks until the
        // ratio falls below BBO_IMB_UNPAUSE_RATIO. Mirror of the directional
        // cancel in pause_edge_update.
        {
            let sig = bbo_imbalance_signed();   // + bid-heavy, − ask-heavy
            let blocked = sig.abs() > BBO_IMB_UNPAUSE_RATIO
                && ((side == "BUY"  && sig < 0.0)    // ask-heavy threatens BUY
                 || (side == "SELL" && sig > 0.0));  // bid-heavy threatens SELL
            if blocked { return; }
        }

        let cur_id = cur_id_atom.load(Ordering::Relaxed);
        let cur_px = cur_px_atom.load(Ordering::Relaxed) as f64 / 100.0;

        let s: &'static str = if side == "BUY" { "BUY" } else { "SELL" };

        if inflight_count(s) > 0 { return; }

        if cur_id == 0 {
            // Recompute target from fresh BBO right before send.
            let (fresh_bid, fresh_ask) = compute_quotes();
            let fresh_target = if side == "BUY" { fresh_bid } else { fresh_ask };
            if fresh_target <= 0.0 || fresh_target * qty < MIN_NOTIONAL { return; }
            if STATE.was_paused.load(Ordering::Relaxed) { return; }

            let cid = fix::gen_clordid('m', next_cid_counter());
            inflight_add(&cid, s);
            cid_side_add(&cid, s);

            // Stamp epoch at send; spawn a retry loop that fires cancel_by_cid
            // every 40ms while the place is in flight if the pause epoch changes.
            let epoch_at_send = STATE.pause_epoch.load(Ordering::Relaxed);
            let cancel_cid = cid.clone();
            let cancel_oe = self.oe.clone();
            let cancel_done = Arc::new(AtomicBool::new(false));
            let cancel_done2 = cancel_done.clone();
            tokio::spawn(async move {
                let deadline = tokio::time::Instant::now() + Duration::from_millis(800);
                let mut iv = tokio::time::interval(Duration::from_millis(40));
                iv.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                iv.tick().await;
                while tokio::time::Instant::now() < deadline {
                    if cancel_done2.load(Ordering::Relaxed) { return; }
                    if STATE.pause_epoch.load(Ordering::Relaxed) != epoch_at_send {
                        let _ = cancel_oe.cancel_by_cid(&cancel_cid).await;
                    }
                    iv.tick().await;
                }
            });

            let bbo_bid_at_send = STATE.spot_bid_x100.load(Ordering::Relaxed);
            let bbo_ask_at_send = STATE.spot_ask_x100.load(Ordering::Relaxed);

            let ack_opt = self.oe.place_limit(s, fresh_target, qty, &cid).await;
            cancel_done.store(true, Ordering::Relaxed);
            // Stamp activity timestamp — used by tick()'s stuck-order watchdog.
            match s {
                "BUY"  => STATE.last_buy_action_ms.store(now_ms(), Ordering::Relaxed),
                "SELL" => STATE.last_sell_action_ms.store(now_ms(), Ordering::Relaxed),
                _ => {}
            }

            if ack_opt.is_some() {
                last_rq_atom.store(now_m, Ordering::Relaxed);
                // The order's resting state and OrderID are established by the
                // inbound ExecutionReport keyed on this cid. If a pause or BBO
                // move happened during the send window, cancel by cid (OrderID
                // not yet known).
                if STATE.pause_epoch.load(Ordering::Relaxed) != epoch_at_send
                    || STATE.was_paused.load(Ordering::Relaxed) {
                    let oe = self.oe.clone();
                    let c = cid.clone();
                    tokio::spawn(async move { let _ = oe.cancel_by_cid(&c).await; });
                    write_tick("PAUSE", s, qty, fresh_target, 0.0);
                } else if bbo_moved_during_send(bbo_bid_at_send, bbo_ask_at_send) {
                    let oe = self.oe.clone();
                    let c = cid.clone();
                    tokio::spawn(async move { let _ = oe.cancel_by_cid(&c).await; });
                    log!("[STALE] cancel cid={} (BBO moved during place)", cid);
                    write_tick("STALE", s, qty, fresh_target, 0.0);
                } else {
                    // Provisionally record the price; OrderID is bound by the
                    // inbound ExecutionReport (NEW) for this cid.
                    cur_px_atom.store((fresh_target * 100.0) as i64, Ordering::Relaxed);
                    write_tick("PLACE", s, qty, fresh_target, 0.0);
                }
            }
            return;
        }

        // Existing order: cancel-replace if drift large or adverse.
        let drift_ticks = (cur_px - target_px).abs() / TICK_SIZE;
        let fv = fair_value();
        let edge_now = (cur_px - fv).abs();
        let cur_delta = (target_px - fv).abs();
        let adverse = edge_now < ADVERSE_FRAC * cur_delta;

        if drift_ticks < REQUOTE_TICKS && !adverse { return; }
        if (target_px - cur_px).abs() < TICK_SIZE / 2.0 { return; }

        // Issue cancel; do NOT zero cur_id/cur_px optimistically — wait for
        // the cancel's ExecutionReport(CANCELED) in handle_exec_report to
        // clear the slot. Also register the cancel cid in inflight so the
        // next tick sees the side as busy and won't place a duplicate.
        let cancel_cid = fix::gen_clordid('c', next_cid_counter());
        inflight_add(&cancel_cid, s);
        cid_side_add(&cancel_cid, s);
        let ok = self.oe.cancel_with_cid(cur_id, &cancel_cid).await;
        if !ok {
            inflight_remove(&cancel_cid);
            cid_side_remove(&cancel_cid);
            return;
        }
        arm_post_cancel_cooldown(s);
        match s {
            "BUY"  => STATE.last_buy_action_ms.store(now_ms(), Ordering::Relaxed),
            "SELL" => STATE.last_sell_action_ms.store(now_ms(), Ordering::Relaxed),
            _ => {}
        }
        write_tick("CANCEL", s, qty, cur_px, 0.0);
        // Return: next tick (or the inbound CANCELED ExecutionReport, whichever
        // is first) drives the replacement. This means cancel-and-replace is
        // now two ticks instead of one, but it's safe: no duplicate orders.
    }
}

// ─── FILL HANDLING ──────────────────────────────────────────────────────────
//
// Driven by ExecutionReport from the OE read loop. The Order Entry session
// runs with ResponseMode=Everything so we receive full ExecutionReports (with
// fill price/qty). ExecutionReportAck is also handled in case the session is
// reconfigured to Mini/OnlyAcks.

fn handle_exec_report(er: &sbe::ExecutionReport, hedger: &Arc<Hedger>) {
    let cid = String::from_utf8_lossy(&er.cl_ord_id).into_owned();
    let order_id = er.order_id;

    // Resolve the side: prefer the SBE Side field; fall back to cid map.
    let side: &'static str = match er.side {
        sbe::SIDE_BUY => "BUY",
        sbe::SIDE_SELL => "SELL",
        _ => cid_side_get(&cid).unwrap_or(""),
    };

    let status = er.ord_status;
    let exec_type = er.exec_type;

    let is_fill = exec_type == sbe::EXECTYPE_TRADE
        && (status == sbe::ORDSTATUS_PARTIALLY_FILLED || status == sbe::ORDSTATUS_FILLED);
    let is_new = exec_type == sbe::EXECTYPE_NEW || status == sbe::ORDSTATUS_NEW;
    let is_terminal_nonfill = matches!(
        status,
        sbe::ORDSTATUS_CANCELED | sbe::ORDSTATUS_REJECTED | sbe::ORDSTATUS_EXPIRED
    );

    // Bind OrderID to our side slot when the order is first acknowledged NEW.
    if is_new && order_id != 0 && !cid.is_empty() {
        match side {
            "BUY"  => { STATE.bid_id.store(order_id, Ordering::Relaxed); }
            "SELL" => { STATE.ask_id.store(order_id, Ordering::Relaxed); }
            _ => {}
        }
    }

    // Resolve the in-flight gate. Any non-pending status (NEW, filled, canceled,
    // rejected, expired) means this cid is no longer "in flight" — drop it from
    // the inflight map so the next tick for this side may act. This is the
    // counterpart to NOT calling inflight_remove in reconcile.
    let resolves_inflight = is_new
        || status == sbe::ORDSTATUS_FILLED
        || is_terminal_nonfill
        || exec_type == sbe::EXECTYPE_REJECTED;
    if resolves_inflight && !cid.is_empty() {
        inflight_remove(&cid);
    }

    // Clear our side slot on any terminal status for our order.
    if order_id != 0 {
        let terminal = status == sbe::ORDSTATUS_FILLED || is_terminal_nonfill;
        if terminal {
            let was_canceled = status == sbe::ORDSTATUS_CANCELED;
            // Reset cum_filled only when the order is truly done (FILLED) or
            // dead (REJECTED/EXPIRED). On CANCELED, keep cum_filled so the
            // next place sizes as ORDER_QTY - already_filled.
            let reset_cum = !was_canceled;
            if STATE.bid_id.load(Ordering::Relaxed) == order_id {
                STATE.bid_id.store(0, Ordering::Relaxed);
                STATE.bid_px_x100.store(0, Ordering::Relaxed);
                if reset_cum { STATE.bid_cum_filled_e6.store(0, Ordering::Relaxed); }
                if was_canceled {
                    STATE.post_cancel_buy_ms.store(
                        now_ms() + POST_CANCEL_COOLDOWN_MS, Ordering::Relaxed);
                }
            }
            if STATE.ask_id.load(Ordering::Relaxed) == order_id {
                STATE.ask_id.store(0, Ordering::Relaxed);
                STATE.ask_px_x100.store(0, Ordering::Relaxed);
                if reset_cum { STATE.ask_cum_filled_e6.store(0, Ordering::Relaxed); }
                if was_canceled {
                    STATE.post_cancel_sell_ms.store(
                        now_ms() + POST_CANCEL_COOLDOWN_MS, Ordering::Relaxed);
                }
            }
            cid_side_remove(&cid);
        }
    }

    if exec_type == sbe::EXECTYPE_REJECTED {
        log!("[REJECT] cid={} order_id={} err={} text={}",
             cid, order_id, er.error_code,
             String::from_utf8_lossy(&er.error_text));
        match side {
            "BUY"  => { STATE.bid_px_x100.store(0, Ordering::Relaxed); }
            "SELL" => { STATE.ask_px_x100.store(0, Ordering::Relaxed); }
            _ => {}
        }
        cid_side_remove(&cid);
        if er.error_code == -1015 {
            STATE.halted.store(true, Ordering::Relaxed);
            STATE.rate_limit_until_ms.store(now_ms() + 1_000, Ordering::Relaxed);
            log!("[RATE LIMIT] -1015 — halting placement 1s");
        }
        // -2010 with "insufficient balance" → fatal. Avoid repeated retries
        // that would trip Binance's abuse limits.
        if er.error_code == -2010 {
            let txt = String::from_utf8_lossy(&er.error_text);
            if txt.contains("insufficient balance") || txt.contains("Insufficient balance") {
                log!("[FATAL] insufficient balance — exiting to avoid ban");
                std::process::exit(1);
            }
        }
        return;
    }

    if !is_fill { return; }

    let last_qty = sbe::decimal_to_f64(er.last_qty, er.qty_exponent);
    let last_px = sbe::decimal_to_f64(er.last_px, er.price_exponent);
    if last_qty <= 0.0 || last_px <= 0.0 { return; }
    if side.is_empty() { return; }

    // Apply fill to inventory and PnL.
    let inv_e6 = STATE.inventory_e6.load(Ordering::Relaxed);
    let inv = inv_e6 as f64 / 1e6;
    let avg = STATE.avg_cost_x100.load(Ordering::Relaxed) as f64 / 100.0;
    let mut new_inv = inv;
    let mut new_avg = avg;
    let mut realized_delta = 0.0;
    let mut cash_delta = 0.0;

    if side == "BUY" {
        if inv < 0.0 {
            let closed = last_qty.min(-inv);
            realized_delta += (avg - last_px) * closed;
            let remaining = last_qty - closed;
            new_inv = inv + last_qty;
            if remaining > 0.0 { new_avg = last_px; }
        } else {
            new_inv = inv + last_qty;
            if new_inv > 0.0 {
                new_avg = (avg * inv + last_px * last_qty) / new_inv;
            }
        }
        cash_delta = -last_qty * last_px;
    } else {
        if inv > 0.0 {
            let closed = last_qty.min(inv);
            realized_delta += (last_px - avg) * closed;
            let remaining = last_qty - closed;
            new_inv = inv - last_qty;
            if remaining > 0.0 { new_avg = last_px; }
        } else {
            let new_short = -inv + last_qty;
            new_inv = inv - last_qty;
            if new_short > 0.0 {
                new_avg = (avg * (-inv) + last_px * last_qty) / new_short;
            }
        }
        cash_delta = last_qty * last_px;
    }

    if new_inv.abs() < 1e-9 { new_avg = 0.0; }

    STATE.inventory_e6.store((new_inv * 1e6).round() as i64, Ordering::Relaxed);
    STATE.avg_cost_x100.store((new_avg * 100.0).round() as i64, Ordering::Relaxed);
    STATE.realized_pnl_x1m.fetch_add((realized_delta * 1e6).round() as i64, Ordering::Relaxed);
    STATE.cash_x1m.fetch_add((cash_delta * 1e6).round() as i64, Ordering::Relaxed);
    STATE.fills_count.fetch_add(1, Ordering::Relaxed);

    // Track cumulative filled on the resting order for accurate re-place sizing.
    let last_qty_e6 = (last_qty * 1e6) as i64;
    match side {
        "BUY"  => { STATE.bid_cum_filled_e6.fetch_add(last_qty_e6, Ordering::Relaxed); }
        "SELL" => { STATE.ask_cum_filled_e6.fetch_add(last_qty_e6, Ordering::Relaxed); }
        _ => {}
    }

    let total_real = STATE.realized_pnl_x1m.load(Ordering::Relaxed) as f64 / 1e6;
    let sm = spot_mid();
    let unreal = if new_avg > 0.0 && sm > 0.0 { new_inv * (sm - new_avg) } else { 0.0 };
    let cash = STATE.cash_x1m.load(Ordering::Relaxed) as f64 / 1e6;
    let mtm = cash + new_inv * sm;
    let ub = STATE.usdt_bid_x100.load(Ordering::Relaxed) as f64 / 100.0;
    let ua = STATE.usdt_ask_x100.load(Ordering::Relaxed) as f64 / 100.0;
    let sb = STATE.spot_bid_x100.load(Ordering::Relaxed) as f64 / 100.0;
    let sa = STATE.spot_ask_x100.load(Ordering::Relaxed) as f64 / 100.0;
    log!("[FILL] {} {:.5} @ {:.2} | inv={:+.6} real={:+.4} unreal={:+.4} mtm={:+.4} | eth={:.2}/{:.2} usdt={:.2}/{:.2} | {}",
         side, last_qty, last_px, new_inv, total_real, unreal, mtm, sb, sa, ub, ua,
         signal_suffix());
    write_tick("FILL", side, last_qty, last_px, realized_delta);

    // Post-fill adverse-selection probe.
    let is_buy = side == "BUY";
    let fill_px = last_px;
    let qty_probe = last_qty;
    let probe_side: &'static str = if is_buy { "BUY" } else { "SELL" };
    let same_side_now = if is_buy {
        STATE.spot_bid_x100.load(Ordering::Relaxed) as f64 / 100.0
    } else {
        STATE.spot_ask_x100.load(Ordering::Relaxed) as f64 / 100.0
    };
    let mid_now = spot_mid();
    let signed_move_0 = if is_buy { same_side_now - fill_px } else { fill_px - same_side_now };
    let mid_move_0 = if is_buy { mid_now - fill_px } else { fill_px - mid_now };
    log!("[ADV t=0s ] {} fill={:.2} same_side={:.2} mid={:.2} move={:+.2} mid_move={:+.2}",
         probe_side, fill_px, same_side_now, mid_now, signed_move_0, mid_move_0);
    write_tick("ADV0", probe_side, qty_probe, same_side_now, signed_move_0);

    tokio::spawn(async move {
        for (delay_ms, label, evt) in &[
            (1_000u64, "1s", "ADV1"),
            (5_000u64, "5s", "ADV5"),
        ] {
            tokio::time::sleep(Duration::from_millis(*delay_ms)).await;
            let same_side = if is_buy {
                STATE.spot_bid_x100.load(Ordering::Relaxed) as f64 / 100.0
            } else {
                STATE.spot_ask_x100.load(Ordering::Relaxed) as f64 / 100.0
            };
            let mid = spot_mid();
            let signed_move = if is_buy { same_side - fill_px } else { fill_px - same_side };
            let mid_move = if is_buy { mid - fill_px } else { fill_px - mid };
            log!("[ADV t={}] {} fill={:.2} same_side={:.2} mid={:.2} move={:+.2} mid_move={:+.2}",
                 label, probe_side, fill_px, same_side, mid, signed_move, mid_move);
            write_tick(evt, probe_side, qty_probe, same_side, signed_move);
        }
    });

    // Single-side mode: flip active side on full FILL, or on a partial whose
    // remaining leaves_qty is below MIN_HEDGE_QTY (too small to bother
    // completing — cancel the dust remainder and flip).
    if SINGLE_SIDE_MODE {
        let is_partial = status == sbe::ORDSTATUS_PARTIALLY_FILLED;
        let leaves = sbe::decimal_to_f64(er.leaves_qty, er.qty_exponent);
        let dust_remainder = is_partial && leaves > 0.0 && leaves < MIN_HEDGE_QTY;

        if status == sbe::ORDSTATUS_FILLED || dust_remainder {
            let new_active: u64 = if side == "BUY" { 1 } else { 0 };
            let prev = STATE.active_side.swap(new_active, Ordering::Relaxed);
            if prev != new_active {
                STATE.bid_cum_filled_e6.store(0, Ordering::Relaxed);
                STATE.ask_cum_filled_e6.store(0, Ordering::Relaxed);
                if dust_remainder {
                    log!("[FLIP] {} partial leaves={:.6} < {:.6} → {}",
                         side, leaves, MIN_HEDGE_QTY,
                         if new_active == 0 { "BUY" } else { "SELL" });
                } else {
                    log!("[FLIP] {} → {}", side,
                         if new_active == 0 { "BUY" } else { "SELL" });
                }
                wake();
            }

            // Dust-remainder: cancel the resting partial so it stops working.
            // Order slot clears on the inbound CANCELED ER.
            if dust_remainder && order_id != 0 {
                if let Some(oe) = ORDER_ENTRY_HANDLE.get().cloned() {
                    let oid = order_id;
                    tokio::spawn(async move { let _ = oe.cancel(oid).await; });
                }
            }
        }
    }

    // Dispatch Lighter hedge — opposite direction, accumulate sub-min dust.
    let fill_qty_e6 = (last_qty * 1e6) as i64;
    let (hedge_side, hedge_qty) = {
        let (my_atom, opp_atom, hedge_side) = if side == "BUY" {
            (&STATE.pending_hedge_buy_e6, &STATE.pending_hedge_sell_e6, "SELL")
        } else {
            (&STATE.pending_hedge_sell_e6, &STATE.pending_hedge_buy_e6, "BUY")
        };
        my_atom.fetch_add(fill_qty_e6, Ordering::Relaxed);

        let mine = my_atom.load(Ordering::Relaxed);
        let opp = opp_atom.load(Ordering::Relaxed);
        let net = mine.min(opp);
        if net > 0 {
            my_atom.fetch_sub(net, Ordering::Relaxed);
            opp_atom.fetch_sub(net, Ordering::Relaxed);
        }

        let pending = my_atom.load(Ordering::Relaxed) as f64 / 1e6;
        if pending >= MIN_HEDGE_QTY {
            my_atom.store(0, Ordering::Relaxed);
            (hedge_side, pending)
        } else {
            log!("[HEDGE PEND] {} accumulated {:.6} (< {:.6})",
                 hedge_side, pending, MIN_HEDGE_QTY);
            ("", 0.0)
        }
    };

    if hedge_qty > 0.0 && !HEDGING_ENABLED {
        log!("[HEDGE OFF] would have hedged {} {:.6} @ ref {:.2} (HEDGING_ENABLED=false)",
             hedge_side, hedge_qty, last_px);
    }

    if hedge_qty > 0.0 && HEDGING_ENABLED {
        let h = hedger.clone();
        let ref_px = last_px;
        let hedge_side_owned = hedge_side.to_string();
        tokio::spawn(async move {
            let result = h.hedge(&hedge_side_owned, hedge_qty, ref_px).await;
            let failed = !result.ok || result.qty <= 0.0 || result.px <= 0.0;
            if !failed {
                apply_lighter_fill(&hedge_side_owned, &result);
                return;
            }
            // First failure: retry once, immediately.
            log!("[HEDGE FAIL] {} err={:?} — retrying immediately {:.6}",
                 hedge_side_owned, result.err, hedge_qty);
            let retry = h.hedge(&hedge_side_owned, hedge_qty, ref_px).await;
            let retry_failed = !retry.ok || retry.qty <= 0.0 || retry.px <= 0.0;
            if !retry_failed {
                apply_lighter_fill(&hedge_side_owned, &retry);
                return;
            }
            // Second failure: cannot hedge — cancel all Binance orders, abort.
            log!("[HEDGE FATAL] {} retry also failed err={:?} — cancelling Binance orders and exiting",
                 hedge_side_owned, retry.err);
            if let Some(oe) = ORDER_ENTRY_HANDLE.get().cloned() {
                let _ = oe.cancel_all().await;
            }
            log!("[HEDGE FATAL] cancel_all sent — exiting now");
            std::process::exit(1);
        });
    }
}

/// ExecutionReportAck handler — used when OE runs in OnlyAcks/Mini mode.
/// Carries no fill price/qty, so it only maintains order-slot state and logs.
fn handle_exec_report_ack(ack: &sbe::ExecutionReportAck) {
    let cid = String::from_utf8_lossy(&ack.cl_ord_id).into_owned();
    let order_id = ack.order_id;
    let side = cid_side_get(&cid).unwrap_or("");

    let is_new = ack.exec_type == sbe::EXECTYPE_NEW || ack.ord_status == sbe::ORDSTATUS_NEW;
    let is_terminal = ack.ord_status == sbe::ORDSTATUS_FILLED
        || matches!(ack.ord_status,
            sbe::ORDSTATUS_CANCELED | sbe::ORDSTATUS_REJECTED | sbe::ORDSTATUS_EXPIRED);

    if is_new && order_id != 0 {
        match side {
            "BUY"  => { STATE.bid_id.store(order_id, Ordering::Relaxed); }
            "SELL" => { STATE.ask_id.store(order_id, Ordering::Relaxed); }
            _ => {}
        }
    }
    if is_terminal && order_id != 0 {
        if STATE.bid_id.load(Ordering::Relaxed) == order_id {
            STATE.bid_id.store(0, Ordering::Relaxed);
            STATE.bid_px_x100.store(0, Ordering::Relaxed);
        }
        if STATE.ask_id.load(Ordering::Relaxed) == order_id {
            STATE.ask_id.store(0, Ordering::Relaxed);
            STATE.ask_px_x100.store(0, Ordering::Relaxed);
        }
        cid_side_remove(&cid);
    }
    if ack.exec_type == sbe::EXECTYPE_REJECTED {
        log!("[REJECT] cid={} order_id={} err={}", cid, order_id, ack.error_code);
    }
}

/// Apply a successful Lighter taker fill to Lighter-side PnL/inventory.
/// Failure handling (retry + abort) is done by the dispatch task before this
/// is called, so a non-ok result here should not occur — guarded defensively.
fn apply_lighter_fill(side: &str, r: &HedgeResult) {
    if !r.ok || r.qty <= 0.0 || r.px <= 0.0 {
        log!("[HEDGE FAIL] {} err={:?} (unexpected: task should have handled)", side, r.err);
        return;
    }
    let inv = STATE.lighter_inv_e6.load(Ordering::Relaxed) as f64 / 1e6;
    let avg = STATE.lighter_avg_cost_x100.load(Ordering::Relaxed) as f64 / 100.0;
    let mut new_inv = inv;
    let mut new_avg = avg;
    let mut realized_delta = 0.0;
    let cash_delta;

    if side == "BUY" {
        if inv < 0.0 {
            let closed = r.qty.min(-inv);
            realized_delta += (avg - r.px) * closed;
            let remaining = r.qty - closed;
            new_inv = inv + r.qty;
            if remaining > 0.0 { new_avg = r.px; }
        } else {
            new_inv = inv + r.qty;
            if new_inv > 0.0 { new_avg = (avg * inv + r.px * r.qty) / new_inv; }
        }
        cash_delta = -r.qty * r.px;
    } else {
        if inv > 0.0 {
            let closed = r.qty.min(inv);
            realized_delta += (r.px - avg) * closed;
            let remaining = r.qty - closed;
            new_inv = inv - r.qty;
            if remaining > 0.0 { new_avg = r.px; }
        } else {
            let new_short = -inv + r.qty;
            new_inv = inv - r.qty;
            if new_short > 0.0 { new_avg = (avg * (-inv) + r.px * r.qty) / new_short; }
        }
        cash_delta = r.qty * r.px;
    }
    if new_inv.abs() < 1e-9 { new_avg = 0.0; }

    STATE.lighter_inv_e6.store((new_inv * 1e6).round() as i64, Ordering::Relaxed);
    STATE.lighter_avg_cost_x100.store((new_avg * 100.0).round() as i64, Ordering::Relaxed);
    STATE.lighter_realized_x1m.fetch_add((realized_delta * 1e6).round() as i64, Ordering::Relaxed);
    STATE.lighter_cash_x1m.fetch_add((cash_delta * 1e6).round() as i64, Ordering::Relaxed);
    STATE.lighter_hedges.fetch_add(1, Ordering::Relaxed);

    let spot_inv = STATE.inventory_e6.load(Ordering::Relaxed) as f64 / 1e6;
    let net = spot_inv + new_inv;
    log!("[HEDGE OK] {} {:.6} @ {:.2} | l_inv={:+.6} a_inv={:+.6} net={:+.6}",
         side, r.qty, r.px, new_inv, spot_inv, net);
    write_tick("HEDGE", if side == "BUY" { "BUY" } else { "SELL" },
               r.qty, r.px, realized_delta);
}

// ─── FEEDS (FIX-SBE sessions) ───────────────────────────────────────────────

fn pin_to_core(idx: usize) {
    if let Some(ids) = core_affinity::get_core_ids() {
        if let Some(id) = ids.get(idx) { core_affinity::set_for_current(*id); }
    }
}

/// Order Entry session: connect, logon, run the read loop forwarding
/// ExecutionReport into the fill handlers. Reconnects on drop.
async fn run_order_entry(
    oe: Arc<OrderEntry>,
    hedger: Arc<Hedger>,
    key: Arc<ed25519_dalek::SigningKey>,
    api_key: String,
    sender_comp_id: String,
) {
    loop {
        match FixSession::connect(
            FIX_OE_HOST, FIX_OE_PORT, SessionKind::OrderEntry,
            sender_comp_id.clone(), "SPOT".to_string(),
        ).await {
            Ok((session, mut read)) => {
                // Order Entry logon: UNORDERED handling for best perf,
                // ResponseMode=Everything so we receive full ExecutionReports
                // (with fill px/qty) rather than Mini acks.
                match session.logon(
                    &mut read, &key, &api_key, HEARTBEAT_SECS, RECV_WINDOW_US,
                    sbe::MSGHANDLING_UNORDERED,
                    Some(sbe::RESPMODE_EVERYTHING),
                    Some(sbe::ERTYPE_FULL),
                ).await {
                    Ok(()) => {
                        log!("[OE] logon ok (hb={}s)", session.heartbeat_secs());
                        oe.set_session(session.clone());
                        let hb = fix::spawn_heartbeat(session.clone());

                        // On (re)connect we can't map any pre-existing resting
                        // orders, so clear local slots.
                        STATE.bid_id.store(0, Ordering::Relaxed);
                        STATE.ask_id.store(0, Ordering::Relaxed);
                        STATE.bid_px_x100.store(0, Ordering::Relaxed);
                        STATE.ask_px_x100.store(0, Ordering::Relaxed);

                        let h = hedger.clone();
                        let res = fix::run_read_loop(session.clone(), read, move |msg| {
                            match msg {
                                InboundMsg::ExecutionReport(er) => {
                                    handle_exec_report(&er, &h);
                                    wake();
                                }
                                InboundMsg::ExecutionReportAck(ack) => {
                                    handle_exec_report_ack(&ack);
                                    wake();
                                }
                                InboundMsg::OrderCancelReject(r) => {
                                    log!("[CANCEL REJECT] order_id={} err={} sym={} | {}",
                                         r.order_id, r.error_code,
                                         String::from_utf8_lossy(&r.symbol), signal_suffix());
                                }
                                InboundMsg::Reject(r) => {
                                    log!("[OE REJECT] refSeq={} refTag={} reason={} err={} text={}",
                                         r.ref_seq_num, r.ref_tag_id,
                                         r.session_reject_reason,
                                         r.error_code,
                                         String::from_utf8_lossy(&r.text));
                                }
                                InboundMsg::Logout(t) => {
                                    log!("[OE] logout: {}", String::from_utf8_lossy(&t));
                                }
                                _ => {}
                            }
                        }).await;
                        hb.abort();
                        oe.set_session_none();
                        if let Err(e) = res { log!("[OE] read loop ended: {}", e); }
                    }
                    Err(e) => log!("[OE] logon failed: {}", e),
                }
            }
            Err(e) => log!("[OE] connect err: {}", e),
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
}

/// Market Data session for BTCU: book ticker (→ FV, basis) + trades (→ k).
async fn run_md_spot(
    core: Option<usize>,
    key: Arc<ed25519_dalek::SigningKey>,
    api_key: String,
    sender_comp_id: String,
) {
    if let Some(c) = core { pin_to_core(c); }
    loop {
        match FixSession::connect(
            FIX_MD_HOST, FIX_MD_PORT, SessionKind::MarketData,
            sender_comp_id.clone(), "SPOT".to_string(),
        ).await {
            Ok((session, mut read)) => {
                match session.logon(
                    &mut read, &key, &api_key, HEARTBEAT_SECS, RECV_WINDOW_US,
                    sbe::MSGHANDLING_UNORDERED, None, None,
                ).await {
                    Ok(()) => {
                        log!("[MD-SPOT] logon ok");
                        let hb = fix::spawn_heartbeat(session.clone());

                        // Subscribe: book ticker + trades for BTCU.
                        let sym = SYMBOL_SPOT.as_bytes().to_vec();
                        let sym2 = sym.clone();
                        if let Err(e) = session.send_with_seq(move |seq, ts| {
                            sbe::encode_market_data_request(
                                seq, ts, true, Some(1), Some(true),
                                &sym, &[sbe::MDENTRY_BID, sbe::MDENTRY_OFFER],
                                MDREQ_SPOT_BOOK.as_bytes(),
                            )
                        }).await { log!("[MD-SPOT] book sub err: {}", e); }

                        if let Err(e) = session.send_with_seq(move |seq, ts| {
                            sbe::encode_market_data_request(
                                seq, ts, true, None, None,
                                &sym2, &[sbe::MDENTRY_TRADE],
                                MDREQ_SPOT_TRADE.as_bytes(),
                            )
                        }).await { log!("[MD-SPOT] trade sub err: {}", e); }

                        let res = fix::run_read_loop(session.clone(), read, move |msg| {
                            match msg {
                                InboundMsg::BookTicker(bt) => {
                                    // BTCU IncrementalBookTicker frames are
                                    // single-sided (one frame = bid OR ask
                                    // update). Merge into STATE: only overwrite
                                    // the side that's present in this frame.
                                    let pb = STATE.spot_bid_x100.load(Ordering::Relaxed);
                                    let pa = STATE.spot_ask_x100.load(Ordering::Relaxed);
                                    let nb = if bt.bid_px > 0.0 { (bt.bid_px * 100.0) as i64 } else { pb };
                                    let na = if bt.ask_px > 0.0 { (bt.ask_px * 100.0) as i64 } else { pa };
                                    if nb == pb && na == pa { return; }
                                    STATE.spot_bid_x100.store(nb, Ordering::Relaxed);
                                    STATE.spot_ask_x100.store(na, Ordering::Relaxed);
                                    if bt.bid_px > 0.0 {
                                        STATE.spot_bid_sz_e6.store((bt.bid_qty * 1e6) as u64, Ordering::Relaxed);
                                    }
                                    if bt.ask_px > 0.0 {
                                        STATE.spot_ask_sz_e6.store((bt.ask_qty * 1e6) as u64, Ordering::Relaxed);
                                    }
                                    update_basis();
                                    write_tick("", "", 0.0, 0.0, 0.0);
                                    wake();
                                }
                                InboundMsg::IncrementalTrade(tr) => {
                                    if tr.px > 0.0 {
                                        // AggressorSide == SELL means the buyer
                                        // is the maker (taker sold). Map to the
                                        // old is_buyer_maker convention.
                                        let is_buyer_maker = tr.aggressor_side == sbe::SIDE_SELL;
                                        update_k(tr.px, is_buyer_maker, spot_mid());
                                    }
                                }
                                InboundMsg::MdRequestReject(r) => {
                                    log!("[MD-SPOT] sub reject: id={} err={} text={}",
                                         String::from_utf8_lossy(&r.md_req_id),
                                         r.error_code,
                                         String::from_utf8_lossy(&r.text));
                                }
                                InboundMsg::Logout(t) => {
                                    log!("[MD-SPOT] logout: {}", String::from_utf8_lossy(&t));
                                }
                                _ => {}
                            }
                        }).await;
                        hb.abort();
                        if let Err(e) = res { log!("[MD-SPOT] read loop ended: {}", e); }
                    }
                    Err(e) => log!("[MD-SPOT] logon failed: {}", e),
                }
            }
            Err(e) => log!("[MD-SPOT] connect err: {}", e),
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

/// Market Data session for BTCUSDT: book ticker → σ sampling source + jump
/// detector pause. This is the role the Binance perp feed played in the Aster
/// version. Inline pause-cancel on the rising edge mirrors the old feed_bperp.
async fn run_md_usdt(
    core: Option<usize>,
    oe: Arc<OrderEntry>,
    key: Arc<ed25519_dalek::SigningKey>,
    api_key: String,
    sender_comp_id: String,
) {
    if let Some(c) = core { pin_to_core(c); }
    loop {
        match FixSession::connect(
            FIX_MD_HOST, FIX_MD_PORT, SessionKind::MarketData,
            sender_comp_id.clone(), "SPOT".to_string(),
        ).await {
            Ok((session, mut read)) => {
                match session.logon(
                    &mut read, &key, &api_key, HEARTBEAT_SECS, RECV_WINDOW_US,
                    sbe::MSGHANDLING_UNORDERED, None, None,
                ).await {
                    Ok(()) => {
                        log!("[MD-USDT] logon ok");
                        let hb = fix::spawn_heartbeat(session.clone());

                        let sym = SYMBOL_USDT.as_bytes().to_vec();
                        let sym_tr = sym.clone();
                        if let Err(e) = session.send_with_seq(move |seq, ts| {
                            sbe::encode_market_data_request(
                                seq, ts, true, Some(1), Some(true),
                                &sym, &[sbe::MDENTRY_BID, sbe::MDENTRY_OFFER],
                                MDREQ_USDT_BOOK.as_bytes(),
                            )
                        }).await { log!("[MD-USDT] book sub err: {}", e); }

                        if let Err(e) = session.send_with_seq(move |seq, ts| {
                            sbe::encode_market_data_request(
                                seq, ts, true, None, None,
                                &sym_tr, &[sbe::MDENTRY_TRADE],
                                MDREQ_USDT_TRADE.as_bytes(),
                            )
                        }).await { log!("[MD-USDT] trade sub err: {}", e); }

                        let oe_inner = oe.clone();
                        let res = fix::run_read_loop(session.clone(), read, move |msg| {
                            match msg {
                                InboundMsg::BookTicker(bt) => {
                                    // BBO sizes drive the imbalance trigger —
                                    // store BEFORE the price early-out, since
                                    // size changes with price unchanged still
                                    // matter for the trigger.
                                    if bt.bid_qty > 0.0 {
                                        STATE.usdt_bid_sz_e6.store(
                                            (bt.bid_qty * 1e6) as i64, Ordering::Relaxed);
                                    }
                                    if bt.ask_qty > 0.0 {
                                        STATE.usdt_ask_sz_e6.store(
                                            (bt.ask_qty * 1e6) as i64, Ordering::Relaxed);
                                    }
                                    let pb = STATE.usdt_bid_x100.load(Ordering::Relaxed);
                                    let pa = STATE.usdt_ask_x100.load(Ordering::Relaxed);
                                    let nb = if bt.bid_px > 0.0 { (bt.bid_px * 100.0) as i64 } else { pb };
                                    let na = if bt.ask_px > 0.0 { (bt.ask_px * 100.0) as i64 } else { pa };
                                    if nb == pb && na == pa {
                                        // Price unchanged but size may have moved
                                        // the imbalance — still run the pause check.
                                        pause_edge_update(&oe_inner, "bbo");
                                        return;
                                    }
                                    STATE.usdt_bid_x100.store(nb, Ordering::Relaxed);
                                    STATE.usdt_ask_x100.store(na, Ordering::Relaxed);
                                    update_usdt_ewma();
                                    update_basis();
                                    write_tick("", "", 0.0, 0.0, 0.0);
                                    pause_edge_update(&oe_inner, "book");
                                    wake();
                                }
                                InboundMsg::IncrementalTradeBatch(trades) => {
                                    // Fold every ETHUSDT trade into the signed
                                    // rolling-qty accumulator, then run the same
                                    // rising-edge pause check — the impulse
                                    // predictor (β·√|Q|) is one of its triggers.
                                    for tr in &trades {
                                        update_signed_qty(tr.qty, tr.aggressor_side);
                                    }
                                    pause_edge_update(&oe_inner, "trade");
                                    wake();
                                }
                                InboundMsg::MdRequestReject(r) => {
                                    log!("[MD-USDT] sub reject: err={} text={}",
                                         r.error_code, String::from_utf8_lossy(&r.text));
                                }
                                InboundMsg::Logout(t) => {
                                    log!("[MD-USDT] logout: {}", String::from_utf8_lossy(&t));
                                }
                                _ => {}
                            }
                        }).await;
                        hb.abort();
                        if let Err(e) = res { log!("[MD-USDT] read loop ended: {}", e); }
                    }
                    Err(e) => log!("[MD-USDT] logon failed: {}", e),
                }
            }
            Err(e) => log!("[MD-USDT] connect err: {}", e),
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

// ─── TICK / DRIVER ──────────────────────────────────────────────────────────

async fn tick(router: Arc<Router>) {
    if STATE.halted.load(Ordering::Relaxed) {
        if now_ms() >= STATE.rate_limit_until_ms.load(Ordering::Relaxed) {
            STATE.halted.store(false, Ordering::Relaxed);
            log!("[RATE LIMIT] resumed");
        } else {
            return;
        }
    }

    // Stuck-order watchdog: if a side has bid_id/ask_id set but no ER activity
    // for STUCK_ORDER_TIMEOUT_MS, force-clear it. Defends against lost or
    // mis-matched ExecutionReports leaving state permanently wedged.
    let now_m = now_ms();
    let bid_id = STATE.bid_id.load(Ordering::Relaxed);
    let ask_id = STATE.ask_id.load(Ordering::Relaxed);
    let last_buy = STATE.last_buy_action_ms.load(Ordering::Relaxed);
    let last_sell = STATE.last_sell_action_ms.load(Ordering::Relaxed);
    if bid_id != 0 && last_buy != 0 && now_m.saturating_sub(last_buy) > STUCK_ORDER_TIMEOUT_MS {
        log!("[WATCHDOG] BUY slot stuck (id={}, idle={}ms) — clearing", bid_id, now_m - last_buy);
        let oe = router.oe.clone();
        tokio::spawn(async move { let _ = oe.cancel(bid_id).await; });
        STATE.bid_id.store(0, Ordering::Relaxed);
        STATE.bid_px_x100.store(0, Ordering::Relaxed);
        STATE.last_buy_action_ms.store(now_m, Ordering::Relaxed);
        arm_post_cancel_cooldown("BUY");
    }
    // Stale-inflight eviction: a cid whose ExecutionReport never arrived would
    // otherwise wedge inflight_count for that side forever (reconcile keeps
    // returning early). Drop entries older than the stuck-order timeout and
    // fire a best-effort cancel-by-cid for each.
    for (cid, side) in inflight_evict_stale(STUCK_ORDER_TIMEOUT_MS) {
        log!("[WATCHDOG] stale inflight cid={} side={} — evicting", cid, side);
        arm_post_cancel_cooldown(side);
        let oe = router.oe.clone();
        tokio::spawn(async move { let _ = oe.cancel_by_cid(&cid).await; });
    }
    if ask_id != 0 && last_sell != 0 && now_m.saturating_sub(last_sell) > STUCK_ORDER_TIMEOUT_MS {
        log!("[WATCHDOG] SELL slot stuck (id={}, idle={}ms) — clearing", ask_id, now_m - last_sell);
        let oe = router.oe.clone();
        tokio::spawn(async move { let _ = oe.cancel(ask_id).await; });
        STATE.ask_id.store(0, Ordering::Relaxed);
        STATE.ask_px_x100.store(0, Ordering::Relaxed);
        STATE.last_sell_action_ms.store(now_m, Ordering::Relaxed);
        arm_post_cancel_cooldown("SELL");
    }

    let (bid_px, ask_px) = compute_quotes();
    if bid_px <= 0.0 || ask_px <= 0.0 {
        let last = NOQUOTE_LOG_MS.load(Ordering::Relaxed);
        let nm = now_ms();
        if nm.saturating_sub(last) > 60_000 {
            NOQUOTE_LOG_MS.store(nm, Ordering::Relaxed);
            log!("[TICK] no quote: bid={:.2} ask={:.2} fv={:.2} inv={:+.6} active={}",
                 bid_px, ask_px, fair_value(),
                 STATE.inventory_e6.load(Ordering::Relaxed) as f64 / 1e6,
                 STATE.active_side.load(Ordering::Relaxed));
        }
        return;
    }
    let qty = round_qty(ORDER_QTY);
    if qty <= 0.0 { return; }

    if SINGLE_SIDE_MODE {
        let active = STATE.active_side.load(Ordering::Relaxed);
        if active == 0 {
            let stale_ask = STATE.ask_id.load(Ordering::Relaxed);
            if stale_ask != 0 {
                let oe = router.oe.clone();
                arm_post_cancel_cooldown("SELL");
                tokio::spawn(async move { let _ = oe.cancel(stale_ask).await; });
            }
            let filled = STATE.bid_cum_filled_e6.load(Ordering::Relaxed) as f64 / 1e6;
            let remaining = round_qty((ORDER_QTY - filled).max(0.0));
            if remaining <= 0.0 { return; }
            router.reconcile("BUY", bid_px, remaining).await;
        } else {
            let stale_bid = STATE.bid_id.load(Ordering::Relaxed);
            if stale_bid != 0 {
                let oe = router.oe.clone();
                arm_post_cancel_cooldown("BUY");
                tokio::spawn(async move { let _ = oe.cancel(stale_bid).await; });
            }
            let filled = STATE.ask_cum_filled_e6.load(Ordering::Relaxed) as f64 / 1e6;
            let remaining = round_qty((ORDER_QTY - filled).max(0.0));
            if remaining <= 0.0 { return; }
            router.reconcile("SELL", ask_px, remaining).await;
        }
    } else {
        let r1 = router.clone();
        let r2 = router.clone();
        tokio::join!(
            r1.reconcile("BUY", bid_px, qty),
            r2.reconcile("SELL", ask_px, qty),
        );
    }
}

async fn sigma_sampler() {
    let interval = Duration::from_millis(SIGMA_SAMPLE_MS);
    let mut iv = tokio::time::interval(interval);
    iv.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        iv.tick().await;
        let sb = STATE.spot_bid_x100.load(Ordering::Relaxed);
        let sa = STATE.spot_ask_x100.load(Ordering::Relaxed);
        if sb > 0 && sa > 0 {
            sample_sigma((sb + sa) as f64 / 200.0);
        }
    }
}

async fn tick_driver(router: Arc<Router>) {
    let notify = WAKE.get().unwrap().clone();
    let mut iv = tokio::time::interval(Duration::from_millis(500));
    iv.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            _ = notify.notified() => {}
            _ = iv.tick() => {}
        }
        tick(router.clone()).await;
    }
}

async fn stats_loop() {
    let mut iv = tokio::time::interval(Duration::from_secs(15));
    loop {
        iv.tick().await;
        let inv = STATE.inventory_e6.load(Ordering::Relaxed) as f64 / 1e6;
        let real = STATE.realized_pnl_x1m.load(Ordering::Relaxed) as f64 / 1e6;
        let cash = STATE.cash_x1m.load(Ordering::Relaxed) as f64 / 1e6;
        let sm = spot_mid();
        let mtm = cash + inv * sm;
        let fv = fair_value();
        let basis = STATE.basis_x100.load(Ordering::Relaxed) as f64 / 100.0;
        let sigma_bp = SIGMA2.lock().sqrt() * 1e4;
        let k = *K_HAT.lock();
        let usdt_bid = STATE.usdt_bid_x100.load(Ordering::Relaxed) as f64 / 100.0;
        let usdt_ask = STATE.usdt_ask_x100.load(Ordering::Relaxed) as f64 / 100.0;
        let spot_bid_ = STATE.spot_bid_x100.load(Ordering::Relaxed) as f64 / 100.0;
        let spot_ask_ = STATE.spot_ask_x100.load(Ordering::Relaxed) as f64 / 100.0;
        let bid_id = STATE.bid_id.load(Ordering::Relaxed);
        let ask_id = STATE.ask_id.load(Ordering::Relaxed);
        let bid_px = STATE.bid_px_x100.load(Ordering::Relaxed) as f64 / 100.0;
        let ask_px = STATE.ask_px_x100.load(Ordering::Relaxed) as f64 / 100.0;
        let paused = pause_edge_check();
        let usdt_ewma = STATE.usdt_mid_ewma_x100.load(Ordering::Relaxed) as f64 / 100.0;
        let usdt_mid = if usdt_bid > 0.0 && usdt_ask > 0.0 { (usdt_bid + usdt_ask) / 2.0 } else { 0.0 };
        let usdt_dev = if usdt_ewma > 0.0 { usdt_mid - usdt_ewma } else { 0.0 };
        let l_inv = STATE.lighter_inv_e6.load(Ordering::Relaxed) as f64 / 1e6;
        let l_real = STATE.lighter_realized_x1m.load(Ordering::Relaxed) as f64 / 1e6;
        let l_cash = STATE.lighter_cash_x1m.load(Ordering::Relaxed) as f64 / 1e6;
        let l_mtm = l_cash + l_inv * sm;
        let net_pos = inv + l_inv;
        let combined_mtm = mtm + l_mtm;
        let combined_real = real + l_real;
        let pend_b = STATE.pending_hedge_buy_e6.load(Ordering::Relaxed) as f64 / 1e6;
        let pend_s = STATE.pending_hedge_sell_e6.load(Ordering::Relaxed) as f64 / 1e6;
        let signed_q = signed_qty_now();
        let impulse = impulse_signal();
        log!(
            "[STATS] a_inv={:+.6} l_inv={:+.6} net={:+.6} a_real={:+.4} l_real={:+.4} real={:+.4} mtm={:+.4} fills={} hedges={} pend_b={:.6} pend_s={:.6} fv={:.2} basis_ewma={:+.3} usdt_dev={:+.2} Q={:+.4} impulse={:+.3} dQ/dt={:+.1} pdev={:+.2} bboImb={:+.1} paused={} σ={:.2}bp/√s k={:.3} | usdt={:.2}/{:.2} spot={:.2}/{:.2} | quotes: BUY={}@{:.2} SELL={}@{:.2}",
            inv, l_inv, net_pos, real, l_real, combined_real, combined_mtm,
            STATE.fills_count.load(Ordering::Relaxed),
            STATE.lighter_hedges.load(Ordering::Relaxed),
            pend_b, pend_s,
            fv, basis, usdt_dev, signed_q, impulse, qrate_now(),
            perp_dev(), bbo_imbalance_signed(), paused, sigma_bp, k,
            usdt_bid, usdt_ask, spot_bid_, spot_ask_,
            bid_id, bid_px, ask_id, ask_px
        );
    }
}

// ─── ETHUSDT PERPETUAL FEED (fstream WS, JSON) ──────────────────────────────
// Futures has no SBE/FIX market data — WS JSON only. bookTicker drives perp
// dev; depth@0ms drives book-pressure imbalance + its derivative. All three
// feed is_paused() as extra triggers. Self-contained: reconnects on drop.

/// Fold a perp bookTicker update: store BBO, update perp mid EWMA.
fn perp_update_book(bid: f64, ask: f64) {
    if bid <= 0.0 || ask <= 0.0 { return; }
    let b = (bid * 100.0) as i64;
    let a = (ask * 100.0) as i64;
    STATE.perp_bid_x100.store(b, Ordering::Relaxed);
    STATE.perp_ask_x100.store(a, Ordering::Relaxed);
    let mid_x100 = (b + a) / 2;

    let now_u = now_us();
    let last = STATE.last_perp_ewma_ts_us.swap(now_u, Ordering::Relaxed);
    if last == 0 {
        STATE.perp_mid_ewma_x100.store(mid_x100, Ordering::Relaxed);
    } else {
        let dt = (now_u - last) as f64 / 1e6;
        let alpha = ewma_alpha(dt, PERP_EWMA_HALFLIFE);
        let prev = STATE.perp_mid_ewma_x100.load(Ordering::Relaxed) as f64;
        let nb = (1.0 - alpha) * prev + alpha * mid_x100 as f64;
        STATE.perp_mid_ewma_x100.store(nb as i64, Ordering::Relaxed);
    }
}

/// perp_mid - perp_mid_ewma (USD). 0 until the feed warms up.
fn perp_dev() -> f64 {
    let b = STATE.perp_bid_x100.load(Ordering::Relaxed);
    let a = STATE.perp_ask_x100.load(Ordering::Relaxed);
    let e = STATE.perp_mid_ewma_x100.load(Ordering::Relaxed);
    if b == 0 || a == 0 || e == 0 { return 0.0; }
    (b + a) as f64 / 200.0 - e as f64 / 100.0
}

/// Connect to the perp WS feed and process messages forever, reconnecting on
/// any drop. Public market data — no auth.
async fn run_perp_feed() {
    use futures_util::StreamExt;
    loop {
        log!("[PERP] connecting {}", PERP_WS_URL);
        let ws = match tokio_tungstenite::connect_async(PERP_WS_URL).await {
            Ok((s, _)) => s,
            Err(e) => {
                log!("[PERP] connect failed: {} — retry in 2s", e);
                tokio::time::sleep(Duration::from_secs(2)).await;
                continue;
            }
        };
        log!("[PERP] connected");
        let (_w, mut r) = ws.split();
        while let Some(msg) = r.next().await {
            let txt = match msg {
                Ok(tokio_tungstenite::tungstenite::Message::Text(t)) => t,
                Ok(tokio_tungstenite::tungstenite::Message::Ping(_)) => continue,
                Ok(tokio_tungstenite::tungstenite::Message::Close(_)) => break,
                Ok(_) => continue,
                Err(e) => { log!("[PERP] ws error: {}", e); break; }
            };
            let v: serde_json::Value = match serde_json::from_str(&txt) {
                Ok(v) => v,
                Err(_) => continue,
            };
            // Combined-stream wrapper: {"stream":..,"data":..}
            let (stream, data) = match (v.get("stream").and_then(|s| s.as_str()), v.get("data")) {
                (Some(s), Some(d)) => (s, d),
                _ => continue,
            };
            if stream.ends_with("@bookTicker") {
                // {"b":"bid","B":"bidQty","a":"ask","A":"askQty",..}
                let bid = data.get("b").and_then(|x| x.as_str()).and_then(|s| s.parse().ok());
                let ask = data.get("a").and_then(|x| x.as_str()).and_then(|s| s.parse().ok());
                if let (Some(b), Some(a)) = (bid, ask) {
                    perp_update_book(b, a);
                }
            }
        }
        log!("[PERP] disconnected — reconnecting");
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

// ─── MAIN ───────────────────────────────────────────────────────────────────

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> Result<()> {
    rustls::crypto::ring::default_provider()
        .install_default()
        .expect("install rustls crypto provider");
    init_clock();
    init_tick_logger();
    inflight_init();
    cid_side_init();

    // Env config:
    //   BINANCE_API_KEY          — Ed25519 API key string (sent as Username)
    //   BINANCE_ED25519_KEY_PATH — path to the PKCS#8 PEM private key
    //   BINANCE_FIX_SENDER       — base SenderCompID for FIX sessions
    let api_key = std::env::var("BINANCE_API_KEY")?;
    let key_path = std::env::var("BINANCE_ED25519_KEY_PATH")?;
    let sender_base = std::env::var("BINANCE_FIX_SENDER")
        .unwrap_or_else(|_| "MMBNY".to_string());

    let key = Arc::new(fix::load_ed25519_pem(&key_path)?);

    log!("[CFG] sym={} usdt={} qty={} max_inv={} tick={} lot={} edge={} step_ticks={}",
         SYMBOL_SPOT, SYMBOL_USDT, ORDER_QTY, MAX_INVENTORY,
         TICK_SIZE, LOT_SIZE, MIN_EDGE_USDC, STEP_TICKS);
    log!("[CFG] σ_HL={}s k_HL={}s basis_HL={}s requote_ticks={} step_ticks={}",
         SIGMA_HALFLIFE, K_HALFLIFE, BASIS_HALFLIFE, REQUOTE_TICKS, STEP_TICKS);
    log!("[CFG] price_exp={} qty_exp={} recv_window={}us hb={}s",
         PRICE_EXPONENT, QTY_EXPONENT, RECV_WINDOW_US, HEARTBEAT_SECS);

    WAKE.set(Arc::new(Notify::new())).ok();

    let hedger = Hedger::spawn().await?;
    log!("[STARTUP] lighter hedger spawned");

    let oe = Arc::new(OrderEntry::new());
    ORDER_ENTRY_HANDLE.set(oe.clone()).ok();
    let router = Arc::new(Router::new(oe.clone()));

    // Distinct SenderCompIDs per session — Binance requires uniqueness across
    // concurrent connections on the same account.
    let oe_sender   = format!("{}OE", sender_base);
    let mds_sender  = format!("{}MS", sender_base);
    let mdu_sender  = format!("{}MU", sender_base);

    tokio::join!(
        run_order_entry(oe.clone(), hedger.clone(), key.clone(),
                        api_key.clone(), oe_sender),
        run_md_spot(Some(1), key.clone(), api_key.clone(), mds_sender),
        run_md_usdt(Some(0), oe.clone(), key.clone(), api_key.clone(), mdu_sender),
        run_perp_feed(),
        sigma_sampler(),
        tick_driver(router.clone()),
        stats_loop(),
    );
    Ok(())
}