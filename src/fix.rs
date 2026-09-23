// FIX-SBE session layer for Binance Spot.
//
// One FixSession = one TCP+TLS connection running one of:
//   - Order Entry  (place / cancel orders, receive ExecutionReport[Ack])
//   - Market Data  (subscribe streams, receive incremental messages)
//
// Responsibilities:
//   - native TLS via tokio-rustls
//   - SOFH-framed read loop (length-prefixed; handles partial reads)
//   - outbound MsgSeqNum management (strictly +1 monotonic, starts at 1)
//   - Logon with Ed25519 RawData signature
//   - Heartbeat / TestRequest servicing
//   - hands decoded application messages to a callback
//
// Binance specifics baked in:
//   - FIX sessions are Ed25519-only
//   - logon signature payload = SHA-256-free: sign the raw concatenation
//     described below (see sign_logon)
//   - MessageHandling=UNORDERED for order entry (best perf)
//   - no Resend support; on seq gap the server disconnects — we just reconnect

use crate::sbe;
use anyhow::{anyhow, bail, Context, Result};
use ed25519_dalek::{Signer, SigningKey};
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::Mutex as AsyncMutex;
use tokio_rustls::{rustls, TlsConnector};
use tokio_rustls::client::TlsStream;

/// Which kind of session — determines the SenderCompID suffix convention and
/// whether order-entry-only logon fields are sent.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionKind {
    OrderEntry,
    MarketData,
}

/// Inbound application message, already decoded by the SBE codec.
#[derive(Debug, Clone)]
pub enum InboundMsg {
    ExecutionReport(sbe::ExecutionReport),
    ExecutionReportAck(sbe::ExecutionReportAck),
    OrderCancelReject(sbe::OrderCancelRejectMsg),
    BookTicker(sbe::BookTicker),
    IncrementalTrade(sbe::IncrementalTrade),
    /// All entries of a MarketDataIncrementalTrade frame. Used by the
    /// signed-rolling-qty impulse predictor, which must see every trade.
    IncrementalTradeBatch(Vec<sbe::IncrementalTrade>),
    MdRequestReject(sbe::MarketDataRequestReject),
    Reject(sbe::RejectMsg),
    Logout(Vec<u8>),
}

/// Microseconds since the Unix epoch — SBE sendingTime unit.
#[inline]
pub fn now_us() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_micros() as i64
}

/// Milliseconds since epoch — used for logon signature timestamps.
#[inline]
fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

/// Load an Ed25519 private key from a PKCS#8 PEM file (Binance's key format).
pub fn load_ed25519_pem(path: &str) -> Result<SigningKey> {
    let pem = std::fs::read_to_string(path)
        .with_context(|| format!("read ed25519 key {}", path))?;
    // ed25519-dalek's pkcs8 feature parses PEM directly.
    use ed25519_dalek::pkcs8::DecodePrivateKey;
    let key = SigningKey::from_pkcs8_pem(&pem)
        .map_err(|e| anyhow!("parse ed25519 pkcs8 pem: {}", e))?;
    Ok(key)
}

/// Compute the FIX SBE Logon RawData signature.
///
/// Binance's documented payload for the FIX logon signature is the
/// concatenation, joined by SOH (0x01), of these logon fields in this order:
///   MsgType(A), SenderCompID, TargetCompID, MsgSeqNum, SendingTime
/// signed with Ed25519, then base64-encoded.
///
/// SendingTime here is formatted as the FIX UTCTimestamp string
/// (YYYYMMDD-HH:MM:SS.ssssss) for the signature payload — this is what the
/// docs' worked example uses. The value we put in the SBE messageHeader is the
/// integer-microsecond form; the signed payload uses the string form.
pub fn sign_logon(
    key: &SigningKey,
    sender_comp_id: &str,
    target_comp_id: &str,
    msg_seq_num: u32,
    sending_time_str: &str,
) -> String {
    const SOH: u8 = 0x01;
    let mut payload = Vec::with_capacity(128);
    payload.extend_from_slice(b"A");
    payload.push(SOH);
    payload.extend_from_slice(sender_comp_id.as_bytes());
    payload.push(SOH);
    payload.extend_from_slice(target_comp_id.as_bytes());
    payload.push(SOH);
    payload.extend_from_slice(msg_seq_num.to_string().as_bytes());
    payload.push(SOH);
    payload.extend_from_slice(sending_time_str.as_bytes());

    let sig = key.sign(&payload);
    use base64::Engine;
    base64::engine::general_purpose::STANDARD.encode(sig.to_bytes())
}

/// Format a micros-since-epoch instant as FIX UTCTimestamp:
/// YYYYMMDD-HH:MM:SS.ssssss (UTC).
pub fn fix_utc_timestamp(us_since_epoch: i64) -> String {
    // Decompose without chrono. us → (days, time-of-day).
    let total_us = us_since_epoch as u64;
    let us = total_us % 1_000_000;
    let total_secs = total_us / 1_000_000;
    let secs_of_day = total_secs % 86_400;
    let days_since_epoch = total_secs / 86_400;

    let (h, m, s) = (
        secs_of_day / 3600,
        (secs_of_day % 3600) / 60,
        secs_of_day % 60,
    );

    // Civil date from days since 1970-01-01 (Howard Hinnant's algorithm).
    let z = days_since_epoch as i64 + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = (z - era * 146_097) as u64;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = if month <= 2 { y + 1 } else { y };

    format!(
        "{:04}{:02}{:02}-{:02}:{:02}:{:02}.{:06}",
        year, month, d, h, m, s, us
    )
}

/// Shared handle to a live FIX session. Cloneable; the read loop owns the
/// socket-read half via the internal Arc.
pub struct FixSession {
    pub kind: SessionKind,
    /// Outbound write half + next seq num, behind one lock so a frame and its
    /// seq number are assigned atomically (seq must be strictly monotonic).
    inner: Arc<AsyncMutex<WriteHalf>>,
    /// Last inbound seq num seen — informational; on a gap we just reconnect.
    last_in_seq: AtomicU32,
    /// Heartbeat interval negotiated at logon (seconds).
    heartbeat_secs: AtomicU64,
    pub sender_comp_id: String,
    pub target_comp_id: String,
}

struct WriteHalf {
    stream: WriteStream,
    next_seq: u32,
}

/// The write side of the TLS stream. We split logically by wrapping the whole
/// stream in the mutex and only reading from the dedicated read task via a
/// separate owned half (see `connect`).
type WriteStream = tokio::io::WriteHalf<TlsStream<TcpStream>>;
type ReadStream = tokio::io::ReadHalf<TlsStream<TcpStream>>;

impl FixSession {
    /// Connect, TLS-handshake, and return (session, read_half). The caller
    /// must then call `logon` and spawn `run_read_loop` with the read half.
    pub async fn connect(
        host: &str,
        port: u16,
        kind: SessionKind,
        sender_comp_id: String,
        target_comp_id: String,
    ) -> Result<(Arc<FixSession>, ReadStream)> {
        let tcp = TcpStream::connect((host, port))
            .await
            .with_context(|| format!("tcp connect {}:{}", host, port))?;
        tcp.set_nodelay(true).ok();

        // rustls client config with native roots.
        let mut roots = rustls::RootCertStore::empty();
        let certs = rustls_native_certs::load_native_certs();
        for cert in certs.certs {
            roots.add(cert).ok();
        }
        let config = rustls::ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth();
        let connector = TlsConnector::from(Arc::new(config));
        let server_name = rustls::pki_types::ServerName::try_from(host.to_string())
            .map_err(|_| anyhow!("invalid server name {}", host))?;
        let tls = connector
            .connect(server_name, tcp)
            .await
            .context("tls handshake")?;

        let (read_half, write_half) = tokio::io::split(tls);

        let session = Arc::new(FixSession {
            kind,
            inner: Arc::new(AsyncMutex::new(WriteHalf {
                stream: write_half,
                next_seq: 1,
            })),
            last_in_seq: AtomicU32::new(0),
            heartbeat_secs: AtomicU64::new(30),
            sender_comp_id,
            target_comp_id,
        });
        Ok((session, read_half))
    }

    /// Reserve the next outbound MsgSeqNum.
    async fn take_seq(&self) -> u32 {
        let mut w = self.inner.lock().await;
        let s = w.next_seq;
        w.next_seq += 1;
        s
    }

    /// Write a pre-built SBE frame. The frame must already carry its seq num;
    /// callers obtain that via `take_seq` then build, OR use `send_with_seq`.
    async fn write_frame(&self, frame: &[u8]) -> Result<()> {
        let mut w = self.inner.lock().await;
        w.stream.write_all(frame).await.context("write frame")?;
        w.stream.flush().await.context("flush frame")?;
        Ok(())
    }

    /// Atomically reserve a seq num, build a frame with `build(seq, ts_us)`,
    /// and write it. Guarantees seq numbers go out in strict order.
    pub async fn send_with_seq<F>(&self, build: F) -> Result<u32>
    where
        F: FnOnce(u32, i64) -> Vec<u8>,
    {
        let mut w = self.inner.lock().await;
        let seq = w.next_seq;
        w.next_seq += 1;
        let frame = build(seq, now_us());
        w.stream.write_all(&frame).await.context("write frame")?;
        w.stream.flush().await.context("flush frame")?;
        Ok(seq)
    }

    pub fn heartbeat_secs(&self) -> u64 {
        self.heartbeat_secs.load(Ordering::Relaxed)
    }

    /// Perform the Logon handshake. Sends Logon<A>, then blocks reading from
    /// `read` until LogonAck<A> arrives (or Reject/Logout/timeout).
    pub async fn logon(
        self: &Arc<Self>,
        read: &mut ReadStream,
        key: &SigningKey,
        api_key: &str,
        heartbeat_secs: u32,
        recv_window_us: u32,
        message_handling: u8,
        response_mode: Option<u8>,
        exec_report_type: Option<u8>,
    ) -> Result<()> {
        // Build the logon. Seq num for logon is always 1 on a fresh session
        // with ResetSeqNumFlag=Y.
        let seq = {
            let mut w = self.inner.lock().await;
            let s = w.next_seq;
            w.next_seq += 1;
            s
        };
        let ts_us = now_us();
        let ts_str = fix_utc_timestamp(ts_us);
        let raw_data = sign_logon(
            key,
            &self.sender_comp_id,
            &self.target_comp_id,
            seq,
            &ts_str,
        );

        let frame = sbe::encode_logon(
            seq,
            ts_us,
            heartbeat_secs,
            true, // ResetSeqNumFlag
            message_handling,
            response_mode,
            exec_report_type,
            recv_window_us,
            self.sender_comp_id.as_bytes(),
            self.target_comp_id.as_bytes(),
            raw_data.as_bytes(),
            api_key.as_bytes(),
        );
        self.write_frame(&frame).await.context("send logon")?;

        // Await LogonAck.
        let mut acc: Vec<u8> = Vec::with_capacity(4096);
        let mut tmp = [0u8; 4096];
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        loop {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                bail!("logon ack timeout");
            }
            let n = tokio::time::timeout(remaining, read.read(&mut tmp))
                .await
                .context("logon read timeout")??;
            if n == 0 {
                bail!("connection closed during logon");
            }
            acc.extend_from_slice(&tmp[..n]);

            while let Some(flen) = sbe::next_frame_len(&acc) {
                if flen < sbe::FRAME_PREFIX || acc.len() < flen {
                    break;
                }
                let frame = acc[..flen].to_vec();
                acc.drain(..flen);
                let hdr = match sbe::parse_frame_header(&frame) {
                    Some(Ok(h)) => h,
                    Some(Err(e)) => bail!("logon: bad frame header: {}", e),
                    None => break,
                };
                match hdr.template_id {
                    sbe::tid::LOGON_ACK => {
                        if let Some(ack) = sbe::decode_logon_ack(&hdr, &frame) {
                            self.heartbeat_secs
                                .store(ack.heart_bt_int as u64, Ordering::Relaxed);
                            self.last_in_seq.store(hdr.seq_num, Ordering::Relaxed);
                            if ack.schema_deprecated {
                                eprintln!(
                                    "[FIX] warning: SBE schema id/version deprecated"
                                );
                            }
                            return Ok(());
                        }
                        bail!("logon: failed to decode LogonAck");
                    }
                    sbe::tid::REJECT => {
                        let r = sbe::decode_reject(&hdr, &frame);
                        let hex_str: String = frame.iter().map(|b| format!("{:02x}", b)).collect();
                        eprintln!("[FIX DEBUG] reject tid={} seq={} raw_hex={}", hdr.template_id, hdr.seq_num, hex_str);
                        bail!("logon rejected: {:?}", r);
                    }
                    sbe::tid::LOGOUT => {
                        let t = sbe::decode_logout(&hdr, &frame);
                        let hex_str: String = frame.iter().map(|b| format!("{:02x}", b)).collect();
                        eprintln!("[FIX DEBUG] logout tid={} seq={} raw_hex={}", hdr.template_id, hdr.seq_num, hex_str);
                        bail!(
                            "logon: server sent Logout: {}",
                            t.as_deref()
                                .map(|b| String::from_utf8_lossy(b).into_owned())
                                .unwrap_or_default()
                        );
                    }
                    other => {
                        let hex_str: String = frame.iter().map(|b| format!("{:02x}", b)).collect();
                        eprintln!("[FIX DEBUG] unknown pre-logon tid={} seq={} raw_hex={}", other, hdr.seq_num, hex_str);
                    }
                }
            }
        }
    }

    /// Send a Heartbeat<0>, optionally echoing a TestReqID.
    pub async fn send_heartbeat(&self, test_req_id: &[u8]) -> Result<()> {
        self.send_with_seq(|seq, ts| sbe::encode_heartbeat(seq, ts, test_req_id))
            .await
            .map(|_| ())
    }

    /// Send a TestRequest<1>.
    pub async fn send_test_request(&self, test_req_id: &[u8]) -> Result<()> {
        self.send_with_seq(|seq, ts| sbe::encode_test_request(seq, ts, test_req_id))
            .await
            .map(|_| ())
    }

    /// Send a Logout<5>.
    pub async fn send_logout(&self, text: &[u8]) -> Result<()> {
        self.send_with_seq(|seq, ts| sbe::encode_logout(seq, ts, text))
            .await
            .map(|_| ())
    }

    pub fn last_inbound_seq(&self) -> u32 {
        self.last_in_seq.load(Ordering::Relaxed)
    }
}

/// Run the inbound read loop. Decodes frames, services session-level messages
/// (Heartbeat/TestRequest) inline, and forwards application messages to
/// `on_msg`. Returns when the connection closes or errors — the caller
/// reconnects.
pub async fn run_read_loop<F>(
    session: Arc<FixSession>,
    mut read: ReadStream,
    mut on_msg: F,
) -> Result<()>
where
    F: FnMut(InboundMsg),
{
    let mut acc: Vec<u8> = Vec::with_capacity(64 * 1024);
    let mut tmp = [0u8; 16 * 1024];

    loop {
        let n = read.read(&mut tmp).await.context("read loop")?;
        if n == 0 {
            return Err(anyhow!("connection closed"));
        }
        acc.extend_from_slice(&tmp[..n]);

        // Drain all complete frames currently buffered.
        loop {
            let flen = match sbe::next_frame_len(&acc) {
                Some(l) => l,
                None => break, // need more bytes
            };
            if flen < sbe::FRAME_PREFIX {
                // Malformed length — unrecoverable in a binary stream; bail to
                // force a reconnect with a fresh seq reset.
                return Err(anyhow!("malformed frame length {}", flen));
            }
            if acc.len() < flen {
                break;
            }
            // Own the frame bytes, advance the buffer.
            let frame: Vec<u8> = acc[..flen].to_vec();
            acc.drain(..flen);

            let hdr = match sbe::parse_frame_header(&frame) {
                Some(Ok(h)) => h,
                Some(Err(e)) => return Err(anyhow!("bad frame header: {}", e)),
                None => break,
            };
            session.last_in_seq.store(hdr.seq_num, Ordering::Relaxed);

            match hdr.template_id {
                // ── session-level ────────────────────────────────────────
                sbe::tid::HEARTBEAT => {
                    // nothing to do — inbound heartbeat just resets liveness
                }
                sbe::tid::TEST_REQUEST => {
                    if let Some(id) = sbe::decode_test_request(&hdr, &frame) {
                        // Must reply with a Heartbeat echoing the TestReqID.
                        let s = session.clone();
                        tokio::spawn(async move {
                            let _ = s.send_heartbeat(&id).await;
                        });
                    }
                }
                sbe::tid::LOGOUT => {
                    let txt = sbe::decode_logout(&hdr, &frame).unwrap_or_default();
                    on_msg(InboundMsg::Logout(txt));
                    return Err(anyhow!("server logout"));
                }
                sbe::tid::REJECT => {
                    if let Some(r) = sbe::decode_reject(&hdr, &frame) {
                        on_msg(InboundMsg::Reject(r));
                    }
                }
                // ── order entry ──────────────────────────────────────────
                sbe::tid::EXECUTION_REPORT => {
                    if let Some(er) = sbe::decode_execution_report(&hdr, &frame) {
                        on_msg(InboundMsg::ExecutionReport(er));
                    }
                }
                sbe::tid::EXECUTION_REPORT_ACK => {
                    if let Some(er) = sbe::decode_execution_report_ack(&hdr, &frame) {
                        on_msg(InboundMsg::ExecutionReportAck(er));
                    }
                }
                sbe::tid::ORDER_CANCEL_REJECT => {
                    if let Some(r) = sbe::decode_order_cancel_reject(&hdr, &frame) {
                        on_msg(InboundMsg::OrderCancelReject(r));
                    }
                }
                // ── market data ──────────────────────────────────────────
                sbe::tid::MD_INCREMENTAL_BOOK_TICKER => {
                    let decoded = sbe::decode_book_ticker(&hdr, &frame);
                    // let sym = decoded.as_ref().map(|b| String::from_utf8_lossy(&b.symbol).to_string()).unwrap_or_default();
                    // eprintln!("[MD RX {:?}] tid=206 BOOK_TICKER seq={} sym={} bid={:?} ask={:?}",
                    //           session.kind, hdr.seq_num, sym,
                    //           decoded.as_ref().map(|b| b.bid_px),
                    //           decoded.as_ref().map(|b| b.ask_px));
                    if let Some(bt) = decoded {
                        on_msg(InboundMsg::BookTicker(bt));
                    }
                }
                sbe::tid::MD_INCREMENTAL_TRADE => {
                    let decoded = sbe::decode_incremental_trade(&hdr, &frame);
                    // eprintln!("[MD RX {:?}] tid=205 TRADE seq={} decoded={}",
                    //           session.kind, hdr.seq_num, decoded.is_some());
                    if let Some(tr) = decoded {
                        on_msg(InboundMsg::IncrementalTrade(tr));
                    }
                    let batch = sbe::decode_incremental_trade_all(&hdr, &frame);
                    if !batch.is_empty() {
                        on_msg(InboundMsg::IncrementalTradeBatch(batch));
                    }
                }
                sbe::tid::MARKET_DATA_SNAPSHOT => {
                    let decoded = sbe::decode_snapshot(&hdr, &frame);
                    let sym = decoded.as_ref().map(|b| String::from_utf8_lossy(&b.symbol).to_string()).unwrap_or_default();
                    eprintln!("[MD {:?}] snapshot sym={} bid={:?} ask={:?}",
                              session.kind, sym,
                              decoded.as_ref().map(|b| b.bid_px),
                              decoded.as_ref().map(|b| b.ask_px));
                    if let Some(bt) = decoded {
                        on_msg(InboundMsg::BookTicker(bt));
                    }
                }
                sbe::tid::MARKET_DATA_REQUEST_REJECT => {
                    if let Some(r) = sbe::decode_md_request_reject(&hdr, &frame) {
                        on_msg(InboundMsg::MdRequestReject(r));
                    }
                }
                other => {
                    eprintln!("[MD {:?}] unhandled tid={} seq={}",
                              session.kind, other, hdr.seq_num);
                }
            }
        }
    }
}

/// Spawn a periodic heartbeat sender at the negotiated interval.
/// Binance disconnects if it doesn't hear from us within ~heartbeat window.
pub fn spawn_heartbeat(session: Arc<FixSession>) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            let hb = session.heartbeat_secs().max(1);
            tokio::time::sleep(Duration::from_secs(hb)).await;
            if session.send_heartbeat(&[]).await.is_err() {
                break;
            }
        }
    })
}

/// Generate a unique-ish ClOrdID suffix from a monotonic counter + time.
pub fn gen_clordid(prefix: char, counter: u64) -> String {
    format!("{}{:x}{:x}", prefix, now_ms(), counter & 0xffffff)
}