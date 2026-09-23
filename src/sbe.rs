// SBE codec for Binance Spot FIX API — schema spot-fixsbe-1_1.xml (id=1, version=1).
//
// Wire format per message on the socket:
//   [SOFH: u32 messageLength LE | u16 encodingType=0xEB50]
//   [messageHeader: u16 blockLength | u16 templateId | u16 schemaId | u16 version
//                   u32 seqNum | i64 sendingTime(us)]
//   [root block: fixed-length fields, exactly blockLength bytes]
//   [repeating groups, in schema order]
//   [variable-length data fields, in schema order]
//
// SOFH.messageLength counts the SOFH header itself (6) + everything after.
// All integers little-endian. Decimals = (mantissa i64, exponent i8) encoded
// as separate primitive fields; exponent always precedes its mantissa.
//
// This codec only implements the messages the MM actually sends/receives.
// Field offsets below are derived directly from the schema field order.

#![allow(dead_code)]

pub const ENCODING_TYPE: u16 = 0xEB50;
pub const SCHEMA_ID: u16 = 1;
pub const SCHEMA_VERSION: u16 = 1;
pub const SOFH_LEN: usize = 6;
pub const MSG_HEADER_LEN: usize = 20;
pub const FRAME_PREFIX: usize = SOFH_LEN + MSG_HEADER_LEN; // 22

// ── Template IDs (from <sbe:message id=...>) ────────────────────────────────
pub mod tid {
    pub const HEARTBEAT: u16 = 20001;
    pub const TEST_REQUEST: u16 = 20002;
    pub const REJECT: u16 = 20003;
    pub const LOGOUT: u16 = 20004;
    pub const LOGON: u16 = 20008;
    pub const LOGON_ACK: u16 = 20009;
    pub const NEWS: u16 = 20100;

    pub const NEW_ORDER_SINGLE: u16 = 99;
    pub const ORDER_CANCEL_REQUEST: u16 = 101;
    pub const EXECUTION_REPORT: u16 = 98;
    pub const EXECUTION_REPORT_ACK: u16 = 198;
    pub const ORDER_CANCEL_REJECT: u16 = 96;

    pub const MARKET_DATA_REQUEST: u16 = 202;
    pub const MARKET_DATA_REQUEST_REJECT: u16 = 203;
    pub const MARKET_DATA_SNAPSHOT: u16 = 204;
    pub const MD_INCREMENTAL_TRADE: u16 = 205;
    pub const MD_INCREMENTAL_BOOK_TICKER: u16 = 206;
    pub const MD_INCREMENTAL_DEPTH: u16 = 207;
}

// ── Enum byte values (from schema <enum> validValue) ────────────────────────
pub const BOOL_FALSE: u8 = 0;
pub const BOOL_TRUE: u8 = 1;

// side (char)
pub const SIDE_BUY: u8 = b'1';
pub const SIDE_SELL: u8 = b'2';

// ordType (char)
pub const ORDTYPE_MARKET: u8 = b'1';
pub const ORDTYPE_LIMIT: u8 = b'2';

// execInst (char) — ParticipateDontInitiate = post-only
pub const EXECINST_PARTICIPATE_DONT_INITIATE: u8 = b'6';

// timeInForce (char)
pub const TIF_GTC: u8 = b'1';
pub const TIF_IOC: u8 = b'3';
pub const TIF_FOK: u8 = b'4';

// messageHandling (uint8)
pub const MSGHANDLING_UNORDERED: u8 = 1;
pub const MSGHANDLING_SEQUENTIAL: u8 = 2;

// responseMode (uint8)
pub const RESPMODE_EVERYTHING: u8 = 1;
pub const RESPMODE_ONLY_ACKS: u8 = 2;

// executionReportType (uint8)
pub const ERTYPE_FULL: u8 = 1;
pub const ERTYPE_MINI: u8 = 2;

// execType (char)
pub const EXECTYPE_NEW: u8 = b'0';
pub const EXECTYPE_CANCELED: u8 = b'4';
pub const EXECTYPE_REPLACED: u8 = b'5';
pub const EXECTYPE_REJECTED: u8 = b'8';
pub const EXECTYPE_TRADE: u8 = b'F';
pub const EXECTYPE_EXPIRED: u8 = b'C';

// ordStatus (char)
pub const ORDSTATUS_NEW: u8 = b'0';
pub const ORDSTATUS_PARTIALLY_FILLED: u8 = b'1';
pub const ORDSTATUS_FILLED: u8 = b'2';
pub const ORDSTATUS_CANCELED: u8 = b'4';
pub const ORDSTATUS_PENDING_CANCEL: u8 = b'6';
pub const ORDSTATUS_REJECTED: u8 = b'8';
pub const ORDSTATUS_PENDING_NEW: u8 = b'A';
pub const ORDSTATUS_EXPIRED: u8 = b'C';

// subscriptionRequestType (char)
pub const SUBREQ_SUBSCRIBE: u8 = b'1';
pub const SUBREQ_UNSUBSCRIBE: u8 = b'2';

// mdEntryType (char)
pub const MDENTRY_BID: u8 = b'0';
pub const MDENTRY_OFFER: u8 = b'1';
pub const MDENTRY_TRADE: u8 = b'2';

// selfTradePreventionMode (char)
pub const STP_NONE: u8 = b'1';

// NULL sentinels for optional primitives (SBE convention: max value of type).
pub const NULL_I64: i64 = i64::MIN;
pub const NULL_I32: i32 = i32::MAX;
pub const NULL_U32: u32 = u32::MAX;
pub const NULL_U16: u16 = u16::MAX;
pub const NULL_U8: u8 = u8::MAX;
pub const NULL_I8: i8 = i8::MIN;
// Optional `char`/char-enum fields: SBE null sentinel is 0x00.
// (The schema's `~` = NonRepresentable is a decode marker, not the null value.)
pub const NULL_CHAR: u8 = 0x00;

// ── Low-level LE writers ────────────────────────────────────────────────────

#[inline]
fn put_u8(buf: &mut Vec<u8>, v: u8) { buf.push(v); }
#[inline]
fn put_i8(buf: &mut Vec<u8>, v: i8) { buf.push(v as u8); }
#[inline]
fn put_u16(buf: &mut Vec<u8>, v: u16) { buf.extend_from_slice(&v.to_le_bytes()); }
#[inline]
fn put_u32(buf: &mut Vec<u8>, v: u32) { buf.extend_from_slice(&v.to_le_bytes()); }
#[inline]
fn put_i32(buf: &mut Vec<u8>, v: i32) { buf.extend_from_slice(&v.to_le_bytes()); }
#[inline]
fn put_i64(buf: &mut Vec<u8>, v: i64) { buf.extend_from_slice(&v.to_le_bytes()); }
#[inline]
fn put_u64(buf: &mut Vec<u8>, v: u64) { buf.extend_from_slice(&v.to_le_bytes()); }

/// varString8: u8 length + UTF-8 bytes.
#[inline]
fn put_var8(buf: &mut Vec<u8>, s: &[u8]) {
    debug_assert!(s.len() <= u8::MAX as usize);
    buf.push(s.len() as u8);
    buf.extend_from_slice(s);
}
/// varString: u16 length + UTF-8 bytes.
#[inline]
fn put_var16(buf: &mut Vec<u8>, s: &[u8]) {
    debug_assert!(s.len() <= u16::MAX as usize);
    buf.extend_from_slice(&(s.len() as u16).to_le_bytes());
    buf.extend_from_slice(s);
}

// ── Low-level LE readers (bounds-checked) ───────────────────────────────────

pub struct Reader<'a> {
    pub buf: &'a [u8],
    pub pos: usize,
}

impl<'a> Reader<'a> {
    #[inline]
    pub fn new(buf: &'a [u8]) -> Self { Self { buf, pos: 0 } }
    #[inline]
    pub fn at(buf: &'a [u8], pos: usize) -> Self { Self { buf, pos } }
    #[inline]
    pub fn remaining(&self) -> usize { self.buf.len().saturating_sub(self.pos) }

    #[inline]
    pub fn u8(&mut self) -> Option<u8> {
        let v = *self.buf.get(self.pos)?;
        self.pos += 1;
        Some(v)
    }
    #[inline]
    pub fn i8(&mut self) -> Option<i8> { self.u8().map(|v| v as i8) }
    #[inline]
    pub fn u16(&mut self) -> Option<u16> {
        let b = self.buf.get(self.pos..self.pos + 2)?;
        self.pos += 2;
        Some(u16::from_le_bytes([b[0], b[1]]))
    }
    #[inline]
    pub fn u32(&mut self) -> Option<u32> {
        let b = self.buf.get(self.pos..self.pos + 4)?;
        self.pos += 4;
        Some(u32::from_le_bytes([b[0], b[1], b[2], b[3]]))
    }
    #[inline]
    pub fn i32(&mut self) -> Option<i32> { self.u32().map(|v| v as i32) }
    #[inline]
    pub fn u64(&mut self) -> Option<u64> {
        let b = self.buf.get(self.pos..self.pos + 8)?;
        self.pos += 8;
        Some(u64::from_le_bytes([
            b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7],
        ]))
    }
    #[inline]
    pub fn i64(&mut self) -> Option<i64> { self.u64().map(|v| v as i64) }
    #[inline]
    pub fn skip(&mut self, n: usize) -> Option<()> {
        if self.pos + n > self.buf.len() { return None; }
        self.pos += n;
        Some(())
    }
    /// varString8: u8 len + bytes.
    #[inline]
    pub fn var8(&mut self) -> Option<&'a [u8]> {
        let len = self.u8()? as usize;
        let s = self.buf.get(self.pos..self.pos + len)?;
        self.pos += len;
        Some(s)
    }
    /// varString / optionalVarString: u16 len + bytes.
    #[inline]
    pub fn var16(&mut self) -> Option<&'a [u8]> {
        let len = self.u16()? as usize;
        let s = self.buf.get(self.pos..self.pos + len)?;
        self.pos += len;
        Some(s)
    }
}

// ── Frame header ────────────────────────────────────────────────────────────

/// Parsed SOFH + messageHeader.
#[derive(Debug, Clone, Copy)]
pub struct FrameHeader {
    pub message_length: u32, // SOFH: total bytes incl. 6-byte SOFH
    pub block_length: u16,
    pub template_id: u16,
    pub schema_id: u16,
    pub version: u16,
    pub seq_num: u32,
    pub sending_time: i64,
}

impl FrameHeader {
    /// Total frame length on the wire, including SOFH.
    #[inline]
    pub fn frame_len(&self) -> usize { self.message_length as usize }
    /// Byte offset where the root block begins.
    #[inline]
    pub fn root_offset(&self) -> usize { FRAME_PREFIX }
    /// Byte offset where groups/var-data begin (root block end).
    #[inline]
    pub fn after_root(&self) -> usize { FRAME_PREFIX + self.block_length as usize }
}

/// Try to parse a frame header from the front of `buf`.
/// Returns None if fewer than FRAME_PREFIX bytes are available.
/// Returns Some(Err) if the SOFH encodingType is wrong.
pub fn parse_frame_header(buf: &[u8]) -> Option<Result<FrameHeader, &'static str>> {
    if buf.len() < FRAME_PREFIX { return None; }
    let mut r = Reader::new(buf);
    let message_length = r.u32()?;
    let encoding_type = r.u16()?;
    if encoding_type != ENCODING_TYPE {
        return Some(Err("bad SOFH encodingType"));
    }
    let block_length = r.u16()?;
    let template_id = r.u16()?;
    let schema_id = r.u16()?;
    let version = r.u16()?;
    let seq_num = r.u32()?;
    let sending_time = r.i64()?;
    Some(Ok(FrameHeader {
        message_length,
        block_length,
        template_id,
        schema_id,
        version,
        seq_num,
        sending_time,
    }))
}

/// Given a partially-filled read buffer, return the length of the next
/// complete frame if one is fully present, else None.
#[inline]
pub fn next_frame_len(buf: &[u8]) -> Option<usize> {
    if buf.len() < SOFH_LEN { return None; }
    let mlen = u32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
    if mlen < FRAME_PREFIX { return Some(mlen.max(FRAME_PREFIX)); } // malformed; let caller drop
    if buf.len() < mlen { return None; }
    Some(mlen)
}

// ── Encoder ─────────────────────────────────────────────────────────────────

/// Builds one SBE frame. Caller writes the root block then groups/var-data,
/// then `finish()` back-patches SOFH.messageLength.
pub struct Encoder {
    pub buf: Vec<u8>,
    block_length: u16,
}

impl Encoder {
    /// Begin a frame. `block_length` = exact byte size of the fixed root block.
    /// `seq_num` is the FIX MsgSeqNum. `sending_time` is micros since epoch.
    pub fn start(template_id: u16, block_length: u16, seq_num: u32, sending_time: i64) -> Self {
        let mut buf = Vec::with_capacity(256);
        // SOFH — messageLength patched in finish().
        put_u32(&mut buf, 0);
        put_u16(&mut buf, ENCODING_TYPE);
        // messageHeader
        put_u16(&mut buf, block_length);
        put_u16(&mut buf, template_id);
        put_u16(&mut buf, SCHEMA_ID);
        put_u16(&mut buf, SCHEMA_VERSION);
        put_u32(&mut buf, seq_num);
        put_i64(&mut buf, sending_time);
        debug_assert_eq!(buf.len(), FRAME_PREFIX);
        Self { buf, block_length }
    }

    // Root-block / group field writers (caller invokes in schema order).
    #[inline] pub fn u8(&mut self, v: u8) { put_u8(&mut self.buf, v); }
    #[inline] pub fn i8(&mut self, v: i8) { put_i8(&mut self.buf, v); }
    #[inline] pub fn u16(&mut self, v: u16) { put_u16(&mut self.buf, v); }
    #[inline] pub fn u32(&mut self, v: u32) { put_u32(&mut self.buf, v); }
    #[inline] pub fn i32(&mut self, v: i32) { put_i32(&mut self.buf, v); }
    #[inline] pub fn i64(&mut self, v: i64) { put_i64(&mut self.buf, v); }
    #[inline] pub fn u64(&mut self, v: u64) { put_u64(&mut self.buf, v); }
    #[inline] pub fn var8(&mut self, s: &[u8]) { put_var8(&mut self.buf, s); }
    #[inline] pub fn var16(&mut self, s: &[u8]) { put_var16(&mut self.buf, s); }

    /// Write a repeating-group dimension header.
    /// `smallGroupSize8Encoding`: u8 blockLength, u8 numInGroup.
    #[inline]
    pub fn group_dim_small8(&mut self, block_length: u8, num_in_group: u8) {
        put_u8(&mut self.buf, block_length);
        put_u8(&mut self.buf, num_in_group);
    }
    /// `groupSize16Encoding`: u16 blockLength, u16 numInGroup.
    #[inline]
    pub fn group_dim_16(&mut self, block_length: u16, num_in_group: u16) {
        put_u16(&mut self.buf, block_length);
        put_u16(&mut self.buf, num_in_group);
    }
    /// `smallGroupSize16Encoding`: u8 blockLength, u16 numInGroup.
    #[inline]
    pub fn group_dim_small16(&mut self, block_length: u8, num_in_group: u16) {
        put_u8(&mut self.buf, block_length);
        put_u16(&mut self.buf, num_in_group);
    }
    /// `groupSize32Encoding`: u16 blockLength, u32 numInGroup.
    #[inline]
    pub fn group_dim_32(&mut self, block_length: u16, num_in_group: u32) {
        put_u16(&mut self.buf, block_length);
        put_u32(&mut self.buf, num_in_group);
    }

    /// Back-patch SOFH.messageLength and return the finished frame.
    pub fn finish(mut self) -> Vec<u8> {
        let total = self.buf.len() as u32;
        self.buf[0..4].copy_from_slice(&total.to_le_bytes());
        let _ = self.block_length;
        self.buf
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Root-block lengths.
//
// blockLength = sum of the fixed-size primitive fields in the root block, in
// schema order, BEFORE the first repeating group or var-data field. Optional
// primitives still occupy their full width (NULL sentinel). var-data and
// groups are NOT part of the root block.
// ─────────────────────────────────────────────────────────────────────────────

// Logon (id 20008): root block fields:
//   98  EncryptMethod   uint8     1
//   108 HeartBtInt      uint32    4
//   141 ResetSeqNumFlag boolEnum  1
//   25035 MessageHandling uint8   1
//   25036 ResponseMode    uint8   1
//   25045 ExecutionReportType uint8 1
//   9406 DropCopyFlag    boolEnum 1
//   25000 RecvWindow     durationUs(uint32) 4
//  = 14 ; then var: SenderCompId, TargetCompId, RawData, Username
pub const LOGON_BLOCK_LEN: u16 = 14;

// MarketDataRequest (id 202): root block:
//   263 SubscriptionRequestType char 1
//   264 MarketDepth   uint16  2
//   266 AggregatedBook boolEnum 1
//  = 4 ; then group RelatedSym, group MDEntryTypes, var MDReqID
pub const MARKET_DATA_REQUEST_BLOCK_LEN: u16 = 4;
//   RelatedSym group: only a var field (Symbol) inside → group blockLength = 0
pub const RELATED_SYM_GROUP_BLOCK_LEN: u16 = 0;
//   MDEntryTypes group: MDEntryType char → blockLength = 1
pub const MD_ENTRY_TYPES_GROUP_BLOCK_LEN: u8 = 1;

// NewOrderSingle (id 99): root block fields, in schema order:
//   25054 PriceExponent exponent8(i8)        1
//   25055 QtyExponent   exponent8(i8)        1
//   38    OrderQty      mantissa64(i64) opt  8
//   40    OrdType       ordType(char)        1
//   18    ExecInst      execInst(char) opt   1
//   44    Price         mantissa64(i64) opt  8
//   1100  TriggerType   char opt             1
//   1101  TriggerAction char opt             1
//   1102  TriggerPrice  mantissa64 opt       8
//   1107  TriggerPriceType char opt          1
//   1109  TriggerPriceDirection char opt     1
//   25009 TriggerTrailingDeltaBips uint64 opt 8
//   211   PegOffsetValue uint8 opt           1
//   1094  PegPriceType char opt              1
//   835   PegMoveType  uint8 opt             1
//   836   PegOffsetType char opt             1
//   54    Side         side(char)           1
//   59    TimeInForce  timeInForce(char) opt 1
//   111   MaxFloor     mantissa64 opt        8
//   152   CashOrderQty mantissa64 opt        8
//   847   TargetStrategy int32 opt           4
//   7940  StrategyID   int64 opt             8
//   25001 SelfTradePreventionMode char opt   1
//   25032 SOR          boolEnum opt          1
//  = 1+1+8+1+1+8+1+1+8+1+1+8+1+1+1+1+1+1+8+8+4+8+1+1 = 86
//  then var: ClOrdID, Symbol
pub const NEW_ORDER_SINGLE_BLOCK_LEN: u16 = 76;

// OrderCancelRequest (id 101): root block:
//   37    OrderID  ordId(i64) opt         8
//   66    ListID   ordListId(i64) opt     8
//   25002 CancelRestrictions uint8 opt    1
//  = 17 ; then var: ClOrdID, OrigClOrdID, OrigClListID, Symbol
pub const ORDER_CANCEL_REQUEST_BLOCK_LEN: u16 = 17;

// Heartbeat (id 20001): root block empty (0); then var TestReqID (optionalVarString8)
pub const HEARTBEAT_BLOCK_LEN: u16 = 0;
// TestRequest (id 20002): root block empty (0); then var TestReqID (varString8)
pub const TEST_REQUEST_BLOCK_LEN: u16 = 0;
// Logout (id 20004): root block empty (0); then var Text (optionalVarString)
pub const LOGOUT_BLOCK_LEN: u16 = 0;

// ── Encoders for the messages the MM sends ──────────────────────────────────

/// Logon <A>. `raw_data` is the base64 Ed25519 signature string.
/// `recv_window` micros, applied to the whole session.
pub fn encode_logon(
    seq_num: u32,
    sending_time: i64,
    heart_bt_int_sec: u32,
    reset_seq: bool,
    msg_handling: u8,
    response_mode: Option<u8>,
    exec_report_type: Option<u8>,
    recv_window_us: u32,
    sender_comp_id: &[u8],
    target_comp_id: &[u8],
    raw_data: &[u8],
    username: &[u8],
) -> Vec<u8> {
    let mut e = Encoder::start(tid::LOGON, LOGON_BLOCK_LEN, seq_num, sending_time);
    // root block
    e.u8(0);                             // 98 EncryptMethod (NONE)
    e.u32(heart_bt_int_sec);             // 108 HeartBtInt
    e.u8(if reset_seq { BOOL_TRUE } else { BOOL_FALSE }); // 141 ResetSeqNumFlag
    e.u8(msg_handling);                  // 25035 MessageHandling
    e.u8(response_mode.unwrap_or(NULL_U8));     // 25036 ResponseMode
    e.u8(exec_report_type.unwrap_or(NULL_U8));  // 25045 ExecutionReportType
    e.u8(NULL_U8);                       // 9406 DropCopyFlag (optional → null)
    e.u32(recv_window_us);               // 25000 RecvWindow
    // var-data, in schema order
    e.var8(sender_comp_id);              // 49 SenderCompId  varString8
    e.var8(target_comp_id);              // 56 TargetCompId  varString8
    e.var16(raw_data);                   // 96 RawData       varString
    e.var16(username);                   // 553 Username     varString
    e.finish()
}

/// MarketDataRequest <V> for a single symbol and a set of MDEntryTypes.
pub fn encode_market_data_request(
    seq_num: u32,
    sending_time: i64,
    subscribe: bool,
    market_depth: Option<u16>,
    aggregated_book: Option<bool>,
    symbol: &[u8],
    md_entry_types: &[u8], // each a mdEntryType char value
    md_req_id: &[u8],
) -> Vec<u8> {
    let mut e = Encoder::start(
        tid::MARKET_DATA_REQUEST,
        MARKET_DATA_REQUEST_BLOCK_LEN,
        seq_num,
        sending_time,
    );
    // root block
    e.u8(if subscribe { SUBREQ_SUBSCRIBE } else { SUBREQ_UNSUBSCRIBE }); // 263
    e.u16(market_depth.unwrap_or(NULL_U16)); // 264 MarketDepth
    e.u8(match aggregated_book {             // 266 AggregatedBook
        Some(true) => BOOL_TRUE,
        Some(false) => BOOL_FALSE,
        None => NULL_U8,
    });
    // group 146 RelatedSym — groupSize16Encoding, blockLength 0, one entry (var Symbol)
    e.group_dim_16(RELATED_SYM_GROUP_BLOCK_LEN, 1);
    e.var8(symbol); // Symbol varString8
    // group 267 MDEntryTypes — smallGroupSize8Encoding, blockLength 1
    e.group_dim_small8(MD_ENTRY_TYPES_GROUP_BLOCK_LEN, md_entry_types.len() as u8);
    for &t in md_entry_types {
        e.u8(t); // 269 MDEntryType
    }
    // var MDReqID
    e.var8(md_req_id);
    e.finish()
}

/// NewOrderSingle <D> — LIMIT, post-only (ParticipateDontInitiate), GTC.
/// `price_mantissa` / `qty_mantissa` are integers scaled by 10^-exponent.
pub fn encode_new_order_limit_postonly(
    seq_num: u32,
    sending_time: i64,
    price_exponent: i8,
    qty_exponent: i8,
    qty_mantissa: i64,
    price_mantissa: i64,
    side: u8,
    cl_ord_id: &[u8],
    symbol: &[u8],
) -> Vec<u8> {
    let mut e = Encoder::start(
        tid::NEW_ORDER_SINGLE,
        NEW_ORDER_SINGLE_BLOCK_LEN,
        seq_num,
        sending_time,
    );
    // root block — exact schema order
    e.i8(price_exponent);                // 25054 PriceExponent
    e.i8(qty_exponent);                  // 25055 QtyExponent
    e.i64(qty_mantissa);                 // 38 OrderQty
    e.u8(ORDTYPE_LIMIT);                 // 40 OrdType
    e.u8(EXECINST_PARTICIPATE_DONT_INITIATE); // 18 ExecInst (post-only)
    e.i64(price_mantissa);               // 44 Price
    e.u8(NULL_CHAR);                     // 1100 TriggerType
    e.u8(NULL_CHAR);                     // 1101 TriggerAction
    e.i64(NULL_I64);                     // 1102 TriggerPrice
    e.u8(NULL_CHAR);                     // 1107 TriggerPriceType
    e.u8(NULL_CHAR);                     // 1109 TriggerPriceDirection
    e.u64(u64::MAX);                     // 25009 TriggerTrailingDeltaBips
    e.u8(NULL_U8);                       // 211 PegOffsetValue
    e.u8(NULL_CHAR);                     // 1094 PegPriceType
    e.u8(NULL_U8);                       // 835 PegMoveType
    e.u8(NULL_CHAR);                     // 836 PegOffsetType
    e.u8(side);                          // 54 Side
    e.u8(NULL_CHAR);                     // 59 TimeInForce — omitted: LIMIT_MAKER (ExecInst=6) does not accept TIF
    e.i64(NULL_I64);                     // 111 MaxFloor
    e.i64(NULL_I64);                     // 152 CashOrderQty
    e.i32(NULL_I32);                     // 847 TargetStrategy (optional int32 null = i32::MAX)
    e.i64(NULL_I64);                     // 7940 StrategyID
    e.u8(NULL_CHAR);                     // 25001 SelfTradePreventionMode
    e.u8(NULL_U8);                       // 25032 SOR (bool null)
    // var-data
    e.var8(cl_ord_id);                   // 11 ClOrdID
    e.var8(symbol);                      // 55 Symbol
    e.finish()
}

/// OrderCancelRequest <F> — cancel by OrderID (preferred) or OrigClOrdID.
/// Pass `order_id = NULL_I64` and a non-empty `orig_cl_ord_id` to cancel by cid.
pub fn encode_order_cancel(
    seq_num: u32,
    sending_time: i64,
    order_id: i64,
    cl_ord_id: &[u8],
    orig_cl_ord_id: &[u8],
    symbol: &[u8],
) -> Vec<u8> {
    let mut e = Encoder::start(
        tid::ORDER_CANCEL_REQUEST,
        ORDER_CANCEL_REQUEST_BLOCK_LEN,
        seq_num,
        sending_time,
    );
    // root block
    e.i64(order_id);     // 37 OrderID (NULL_I64 → absent)
    e.i64(NULL_I64);     // 66 ListID
    e.u8(NULL_U8);       // 25002 CancelRestrictions
    // var-data, schema order: ClOrdID, OrigClOrdID, OrigClListID, Symbol
    e.var8(cl_ord_id);                 // 11 ClOrdID (this cancel's own id)
    e.var8(orig_cl_ord_id);            // 41 OrigClOrdID (optionalVarString8; empty = null)
    e.var8(&[]);                       // 25015 OrigClListID (empty = null)
    e.var8(symbol);                    // 55 Symbol
    e.finish()
}

/// Heartbeat <0>. `test_req_id` empty → null.
pub fn encode_heartbeat(seq_num: u32, sending_time: i64, test_req_id: &[u8]) -> Vec<u8> {
    let mut e = Encoder::start(tid::HEARTBEAT, HEARTBEAT_BLOCK_LEN, seq_num, sending_time);
    e.var8(test_req_id); // 112 TestReqID (optionalVarString8)
    e.finish()
}

/// TestRequest <1>.
pub fn encode_test_request(seq_num: u32, sending_time: i64, test_req_id: &[u8]) -> Vec<u8> {
    let mut e = Encoder::start(tid::TEST_REQUEST, TEST_REQUEST_BLOCK_LEN, seq_num, sending_time);
    e.var8(test_req_id); // 112 TestReqID (varString8)
    e.finish()
}

/// Logout <5>.
pub fn encode_logout(seq_num: u32, sending_time: i64, text: &[u8]) -> Vec<u8> {
    let mut e = Encoder::start(tid::LOGOUT, LOGOUT_BLOCK_LEN, seq_num, sending_time);
    e.var16(text); // 58 Text (optionalVarString)
    e.finish()
}

// ── Decoders for inbound messages ───────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct LogonAck {
    pub heart_bt_int: u32,
    pub schema_deprecated: bool,
    pub uuid: Vec<u8>,
}

/// Decode LogonAck <id 20009>. `body` is the full frame.
pub fn decode_logon_ack(hdr: &FrameHeader, body: &[u8]) -> Option<LogonAck> {
    // root block:
    //   98  EncryptMethod uint8 opt   1
    //   108 HeartBtInt    uint32      4
    //   25052 SbeSchemaIdVersionDeprecated boolEnum 1
    // then var UUID (varString8)
    let mut r = Reader::at(body, hdr.root_offset());
    let _encrypt = r.u8()?;
    let heart_bt_int = r.u32()?;
    let dep = r.u8()?;
    // var-data starts at after_root (block_length may exceed what we read if
    // schema added trailing fixed fields; jump explicitly).
    let mut vr = Reader::at(body, hdr.after_root());
    let uuid = vr.var8()?;
    Some(LogonAck {
        heart_bt_int,
        schema_deprecated: dep == BOOL_TRUE,
        uuid: uuid.to_vec(),
    })
}

/// A decimal value carried as (mantissa, exponent).
#[inline]
pub fn decimal_to_f64(mantissa: i64, exponent: i8) -> f64 {
    if mantissa == NULL_I64 { return 0.0; }
    (mantissa as f64) * 10f64.powi(exponent as i32)
}

#[derive(Debug, Clone)]
pub struct ExecutionReport {
    pub price_exponent: i8,
    pub qty_exponent: i8,
    pub exec_id: i64,
    pub order_id: i64,
    pub order_qty: i64,    // mantissa
    pub ord_type: u8,
    pub side: u8,
    pub price: i64,        // mantissa
    pub transact_time: i64,
    pub exec_type: u8,
    pub cum_qty: i64,      // mantissa
    pub leaves_qty: i64,   // mantissa
    pub cum_quote_qty: i64,// mantissa
    pub trade_id: i64,
    pub last_px: i64,      // mantissa
    pub last_qty: i64,     // mantissa
    pub ord_status: u8,
    pub error_code: i32,
    pub cl_ord_id: Vec<u8>,
    pub orig_cl_ord_id: Vec<u8>,
    pub symbol: Vec<u8>,
    pub error_text: Vec<u8>,
}

/// Decode ExecutionReport <id 98>.
///
/// The root block is long and full of optional fields. We read it strictly in
/// schema order so positions stay correct, then locate var-data at after_root().
/// The MiscFees group sits between the root block and var-data; we skip it
/// using its dimension header.
pub fn decode_execution_report(hdr: &FrameHeader, body: &[u8]) -> Option<ExecutionReport> {
    let mut r = Reader::at(body, hdr.root_offset());

    let price_exponent = r.i8()?;            // 25054
    let qty_exponent = r.i8()?;              // 25055
    let exec_id = r.i64()?;                  // 17  opt
    let order_id = r.i64()?;                 // 37  opt
    let order_qty = r.i64()?;                // 38  opt
    let ord_type = r.u8()?;                  // 40
    let side = r.u8()?;                      // 54
    let _exec_inst = r.u8()?;                // 18  opt
    let price = r.i64()?;                    // 44  opt
    let _trigger_type = r.u8()?;             // 1100 opt
    let _trigger_action = r.u8()?;           // 1101 opt
    let _trigger_price = r.i64()?;           // 1102 opt
    let _trigger_price_type = r.u8()?;       // 1107 opt
    let _trigger_price_dir = r.u8()?;        // 1109 opt
    let _trigger_trail = r.u64()?;           // 25009 opt
    let _peg_offset_value = r.u8()?;         // 211  opt
    let _peg_price_type = r.u8()?;           // 1094 opt
    let _peg_move_type = r.u8()?;            // 835  opt
    let _peg_offset_type = r.u8()?;          // 836  opt
    let _pegged_price = r.i64()?;            // 839  opt
    let _time_in_force = r.u8()?;            // 59   opt
    let transact_time = r.i64()?;            // 60   opt
    let _order_creation_time = r.i64()?;     // 25018 opt
    let _max_floor = r.i64()?;               // 111  opt
    let _list_id = r.i64()?;                 // 66   opt
    let _cash_order_qty = r.i64()?;          // 152  opt
    let _target_strategy = r.i32()?;         // 847  opt
    let _strategy_id = r.i64()?;             // 7940 opt
    let _order_capacity = r.u8()?;           // 528  opt
    let _stp_mode = r.u8()?;                 // 25001 opt
    let exec_type = r.u8()?;                 // 150
    let cum_qty = r.i64()?;                  // 14
    let leaves_qty = r.i64()?;               // 151  opt
    let cum_quote_qty = r.i64()?;            // 25017 opt
    let _aggressor_indicator = r.u8()?;      // 1057 opt
    let trade_id = r.i64()?;                 // 1003 opt
    let last_px = r.i64()?;                  // 31   opt
    let last_qty = r.i64()?;                 // 32
    let ord_status = r.u8()?;                // 39
    let _secondary_order_id = r.i64()?;      // 198  opt
    let _secondary_ext_acct = r.i64()?;      // 25020 opt
    let _alloc_id = r.i64()?;                // 70   opt
    let _match_type = r.u8()?;               // 574  opt
    let _working_floor = r.u8()?;            // 25021 opt
    let _working_indicator = r.u8()?;        // 636  opt
    let _working_time = r.i64()?;            // 25023 opt
    let _trailing_time = r.i64()?;           // 25022 opt
    let _prevented_match_id = r.i64()?;      // 25024 opt
    let _prevented_exec_price = r.i64()?;    // 25025 opt
    let _prevented_exec_qty = r.i64()?;      // 25026 opt
    let _trade_group_id = r.i64()?;          // 25027 opt
    let _counter_order_id = r.i64()?;        // 25029 opt
    let _prevented_qty = r.i64()?;           // 25030 opt
    let _last_prevented_qty = r.i64()?;      // 25031 opt
    let _sor = r.u8()?;                      // 25032 opt
    let _ord_rej_reason = r.u8()?;           // 103  opt
    let error_code = r.i32()?;               // 25016 opt
    let _expiry_reason = r.u8()?;            // 25056 opt (sinceVersion=1)

    // var-data begins at after_root(); the MiscFees group (136) is between the
    // root block and var-data. Use the dimension header to skip it.
    let mut g = Reader::at(body, hdr.after_root());
    // MiscFees: smallGroupSize16Encoding → u8 blockLength, u16 numInGroup.
    let mf_block = g.u8()? as usize;
    let mf_num = g.u16()? as usize;
    for _ in 0..mf_num {
        // each entry: blockLength fixed bytes + var MiscFeeCurr (varString8)
        g.skip(mf_block)?;
        let _curr = g.var8()?;
    }
    // var-data fields, schema order:
    //   11 ClOrdID optionalVarString8
    //   41 OrigClOrdID optionalVarString8
    //   55 Symbol varString8
    //   25019 SecondarySymbol optionalVarString8 (u8 len)
    //   25028 CounterSymbol  optionalVarString8 (u8 len)
    //   58 ErrorText        optionalVarString  (u16 len)
    let cl_ord_id = g.var8()?;
    let orig_cl_ord_id = g.var8()?;
    let symbol = g.var8()?;
    let _secondary_symbol = g.var8().unwrap_or(&[]);
    let _counter_symbol = g.var8().unwrap_or(&[]);
    let error_text = g.var16().map(|s| s.to_vec()).unwrap_or_default();

    Some(ExecutionReport {
        price_exponent,
        qty_exponent,
        exec_id,
        order_id,
        order_qty,
        ord_type,
        side,
        price,
        transact_time,
        exec_type,
        cum_qty,
        leaves_qty,
        cum_quote_qty,
        trade_id,
        last_px,
        last_qty,
        ord_status,
        error_code,
        cl_ord_id: cl_ord_id.to_vec(),
        orig_cl_ord_id: orig_cl_ord_id.to_vec(),
        symbol: symbol.to_vec(),
        error_text,
    })
}

#[derive(Debug, Clone)]
pub struct ExecutionReportAck {
    pub order_id: i64,
    pub transact_time: i64,
    pub exec_type: u8,
    pub ord_status: u8,
    pub error_code: i32,
    pub cl_ord_id: Vec<u8>,
    pub symbol: Vec<u8>,
}

/// Decode ExecutionReportAck <id 198> (the "Mini" report).
pub fn decode_execution_report_ack(hdr: &FrameHeader, body: &[u8]) -> Option<ExecutionReportAck> {
    // root block:
    //   37 OrderID i64 opt        8
    //   66 ListID  i64 opt        8
    //   60 TransactTime i64 opt   8
    //   150 ExecType char         1
    //   39  OrdStatus char        1
    //   103 OrdRejReason u8 opt   1
    //   25016 ErrorCode i32 opt   4
    // then var: 11 ClOrdID optionalVarString8, 55 Symbol varString8,
    //           58 ErrorText optionalVarString
    let mut r = Reader::at(body, hdr.root_offset());
    let order_id = r.i64()?;
    let _list_id = r.i64()?;
    let transact_time = r.i64()?;
    let exec_type = r.u8()?;
    let ord_status = r.u8()?;
    let _ord_rej_reason = r.u8()?;
    let error_code = r.i32()?;

    let mut g = Reader::at(body, hdr.after_root());
    let cl_ord_id = g.var8()?;
    let symbol = g.var8()?;
    Some(ExecutionReportAck {
        order_id,
        transact_time,
        exec_type,
        ord_status,
        error_code,
        cl_ord_id: cl_ord_id.to_vec(),
        symbol: symbol.to_vec(),
    })
}

#[derive(Debug, Clone)]
pub struct OrderCancelRejectMsg {
    pub order_id: i64,
    pub error_code: i32,
    pub cl_ord_id: Vec<u8>,
    pub orig_cl_ord_id: Vec<u8>,
    pub symbol: Vec<u8>,
}

/// Decode OrderCancelReject <id 96>.
pub fn decode_order_cancel_reject(hdr: &FrameHeader, body: &[u8]) -> Option<OrderCancelRejectMsg> {
    // root block:
    //   37 OrderID i64 opt              8
    //   66 ListID  i64 opt              8
    //   25002 CancelRestrictions u8 opt 1
    //   434 CxlRejResponseTo char       1
    //   25016 ErrorCode i32             4
    // var: 11 ClOrdID varString8, 41 OrigClOrdID optionalVarString8,
    //      25015 OrigClListID optionalVarString8, 55 Symbol varString8,
    //      58 ErrorText varString
    let mut r = Reader::at(body, hdr.root_offset());
    let order_id = r.i64()?;
    let _list_id = r.i64()?;
    let _cancel_restrictions = r.u8()?;
    let _cxl_rej_response_to = r.u8()?;
    let error_code = r.i32()?;

    let mut g = Reader::at(body, hdr.after_root());
    let cl_ord_id = g.var8()?;
    let orig_cl_ord_id = g.var8()?;
    let _orig_cl_list_id = g.var8()?;
    let symbol = g.var8()?;
    Some(OrderCancelRejectMsg {
        order_id,
        error_code,
        cl_ord_id: cl_ord_id.to_vec(),
        orig_cl_ord_id: orig_cl_ord_id.to_vec(),
        symbol: symbol.to_vec(),
    })
}

#[derive(Debug, Clone)]
pub struct RejectMsg {
    pub ref_seq_num: u32,
    pub ref_tag_id: u32,
    pub session_reject_reason: u8,
    pub error_code: i32,
    pub text: Vec<u8>,
}

/// Decode Reject <id 20003>.
pub fn decode_reject(hdr: &FrameHeader, body: &[u8]) -> Option<RejectMsg> {
    // root block:
    //   45 RefSeqNum u32 opt              4
    //   371 RefTagID u32 opt              4
    //   373 SessionRejectReason u8 opt    1
    //   25016 ErrorCode i32 opt           4
    // var: 372 RefMsgType optionalVarString8, 58 Text optionalVarString
    let mut r = Reader::at(body, hdr.root_offset());
    let ref_seq_num = r.u32()?;
    let ref_tag_id = r.u32()?;
    let session_reject_reason = r.u8()?;
    let error_code = r.i32()?;

    let mut g = Reader::at(body, hdr.after_root());
    let _ref_msg_type = g.var8()?;
    let text = g.var16()?;
    Some(RejectMsg {
        ref_seq_num,
        ref_tag_id,
        session_reject_reason,
        error_code,
        text: text.to_vec(),
    })
}

/// Decode the TestReqID out of an inbound TestRequest <id 20002>.
pub fn decode_test_request(hdr: &FrameHeader, body: &[u8]) -> Option<Vec<u8>> {
    let mut g = Reader::at(body, hdr.after_root());
    g.var8().map(|s| s.to_vec())
}

#[derive(Debug, Clone)]
pub struct BookTicker {
    pub last_book_update_id: i64,
    pub bid_px: f64,
    pub bid_qty: f64,
    pub ask_px: f64,
    pub ask_qty: f64,
    pub symbol: Vec<u8>,
}

/// Decode MarketDataIncrementalBookTicker <id 206>.
///
/// root block:
///   25044 LastBookUpdateID i64   8
///   25054 PriceExponent i8       1
///   25055 QtyExponent  i8        1
/// then group 25047 MDEntriesBids (smallGroupSize8Encoding: u8 bl, u8 num),
///      group 25048 MDEntriesAsks (same),
///      var 55 Symbol.
/// Each MDEntries* entry: 270 MDEntryPx mantissa64 (8) + 271 MDEntrySize
/// mantissa64 opt (8) → entry blockLength = 16.
pub fn decode_book_ticker(hdr: &FrameHeader, body: &[u8]) -> Option<BookTicker> {
    let mut r = Reader::at(body, hdr.root_offset());
    let last_book_update_id = r.i64()?;
    let price_exponent = r.i8()?;
    let qty_exponent = r.i8()?;

    let mut g = Reader::at(body, hdr.after_root());

    // Bids group
    let bid_bl = g.u8()? as usize;
    let bid_num = g.u8()? as usize;
    let mut bid_px = 0i64;
    let mut bid_qty = 0i64;
    for i in 0..bid_num {
        let start = g.pos;
        let px = g.i64()?;
        let sz = g.i64()?;
        if i == 0 {
            bid_px = px;
            bid_qty = sz;
        }
        // Honor the declared entry blockLength in case schema grew.
        g.pos = start + bid_bl;
    }

    // Asks group
    let ask_bl = g.u8()? as usize;
    let ask_num = g.u8()? as usize;
    let mut ask_px = 0i64;
    let mut ask_qty = 0i64;
    for i in 0..ask_num {
        let start = g.pos;
        let px = g.i64()?;
        let sz = g.i64()?;
        if i == 0 {
            ask_px = px;
            ask_qty = sz;
        }
        g.pos = start + ask_bl;
    }

    // Symbol var-data (varString8: u8 length + bytes).
    let symbol = g.var8().map(|s| s.to_vec()).unwrap_or_default();

    Some(BookTicker {
        last_book_update_id,
        bid_px: decimal_to_f64(bid_px, price_exponent),
        bid_qty: decimal_to_f64(bid_qty, qty_exponent),
        ask_px: decimal_to_f64(ask_px, price_exponent),
        ask_qty: decimal_to_f64(ask_qty, qty_exponent),
        symbol,
    })
}

#[derive(Debug, Clone, Copy)]
pub struct IncrementalTrade {
    pub transact_time: i64,
    pub px: f64,
    pub qty: f64,
    /// AggressorSide: SIDE_BUY / SIDE_SELL, or 0 if absent.
    pub aggressor_side: u8,
}

/// Decode MarketDataIncrementalTrade <id 205>. Returns the last entry only
/// (the MM's k estimator consumes one trade at a time; multiple entries in
/// one frame are rare and the most recent is what matters).
///
/// root block:
///   60 TransactTime i64    8
///   25054 PriceExponent i8 1
///   25055 QtyExponent i8   1
/// then group 268 MDEntries (groupSize32Encoding: u16 bl, u32 num),
///      var 55 Symbol.
/// Each entry: 1003 TradeID i64 (8) + 270 MDEntryPx mantissa64 (8)
///           + 271 MDEntrySize mantissa64 (8) + 2446 AggressorSide char opt (1)
///           → entry blockLength = 25.
pub fn decode_incremental_trade(hdr: &FrameHeader, body: &[u8]) -> Option<IncrementalTrade> {
    let mut r = Reader::at(body, hdr.root_offset());
    let transact_time = r.i64()?;
    let price_exponent = r.i8()?;
    let qty_exponent = r.i8()?;

    let mut g = Reader::at(body, hdr.after_root());
    let entry_bl = g.u16()? as usize;
    let num = g.u32()? as usize;
    if num == 0 { return None; }

    let mut px = 0i64;
    let mut qty = 0i64;
    let mut aggressor = 0u8;
    for i in 0..num {
        let start = g.pos;
        let _trade_id = g.i64()?;
        let e_px = g.i64()?;
        let e_sz = g.i64()?;
        let e_agg = g.u8()?;
        if i == num - 1 {
            px = e_px;
            qty = e_sz;
            aggressor = if e_agg == NULL_U8 { 0 } else { e_agg };
        }
        g.pos = start + entry_bl;
    }

    Some(IncrementalTrade {
        transact_time,
        px: decimal_to_f64(px, price_exponent),
        qty: decimal_to_f64(qty, qty_exponent),
        aggressor_side: aggressor,
    })
}

/// Decode MarketDataIncrementalTrade <id 205>, returning EVERY entry in the
/// frame rather than just the last. Used by the signed-rolling-qty impulse
/// predictor, which must sum all trade quantities — dropping intermediate
/// entries would understate aggressor flow. Layout is identical to
/// decode_incremental_trade.
pub fn decode_incremental_trade_all(hdr: &FrameHeader, body: &[u8]) -> Vec<IncrementalTrade> {
    let mut out = Vec::new();
    let mut r = Reader::at(body, hdr.root_offset());
    let transact_time = match r.i64() { Some(v) => v, None => return out };
    let price_exponent = match r.i8() { Some(v) => v, None => return out };
    let qty_exponent = match r.i8() { Some(v) => v, None => return out };

    let mut g = Reader::at(body, hdr.after_root());
    let entry_bl = match g.u16() { Some(v) => v as usize, None => return out };
    let num = match g.u32() { Some(v) => v as usize, None => return out };
    if num == 0 { return out; }

    for _ in 0..num {
        let start = g.pos;
        let _trade_id = match g.i64() { Some(v) => v, None => return out };
        let e_px = match g.i64() { Some(v) => v, None => return out };
        let e_sz = match g.i64() { Some(v) => v, None => return out };
        let e_agg = match g.u8() { Some(v) => v, None => return out };
        out.push(IncrementalTrade {
            transact_time,
            px: decimal_to_f64(e_px, price_exponent),
            qty: decimal_to_f64(e_sz, qty_exponent),
            aggressor_side: if e_agg == NULL_U8 { 0 } else { e_agg },
        });
        g.pos = start + entry_bl;
    }
    out
}

#[derive(Debug, Clone)]
pub struct MarketDataRequestReject {
    pub error_code: i32,
    pub md_req_id: Vec<u8>,
    pub text: Vec<u8>,
}

/// Decode MarketDataRequestReject <id 203>.
pub fn decode_md_request_reject(hdr: &FrameHeader, body: &[u8]) -> Option<MarketDataRequestReject> {
    // root block:
    //   281 MDReqRejReason char opt   1
    //   25016 ErrorCode i32 opt       4
    // var: 262 MDReqID varString8, 58 Text optionalVarString
    let mut r = Reader::at(body, hdr.root_offset());
    let _reason = r.u8()?;
    let error_code = r.i32()?;
    let mut g = Reader::at(body, hdr.after_root());
    let md_req_id = g.var8()?;
    let text = g.var16()?;
    Some(MarketDataRequestReject {
        error_code,
        md_req_id: md_req_id.to_vec(),
        text: text.to_vec(),
    })
}

/// Decode Logout <id 20004> text.
pub fn decode_logout(hdr: &FrameHeader, body: &[u8]) -> Option<Vec<u8>> {
    let mut g = Reader::at(body, hdr.after_root());
    g.var16().map(|s| s.to_vec())
}

/// Decode MarketDataSnapshot <id 204>.
///
/// root block:
///   25044 LastBookUpdateID i64 opt   8
///   25054 PriceExponent i8           1
///   25055 QtyExponent  i8            1
/// then group 25047 MDEntriesBids (smallGroupSize16Encoding: u8 bl, u16 num),
///      group 25048 MDEntriesAsks (same),
///      var 55 Symbol.
/// Each entry: 270 MDEntryPx mantissa64 (8) + 271 MDEntrySize mantissa64 (8) = 16.
pub fn decode_snapshot(hdr: &FrameHeader, body: &[u8]) -> Option<BookTicker> {
    let mut r = Reader::at(body, hdr.root_offset());
    let last_book_update_id = r.i64()?;
    let price_exponent = r.i8()?;
    let qty_exponent = r.i8()?;

    let mut g = Reader::at(body, hdr.after_root());

    // Bids group — smallGroupSize16Encoding: u8 blockLength, u16 numInGroup
    let bid_bl = g.u8()? as usize;
    let bid_num = g.u16()? as usize;
    let mut bid_px = 0i64;
    let mut bid_qty = 0i64;
    for i in 0..bid_num {
        let start = g.pos;
        let px = g.i64()?;
        let sz = g.i64()?;
        if i == 0 { bid_px = px; bid_qty = sz; }
        g.pos = start + bid_bl;
    }

    // Asks group
    let ask_bl = g.u8()? as usize;
    let ask_num = g.u16()? as usize;
    let mut ask_px = 0i64;
    let mut ask_qty = 0i64;
    for i in 0..ask_num {
        let start = g.pos;
        let px = g.i64()?;
        let sz = g.i64()?;
        if i == 0 { ask_px = px; ask_qty = sz; }
        g.pos = start + ask_bl;
    }

    // Symbol var-data.
    let symbol = g.var8().map(|s| s.to_vec()).unwrap_or_default();

    Some(BookTicker {
        last_book_update_id,
        bid_px: decimal_to_f64(bid_px, price_exponent),
        bid_qty: decimal_to_f64(bid_qty, qty_exponent),
        ask_px: decimal_to_f64(ask_px, price_exponent),
        ask_qty: decimal_to_f64(ask_qty, qty_exponent),
        symbol,
    })
}