//! MQTT 5.0 packet codec – Subset for current impl
//!
//! Covers:
//!   Parsing  : CONNECT, SUBSCRIBE, UNSUBSCRIBE, PINGREQ, DISCONNECT, PUBLISH
//!   Building : CONNACK, SUBACK, UNSUBACK, PUBLISH (QoS 0), PINGRESP, DISCONNECT

// ---------------------------------------------------------------------------
// Packet types
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum PacketType {
    Connect = 1,
    Connack = 2,
    Publish = 3,
    Puback = 4,
    Subscribe = 8,
    Suback = 9,
    Unsubscribe = 10,
    Unsuback = 11,
    Pingreq = 12,
    Pingresp = 13,
    Disconnect = 14,
}

impl PacketType {
    pub fn from_u8(val: u8) -> Option<Self> {
        match val {
            1 => Some(Self::Connect),
            2 => Some(Self::Connack),
            3 => Some(Self::Publish),
            4 => Some(Self::Puback),
            8 => Some(Self::Subscribe),
            9 => Some(Self::Suback),
            10 => Some(Self::Unsubscribe),
            11 => Some(Self::Unsuback),
            12 => Some(Self::Pingreq),
            13 => Some(Self::Pingresp),
            14 => Some(Self::Disconnect),
            _ => None,
        }
    }

    /// Required fixed-header flags for non-PUBLISH packets (MQTT 5.0 Table 2-2).
    pub fn required_flags(self) -> Option<u8> {
        match self {
            Self::Subscribe | Self::Unsubscribe => Some(0x02), // bit 1 set
            Self::Publish => None,                             // flags are variable
            _ => Some(0x00),
        }
    }
}

// ---------------------------------------------------------------------------
// Reason codes (MQTT 5.0 subset)
// ---------------------------------------------------------------------------

#[allow(dead_code)]
pub mod reason {
    pub const SUCCESS: u8 = 0x00;
    pub const NORMAL_DISCONNECT: u8 = 0x00;
    pub const GRANTED_QOS_0: u8 = 0x00;
    pub const GRANTED_QOS_1: u8 = 0x01;
    pub const GRANTED_QOS_2: u8 = 0x02;
    pub const UNSPECIFIED_ERROR: u8 = 0x80;
    pub const MALFORMED_PACKET: u8 = 0x81;
    pub const PROTOCOL_ERROR: u8 = 0x82;
    pub const NOT_AUTHORIZED: u8 = 0x87;
    pub const TOPIC_FILTER_INVALID: u8 = 0x8F;
    pub const PACKET_ID_IN_USE: u8 = 0x91;
}

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub enum MqttError {
    Incomplete,
    MalformedPacket(String),
    ProtocolError(String),
    UnsupportedPacket(u8),
}

impl std::fmt::Display for MqttError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Incomplete => write!(f, "incomplete packet"),
            Self::MalformedPacket(s) => write!(f, "malformed: {}", s),
            Self::ProtocolError(s) => write!(f, "protocol error: {}", s),
            Self::UnsupportedPacket(t) => write!(f, "unsupported packet type {}", t),
        }
    }
}

type Result<T> = std::result::Result<T, MqttError>;

// ---------------------------------------------------------------------------
// Variable-byte integer encoding (MQTT §1.5.5)
// ---------------------------------------------------------------------------

pub fn encode_variable_byte_int(mut value: usize) -> Vec<u8> {
    let mut out = Vec::with_capacity(4);
    loop {
        let mut byte = (value & 0x7F) as u8;
        value >>= 7;
        if value > 0 {
            byte |= 0x80;
        }
        out.push(byte);
        if value == 0 {
            break;
        }
    }
    out
}

pub fn decode_variable_byte_int(buf: &[u8]) -> Result<(usize, usize)> {
    let mut value: usize = 0;
    let mut multiplier: usize = 1;
    for (i, &byte) in buf.iter().enumerate().take(4) {
        value += (byte as usize & 0x7F) * multiplier;
        if byte & 0x80 == 0 {
            return Ok((value, i + 1));
        }
        multiplier *= 128;
    }
    Err(MqttError::Incomplete)
}

// ---------------------------------------------------------------------------
// UTF-8 string helpers (MQTT §1.5.4)
// ---------------------------------------------------------------------------

fn decode_utf8(buf: &[u8], offset: usize) -> Result<(String, usize)> {
    if buf.len() < offset + 2 {
        return Err(MqttError::Incomplete);
    }
    let len = u16::from_be_bytes([buf[offset], buf[offset + 1]]) as usize;
    let end = offset + 2 + len;
    if buf.len() < end {
        return Err(MqttError::Incomplete);
    }
    let s = std::str::from_utf8(&buf[offset + 2..end])
        .map_err(|_| MqttError::MalformedPacket("invalid utf8".into()))?;
    Ok((s.to_string(), end))
}

fn encode_utf8(s: &str) -> Vec<u8> {
    let len = s.len() as u16;
    let mut out = Vec::with_capacity(2 + s.len());
    out.extend_from_slice(&len.to_be_bytes());
    out.extend_from_slice(s.as_bytes());
    out
}

fn decode_binary(buf: &[u8], offset: usize) -> Result<(Vec<u8>, usize)> {
    if buf.len() < offset + 2 {
        return Err(MqttError::Incomplete);
    }
    let len = u16::from_be_bytes([buf[offset], buf[offset + 1]]) as usize;
    let end = offset + 2 + len;
    if buf.len() < end {
        return Err(MqttError::Incomplete);
    }
    Ok((buf[offset + 2..end].to_vec(), end))
}

// ---------------------------------------------------------------------------
// Properties – we mostly skip them for current impl, but must parse to advance offset
// ---------------------------------------------------------------------------

fn skip_properties(buf: &[u8], offset: usize) -> Result<usize> {
    if offset >= buf.len() {
        return Ok(offset);
    }
    let (prop_len, consumed) = decode_variable_byte_int(&buf[offset..])?;
    let end = offset + consumed + prop_len;
    if buf.len() < end {
        return Err(MqttError::Incomplete);
    }
    Ok(end)
}

fn encode_empty_properties() -> Vec<u8> {
    vec![0x00] // property length = 0
}

// ---------------------------------------------------------------------------
// Fixed header parsing
// ---------------------------------------------------------------------------

/// Parse the fixed header. Returns `(packet_type, flags, remaining_length, header_size)`.
pub fn parse_fixed_header(buf: &[u8]) -> Result<(PacketType, u8, usize, usize)> {
    if buf.is_empty() {
        return Err(MqttError::Incomplete);
    }
    let type_val = buf[0] >> 4;
    let flags = buf[0] & 0x0F;
    let ptype = PacketType::from_u8(type_val).ok_or(MqttError::UnsupportedPacket(type_val))?;

    // Validate flags
    if let Some(required) = ptype.required_flags() {
        if flags != required {
            return Err(MqttError::MalformedPacket(format!(
                "{:?} flags must be 0x{:02x}, got 0x{:02x}",
                ptype, required, flags
            )));
        }
    }

    let (remaining_length, vbi_len) = decode_variable_byte_int(&buf[1..])?;
    let header_size = 1 + vbi_len;
    Ok((ptype, flags, remaining_length, header_size))
}

/// Returns the total bytes needed for this packet, or `Err(Incomplete)`.
pub fn packet_len(buf: &[u8]) -> Result<usize> {
    let (_, _, remaining, hdr_size) = parse_fixed_header(buf)?;
    let total = hdr_size + remaining;
    if buf.len() < total {
        Err(MqttError::Incomplete)
    } else {
        Ok(total)
    }
}

// ---------------------------------------------------------------------------
// Parsed inbound packets
// ---------------------------------------------------------------------------

#[derive(Debug)]
#[allow(dead_code)]
pub struct ConnectPacket {
    pub client_id: String,
    pub clean_start: bool,
    pub keep_alive: u16,
    pub protocol_version: u8,
}

#[derive(Debug)]
pub struct SubscribeRequest {
    pub packet_id: u16,
    pub topics: Vec<(String, u8)>, // (topic_filter, requested_qos)
}

#[derive(Debug)]
pub struct UnsubscribeRequest {
    pub packet_id: u16,
    pub topics: Vec<String>,
}

#[derive(Debug)]
#[allow(dead_code)]
pub struct PublishPacket {
    pub topic: String,
    pub payload: Vec<u8>,
    pub qos: u8,
    pub dup: bool,
    pub retain: bool,
    pub packet_id: Option<u16>,
}

#[derive(Debug)]
pub enum InboundPacket {
    Connect(ConnectPacket),
    Publish(PublishPacket),
    Puback(u16), // packet_id
    Subscribe(SubscribeRequest),
    Unsubscribe(UnsubscribeRequest),
    Pingreq,
    Disconnect,
}

// ---------------------------------------------------------------------------
// Inbound packet parsing
// ---------------------------------------------------------------------------

pub fn parse_packet(buf: &[u8]) -> Result<(InboundPacket, usize)> {
    let (ptype, flags, remaining, hdr_size) = parse_fixed_header(buf)?;
    let total = hdr_size + remaining;
    if buf.len() < total {
        return Err(MqttError::Incomplete);
    }

    let payload = &buf[hdr_size..total];

    let pkt = match ptype {
        PacketType::Connect => InboundPacket::Connect(parse_connect(payload)?),
        PacketType::Publish => InboundPacket::Publish(parse_publish(payload, flags)?),
        PacketType::Puback => InboundPacket::Puback(parse_puback(payload)?),
        PacketType::Subscribe => InboundPacket::Subscribe(parse_subscribe(payload)?),
        PacketType::Unsubscribe => InboundPacket::Unsubscribe(parse_unsubscribe(payload)?),
        PacketType::Pingreq => InboundPacket::Pingreq,
        PacketType::Disconnect => InboundPacket::Disconnect,
        other => return Err(MqttError::UnsupportedPacket(other as u8)),
    };

    Ok((pkt, total))
}

fn parse_connect(buf: &[u8]) -> Result<ConnectPacket> {
    // Protocol name
    let (proto_name, off) = decode_utf8(buf, 0)?;
    if proto_name != "MQTT" {
        return Err(MqttError::ProtocolError(format!(
            "expected protocol 'MQTT', got '{}'",
            proto_name
        )));
    }

    if buf.len() < off + 4 {
        return Err(MqttError::Incomplete);
    }

    let protocol_version = buf[off];
    if protocol_version != 5 {
        return Err(MqttError::ProtocolError(format!(
            "only MQTT 5.0 supported, got version {}",
            protocol_version
        )));
    }

    let connect_flags = buf[off + 1];
    let clean_start = (connect_flags & 0x02) != 0;
    let has_will = (connect_flags & 0x04) != 0;
    let _will_qos = (connect_flags >> 3) & 0x03;
    let _will_retain = (connect_flags & 0x20) != 0;
    let has_password = (connect_flags & 0x40) != 0;
    let has_username = (connect_flags & 0x80) != 0;

    let keep_alive = u16::from_be_bytes([buf[off + 2], buf[off + 3]]);

    // Skip CONNECT properties
    let mut off = skip_properties(buf, off + 4)?;

    // Client ID
    let (client_id, new_off) = decode_utf8(buf, off)?;
    off = new_off;

    // Skip will properties + will topic + will payload if present
    if has_will {
        off = skip_properties(buf, off)?;
        let (_will_topic, new_off) = decode_utf8(buf, off)?;
        off = new_off;
        let (_will_payload, new_off) = decode_binary(buf, off)?;
        off = new_off;
    }

    // Skip username/password if present
    if has_username {
        let (_, new_off) = decode_utf8(buf, off)?;
        off = new_off;
    }
    if has_password {
        let (_, _new_off) = decode_binary(buf, off)?;
    }

    Ok(ConnectPacket {
        client_id,
        clean_start,
        keep_alive,
        protocol_version,
    })
}

fn parse_publish(buf: &[u8], flags: u8) -> Result<PublishPacket> {
    let dup = (flags & 0x08) != 0;
    let qos = (flags >> 1) & 0x03;
    let retain = (flags & 0x01) != 0;

    let (topic, mut off) = decode_utf8(buf, 0)?;

    let packet_id = if qos > 0 {
        if buf.len() < off + 2 {
            return Err(MqttError::Incomplete);
        }
        let id = u16::from_be_bytes([buf[off], buf[off + 1]]);
        off += 2;
        Some(id)
    } else {
        None
    };

    // Skip properties
    off = skip_properties(buf, off)?;

    let payload = buf[off..].to_vec();

    Ok(PublishPacket {
        topic,
        payload,
        qos,
        dup,
        retain,
        packet_id,
    })
}

fn parse_subscribe(buf: &[u8]) -> Result<SubscribeRequest> {
    if buf.len() < 2 {
        return Err(MqttError::Incomplete);
    }
    let packet_id = u16::from_be_bytes([buf[0], buf[1]]);
    let mut off = 2;

    // Skip properties
    off = skip_properties(buf, off)?;

    let mut topics = Vec::new();
    while off < buf.len() {
        let (topic_filter, new_off) = decode_utf8(buf, off)?;
        off = new_off;
        if off >= buf.len() {
            return Err(MqttError::Incomplete);
        }
        let sub_options = buf[off];
        let qos = sub_options & 0x03;
        off += 1;
        topics.push((topic_filter, qos));
    }

    if topics.is_empty() {
        return Err(MqttError::ProtocolError(
            "SUBSCRIBE must have at least one topic".into(),
        ));
    }

    Ok(SubscribeRequest { packet_id, topics })
}

fn parse_unsubscribe(buf: &[u8]) -> Result<UnsubscribeRequest> {
    if buf.len() < 2 {
        return Err(MqttError::Incomplete);
    }
    let packet_id = u16::from_be_bytes([buf[0], buf[1]]);
    let mut off = 2;

    // Skip properties
    off = skip_properties(buf, off)?;

    let mut topics = Vec::new();
    while off < buf.len() {
        let (topic_filter, new_off) = decode_utf8(buf, off)?;
        off = new_off;
        topics.push(topic_filter);
    }

    if topics.is_empty() {
        return Err(MqttError::ProtocolError(
            "UNSUBSCRIBE must have at least one topic".into(),
        ));
    }

    Ok(UnsubscribeRequest { packet_id, topics })
}

fn parse_puback(buf: &[u8]) -> Result<u16> {
    if buf.len() < 2 {
        return Err(MqttError::Incomplete);
    }
    let packet_id = u16::from_be_bytes([buf[0], buf[1]]);
    // MQTT 5.0 PUBACK might have a reason code and properties, but we skip them for current impl
    Ok(packet_id)
}

// ---------------------------------------------------------------------------
// Outbound packet building
// ---------------------------------------------------------------------------

fn build_packet(ptype: PacketType, flags: u8, variable_header_and_payload: &[u8]) -> Vec<u8> {
    let first_byte = ((ptype as u8) << 4) | (flags & 0x0F);
    let remaining = encode_variable_byte_int(variable_header_and_payload.len());
    let mut pkt = Vec::with_capacity(1 + remaining.len() + variable_header_and_payload.len());
    pkt.push(first_byte);
    pkt.extend_from_slice(&remaining);
    pkt.extend_from_slice(variable_header_and_payload);
    pkt
}

pub fn build_connack(session_present: bool, reason_code: u8) -> Vec<u8> {
    let mut vh = Vec::with_capacity(3);
    vh.push(if session_present { 0x01 } else { 0x00 }); // connect ack flags
    vh.push(reason_code);
    vh.extend_from_slice(&encode_empty_properties());
    build_packet(PacketType::Connack, 0x00, &vh)
}

pub fn build_suback(packet_id: u16, reason_codes: &[u8]) -> Vec<u8> {
    let mut vh = Vec::with_capacity(2 + 1 + reason_codes.len());
    vh.extend_from_slice(&packet_id.to_be_bytes());
    vh.extend_from_slice(&encode_empty_properties());
    vh.extend_from_slice(reason_codes);
    build_packet(PacketType::Suback, 0x00, &vh)
}

pub fn build_unsuback(packet_id: u16, reason_codes: &[u8]) -> Vec<u8> {
    let mut vh = Vec::with_capacity(2 + 1 + reason_codes.len());
    vh.extend_from_slice(&packet_id.to_be_bytes());
    vh.extend_from_slice(&encode_empty_properties());
    vh.extend_from_slice(reason_codes);
    build_packet(PacketType::Unsuback, 0x00, &vh)
}

pub fn build_publish_qos0(topic: &str, payload: &[u8]) -> Vec<u8> {
    let mut vh = Vec::new();
    vh.extend_from_slice(&encode_utf8(topic));
    // No packet ID for QoS 0
    vh.extend_from_slice(&encode_empty_properties());
    vh.extend_from_slice(payload);
    build_packet(PacketType::Publish, 0x00, &vh) // flags: DUP=0 QoS=00 RETAIN=0
}

pub fn build_publish_qos1(topic: &str, payload: &[u8], packet_id: u16) -> Vec<u8> {
    let mut vh = Vec::new();
    vh.extend_from_slice(&encode_utf8(topic));
    vh.extend_from_slice(&packet_id.to_be_bytes());
    vh.extend_from_slice(&encode_empty_properties());
    vh.extend_from_slice(payload);
    build_packet(PacketType::Publish, 0x02, &vh) // flags: DUP=0 QoS=01 RETAIN=0
}

pub fn build_publish_qos1_dup(topic: &str, payload: &[u8], packet_id: u16) -> Vec<u8> {
    let mut vh = Vec::new();
    vh.extend_from_slice(&encode_utf8(topic));
    vh.extend_from_slice(&packet_id.to_be_bytes());
    vh.extend_from_slice(&encode_empty_properties());
    vh.extend_from_slice(payload);
    build_packet(PacketType::Publish, 0x0A, &vh) // flags: DUP=1 QoS=01 RETAIN=0 (0x08 | 0x02)
}

pub fn build_puback(packet_id: u16) -> Vec<u8> {
    let mut vh = Vec::with_capacity(3); // packet_id (2) + reason code (0) or props
    vh.extend_from_slice(&packet_id.to_be_bytes());
    // In MQTT 5.0, if Reason Code is Success (0x00) and no properties, we can omit them.
    // Table 3-5: The Reason Code and Property Length can be omitted if the Reason Code is 0x00 and there are no Properties.
    build_packet(PacketType::Puback, 0x00, &vh)
}

pub fn build_pingresp() -> Vec<u8> {
    build_packet(PacketType::Pingresp, 0x00, &[])
}

pub fn build_disconnect(reason_code: u8) -> Vec<u8> {
    let mut vh = Vec::with_capacity(2);
    vh.push(reason_code);
    vh.extend_from_slice(&encode_empty_properties());
    build_packet(PacketType::Disconnect, 0x00, &vh)
}

// ---------------------------------------------------------------------------
// Topic filter matching (MQTT §4.7)
// ---------------------------------------------------------------------------

/// Check if a concrete topic matches an MQTT topic filter.
///
/// Rules:
/// - `+` matches exactly one level
/// - `#` matches zero or more levels (must be last)
/// - `$`-prefixed topics don't match wildcard filters starting with `#` or `+`
pub fn topic_matches_filter(topic: &str, filter: &str) -> bool {
    // $-topics must not match filters starting with wildcard
    if topic.starts_with('$') {
        if filter.starts_with('+') || filter.starts_with('#') {
            return false;
        }
    }

    let topic_levels: Vec<&str> = topic.split('/').collect();
    let filter_levels: Vec<&str> = filter.split('/').collect();

    let mut ti = 0;
    for (fi, fpart) in filter_levels.iter().enumerate() {
        match *fpart {
            "#" => {
                // # must be last level
                return fi == filter_levels.len() - 1;
            }
            "+" => {
                if ti >= topic_levels.len() {
                    return false;
                }
                ti += 1;
            }
            exact => {
                if ti >= topic_levels.len() || topic_levels[ti] != exact {
                    return false;
                }
                ti += 1;
            }
        }
    }

    ti == topic_levels.len()
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_variable_byte_int_roundtrip() {
        for val in [0, 1, 127, 128, 16383, 16384, 2_097_151, 268_435_455] {
            let encoded = encode_variable_byte_int(val);
            let (decoded, consumed) = decode_variable_byte_int(&encoded).unwrap();
            assert_eq!(decoded, val);
            assert_eq!(consumed, encoded.len());
        }
    }

    #[test]
    fn test_topic_matching() {
        assert!(topic_matches_filter("a/b/c", "a/b/c"));
        assert!(topic_matches_filter("a/b/c", "a/+/c"));
        assert!(topic_matches_filter("a/b/c", "a/#"));
        assert!(topic_matches_filter("a/b/c", "#"));
        assert!(topic_matches_filter("a", "a"));
        assert!(topic_matches_filter("a/b", "+/+"));
        assert!(!topic_matches_filter("a/b/c", "a/b"));
        assert!(!topic_matches_filter("a/b", "a/b/c"));
        assert!(!topic_matches_filter("$SYS/info", "#"));
        assert!(!topic_matches_filter("$SYS/info", "+/info"));
        assert!(topic_matches_filter("$SYS/info", "$SYS/info"));
        assert!(topic_matches_filter("$SYS/info", "$SYS/#"));
    }

    #[test]
    fn test_build_connack_roundtrip() {
        let pkt = build_connack(false, reason::SUCCESS);
        let (ptype, _flags, remaining, hdr_size) = parse_fixed_header(&pkt).unwrap();
        assert_eq!(ptype, PacketType::Connack);
        assert_eq!(remaining + hdr_size, pkt.len());
    }

    #[test]
    fn test_build_publish_qos0() {
        let pkt = build_publish_qos0("test/topic", b"hello");
        let (ptype, flags, _remaining, _hdr_size) = parse_fixed_header(&pkt).unwrap();
        assert_eq!(ptype, PacketType::Publish);
        assert_eq!(flags, 0x00); // QoS 0, no DUP, no RETAIN
    }

    #[test]
    fn test_build_pingresp() {
        let pkt = build_pingresp();
        assert_eq!(pkt, vec![0xD0, 0x00]);
    }
}
