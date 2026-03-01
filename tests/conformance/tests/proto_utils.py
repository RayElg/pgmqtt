import struct
import os

# Broker configuration from environment variables
MQTT_HOST = os.environ.get('MQTT_HOST', 'localhost')
MQTT_PORT = int(os.environ.get('MQTT_PORT', '1883'))

def get_broker_config():
    """Return (host, port) tuple for broker connection."""
    return MQTT_HOST, MQTT_PORT

def encode_variable_byte_integer(value):
    res = bytearray()
    while True:
        digit = value % 128
        value //= 128
        if value > 0:
            digit |= 0x80
        res.append(digit)
        if value <= 0:
            break
    return res

def decode_variable_byte_integer(buffer, offset=0):
    multiplier = 1
    value = 0
    while True:
        encoded_byte = buffer[offset]
        value += (encoded_byte & 127) * multiplier
        if multiplier > 128*128*128:
            raise Exception("Malformed variable byte integer")
        multiplier *= 128
        offset += 1
        if (encoded_byte & 128) == 0:
            break
    return value, offset

def encode_utf8_string(s):
    encoded = s.encode('utf-8')
    return struct.pack('!H', len(encoded)) + encoded

def decode_utf8_string(buffer, offset=0):
    length = struct.unpack_from('!H', buffer, offset)[0]
    offset += 2
    s = buffer[offset:offset+length].decode('utf-8')
    offset += length
    return s, offset

class MQTTControlPacket:
    CONNECT = 1
    CONNACK = 2
    PUBLISH = 3
    PUBACK = 4
    PUBREC = 5
    PUBREL = 6
    PUBCOMP = 7
    SUBSCRIBE = 8
    SUBACK = 9
    UNSUBSCRIBE = 10
    UNSUBACK = 11
    PINGREQ = 12
    PINGRESP = 13
    DISCONNECT = 14
    AUTH = 15

# MQTT 5.0 Reason Codes per Table 2-6
class ReasonCode:
    SUCCESS = 0x00
    NORMAL_DISCONNECTION = 0x00
    GRANTED_QOS_0 = 0x00
    GRANTED_QOS_1 = 0x01
    GRANTED_QOS_2 = 0x02
    DISCONNECT_WITH_WILL = 0x04
    NO_MATCHING_SUBSCRIBERS = 0x10
    NO_SUBSCRIPTION_EXISTED = 0x11
    CONTINUE_AUTH = 0x18
    REAUTHENTICATE = 0x19
    UNSPECIFIED_ERROR = 0x80
    MALFORMED_PACKET = 0x81
    PROTOCOL_ERROR = 0x82
    IMPLEMENTATION_SPECIFIC_ERROR = 0x83
    UNSUPPORTED_PROTOCOL_VERSION = 0x84
    CLIENT_ID_NOT_VALID = 0x85
    BAD_USERNAME_PASSWORD = 0x86
    NOT_AUTHORIZED = 0x87
    SERVER_UNAVAILABLE = 0x88
    SERVER_BUSY = 0x89
    BANNED = 0x8A
    SERVER_SHUTTING_DOWN = 0x8B
    BAD_AUTH_METHOD = 0x8C
    KEEP_ALIVE_TIMEOUT = 0x8D
    SESSION_TAKEN_OVER = 0x8E
    TOPIC_FILTER_INVALID = 0x8F
    TOPIC_NAME_INVALID = 0x90
    PACKET_ID_IN_USE = 0x91
    PACKET_ID_NOT_FOUND = 0x92
    RECEIVE_MAXIMUM_EXCEEDED = 0x93
    TOPIC_ALIAS_INVALID = 0x94
    PACKET_TOO_LARGE = 0x95
    MESSAGE_RATE_TOO_HIGH = 0x96
    QUOTA_EXCEEDED = 0x97
    ADMINISTRATIVE_ACTION = 0x98
    PAYLOAD_FORMAT_INVALID = 0x99
    RETAIN_NOT_SUPPORTED = 0x9A
    QOS_NOT_SUPPORTED = 0x9B
    USE_ANOTHER_SERVER = 0x9C
    SERVER_MOVED = 0x9D
    SHARED_SUBS_NOT_SUPPORTED = 0x9E
    CONNECTION_RATE_EXCEEDED = 0x9F
    MAX_CONNECT_TIME = 0xA0
    SUBSCRIPTION_IDS_NOT_SUPPORTED = 0xA1
    WILDCARD_SUBS_NOT_SUPPORTED = 0xA2

# Expected fixed header flags (bits 3-0) per MQTT 5.0 Table 2-2
EXPECTED_FLAGS = {
    MQTTControlPacket.CONNECT: 0x00,
    MQTTControlPacket.CONNACK: 0x00,
    # PUBLISH flags are variable (DUP, QoS, RETAIN)
    MQTTControlPacket.PUBACK: 0x00,
    MQTTControlPacket.PUBREC: 0x00,
    MQTTControlPacket.PUBREL: 0x02,  # Reserved bits must be 0010
    MQTTControlPacket.PUBCOMP: 0x00,
    MQTTControlPacket.SUBSCRIBE: 0x02,  # Reserved bits must be 0010
    MQTTControlPacket.SUBACK: 0x00,
    MQTTControlPacket.UNSUBSCRIBE: 0x02,  # Reserved bits must be 0010
    MQTTControlPacket.UNSUBACK: 0x00,
    MQTTControlPacket.PINGREQ: 0x00,
    MQTTControlPacket.PINGRESP: 0x00,
    MQTTControlPacket.DISCONNECT: 0x00,
    MQTTControlPacket.AUTH: 0x00,
}

def validate_fixed_header(packet, expected_type, check_flags=True):
    """
    Validate packet type and fixed header flags per MQTT 5.0 Table 2-2.
    Returns (packet_type, flags, remaining_length, offset) or raises AssertionError.
    """
    assert packet and len(packet) >= 2, "Packet too short"
    packet_type = (packet[0] & 0xF0) >> 4
    flags = packet[0] & 0x0F
    
    assert packet_type == expected_type, \
        f"Expected packet type {expected_type}, got {packet_type}"
    
    if check_flags and expected_type != MQTTControlPacket.PUBLISH:
        expected_flags = EXPECTED_FLAGS.get(expected_type, 0x00)
        assert flags == expected_flags, \
            f"Invalid flags for packet type {expected_type}: expected 0x{expected_flags:02x}, got 0x{flags:02x}"
    
    remaining_length, offset = decode_variable_byte_integer(packet, 1)
    return packet_type, flags, remaining_length, offset

def validate_connack(packet):
    """
    Validate CONNACK packet per spec.
    Returns (session_present, reason_code, properties).
    """
    packet_type, flags, remain_len, offset = validate_fixed_header(packet, MQTTControlPacket.CONNACK)
    
    # Byte 1 of variable header: Connect Acknowledge Flags
    ack_flags = packet[offset]
    session_present = bool(ack_flags & 0x01)
    assert (ack_flags & 0xFE) == 0, "Reserved bits in CONNACK flags must be 0"
    offset += 1
    
    # Byte 2: Reason Code
    reason_code = packet[offset]
    offset += 1
    
    # Properties
    props, offset = decode_properties(packet, offset)
    
    return session_present, reason_code, props

def validate_suback(packet, expected_packet_id):
    """
    Validate SUBACK packet per spec.
    Returns list of reason codes (granted QoS or error codes).
    """
    packet_type, flags, remain_len, offset = validate_fixed_header(packet, MQTTControlPacket.SUBACK)
    
    # Packet Identifier
    packet_id = struct.unpack_from('!H', packet, offset)[0]
    assert packet_id == expected_packet_id, \
        f"SUBACK packet ID mismatch: expected {expected_packet_id}, got {packet_id}"
    offset += 2
    
    # Properties
    props, offset = decode_properties(packet, offset)
    
    # Payload: list of reason codes
    header_len = 1 + len(encode_variable_byte_integer(remain_len))
    end = header_len + remain_len
    reason_codes = list(packet[offset:end])
    
    return reason_codes

def validate_unsuback(packet, expected_packet_id):
    """
    Validate UNSUBACK packet per spec.
    Returns list of reason codes.
    """
    packet_type, flags, remain_len, offset = validate_fixed_header(packet, MQTTControlPacket.UNSUBACK)
    
    packet_id = struct.unpack_from('!H', packet, offset)[0]
    assert packet_id == expected_packet_id, \
        f"UNSUBACK packet ID mismatch: expected {expected_packet_id}, got {packet_id}"
    offset += 2
    
    props, offset = decode_properties(packet, offset)
    
    header_len = 1 + len(encode_variable_byte_integer(remain_len))
    end = header_len + remain_len
    reason_codes = list(packet[offset:end])
    
    return reason_codes

def validate_puback(packet, expected_packet_id):
    """
    Validate PUBACK packet per spec.
    Returns reason_code.
    """
    packet_type, flags, remain_len, offset = validate_fixed_header(packet, MQTTControlPacket.PUBACK)
    
    packet_id = struct.unpack_from('!H', packet, offset)[0]
    assert packet_id == expected_packet_id, \
        f"PUBACK packet ID mismatch: expected {expected_packet_id}, got {packet_id}"
    offset += 2
    
    # Reason code is optional if remain_len == 2
    if remain_len > 2:
        reason_code = packet[offset]
    else:
        reason_code = ReasonCode.SUCCESS
    
    return reason_code

def validate_pubrec(packet, expected_packet_id):
    """Validate PUBREC packet. Returns reason_code."""
    packet_type, flags, remain_len, offset = validate_fixed_header(packet, MQTTControlPacket.PUBREC)
    
    packet_id = struct.unpack_from('!H', packet, offset)[0]
    assert packet_id == expected_packet_id, \
        f"PUBREC packet ID mismatch: expected {expected_packet_id}, got {packet_id}"
    offset += 2
    
    reason_code = packet[offset] if remain_len > 2 else ReasonCode.SUCCESS
    return reason_code

def validate_pubrel(packet, expected_packet_id):
    """Validate PUBREL packet. Returns reason_code."""
    packet_type, flags, remain_len, offset = validate_fixed_header(packet, MQTTControlPacket.PUBREL)
    
    packet_id = struct.unpack_from('!H', packet, offset)[0]
    assert packet_id == expected_packet_id, \
        f"PUBREL packet ID mismatch: expected {expected_packet_id}, got {packet_id}"
    offset += 2
    
    reason_code = packet[offset] if remain_len > 2 else ReasonCode.SUCCESS
    return reason_code

def validate_pubcomp(packet, expected_packet_id):
    """Validate PUBCOMP packet. Returns reason_code."""
    packet_type, flags, remain_len, offset = validate_fixed_header(packet, MQTTControlPacket.PUBCOMP)
    
    packet_id = struct.unpack_from('!H', packet, offset)[0]
    assert packet_id == expected_packet_id, \
        f"PUBCOMP packet ID mismatch: expected {expected_packet_id}, got {packet_id}"
    offset += 2
    
    reason_code = packet[offset] if remain_len > 2 else ReasonCode.SUCCESS
    return reason_code

def validate_publish(packet):
    """
    Validate PUBLISH packet per spec.
    Returns (topic, payload, qos, dup, retain, packet_id, properties).
    packet_id is None for QoS 0.
    """
    assert packet and len(packet) >= 2, "Packet too short"
    packet_type = (packet[0] & 0xF0) >> 4
    flags = packet[0] & 0x0F
    
    assert packet_type == MQTTControlPacket.PUBLISH, \
        f"Expected PUBLISH, got packet type {packet_type}"
    
    dup = bool(flags & 0x08)
    qos = (flags & 0x06) >> 1
    retain = bool(flags & 0x01)
    
    assert qos in (0, 1, 2), f"Invalid QoS value: {qos}"
    
    # DUP must be 0 for QoS 0
    if qos == 0:
        assert not dup, "DUP flag must be 0 for QoS 0 PUBLISH"
    
    remain_len, offset = decode_variable_byte_integer(packet, 1)
    header_len = offset
    
    # Topic Name
    topic, offset = decode_utf8_string(packet, offset)
    
    # Packet Identifier (only for QoS > 0)
    packet_id = None
    if qos > 0:
        packet_id = struct.unpack_from('!H', packet, offset)[0]
        offset += 2
    
    # Properties
    props, offset = decode_properties(packet, offset)
    
    # Payload
    end = header_len + remain_len
    payload = packet[offset:end]
    
    return topic, payload, qos, dup, retain, packet_id, props

def validate_pingresp(packet):
    """Validate PINGRESP packet (no variable header or payload)."""
    packet_type, flags, remain_len, offset = validate_fixed_header(packet, MQTTControlPacket.PINGRESP)
    assert remain_len == 0, f"PINGRESP remaining length must be 0, got {remain_len}"
    # PINGRESP has no variable header or payload
    return True

def validate_disconnect(packet):
    """
    Validate DISCONNECT packet per spec.
    Returns reason_code.
    """
    packet_type, flags, remain_len, offset = validate_fixed_header(packet, MQTTControlPacket.DISCONNECT)
    
    # Reason code is optional if remain_len == 0
    if remain_len == 0:
        return ReasonCode.NORMAL_DISCONNECTION
    
    reason_code = packet[offset]
    return reason_code

def parse_fixed_header(buffer):
    if len(buffer) < 2:
        return None, None, None, None
    packet_type = (buffer[0] & 0xF0) >> 4
    flags = buffer[0] & 0x0F
    try:
        remaining_length, offset = decode_variable_byte_integer(buffer, 1)
        return packet_type, flags, remaining_length, offset
    except IndexError:
        return None, None, None, None

def encode_properties(props_dict):
    # Very basic property encoder for common types
    # props_dict = { property_id: value }
    res = bytearray()
    for prop_id, value in props_dict.items():
        res.append(prop_id)
        if prop_id in [0x0B, 0x1F, 0x03, 0x08]: # VarInt or UTF-8 String or String Pair
            if isinstance(value, int):
                res.extend(encode_variable_byte_integer(value))
            elif isinstance(value, str):
                res.extend(encode_utf8_string(value))
        elif prop_id in [0x02, 0x11]: # 4-byte integer (Message Expiry, Session Expiry)
             res.extend(struct.pack('!I', value))
        elif prop_id in [0x21, 0x23]: # 2-byte integer (Receive Max, Topic Alias)
             res.extend(struct.pack('!H', value))
        elif prop_id == 0x01: # 1-byte
            res.append(value)
    
    return encode_variable_byte_integer(len(res)) + res

def decode_properties(buffer, offset):
    prop_len, offset = decode_variable_byte_integer(buffer, offset)
    end = offset + prop_len
    props = {}
    while offset < end:
        prop_id = buffer[offset]
        offset += 1
        # Simplified decoding logic
        if prop_id == 0x0B: # Subscription Identifier (VarInt)
            val, offset = decode_variable_byte_integer(buffer, offset)
            props[prop_id] = val
        elif prop_id in [0x1F, 0x03, 0x08]: # Reason String, Content Type, Response Topic (UTF-8)
            val, offset = decode_utf8_string(buffer, offset)
            props[prop_id] = val
        elif prop_id == 0x26: # User Property (String Pair)
            key, offset = decode_utf8_string(buffer, offset)
            val, offset = decode_utf8_string(buffer, offset)
            if prop_id not in props: props[prop_id] = []
            props[prop_id].append((key, val))
        elif prop_id in [0x02, 0x11]: # 4-byte (Message Expiry, Session Expiry)
            props[prop_id] = struct.unpack_from('!I', buffer, offset)[0]
            offset += 4
        elif prop_id in [0x21, 0x23]: # 2-byte (Receive Max, Topic Alias)
            props[prop_id] = struct.unpack_from('!H', buffer, offset)[0]
            offset += 2
        elif prop_id == 0x01: # 1-byte (Payload Format)
            props[prop_id] = buffer[offset]
            offset += 1
        else:
            # Fallback or Skip unknown
            break
    return props, offset

import socket
def recv_packet(s, timeout=5.0):
    s.settimeout(timeout)
    try:
        data = s.recv(1)
        if not data:
            return None
        header = data
        multiplier = 1
        remain_len = 0
        while True:
            b = s.recv(1)
            header += b
            digit = b[0]
            remain_len += (digit & 127) * multiplier
            if (digit & 128) == 0:
                break
            multiplier *= 128
        payload = b''
        while len(payload) < remain_len:
            chunk = s.recv(remain_len - len(payload))
            if not chunk:
                break
            payload += chunk
        return header + payload
    except socket.timeout:
        return None
    except Exception:
        return None

def create_connect_packet(client_id, clean_start=True, properties=None, keep_alive=60,
                           will_topic=None, will_payload=None, will_qos=0, will_retain=False,
                           will_properties=None):
    """
    Create MQTT 5.0 CONNECT packet with optional Will Message support.
    """
    protocol_name = encode_utf8_string("MQTT")
    protocol_version = b'\x05'
    
    flags = 0x00
    if clean_start:
        flags |= 0x02
    
    # Will Message
    will_payload_bytes = b''
    if will_topic is not None:
        flags |= 0x04  # Will Flag
        flags |= (will_qos << 3)  # Will QoS
        if will_retain:
            flags |= 0x20  # Will Retain
        
        will_prop_bytes = encode_properties(will_properties if will_properties else {})
        will_payload_bytes = will_prop_bytes + encode_utf8_string(will_topic)
        if will_payload:
            will_payload_bytes += struct.pack('!H', len(will_payload)) + will_payload
        else:
            will_payload_bytes += struct.pack('!H', 0)
    
    connect_flags = bytes([flags])
    keep_alive_bytes = struct.pack('!H', keep_alive)
    
    prop_bytes = encode_properties(properties if properties else {})
    payload = encode_utf8_string(client_id) + will_payload_bytes
    variable_header = protocol_name + protocol_version + connect_flags + keep_alive_bytes + prop_bytes
    remaining_length = len(variable_header) + len(payload)
    fixed_header = bytes([(MQTTControlPacket.CONNECT << 4)]) + encode_variable_byte_integer(remaining_length)
    return fixed_header + variable_header + payload

def create_subscribe_packet(packet_id, topic, qos=0, properties=None):
    prop_bytes = encode_properties(properties if properties else {})
    payload = struct.pack('!H', packet_id) + prop_bytes + encode_utf8_string(topic) + bytes([qos])
    remaining_length = len(payload)
    fixed_header = bytes([(MQTTControlPacket.SUBSCRIBE << 4) | 0x02]) + encode_variable_byte_integer(remaining_length)
    return fixed_header + payload

def create_unsubscribe_packet(packet_id, topic, properties=None):
    """Create MQTT 5.0 UNSUBSCRIBE packet."""
    prop_bytes = encode_properties(properties if properties else {})
    payload = struct.pack('!H', packet_id) + prop_bytes + encode_utf8_string(topic)
    remaining_length = len(payload)
    # UNSUBSCRIBE fixed header flags must be 0010
    fixed_header = bytes([(MQTTControlPacket.UNSUBSCRIBE << 4) | 0x02]) + encode_variable_byte_integer(remaining_length)
    return fixed_header + payload

def create_publish_packet(topic, payload, qos=0, packet_id=None, properties=None, dup=False, retain=False):
    flags = (qos << 1)
    if dup: flags |= 0x08
    if retain: flags |= 0x01
    
    prop_bytes = encode_properties(properties if properties else {})
    var_header = encode_utf8_string(topic)
    if qos > 0:
        if packet_id is None: raise ValueError("Packet ID required for QoS > 0")
        var_header += struct.pack('!H', packet_id)
    var_header += prop_bytes
    
    remaining_length = len(var_header) + len(payload)
    fixed_header = bytes([(MQTTControlPacket.PUBLISH << 4) | flags]) + \
                   encode_variable_byte_integer(remaining_length)
    return fixed_header + var_header + payload

def create_puback_packet(packet_id, reason_code=0, properties=None):
    prop_bytes = encode_properties(properties if properties else {})
    var_header = struct.pack('!H', packet_id)
    if reason_code != 0 or prop_bytes != b'\x00':
        var_header += bytes([reason_code]) + prop_bytes
    
    return bytes([(MQTTControlPacket.PUBACK << 4)]) + encode_variable_byte_integer(len(var_header)) + var_header

def create_pingreq_packet():
    """Create MQTT 5.0 PINGREQ packet."""
    # PINGREQ has no variable header or payload
    return bytes([(MQTTControlPacket.PINGREQ << 4), 0x00])

def create_disconnect_packet(reason_code=0, properties=None):
    """Create MQTT 5.0 DISCONNECT packet."""
    if reason_code == 0 and not properties:
        # Short form: no variable header
        return bytes([(MQTTControlPacket.DISCONNECT << 4), 0x00])
    
    prop_bytes = encode_properties(properties if properties else {})
    var_header = bytes([reason_code]) + prop_bytes
    return bytes([(MQTTControlPacket.DISCONNECT << 4)]) + encode_variable_byte_integer(len(var_header)) + var_header

def create_pubrec_packet(packet_id, reason_code=0):
    """Create MQTT 5.0 PUBREC packet."""
    var_header = struct.pack('!H', packet_id)
    if reason_code != 0:
        var_header += bytes([reason_code])
    return bytes([(MQTTControlPacket.PUBREC << 4)]) + encode_variable_byte_integer(len(var_header)) + var_header

def create_pubrel_packet(packet_id, reason_code=0):
    """Create MQTT 5.0 PUBREL packet. Note: flags must be 0x02."""
    var_header = struct.pack('!H', packet_id)
    if reason_code != 0:
        var_header += bytes([reason_code])
    return bytes([(MQTTControlPacket.PUBREL << 4) | 0x02]) + encode_variable_byte_integer(len(var_header)) + var_header

def create_pubcomp_packet(packet_id, reason_code=0):
    """Create MQTT 5.0 PUBCOMP packet."""
    var_header = struct.pack('!H', packet_id)
    if reason_code != 0:
        var_header += bytes([reason_code])
    return bytes([(MQTTControlPacket.PUBCOMP << 4)]) + encode_variable_byte_integer(len(var_header)) + var_header
