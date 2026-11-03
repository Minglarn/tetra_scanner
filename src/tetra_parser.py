#!/usr/bin/env python3
import os
import sys
import json
import time
import socket
import select
import signal
import re
from datetime import datetime, timezone

try:
    import paho.mqtt.client as mqtt
except ImportError:
    mqtt = None


APP_VERSION = os.environ.get("APP_VERSION", "2026.11.03.4")

TETRA_CHANNELS_STR = os.environ.get(
    "TETRA_CHANNELS",
    "427000000:71,427300000:17,427550000:23,427850000:1,427950000:16,428500000:7",
)

MQTT_ENABLED = os.environ.get("MQTT_ENABLED", "true").lower() in ("true", "1", "yes")
MQTT_HOST = os.environ.get("MQTT_HOST", "192.168.1.121")
MQTT_PORT = int(os.environ.get("MQTT_PORT", "1883"))
MQTT_USER = os.environ.get("MQTT_USER", "home-assistant-server")
MQTT_PASS = os.environ.get("MQTT_PASS", "bytmig")
MQTT_BASE_TOPIC = os.environ.get("MQTT_BASE_TOPIC", "tetra/240_11").rstrip("/")
HA_DISCOVERY_ENABLED = os.environ.get("HA_DISCOVERY_ENABLED", "true").lower() in (
    "true",
    "1",
    "yes",
)
HA_DISCOVERY_PREFIX = os.environ.get("HA_DISCOVERY_PREFIX", "homeassistant").rstrip("/")

UDP_OUT_ENABLED = os.environ.get("UDP_OUT_ENABLED", "false").lower() in (
    "true",
    "1",
    "yes",
)
UDP_OUT_TARGETS_STR = os.environ.get("UDP_OUT_TARGETS", "192.168.1.10:5555")
UDP_OUT_INCLUDE_RAW = os.environ.get("UDP_OUT_INCLUDE_RAW", "false").lower() in (
    "true",
    "1",
    "yes",
)

HOST_NAME = os.environ.get("HOST_NAME", "iridium")
LOG_DIR = os.environ.get("LOG_DIR", "/app/logs")
LOG_FILE = os.path.join(LOG_DIR, "tetra_events.jsonl")

IGNORED_PDUS = {"D-NWRK-BROADCAST", "SYSINFO", "D-CK CHANGE DEMAND"}
CALL_PDUS = {
    "D-SETUP",
    "D-CONNECT",
    "D-CONNECT ACK",
    "D-TX GRANTED",
    "D-TX CEASED",
    "D-TX WAIT",
    "D-TX INTERRUPT",
    "D-RELEASE",
    "D-CALL RESTORE",
}

STATUS_MAP = {
    0: "NÖDLARM / Emergency (0)",
    65024: "Heartbeat / ACK (65024)",
    65025: "Status ACK (65025)",
    65535: "System Status (65535)",
}

TX_GRANT_MAP = {
    0: "Granted",
    1: "Not granted",
    2: "Request queued",
    3: "Granted to another unit",
}

DISCONNECT_CAUSE_MAP = {
    0: "Not defined",
    1: "User requested disconnect",
    2: "Called party busy",
    3: "Called party not reachable",
    4: "Called party does not support encryption",
    5: "Congestion in infrastructure",
    6: "Not allowed traffic",
    7: "Incompatible traffic",
    8: "Service not available",
    9: "Pre-emption",
    10: "Invalid call identifier",
    11: "Called party rejection",
    12: "No idle CCK",
    13: "Expiry of timer",
    14: "SwMI requested disconnection",
    15: "Acknowledged service not completed",
}

TRIONA_CMD_MAP = {
    "900": "Session Init",
    "901": "Gateway Handshake / Poll",
    "902": "Gateway ACK / Keepalive",
    "903": "Configuration Update",
    "904": "Status Report",
    "905": "Disconnect / Reset",
}

TRIONA_REGEX = re.compile(r"^\*(\d+)\*([^*]+)\*([^*]+)\*([0-9A-Fa-f]{6,24})#$")
STAR_FRAME_REGEX = re.compile(r"^\*(.+)#$")
ROUTE_SYNC_REGEX = re.compile(
    r"^([A-Za-z0-9_]+),(\d+),(\d+)-(\d+)-(\d+),(\d+)$"
)
GENERIC_CSV_SYNC_REGEX = re.compile(r"^([A-Z]_[A-Z]_),(.+)$")


def parse_channels(channels_str):
    channels = []
    entries = [c.strip() for c in channels_str.split(",") if c.strip()]
    for idx, entry in enumerate(entries, start=1):
        parts = entry.split(":")
        freq_hz = int(parts[0])
        la = (
            int(parts[1])
            if len(parts) > 1 and parts[1].isdigit()
            else parts[1]
            if len(parts) > 1
            else idx
        )
        freq_mhz = f"{freq_hz / 1e6:.4f}"
        channels.append(
            {
                "idx": idx,
                "port": 42100 + idx,
                "freq_hz": freq_hz,
                "freq_mhz": freq_mhz,
                "la": la,
            }
        )
    return channels


def parse_udp_targets(targets_str):
    targets = []
    if not targets_str:
        return targets
    for item in targets_str.split(","):
        item = item.strip()
        if not item or ":" not in item:
            continue
        host, port_s = item.rsplit(":", 1)
        try:
            targets.append((host.strip(), int(port_s.strip())))
        except ValueError:
            print(f"[parser] Invalid UDP target ignored: {item}", flush=True)
    return targets


def get_ci(pkt, candidate_keys, default=None):
    if not isinstance(pkt, dict):
        return default
    lower_map = {str(k).lower().strip(): v for k, v in pkt.items() if v is not None}
    for key in candidate_keys:
        k_low = key.lower().strip()
        if k_low in lower_map:
            return lower_map[k_low]
    return default


def to_int(val, default=None):
    if val is None:
        return default
    try:
        return int(val)
    except (ValueError, TypeError):
        return default


def classify_ssi(ssi):
    val = to_int(ssi)
    if val is None:
        return None
    if 1 <= val <= 999:
        return "Gateway/Dispatch"
    if 3000000 <= val <= 3999999:
        return "Talkgroup (GSSI)"
    if 4500000 <= val <= 4599999:
        return "Vehicle/Mobile (ISSI)"
    return "Radio/Group"


def format_mac(hex_id):
    clean = re.sub(r"[^0-9A-Fa-f]", "", str(hex_id)).upper()
    if len(clean) == 12:
        return ":".join(clean[i : i + 2] for i in range(0, 12, 2))
    return None


def extract_bits_signed(data_bytes, bit_offset, num_bits):
    total_bits = len(data_bytes) * 8
    if bit_offset + num_bits > total_bits:
        return None
    val = 0
    for i in range(num_bits):
        byte_idx = (bit_offset + i) // 8
        bit_idx = 7 - ((bit_offset + i) % 8)
        bit = (data_bytes[byte_idx] >> bit_idx) & 1
        val = (val << 1) | bit
    if val & (1 << (num_bits - 1)):
        val -= 1 << num_bits
    return val


def decode_lip_gps(raw_bytes):
    if not raw_bytes:
        return None, None
    payload = (
        raw_bytes[1:] if len(raw_bytes) >= 8 and raw_bytes[0] == 0x0A else raw_bytes
    )
    if len(payload) < 7:
        return None, None

    candidate_offsets = [8, 6, 4, 2, 0]
    fallback_coords = None

    for offset in candidate_offsets:
        lon_raw = extract_bits_signed(payload, offset, 25)
        lat_raw = extract_bits_signed(payload, offset + 25, 24)
        if lon_raw is None or lat_raw is None:
            continue
        lon = (lon_raw * 360.0) / float(1 << 25)
        lat = (lat_raw * 180.0) / float(1 << 24)
        if -90.0 <= lat <= 90.0 and -180.0 <= lon <= 180.0:
            if fallback_coords is None:
                fallback_coords = (round(lat, 6), round(lon, 6))
            if 54.0 <= lat <= 70.0 and 10.0 <= lon <= 25.0:
                return round(lat, 6), round(lon, 6)

    if fallback_coords is not None:
        return fallback_coords
    return None, None


def extract_ascii_strings(raw_bytes, min_len=3):
    text = "".join(chr(b) if 32 <= b <= 126 else "\x00" for b in raw_bytes)
    parts = [p.strip() for p in text.split("\x00") if len(p.strip()) >= min_len]
    return " ".join(parts) if parts else None


def decode_sds_tl_hex(raw_bytes):
    if not raw_bytes:
        return None
    candidates = [raw_bytes]
    if len(raw_bytes) > 2:
        candidates.append(raw_bytes[2:])
    if len(raw_bytes) > 3:
        candidates.append(raw_bytes[3:])
    for cand in candidates:
        printable = sum(1 for b in cand if 32 <= b <= 126 or b in (10, 13))
        if len(cand) >= 3 and printable / float(len(cand)) >= 0.75:
            return "".join(chr(b) for b in cand if 32 <= b <= 126).strip()
    return extract_ascii_strings(raw_bytes, min_len=3)


def parse_structured_text(text):
    if not text:
        return None, None

    cleaned = text.strip()
    m_triona = TRIONA_REGEX.match(cleaned)
    if m_triona:
        cmd_code, app_name, app_ver, dev_id = m_triona.groups()
        dev_id_up = dev_id.upper()
        mac = format_mac(dev_id_up)
        cmd_desc = TRIONA_CMD_MAP.get(cmd_code, f"Command {cmd_code}")
        parsed = {
            "protocol": "Triona",
            "command_code": int(cmd_code),
            "command_name": cmd_desc,
            "app_name": app_name,
            "app_version": app_ver,
            "device_id": dev_id_up,
            "device_mac": mac,
        }
        return "TRIONA_GATEWAY", parsed

    m_route = ROUTE_SYNC_REGEX.match(cleaned)
    if m_route:
        flag, consist_id, unit_id, direction, route_no, status_code = m_route.groups()
        parsed = {
            "protocol": "TrainRouteSync",
            "control_flag": flag,
            "consist_or_tx_id": consist_id,
            "assignment_raw": f"{unit_id}-{direction}-{route_no}",
            "unit_or_op_id": int(unit_id) if unit_id.isdigit() else unit_id,
            "direction": int(direction) if direction.isdigit() else direction,
            "train_or_route_no": int(route_no) if route_no.isdigit() else route_no,
            "status_code": int(status_code) if status_code.isdigit() else status_code,
        }
        return "TRAIN_ROUTE_SYNC", parsed

    m_csv_sync = GENERIC_CSV_SYNC_REGEX.match(cleaned)
    if m_csv_sync:
        flag, rest = m_csv_sync.groups()
        csv_fields = [f.strip() for f in rest.split(",")]
        parsed = {
            "protocol": "TrainRouteSync",
            "control_flag": flag,
            "fields": csv_fields,
        }
        for f in csv_fields:
            if "-" in f:
                sub = f.split("-")
                if len(sub) == 3:
                    parsed["assignment_raw"] = f
                    parsed["unit_or_op_id"] = int(sub[0]) if sub[0].isdigit() else sub[0]
                    parsed["direction"] = int(sub[1]) if sub[1].isdigit() else sub[1]
                    parsed["train_or_route_no"] = int(sub[2]) if sub[2].isdigit() else sub[2]
        return "TRAIN_ROUTE_SYNC", parsed

    m_star = STAR_FRAME_REGEX.match(cleaned)
    if m_star:
        fields = [f.strip() for f in m_star.group(1).split("*")]
        if len(fields) >= 2:
            parsed = {
                "protocol": "StarFrame",
                "fields": fields,
            }
            if fields[0].isdigit():
                parsed["command_code"] = int(fields[0])
            for f in fields:
                mac = format_mac(f)
                if mac:
                    parsed["device_mac"] = mac
                    parsed["device_id"] = f.upper()
            return "CONTROL_FRAME", parsed

    if ":" in cleaned and len(cleaned) <= 32:
        parts = cleaned.split(":", 1)
        if parts[0].isdigit():
            return "AVL_TELEMETRY", {
                "line_or_unit": int(parts[0]),
                "telemetry_code": parts[1],
            }

    return None, None


def decode_sds_payload(proto_id, hex_str, infos_list):
    sds_type = "BINARY"
    sds_text = None
    sds_binary_tail = None
    gps_lat = None
    gps_lon = None
    parsed_data = None

    infos_text = None
    if isinstance(infos_list, list) and infos_list:
        extracted = []
        for item in infos_list:
            if isinstance(item, dict):
                if "text" in item and item["text"] is not None:
                    extracted.append(str(item["text"]))
                else:
                    for k, v in item.items():
                        extracted.append(f"{k}: {v}")
            elif isinstance(item, str):
                extracted.append(item)
        if extracted:
            infos_text = " | ".join(extracted)
    elif isinstance(infos_list, str) and infos_list.strip():
        infos_text = infos_list.strip()

    raw_bytes = b""
    clean_hex = ""
    if hex_str:
        clean_hex = re.sub(r"[^0-9a-fA-F]", "", str(hex_str)).lower()
        if len(clean_hex) % 2 == 1:
            clean_hex = clean_hex[:-1]
        try:
            raw_bytes = bytes.fromhex(clean_hex)
        except ValueError:
            raw_bytes = b""

    if proto_id == 10:
        sds_type = "LIP_GPS"
        gps_lat, gps_lon = decode_lip_gps(raw_bytes)
        if gps_lat is not None and gps_lon is not None:
            sds_text = f"GPS {gps_lat:.6f}, {gps_lon:.6f}"
            parsed_data = {
                "lat": gps_lat,
                "lon": gps_lon,
                "maps_url": f"https://www.google.com/maps?q={gps_lat},{gps_lon}",
            }
        elif infos_text:
            sds_text = infos_text
    elif proto_id == 130:
        sds_type = "TEXT"
        if infos_text:
            sds_text = infos_text
        elif raw_bytes:
            sds_text = decode_sds_tl_hex(raw_bytes) or clean_hex
    elif proto_id == 245:
        sds_type = "AVL_TELEMETRY"
        body = raw_bytes[3:] if len(raw_bytes) > 3 else raw_bytes
        if b"\n" in body:
            prefix, tail = body.split(b"\n", 1)
            sds_text = prefix.decode("ascii", errors="replace").strip()
            sds_binary_tail = tail.hex()
        else:
            ascii_str = extract_ascii_strings(body, min_len=2)
            sds_text = ascii_str
            sds_binary_tail = body.hex()
        if sds_text and ":" in sds_text:
            p_parts = sds_text.split(":", 1)
            parsed_data = {
                "avl_prefix": sds_text,
                "line_or_id": int(p_parts[0]) if p_parts[0].isdigit() else p_parts[0],
                "state_code": p_parts[1],
                "tail_bytes": len(bytes.fromhex(sds_binary_tail)) if sds_binary_tail else 0,
            }
    else:
        if infos_text:
            sds_type = "TEXT"
            sds_text = infos_text
        elif raw_bytes:
            ascii_str = decode_sds_tl_hex(raw_bytes)
            if ascii_str:
                sds_type = "TEXT"
                sds_text = ascii_str
            else:
                sds_type = "BINARY"

    if sds_text and sds_type in ("TEXT", "AVL_TELEMETRY"):
        detected_type, struct_dict = parse_structured_text(sds_text)
        if detected_type and struct_dict:
            if sds_type == "TEXT":
                sds_type = detected_type
            if parsed_data is None:
                parsed_data = struct_dict
            else:
                parsed_data.update(struct_dict)

    return {
        "sds_type": sds_type,
        "sds_text": sds_text,
        "sds_hex": clean_hex if clean_hex else None,
        "sds_binary_tail": sds_binary_tail,
        "gps_lat": gps_lat,
        "gps_lon": gps_lon,
        "parsed": parsed_data,
    }


def format_sds_human_text(ssi, dest_ssi, proto_id, decoded):
    sds_type = decoded["sds_type"]
    sds_text = decoded["sds_text"]
    parsed = decoded.get("parsed") or {}
    src_str = f"SSI {ssi}" if ssi is not None else "Unknown SSI"
    dst_str = f"SSI {dest_ssi}" if dest_ssi is not None else "Broadcast"

    if sds_type == "TRIONA_GATEWAY":
        app = parsed.get("app_name", "TrionaGateway")
        ver = parsed.get("app_version", "?")
        cmd_name = parsed.get("command_name", f"Cmd {parsed.get('command_code')}")
        mac = parsed.get("device_mac") or parsed.get("device_id") or "?"
        return f"{app} v{ver} [{cmd_name}] {src_str} -> {dst_str} (Device {mac})"

    if sds_type == "TRAIN_ROUTE_SYNC":
        flag = parsed.get("control_flag", "SYNC")
        train_no = parsed.get("train_or_route_no", "?")
        direction = parsed.get("direction", "?")
        unit_id = parsed.get("unit_or_op_id", "?")
        consist_id = parsed.get("consist_or_tx_id", "?")
        st_code = parsed.get("status_code", "?")
        return (
            f"Train/Route Sync [{flag}] {src_str} -> {dst_str}: "
            f"Train/Line {train_no} (Dir {direction}, Unit/Op {unit_id}, "
            f"Consist/TxID {consist_id}, Status {st_code})"
        )

    if sds_type == "LIP_GPS":
        lat = decoded.get("gps_lat")
        lon = decoded.get("gps_lon")
        if lat is not None and lon is not None:
            return f"GPS Position {src_str} -> {dst_str}: {lat:.6f}, {lon:.6f}"
        return f"LIP GPS packet {src_str} -> {dst_str} ({decoded.get('sds_hex')})"

    if sds_type == "AVL_TELEMETRY":
        tail = decoded.get("sds_binary_tail")
        tail_len = len(tail) // 2 if tail else 0
        label = sds_text or "Binary AVL"
        return f"AVL Telemetry {src_str} -> {dst_str}: {label} (+{tail_len}B payload)"

    if sds_type == "CONTROL_FRAME":
        fields_str = ", ".join(str(x) for x in parsed.get("fields", []))
        return f"Control Frame {src_str} -> {dst_str}: [{fields_str}]"

    if sds_text:
        return f"SDS Text {src_str} -> {dst_str} (Proto {proto_id}): {sds_text}"

    return f"SDS Binary {src_str} -> {dst_str} (Proto {proto_id}): {decoded.get('sds_hex') or 'empty'}"


def publish_ha_discovery(client):
    if not HA_DISCOVERY_ENABLED or client is None:
        return

    device_info = {
        "name": "TETRA Scanner (240-11)",
        "identifiers": ["tetra_scanner_240_11"],
        "manufacturer": "Minglarn",
        "sw_version": APP_VERSION,
    }

    availability_topic = f"{MQTT_BASE_TOPIC}/status"

    sensors = [
        {
            "id": "last_sds",
            "name": "Last SDS",
            "state_topic": f"{MQTT_BASE_TOPIC}/SDS",
            "value_template": "{{ value_json.human_text | default(value_json.sds_text) | default(value_json.sds_hex) }}",
        },
        {
            "id": "last_call",
            "name": "Last Call",
            "state_topic": f"{MQTT_BASE_TOPIC}/CALL",
            "value_template": "{{ value_json.human_text | default(value_json.pdu ~ ' | SSI ' ~ value_json.ssi) }}",
        },
        {
            "id": "last_status",
            "name": "Last Status",
            "state_topic": f"{MQTT_BASE_TOPIC}/STATUS",
            "value_template": "{{ value_json.human_text | default(value_json.status_text) }}",
        },
        {
            "id": "last_roaming",
            "name": "Last Roaming",
            "state_topic": f"{MQTT_BASE_TOPIC}/ROAMING",
            "value_template": "{{ value_json.human_text | default('SSI ' ~ value_json.ssi ~ ' (LA' ~ value_json.la ~ ')') }}",
        },
        {
            "id": "packets_per_min",
            "name": "Packets Per Minute",
            "state_topic": f"{MQTT_BASE_TOPIC}/heartbeat",
            "value_template": "{{ value_json.total_packets_60s }}",
            "unit_of_measurement": "pkt/min",
        },
    ]

    for s in sensors:
        config_topic = f"{HA_DISCOVERY_PREFIX}/sensor/tetra_240_11/{s['id']}/config"
        payload = {
            "name": s["name"],
            "unique_id": f"tetra_240_11_{s['id']}",
            "state_topic": s["state_topic"],
            "json_attributes_topic": s["state_topic"],
            "availability_topic": availability_topic,
            "value_template": s["value_template"],
            "device": device_info,
        }
        if "unit_of_measurement" in s:
            payload["unit_of_measurement"] = s["unit_of_measurement"]

        client.publish(config_topic, json.dumps(payload), qos=1, retain=True)

    print("[parser] Published Home Assistant MQTT Auto-Discovery configs.", flush=True)


def setup_mqtt():
    if not MQTT_ENABLED:
        print("[parser] MQTT is disabled via configuration.", flush=True)
        return None
    if mqtt is None:
        print("[parser] Warning: paho-mqtt not installed, MQTT disabled.", flush=True)
        return None

    client = mqtt.Client(client_id=f"tetra_scanner_{HOST_NAME}")
    if MQTT_USER:
        client.username_pw_set(MQTT_USER, MQTT_PASS)

    status_topic = f"{MQTT_BASE_TOPIC}/status"
    client.will_set(status_topic, payload="offline", qos=1, retain=True)

    def on_connect(c, userdata, flags, rc):
        if rc == 0:
            print(f"[parser] Connected to MQTT broker {MQTT_HOST}:{MQTT_PORT}", flush=True)
            c.publish(status_topic, payload="online", qos=1, retain=True)
            publish_ha_discovery(c)
        else:
            print(f"[parser] MQTT connection failed with code {rc}", flush=True)

    client.on_connect = on_connect

    try:
        client.connect_async(MQTT_HOST, MQTT_PORT, keepalive=60)
        client.loop_start()
    except Exception as exc:
        print(f"[parser] Failed to start MQTT client: {exc}", flush=True)

    return client


def main():
    os.makedirs(LOG_DIR, exist_ok=True)
    channels = parse_channels(TETRA_CHANNELS_STR)
    udp_targets = parse_udp_targets(UDP_OUT_TARGETS_STR) if UDP_OUT_ENABLED else []

    udp_out_sock = None
    if UDP_OUT_ENABLED and udp_targets:
        udp_out_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        udp_out_sock.setblocking(False)
        print(f"[parser] UDP JSON stream enabled -> {udp_targets}", flush=True)

    mqtt_client = setup_mqtt()

    rx_sockets = []
    sock_to_chan = {}
    for ch in channels:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind(("127.0.0.1", ch["port"]))
        s.setblocking(False)
        rx_sockets.append(s)
        sock_to_chan[s] = ch
        print(
            f"[parser] Listening on UDP 127.0.0.1:{ch['port']} for {ch['freq_mhz']} MHz (LA {ch['la']})",
            flush=True,
        )

    pending_sds = {}
    dedup_cache = {}
    active_tx = {}
    stats_60s = {ch["freq_mhz"]: 0 for ch in channels}
    last_heartbeat = time.time()
    running = True

    def handle_sig(signum, frame):
        nonlocal running
        running = False

    signal.signal(signal.SIGINT, handle_sig)
    signal.signal(signal.SIGTERM, handle_sig)

    def is_duplicate(sig_tuple, now_ts, ttl=1.2):
        expired = [k for k, ts in dedup_cache.items() if now_ts - ts > 5.0]
        for k in expired:
            dedup_cache.pop(k, None)
        prev_ts = dedup_cache.get(sig_tuple)
        if prev_ts is not None and (now_ts - prev_ts) <= ttl:
            return True
        dedup_cache[sig_tuple] = now_ts
        return False

    def send_udp_json(payload_dict):
        if not udp_out_sock or not udp_targets:
            return
        data = (json.dumps(payload_dict, ensure_ascii=False) + "\n").encode("utf-8")
        for target in udp_targets:
            try:
                udp_out_sock.sendto(data, target)
            except Exception:
                pass

    def emit_event(event_dict, raw_pkt=None):
        if UDP_OUT_INCLUDE_RAW and raw_pkt is not None:
            event_with_opt_raw = dict(event_dict)
            event_with_opt_raw["raw"] = raw_pkt
        else:
            event_with_opt_raw = event_dict

        try:
            with open(LOG_FILE, "a", encoding="utf-8") as f:
                f.write(json.dumps(event_dict, ensure_ascii=False) + "\n")
        except Exception as exc:
            print(f"[parser] Log write error: {exc}", flush=True)

        if mqtt_client is not None:
            payload_str = json.dumps(event_dict, ensure_ascii=False)
            cat = event_dict.get("category", "OTHER")
            mqtt_client.publish(
                f"{MQTT_BASE_TOPIC}/event", payload_str, qos=0, retain=False
            )
            mqtt_client.publish(
                f"{MQTT_BASE_TOPIC}/{cat}", payload_str, qos=0, retain=True
            )

        send_udp_json(event_with_opt_raw)

        cat = event_dict.get("category", "OTHER")
        freq = event_dict.get("freq_mhz", "?")
        la = event_dict.get("la", "?")
        human = event_dict.get("human_text") or event_dict.get("pdu", "?")
        print(f"[{cat}] {freq} MHz (LA{la}) | {human}", flush=True)

    def build_and_emit_sds(ch, pkt1, pkt2=None):
        now_ts = time.time()
        now_iso = datetime.now(timezone.utc).isoformat()
        merged_raw = dict(pkt1)
        if pkt2:
            merged_raw.update(pkt2)

        mac_ssi = to_int(get_ci(pkt1, ["ssi"]))
        if mac_ssi is None and pkt2:
            mac_ssi = to_int(get_ci(pkt2, ["ssi"]))

        caller_ssi = to_int(
            get_ci(
                merged_raw,
                [
                    "calling party ssi",
                    "calling party address ssi",
                    "calling_ssi",
                    "source ssi",
                ],
            )
        )
        forward_ssi = to_int(
            get_ci(
                merged_raw,
                [
                    "forward address ssi",
                    "called party ssi",
                    "called party address ssi",
                    "dest_ssi",
                ],
            )
        )

        if caller_ssi is not None:
            ssi = caller_ssi
            dest_ssi = forward_ssi if forward_ssi is not None else mac_ssi
        else:
            ssi = mac_ssi
            dest_ssi = forward_ssi

        proto_id = to_int(get_ci(merged_raw, ["protocol id", "protocol_id"]), default=0)
        msg_ref = to_int(get_ci(merged_raw, ["message reference", "message_reference"]))
        sds_pdu = get_ci(merged_raw, ["sds-pdu", "sds_pdu", "message type"])
        infos = get_ci(pkt1, ["infos"]) or (get_ci(pkt2, ["infos"]) if pkt2 else None)
        hex_str = (
            get_ci(pkt2, ["hex"])
            if pkt2 and get_ci(pkt2, ["hex"]) is not None
            else get_ci(pkt1, ["hex"])
        )

        decoded = decode_sds_payload(proto_id, hex_str, infos)
        human_text = format_sds_human_text(ssi, dest_ssi, proto_id, decoded)

        sig = (
            ch["freq_mhz"],
            "D-SDS-DATA",
            ssi,
            dest_ssi,
            proto_id,
            decoded["sds_hex"],
            decoded["sds_text"],
        )
        if is_duplicate(sig, now_ts, ttl=1.2):
            return

        event = {
            "timestamp": now_iso,
            "host": HOST_NAME,
            "category": "SDS",
            "service": str(get_ci(pkt1, ["service"], "CMCE")).upper(),
            "pdu": str(get_ci(pkt1, ["pdu"], "D-SDS-DATA")).upper(),
            "freq_mhz": ch["freq_mhz"],
            "la": ch["la"],
            "ssi": ssi,
            "ssi_type": classify_ssi(ssi),
            "dest_ssi": dest_ssi,
            "dest_ssi_type": classify_ssi(dest_ssi),
            "protocol_id": proto_id,
            "message_reference": msg_ref,
            "sds_pdu": sds_pdu,
            "sds_type": decoded["sds_type"],
            "sds_text": decoded["sds_text"],
            "sds_hex": decoded["sds_hex"],
            "sds_binary_tail": decoded["sds_binary_tail"],
            "gps_lat": decoded["gps_lat"],
            "gps_lon": decoded["gps_lon"],
            "parsed": decoded["parsed"],
            "human_text": human_text,
        }
        emit_event(event, raw_pkt=merged_raw)

    def flush_expired_sds(now_ts, force=False):
        expired_keys = [
            k for k, val in pending_sds.items() if force or (now_ts - val["ts"] > 2.0)
        ]
        for k in expired_keys:
            entry = pending_sds.pop(k)
            build_and_emit_sds(entry["ch"], entry["pkt"], None)

    def process_packet(ch, pkt):
        freq_str = ch["freq_mhz"]
        stats_60s[freq_str] = stats_60s.get(freq_str, 0) + 1

        service = str(get_ci(pkt, ["service"], "")).upper()
        pdu = str(get_ci(pkt, ["pdu"], "")).upper()

        if service == "UPLANE":
            return
        if pdu in IGNORED_PDUS:
            return

        now_ts = time.time()
        flush_expired_sds(now_ts)

        mac_ssi = to_int(get_ci(pkt, ["ssi"]))
        caller_ssi = to_int(
            get_ci(
                pkt,
                [
                    "calling party ssi",
                    "calling party address ssi",
                    "transmitting party ssi",
                    "transmitting party address ssi",
                    "source ssi",
                ],
            )
        )

        if pdu == "D-SDS-DATA":
            has_hex = get_ci(pkt, ["hex"]) is not None
            key = (freq_str, mac_ssi if mac_ssi is not None else caller_ssi)
            if not has_hex:
                if key in pending_sds:
                    prev = pending_sds.pop(key)
                    build_and_emit_sds(prev["ch"], prev["pkt"], None)
                pending_sds[key] = {"ts": now_ts, "ch": ch, "pkt": pkt}
                return
            else:
                if key in pending_sds and (now_ts - pending_sds[key]["ts"] <= 2.0):
                    prev = pending_sds.pop(key)
                    build_and_emit_sds(ch, prev["pkt"], pkt)
                    return
                for pend_key in list(pending_sds.keys()):
                    if pend_key[0] == freq_str and (
                        now_ts - pending_sds[pend_key]["ts"] <= 2.0
                    ):
                        prev = pending_sds.pop(pend_key)
                        build_and_emit_sds(ch, prev["pkt"], pkt)
                        return
                build_and_emit_sds(ch, pkt, pkt)
                return

        now_iso = datetime.now(timezone.utc).isoformat()

        if pdu == "D-STATUS":
            status_code = to_int(
                get_ci(pkt, ["pre-coded status", "status", "status_code"]), default=-1
            )
            status_hex = f"0x{status_code:04X}" if status_code >= 0 else None
            status_text = STATUS_MAP.get(
                status_code,
                f"Status {status_code} ({status_hex})"
                if status_hex
                else f"Status ({status_code})",
            )
            src_ssi = caller_ssi if caller_ssi is not None else mac_ssi
            dst_ssi = (
                mac_ssi
                if (caller_ssi is not None and mac_ssi != caller_ssi)
                else to_int(get_ci(pkt, ["called party ssi", "forward address ssi"]))
            )

            sig = (freq_str, pdu, src_ssi, dst_ssi, status_code)
            if is_duplicate(sig, now_ts, ttl=1.5):
                return

            target_part = f" -> SSI {dst_ssi}" if dst_ssi is not None else ""
            human_text = f"Status from SSI {src_ssi}{target_part}: {status_text}"

            event = {
                "timestamp": now_iso,
                "host": HOST_NAME,
                "category": "STATUS",
                "service": service,
                "pdu": pdu,
                "freq_mhz": freq_str,
                "la": ch["la"],
                "ssi": src_ssi,
                "ssi_type": classify_ssi(src_ssi),
                "dest_ssi": dst_ssi,
                "status_code": status_code,
                "status_hex": status_hex,
                "status_text": status_text,
                "human_text": human_text,
            }
            emit_event(event, raw_pkt=pkt)
            return

        if pdu in CALL_PDUS:
            call_id = to_int(get_ci(pkt, ["call identifier", "call_identifier"]))
            usage_marker = to_int(get_ci(pkt, ["usage marker", "usage_marker"]))
            tx_grant_code = to_int(get_ci(pkt, ["transmission grant"]))
            tx_grant_text = TX_GRANT_MAP.get(tx_grant_code)
            disc_cause_code = to_int(get_ci(pkt, ["disconnect cause"]))
            disc_cause_text = DISCONNECT_CAUSE_MAP.get(disc_cause_code)

            explicit_gssi = to_int(
                get_ci(pkt, ["gssi", "group ssi", "called party ssi", "called party address ssi"])
            )
            if explicit_gssi is not None:
                gssi = explicit_gssi
            elif mac_ssi is not None and (
                3000000 <= mac_ssi <= 3999999 or caller_ssi is not None
            ):
                gssi = mac_ssi
            else:
                gssi = None

            ssi = caller_ssi if caller_ssi is not None else mac_ssi

            sig = (freq_str, pdu, ssi, gssi, call_id, tx_grant_code)
            if is_duplicate(sig, now_ts, ttl=1.2):
                return

            tx_key = (freq_str, gssi or ssi)
            tx_duration_s = None

            if pdu == "D-TX GRANTED":
                active_tx[tx_key] = {
                    "ts": now_ts,
                    "speaker_ssi": caller_ssi,
                    "call_id": call_id,
                }
            elif pdu in ("D-TX CEASED", "D-RELEASE"):
                prev_tx = active_tx.pop(tx_key, None)
                if prev_tx is not None:
                    dur = round(now_ts - prev_tx["ts"], 1)
                    if 0.1 <= dur <= 300.0:
                        tx_duration_s = dur
                    if caller_ssi is None and prev_tx.get("speaker_ssi") is not None:
                        caller_ssi = prev_tx["speaker_ssi"]
                        ssi = caller_ssi
                    if call_id is None and prev_tx.get("call_id") is not None:
                        call_id = prev_tx["call_id"]

            grp_label = f"Group {gssi}" if gssi is not None else f"SSI {ssi}"
            speaker_label = (
                f" by SSI {caller_ssi}"
                if (caller_ssi is not None and caller_ssi != gssi)
                else ""
            )
            cid_label = f" (Call ID {call_id})" if call_id is not None else ""

            if pdu == "D-SETUP":
                human_text = f"{grp_label}: Call setup{speaker_label}{cid_label}"
            elif pdu == "D-TX GRANTED":
                grant_info = f" [{tx_grant_text}]" if tx_grant_text else ""
                human_text = f"{grp_label}: PTT TX granted{speaker_label}{grant_info}{cid_label}"
            elif pdu == "D-TX CEASED":
                dur_info = f" after {tx_duration_s}s" if tx_duration_s is not None else ""
                human_text = f"{grp_label}: PTT TX ended{speaker_label}{dur_info}{cid_label}"
            elif pdu == "D-RELEASE":
                cause_info = f" ({disc_cause_text})" if disc_cause_text else ""
                dur_info = f" after {tx_duration_s}s" if tx_duration_s is not None else ""
                human_text = f"{grp_label}: Call released{dur_info}{cause_info}{cid_label}"
            else:
                human_text = f"{grp_label}: {pdu}{speaker_label}{cid_label}"

            event = {
                "timestamp": now_iso,
                "host": HOST_NAME,
                "category": "CALL",
                "service": service,
                "pdu": pdu,
                "freq_mhz": freq_str,
                "la": ch["la"],
                "ssi": ssi,
                "ssi_type": classify_ssi(ssi),
                "caller_ssi": caller_ssi,
                "gssi": gssi,
                "call_identifier": call_id,
                "usage_marker": usage_marker,
                "tx_grant": tx_grant_text,
                "tx_duration_s": tx_duration_s,
                "disconnect_cause": disc_cause_text,
                "human_text": human_text,
            }
            emit_event(event, raw_pkt=pkt)
            return

        if "LOCATION UPDATE" in pdu:
            gssi = to_int(
                get_ci(
                    pkt,
                    [
                        "gssi",
                        "group identity",
                        "group ssi",
                        "group identity location accept",
                    ],
                )
            )
            ssi = caller_ssi if caller_ssi is not None else mac_ssi
            sig = (freq_str, pdu, ssi, gssi)
            if is_duplicate(sig, now_ts, ttl=1.5):
                return

            gssi_part = f" (Group {gssi})" if gssi is not None else ""
            human_text = (
                f"Roaming: SSI {ssi}{gssi_part} registered on LA {ch['la']} ({freq_str} MHz) [{pdu}]"
            )
            event = {
                "timestamp": now_iso,
                "host": HOST_NAME,
                "category": "ROAMING",
                "service": service,
                "pdu": pdu,
                "freq_mhz": freq_str,
                "la": ch["la"],
                "ssi": ssi,
                "ssi_type": classify_ssi(ssi),
                "gssi": gssi,
                "human_text": human_text,
            }
            emit_event(event, raw_pkt=pkt)
            return

        ssi = caller_ssi if caller_ssi is not None else mac_ssi
        sig = (freq_str, service, pdu, ssi)
        if is_duplicate(sig, now_ts, ttl=1.2):
            return

        human_text = f"{service} {pdu} on LA {ch['la']} (SSI {ssi})"
        event = {
            "timestamp": now_iso,
            "host": HOST_NAME,
            "category": "OTHER",
            "service": service,
            "pdu": pdu,
            "freq_mhz": freq_str,
            "la": ch["la"],
            "ssi": ssi,
            "ssi_type": classify_ssi(ssi),
            "human_text": human_text,
        }
        emit_event(event, raw_pkt=pkt)

    while running:
        readable, _, _ = select.select(rx_sockets, [], [], 0.5)
        now_ts = time.time()

        for s in readable:
            ch = sock_to_chan[s]
            try:
                data, _ = s.recvfrom(65535)
            except Exception:
                continue
            text = data.decode("utf-8", errors="replace")
            for line in text.splitlines():
                line = line.strip()
                if not line:
                    continue
                try:
                    pkt = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if isinstance(pkt, dict):
                    process_packet(ch, pkt)

        flush_expired_sds(now_ts)

        if now_ts - last_heartbeat >= 60.0:
            total_60s = sum(stats_60s.values())
            hb_payload = {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "host": HOST_NAME,
                "category": "HEARTBEAT",
                "version": APP_VERSION,
                "total_packets_60s": total_60s,
                "channels_60s": dict(stats_60s),
            }
            if mqtt_client is not None:
                mqtt_client.publish(
                    f"{MQTT_BASE_TOPIC}/heartbeat",
                    json.dumps(hb_payload, ensure_ascii=False),
                    qos=0,
                    retain=True,
                )
            send_udp_json(hb_payload)
            print(
                f"[heartbeat] 60s total packets: {total_60s} | per channel: {stats_60s}",
                flush=True,
            )
            for k in stats_60s:
                stats_60s[k] = 0
            last_heartbeat = now_ts

    flush_expired_sds(time.time(), force=True)
    if mqtt_client is not None:
        mqtt_client.publish(
            f"{MQTT_BASE_TOPIC}/status", payload="offline", qos=1, retain=True
        )
        mqtt_client.loop_stop()
        mqtt_client.disconnect()
    for s in rx_sockets:
        s.close()
    if udp_out_sock is not None:
        udp_out_sock.close()
    print("[parser] Parser stopped cleanly.", flush=True)


if __name__ == "__main__":
    main()
