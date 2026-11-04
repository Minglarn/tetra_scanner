#!/usr/bin/env python3
import os
import sys
import json
import time
import socket
import select
import signal
import struct
import re
import base64
import zlib
import wave
import shutil
import logging
import threading
import subprocess
from datetime import datetime, timezone

logging.basicConfig(
    level=logging.INFO,
    format="%(message)s",
    stream=sys.stdout,
)

try:
    import paho.mqtt.client as mqtt
except ImportError:
    mqtt = None


APP_VERSION = os.environ.get("APP_VERSION", "2026.11.04.28")

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
AUDIO_DIR = os.environ.get("AUDIO_DIR", os.path.join(LOG_DIR, "audio"))
MIN_VOICE_FRAMES = int(os.environ.get("MIN_VOICE_FRAMES", "6"))
MIN_AUDIO_DURATION_SEC = float(os.environ.get("MIN_AUDIO_DURATION_SEC", "1.2"))
VOICE_SPLIT_TIMEOUT_S = float(os.environ.get("VOICE_SPLIT_TIMEOUT_S", "1.5"))
MAX_AUDIO_FILES = int(os.environ.get("MAX_AUDIO_FILES", "500"))
LOG_FILE = os.path.join(LOG_DIR, "tetra_events.jsonl")
DISCOVERED_NETWORKS_FILE = os.path.join(LOG_DIR, "discovered_networks.jsonl")
WHITELIST_FILE = os.path.join(LOG_DIR, "whitelist.json")
BLACKLIST_FILE = os.path.join(LOG_DIR, "blacklist.json")
NETWORK_LOG_COOLDOWN_S = 3600.0

IGNORED_PDUS = {"D-CK CHANGE DEMAND"}
NETWORK_PDUS = {"SYSINFO", "SYNC", "D-NWRK-BROADCAST"}
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
    1: "Status 1 (In Service / Active)",
    2: "Status 2 (Acknowledged / Sync OK)",
    3: "Status 3 (At Stop / Station)",
    4: "Status 4 (Delayed / Traffic Info)",
    5: "Status 5 (Ready for Departure)",
    6: "Status 6 (Out of Service / Depot)",
    10: "Status 10 (Driver Call Request)",
    12: "Status 12 (Vehicle Fault / Disturbance)",
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
NMEA_REGEX = re.compile(r"(\$G[PN][A-Z]{3},[^\r\n]+)")


def parse_channels(channels_str):
    channels = []
    entries = [c.strip() for c in channels_str.split(",") if c.strip()]
    for idx, entry in enumerate(entries, start=1):
        parts = entry.split(":")
        freq_hz = int(parts[0])
        la_part = parts[1].strip() if len(parts) > 1 else ""
        la = (
            int(la_part)
            if la_part.isdigit()
            else (la_part if la_part and la_part != "?" else None)
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
        return "Vehicle/Train Radio (ISSI)"
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


def scan_binary_for_nordic_coords(raw_bytes):
    if not raw_bytes or len(raw_bytes) < 7:
        return None, None

    for i in range(0, max(1, len(raw_bytes) - 6)):
        lat, lon = decode_lip_gps(raw_bytes[i : i + 8])
        if lat is not None and lon is not None and 55.0 <= lat <= 69.5 and 11.0 <= lon <= 24.5:
            return lat, lon

    if len(raw_bytes) >= 8:
        for i in range(0, len(raw_bytes) - 7):
            chunk = raw_bytes[i : i + 8]
            for endian in (">", "<"):
                try:
                    a, b = struct.unpack(f"{endian}ii", chunk)
                    for scale in (1e6, 1e7, 1e5):
                        lat1, lon1 = a / scale, b / scale
                        if 55.0 <= lat1 <= 69.5 and 11.0 <= lon1 <= 24.5:
                            return round(lat1, 6), round(lon1, 6)
                        lat2, lon2 = b / scale, a / scale
                        if 55.0 <= lat2 <= 69.5 and 11.0 <= lon2 <= 24.5:
                            return round(lat2, 6), round(lon2, 6)
                except struct.error:
                    pass
                try:
                    fa, fb = struct.unpack(f"{endian}ff", chunk)
                    if 55.0 <= fa <= 69.5 and 11.0 <= fb <= 24.5:
                        return round(float(fa), 6), round(float(fb), 6)
                    if 55.0 <= fb <= 69.5 and 11.0 <= fa <= 24.5:
                        return round(float(fb), 6), round(float(fa), 6)
                except struct.error:
                    pass

    return None, None


def nmea_coord_to_decimal(coord_str, hemi):
    if not coord_str or "." not in coord_str:
        return None
    try:
        dot_idx = coord_str.index(".")
        deg_len = dot_idx - 2
        if deg_len not in (2, 3):
            return None
        degrees = float(coord_str[:deg_len])
        minutes = float(coord_str[deg_len:])
        dec = degrees + (minutes / 60.0)
        if hemi in ("S", "W"):
            dec = -dec
        return round(dec, 6)
    except ValueError:
        return None


def parse_nmea_sentence(text):
    if not text:
        return None
    m = NMEA_REGEX.search(text)
    if not m:
        return None

    sentence = m.group(1).split("*")[0].strip()
    parts = [p.strip() for p in sentence.split(",")]
    talker_cmd = parts[0].upper()

    lat = None
    lon = None
    speed_kmh = None
    course_deg = None
    utc_time = None
    valid = True

    if talker_cmd.endswith("RMC") and len(parts) >= 7:
        utc_time = parts[1] or None
        status = parts[2].upper()
        valid = status == "A"
        lat = nmea_coord_to_decimal(parts[3], parts[4].upper())
        lon = nmea_coord_to_decimal(parts[5], parts[6].upper())
        if len(parts) > 7 and parts[7]:
            try:
                speed_kmh = round(float(parts[7]) * 1.852, 1)
            except ValueError:
                pass
        if len(parts) > 8 and parts[8]:
            try:
                course_deg = round(float(parts[8]), 1)
            except ValueError:
                pass
    elif talker_cmd.endswith("GGA") and len(parts) >= 6:
        utc_time = parts[1] or None
        lat = nmea_coord_to_decimal(parts[2], parts[3].upper())
        lon = nmea_coord_to_decimal(parts[4], parts[5].upper())
    elif talker_cmd.endswith("GLL") and len(parts) >= 5:
        lat = nmea_coord_to_decimal(parts[1], parts[2].upper())
        lon = nmea_coord_to_decimal(parts[3], parts[4].upper())
        if len(parts) > 5 and parts[5]:
            utc_time = parts[5]

    if lat is None or lon is None:
        return None

    return {
        "protocol": "NMEA_GPS",
        "nmea_type": talker_cmd.lstrip("$"),
        "valid": valid,
        "utc_time": utc_time,
        "lat": lat,
        "lon": lon,
        "speed_kmh": speed_kmh,
        "course_deg": course_deg,
        "maps_url": f"https://www.google.com/maps?q={lat},{lon}",
    }


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

    nmea_info = parse_nmea_sentence(cleaned)
    if nmea_info:
        return "NMEA_GPS", nmea_info

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
            tail_bytes_raw = tail
        else:
            ascii_str = extract_ascii_strings(body, min_len=2)
            sds_text = ascii_str
            sds_binary_tail = body.hex()
            tail_bytes_raw = body

        gps_lat, gps_lon = scan_binary_for_nordic_coords(tail_bytes_raw)

        parsed_data = {
            "avl_prefix": sds_text,
            "tail_bytes": len(tail_bytes_raw),
        }
        if sds_text and ":" in sds_text:
            p_parts = sds_text.split(":", 1)
            parsed_data["line_or_id"] = (
                int(p_parts[0]) if p_parts[0].isdigit() else p_parts[0]
            )
            parsed_data["state_code"] = p_parts[1]
        if gps_lat is not None and gps_lon is not None:
            parsed_data["lat"] = gps_lat
            parsed_data["lon"] = gps_lon
            parsed_data["maps_url"] = f"https://www.google.com/maps?q={gps_lat},{gps_lon}"
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
                gps_lat, gps_lon = scan_binary_for_nordic_coords(raw_bytes)
                if gps_lat is not None and gps_lon is not None:
                    sds_type = "BINARY_GPS"
                    sds_text = f"GPS {gps_lat:.6f}, {gps_lon:.6f}"
                    parsed_data = {
                        "lat": gps_lat,
                        "lon": gps_lon,
                        "maps_url": f"https://www.google.com/maps?q={gps_lat},{gps_lon}",
                    }

    if sds_text and sds_type in ("TEXT", "AVL_TELEMETRY"):
        detected_type, struct_dict = parse_structured_text(sds_text)
        if detected_type and struct_dict:
            if sds_type == "TEXT":
                sds_type = detected_type
            if parsed_data is None:
                parsed_data = struct_dict
            else:
                parsed_data.update(struct_dict)
            if "lat" in struct_dict and "lon" in struct_dict:
                gps_lat = struct_dict["lat"]
                gps_lon = struct_dict["lon"]

    return {
        "sds_type": sds_type,
        "sds_text": sds_text,
        "sds_hex": clean_hex if clean_hex else None,
        "sds_binary_tail": sds_binary_tail,
        "gps_lat": gps_lat,
        "gps_lon": gps_lon,
        "parsed": parsed_data,
    }


def format_unit_badge(ssi, unit_registry):
    val = to_int(ssi)
    if val is None or val not in unit_registry:
        return ""
    ctx = unit_registry[val]
    parts = []
    if ctx.get("train_or_route_no") is not None:
        r_str = f"Train/Line {ctx['train_or_route_no']}"
        if ctx.get("direction") is not None:
            r_str += f" Dir {ctx['direction']}"
        parts.append(r_str)
    if ctx.get("device_mac"):
        parts.append(f"MAC {ctx['device_mac']}")
    return f" [{' | '.join(parts)}]" if parts else ""


def format_sds_human_text(ssi, dest_ssi, proto_id, decoded, unit_registry):
    sds_type = decoded["sds_type"]
    sds_text = decoded["sds_text"]
    parsed = decoded.get("parsed") or {}
    src_badge = format_unit_badge(ssi, unit_registry)
    dst_badge = format_unit_badge(dest_ssi, unit_registry)
    src_str = f"SSI {ssi}{src_badge}" if ssi is not None else "Unknown SSI"
    dst_str = f"SSI {dest_ssi}{dst_badge}" if dest_ssi is not None else "Broadcast"

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

    if sds_type in ("LIP_GPS", "NMEA_GPS", "BINARY_GPS"):
        lat = decoded.get("gps_lat")
        lon = decoded.get("gps_lon")
        spd = parsed.get("speed_kmh")
        crs = parsed.get("course_deg")
        extra = ""
        if spd is not None:
            extra += f" Speed {spd} km/h"
        if crs is not None:
            extra += f" Course {crs} deg"
        if lat is not None and lon is not None:
            return f"GPS Position ({sds_type}) {src_str} -> {dst_str}: {lat:.6f}, {lon:.6f}{extra}"
        return f"GPS packet {src_str} -> {dst_str} ({decoded.get('sds_hex')})"

    if sds_type == "AVL_TELEMETRY":
        tail = decoded.get("sds_binary_tail")
        tail_len = len(tail) // 2 if tail else 0
        label = sds_text or "Binary AVL"
        lat = decoded.get("gps_lat")
        lon = decoded.get("gps_lon")
        coord_str = f" GPS {lat:.6f}, {lon:.6f}" if (lat is not None and lon is not None) else ""
        return f"AVL Telemetry {src_str} -> {dst_str}: {label} (+{tail_len}B payload){coord_str}"

    if sds_type == "CONTROL_FRAME":
        fields_str = ", ".join(str(x) for x in parsed.get("fields", []))
        return f"Control Frame {src_str} -> {dst_str}: [{fields_str}]"

    if sds_text:
        return f"SDS Text {src_str} -> {dst_str} (Proto {proto_id}): {sds_text}"

    return f"SDS Binary {src_str} -> {dst_str} (Proto {proto_id}): {decoded.get('sds_hex') or 'empty'}"


def load_seen_networks(filepath):
    seen = {}
    if not os.path.exists(filepath):
        return seen
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    obj = json.loads(line)
                    net_id = obj.get("network_id")
                    ts_epoch = obj.get("seen_epoch")
                    if net_id and isinstance(ts_epoch, (int, float)):
                        seen[net_id] = float(ts_epoch)
                except Exception:
                    continue
    except Exception:
        pass
    return seen


def load_json_dict(filepath):
    if not os.path.exists(filepath):
        return {}
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            data = json.load(f)
            if isinstance(data, dict):
                return data
    except Exception:
        pass
    return {}


def save_json_dict(filepath, data):
    try:
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        tmp_path = f"{filepath}.tmp"
        with open(tmp_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, sort_keys=True)
        os.replace(tmp_path, filepath)
    except Exception as exc:
        print(f"[parser] Failed writing {filepath}: {exc}", flush=True)


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

    client = mqtt.Client(client_id=f"tetra_scanner_{HOST_NAME}_{os.getpid()}")
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
    os.makedirs(AUDIO_DIR, exist_ok=True)
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
    channel_call_context = {}
    voice_buffers = {}
    voice_lock = threading.Lock()
    unit_registry = {}
    seen_networks = load_seen_networks(DISCOVERED_NETWORKS_FILE)
    raw_whitelist_data = load_json_dict(WHITELIST_FILE)
    valid_wl_categories = {"SDS", "CALL", "VOICE", "STATUS", "ROAMING", "NETWORK"}
    whitelist_data = {
        k: v
        for k, v in raw_whitelist_data.items()
        if isinstance(v, dict)
        and str(v.get("last_category", "")).upper() in valid_wl_categories
    }
    if len(whitelist_data) != len(raw_whitelist_data):
        save_json_dict(WHITELIST_FILE, whitelist_data)
    whitelist_last_save = {}
    whitelist_disk_mtime = (
        os.path.getmtime(WHITELIST_FILE) if os.path.exists(WHITELIST_FILE) else 0.0
    )
    channel_net_cache = {}
    stats_60s = {ch["freq_mhz"]: 0 for ch in channels}
    last_heartbeat = time.time()
    first_hb_sent = False
    running = True

    def handle_sig(signum, frame):
        nonlocal running
        running = False

    signal.signal(signal.SIGINT, handle_sig)
    signal.signal(signal.SIGTERM, handle_sig)

    def register_whitelisted_carrier(ch, category, mcc=None, mnc=None, la=None):
        nonlocal whitelist_data, whitelist_disk_mtime
        freq_hz = int(ch["freq_hz"])
        freq_key = str(freq_hz)
        freq_mhz = ch["freq_mhz"]
        now_ts = time.time()
        now_iso = datetime.now(timezone.utc).isoformat()

        cur_mtime = (
            os.path.getmtime(WHITELIST_FILE) if os.path.exists(WHITELIST_FILE) else 0.0
        )
        if cur_mtime != whitelist_disk_mtime:
            raw_disk = load_json_dict(WHITELIST_FILE)
            whitelist_data = {
                k: v
                for k, v in raw_disk.items()
                if isinstance(v, dict)
                and str(v.get("last_category", "")).upper() in valid_wl_categories
            }
            whitelist_last_save.clear()
            whitelist_disk_mtime = cur_mtime

        net_state = channel_net_cache.get(freq_mhz, {})
        eff_mcc = mcc if mcc is not None else net_state.get("mcc")
        eff_mnc = mnc if mnc is not None else net_state.get("mnc")
        eff_la = la if la is not None else net_state.get("la") or ch.get("la")

        is_new = freq_key not in whitelist_data
        entry = whitelist_data.get(
            freq_key,
            {
                "freq_hz": freq_hz,
                "freq_mhz": freq_mhz,
                "first_seen": now_iso,
                "hit_count": 0,
            },
        )
        entry["freq_hz"] = freq_hz
        entry["freq_mhz"] = freq_mhz
        entry["last_seen"] = now_iso
        entry["last_epoch"] = round(now_ts, 3)
        entry["last_category"] = category
        entry["hit_count"] = int(entry.get("hit_count", 0)) + 1
        if eff_mcc is not None:
            entry["mcc"] = eff_mcc
        if eff_mnc is not None:
            entry["mnc"] = eff_mnc
        if eff_la is not None:
            entry["la"] = eff_la

        whitelist_data[freq_key] = entry

        if is_new:
            print(
                f"[WHITELIST] Verified active TETRA carrier {freq_mhz} MHz (LA {eff_la}) added to whitelist.json (via {category})",
                flush=True,
            )
            bl = load_json_dict(BLACKLIST_FILE)
            if freq_key in bl:
                del bl[freq_key]
                save_json_dict(BLACKLIST_FILE, bl)

        last_saved = whitelist_last_save.get(freq_key, 0.0)
        if is_new or (now_ts - last_saved) >= 10.0:
            disk_wl = load_json_dict(WHITELIST_FILE)
            disk_wl[freq_key] = entry
            save_json_dict(WHITELIST_FILE, disk_wl)
            whitelist_data = disk_wl
            whitelist_disk_mtime = (
                os.path.getmtime(WHITELIST_FILE)
                if os.path.exists(WHITELIST_FILE)
                else 0.0
            )
            whitelist_last_save[freq_key] = now_ts

    def update_unit_registry(ssi, dest_ssi, decoded):
        parsed = decoded.get("parsed")
        if not isinstance(parsed, dict):
            return
        sds_type = decoded.get("sds_type")
        target_ids = []
        if dest_ssi is not None and dest_ssi >= 1000:
            target_ids.append(dest_ssi)
        if ssi is not None and ssi >= 1000:
            target_ids.append(ssi)
        op_id = to_int(parsed.get("unit_or_op_id"))
        if op_id is not None and op_id >= 1000:
            target_ids.append(op_id)

        for tid in target_ids:
            entry = unit_registry.setdefault(tid, {})
            if sds_type == "TRAIN_ROUTE_SYNC":
                if parsed.get("train_or_route_no") is not None:
                    entry["train_or_route_no"] = parsed["train_or_route_no"]
                if parsed.get("direction") is not None:
                    entry["direction"] = parsed["direction"]
                if parsed.get("consist_or_tx_id") is not None:
                    entry["consist_or_tx_id"] = parsed["consist_or_tx_id"]
                if op_id is not None:
                    entry["unit_or_op_id"] = op_id
            elif sds_type == "TRIONA_GATEWAY":
                if parsed.get("device_mac"):
                    entry["device_mac"] = parsed["device_mac"]
                if parsed.get("device_id"):
                    entry["device_id"] = parsed["device_id"]
            elif sds_type == "AVL_TELEMETRY":
                if parsed.get("line_or_id") is not None:
                    entry.setdefault("train_or_route_no", parsed["line_or_id"])

    def get_unit_context(ssi, dest_ssi=None):
        for cand in (ssi, dest_ssi):
            val = to_int(cand)
            if val is not None and val in unit_registry:
                return dict(unit_registry[val])
        return None

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
        for ch_obj in channels:
            if ch_obj["freq_mhz"] == freq:
                register_whitelisted_carrier(ch_obj, cat, la=la)
                break
        human = event_dict.get("human_text") or event_dict.get("pdu", "?")
        print(f"[{cat}] {freq} MHz (LA{la}) | {human}", flush=True)

    def calc_carrier_number(freq_hz):
        f_hz = int(freq_hz)
        base_hz = 300_000_000 if f_hz < 400_000_000 else 400_000_000
        if f_hz >= base_hz and (f_hz - base_hz) % 25_000 == 0:
            return (f_hz - base_hz) // 25_000
        return None

    def extract_neighbour_cells(pkt, serving_freq_hz):
        if not isinstance(pkt, dict):
            return []
        base_hz = 300_000_000 if int(serving_freq_hz) < 400_000_000 else 400_000_000
        neighbours = []
        for k, v in pkt.items():
            k_low = str(k).lower().strip()
            if not (k_low.startswith("cell ") or k_low.startswith("neighbour")):
                continue
            items_to_parse = []
            if isinstance(v, list):
                merged = {}
                for elem in v:
                    if isinstance(elem, dict):
                        merged.update(elem)
                if merged:
                    items_to_parse.append(merged)
            elif isinstance(v, dict):
                items_to_parse.append(v)

            for cdict in items_to_parse:
                carrier_num = to_int(
                    get_ci(cdict, ["main carrier number", "main_carrier_number", "carrier"])
                )
                cell_id = to_int(
                    get_ci(cdict, ["cell identifier ca", "cell identifier", "cell_id"])
                )
                nb_la = to_int(get_ci(cdict, ["la", "location area"]))
                cell_load = to_int(get_ci(cdict, ["cell load ca", "cell_load"]))
                sync_val = to_int(
                    get_ci(cdict, ["neighbour cell synchronized", "synchronized"])
                )
                reselect_types = to_int(
                    get_ci(cdict, ["cell reselection types supported"])
                )
                nb_freq_hz = None
                nb_freq_mhz = None
                if carrier_num is not None and 0 < carrier_num < 4096:
                    nb_freq_hz = base_hz + carrier_num * 25_000
                    nb_freq_mhz = f"{nb_freq_hz / 1e6:.4f}"
                if nb_freq_mhz or nb_la is not None or cell_id is not None:
                    neighbours.append(
                        {
                            "cell_id": cell_id,
                            "carrier_number": carrier_num,
                            "freq_hz": nb_freq_hz,
                            "freq_mhz": nb_freq_mhz,
                            "la": nb_la,
                            "cell_load": cell_load,
                            "synchronized": bool(sync_val == 1)
                            if sync_val is not None
                            else None,
                            "reselection_types": reselect_types,
                        }
                    )
        return neighbours

    def get_or_init_net_state(freq_mhz, freq_hz, default_la=None, role="ACTIVE CELL"):
        state = channel_net_cache.get(freq_mhz)
        if state is None:
            state = {
                "freq_mhz": freq_mhz,
                "freq_hz": int(freq_hz) if freq_hz else 0,
                "role": role,
                "carrier_number": calc_carrier_number(freq_hz) if freq_hz else None,
                "mcc": None,
                "mnc": None,
                "colour_code": None,
                "la": default_la,
                "cell_id": None,
                "cell_load": None,
                "encryption_mode": None,
                "synchronized": None,
                "fast_reselect_threshold": None,
                "fast_reselect_hysteresis": None,
                "slow_reselect_hysteresis": None,
                "slow_reselect_threshold_above_fast": None,
                "dl_freq": None,
                "ul_freq": None,
                "active_timeslots": set(),
                "usage_markers": set(),
                "neighbours": {},
                "reported_by": None,
                "packet_count": 0,
                "last_pdu": None,
                "last_emit_ts": 0.0,
                "last_sig": None,
            }
            channel_net_cache[freq_mhz] = state
        else:
            if role == "ACTIVE CELL":
                state["role"] = "ACTIVE CELL"
            if state.get("la") is None and default_la is not None:
                state["la"] = default_la
            if not state.get("freq_hz") and freq_hz:
                state["freq_hz"] = int(freq_hz)
                state["carrier_number"] = calc_carrier_number(freq_hz)
        return state

    def persist_net_state(state, pdu_label, now_ts, emit_log_event=False):
        freq_str = state["freq_mhz"]
        active_slots = sorted(state["active_timeslots"])
        usage_list = sorted(state["usage_markers"])[:12]
        neighbours_list = list(state["neighbours"].values())

        sig = (
            state["role"],
            state["mcc"],
            state["mnc"],
            state["la"],
            state["cell_id"],
            state["cell_load"],
            state["encryption_mode"],
            state["fast_reselect_threshold"],
            state["fast_reselect_hysteresis"],
            tuple(active_slots),
            tuple(usage_list[:6]),
            tuple(
                (
                    nb.get("freq_mhz"),
                    nb.get("la"),
                    nb.get("cell_id"),
                    nb.get("synchronized"),
                )
                for nb in neighbours_list
            ),
        )

        sig_changed = sig != state.get("last_sig")
        if not sig_changed and (now_ts - state.get("last_emit_ts", 0.0)) < 15.0:
            return

        state["last_sig"] = sig
        state["last_emit_ts"] = now_ts

        net_id = f"NET_{freq_str}"
        seen_networks[net_id] = now_ts
        now_iso = datetime.now(timezone.utc).isoformat()

        enc_mode = state.get("encryption_mode")
        enc_str = (
            "Clear (ENC 0)"
            if enc_mode == 0
            else (f"Encrypted (ENC {enc_mode})" if enc_mode is not None else "Unknown")
        )
        parts = [f"{state['role']} {freq_str} MHz"]
        if state.get("la") is not None:
            parts.append(f"LA={state['la']}")
        if state.get("cell_id") is not None:
            parts.append(f"Cell={state['cell_id']}")
        if state.get("mcc") is not None or state.get("mnc") is not None:
            parts.append(f"MCC={state.get('mcc') or '?'} MNC={state.get('mnc') or '?'}")
        if enc_mode is not None:
            parts.append(enc_str)
        if state.get("cell_load") is not None:
            parts.append(f"Load={state['cell_load']}")
        if active_slots:
            parts.append(f"Slots=TN{','.join(str(s) for s in active_slots)}")
        if neighbours_list:
            nb_strs = [
                f"{nb.get('freq_mhz') or '?'} MHz (LA {nb.get('la')}, Cell {nb.get('cell_id')})"
                for nb in neighbours_list
            ]
            parts.append(f"Neighbours=[{'; '.join(nb_strs)}]")

        human_text = " | ".join(parts)

        net_event = {
            "timestamp": now_iso,
            "time": now_iso,
            "seen_epoch": round(now_ts, 3),
            "host": HOST_NAME,
            "category": "NETWORK",
            "pdu": pdu_label or state.get("last_pdu") or "ACTIVE",
            "network_id": net_id,
            "role": state["role"],
            "freq_mhz": freq_str,
            "freq_hz": state["freq_hz"],
            "carrier_number": state.get("carrier_number"),
            "mcc": state.get("mcc"),
            "mnc": state.get("mnc"),
            "colour_code": state.get("colour_code"),
            "la": state.get("la"),
            "cell_id": state.get("cell_id"),
            "cell_load": state.get("cell_load"),
            "encryption_mode": enc_mode,
            "synchronized": state.get("synchronized"),
            "fast_reselect_threshold": state.get("fast_reselect_threshold"),
            "fast_reselect_hysteresis": state.get("fast_reselect_hysteresis"),
            "slow_reselect_hysteresis": state.get("slow_reselect_hysteresis"),
            "slow_reselect_threshold_above_fast": state.get(
                "slow_reselect_threshold_above_fast"
            ),
            "active_timeslots": active_slots,
            "usage_markers": usage_list,
            "neighbours": neighbours_list,
            "reported_by": state.get("reported_by"),
            "packet_count": state.get("packet_count", 0),
            "dl_freq": state.get("dl_freq"),
            "ul_freq": state.get("ul_freq"),
            "human_text": human_text,
        }

        try:
            with open(DISCOVERED_NETWORKS_FILE, "a", encoding="utf-8") as f:
                f.write(json.dumps(net_event, ensure_ascii=False) + "\n")
                f.flush()
        except Exception as exc:
            print(f"[parser] Failed writing {DISCOVERED_NETWORKS_FILE}: {exc}", flush=True)

        if mqtt_client is not None:
            payload_str = json.dumps(net_event, ensure_ascii=False)
            mqtt_client.publish(
                f"{MQTT_BASE_TOPIC}/NETWORK", payload_str, qos=0, retain=True
            )

        if emit_log_event and sig_changed:
            cooldown_key = f"LOG_{freq_str}_{state.get('la')}_{len(neighbours_list)}"
            last_log = seen_networks.get(cooldown_key, 0.0)
            if (now_ts - last_log) >= 300.0:
                seen_networks[cooldown_key] = now_ts
                emit_event(net_event)

    def observe_channel_activity(ch, pkt, pdu, service, now_ts):
        is_uplane = (pdu == "TCH_S" or service == "UPLANE")
        freq_str = ch["freq_mhz"]
        state = get_or_init_net_state(
            freq_str, ch["freq_hz"], default_la=ch.get("la"), role="ACTIVE CELL"
        )
        if is_uplane and not state.get("control_verified"):
            return

        if not is_uplane:
            state["control_verified"] = True
            state["packet_count"] = int(state.get("packet_count", 0)) + 1
            if pdu:
                state["last_pdu"] = pdu
            elif service:
                state["last_pdu"] = service

        tn = to_int(get_ci(pkt, ["tn", "timeslot"]))
        if tn is not None and 1 <= tn <= 4:
            state["active_timeslots"].add(tn)

        um = to_int(
            get_ci(pkt, ["downlink usage marker", "usage marker", "usage_marker"])
        )
        if um is not None and um > 0:
            state["usage_markers"].add(um)

        enc_mode = to_int(
            get_ci(pkt, ["encryption mode", "encryption_mode", "encrypted"])
        )
        if enc_mode is not None:
            state["encryption_mode"] = enc_mode

        if state.get("control_verified") and state["packet_count"] >= 3 and (pdu or service):
            register_whitelisted_carrier(
                ch,
                "NETWORK",
                mcc=state.get("mcc"),
                mnc=state.get("mnc"),
                la=state.get("la"),
            )

        if state.get("control_verified"):
            persist_net_state(state, pdu or service, now_ts, emit_log_event=False)

    def handle_network_discovery(ch, pkt, pdu, now_ts):
        freq_str = ch["freq_mhz"]
        state = get_or_init_net_state(
            freq_str, ch["freq_hz"], default_la=ch.get("la"), role="ACTIVE CELL"
        )

        pkt_mcc = to_int(
            get_ci(pkt, ["mcc", "mobile country code", "country code"])
        )
        pkt_mnc = to_int(
            get_ci(pkt, ["mnc", "mobile network code", "network code"])
        )
        pkt_la = to_int(
            get_ci(pkt, ["la", "location area", "cell la"])
        )
        pkt_colour = to_int(get_ci(pkt, ["colour code", "color code", "cc"]))
        pkt_cell_id = to_int(
            get_ci(pkt, ["cell identifier ca", "cell identifier", "cell_id"])
        )
        pkt_cell_load = to_int(get_ci(pkt, ["cell load ca", "cell_load"]))
        pkt_fast_thr = to_int(
            get_ci(pkt, ["fast_reselect_threshold", "fast reselect threshold"])
        )
        pkt_fast_hyst = to_int(
            get_ci(pkt, ["fast_reselect_hysteresis", "fast reselect hysteresis"])
        )
        pkt_slow_hyst = to_int(
            get_ci(pkt, ["slow_reselect hysteresis", "slow_reselect_hysteresis"])
        )
        pkt_slow_thr = to_int(
            get_ci(
                pkt,
                [
                    "slow_reselect_threshold_above_fast",
                    "slow reselect threshold above fast",
                ],
            )
        )
        dl_f = get_ci(pkt, ["dl_freq", "downlink frequency"])
        ul_f = get_ci(pkt, ["ul_freq", "uplink frequency"])

        if pkt_mcc is not None:
            state["mcc"] = pkt_mcc
        if pkt_mnc is not None:
            state["mnc"] = pkt_mnc
        if pkt_colour is not None:
            state["colour_code"] = pkt_colour
        if pkt_la is not None:
            state["la"] = pkt_la
            ch["la"] = pkt_la
        if pkt_cell_id is not None:
            state["cell_id"] = pkt_cell_id
        if pkt_cell_load is not None:
            state["cell_load"] = pkt_cell_load
        if pkt_fast_thr is not None:
            state["fast_reselect_threshold"] = pkt_fast_thr
        if pkt_fast_hyst is not None:
            state["fast_reselect_hysteresis"] = pkt_fast_hyst
        if pkt_slow_hyst is not None:
            state["slow_reselect_hysteresis"] = pkt_slow_hyst
        if pkt_slow_thr is not None:
            state["slow_reselect_threshold_above_fast"] = pkt_slow_thr
        if dl_f is not None:
            state["dl_freq"] = dl_f
        if ul_f is not None:
            state["ul_freq"] = ul_f

        neighbours = extract_neighbour_cells(pkt, ch["freq_hz"])
        for nb in neighbours:
            nb_key = nb.get("freq_mhz") or f"CH_{nb.get('carrier_number')}"
            state["neighbours"][nb_key] = nb

            nb_freq_mhz = nb.get("freq_mhz")
            nb_freq_hz = nb.get("freq_hz")
            if nb_freq_mhz and nb_freq_hz:
                nb_state = get_or_init_net_state(
                    nb_freq_mhz,
                    nb_freq_hz,
                    default_la=nb.get("la"),
                    role="NEIGHBOUR CELL",
                )
                if nb.get("la") is not None:
                    nb_state["la"] = nb["la"]
                    for ch_obj in channels:
                        if ch_obj["freq_mhz"] == nb_freq_mhz:
                            ch_obj["la"] = nb["la"]
                if nb.get("cell_id") is not None:
                    nb_state["cell_id"] = nb["cell_id"]
                if nb.get("cell_load") is not None and nb_state.get("cell_load") is None:
                    nb_state["cell_load"] = nb["cell_load"]
                if nb.get("synchronized") is not None:
                    nb_state["synchronized"] = nb["synchronized"]
                if nb_state.get("mcc") is None and state.get("mcc") is not None:
                    nb_state["mcc"] = state["mcc"]
                if nb_state.get("mnc") is None and state.get("mnc") is not None:
                    nb_state["mnc"] = state["mnc"]
                nb_state["reported_by"] = freq_str

                persist_net_state(
                    nb_state,
                    f"NEIGHBOUR ({pdu})",
                    now_ts,
                    emit_log_event=False,
                )

        register_whitelisted_carrier(
            ch, "NETWORK", mcc=state.get("mcc"), mnc=state.get("mnc"), la=state.get("la")
        )
        persist_net_state(state, pdu, now_ts, emit_log_event=True)

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
        update_unit_registry(ssi, dest_ssi, decoded)
        human_text = format_sds_human_text(
            ssi, dest_ssi, proto_id, decoded, unit_registry
        )

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
            logging.info(
                "[DEBUG] Dropped duplicate SDS on %s MHz (SSI %s -> %s, Proto %s)",
                ch["freq_mhz"],
                ssi,
                dest_ssi,
                proto_id,
            )
            return

        logging.info(
            "[DEBUG] Emitting SDS (%s) on %s MHz (SSI %s -> %s, Proto %s): %s",
            decoded["sds_type"],
            ch["freq_mhz"],
            ssi,
            dest_ssi,
            proto_id,
            human_text,
        )

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
            "unit_context": get_unit_context(ssi, dest_ssi),
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

    def find_codec_binary(name):
        candidates = [
            f"/usr/local/bin/{name}",
            f"/opt/tetra-kit/recorder/wav/{name}",
            f"/opt/tetra-kit/codec/{name}",
        ]
        for path in candidates:
            if os.path.isfile(path) and os.access(path, os.X_OK):
                return path
        return shutil.which(name)

    def cleanup_old_audio_files(max_files=MAX_AUDIO_FILES):
        if max_files <= 0 or not os.path.isdir(AUDIO_DIR):
            return
        try:
            wav_entries = []
            for fname in os.listdir(AUDIO_DIR):
                if not fname.lower().endswith(".wav"):
                    continue
                fpath = os.path.join(AUDIO_DIR, fname)
                try:
                    if os.path.isfile(fpath):
                        wav_entries.append((os.path.getmtime(fpath), fpath))
                except OSError:
                    continue
            if len(wav_entries) <= max_files:
                return
            wav_entries.sort(key=lambda item: item[0])
            to_delete = wav_entries[: len(wav_entries) - max_files]
            for _, fpath in to_delete:
                try:
                    os.remove(fpath)
                except OSError:
                    pass
        except Exception as exc:
            logging.error("[parser] Failed cleaning up old audio files in %s: %s", AUDIO_DIR, exc)

    def decode_and_emit_voice(buf):
        frames = buf.get("frames") or []
        freq_mhz = buf.get("freq_mhz", "?.????")
        la = buf.get("la")
        tn = buf.get("tn")
        usage_marker = to_int(buf.get("usage_marker"))
        if len(frames) < MIN_VOICE_FRAMES:
            if len(frames) >= 3 or buf.get("ssi") is not None or buf.get("gssi") is not None:
                logging.info(
                    "[DEBUG] Dropped Voice on %s MHz (TN %s, UM %s): Too few frames (%d < %d)",
                    freq_mhz,
                    tn,
                    usage_marker,
                    len(frames),
                    MIN_VOICE_FRAMES,
                )
            return

        ssi = to_int(buf.get("ssi"))
        if ssi in (0, 16777215):
            ssi = None
        gssi = to_int(buf.get("gssi"))
        if gssi in (0, 16777215):
            gssi = None

        if ssi is None and gssi is None:
            now_check = time.time()
            late_ctx = (
                (channel_call_context.get((freq_mhz, f"tn:{tn}")) if tn is not None else None)
                or (
                    channel_call_context.get((freq_mhz, usage_marker))
                    if usage_marker
                    else None
                )
                or channel_call_context.get(freq_mhz)
                or (
                    channel_call_context.get(("GLOBAL_UM", usage_marker))
                    if usage_marker
                    else None
                )
                or channel_call_context.get("GLOBAL_LAST_CALL")
            )
            if late_ctx and (now_check - late_ctx.get("ts", 0) <= 120.0):
                c_ssi = to_int(late_ctx.get("ssi"))
                if c_ssi not in (None, 0, 16777215):
                    ssi = c_ssi
                c_gssi = to_int(late_ctx.get("gssi"))
                if c_gssi not in (None, 0, 16777215):
                    gssi = c_gssi

        if ssi is None and gssi is None and not (usage_marker and usage_marker > 0):
            logging.info(
                "[DEBUG] Dropped Voice on %s MHz (TN %s): No valid SSI, GSSI, or Usage Marker (%d frames)",
                freq_mhz,
                tn,
                len(frames),
            )
            return

        if ssi is not None:
            ssi_label = str(ssi)
        elif gssi is not None:
            ssi_label = str(gssi)
        elif usage_marker and usage_marker > 0:
            ssi_label = f"UM{usage_marker}-TN{tn}" if tn is not None else f"UM{usage_marker}"
        else:
            ssi_label = f"TN{tn}" if tn is not None else "UNKNOWN"
        safe_ssi = re.sub(r"[^A-Za-z0-9_-]", "", ssi_label) or "UNKNOWN"

        now_ts = time.time()
        ts_int = int(now_ts)
        ts_ms = int(now_ts * 1000)
        tid = threading.get_ident()
        now_iso = datetime.now(timezone.utc).isoformat()

        raw_chunks = []
        for b64_str in frames:
            try:
                raw_b64 = base64.b64decode(b64_str)
                try:
                    unpacked = zlib.decompress(raw_b64)
                except Exception:
                    unpacked = raw_b64
                if unpacked:
                    raw_chunks.append(unpacked)
            except Exception:
                continue

        if len(raw_chunks) < MIN_VOICE_FRAMES:
            logging.info(
                "[DEBUG] Dropped Voice on %s MHz (SSI %s): Too few decoded payload chunks (%d < %d)",
                freq_mhz,
                ssi_label,
                len(raw_chunks),
                MIN_VOICE_FRAMES,
            )
            return

        out_bytes = b"".join(raw_chunks)
        tmp_base = f"/tmp/voice_{ts_ms}_{tid}_{safe_ssi}"
        out_path = f"{tmp_base}.out"
        cod_path = f"{tmp_base}.cod"
        raw_path = f"{tmp_base}.raw"

        wav_filename = f"voice_{ts_int}_{safe_ssi}.wav"
        wav_path = os.path.join(AUDIO_DIR, wav_filename)

        try:
            os.makedirs(AUDIO_DIR, exist_ok=True)
            with open(out_path, "wb") as f:
                f.write(out_bytes)

            cdecoder_bin = find_codec_binary("cdecoder")
            sdecoder_bin = find_codec_binary("sdecoder")

            if not cdecoder_bin or not sdecoder_bin:
                logging.error(
                    "[parser] ETSI codec binary missing (cdecoder=%s, sdecoder=%s). Dropping %d voice frames on %s MHz.",
                    cdecoder_bin,
                    sdecoder_bin,
                    len(raw_chunks),
                    freq_mhz,
                )
                return

            cdec_res = subprocess.run(
                [cdecoder_bin, out_path, cod_path],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                timeout=20,
                check=False,
            )
            if cdec_res.returncode != 0:
                logging.error(
                    "[parser] cdecoder failed (exit %s) on %s MHz (SSI %s): %s",
                    cdec_res.returncode,
                    freq_mhz,
                    ssi_label,
                    (cdec_res.stderr or cdec_res.stdout or "").strip(),
                )
                return

            if not os.path.exists(cod_path) or os.path.getsize(cod_path) == 0:
                logging.error(
                    "[parser] cdecoder produced empty .cod output for %d frames on %s MHz (SSI %s)",
                    len(raw_chunks),
                    freq_mhz,
                    ssi_label,
                )
                return

            sdec_res = subprocess.run(
                [sdecoder_bin, cod_path, raw_path],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                timeout=20,
                check=False,
            )
            if sdec_res.returncode != 0:
                logging.error(
                    "[parser] sdecoder failed (exit %s) on %s MHz (SSI %s): %s",
                    sdec_res.returncode,
                    freq_mhz,
                    ssi_label,
                    (sdec_res.stderr or sdec_res.stdout or "").strip(),
                )
                return

            if not os.path.exists(raw_path) or os.path.getsize(raw_path) < 960:
                logging.error(
                    "[parser] sdecoder produced insufficient .raw PCM data (%d bytes) on %s MHz (SSI %s)",
                    os.path.getsize(raw_path) if os.path.exists(raw_path) else 0,
                    freq_mhz,
                    ssi_label,
                )
                return

            with open(raw_path, "rb") as rf:
                pcm_source = rf.read()

            pcm_bytes_len = len(pcm_source)
            actual_duration_s = pcm_bytes_len / 16000.0
            if actual_duration_s < MIN_AUDIO_DURATION_SEC:
                logging.info(
                    "[DEBUG] Dropped Voice on %s MHz (SSI %s): Duration %.2fs < %.2fs (%d frames, %d PCM bytes)",
                    freq_mhz,
                    ssi_label,
                    actual_duration_s,
                    MIN_AUDIO_DURATION_SEC,
                    len(frames),
                    pcm_bytes_len,
                )
                return

            if pcm_bytes_len % 2 == 1:
                pcm_source += b"\x00"

            with wave.open(wav_path, "wb") as wf:
                wf.setnchannels(1)
                wf.setsampwidth(2)
                wf.setframerate(8000)
                wf.writeframes(pcm_source)
        except Exception as exc:
            logging.error("[parser] Voice WAV conversion error on %s MHz: %s", freq_mhz, exc)
            return
        finally:
            for p in (out_path, cod_path, raw_path):
                try:
                    if os.path.exists(p):
                        os.remove(p)
                except OSError:
                    pass

        min_wav_bytes = int(MIN_AUDIO_DURATION_SEC * 16000) + 44
        if not os.path.exists(wav_path) or os.path.getsize(wav_path) < min_wav_bytes:
            logging.info(
                "[DEBUG] Dropped Voice on %s MHz (SSI %s): WAV file size %d < %d bytes",
                freq_mhz,
                ssi_label,
                os.path.getsize(wav_path) if os.path.exists(wav_path) else 0,
                min_wav_bytes,
            )
            try:
                if os.path.exists(wav_path):
                    os.remove(wav_path)
            except OSError:
                pass
            return

        cleanup_old_audio_files(MAX_AUDIO_FILES)

        duration_s = round(pcm_bytes_len / 16000.0, 1)
        logging.info(
            "[DEBUG] Emitting VOICE on %s MHz: file=%s duration=%.1fs frames=%d SSI=%s GSSI=%s UM=%s TN=%s",
            freq_mhz,
            wav_filename,
            duration_s,
            len(frames),
            ssi,
            gssi,
            usage_marker,
            tn,
        )
        badge = format_unit_badge(ssi, unit_registry) if ssi is not None else ""
        extra_parts = []
        if gssi is not None:
            extra_parts.append(f"Group {gssi}")
        if tn is not None:
            extra_parts.append(f"TN {tn}")
        extra_parts.append(f"{duration_s}s")
        extra_str = f" ({', '.join(extra_parts)})" if extra_parts else ""

        human_text = f"Voice Call from SSI {ssi_label}{badge}{extra_str}"
        audio_url = f"/api/audio/{wav_filename}"

        voice_event = {
            "time": now_iso,
            "timestamp": now_iso,
            "host": HOST_NAME,
            "category": "VOICE",
            "service": "UPLANE",
            "pdu": "TCH_S",
            "freq_mhz": freq_mhz,
            "la": la,
            "ssi": ssi,
            "gssi": gssi,
            "tn": tn,
            "frame_count": len(frames),
            "duration_s": duration_s,
            "audio_url": audio_url,
            "human_text": human_text,
        }
        emit_event(voice_event)

    def flush_voice_buffer(key, usage_marker=None, ssi=None, gssi=None):
        to_decode = []
        freq_match = key[0] if isinstance(key, tuple) and key else str(key)
        with voice_lock:
            if key in voice_buffers:
                to_decode.append(voice_buffers.pop(key))
            else:
                for b_key, buf in list(voice_buffers.items()):
                    if b_key[0] != freq_match:
                        continue
                    if (
                        (usage_marker is not None and buf.get("usage_marker") == usage_marker)
                        or (ssi is not None and buf.get("ssi") == ssi)
                        or (gssi is not None and buf.get("gssi") == gssi)
                        or (usage_marker is None and ssi is None and gssi is None)
                    ):
                        to_decode.append(voice_buffers.pop(b_key))
                if not to_decode:
                    for b_key in list(voice_buffers.keys()):
                        if b_key[0] == freq_match:
                            to_decode.append(voice_buffers.pop(b_key))

        for buf in to_decode:
            if len(buf.get("frames") or []) >= MIN_VOICE_FRAMES:
                threading.Thread(
                    target=decode_and_emit_voice,
                    args=(buf,),
                    daemon=True,
                ).start()

    def flush_expired_voice_buffers(now_ts, force=False):
        ready = []
        with voice_lock:
            for k, buf in list(voice_buffers.items()):
                if force or (now_ts - buf["last_seen"] >= 2.0):
                    ready.append(voice_buffers.pop(k))
        for buf in ready:
            decode_and_emit_voice(buf)

    def voice_monitor_worker():
        last_cleanup = 0.0
        while running:
            time.sleep(0.5)
            now_ts = time.time()
            try:
                flush_expired_voice_buffers(now_ts, force=False)
                if now_ts - last_cleanup >= 30.0:
                    cleanup_old_audio_files(MAX_AUDIO_FILES)
                    last_cleanup = now_ts
            except Exception as exc:
                logging.error("[parser] Error in voice_monitor_worker: %s", exc)

    voice_thread = threading.Thread(target=voice_monitor_worker, daemon=True)
    voice_thread.start()

    def emit_raw_mac_event(ch, pkt):
        try:
            now_iso = datetime.now(timezone.utc).isoformat()
            pdu_val = str(get_ci(pkt, ["pdu", "type", "burst"], "UNKNOWN") or "UNKNOWN").upper()
            srv_val = str(get_ci(pkt, ["service", "layer"], "MAC") or "MAC").upper()
            raw_dump = json.dumps(pkt, ensure_ascii=False, sort_keys=True)
            if len(raw_dump) > 600:
                raw_dump = raw_dump[:600] + "..."
            human_text = f"PDU: {pdu_val} | Service: {srv_val} | Data: {raw_dump}"
            raw_event = {
                "time": now_iso,
                "timestamp": now_iso,
                "host": HOST_NAME,
                "category": "RAW_MAC",
                "pdu": pdu_val,
                "service": srv_val,
                "freq_mhz": ch.get("freq_mhz", "?.????"),
                "la": ch.get("la"),
                "human_text": human_text,
            }
            with open(LOG_FILE, "a", encoding="utf-8") as f:
                f.write(json.dumps(raw_event, ensure_ascii=False) + "\n")
                f.flush()
        except Exception:
            pass

    debug_drop_log_ts = {}

    def log_throttled_debug(key, msg, interval_s=5.0):
        now_t = time.time()
        if now_t - debug_drop_log_ts.get(key, 0.0) >= interval_s:
            debug_drop_log_ts[key] = now_t
            logging.info(msg)

    def process_packet(ch, pkt):
        freq_str = ch["freq_mhz"]
        service = str(get_ci(pkt, ["service"], "")).upper()
        pdu = str(get_ci(pkt, ["pdu"], "")).upper()
        now_ts = time.time()

        if pdu != "TCH_S" and service != "UPLANE":
            stats_60s[freq_str] = stats_60s.get(freq_str, 0) + 1

        observe_channel_activity(ch, pkt, pdu, service, now_ts)

        if pdu == "TCH_S" or (service == "UPLANE" and get_ci(pkt, ["frame"]) is not None):
            chan_verified = bool(channel_net_cache.get(freq_str, {}).get("control_verified"))
            raw_pkt_ssi = to_int(
                get_ci(
                    pkt,
                    [
                        "actual ssi",
                        "ssi",
                        "calling party ssi",
                        "transmitting party ssi",
                        "source ssi",
                    ],
                )
            )
            if raw_pkt_ssi == 16777215:
                log_throttled_debug(
                    (freq_str, "dummy_16777215"),
                    f"[DEBUG] Dropped TCH_S on {freq_str} MHz: Dummy broadcast SSI 16777215",
                )
                if chan_verified:
                    emit_raw_mac_event(ch, pkt)
                return

            pkt_ssi = None if raw_pkt_ssi in (0, 16777215) else raw_pkt_ssi

            enc_mode = to_int(
                get_ci(pkt, ["encryption mode", "encryption_mode", "encrypted"]),
                default=0,
            )
            frame_b64 = get_ci(pkt, ["frame"])
            if enc_mode != 0:
                log_throttled_debug(
                    (freq_str, f"enc_{enc_mode}"),
                    f"[DEBUG] Dropped TCH_S on {freq_str} MHz: Encrypted (enc_mode={enc_mode})",
                )
                if chan_verified:
                    emit_raw_mac_event(ch, pkt)
                return

            emit_this_uplane_raw = chan_verified
            if isinstance(frame_b64, str) and frame_b64.strip():
                tn = to_int(get_ci(pkt, ["tn", "timeslot"]))
                dl_um = to_int(get_ci(pkt, ["downlink usage marker"]), default=0) or 0
                ul_um = to_int(get_ci(pkt, ["usage marker", "usage_marker"]), default=0) or 0
                usage_marker = max(dl_um, ul_um)

                ctx = None
                if tn is not None:
                    ctx = channel_call_context.get((freq_str, f"tn:{tn}"))
                if not ctx and usage_marker > 0:
                    ctx = channel_call_context.get((freq_str, usage_marker))
                if not ctx:
                    ctx = channel_call_context.get(freq_str)
                if not ctx and usage_marker > 0:
                    ctx = channel_call_context.get(("GLOBAL_UM", usage_marker))
                if not ctx:
                    ctx = channel_call_context.get("GLOBAL_LAST_CALL")
                if ctx and (now_ts - ctx.get("ts", 0) > 120.0):
                    ctx = None

                ctx_ssi = to_int(ctx.get("ssi")) if ctx else None
                if ctx_ssi in (0, 16777215):
                    ctx_ssi = None
                ctx_gssi = to_int(ctx.get("gssi")) if ctx else None
                if ctx_gssi in (0, 16777215):
                    ctx_gssi = None

                eff_ssi = pkt_ssi if pkt_ssi is not None else ctx_ssi
                eff_gssi = ctx_gssi

                if eff_ssi is None and eff_gssi is None and usage_marker <= 0:
                    log_throttled_debug(
                        (freq_str, f"no_ctx_tn_{tn}"),
                        f"[DEBUG] Dropped TCH_S on {freq_str} MHz (TN {tn}): No call context and usage_marker=0 (raw_ssi={raw_pkt_ssi})",
                    )
                    if chan_verified:
                        emit_raw_mac_event(ch, pkt)
                    return

                buf_key = (freq_str, tn if tn is not None else (usage_marker or eff_ssi or eff_gssi or 1))
                with voice_lock:
                    buf = voice_buffers.get(buf_key)
                    if buf is not None and (now_ts - buf["last_seen"] > VOICE_SPLIT_TIMEOUT_S):
                        old_buf = voice_buffers.pop(buf_key)
                        if len(old_buf.get("frames", [])) >= MIN_VOICE_FRAMES:
                            threading.Thread(
                                target=decode_and_emit_voice,
                                args=(old_buf,),
                                daemon=True,
                            ).start()
                        buf = None

                    if buf is not None and buf.get("ssi") is None and buf.get("gssi") is None:
                        if (
                            (usage_marker > 0 and buf.get("usage_marker") != usage_marker)
                            or (now_ts - buf["last_seen"] > 0.45)
                        ) and len(buf.get("frames", [])) < MIN_VOICE_FRAMES:
                            voice_buffers.pop(buf_key, None)
                            buf = None

                    if buf is None:
                        buf = {
                            "freq_mhz": freq_str,
                            "la": ch.get("la"),
                            "tn": tn,
                            "usage_marker": usage_marker,
                            "ssi": eff_ssi,
                            "gssi": eff_gssi,
                            "frames": [],
                            "first_seen": now_ts,
                            "last_seen": now_ts,
                            "logged_start": False,
                        }
                        voice_buffers[buf_key] = buf
                    else:
                        buf["last_seen"] = now_ts
                        if buf.get("ssi") is None and eff_ssi is not None:
                            buf["ssi"] = eff_ssi
                        if buf.get("gssi") is None and eff_gssi is not None:
                            buf["gssi"] = eff_gssi
                        if not buf.get("usage_marker") and usage_marker > 0:
                            buf["usage_marker"] = usage_marker
                    buf["frames"].append(frame_b64.strip())
                    if (
                        len(buf["frames"]) >= 3
                        or buf.get("ssi") is not None
                        or buf.get("gssi") is not None
                    ):
                        emit_this_uplane_raw = True
                        if not buf.get("logged_start"):
                            buf["logged_start"] = True
                            logging.info(
                                "[DEBUG] Started TCH_S voice buffer on %s MHz (TN %s, UM %s, SSI %s, GSSI %s)",
                                freq_str,
                                tn,
                                buf.get("usage_marker"),
                                buf.get("ssi"),
                                buf.get("gssi"),
                            )
                    if len(buf["frames"]) >= 3:
                        stats_60s[freq_str] = stats_60s.get(freq_str, 0) + 1

            if emit_this_uplane_raw:
                emit_raw_mac_event(ch, pkt)
            return

        if service == "UPLANE":
            if channel_net_cache.get(freq_str, {}).get("control_verified"):
                emit_raw_mac_event(ch, pkt)
            return
        if pdu in IGNORED_PDUS:
            emit_raw_mac_event(ch, pkt)
            return

        flush_expired_sds(now_ts)

        if pdu in NETWORK_PDUS:
            handle_network_discovery(ch, pkt, pdu, now_ts)
            emit_raw_mac_event(ch, pkt)
            return

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

        if mac_ssi not in (None, 0, 16777215) or caller_ssi not in (None, 0, 16777215):
            pkt_tn = to_int(get_ci(pkt, ["tn", "timeslot", "allocated timeslot"]))
            pkt_dl_um = to_int(get_ci(pkt, ["downlink usage marker"]), default=0) or 0
            pkt_ul_um = to_int(get_ci(pkt, ["usage marker", "usage_marker"]), default=0) or 0
            pkt_um = max(pkt_dl_um, pkt_ul_um)
            eff_mac_ssi = caller_ssi if caller_ssi not in (None, 0, 16777215) else mac_ssi
            eff_mac_gssi = (
                mac_ssi
                if (mac_ssi not in (None, 0, 16777215) and 3000000 <= mac_ssi <= 3999999)
                else None
            )
            mac_ctx_obj = {
                "ssi": eff_mac_ssi,
                "gssi": eff_mac_gssi,
                "usage_marker": pkt_um if pkt_um > 0 else None,
                "tn": pkt_tn,
                "ts": now_ts,
            }
            if pkt_um > 0:
                channel_call_context[(freq_str, pkt_um)] = mac_ctx_obj
                channel_call_context[("GLOBAL_UM", pkt_um)] = mac_ctx_obj
            if pkt_tn is not None:
                existing_tn_ctx = channel_call_context.get((freq_str, f"tn:{pkt_tn}"))
                if not existing_tn_ctx or (now_ts - existing_tn_ctx.get("ts", 0) > 15.0):
                    channel_call_context[(freq_str, f"tn:{pkt_tn}")] = mac_ctx_obj

        if pdu == "D-SDS-DATA" or "SDS" in pdu:
            has_hex = get_ci(pkt, ["hex"]) is not None
            has_infos = get_ci(pkt, ["infos"]) is not None
            key = (freq_str, mac_ssi if mac_ssi is not None else caller_ssi)
            if not has_hex and not has_infos:
                if key in pending_sds:
                    prev = pending_sds.pop(key)
                    build_and_emit_sds(prev["ch"], prev["pkt"], None)
                logging.info(
                    "[DEBUG] Buffered SDS header on %s MHz (SSI %s, Caller %s) waiting for payload...",
                    freq_str,
                    mac_ssi,
                    caller_ssi,
                )
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

            src_badge = format_unit_badge(src_ssi, unit_registry)
            dst_badge = format_unit_badge(dst_ssi, unit_registry)
            target_part = f" -> SSI {dst_ssi}{dst_badge}" if dst_ssi is not None else ""
            human_text = (
                f"Status from SSI {src_ssi}{src_badge}{target_part}: {status_text}"
            )

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
                "unit_context": get_unit_context(src_ssi, dst_ssi),
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
            call_tn = to_int(get_ci(pkt, ["tn", "timeslot", "allocated timeslot"]))
            tx_grant_code = to_int(get_ci(pkt, ["transmission grant"]))
            tx_grant_text = TX_GRANT_MAP.get(tx_grant_code)
            disc_cause_code = to_int(get_ci(pkt, ["disconnect cause"]))
            disc_cause_text = DISCONNECT_CAUSE_MAP.get(disc_cause_code)

            if mac_ssi in (0, 16777215):
                mac_ssi = None
            if caller_ssi in (0, 16777215):
                caller_ssi = None

            explicit_gssi = to_int(
                get_ci(
                    pkt,
                    [
                        "gssi",
                        "group ssi",
                        "called party ssi",
                        "called party address ssi",
                    ],
                )
            )
            if explicit_gssi in (0, 16777215):
                explicit_gssi = None

            if explicit_gssi is not None:
                gssi = explicit_gssi
            elif mac_ssi is not None and (
                3000000 <= mac_ssi <= 3999999 or caller_ssi is not None
            ):
                gssi = mac_ssi
            else:
                gssi = None

            ssi = caller_ssi if caller_ssi is not None else mac_ssi

            if ssi is not None or gssi is not None:
                call_ctx_obj = {
                    "ssi": caller_ssi if caller_ssi is not None else ssi,
                    "gssi": gssi,
                    "call_id": call_id,
                    "usage_marker": usage_marker,
                    "tn": call_tn,
                    "ts": now_ts,
                }
                channel_call_context[freq_str] = call_ctx_obj
                channel_call_context["GLOBAL_LAST_CALL"] = call_ctx_obj
                if usage_marker is not None and usage_marker > 0:
                    channel_call_context[(freq_str, usage_marker)] = call_ctx_obj
                    channel_call_context[("GLOBAL_UM", usage_marker)] = call_ctx_obj
                if call_tn is not None:
                    channel_call_context[(freq_str, f"tn:{call_tn}")] = call_ctx_obj
                with voice_lock:
                    for b_key, b_val in voice_buffers.items():
                        if b_key[0] == freq_str or (
                            usage_marker and b_val.get("usage_marker") == usage_marker
                        ):
                            if b_val.get("ssi") is None and call_ctx_obj["ssi"] is not None:
                                b_val["ssi"] = call_ctx_obj["ssi"]
                            if b_val.get("gssi") is None and gssi is not None:
                                b_val["gssi"] = gssi

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

            grp_label = (
                f"Group {gssi}"
                if gssi is not None
                else f"SSI {ssi}{format_unit_badge(ssi, unit_registry)}"
            )
            speaker_badge = format_unit_badge(caller_ssi, unit_registry)
            speaker_label = (
                f" by SSI {caller_ssi}{speaker_badge}"
                if (caller_ssi is not None and caller_ssi != gssi)
                else ""
            )
            cid_label = f" (Call ID {call_id})" if call_id is not None else ""

            if pdu == "D-SETUP":
                human_text = f"{grp_label}: Call setup{speaker_label}{cid_label}"
            elif pdu == "D-TX GRANTED":
                grant_info = f" [{tx_grant_text}]" if tx_grant_text else ""
                human_text = (
                    f"{grp_label}: PTT TX granted{speaker_label}{grant_info}{cid_label}"
                )
            elif pdu == "D-TX CEASED":
                dur_info = (
                    f" after {tx_duration_s}s" if tx_duration_s is not None else ""
                )
                human_text = (
                    f"{grp_label}: PTT TX ended{speaker_label}{dur_info}{cid_label}"
                )
            elif pdu == "D-RELEASE":
                cause_info = f" ({disc_cause_text})" if disc_cause_text else ""
                dur_info = (
                    f" after {tx_duration_s}s" if tx_duration_s is not None else ""
                )
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
                "unit_context": get_unit_context(caller_ssi, ssi),
                "call_identifier": call_id,
                "usage_marker": usage_marker,
                "tx_grant": tx_grant_text,
                "tx_duration_s": tx_duration_s,
                "disconnect_cause": disc_cause_text,
                "human_text": human_text,
            }
            emit_event(event, raw_pkt=pkt)
            if pdu in ("D-TX CEASED", "D-RELEASE"):
                flush_voice_buffer(
                    (freq_str, usage_marker or ssi or 1),
                    usage_marker=usage_marker,
                    ssi=ssi,
                    gssi=gssi,
                )
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

            badge = format_unit_badge(ssi, unit_registry)
            gssi_part = f" (Group {gssi})" if gssi is not None else ""
            human_text = f"Roaming: SSI {ssi}{badge}{gssi_part} registered on LA {ch['la']} ({freq_str} MHz) [{pdu}]"
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
                "unit_context": get_unit_context(ssi),
                "human_text": human_text,
            }
            emit_event(event, raw_pkt=pkt)
            return

        emit_raw_mac_event(ch, pkt)

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

        total_60s = sum(stats_60s.values())
        hb_interval = 60.0
        if now_ts - last_heartbeat >= hb_interval:
            first_hb_sent = True
            hb_payload = {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "host": HOST_NAME,
                "category": "HEARTBEAT",
                "version": APP_VERSION,
                "total_packets_60s": total_60s,
                "known_units": len(unit_registry),
                "discovered_networks": len(seen_networks),
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
                f"[heartbeat] 60s total packets: {total_60s} | known units: {len(unit_registry)} | networks: {len(seen_networks)} | per channel: {json.dumps(stats_60s)}",
                flush=True,
            )
            for k in stats_60s:
                stats_60s[k] = 0
            last_heartbeat = now_ts

    flush_expired_sds(time.time(), force=True)
    flush_expired_voice_buffers(time.time(), force=True)
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
