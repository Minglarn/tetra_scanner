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


APP_VERSION = os.environ.get("APP_VERSION", "2026.11.03.1")

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
    "D-RELEASE",
}

STATUS_MAP = {
    65024: "Heartbeat / ACK (65024)",
    0: "NÖDLARM / Emergency",
}


def parse_channels(channels_str):
    channels = []
    entries = [c.strip() for c in channels_str.split(",") if c.strip()]
    for idx, entry in enumerate(entries, start=1):
        parts = entry.split(":")
        freq_hz = int(parts[0])
        la = int(parts[1]) if len(parts) > 1 and parts[1].isdigit() else parts[1] if len(parts) > 1 else idx
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
    payload = raw_bytes[1:] if len(raw_bytes) >= 8 and raw_bytes[0] == 0x0A else raw_bytes
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


def decode_sds_payload(proto_id, hex_str, infos_list):
    sds_type = "BINARY"
    sds_text = None
    sds_binary_tail = None
    gps_lat = None
    gps_lon = None

    infos_text = None
    if isinstance(infos_list, list) and infos_list:
        extracted = []
        for item in infos_list:
            if isinstance(item, dict):
                for k, v in item.items():
                    extracted.append(f"{k}: {v}" if k != "text" else str(v))
            elif isinstance(item, str):
                extracted.append(item)
        if extracted:
            infos_text = " | ".join(extracted)
    elif isinstance(infos_list, str) and infos_list.strip():
        infos_text = infos_list.strip()

    raw_bytes = b""
    clean_hex = ""
    if hex_str:
        clean_hex = re.sub(r"[^0-9a-fA-F]", "", str(hex_str))
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
        elif infos_text:
            sds_text = infos_text
    elif proto_id == 130:
        sds_type = "TEXT"
        if infos_text:
            sds_text = infos_text
        elif raw_bytes:
            ascii_str = extract_ascii_strings(raw_bytes, min_len=3)
            sds_text = ascii_str if ascii_str else clean_hex
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
    else:
        if infos_text:
            sds_type = "TEXT"
            sds_text = infos_text
        elif raw_bytes:
            ascii_str = extract_ascii_strings(raw_bytes, min_len=3)
            if ascii_str:
                sds_type = "TEXT"
                sds_text = ascii_str
            else:
                sds_type = "BINARY"

    return {
        "sds_type": sds_type,
        "sds_text": sds_text,
        "sds_hex": clean_hex if clean_hex else None,
        "sds_binary_tail": sds_binary_tail,
        "gps_lat": gps_lat,
        "gps_lon": gps_lon,
    }


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
            "value_template": "{{ value_json.sds_text | default(value_json.sds_hex) }}",
        },
        {
            "id": "last_call",
            "name": "Last Call",
            "state_topic": f"{MQTT_BASE_TOPIC}/CALL",
            "value_template": "{{ value_json.pdu }} | SSI {{ value_json.ssi }}",
        },
        {
            "id": "last_status",
            "name": "Last Status",
            "state_topic": f"{MQTT_BASE_TOPIC}/STATUS",
            "value_template": "{{ value_json.status_text }}",
        },
        {
            "id": "last_roaming",
            "name": "Last Roaming",
            "state_topic": f"{MQTT_BASE_TOPIC}/ROAMING",
            "value_template": "SSI {{ value_json.ssi }} (LA{{ value_json.la }})",
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


def get_first(pkt, keys, default=None):
    for k in keys:
        if k in pkt and pkt[k] is not None:
            return pkt[k]
    return default


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
    stats_60s = {ch["freq_mhz"]: 0 for ch in channels}
    last_heartbeat = time.time()
    running = True

    def handle_sig(signum, frame):
        nonlocal running
        running = False

    signal.signal(signal.SIGINT, handle_sig)
    signal.signal(signal.SIGTERM, handle_sig)

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
            mqtt_client.publish(f"{MQTT_BASE_TOPIC}/event", payload_str, qos=0, retain=False)
            mqtt_client.publish(f"{MQTT_BASE_TOPIC}/{cat}", payload_str, qos=0, retain=True)

        send_udp_json(event_with_opt_raw)

        cat = event_dict.get("category", "OTHER")
        freq = event_dict.get("freq_mhz", "?")
        la = event_dict.get("la", "?")
        pdu = event_dict.get("pdu", "?")
        ssi = event_dict.get("ssi")
        summary = f"[{cat}] {freq} MHz (LA{la}) | PDU={pdu} | SSI={ssi}"
        if cat == "SDS":
            summary += (
                f" -> Dst={event_dict.get('dest_ssi')} Proto={event_dict.get('protocol_id')} "
                f"Type={event_dict.get('sds_type')} Text={event_dict.get('sds_text') or event_dict.get('sds_hex')}"
            )
        elif cat == "STATUS":
            summary += f" | Status={event_dict.get('status_text')}"
        elif cat == "ROAMING":
            summary += f" | GSSI={event_dict.get('gssi')}"
        print(summary, flush=True)

    def build_and_emit_sds(ch, pkt1, pkt2=None):
        now_iso = datetime.now(timezone.utc).isoformat()
        merged_raw = dict(pkt1)
        if pkt2:
            merged_raw.update(pkt2)

        ssi = get_first(pkt1, ["calling party ssi", "ssi", "issi", "calling_ssi"])
        if ssi is None and pkt2:
            ssi = get_first(pkt2, ["calling party ssi", "ssi", "issi", "calling_ssi"])

        dest_ssi = get_first(pkt1, ["forward address ssi", "called party ssi", "dest_ssi", "gssi"])
        proto_id = get_first(pkt1, ["protocol id", "protocol_id"], default=0)
        try:
            proto_id = int(proto_id)
        except (ValueError, TypeError):
            proto_id = 0

        msg_ref = get_first(pkt1, ["message reference", "message_reference"])
        sds_pdu = get_first(pkt1, ["sds-pdu", "sds_pdu"])
        infos = get_first(pkt1, ["infos"])
        hex_str = pkt2.get("hex") if pkt2 and "hex" in pkt2 else pkt1.get("hex")

        decoded = decode_sds_payload(proto_id, hex_str, infos)

        event = {
            "timestamp": now_iso,
            "host": HOST_NAME,
            "category": "SDS",
            "service": get_first(pkt1, ["service"], "MLE"),
            "pdu": get_first(pkt1, ["pdu"], "D-SDS-DATA"),
            "freq_mhz": ch["freq_mhz"],
            "la": ch["la"],
            "ssi": ssi,
            "dest_ssi": dest_ssi,
            "protocol_id": proto_id,
            "message_reference": msg_ref,
            "sds_pdu": sds_pdu,
            "sds_type": decoded["sds_type"],
            "sds_text": decoded["sds_text"],
            "sds_hex": decoded["sds_hex"],
            "sds_binary_tail": decoded["sds_binary_tail"],
            "gps_lat": decoded["gps_lat"],
            "gps_lon": decoded["gps_lon"],
        }
        emit_event(event, raw_pkt=merged_raw)

    def flush_expired_sds(now_ts, force=False):
        expired_keys = [
            k
            for k, val in pending_sds.items()
            if force or (now_ts - val["ts"] > 2.0)
        ]
        for k in expired_keys:
            entry = pending_sds.pop(k)
            build_and_emit_sds(entry["ch"], entry["pkt"], None)

    def process_packet(ch, pkt):
        freq_str = ch["freq_mhz"]
        stats_60s[freq_str] = stats_60s.get(freq_str, 0) + 1

        service = str(pkt.get("service", "")).upper()
        pdu = str(pkt.get("pdu", "")).upper()

        if service == "UPLANE":
            return
        if pdu in IGNORED_PDUS:
            return

        now_ts = time.time()
        flush_expired_sds(now_ts)

        ssi = get_first(pkt, ["calling party ssi", "ssi", "issi", "called party ssi"])

        if pdu == "D-SDS-DATA":
            has_hex = "hex" in pkt and pkt["hex"] is not None
            key = (freq_str, ssi)
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
                    if pend_key[0] == freq_str and (now_ts - pending_sds[pend_key]["ts"] <= 2.0):
                        prev = pending_sds.pop(pend_key)
                        build_and_emit_sds(ch, prev["pkt"], pkt)
                        return
                build_and_emit_sds(ch, pkt, pkt)
                return

        now_iso = datetime.now(timezone.utc).isoformat()

        if pdu == "D-STATUS":
            status_code = get_first(
                pkt, ["pre-coded status", "status", "status_code"], default=-1
            )
            try:
                status_code = int(status_code)
            except (ValueError, TypeError):
                pass
            status_text = STATUS_MAP.get(status_code, f"Status ({status_code})")
            event = {
                "timestamp": now_iso,
                "host": HOST_NAME,
                "category": "STATUS",
                "service": service,
                "pdu": pdu,
                "freq_mhz": freq_str,
                "la": ch["la"],
                "ssi": ssi,
                "dest_ssi": get_first(pkt, ["called party ssi", "forward address ssi", "gssi"]),
                "status_code": status_code,
                "status_text": status_text,
            }
            emit_event(event, raw_pkt=pkt)
            return

        if pdu in CALL_PDUS:
            event = {
                "timestamp": now_iso,
                "host": HOST_NAME,
                "category": "CALL",
                "service": service,
                "pdu": pdu,
                "freq_mhz": freq_str,
                "la": ch["la"],
                "ssi": ssi,
                "gssi": get_first(pkt, ["gssi", "called party ssi", "group ssi"]),
                "call_identifier": get_first(pkt, ["call identifier", "call_identifier"]),
                "usage_marker": get_first(pkt, ["usage marker", "usage_marker"]),
            }
            emit_event(event, raw_pkt=pkt)
            return

        if "LOCATION UPDATE" in pdu:
            event = {
                "timestamp": now_iso,
                "host": HOST_NAME,
                "category": "ROAMING",
                "service": service,
                "pdu": pdu,
                "freq_mhz": freq_str,
                "la": ch["la"],
                "ssi": ssi,
                "gssi": get_first(pkt, ["gssi", "group identity", "group ssi"]),
            }
            emit_event(event, raw_pkt=pkt)
            return

        event = {
            "timestamp": now_iso,
            "host": HOST_NAME,
            "category": "OTHER",
            "service": service,
            "pdu": pdu,
            "freq_mhz": freq_str,
            "la": ch["la"],
            "ssi": ssi,
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
        mqtt_client.publish(f"{MQTT_BASE_TOPIC}/status", payload="offline", qos=1, retain=True)
        mqtt_client.loop_stop()
        mqtt_client.disconnect()
    for s in rx_sockets:
        s.close()
    if udp_out_sock is not None:
        udp_out_sock.close()
    print("[parser] Parser stopped cleanly.", flush=True)


if __name__ == "__main__":
    main()
