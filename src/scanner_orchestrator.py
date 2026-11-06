#!/usr/bin/env python3
import os
import re
import sys
import csv
import glob
import json
import time
import fcntl
import queue
import ctypes
import signal
import atexit
import threading
import subprocess
import statistics
from datetime import datetime, timezone

_SRC_DIR = os.path.dirname(os.path.abspath(__file__))
if _SRC_DIR not in sys.path:
    sys.path.insert(0, _SRC_DIR)
import db


def parse_env_int(key, default):
    raw = os.environ.get(key, str(default))
    clean = str(raw).split("#", 1)[0].strip()
    try:
        return int(float(clean))
    except ValueError:
        return int(default)


def parse_env_float(key, default):
    raw = os.environ.get(key, str(default))
    clean = str(raw).split("#", 1)[0].strip()
    try:
        return float(clean)
    except ValueError:
        return float(default)


def parse_env_str(key, default):
    raw = os.environ.get(key, str(default))
    return str(raw).split("#", 1)[0].strip() or str(default)


LOG_DIR = os.environ.get("LOG_DIR", "/app/logs")
EVENTS_LOG_FILE = os.path.join(LOG_DIR, "tetra_events.jsonl")
DISCOVERED_NETWORKS_FILE = os.path.join(LOG_DIR, "discovered_networks.jsonl")
BLACKLIST_FILE = os.path.join(LOG_DIR, "blacklist.json")
WHITELIST_FILE = os.path.join(LOG_DIR, "whitelist.json")
RUNTIME_CONFIG_FILE = os.path.join(LOG_DIR, "runtime_config.json")

FORCE_SWEEP_FLAG = "/tmp/force_sweep.flag"
RELOAD_CONFIG_FLAG = "/tmp/reload_config.flag"
SOFT_CONFIG_FLAG = "/tmp/soft_config.flag"

FORCE_SWEEP_PENALTY_SEC = 900
MAX_PEAKS = 8
PTS_GRID_HZ = 25000
PTS_GRID_12K5_HZ = 12500
MIN_PEAK_SEPARATION_HZ = 25000
LOCAL_PEAK_RADIUS_HZ = 25000
DC_SPIKE_GUARD_HZ = 10000
EDGE_GUARD_HZ = 15000
MAX_WINDOW_SPAN_HZ = 1800000
MIN_SNR_ABOVE_FLOOR_DB = parse_env_float("SCAN_MIN_SNR_DB", 1.8)
ABSOLUTE_MIN_DBM = parse_env_float("SCAN_MIN_DBM", -75.0)


def compute_safe_center_hz(freq_hz_list, min_dc_guard_hz=25000):
    if not freq_hz_list:
        return 427750000
    freqs = sorted(set(int(f) for f in freq_hz_list if f))
    if not freqs:
        return 427750000
    min_f = freqs[0]
    max_f = freqs[-1]
    raw_center = (min_f + max_f) // 2
    base_center = int(round(raw_center / 12500.0) * 12500)

    for guard_hz in (37500, min_dc_guard_hz):
        for step in range(0, 40):
            for sign in (1, -1) if step > 0 else (1,):
                cand = base_center + sign * step * 12500
                if all(abs(cand - f) >= guard_hz for f in freqs):
                    if (cand - min_f) <= 950000 and (max_f - cand) <= 950000:
                        return cand
    for step in range(0, 40):
        for sign in (1, -1) if step > 0 else (1,):
            cand = base_center + sign * step * 12500
            if all(abs(cand - f) >= min_dc_guard_hz for f in freqs):
                return cand
    return base_center + 50000

PAYLOAD_TAGS = ("[SDS]", "[CALL]", "[VOICE]", "[STATUS]", "[ROAMING]")

ACTIVE_PROCS = []
RUNNING = True


def parse_env_device(key, default="0"):
    raw = os.environ.get(key, str(default))
    clean = str(raw).split("#", 1)[0].strip()
    if not clean:
        clean = str(default)
    if clean.isdigit() and len(clean) < 4:
        return int(clean)
    return clean


def get_default_config():
    return {
        "RTL_DEVICE_INDEX": parse_env_device("RTL_DEVICE_INDEX", "0"),
        "RTL_SERIAL": parse_env_str("RTL_SERIAL", ""),
        "RTL_GAIN": parse_env_float("RTL_GAIN", 28.0),
        "RTL_PPM": parse_env_int("RTL_PPM", 0),
        "MODE": parse_env_str("MODE", "scanner").lower(),
        "SCAN_START_MHZ": parse_env_float("SCAN_START_MHZ", 420.0),
        "SCAN_END_MHZ": parse_env_float("SCAN_END_MHZ", 430.0),
        "SCAN_DWELL_TIME": parse_env_int("SCAN_DWELL_TIME", 120),
        "IDLE_TIMEOUT_SEC": parse_env_int(
            "IDLE_TIMEOUT_SEC", parse_env_int("SCAN_DWELL_TIME", 900)
        ),
        "BLACKLIST_STRIKES": max(1, parse_env_int("BLACKLIST_STRIKES", 3)),
        "BLACKLIST_TIME_SEC": parse_env_int("BLACKLIST_TIME_SEC", 3600),
        "CENTER_FREQ": parse_env_int("CENTER_FREQ", 427750000),
        "TETRA_CHANNELS": parse_env_str(
            "TETRA_CHANNELS",
            "427000000:71,427300000:17,427550000:23,427850000:1,427950000:16,428500000:7",
        ),
    }


def load_runtime_config():
    cfg = get_default_config()
    if os.path.exists(RUNTIME_CONFIG_FILE):
        try:
            with open(RUNTIME_CONFIG_FILE, "r", encoding="utf-8") as f:
                saved = json.load(f)
                if isinstance(saved, dict):
                    cfg.update(saved)
        except Exception as exc:
            print(f"[scanner] Warning: failed reading {RUNTIME_CONFIG_FILE}: {exc}", flush=True)
    else:
        save_runtime_config(cfg)

    raw_dev = str(cfg.get("RTL_DEVICE_INDEX", "0")).strip()
    if raw_dev.isdigit() and len(raw_dev) < 4:
        cfg["RTL_DEVICE_INDEX"] = int(raw_dev)
    else:
        cfg["RTL_DEVICE_INDEX"] = raw_dev or 0
    cfg["RTL_SERIAL"] = str(cfg.get("RTL_SERIAL", "") or "").strip()
    if cfg["RTL_SERIAL"] == "None":
        cfg["RTL_SERIAL"] = ""
    cfg["RTL_GAIN"] = float(cfg.get("RTL_GAIN", 24.0))
    cfg["RTL_PPM"] = int(cfg.get("RTL_PPM", 0))
    cfg["MODE"] = str(cfg.get("MODE", "scanner")).strip().lower()
    if cfg["MODE"] not in ("scanner", "static", "stopped"):
        cfg["MODE"] = "scanner"
    cfg["SCAN_START_MHZ"] = float(cfg.get("SCAN_START_MHZ", 420.0))
    cfg["SCAN_END_MHZ"] = float(cfg.get("SCAN_END_MHZ", 430.0))
    if cfg["SCAN_END_MHZ"] <= cfg["SCAN_START_MHZ"]:
        cfg["SCAN_END_MHZ"] = cfg["SCAN_START_MHZ"] + 2.0
    cfg["SCAN_DWELL_TIME"] = max(10, int(cfg.get("SCAN_DWELL_TIME", 120)))
    cfg["IDLE_TIMEOUT_SEC"] = max(15, int(cfg.get("IDLE_TIMEOUT_SEC", 900)))
    cfg["BLACKLIST_STRIKES"] = max(1, int(cfg.get("BLACKLIST_STRIKES", 3)))
    cfg["BLACKLIST_TIME_SEC"] = max(10, int(cfg.get("BLACKLIST_TIME_SEC", 3600)))
    cfg["CENTER_FREQ"] = int(float(cfg.get("CENTER_FREQ", 427750000)))
    raw_ch_str = str(
        cfg.get(
            "TETRA_CHANNELS",
            "427000000:71,427300000:17,427550000:23,427850000:1,427950000:16,428500000:7",
        )
    ).strip()
    ch_modified = False
    norm_entries = []
    seen_hz = set()
    ch_hz_list = []
    for entry in raw_ch_str.split(","):
        entry = entry.strip()
        if not entry:
            continue
        parts = entry.split(":", 1)
        try:
            f_hz = int(float(parts[0].strip()))
        except Exception:
            continue
        la_part = parts[1].strip() if len(parts) > 1 else "?"
        if 380000000 <= f_hz <= 395000000 and (f_hz % 25000) == 0:
            f_hz += 12500
            ch_modified = True
        if f_hz not in seen_hz:
            seen_hz.add(f_hz)
            ch_hz_list.append(f_hz)
            norm_entries.append(f"{f_hz}:{la_part}")
    if norm_entries:
        cfg["TETRA_CHANNELS"] = ",".join(norm_entries)
    else:
        cfg["TETRA_CHANNELS"] = raw_ch_str
    if ch_hz_list and any(abs(cfg["CENTER_FREQ"] - f) < 25000 for f in ch_hz_list):
        cfg["CENTER_FREQ"] = compute_safe_center_hz(ch_hz_list)
        ch_modified = True
    if ch_modified:
        save_runtime_config(cfg)
    return cfg


def save_runtime_config(cfg):
    try:
        os.makedirs(LOG_DIR, exist_ok=True)
        tmp_path = f"{RUNTIME_CONFIG_FILE}.tmp"
        with open(tmp_path, "w", encoding="utf-8") as f:
            json.dump(cfg, f, indent=2, sort_keys=True)
        os.replace(tmp_path, RUNTIME_CONFIG_FILE)
    except Exception as exc:
        print(f"[scanner] Warning: failed saving {RUNTIME_CONFIG_FILE}: {exc}", flush=True)


def load_blacklist():
    if not os.path.exists(BLACKLIST_FILE):
        return {}
    try:
        with open(BLACKLIST_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
            if isinstance(data, dict):
                return data
    except Exception as exc:
        print(f"[scanner] Warning: failed reading {BLACKLIST_FILE}: {exc}", flush=True)
    return {}


def save_blacklist(data):
    try:
        os.makedirs(LOG_DIR, exist_ok=True)
        tmp_path = f"{BLACKLIST_FILE}.tmp"
        with open(tmp_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, sort_keys=True)
        os.replace(tmp_path, BLACKLIST_FILE)
    except Exception as exc:
        print(f"[scanner] Warning: failed saving {BLACKLIST_FILE}: {exc}", flush=True)


VALID_WHITELIST_CATEGORIES = {
    "SDS",
    "CALL",
    "VOICE",
    "STATUS",
    "ROAMING",
    "NETWORK",
    "NEIGHBOUR_BROADCAST",
}


def save_whitelist(data):
    try:
        os.makedirs(LOG_DIR, exist_ok=True)
        tmp_path = f"{WHITELIST_FILE}.tmp"
        with open(tmp_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, sort_keys=True)
        os.replace(tmp_path, WHITELIST_FILE)
    except Exception as exc:
        print(f"[scanner] Warning: failed saving {WHITELIST_FILE}: {exc}", flush=True)


def load_whitelist():
    if not os.path.exists(WHITELIST_FILE):
        return {}
    try:
        with open(WHITELIST_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
            if not isinstance(data, dict):
                return {}
            cleaned = {}
            modified = False
            for k, v in data.items():
                if not isinstance(v, dict):
                    modified = True
                    continue
                last_cat = str(v.get("last_category") or "").upper()
                if last_cat and last_cat not in VALID_WHITELIST_CATEGORIES:
                    modified = True
                    continue
                try:
                    f_hz = int(v.get("freq_hz") or k)
                except Exception:
                    cleaned[k] = v
                    continue
                if 380000000 <= f_hz <= 395000000 and (f_hz % 25000) == 0:
                    old_mhz = v.get("freq_mhz", f"{f_hz / 1e6:.4f}")
                    f_hz += 12500
                    new_key = str(f_hz)
                    new_mhz = f"{f_hz / 1e6:.4f}"
                    v = dict(v)
                    v["freq_hz"] = f_hz
                    v["freq_mhz"] = new_mhz
                    modified = True
                    print(
                        f"[scanner] Migrated Rakel whitelist entry {old_mhz} -> {new_mhz} MHz (+12.5 kHz offset)",
                        flush=True,
                    )
                    if new_key in cleaned:
                        ex = cleaned[new_key]
                        ex["hit_count"] = int(ex.get("hit_count") or 0) + int(v.get("hit_count") or 1)
                        for fld in ("la", "mcc", "mnc"):
                            if ex.get(fld) is None and v.get(fld) is not None:
                                ex[fld] = v[fld]
                    else:
                        cleaned[new_key] = v
                else:
                    k_str = str(f_hz)
                    if k_str in cleaned:
                        ex = cleaned[k_str]
                        ex["hit_count"] = max(int(ex.get("hit_count") or 0), int(v.get("hit_count") or 1))
                        for fld in ("la", "mcc", "mnc"):
                            if ex.get(fld) is None and v.get(fld) is not None:
                                ex[fld] = v[fld]
                    else:
                        cleaned[k_str] = v
            if modified:
                save_whitelist(cleaned)
            return cleaned
    except Exception as exc:
        print(f"[scanner] Warning: failed reading {WHITELIST_FILE}: {exc}", flush=True)
    return {}


def migrate_discovered_networks_and_blacklist():
    if os.path.exists(DISCOVERED_NETWORKS_FILE):
        try:
            merged_by_freq = {}
            total_lines = 0
            changed = False
            with open(DISCOVERED_NETWORKS_FILE, "r", encoding="utf-8", errors="replace") as nf:
                for raw_line in nf:
                    s_line = raw_line.strip()
                    if not s_line:
                        continue
                    total_lines += 1
                    try:
                        n_obj = json.loads(s_line)
                    except Exception:
                        continue
                    if not isinstance(n_obj, dict):
                        continue
                    f_hz = int(n_obj.get("freq_hz") or 0)
                    if not f_hz and n_obj.get("freq_mhz"):
                        try:
                            f_hz = int(round(float(n_obj["freq_mhz"]) * 1_000_000))
                        except Exception:
                            f_hz = 0
                    if 380000000 <= f_hz <= 395000000 and (f_hz % 25000) == 0:
                        old_mhz = n_obj.get("freq_mhz") or f"{f_hz / 1e6:.4f}"
                        f_hz += 12500
                        new_mhz = f"{f_hz / 1e6:.4f}"
                        n_obj["freq_hz"] = f_hz
                        n_obj["freq_mhz"] = new_mhz
                        n_obj["network_id"] = f"NET_{new_mhz}"
                        if isinstance(n_obj.get("human_text"), str) and old_mhz:
                            n_obj["human_text"] = n_obj["human_text"].replace(old_mhz, new_mhz)
                        changed = True
                    if isinstance(n_obj.get("neighbours"), list):
                        for nb in n_obj["neighbours"]:
                            if not isinstance(nb, dict):
                                continue
                            nb_hz = int(nb.get("freq_hz") or 0)
                            if not nb_hz and nb.get("freq_mhz"):
                                try:
                                    nb_hz = int(round(float(nb["freq_mhz"]) * 1_000_000))
                                except Exception:
                                    nb_hz = 0
                            if 380000000 <= nb_hz <= 395000000 and (nb_hz % 25000) == 0:
                                old_nb_mhz = nb.get("freq_mhz") or f"{nb_hz / 1e6:.4f}"
                                nb_hz += 12500
                                new_nb_mhz = f"{nb_hz / 1e6:.4f}"
                                nb["freq_hz"] = nb_hz
                                nb["freq_mhz"] = new_nb_mhz
                                if isinstance(n_obj.get("human_text"), str) and old_nb_mhz:
                                    n_obj["human_text"] = n_obj["human_text"].replace(old_nb_mhz, new_nb_mhz)
                                changed = True
                    key = n_obj.get("freq_mhz") or n_obj.get("network_id")
                    if not key:
                        continue
                    existing = merged_by_freq.get(key)
                    if existing is None:
                        merged_by_freq[key] = dict(n_obj)
                    else:
                        prev_role = existing.get("role")
                        for k_f, v_f in n_obj.items():
                            if v_f is not None and v_f != [] and v_f != {}:
                                existing[k_f] = v_f
                        if prev_role == "ACTIVE CELL":
                            existing["role"] = "ACTIVE CELL"
            if total_lines > len(merged_by_freq):
                changed = True
            if changed:
                tmp_nf = f"{DISCOVERED_NETWORKS_FILE}.tmp"
                with open(tmp_nf, "w", encoding="utf-8") as wf:
                    for net_item in merged_by_freq.values():
                        wf.write(json.dumps(net_item, ensure_ascii=False) + "\n")
                os.replace(tmp_nf, DISCOVERED_NETWORKS_FILE)
                print(
                    f"[scanner] Compacted and migrated discovered_networks.jsonl ({total_lines} -> {len(merged_by_freq)} records)",
                    flush=True,
                )
        except Exception as exc:
            print(f"[scanner] Warning migrating {DISCOVERED_NETWORKS_FILE}: {exc}", flush=True)

    bl = load_blacklist()
    wl = load_whitelist()
    bl_modified = False
    for k in list(bl.keys()):
        try:
            f_hz = int(k)
        except Exception:
            continue
        if k in wl or (380000000 <= f_hz <= 395000000 and (f_hz % 25000) == 0):
            del bl[k]
            bl_modified = True
    if bl_modified:
        save_blacklist(bl)


def emit_system_event(human_text):
    iso_ts = datetime.now(timezone.utc).isoformat()
    event_obj = {
        "time": iso_ts,
        "timestamp": iso_ts,
        "category": "SYSTEM",
        "freq_mhz": "SYS",
        "human_text": human_text,
    }
    try:
        os.makedirs(LOG_DIR, exist_ok=True)
        with open("/app/logs/tetra_events.jsonl", "a", encoding="utf-8") as f:
            f.write(json.dumps(event_obj, ensure_ascii=False) + "\n")
            f.flush()
    except Exception as exc:
        print(f"[scanner] Warning: failed writing system event: {exc}", flush=True)
    db.record_event(event_obj)


def clear_frequency_strikes(freq_list_hz):
    if not freq_list_hz:
        return
    bl = load_blacklist()
    modified = False
    for f_hz in freq_list_hz:
        key = str(int(f_hz))
        if key in bl:
            del bl[key]
            modified = True
    if modified:
        save_blacklist(bl)


def add_frequencies_to_blacklist(
    freq_list_hz,
    duration_sec,
    max_strikes,
    reason,
    immediate=False,
    force_even_if_whitelisted=False,
):
    bl = load_blacklist()
    wl = load_whitelist()
    now_ts = time.time()

    for f_hz in freq_list_hz:
        key = str(int(f_hz))
        if key in wl and not immediate and not force_even_if_whitelisted:
            msg = f"Keeping whitelisted frequency {int(f_hz) / 1e6:.4f} MHz (protected from Idle Timeout blacklist)"
            print(f"[scanner] {msg}", flush=True)
            emit_system_event(msg)
            continue

        prev_entry = bl.get(key) if isinstance(bl.get(key), dict) else {}
        prev_expire = float(prev_entry.get("expire_at", 0))
        if prev_expire > 0 and now_ts >= prev_expire:
            prev_strikes = 0
        else:
            prev_strikes = int(prev_entry.get("strikes", 0))

        if immediate:
            strikes = max_strikes
        else:
            strikes = prev_strikes + 1

        if strikes >= max_strikes:
            expire_at = round(now_ts + float(duration_sec), 2)
            bl[key] = {
                "strikes": strikes,
                "max_strikes": max_strikes,
                "expire_at": expire_at,
                "last_strike_at": round(now_ts, 2),
                "reason": f"{reason} ({strikes}/{max_strikes})",
            }
            msg = (
                f"Blacklisted {int(f_hz) / 1e6:.4f} MHz for {duration_sec}s "
                f"(Strike {strikes}/{max_strikes} - {reason})"
            )
            print(f"[scanner] {msg}", flush=True)
            emit_system_event(msg)
        else:
            bl[key] = {
                "strikes": strikes,
                "max_strikes": max_strikes,
                "expire_at": 0,
                "last_strike_at": round(now_ts, 2),
                "reason": f"Warning {strikes}/{max_strikes} ({reason})",
            }
            msg = (
                f"Frequency {int(f_hz) / 1e6:.4f} MHz quiet: recorded Strike "
                f"{strikes}/{max_strikes} (not blacklisted yet)"
            )
            print(f"[scanner] {msg}", flush=True)
            emit_system_event(msg)

    save_blacklist(bl)


GR_PROC_REF = None


def _load_librtlsdr():
    for lib_name in ("librtlsdr.so.0", "librtlsdr.so"):
        try:
            return ctypes.CDLL(lib_name)
        except Exception:
            continue
    return None


def resolve_rtl_device_spec(cfg, serial_override=None):
    """
    Resolve RTL_DEVICE_INDEX and/or RTL_SERIAL into:
      (cli_device_arg, resolved_index, resolved_serial)
    Supports index (0, 1), serial ('00000202', '00000222'), or osmosdr string
    ('rtl=0,serial=00000222').
    """
    if isinstance(cfg, dict):
        raw_spec = str(cfg.get("RTL_DEVICE_INDEX", "0")).strip()
        serial_cfg = str(serial_override if serial_override is not None else (cfg.get("RTL_SERIAL", "") or "")).strip()
    else:
        raw_spec = str(cfg if cfg is not None else "0").strip()
        serial_cfg = str(serial_override or "").strip()

    parsed_index = None
    parsed_serial = serial_cfg if serial_cfg else None

    if "serial=" in raw_spec.lower() or "rtl=" in raw_spec.lower():
        m_ser = re.search(r"serial=([A-Za-z0-9_-]+)", raw_spec, re.IGNORECASE)
        if m_ser and not parsed_serial:
            parsed_serial = m_ser.group(1).strip()
        m_rtl = re.search(r"rtl=([A-Za-z0-9_-]+)", raw_spec, re.IGNORECASE)
        if m_rtl:
            rtl_tok = m_rtl.group(1).strip()
            if len(rtl_tok) >= 4 or not rtl_tok.isdigit():
                if not parsed_serial:
                    parsed_serial = rtl_tok
            else:
                parsed_index = int(rtl_tok)
    elif len(raw_spec) >= 4 or not raw_spec.isdigit():
        if not parsed_serial:
            parsed_serial = raw_spec
    else:
        parsed_index = int(raw_spec)

    if parsed_index is not None and not parsed_serial:
        return str(parsed_index), parsed_index, None

    rtlsdr = _load_librtlsdr()
    resolved_index = parsed_index
    resolved_serial = parsed_serial

    if rtlsdr is not None and resolved_serial and resolved_index is None:
        try:
            dev_count = int(rtlsdr.rtlsdr_get_device_count())
        except Exception:
            dev_count = 0

        if dev_count > 0:
            try:
                idx = int(
                    rtlsdr.rtlsdr_get_index_by_serial(
                        resolved_serial.encode("utf-8")
                    )
                )
                if idx >= 0:
                    resolved_index = idx
            except Exception:
                pass

    cli_arg = (
        str(resolved_index)
        if resolved_index is not None
        else (resolved_serial or "0")
    )
    return cli_arg, resolved_index, resolved_serial


def reset_rtl_usb_device(cfg, center_hz=None, perform_usb_ioctl=True):
    """
    Pre-initialization USB Reset:
      Issues Linux kernel USBDEVFS_RESET (0x5514) ioctl ONLY on the target RTL-SDR's
      /dev/bus/usb/<bus>/<dev> node verified by its exact USB serial number in sysfs.
      Never opens librtlsdr in the parent process or immediately before gr-osmosdr,
      avoiding libusb_open (-4) handle collisions.
    """
    cli_arg, resolved_idx, resolved_ser = resolve_rtl_device_spec(cfg)
    dev_idx_int = int(resolved_idx) if resolved_idx is not None else 0

    if not resolved_ser:
        lookup_code = (
            "import ctypes, sys\n"
            "for n in ('librtlsdr.so.0', 'librtlsdr.so'):\n"
            "    try:\n"
            "        lib = ctypes.CDLL(n); break\n"
            "    except Exception: lib = None\n"
            "if not lib: sys.exit(1)\n"
            "m = ctypes.create_string_buffer(256)\n"
            "p = ctypes.create_string_buffer(256)\n"
            "s = ctypes.create_string_buffer(256)\n"
            f"rc = int(lib.rtlsdr_get_device_usb_strings({dev_idx_int}, m, p, s))\n"
            "if rc == 0:\n"
            "    print(s.value.decode('utf-8', errors='ignore').strip())\n"
        )
        try:
            res_ser = subprocess.run(
                ["python3", "-c", lookup_code],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                timeout=3,
                check=False,
            )
            s_out = (res_ser.stdout or "").strip()
            if s_out:
                resolved_ser = s_out
        except Exception:
            pass
        time.sleep(0.40)

    usb_reset_done = False
    if perform_usb_ioctl:
        try:
            rtl_usb_nodes = []
            for sys_dev in sorted(glob.glob("/sys/bus/usb/devices/*")):
                vid_path = os.path.join(sys_dev, "idVendor")
                pid_path = os.path.join(sys_dev, "idProduct")
                if not (os.path.isfile(vid_path) and os.path.isfile(pid_path)):
                    continue
                try:
                    with open(vid_path, "r", encoding="utf-8") as vf:
                        vid = vf.read().strip().lower()
                    with open(pid_path, "r", encoding="utf-8") as pf:
                        pid = pf.read().strip().lower()
                except Exception:
                    continue
                if vid != "0bda" or pid not in ("2838", "2832"):
                    continue

                busnum_p = os.path.join(sys_dev, "busnum")
                devnum_p = os.path.join(sys_dev, "devnum")
                ser_p = os.path.join(sys_dev, "serial")
                try:
                    with open(busnum_p, "r", encoding="utf-8") as bf:
                        bus_num = int(bf.read().strip())
                    with open(devnum_p, "r", encoding="utf-8") as df:
                        dev_num = int(df.read().strip())
                except Exception:
                    continue
                usb_ser = ""
                if os.path.isfile(ser_p):
                    try:
                        with open(ser_p, "r", encoding="utf-8", errors="ignore") as sf:
                            usb_ser = sf.read().strip()
                    except Exception:
                        pass
                dev_node = f"/dev/bus/usb/{bus_num:03d}/{dev_num:03d}"
                if os.path.exists(dev_node):
                    rtl_usb_nodes.append((dev_node, usb_ser))

            chosen_node = None
            if resolved_ser:
                for node_p, u_ser in rtl_usb_nodes:
                    if u_ser and u_ser.strip().lower() == resolved_ser.strip().lower():
                        chosen_node = (node_p, u_ser)
                        break
                if chosen_node is None:
                    print(
                        f"[scanner] [USB RESET] Skipping ioctl USBDEVFS_RESET: serial={resolved_ser} not matched in sysfs",
                        flush=True,
                    )
            else:
                print(
                    f"[scanner] [USB RESET] Skipping ioctl USBDEVFS_RESET: could not resolve exact USB serial for index={resolved_idx}",
                    flush=True,
                )

            if chosen_node is not None:
                node_path, node_ser = chosen_node
                USBDEVFS_RESET = 0x5514  # ord('U') << 8 | 20 (21780)
                fd = os.open(node_path, os.O_WRONLY)
                try:
                    fcntl.ioctl(fd, USBDEVFS_RESET, 0)
                    usb_reset_done = True
                    print(
                        f"[scanner] [USB RESET] Issued kernel USBDEVFS_RESET on {node_path} (serial={node_ser})",
                        flush=True,
                    )
                finally:
                    os.close(fd)
                time.sleep(0.80)
        except Exception as exc:
            print(f"[scanner] [USB RESET] Notice: ioctl reset skipped/failed: {exc}", flush=True)

    time.sleep(0.60)
    return cli_arg, resolved_idx, resolved_ser


def terminate_children():
    global ACTIVE_PROCS, GR_PROC_REF
    if not ACTIVE_PROCS and GR_PROC_REF is None:
        return

    # 1. Stop GNU Radio (tetra_multi_rtl.py) FIRST via SIGINT/SIGTERM so
    #    top_block.stop(), top_block.wait(), and rtlsdr_close() release the
    #    USB interface cleanly before any other process is killed.
    if GR_PROC_REF is not None and GR_PROC_REF.poll() is None:
        try:
            GR_PROC_REF.send_signal(signal.SIGINT)
        except Exception:
            try:
                GR_PROC_REF.terminate()
            except Exception:
                pass
        try:
            GR_PROC_REF.wait(timeout=4.5)
        except subprocess.TimeoutExpired:
            try:
                GR_PROC_REF.terminate()
                GR_PROC_REF.wait(timeout=1.5)
            except subprocess.TimeoutExpired:
                try:
                    GR_PROC_REF.kill()
                except Exception:
                    pass
        time.sleep(0.35)

    # 2. Terminate remaining child processes (tetra_parser.py and tetra-kit decoders)
    for proc in list(ACTIVE_PROCS):
        if proc.poll() is None:
            try:
                proc.terminate()
            except Exception:
                pass

    deadline = time.time() + 4.0
    for proc in list(ACTIVE_PROCS):
        remaining = max(0.1, deadline - time.time())
        if proc.poll() is None:
            try:
                proc.wait(timeout=remaining)
            except subprocess.TimeoutExpired:
                try:
                    proc.kill()
                except Exception:
                    pass

    for proc in list(ACTIVE_PROCS):
        try:
            proc.wait(timeout=1.0)
        except Exception:
            pass

    ACTIVE_PROCS.clear()
    GR_PROC_REF = None


atexit.register(terminate_children)


def handle_signal(signum, frame):
    global RUNNING
    print(f"[scanner] Received signal {signum}, shutting down...", flush=True)
    RUNNING = False
    terminate_children()


signal.signal(signal.SIGINT, handle_signal)
signal.signal(signal.SIGTERM, handle_signal)


def build_center_frequencies(start_mhz, end_mhz):
    centers = []
    cur = start_mhz + 1.0
    while cur < end_mhz:
        centers.append(cur)
        cur += 2.0
    if not centers:
        centers.append(start_mhz + 1.0)
    return centers


def _run_sweep_subproc(cmd, timeout=35):
    proc = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        start_new_session=True,
    )
    deadline = time.time() + timeout
    while proc.poll() is None:
        if not RUNNING or os.path.exists(RELOAD_CONFIG_FLAG):
            try:
                os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
            except Exception:
                try:
                    proc.terminate()
                except Exception:
                    pass
            try:
                proc.wait(timeout=2)
            except Exception:
                try:
                    proc.kill()
                except Exception:
                    pass
            return -1, "", "Aborted due to config reload"
        if time.time() >= deadline:
            try:
                os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
            except Exception:
                proc.kill()
            return -2, "", "Sweep timeout"
        time.sleep(0.15)
    out, err = proc.communicate()
    return proc.returncode, out or "", err or ""


def run_rtl_power_sweep(center_mhz, start_hz, end_hz, cfg):
    if not RUNNING or os.path.exists(RELOAD_CONFIG_FLAG):
        return None
    csv_label = f"{center_mhz:g}"
    csv_path = f"/tmp/scan_{csv_label}.csv"
    try:
        if os.path.exists(csv_path):
            os.remove(csv_path)
    except OSError:
        pass

    cli_dev_arg, resolved_idx, resolved_ser = resolve_rtl_device_spec(cfg)
    center_hz = int(round(center_mhz * 1_000_000))
    gain_val = float(cfg["RTL_GAIN"])
    ppm_val = int(float(cfg.get("RTL_PPM", 0)))

    gr_sweep_code = (
        "import time, gc, sys, numpy as np\n"
        "from gnuradio import gr, blocks\n"
        "import osmosdr\n"
        "tb = gr.top_block()\n"
        "src = None\n"
        "for _att in range(3):\n"
        "    try:\n"
        f"        src = osmosdr.source(args='numchan=1 rtl={cli_dev_arg}')\n"
        "        break\n"
        "    except RuntimeError:\n"
        "        if _att == 2:\n"
        "            raise\n"
        "        time.sleep(0.6)\n"
        "src.set_sample_rate(2000000)\n"
        f"src.set_freq_corr({ppm_val}, 0)\n"
        "src.set_dc_offset_mode(0, 0)\n"
        "src.set_iq_balance_mode(0, 0)\n"
        "src.set_gain_mode(False, 0)\n"
        f"src.set_center_freq({center_hz}, 0)\n"
        f"src.set_gain({gain_val}, 0)\n"
        "s2v = blocks.stream_to_vector(gr.sizeof_gr_complex, 4096)\n"
        "keep = blocks.keep_one_in_n(gr.sizeof_gr_complex * 4096, 5)\n"
        "sink = blocks.vector_sink_c(4096)\n"
        "tb.connect(src, s2v, keep, sink)\n"
        "tb.start()\n"
        "time.sleep(1.0)\n"
        f"src.set_center_freq({center_hz}, 0)\n"
        f"src.set_gain({gain_val}, 0)\n"
        "time.sleep(0.25)\n"
        "sink.reset()\n"
        "time.sleep(2.0)\n"
        "tb.stop()\n"
        "tb.wait()\n"
        "raw = np.asarray(sink.data(), dtype=np.complex64)\n"
        "tb.disconnect_all()\n"
        "src = None\n"
        "del tb\n"
        "gc.collect()\n"
        "time.sleep(0.25)\n"
        "nvec = len(raw) // 4096\n"
        "if nvec <= 0:\n"
        "    sys.exit(2)\n"
        "vecs = raw[:nvec * 4096].reshape((nvec, 4096))\n"
        "win = np.hanning(4096).astype(np.float32)\n"
        "win /= np.sum(win)\n"
        "spec = np.fft.fftshift(np.fft.fft(vecs * win, axis=1), axes=1)\n"
        "pwr = np.mean(np.abs(spec) ** 2, axis=0)\n"
        "dbm = 10.0 * np.log10(np.maximum(pwr, 1e-12))\n"
        "step_hz = 2000000.0 / 4096.0\n"
        f"low_hz = {center_hz} - 1000000.0\n"
        f"high_hz = {center_hz} + 1000000.0 - step_hz\n"
        "vals_str = ', '.join(f'{v:.2f}' for v in dbm)\n"
        f"with open({repr(csv_path)}, 'w', encoding='utf-8') as f:\n"
        "    f.write(f'2026-11-06, 00:00:00, {low_hz:.0f}, {high_hz:.0f}, {step_hz:.2f}, {nvec * 4096}, {vals_str}\\n')\n"
    )
    cmd = ["python3", "-c", gr_sweep_code]
    scan_msg = (
        f"[PHASE 1] Sweeping {start_hz / 1e6:.3f} - {end_hz / 1e6:.3f} MHz "
        f"(Device: {cli_dev_arg}"
        + (f" [SN:{resolved_ser}]" if resolved_ser else "")
        + f", Gain: {cfg['RTL_GAIN']} dB, post-start retune)"
    )
    print(f"[scanner] {scan_msg}", flush=True)
    emit_system_event(scan_msg)
    time.sleep(0.45)
    if not RUNNING or os.path.exists(RELOAD_CONFIG_FLAG):
        return None
    try:
        retcode, _, stderr_text = _run_sweep_subproc(cmd, timeout=35)
        if retcode != 0:
            if not RUNNING or os.path.exists(RELOAD_CONFIG_FLAG):
                return None
            err_str = (stderr_text or "").strip()
            print(
                f"[scanner] Phase 1 sweep exited with code {retcode}: {err_str}",
                flush=True,
            )
            if "usb_claim_interface" in err_str.lower() or retcode != 0:
                print("[scanner] Attempting USB reset and retry for Phase 1 block...", flush=True)
                reset_rtl_usb_device(cfg, center_hz=int(center_mhz * 1e6), perform_usb_ioctl=True)
                if not RUNNING or os.path.exists(RELOAD_CONFIG_FLAG):
                    return None
                ret_retry, _, err_retry = _run_sweep_subproc(cmd, timeout=35)
                if ret_retry != 0 and RUNNING and not os.path.exists(RELOAD_CONFIG_FLAG):
                    print(
                        f"[scanner] Phase 1 sweep retry exited with code {ret_retry}: {(err_retry or '').strip()}",
                        flush=True,
                    )
    except Exception as exc:
        print(f"[scanner] Failed running Phase 1 sweep: {exc}", flush=True)
        return None

    if os.path.exists(csv_path) and os.path.getsize(csv_path) > 0:
        return csv_path
    return None


def classify_rf_peak(dbm, snr_db):
    if dbm >= -12.0:
        return "NEAR SATURATION"
    if snr_db >= 8.0:
        return "STRONG"
    if snr_db >= 4.0:
        return "GOOD"
    if snr_db >= 2.5:
        return "MODERATE"
    return "WEAK/BORDERLINE"


def classify_noise_floor(noise_floor):
    if noise_floor > -29.0:
        return "HIGH / OVERLOAD"
    if noise_floor > -34.5:
        return "ELEVATED"
    if noise_floor >= -62.0:
        return "OPTIMAL"
    return "LOW"


def parse_block_csv(csv_path, start_hz, end_hz, center_hz, current_gain=24.0):
    if not csv_path or not os.path.exists(csv_path):
        return [], {}, None

    fine_bins = {}
    try:
        with open(csv_path, "r", encoding="utf-8", errors="replace") as f:
            reader = csv.reader(f)
            for row in reader:
                if len(row) < 7:
                    continue
                try:
                    hz_low = float(row[2].strip())
                    hz_high = float(row[3].strip())
                    hz_step = float(row[4].strip())
                except ValueError:
                    continue

                dbm_values = []
                for cell in row[6:]:
                    cell_s = cell.strip()
                    if not cell_s or cell_s.lower() == "nan":
                        continue
                    try:
                        dbm_values.append(float(cell_s))
                    except ValueError:
                        continue

                if not dbm_values or hz_step <= 0:
                    continue

                for i, dbm in enumerate(dbm_values):
                    raw_freq = hz_low + i * hz_step
                    if raw_freq > hz_high + hz_step:
                        break
                    if abs(raw_freq - center_hz) <= DC_SPIKE_GUARD_HZ:
                        continue
                    if (
                        raw_freq < (start_hz + EDGE_GUARD_HZ)
                        or raw_freq > (end_hz - EDGE_GUARD_HZ)
                    ):
                        continue
                    freq_int = int(round(raw_freq))
                    fine_bins.setdefault(freq_int, []).append(dbm)
    except Exception as exc:
        print(f"[scanner] Error parsing {csv_path}: {exc}", flush=True)
        return [], {}, None

    if not fine_bins:
        return [], {}, None

    avg_spectrum = sorted(
        (freq, sum(vals) / float(len(vals))) for freq, vals in fine_bins.items()
    )
    all_dbm = [dbm for _, dbm in avg_spectrum]
    noise_floor = statistics.median(all_dbm)
    min_threshold = max(noise_floor + MIN_SNR_ABOVE_FLOOR_DB, ABSOLUTE_MIN_DBM)

    block_grid = {}
    for freq_hz, dbm in avg_spectrum:
        if 380_000_000 <= freq_hz <= 395_000_000:
            # Swedish Rakel (380-395 MHz) uses ETSI Offset = 3 (+12.5 kHz: .xx125, .xx375, .xx625, .xx875 MHz)
            snapped_hz = int(round((freq_hz - 12_500.0) / 25_000.0) * 25_000) + 12_500
        else:
            # Support both Offset = 0 (.xx00/.xx25/.xx50/.xx75) and Offset = 3 (.xx125/.xx375/.xx625/.xx875)
            snapped_hz = int(round(freq_hz / float(PTS_GRID_12K5_HZ)) * PTS_GRID_12K5_HZ)
        snr_db = round(dbm - noise_floor, 1)
        grid_entry = {
            "freq_hz": snapped_hz,
            "dbm": round(dbm, 2),
            "snr_db": snr_db,
            "noise_floor": round(noise_floor, 2),
        }
        if (
            snapped_hz not in block_grid
            or grid_entry["dbm"] > block_grid[snapped_hz]["dbm"]
        ):
            block_grid[snapped_hz] = grid_entry

    snapped_peaks = {}
    sorted_grid_freqs = sorted(block_grid.keys())
    for snapped_hz in sorted_grid_freqs:
        g_entry = block_grid[snapped_hz]
        dbm = g_entry["dbm"]
        if dbm < min_threshold:
            continue

        left_12k = block_grid.get(snapped_hz - PTS_GRID_12K5_HZ)
        right_12k = block_grid.get(snapped_hz + PTS_GRID_12K5_HZ)
        left_25k = block_grid.get(snapped_hz - PTS_GRID_HZ)
        right_25k = block_grid.get(snapped_hz + PTS_GRID_HZ)

        if left_12k and left_12k["dbm"] > dbm:
            continue
        if right_12k and right_12k["dbm"] >= dbm and right_12k["dbm"] >= min_threshold:
            continue
        if left_25k and left_25k["dbm"] > dbm:
            continue
        if right_25k and right_25k["dbm"] >= dbm and right_25k["dbm"] >= min_threshold:
            continue

        entry = dict(g_entry)
        entry["measured_peak"] = True
        snapped_peaks[snapped_hz] = entry

    block_peaks = list(snapped_peaks.values())
    block_peaks.sort(key=lambda x: x["dbm"], reverse=True)
    nf_status = classify_noise_floor(noise_floor)
    lines = [
        f"[PHASE 1 RESULT] {start_hz / 1e6:.3f} - {end_hz / 1e6:.3f} MHz | "
        f"Noise floor: {noise_floor:.1f} dBm [{nf_status}] | Found {len(block_peaks)} RF peak(s):"
    ]
    if block_peaks:
        for pk in block_peaks[:8]:
            q_label = classify_rf_peak(pk["dbm"], pk["snr_db"])
            lines.append(
                f"  -> {pk['freq_hz'] / 1e6:.4f} MHz  |  {pk['dbm']:>6.1f} dBm  |  +{pk['snr_db']:>4.1f} dB SNR  [{q_label}]"
            )
    else:
        lines.append("  -> None above threshold")

    strongest_dbm = block_peaks[0]["dbm"] if block_peaks else -99.0
    strongest_mhz = block_peaks[0]["freq_hz"] / 1e6 if block_peaks else 0.0
    weak_count = sum(1 for pk in block_peaks if pk["snr_db"] < 3.5)

    if noise_floor > -29.0:
        rec_gain = max(0.0, round(current_gain - 8.0, 1))
        lines.append(
            f"  [RF ADVICE] High noise floor ({noise_floor:.1f} dBm) indicates RF front-end overload. "
            f"Try lowering Gain from {current_gain:.1f} dB to ~{rec_gain:.1f} dB."
        )
    elif strongest_dbm >= -12.0:
        rec_gain = max(0.0, round(current_gain - 5.0, 1))
        lines.append(
            f"  [RF ADVICE] Very strong carrier at {strongest_mhz:.4f} MHz ({strongest_dbm:.1f} dBm) is close to RTL-SDR ADC clipping "
            f"and can cause ghost peaks in this 2 MHz block. If monitoring this sub-band, lower Gain to ~{rec_gain:.1f} dB."
        )
    elif len(block_peaks) >= 10 and weak_count >= 6:
        rec_gain = max(0.0, round(current_gain - 3.0, 1))
        lines.append(
            f"  [RF ADVICE] High peak density ({len(block_peaks)} peaks, {weak_count} weak <3.5 dB SNR). "
            f"If false ghost peaks persist, try lowering Gain slightly from {current_gain:.1f} dB to ~{rec_gain:.1f} dB."
        )
    elif noise_floor < -44.0 and not block_peaks:
        rec_gain = min(49.6, round(current_gain + 6.0, 1))
        lines.append(
            f"  [RF ADVICE] Low noise floor ({noise_floor:.1f} dBm) with 0 peaks. "
            f"Consider raising Gain from {current_gain:.1f} dB to ~{rec_gain:.1f} dB."
        )

    block_summary = "\n".join(lines)
    print(block_summary, flush=True)
    emit_system_event(block_summary)
    return block_peaks, block_grid, noise_floor


def phase1_sweep_band(centers, cfg):
    current_gain = float(cfg.get("RTL_GAIN", 24.0))
    sweep_start_msg = (
        f"[PHASE 1: RF SPECTRUM SWEEP]\n"
        f"  Band: {cfg['SCAN_START_MHZ']:.1f} - {cfg['SCAN_END_MHZ']:.1f} MHz ({len(centers)} blocks x 2.0 MHz, Gain: {current_gain:.1f} dB)\n"
        f"  Step: Measuring RF signal strength (25 kHz grid)"
    )
    print(sweep_start_msg, flush=True)
    emit_system_event(sweep_start_msg)

    merged_peaks = {}
    merged_grid = {}
    block_floors = []
    for center_mhz in centers:
        if not RUNNING:
            break
        if os.path.exists(RELOAD_CONFIG_FLAG):
            print("[scanner] Config change detected during sweep, restarting cycle...", flush=True)
            break

        center_hz = int(round(center_mhz * 1_000_000))
        start_hz = int(round((center_mhz - 1.0) * 1_000_000))
        end_hz = int(round((center_mhz + 1.0) * 1_000_000))

        csv_path = run_rtl_power_sweep(center_mhz, start_hz, end_hz, cfg)
        if not csv_path or not RUNNING:
            time.sleep(0.5)
            continue

        block_peaks, block_grid, block_nf = parse_block_csv(
            csv_path, start_hz, end_hz, center_hz, current_gain=current_gain
        )
        if block_nf is not None:
            block_floors.append((center_mhz, block_nf, len(block_peaks)))
        for f_hz, g_entry in block_grid.items():
            if f_hz not in merged_grid or g_entry["dbm"] > merged_grid[f_hz]["dbm"]:
                merged_grid[f_hz] = g_entry
        for pk in block_peaks:
            f_hz = pk["freq_hz"]
            if f_hz not in merged_peaks or pk["dbm"] > merged_peaks[f_hz]["dbm"]:
                merged_peaks[f_hz] = pk

    if not RUNNING or os.path.exists(RELOAD_CONFIG_FLAG):
        return [], {}

    all_peaks = list(merged_peaks.values())
    all_peaks.sort(key=lambda x: x["dbm"], reverse=True)
    done_lines = [
        f"[PHASE 1 DONE] Total unique RF candidates found: {len(all_peaks)}"
    ]
    for idx, p in enumerate(all_peaks[:8], start=1):
        q_label = classify_rf_peak(p["dbm"], p["snr_db"])
        done_lines.append(
            f"  #{idx}: {p['freq_hz'] / 1e6:.4f} MHz  ({p['dbm']:.1f} dBm, +{p['snr_db']:.1f} dB SNR) [{q_label}]"
        )

    if block_floors:
        nf_vals = [nf for _, nf, _ in block_floors]
        med_nf = statistics.median(nf_vals)
        min_nf = min(nf_vals)
        max_nf = max(nf_vals)
        nf_label = classify_noise_floor(med_nf)

        strong_peaks = [p for p in all_peaks if p["snr_db"] >= 4.0]
        moderate_peaks = [p for p in all_peaks if 2.5 <= p["snr_db"] < 4.0]
        weak_peaks = [p for p in all_peaks if p["snr_db"] < 2.5]
        overload_peaks = [p for p in all_peaks if p["dbm"] >= -12.0]

        done_lines.append("  ------------------------------------------------------------")
        done_lines.append("  [RF HEALTH & GAIN ADVISOR]")
        done_lines.append(
            f"  Noise Floor : Median {med_nf:.1f} dBm (range {min_nf:.1f} to {max_nf:.1f} dBm) -> {nf_label} (Target: -44 to -35 dBm)"
        )
        done_lines.append(
            f"  Peak Quality: {len(strong_peaks)} Strong/Good (>=4 dB SNR), "
            f"{len(moderate_peaks)} Moderate (2.5-4 dB), {len(weak_peaks)} Weak (<2.5 dB)"
        )

        if med_nf > -29.0:
            rec_gain = max(0.0, round(current_gain - 8.0, 1))
            done_lines.append(
                f"  Gain Advice : REDUCE GAIN. Median noise floor ({med_nf:.1f} dBm) is too high and masks TETRA signals. "
                f"Lower Gain from {current_gain:.1f} dB to ~{rec_gain:.1f} dB."
            )
        elif med_nf > -34.5:
            rec_gain = max(0.0, round(current_gain - 4.0, 1))
            done_lines.append(
                f"  Gain Advice : SLIGHTLY HIGH GAIN. Noise floor ({med_nf:.1f} dBm) is elevated. "
                f"Try lowering Gain from {current_gain:.1f} dB to ~{rec_gain:.1f} dB for cleaner SNR."
            )
        elif med_nf < -44.0 and len(strong_peaks) < 2:
            rec_gain = min(49.6, round(current_gain + 6.0, 1))
            done_lines.append(
                f"  Gain Advice : INCREASE GAIN. Noise floor ({med_nf:.1f} dBm) is low and few strong signals were found. "
                f"Try raising Gain from {current_gain:.1f} dB to ~{rec_gain:.1f} dB."
            )
        elif overload_peaks:
            ov_list = ", ".join(f"{p['freq_hz']/1e6:.4f} MHz ({p['dbm']:.1f} dBm)" for p in overload_peaks[:2])
            rec_gain = max(0.0, round(current_gain - 4.0, 1))
            done_lines.append(
                f"  Gain Advice : GOOD OVERALL (Gain {current_gain:.1f} dB is optimal for most blocks), "
                f"but near-saturation blocker(s) at {ov_list} may cause ghost peaks in their local 2 MHz block. "
                f"Keep Gain {current_gain:.1f} dB for weak TETRA bands, or lower to ~{rec_gain:.1f} dB / narrow Scan Start-End if targeting that block."
            )
        elif len(strong_peaks) == 0 and len(all_peaks) > 0:
            rec_gain = min(49.6, round(current_gain + 4.0, 1))
            done_lines.append(
                f"  Gain Advice : WEAK SIGNALS ONLY (max SNR +{all_peaks[0]['snr_db']:.1f} dB). "
                f"Noise floor ({med_nf:.1f} dBm) is healthy; try testing Gain ~{rec_gain:.1f} dB or check antenna placement."
            )
        else:
            done_lines.append(
                f"  Gain Advice : OPTIMAL (Gain {current_gain:.1f} dB). Noise floor ({med_nf:.1f} dBm) and dynamic range are well balanced."
            )

    p1_done_msg = "\n".join(done_lines)
    print(p1_done_msg, flush=True)
    emit_system_event(p1_done_msg)
    db.record_rf_sweep_peaks(all_peaks)
    return all_peaks, merged_grid


def phase2_select_best_window(all_peaks, cfg, measured_grid=None):
    whitelist = load_whitelist()
    blacklist = load_blacklist()
    measured_grid = measured_grid or {}
    now_ts = time.time()
    blacklist_modified = False

    start_limit_hz = int(round(cfg["SCAN_START_MHZ"] * 1_000_000))
    end_limit_hz = int(round(cfg["SCAN_END_MHZ"] * 1_000_000))

    peaks_by_hz = {}
    for p in all_peaks or []:
        f_hz = int(p["freq_hz"])
        if not (start_limit_hz <= f_hz <= end_limit_hz):
            continue
        pk_copy = dict(p)
        pk_copy["measured_peak"] = True
        pk_copy["whitelisted"] = False
        pk_copy["source"] = "MEASURED"
        peaks_by_hz[f_hz] = pk_copy

    for wl_key, wl_info in whitelist.items():
        try:
            f_hz = int(wl_key)
        except ValueError:
            continue
        if not (start_limit_hz <= f_hz <= end_limit_hz):
            continue
        wl_la = wl_info.get("la") if isinstance(wl_info, dict) else None
        if f_hz in peaks_by_hz:
            peaks_by_hz[f_hz]["whitelisted"] = True
            peaks_by_hz[f_hz]["source"] = "MEASURED+WHITELIST"
            if wl_la is not None:
                peaks_by_hz[f_hz]["la"] = wl_la
        else:
            grid_meas = measured_grid.get(f_hz)
            if grid_meas:
                peaks_by_hz[f_hz] = {
                    "freq_hz": f_hz,
                    "dbm": grid_meas["dbm"],
                    "snr_db": grid_meas["snr_db"],
                    "noise_floor": grid_meas["noise_floor"],
                    "measured_peak": grid_meas["snr_db"] >= MIN_SNR_ABOVE_FLOOR_DB,
                    "whitelisted": True,
                    "seed_channel": False,
                    "source": "MEASURED+WHITELIST",
                    "la": wl_la,
                }
            else:
                peaks_by_hz[f_hz] = {
                    "freq_hz": f_hz,
                    "dbm": None,
                    "snr_db": None,
                    "noise_floor": None,
                    "measured_peak": False,
                    "whitelisted": True,
                    "seed_channel": False,
                    "source": "WHITELIST",
                    "la": wl_la,
                }

    seed_channels_str = str(cfg.get("TETRA_CHANNELS", "") or "").strip()
    if seed_channels_str:
        for raw_entry in seed_channels_str.split(","):
            raw_entry = raw_entry.strip()
            if not raw_entry:
                continue
            parts = raw_entry.split(":")
            try:
                f_hz = int(float(parts[0].strip()))
            except ValueError:
                continue
            if not (start_limit_hz <= f_hz <= end_limit_hz):
                continue
            seed_la = None
            if len(parts) > 1 and parts[1].strip().isdigit():
                seed_la = int(parts[1].strip())
            if f_hz in peaks_by_hz:
                peaks_by_hz[f_hz]["seed_channel"] = True
                if peaks_by_hz[f_hz].get("la") is None and seed_la is not None:
                    peaks_by_hz[f_hz]["la"] = seed_la
            else:
                grid_meas = measured_grid.get(f_hz)
                if grid_meas:
                    peaks_by_hz[f_hz] = {
                        "freq_hz": f_hz,
                        "dbm": grid_meas["dbm"],
                        "snr_db": grid_meas["snr_db"],
                        "noise_floor": grid_meas["noise_floor"],
                        "measured_peak": grid_meas["snr_db"] >= MIN_SNR_ABOVE_FLOOR_DB,
                        "whitelisted": False,
                        "seed_channel": True,
                        "source": "SEED",
                        "la": seed_la,
                    }
                else:
                    peaks_by_hz[f_hz] = {
                        "freq_hz": f_hz,
                        "dbm": None,
                        "snr_db": None,
                        "noise_floor": None,
                        "measured_peak": False,
                        "whitelisted": False,
                        "seed_channel": True,
                        "source": "SEED",
                        "la": seed_la,
                    }

    if not peaks_by_hz:
        return None, []

    filtered_peaks = []
    for f_hz, pk in peaks_by_hz.items():
        f_key = str(int(f_hz))
        pk["strikes"] = 0
        if f_key in blacklist:
            entry = blacklist[f_key] if isinstance(blacklist[f_key], dict) else {}
            expire_at = float(entry.get("expire_at", 0))
            strikes = int(entry.get("strikes", 0))
            if expire_at > 0:
                if now_ts < expire_at:
                    ign_msg = (
                        f"Ignoring blacklisted frequency {f_hz / 1e6:.4f} MHz "
                        f"({entry.get('reason', 'Blacklisted')})"
                    )
                    print(f"[scanner] {ign_msg}", flush=True)
                    emit_system_event(ign_msg)
                    continue
                else:
                    del blacklist[f_key]
                    blacklist_modified = True
            else:
                pk["strikes"] = strikes
        filtered_peaks.append(pk)

    if blacklist_modified:
        save_blacklist(blacklist)

    if not filtered_peaks:
        return None, []

    sorted_by_freq = sorted(filtered_peaks, key=lambda x: x["freq_hz"])

    unique_sorted = []
    for cand in sorted_by_freq:
        if not unique_sorted:
            unique_sorted.append(cand)
            continue
        prev = unique_sorted[-1]
        if abs(cand["freq_hz"] - prev["freq_hz"]) < MIN_PEAK_SEPARATION_HZ:
            cand_dbm = cand["dbm"] if cand.get("dbm") is not None else -999.0
            prev_dbm = prev["dbm"] if prev.get("dbm") is not None else -999.0
            if cand_dbm > prev_dbm:
                unique_sorted[-1] = cand
        else:
            unique_sorted.append(cand)

    max_bandwidth_hz = MAX_WINDOW_SPAN_HZ
    best_window_peaks = []
    best_score = (-1e9, -1, -1, -1e9, -1e9, -1e9)

    for freq_a_obj in unique_sorted:
        freq_a = freq_a_obj["freq_hz"]
        window_limit = freq_a + max_bandwidth_hz

        in_window = [
            p for p in unique_sorted if freq_a <= p["freq_hz"] <= window_limit
        ]
        if not in_window:
            continue

        if len(in_window) > MAX_PEAKS:
            in_window_strongest = sorted(
                in_window,
                key=lambda x: (
                    1 if x.get("whitelisted") else 0,
                    1 if x.get("seed_channel") else 0,
                    -int(x.get("strikes") or 0),
                    x["snr_db"] if x.get("snr_db") is not None else -999.0,
                    x["dbm"] if x.get("dbm") is not None else -999.0,
                ),
                reverse=True,
            )[:MAX_PEAKS]
            selected = sorted(in_window_strongest, key=lambda x: x["freq_hz"])
        else:
            selected = in_window

        wl_count = sum(
            1
            for p in selected
            if p.get("whitelisted")
        )
        strong_peaks = sum(
            1 for p in selected if (p.get("snr_db") or 0.0) >= 4.0
        )
        total_snr = round(
            sum(max(0.0, p["snr_db"]) for p in selected if p.get("snr_db") is not None),
            2,
        )
        max_snr = round(
            max((p["snr_db"] for p in selected if p.get("snr_db") is not None), default=0.0),
            2,
        )
        total_dbm = round(
            sum(p["dbm"] for p in selected if p.get("dbm") is not None), 2
        )

        weighted_score = 0.0
        for p in selected:
            snr = max(0.0, float(p.get("snr_db") or 0.0))
            dbm = float(p.get("dbm") if p.get("dbm") is not None else -65.0)
            dbm_bonus = max(0.0, (dbm + 45.0) * 0.4) if snr >= 3.0 else 0.0
            strikes = int(p.get("strikes") or 0)
            strike_factor = 0.12 ** strikes
            if p.get("whitelisted"):
                weighted_score += (25.0 + (snr * 2.5) + dbm_bonus) * strike_factor
            elif p.get("seed_channel"):
                weighted_score += (12.0 + (snr * 2.0) + dbm_bonus) * strike_factor
            else:
                weighted_score += (4.0 + (snr * 1.8) + dbm_bonus) * strike_factor

        score = (
            round(weighted_score, 2),
            wl_count,
            strong_peaks,
            total_snr,
            max_snr,
            total_dbm,
        )
        if score > best_score:
            best_score = score
            best_window_peaks = selected

    if not best_window_peaks:
        return None, []

    best_window_peaks.sort(key=lambda x: x["freq_hz"])
    center_hz = compute_safe_center_hz([p["freq_hz"] for p in best_window_peaks])

    if len(best_window_peaks) < MAX_PEAKS:
        existing_hz = {p["freq_hz"] for p in best_window_peaks}
        half_span = max_bandwidth_hz // 2
        extra_candidates = [
            p
            for p in unique_sorted
            if p["freq_hz"] not in existing_hz
            and abs(p["freq_hz"] - center_hz) <= half_span
        ]
        if extra_candidates:
            extra_sorted = sorted(
                extra_candidates,
                key=lambda x: (
                    1 if x.get("whitelisted") else 0,
                    1 if x.get("seed_channel") else 0,
                    -int(x.get("strikes") or 0),
                    x["snr_db"] if x.get("snr_db") is not None else -999.0,
                ),
                reverse=True,
            )[: MAX_PEAKS - len(best_window_peaks)]
            best_window_peaks.extend(extra_sorted)
            best_window_peaks.sort(key=lambda x: x["freq_hz"])
            min_f = best_window_peaks[0]["freq_hz"]
            max_f = best_window_peaks[-1]["freq_hz"]
            if (max_f - min_f) <= max_bandwidth_hz:
                center_hz = compute_safe_center_hz([p["freq_hz"] for p in best_window_peaks])

    if any(abs(p["freq_hz"] - center_hz) < 25000 for p in best_window_peaks):
        center_hz = compute_safe_center_hz([p["freq_hz"] for p in best_window_peaks])

    p2_lines = [
        f"[PHASE 2: SELECTED 1.8 MHz WINDOW]",
        f"  Center: {center_hz / 1e6:.4f} MHz | Candidates: {len(filtered_peaks)} | Whitelisted: {best_score[1]}",
    ]
    for idx, pk in enumerate(best_window_peaks, start=1):
        f_hz = pk["freq_hz"]
        f_mhz = f_hz / 1e6
        dbm_val = pk.get("dbm")
        snr_val = pk.get("snr_db")
        wl_tag = " [WHITELIST]" if pk.get("whitelisted") else (" [SEED]" if pk.get("seed_channel") else "")
        la_tag = f" (LA {pk['la']})" if pk.get("la") is not None else ""
        strike_tag = f" [Strike {pk['strikes']}/{cfg['BLACKLIST_STRIKES']}]" if pk.get("strikes") else ""
        if dbm_val is not None and snr_val is not None:
            p2_lines.append(
                f"  Ch #{idx}: {f_mhz:.4f} MHz  |  {dbm_val:>6.1f} dBm  |  +{snr_val:.1f} dB SNR{wl_tag}{la_tag}{strike_tag}"
            )
        else:
            p2_lines.append(
                f"  Ch #{idx}: {f_mhz:.4f} MHz  |  Static{wl_tag}{la_tag}{strike_tag}"
            )
    p2_msg = "\n".join(p2_lines)
    print(p2_msg, flush=True)
    emit_system_event(p2_msg)

    return center_hz, best_window_peaks


def stdout_reader_worker(proc, line_queue):
    try:
        for line in iter(proc.stdout.readline, ""):
            if not line:
                break
            line_queue.put(line)
    except Exception:
        pass


def phase3_monitor_channels(center_hz, channels_str, cfg, is_scanner_mode=True, active_frequencies=None):
    global ACTIVE_PROCS, GR_PROC_REF, RUNNING

    # Resolve RTL-SDR device index/serial without forcing a cold USB port reset on normal transitions
    cli_dev_arg, resolved_idx, resolved_ser = resolve_rtl_device_spec(cfg)
    needs_usb_recovery = False

    child_env = os.environ.copy()
    child_env["RTL_DEVICE_INDEX"] = str(cfg["RTL_DEVICE_INDEX"])
    child_env["RTL_SERIAL"] = str(resolved_ser or cfg.get("RTL_SERIAL", "")).strip()
    child_env["RTL_GAIN"] = str(cfg["RTL_GAIN"])
    child_env["RTL_PPM"] = str(cfg["RTL_PPM"])
    child_env["CENTER_FREQ"] = str(center_hz)
    child_env["TETRA_CHANNELS"] = channels_str

    raw_chans = [c.strip() for c in channels_str.split(",") if c.strip()]
    num_channels = len(raw_chans)

    if active_frequencies:
        freq_mhz_list = [f"{int(f) / 1e6:.4f}" for f in active_frequencies]
    else:
        freq_mhz_list = []
        for entry in raw_chans:
            try:
                hz_val = int(entry.split(":", 1)[0])
                freq_mhz_list.append(f"{hz_val / 1e6:.4f}")
            except ValueError:
                pass

    center_freq_mhz = f"{center_hz / 1e6:.4f}"
    idle_timeout_sec = cfg["IDLE_TIMEOUT_SEC"]
    blacklist_strikes = cfg["BLACKLIST_STRIKES"]
    blacklist_time_sec = cfg["BLACKLIST_TIME_SEC"]

    mode_tag = "PHASE 3: MONITOR" if is_scanner_mode else "STATIC MODE"
    print(
        f"[scanner] [{mode_tag}] Tuning CENTER_FREQ={center_freq_mhz} MHz ({center_hz} Hz) | "
        f"Gain={cfg['RTL_GAIN']} dB | PPM={cfg['RTL_PPM']} | "
        f"SDR={cli_dev_arg} (idx={resolved_idx}, serial={resolved_ser or 'N/A'})",
        flush=True,
    )
    print(
        f"[scanner] [{mode_tag}] Active TETRA_CHANNELS={channels_str}",
        flush=True,
    )
    if is_scanner_mode:
        print(
            f"[scanner] [{mode_tag}] Idle timeout={idle_timeout_sec}s | "
            f"Strikes={blacklist_strikes} | Blacklist penalty={blacklist_time_sec}s",
            flush=True,
        )

    try:
        for idx in range(1, num_channels + 1):
            rx_port = 42000 + idx
            tx_port = 42100 + idx
            dec_proc = subprocess.Popen(
                [
                    "/opt/tetra-kit/decoder/decoder",
                    "-r",
                    str(rx_port),
                    "-t",
                    str(tx_port),
                ],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                env=child_env,
            )
            ACTIVE_PROCS.append(dec_proc)

        gr_proc = subprocess.Popen(
            ["python3", "/app/src/tetra_multi_rtl.py"],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
            env=child_env,
        )
        GR_PROC_REF = gr_proc
        ACTIVE_PROCS.append(gr_proc)

        parser_proc = subprocess.Popen(
            ["python3", "/app/src/tetra_parser.py"],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
            env=child_env,
        )
        ACTIVE_PROCS.append(parser_proc)

        line_queue = queue.Queue()
        reader_thread = threading.Thread(
            target=stdout_reader_worker,
            args=(parser_proc, line_queue),
            daemon=True,
        )
        reader_thread.start()

        gr_reader_thread = threading.Thread(
            target=stdout_reader_worker,
            args=(gr_proc, line_queue),
            daemon=True,
        )
        gr_reader_thread.start()

        freqs_joined = ", ".join(freq_mhz_list)
        if is_scanner_mode:
            sys_msg = (
                f"[PHASE 3: LIVE TETRA DECODING]\n"
                f"  Center: {center_freq_mhz} MHz | Gain: {cfg['RTL_GAIN']} dB | Channels ({len(freq_mhz_list)}): {freqs_joined} MHz\n"
                f"  Status: Listening for TETRA sync (SYSINFO), SDS, Calls & Voice (Idle timeout: {idle_timeout_sec}s)"
            )
        else:
            sys_msg = (
                f"[STATIC MODE: LIVE TETRA DECODING]\n"
                f"  Center: {center_freq_mhz} MHz | Gain: {cfg['RTL_GAIN']} dB\n"
                f"  Channels ({len(freq_mhz_list)}): {freqs_joined} MHz"
            )
        emit_system_event(sys_msg)

        last_activity = time.time()
        chan_mhz_to_hz = {
            f"{int(f) / 1e6:.4f}": int(f) for f in (active_frequencies or [])
        }
        verified_tetra_hz = set()
        dead_blacklisted_hz = set()
        tuner_diag_lines = []

        while RUNNING:
            try:
                line = line_queue.get(timeout=0.5)
                clean_line = line.rstrip("\r\n")
                print(clean_line, flush=True)
                if any(
                    k in clean_line
                    for k in (
                        "Using device #",
                        "Found Rafael Micro",
                        "RTL-SDR Blog V4",
                        "[R82XX] PLL not locked!",
                    )
                ):
                    tuner_diag_lines.append(clean_line)
                elif clean_line.startswith("[gnuradio] R82XX Tuner locked:"):
                    tuner_diag_lines.append(clean_line)
                    emit_system_event(
                        "[SDR TUNER INIT]\n  " + "\n  ".join(tuner_diag_lines)
                    )
                    tuner_diag_lines.clear()
                if (
                    "usb_claim_interface error" in clean_line
                    or "LIBUSB_ERROR" in clean_line
                    or "Failed to open rtlsdr device" in clean_line
                    or "No supported devices found" in clean_line
                ):
                    usb_err_msg = (
                        f"[SDR HARDWARE RECOVERY] Detected fatal USB/driver fault ({clean_line}). "
                        f"Scheduling USB hardware reset and restarting pipeline..."
                    )
                    print(f"[scanner] {usb_err_msg}", flush=True)
                    emit_system_event(usb_err_msg)
                    needs_usb_recovery = True
                    break
                if any(tag in clean_line for tag in PAYLOAD_TAGS):
                    last_activity = time.time()
                    for mhz_str, hz_val in chan_mhz_to_hz.items():
                        if mhz_str in clean_line:
                            verified_tetra_hz.add(hz_val)
                elif "[NETWORK]" in clean_line or "[WHITELIST]" in clean_line:
                    for mhz_str, hz_val in chan_mhz_to_hz.items():
                        if mhz_str in clean_line:
                            verified_tetra_hz.add(hz_val)
                elif clean_line.startswith("[heartbeat]"):
                    if "per channel:" in clean_line:
                        after_per_chan = clean_line.split("per channel:", 1)[1].strip()
                        raw_dict_str = after_per_chan
                        raw_bd_str = None
                        if "| pdu_breakdown:" in after_per_chan:
                            raw_dict_str, raw_bd_str = [
                                s.strip()
                                for s in after_per_chan.split("| pdu_breakdown:", 1)
                            ]
                        per_chan = None
                        pdu_bd = {}
                        try:
                            per_chan = json.loads(raw_dict_str)
                        except Exception:
                            try:
                                per_chan = json.loads(raw_dict_str.replace("'", '"'))
                            except Exception:
                                per_chan = None
                        if raw_bd_str:
                            try:
                                pdu_bd = json.loads(raw_bd_str)
                            except Exception:
                                pdu_bd = {}

                        if isinstance(per_chan, dict):
                            hb_lines = ["[60s CHANNEL CHECK]"]
                            for m_str, p_cnt in per_chan.items():
                                crc_cnt = int(p_cnt or 0)
                                ch_bd = (
                                    pdu_bd.get(m_str, {})
                                    if isinstance(pdu_bd, dict)
                                    else {}
                                )
                                sys_c = int(ch_bd.get("sysinfo", 0) or 0)
                                sync_c = int(ch_bd.get("sync", 0) or 0)
                                mac_c = int(ch_bd.get("mac", 0) or 0)
                                sds_c = int(ch_bd.get("sds", 0) or 0)
                                call_c = int(ch_bd.get("call", 0) or 0)
                                tch_c = int(ch_bd.get("tch_s", 0) or 0)

                                status_lbl = (
                                    "ACTIVE TETRA (CRC Sync OK)"
                                    if crc_cnt > 0
                                    else "Silent / No TETRA CRC Sync"
                                )
                                if crc_cnt > 0:
                                    bd_tag = (
                                        f" [SYSINFO:{sys_c}, SYNC:{sync_c}, "
                                        f"MAC:{mac_c}, SDS:{sds_c}, CALL:{call_c} | TCH_S:{tch_c}]"
                                    )
                                elif tch_c > 0:
                                    bd_tag = f" [SYSINFO:0, SYNC:0 | TCH_S noise:{tch_c}]"
                                else:
                                    bd_tag = ""
                                hb_lines.append(
                                    f"  - {m_str} MHz: {crc_cnt} pkt/min{bd_tag} ({status_lbl})"
                                )
                            emit_system_event("\n".join(hb_lines))
                        else:
                            emit_system_event(clean_line)
                    else:
                        emit_system_event(clean_line)

                    if is_scanner_mode and "per channel:" in clean_line and chan_mhz_to_hz:
                        if isinstance(per_chan, dict):
                            alive_in_hb = []
                            dead_in_hb = []
                            for mhz_str, pkt_cnt in per_chan.items():
                                hz_val = chan_mhz_to_hz.get(str(mhz_str).strip())
                                if hz_val is None:
                                    continue
                                try:
                                    cnt = int(pkt_cnt)
                                except (ValueError, TypeError):
                                    cnt = 0
                                if cnt > 0:
                                    alive_in_hb.append(hz_val)
                                else:
                                    dead_in_hb.append(hz_val)

                            if alive_in_hb:
                                verified_tetra_hz.update(alive_in_hb)
                                clear_frequency_strikes(alive_in_hb)

                            if not alive_in_hb and not verified_tetra_hz and dead_in_hb:
                                add_frequencies_to_blacklist(
                                    dead_in_hb,
                                    blacklist_time_sec,
                                    blacklist_strikes,
                                    "No TETRA sync (0 pkt/min)",
                                    immediate=False,
                                    force_even_if_whitelisted=False,
                                )
                                dead_blacklisted_hz.update(dead_in_hb)
                                abort_msg = (
                                    "[WINDOW ROTATION] 0 TETRA packets received on all channels in this window.\n"
                                    "  Recorded 1 strike and rotating to next candidate window..."
                                )
                                print(f"[scanner] {abort_msg}", flush=True)
                                emit_system_event(abort_msg)
                                break

                            isolated_dead = [
                                hz
                                for hz in dead_in_hb
                                if hz not in verified_tetra_hz
                                and hz not in dead_blacklisted_hz
                            ]
                            if isolated_dead:
                                add_frequencies_to_blacklist(
                                    isolated_dead,
                                    blacklist_time_sec,
                                    blacklist_strikes,
                                    "No TETRA sync (0 pkt/min)",
                                    immediate=False,
                                    force_even_if_whitelisted=False,
                                )
                                dead_blacklisted_hz.update(isolated_dead)
            except queue.Empty:
                pass

            if os.path.exists(SOFT_CONFIG_FLAG):
                try:
                    os.remove(SOFT_CONFIG_FLAG)
                except OSError:
                    pass
                new_cfg = load_runtime_config()
                idle_timeout_sec = new_cfg["IDLE_TIMEOUT_SEC"]
                blacklist_strikes = new_cfg["BLACKLIST_STRIKES"]
                blacklist_time_sec = new_cfg["BLACKLIST_TIME_SEC"]
                print(
                    f"[scanner] Hot-reloaded parameters: IDLE_TIMEOUT_SEC={idle_timeout_sec}s, "
                    f"BLACKLIST_STRIKES={blacklist_strikes}, BLACKLIST_TIME_SEC={blacklist_time_sec}s",
                    flush=True,
                )

            if os.path.exists(RELOAD_CONFIG_FLAG):
                try:
                    os.remove(RELOAD_CONFIG_FLAG)
                except OSError:
                    pass
                print(
                    "[scanner] Hot-reloading receiver pipeline with updated WebUI settings...",
                    flush=True,
                )
                break

            if is_scanner_mode and os.path.exists(FORCE_SWEEP_FLAG):
                try:
                    os.remove(FORCE_SWEEP_FLAG)
                except OSError:
                    pass
                force_msg = "Manual Force Sweep flag detected. Blacklisting current frequencies for 900s..."
                print(f"[scanner] {force_msg}", flush=True)
                emit_system_event(force_msg)
                if active_frequencies:
                    add_frequencies_to_blacklist(
                        active_frequencies,
                        FORCE_SWEEP_PENALTY_SEC,
                        blacklist_strikes,
                        "Manual Force",
                        immediate=True,
                    )
                break

            if gr_proc.poll() is not None:
                crash_msg = (
                    "[SDR HARDWARE RECOVERY] tetra_multi_rtl.py exited unexpectedly. "
                    "Performing fallback USB reset and restarting pipeline..."
                )
                print(f"[scanner] {crash_msg}", flush=True)
                emit_system_event(crash_msg)
                needs_usb_recovery = True
                break

            if parser_proc.poll() is not None and line_queue.empty():
                print(
                    "[scanner] Warning: tetra_parser.py exited unexpectedly. Restarting pipeline...",
                    flush=True,
                )
                break

            if is_scanner_mode and (time.time() - last_activity) > idle_timeout_sec:
                idle_msg = "IDLE TIMEOUT REACHED - No active payload data. Restarting scan..."
                print(idle_msg, flush=True)
                emit_system_event(idle_msg)
                if active_frequencies:
                    if verified_tetra_hz:
                        kept_str = ", ".join(
                            f"{hz / 1e6:.4f} MHz" for hz in sorted(verified_tetra_hz)
                        )
                        keep_msg = (
                            f"Preserving verified TETRA base station(s) with active control frames "
                            f"(>0 pkt/min): {kept_str}"
                        )
                        print(f"[scanner] {keep_msg}", flush=True)
                        emit_system_event(keep_msg)
                    unverified_idle = [
                        hz
                        for hz in active_frequencies
                        if hz not in verified_tetra_hz
                        and hz not in dead_blacklisted_hz
                    ]
                    if unverified_idle:
                        add_frequencies_to_blacklist(
                            unverified_idle,
                            blacklist_time_sec,
                            blacklist_strikes,
                            "Idle Timeout",
                            immediate=False,
                        )
                break
    finally:
        terminate_children()
        if needs_usb_recovery and RUNNING:
            reset_rtl_usb_device(cfg, center_hz=center_hz, perform_usb_ioctl=True)
        time.sleep(1.5)


def main():
    migrate_discovered_networks_and_blacklist()
    cycle = 1
    try:
        while RUNNING:
            for flag_path in (RELOAD_CONFIG_FLAG, SOFT_CONFIG_FLAG, FORCE_SWEEP_FLAG):
                if os.path.exists(flag_path):
                    try:
                        os.remove(flag_path)
                    except OSError:
                        pass

            cfg = load_runtime_config()
            mode = cfg["MODE"]

            if mode == "stopped":
                terminate_children()
                stop_msg = (
                    f"[STOPPED / SDR RELEASED] Scanning stopped. RTL-SDR device #{cfg['RTL_DEVICE_INDEX']} "
                    f"is completely released and free for other applications."
                )
                print(f"[scanner] {stop_msg}", flush=True)
                emit_system_event(stop_msg)
                while RUNNING:
                    if os.path.exists(RELOAD_CONFIG_FLAG) or os.path.exists(FORCE_SWEEP_FLAG):
                        break
                    time.sleep(0.4)
                continue

            if mode == "static":
                print(
                    f"[scanner] === Running in STATIC Mode (CENTER_FREQ={cfg['CENTER_FREQ']} Hz, "
                    f"GAIN={cfg['RTL_GAIN']} dB, PPM={cfg['RTL_PPM']}) ===",
                    flush=True,
                )
                phase3_monitor_channels(
                    center_hz=cfg["CENTER_FREQ"],
                    channels_str=cfg["TETRA_CHANNELS"],
                    cfg=cfg,
                    is_scanner_mode=False,
                )
                continue

            centers = build_center_frequencies(
                cfg["SCAN_START_MHZ"], cfg["SCAN_END_MHZ"]
            )
            wl_now = load_whitelist()
            bl_now = load_blacklist()
            if cycle == 1:
                intro_msg = (
                    f"[STARTUP] TETRA Smart Auto-Scanner\n"
                    f"  1. PHASE 1: Sweeps {cfg['SCAN_START_MHZ']:.1f} - {cfg['SCAN_END_MHZ']:.1f} MHz for RF signal peaks\n"
                    f"  2. PHASE 2: Selects best 1.8 MHz window (up to 8 channels)\n"
                    f"  3. PHASE 3: Verifies TETRA base stations (SYSINFO) & decodes SDS/Voice"
                )
                print(intro_msg, flush=True)
                emit_system_event(intro_msg)

            cycle_msg = (
                f"[CYCLE #{cycle}] Band: {cfg['SCAN_START_MHZ']:.1f} - {cfg['SCAN_END_MHZ']:.1f} MHz | "
                f"Gain: {cfg['RTL_GAIN']} dB\n"
                f"  Whitelisted Cells: {len(wl_now)} | Blacklist/Warnings: {len(bl_now)} | "
                f"Strikes Policy: {cfg['BLACKLIST_STRIKES']} strikes -> {cfg['BLACKLIST_TIME_SEC']}s"
            )
            print(cycle_msg, flush=True)
            emit_system_event(cycle_msg)

            all_peaks, measured_grid = phase1_sweep_band(centers, cfg)
            if not RUNNING:
                break
            if os.path.exists(RELOAD_CONFIG_FLAG):
                continue

            cfg = load_runtime_config()
            center_hz, best_peaks = phase2_select_best_window(
                all_peaks, cfg, measured_grid=measured_grid
            )
            if not best_peaks or center_hz is None:
                retry_msg = "No non-blacklisted TETRA carriers available. Retrying in 10s..."
                print(f"[scanner] {retry_msg}", flush=True)
                emit_system_event(retry_msg)
                for _ in range(20):
                    if not RUNNING or os.path.exists(RELOAD_CONFIG_FLAG):
                        break
                    time.sleep(0.5)
                cycle += 1
                continue

            active_frequencies = [int(pk["freq_hz"]) for pk in best_peaks]
            channels_list = [
                f"{pk['freq_hz']}:{pk.get('la') if pk.get('la') is not None else '?'}"
                for idx, pk in enumerate(best_peaks, start=1)
            ]
            channels_str = ",".join(channels_list)

            phase3_monitor_channels(
                center_hz=center_hz,
                channels_str=channels_str,
                cfg=cfg,
                is_scanner_mode=True,
                active_frequencies=active_frequencies,
            )
            cycle += 1
    finally:
        terminate_children()
        print("[scanner] Orchestrator stopped cleanly.", flush=True)


if __name__ == "__main__":
    main()
