#!/usr/bin/env python3
import os
import sys
import csv
import json
import time
import queue
import signal
import atexit
import threading
import subprocess
import statistics
from datetime import datetime, timezone


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
BLACKLIST_FILE = os.path.join(LOG_DIR, "blacklist.json")
WHITELIST_FILE = os.path.join(LOG_DIR, "whitelist.json")
RUNTIME_CONFIG_FILE = os.path.join(LOG_DIR, "runtime_config.json")

FORCE_SWEEP_FLAG = "/tmp/force_sweep.flag"
RELOAD_CONFIG_FLAG = "/tmp/reload_config.flag"
SOFT_CONFIG_FLAG = "/tmp/soft_config.flag"

FORCE_SWEEP_PENALTY_SEC = 900
MAX_PEAKS = 8
PTS_GRID_HZ = 25000
MIN_PEAK_SEPARATION_HZ = 25000
LOCAL_PEAK_RADIUS_HZ = 16000
DC_SPIKE_GUARD_HZ = 15000
EDGE_GUARD_HZ = 15000
MAX_WINDOW_SPAN_HZ = 1800000
MIN_SNR_ABOVE_FLOOR_DB = parse_env_float("SCAN_MIN_SNR_DB", 3.0)
ABSOLUTE_MIN_DBM = parse_env_float("SCAN_MIN_DBM", -65.0)

PAYLOAD_TAGS = ("[SDS]", "[CALL]", "[VOICE]", "[STATUS]", "[ROAMING]")

ACTIVE_PROCS = []
RUNNING = True


def get_default_config():
    return {
        "RTL_DEVICE_INDEX": parse_env_int("RTL_DEVICE_INDEX", 1),
        "RTL_GAIN": parse_env_float("RTL_GAIN", 24.0),
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

    cfg["RTL_DEVICE_INDEX"] = int(cfg.get("RTL_DEVICE_INDEX", 1))
    cfg["RTL_GAIN"] = float(cfg.get("RTL_GAIN", 24.0))
    cfg["RTL_PPM"] = int(cfg.get("RTL_PPM", 0))
    cfg["MODE"] = str(cfg.get("MODE", "scanner")).strip().lower()
    if cfg["MODE"] not in ("scanner", "static"):
        cfg["MODE"] = "scanner"
    cfg["SCAN_START_MHZ"] = float(cfg.get("SCAN_START_MHZ", 420.0))
    cfg["SCAN_END_MHZ"] = float(cfg.get("SCAN_END_MHZ", 430.0))
    if cfg["SCAN_END_MHZ"] <= cfg["SCAN_START_MHZ"]:
        cfg["SCAN_END_MHZ"] = cfg["SCAN_START_MHZ"] + 2.0
    cfg["SCAN_DWELL_TIME"] = max(10, int(cfg.get("SCAN_DWELL_TIME", 120)))
    cfg["IDLE_TIMEOUT_SEC"] = max(15, int(cfg.get("IDLE_TIMEOUT_SEC", 900)))
    cfg["BLACKLIST_STRIKES"] = max(1, int(cfg.get("BLACKLIST_STRIKES", 3)))
    cfg["BLACKLIST_TIME_SEC"] = max(10, int(cfg.get("BLACKLIST_TIME_SEC", 3600)))
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
                cleaned[k] = v
            if modified:
                save_whitelist(cleaned)
            return cleaned
    except Exception as exc:
        print(f"[scanner] Warning: failed reading {WHITELIST_FILE}: {exc}", flush=True)
    return {}


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


def terminate_children():
    global ACTIVE_PROCS
    if not ACTIVE_PROCS:
        return

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


def run_rtl_power_sweep(center_mhz, start_hz, end_hz, cfg):
    csv_label = f"{center_mhz:g}"
    csv_path = f"/tmp/scan_{csv_label}.csv"
    try:
        if os.path.exists(csv_path):
            os.remove(csv_path)
    except OSError:
        pass

    cmd = [
        "rtl_power",
        "-d",
        str(cfg["RTL_DEVICE_INDEX"]),
        "-f",
        f"{start_hz}:{end_hz}:2k",
        "-g",
        str(cfg["RTL_GAIN"]),
        "-p",
        str(cfg["RTL_PPM"]),
        "-i",
        "10",
        "-1",
        csv_path,
    ]
    scan_msg = (
        f"[PHASE 1: SWEEP] Scanning {start_hz / 1e6:.3f}-{end_hz / 1e6:.3f} MHz "
        f"(center {center_mhz:.3f} MHz, gain={cfg['RTL_GAIN']} dB, 2k step, 10s)"
    )
    print(f"[scanner] {scan_msg} -> {csv_path}", flush=True)
    emit_system_event(scan_msg)
    try:
        res = subprocess.run(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            timeout=35,
            check=False,
        )
        if res.returncode != 0:
            print(
                f"[scanner] rtl_power exited with code {res.returncode}: {res.stderr.strip()}",
                flush=True,
            )
    except Exception as exc:
        print(f"[scanner] Failed running rtl_power: {exc}", flush=True)
        return None

    if os.path.exists(csv_path) and os.path.getsize(csv_path) > 0:
        return csv_path
    return None


def parse_block_csv(csv_path, start_hz, end_hz, center_hz):
    if not csv_path or not os.path.exists(csv_path):
        return [], {}

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
        return [], {}

    if not fine_bins:
        return [], {}

    avg_spectrum = sorted(
        (freq, sum(vals) / float(len(vals))) for freq, vals in fine_bins.items()
    )
    all_dbm = [dbm for _, dbm in avg_spectrum]
    noise_floor = statistics.median(all_dbm)
    min_threshold = max(noise_floor + MIN_SNR_ABOVE_FLOOR_DB, ABSOLUTE_MIN_DBM)

    block_grid = {}
    snapped_peaks = {}
    num_bins = len(avg_spectrum)

    for idx, (freq_hz, dbm) in enumerate(avg_spectrum):
        snapped_25k = int(round(freq_hz / float(PTS_GRID_HZ)) * PTS_GRID_HZ)
        snr_db = round(dbm - noise_floor, 1)
        grid_entry = {
            "freq_hz": snapped_25k,
            "dbm": round(dbm, 2),
            "snr_db": snr_db,
            "noise_floor": round(noise_floor, 2),
        }
        if (
            snapped_25k not in block_grid
            or grid_entry["dbm"] > block_grid[snapped_25k]["dbm"]
        ):
            block_grid[snapped_25k] = grid_entry

        if dbm < min_threshold:
            continue

        is_local_max = True
        j = idx - 1
        while j >= 0 and (freq_hz - avg_spectrum[j][0]) <= LOCAL_PEAK_RADIUS_HZ:
            if avg_spectrum[j][1] > dbm:
                is_local_max = False
                break
            j -= 1

        if not is_local_max:
            continue

        j = idx + 1
        while j < num_bins and (avg_spectrum[j][0] - freq_hz) <= LOCAL_PEAK_RADIUS_HZ:
            if avg_spectrum[j][1] > dbm:
                is_local_max = False
                break
            j += 1

        if not is_local_max:
            continue

        entry = {
            "freq_hz": snapped_25k,
            "dbm": round(dbm, 2),
            "snr_db": snr_db,
            "noise_floor": round(noise_floor, 2),
            "measured_peak": True,
        }
        if (
            snapped_25k not in snapped_peaks
            or entry["dbm"] > snapped_peaks[snapped_25k]["dbm"]
        ):
            snapped_peaks[snapped_25k] = entry

    block_peaks = list(snapped_peaks.values())
    block_peaks.sort(key=lambda x: x["dbm"], reverse=True)
    block_summary = (
        f"Block {start_hz / 1e6:.3f}-{end_hz / 1e6:.3f} MHz | "
        f"Noise floor: {noise_floor:.1f} dBm | Threshold (+{MIN_SNR_ABOVE_FLOOR_DB:.1f} dB): "
        f"{min_threshold:.1f} dBm | Peaks found: {len(block_peaks)}"
    )
    print(f"[scanner]   {block_summary}", flush=True)
    emit_system_event(block_summary)
    for pk in block_peaks[:8]:
        cand_msg = (
            f"Candidate: {pk['freq_hz'] / 1e6:.4f} MHz "
            f"({pk['dbm']:.1f} dBm, +{pk['snr_db']:.1f} dB SNR)"
        )
        print(f"[scanner]     {cand_msg}", flush=True)
        emit_system_event(cand_msg)
    return block_peaks, block_grid


def phase1_sweep_band(centers, cfg):
    print(
        "==============================================================================================",
        flush=True,
    )
    sweep_start_msg = (
        f"[PHASE 1: SWEEP] Starting high-resolution full-band hunt across "
        f"{cfg['SCAN_START_MHZ']:.3f}-{cfg['SCAN_END_MHZ']:.3f} MHz (2 kHz step -> 25 kHz PTS grid)"
    )
    print(f"[scanner] {sweep_start_msg}", flush=True)
    emit_system_event(sweep_start_msg)
    print(
        "==============================================================================================",
        flush=True,
    )

    merged_peaks = {}
    merged_grid = {}
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

        block_peaks, block_grid = parse_block_csv(csv_path, start_hz, end_hz, center_hz)
        for f_hz, g_entry in block_grid.items():
            if f_hz not in merged_grid or g_entry["dbm"] > merged_grid[f_hz]["dbm"]:
                merged_grid[f_hz] = g_entry
        for pk in block_peaks:
            f_hz = pk["freq_hz"]
            if f_hz not in merged_peaks or pk["dbm"] > merged_peaks[f_hz]["dbm"]:
                merged_peaks[f_hz] = pk

    all_peaks = list(merged_peaks.values())
    all_peaks.sort(key=lambda x: x["dbm"], reverse=True)
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
            if grid_meas and grid_meas["dbm"] >= ABSOLUTE_MIN_DBM and grid_meas["snr_db"] >= 1.5:
                peaks_by_hz[f_hz] = {
                    "freq_hz": f_hz,
                    "dbm": grid_meas["dbm"],
                    "snr_db": grid_meas["snr_db"],
                    "noise_floor": grid_meas["noise_floor"],
                    "measured_peak": grid_meas["snr_db"] >= MIN_SNR_ABOVE_FLOOR_DB,
                    "whitelisted": True,
                    "source": "MEASURED+WHITELIST",
                    "la": wl_la,
                }
            elif not all_peaks:
                peaks_by_hz[f_hz] = {
                    "freq_hz": f_hz,
                    "dbm": grid_meas["dbm"] if grid_meas else None,
                    "snr_db": grid_meas["snr_db"] if grid_meas else None,
                    "noise_floor": grid_meas["noise_floor"] if grid_meas else None,
                    "measured_peak": False,
                    "whitelisted": True,
                    "source": "WHITELIST",
                    "la": wl_la,
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
                    1 if (x.get("whitelisted") and (x.get("snr_db") or 0.0) >= 1.5) else 0,
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
            if p.get("whitelisted") and (p.get("snr_db") is None or p["snr_db"] >= 1.5)
        )
        strong_peaks = sum(
            1 for p in selected if (p.get("snr_db") or 0.0) >= 6.0
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
            dbm_bonus = max(0.0, (dbm + 45.0) * 0.4) if snr >= 5.0 else 0.0
            if p.get("whitelisted") and (p.get("snr_db") is None or snr >= 1.5):
                weighted_score += 25.0 + (snr * 2.5) + dbm_bonus
            elif snr >= 6.0:
                weighted_score += 8.0 + (snr * 2.0) + dbm_bonus
            else:
                weighted_score += snr * 0.6

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
    min_f = best_window_peaks[0]["freq_hz"]
    max_f = best_window_peaks[-1]["freq_hz"]
    raw_center = (min_f + max_f) // 2
    center_hz = int(round(raw_center / float(PTS_GRID_HZ)) * PTS_GRID_HZ)

    for p in best_window_peaks:
        if abs(p["freq_hz"] - center_hz) < 12500:
            center_hz += 12500
            break

    print(
        "==============================================================================================",
        flush=True,
    )
    print(
        "  [PHASE 2: TOP LIST] WINNING 2 MHz TETRA WINDOW (Sliding Window 1.8 MHz)",
        flush=True,
    )
    print(
        "----------------------------------------------------------------------------------------------",
        flush=True,
    )
    print(
        "  Chan # | Frequency (MHz) | Frequency (Hz) | Signal (dBm) | SNR (dB) | Offset / Source",
        flush=True,
    )
    print(
        "---------+-----------------+----------------+--------------+----------+-----------------------",
        flush=True,
    )
    win_parts = []
    for idx, pk in enumerate(best_window_peaks, start=1):
        f_hz = pk["freq_hz"]
        f_mhz = f_hz / 1e6
        offset_khz = (f_hz - center_hz) / 1e3
        dbm_val = pk.get("dbm")
        snr_val = pk.get("snr_db")
        dbm_str = f"{dbm_val:>7.1f} dBm" if dbm_val is not None else "    N/A dBm"
        snr_str = f"+{snr_val:>4.1f} dB" if snr_val is not None else "  N/A dB"
        wl_tag = " [WHITELIST]" if pk.get("whitelisted") else ""
        la_tag = f" (LA {pk['la']})" if pk.get("la") is not None else ""
        print(
            f"  #{idx:<5} | {f_mhz:>11.4f} MHz | {f_hz:>11} Hz | {dbm_str} | "
            f"{snr_str} | {offset_khz:>+8.1f} kHz{wl_tag}{la_tag}",
            flush=True,
        )
        if dbm_val is not None and snr_val is not None:
            win_parts.append(
                f"#{idx} {f_mhz:.4f} MHz ({dbm_val:.1f} dBm, +{snr_val:.1f} dB SNR){wl_tag}{la_tag}"
            )
        else:
            win_parts.append(f"#{idx} {f_mhz:.4f} MHz (static){wl_tag}{la_tag}")
    print(
        "==============================================================================================",
        flush=True,
    )
    emit_system_event(
        f"[PHASE 2: TOP LIST] Winning 1.8 MHz Window (Center {center_hz / 1e6:.4f} MHz): "
        + " | ".join(win_parts)
    )

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
    global ACTIVE_PROCS, RUNNING

    os.environ["RTL_DEVICE_INDEX"] = str(cfg["RTL_DEVICE_INDEX"])
    os.environ["RTL_GAIN"] = str(cfg["RTL_GAIN"])
    os.environ["RTL_PPM"] = str(cfg["RTL_PPM"])
    os.environ["CENTER_FREQ"] = str(center_hz)
    os.environ["TETRA_CHANNELS"] = channels_str
    child_env = os.environ.copy()

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
        f"Gain={cfg['RTL_GAIN']} dB | PPM={cfg['RTL_PPM']}",
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
            env=child_env,
        )
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

        freqs_joined = ", ".join(freq_mhz_list)
        if is_scanner_mode:
            sys_msg = (
                f"Sweep complete. Locked on center {center_freq_mhz} MHz. "
                f"Monitoring {len(freq_mhz_list)} active channels ({freqs_joined}) "
                f"for {idle_timeout_sec}s idle timeout."
            )
        else:
            sys_msg = (
                f"Static mode active. Locked on center {center_freq_mhz} MHz. "
                f"Monitoring {len(freq_mhz_list)} channels ({freqs_joined})."
            )
        emit_system_event(sys_msg)

        last_activity = time.time()
        chan_mhz_to_hz = {
            f"{int(f) / 1e6:.4f}": int(f) for f in (active_frequencies or [])
        }
        verified_tetra_hz = set()
        dead_blacklisted_hz = set()

        while RUNNING:
            try:
                line = line_queue.get(timeout=0.5)
                clean_line = line.rstrip("\r\n")
                print(clean_line, flush=True)
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
                    emit_system_event(clean_line)
                    if is_scanner_mode and "per channel:" in clean_line and chan_mhz_to_hz:
                        raw_dict_str = clean_line.split("per channel:", 1)[1].strip()
                        per_chan = None
                        try:
                            per_chan = json.loads(raw_dict_str)
                        except Exception:
                            try:
                                per_chan = json.loads(raw_dict_str.replace("'", '"'))
                            except Exception:
                                per_chan = None

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
                                    immediate=True,
                                    force_even_if_whitelisted=True,
                                )
                                dead_blacklisted_hz.update(dead_in_hb)
                                abort_msg = (
                                    "All monitored channels in 1.8 MHz window reported 0 pkt/min "
                                    "(no TETRA sync). Breaking dead window immediately..."
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
                                    immediate=True,
                                    force_even_if_whitelisted=True,
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
                print(
                    "[scanner] Warning: tetra_multi_rtl.py exited unexpectedly. Restarting pipeline...",
                    flush=True,
                )
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
        time.sleep(1.5)


def main():
    cycle = 1
    try:
        while RUNNING:
            for flag_path in (RELOAD_CONFIG_FLAG, SOFT_CONFIG_FLAG):
                if os.path.exists(flag_path):
                    try:
                        os.remove(flag_path)
                    except OSError:
                        pass

            cfg = load_runtime_config()
            mode = cfg["MODE"]

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
            cycle_msg = (
                f"=== Smart Auto-Tune Cycle #{cycle} | "
                f"{cfg['SCAN_START_MHZ']:.1f}-{cfg['SCAN_END_MHZ']:.1f} MHz | "
                f"GAIN={cfg['RTL_GAIN']} dB | IDLE_TIMEOUT={cfg['IDLE_TIMEOUT_SEC']}s | "
                f"STRIKES={cfg['BLACKLIST_STRIKES']} ==="
            )
            print(f"[scanner] {cycle_msg}", flush=True)
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
                f"{pk['freq_hz']}:{pk.get('la') if pk.get('la') is not None else idx}"
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
