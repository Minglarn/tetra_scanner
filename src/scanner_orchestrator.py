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
BLACKLIST_FILE = os.path.join(LOG_DIR, "blacklist.json")
WHITELIST_FILE = os.path.join(LOG_DIR, "whitelist.json")
RUNTIME_CONFIG_FILE = os.path.join(LOG_DIR, "runtime_config.json")

FORCE_SWEEP_FLAG = "/tmp/force_sweep.flag"
RELOAD_CONFIG_FLAG = "/tmp/reload_config.flag"
SOFT_CONFIG_FLAG = "/tmp/soft_config.flag"

FORCE_SWEEP_PENALTY_SEC = 900
MAX_PEAKS = 6
PTS_GRID_HZ = 25000
MIN_PEAK_SEPARATION_HZ = 25000
LOCAL_PEAK_RADIUS_HZ = 16000
DC_SPIKE_GUARD_HZ = 15000
EDGE_GUARD_HZ = 15000
MAX_WINDOW_SPAN_HZ = 1800000
MIN_SNR_ABOVE_FLOOR_DB = parse_env_float("SCAN_MIN_SNR_DB", 3.0)
ABSOLUTE_MIN_DBM = parse_env_float("SCAN_MIN_DBM", -65.0)

PAYLOAD_TAGS = ("[SDS]", "[CALL]", "[STATUS]", "[ROAMING]")

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


def load_whitelist():
    if not os.path.exists(WHITELIST_FILE):
        return {}
    try:
        with open(WHITELIST_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
            if isinstance(data, dict):
                return data
    except Exception as exc:
        print(f"[scanner] Warning: failed reading {WHITELIST_FILE}: {exc}", flush=True)
    return {}


def add_frequencies_to_blacklist(
    freq_list_hz, duration_sec, max_strikes, reason, immediate=False
):
    bl = load_blacklist()
    wl = load_whitelist()
    now_ts = time.time()

    for f_hz in freq_list_hz:
        key = str(int(f_hz))
        if key in wl and not immediate:
            print(
                f"[scanner] Keeping whitelisted frequency {int(f_hz) / 1e6:.4f} MHz (protected from Idle Timeout blacklist)",
                flush=True,
            )
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
            print(
                f"[scanner] Blacklisted {int(f_hz) / 1e6:.4f} MHz for {duration_sec}s "
                f"(Strike {strikes}/{max_strikes} - {reason})",
                flush=True,
            )
        else:
            bl[key] = {
                "strikes": strikes,
                "max_strikes": max_strikes,
                "expire_at": 0,
                "last_strike_at": round(now_ts, 2),
                "reason": f"Warning {strikes}/{max_strikes} ({reason})",
            }
            print(
                f"[scanner] Frequency {int(f_hz) / 1e6:.4f} MHz quiet: recorded Strike "
                f"{strikes}/{max_strikes} (not blacklisted yet)",
                flush=True,
            )

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
    print(
        f"[scanner] [PHASE 1: SWEEP] Scanning {start_hz / 1e6:.3f}-{end_hz / 1e6:.3f} MHz "
        f"(center {center_mhz:.3f} MHz, gain={cfg['RTL_GAIN']} dB, 2k step, 10s) -> {csv_path}",
        flush=True,
    )
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
        return []

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
        return []

    if not fine_bins:
        return []

    avg_spectrum = sorted(
        (freq, sum(vals) / float(len(vals))) for freq, vals in fine_bins.items()
    )
    all_dbm = [dbm for _, dbm in avg_spectrum]
    noise_floor = statistics.median(all_dbm)
    min_threshold = max(noise_floor + MIN_SNR_ABOVE_FLOOR_DB, ABSOLUTE_MIN_DBM)

    snapped_peaks = {}
    num_bins = len(avg_spectrum)

    for idx, (freq_hz, dbm) in enumerate(avg_spectrum):
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

        snapped_25k = int(round(freq_hz / float(PTS_GRID_HZ)) * PTS_GRID_HZ)
        snr_db = round(dbm - noise_floor, 1)
        entry = {
            "freq_hz": snapped_25k,
            "dbm": round(dbm, 2),
            "snr_db": snr_db,
            "noise_floor": round(noise_floor, 2),
        }
        if (
            snapped_25k not in snapped_peaks
            or entry["dbm"] > snapped_peaks[snapped_25k]["dbm"]
        ):
            snapped_peaks[snapped_25k] = entry

    block_peaks = list(snapped_peaks.values())
    block_peaks.sort(key=lambda x: x["dbm"], reverse=True)
    print(
        f"[scanner]   Block {start_hz / 1e6:.3f}-{end_hz / 1e6:.3f} MHz | "
        f"Noise floor: {noise_floor:.1f} dBm | Threshold (+{MIN_SNR_ABOVE_FLOOR_DB:.1f} dB): "
        f"{min_threshold:.1f} dBm | Peaks found: {len(block_peaks)}",
        flush=True,
    )
    for pk in block_peaks[:8]:
        print(
            f"[scanner]     Candidate: {pk['freq_hz'] / 1e6:.4f} MHz "
            f"({pk['dbm']:.1f} dBm, +{pk['snr_db']:.1f} dB SNR)",
            flush=True,
        )
    return block_peaks


def phase1_sweep_band(centers, cfg):
    print(
        "==============================================================================================",
        flush=True,
    )
    print(
        f"[scanner] [PHASE 1: SWEEP] Starting high-resolution full-band hunt across "
        f"{cfg['SCAN_START_MHZ']:.3f}-{cfg['SCAN_END_MHZ']:.3f} MHz (2 kHz step -> 25 kHz PTS grid)",
        flush=True,
    )
    print(
        "==============================================================================================",
        flush=True,
    )

    merged_peaks = {}
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

        block_peaks = parse_block_csv(csv_path, start_hz, end_hz, center_hz)
        for pk in block_peaks:
            f_hz = pk["freq_hz"]
            if f_hz not in merged_peaks or pk["dbm"] > merged_peaks[f_hz]["dbm"]:
                merged_peaks[f_hz] = pk

    all_peaks = list(merged_peaks.values())
    all_peaks.sort(key=lambda x: x["dbm"], reverse=True)
    return all_peaks


def phase2_select_best_window(all_peaks, cfg):
    whitelist = load_whitelist()
    blacklist = load_blacklist()
    now_ts = time.time()
    blacklist_modified = False

    peaks_by_hz = {int(p["freq_hz"]): dict(p) for p in (all_peaks or [])}

    start_limit_hz = int(round(cfg["SCAN_START_MHZ"] * 1_000_000))
    end_limit_hz = int(round(cfg["SCAN_END_MHZ"] * 1_000_000))

    for wl_key, wl_info in whitelist.items():
        try:
            f_hz = int(wl_key)
        except ValueError:
            continue
        if not (start_limit_hz <= f_hz <= end_limit_hz):
            continue
        if f_hz in peaks_by_hz:
            peaks_by_hz[f_hz]["whitelisted"] = True
            peaks_by_hz[f_hz]["la"] = wl_info.get("la")
        else:
            peaks_by_hz[f_hz] = {
                "freq_hz": f_hz,
                "dbm": -40.0,
                "snr_db": 10.0,
                "noise_floor": -50.0,
                "whitelisted": True,
                "la": wl_info.get("la"),
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
                    print(
                        f"[scanner] Ignoring blacklisted frequency {f_hz / 1e6:.4f} MHz "
                        f"({entry.get('reason', 'Blacklisted')})",
                        flush=True,
                    )
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
            if cand["dbm"] > prev["dbm"]:
                unique_sorted[-1] = cand
        else:
            unique_sorted.append(cand)

    max_bandwidth_hz = MAX_WINDOW_SPAN_HZ
    best_window_peaks = []
    best_score = (-1, -1e9, -1e9)

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
                key=lambda x: (x["snr_db"], x["dbm"]),
                reverse=True,
            )[:MAX_PEAKS]
            selected = sorted(in_window_strongest, key=lambda x: x["freq_hz"])
        else:
            selected = in_window

        density = len(selected)
        total_snr = round(sum(p["snr_db"] for p in selected), 2)
        total_dbm = round(sum(p["dbm"] for p in selected), 2)

        score = (density, total_snr, total_dbm)
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
        "  Chan # | Frequency (MHz) | Frequency (Hz) | Signal (dBm) | SNR (dB) | Offset from Center",
        flush=True,
    )
    print(
        "---------+-----------------+----------------+--------------+----------+-----------------------",
        flush=True,
    )
    for idx, pk in enumerate(best_window_peaks, start=1):
        f_hz = pk["freq_hz"]
        f_mhz = f_hz / 1e6
        offset_khz = (f_hz - center_hz) / 1e3
        print(
            f"  #{idx:<5} | {f_mhz:>11.4f} MHz | {f_hz:>11} Hz | {pk['dbm']:>7.1f} dBm | "
            f"+{pk['snr_db']:>4.1f} dB | {offset_khz:>+8.1f} kHz",
            flush=True,
        )
    print(
        "==============================================================================================",
        flush=True,
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


def run_receiver_pipeline(center_hz, channels_str, cfg, is_scanner_mode=True, active_frequencies=None):
    global ACTIVE_PROCS, RUNNING

    os.environ["RTL_DEVICE_INDEX"] = str(cfg["RTL_DEVICE_INDEX"])
    os.environ["RTL_GAIN"] = str(cfg["RTL_GAIN"])
    os.environ["RTL_PPM"] = str(cfg["RTL_PPM"])
    os.environ["CENTER_FREQ"] = str(center_hz)
    os.environ["TETRA_CHANNELS"] = channels_str
    child_env = os.environ.copy()

    raw_chans = [c.strip() for c in channels_str.split(",") if c.strip()]
    num_channels = len(raw_chans)

    idle_timeout_sec = cfg["IDLE_TIMEOUT_SEC"]
    blacklist_strikes = cfg["BLACKLIST_STRIKES"]
    blacklist_time_sec = cfg["BLACKLIST_TIME_SEC"]

    mode_tag = "PHASE 3: MONITOR" if is_scanner_mode else "STATIC MODE"
    print(
        f"[scanner] [{mode_tag}] Tuning CENTER_FREQ={center_hz / 1e6:.4f} MHz ({center_hz} Hz) | "
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

        last_activity = time.time()

        while RUNNING:
            try:
                line = line_queue.get(timeout=0.5)
                clean_line = line.rstrip("\r\n")
                print(clean_line, flush=True)
                if any(tag in clean_line for tag in PAYLOAD_TAGS):
                    last_activity = time.time()
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
                print(
                    "[scanner] Manual Force Sweep flag detected. Blacklisting current frequencies for 900s...",
                    flush=True,
                )
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
                print(
                    "IDLE TIMEOUT REACHED - No active payload data. Restarting scan...",
                    flush=True,
                )
                if active_frequencies:
                    add_frequencies_to_blacklist(
                        active_frequencies,
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
                run_receiver_pipeline(
                    center_hz=cfg["CENTER_FREQ"],
                    channels_str=cfg["TETRA_CHANNELS"],
                    cfg=cfg,
                    is_scanner_mode=False,
                )
                continue

            centers = build_center_frequencies(
                cfg["SCAN_START_MHZ"], cfg["SCAN_END_MHZ"]
            )
            print(
                f"[scanner] === Smart Auto-Tune Cycle #{cycle} | "
                f"{cfg['SCAN_START_MHZ']:.1f}-{cfg['SCAN_END_MHZ']:.1f} MHz | "
                f"GAIN={cfg['RTL_GAIN']} dB | IDLE_TIMEOUT={cfg['IDLE_TIMEOUT_SEC']}s | "
                f"STRIKES={cfg['BLACKLIST_STRIKES']} ===",
                flush=True,
            )

            all_peaks = phase1_sweep_band(centers, cfg)
            if not RUNNING:
                break
            if os.path.exists(RELOAD_CONFIG_FLAG):
                continue

            cfg = load_runtime_config()
            center_hz, best_peaks = phase2_select_best_window(all_peaks, cfg)
            if not best_peaks or center_hz is None:
                print(
                    "[scanner] No non-blacklisted TETRA carriers available. Retrying in 10s...",
                    flush=True,
                )
                for _ in range(20):
                    if not RUNNING or os.path.exists(RELOAD_CONFIG_FLAG):
                        break
                    time.sleep(0.5)
                cycle += 1
                continue

            active_frequencies = [int(pk["freq_hz"]) for pk in best_peaks]
            channels_list = [
                f"{pk['freq_hz']}:{idx}"
                for idx, pk in enumerate(best_peaks, start=1)
            ]
            channels_str = ",".join(channels_list)

            run_receiver_pipeline(
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
