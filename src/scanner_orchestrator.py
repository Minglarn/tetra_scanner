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
    clean = raw.split("#", 1)[0].strip()
    try:
        return int(float(clean))
    except ValueError:
        return int(default)


def parse_env_float(key, default):
    raw = os.environ.get(key, str(default))
    clean = raw.split("#", 1)[0].strip()
    try:
        return float(clean)
    except ValueError:
        return float(default)


SCAN_START_MHZ = parse_env_float("SCAN_START_MHZ", 420.0)
SCAN_END_MHZ = parse_env_float("SCAN_END_MHZ", 430.0)
IDLE_TIMEOUT_SEC = parse_env_int(
    "IDLE_TIMEOUT_SEC", parse_env_int("SCAN_DWELL_TIME", 900)
)
BLACKLIST_STRIKES = max(1, parse_env_int("BLACKLIST_STRIKES", 3))
BLACKLIST_TIME_SEC = parse_env_int("BLACKLIST_TIME_SEC", 3600)
FORCE_SWEEP_PENALTY_SEC = 900

LOG_DIR = os.environ.get("LOG_DIR", "/app/logs")
BLACKLIST_FILE = os.path.join(LOG_DIR, "blacklist.json")
WHITELIST_FILE = os.path.join(LOG_DIR, "whitelist.json")
FORCE_SWEEP_FLAG = "/tmp/force_sweep.flag"

RTL_DEVICE_INDEX = parse_env_int("RTL_DEVICE_INDEX", 1)
RTL_GAIN = parse_env_float("RTL_GAIN", 24.0)
RTL_PPM = parse_env_int("RTL_PPM", 0)

MAX_PEAKS = 6
PTS_GRID_HZ = 25000
MIN_PEAK_SEPARATION_HZ = 25000
LOCAL_PEAK_RADIUS_HZ = 16000
DC_SPIKE_GUARD_HZ = 15000
EDGE_GUARD_HZ = 15000
MAX_WINDOW_SPAN_HZ = 1900000
MIN_SNR_ABOVE_FLOOR_DB = parse_env_float("SCAN_MIN_SNR_DB", 3.0)
ABSOLUTE_MIN_DBM = parse_env_float("SCAN_MIN_DBM", -65.0)

PAYLOAD_TAGS = ("[SDS]", "[CALL]", "[STATUS]", "[ROAMING]")

ACTIVE_PROCS = []
RUNNING = True


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


def add_frequencies_to_blacklist(freq_list_hz, duration_sec, reason, immediate=False):
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
            strikes = BLACKLIST_STRIKES
        else:
            strikes = prev_strikes + 1

        if strikes >= BLACKLIST_STRIKES:
            expire_at = round(now_ts + float(duration_sec), 2)
            bl[key] = {
                "strikes": strikes,
                "max_strikes": BLACKLIST_STRIKES,
                "expire_at": expire_at,
                "last_strike_at": round(now_ts, 2),
                "reason": f"{reason} ({strikes}/{BLACKLIST_STRIKES})",
            }
            print(
                f"[scanner] Blacklisted {int(f_hz) / 1e6:.4f} MHz for {duration_sec}s "
                f"(Strike {strikes}/{BLACKLIST_STRIKES} - {reason})",
                flush=True,
            )
        else:
            bl[key] = {
                "strikes": strikes,
                "max_strikes": BLACKLIST_STRIKES,
                "expire_at": 0,
                "last_strike_at": round(now_ts, 2),
                "reason": f"Warning {strikes}/{BLACKLIST_STRIKES} ({reason})",
            }
            print(
                f"[scanner] Frequency {int(f_hz) / 1e6:.4f} MHz quiet: recorded Strike "
                f"{strikes}/{BLACKLIST_STRIKES} (not blacklisted yet)",
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


def run_rtl_power_sweep(center_mhz, start_hz, end_hz):
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
        str(RTL_DEVICE_INDEX),
        "-f",
        f"{start_hz}:{end_hz}:2k",
        "-g",
        str(RTL_GAIN),
        "-p",
        str(RTL_PPM),
        "-i",
        "10",
        "-1",
        csv_path,
    ]
    print(
        f"[scanner] [PHASE 1: SWEEP] Scanning {start_hz / 1e6:.3f}-{end_hz / 1e6:.3f} MHz "
        f"(center {center_mhz:.3f} MHz, 2k step, 10s integration) -> {csv_path}",
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


def phase1_sweep_band(centers):
    print(
        "==============================================================================================",
        flush=True,
    )
    print(
        f"[scanner] [PHASE 1: SWEEP] Starting high-resolution full-band hunt across "
        f"{SCAN_START_MHZ:.3f}-{SCAN_END_MHZ:.3f} MHz (2 kHz step -> 25 kHz PTS grid)",
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
        center_hz = int(round(center_mhz * 1_000_000))
        start_hz = int(round((center_mhz - 1.0) * 1_000_000))
        end_hz = int(round((center_mhz + 1.0) * 1_000_000))

        csv_path = run_rtl_power_sweep(center_mhz, start_hz, end_hz)
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


def phase2_select_best_window(all_peaks):
    whitelist = load_whitelist()
    blacklist = load_blacklist()
    now_ts = time.time()
    blacklist_modified = False

    peaks_by_hz = {int(p["freq_hz"]): dict(p) for p in (all_peaks or [])}

    start_limit_hz = int(round(SCAN_START_MHZ * 1_000_000))
    end_limit_hz = int(round(SCAN_END_MHZ * 1_000_000))

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
    candidate_starts = [p["freq_hz"] for p in sorted_by_freq]

    best_window_peaks = []
    best_score = (-1, -1, -1e9)

    for w_start in candidate_starts:
        w_end = w_start + MAX_WINDOW_SPAN_HZ
        in_window = [
            p for p in filtered_peaks if w_start <= p["freq_hz"] <= w_end
        ]
        in_window.sort(
            key=lambda x: (
                1 if x.get("whitelisted") else 0,
                -int(x.get("strikes", 0)),
                x["dbm"],
            ),
            reverse=True,
        )

        selected = []
        for cand in in_window:
            if any(
                abs(cand["freq_hz"] - sel["freq_hz"]) < MIN_PEAK_SEPARATION_HZ
                for sel in selected
            ):
                continue
            selected.append(cand)
            if len(selected) >= MAX_PEAKS:
                break

        if not selected:
            continue

        wl_count = sum(1 for p in selected if p.get("whitelisted"))
        unpenalized_count = sum(1 for p in selected if int(p.get("strikes", 0)) == 0)
        count = len(selected)
        total_snr = sum(
            p["snr_db"] - (int(p.get("strikes", 0)) * 4.0) for p in selected
        )
        score = (wl_count, unpenalized_count, count, round(total_snr, 2))
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
        "  [PHASE 2: TOP LIST] WINNING 2 MHz TETRA WINDOW",
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


def phase3_monitor_channels(center_hz, selected_peaks):
    global ACTIVE_PROCS, RUNNING

    active_frequencies = [int(pk["freq_hz"]) for pk in selected_peaks]
    channels_list = [
        f"{pk['freq_hz']}:{idx}"
        for idx, pk in enumerate(selected_peaks, start=1)
    ]
    channels_str = ",".join(channels_list)

    os.environ["CENTER_FREQ"] = str(center_hz)
    os.environ["TETRA_CHANNELS"] = channels_str
    child_env = os.environ.copy()

    print(
        f"[scanner] [PHASE 3: MONITOR] Tuning CENTER_FREQ={center_hz / 1e6:.4f} MHz ({center_hz} Hz)",
        flush=True,
    )
    print(
        f"[scanner] [PHASE 3: MONITOR] Active TETRA_CHANNELS={channels_str}",
        flush=True,
    )
    print(
        f"[scanner] [PHASE 3: MONITOR] Idle payload timeout set to {IDLE_TIMEOUT_SEC}s "
        f"(blacklist penalty={BLACKLIST_TIME_SEC}s)",
        flush=True,
    )

    try:
        for idx, _ in enumerate(selected_peaks, start=1):
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

            if os.path.exists(FORCE_SWEEP_FLAG):
                try:
                    os.remove(FORCE_SWEEP_FLAG)
                except OSError:
                    pass
                print(
                    "[scanner] Manual Force Sweep flag detected. Blacklisting current frequencies for 900s...",
                    flush=True,
                )
                add_frequencies_to_blacklist(
                    active_frequencies,
                    FORCE_SWEEP_PENALTY_SEC,
                    "Manual Force",
                    immediate=True,
                )
                break

            if gr_proc.poll() is not None:
                print(
                    "[scanner] Warning: tetra_multi_rtl.py exited unexpectedly. Restarting scan...",
                    flush=True,
                )
                break

            if parser_proc.poll() is not None and line_queue.empty():
                print(
                    "[scanner] Warning: tetra_parser.py exited unexpectedly. Restarting scan...",
                    flush=True,
                )
                break

            if (time.time() - last_activity) > IDLE_TIMEOUT_SEC:
                print(
                    "IDLE TIMEOUT REACHED - No active payload data. Restarting scan...",
                    flush=True,
                )
                add_frequencies_to_blacklist(
                    active_frequencies, BLACKLIST_TIME_SEC, "Idle Timeout"
                )
                break
    finally:
        terminate_children()
        time.sleep(1.5)


def main():
    centers = build_center_frequencies(SCAN_START_MHZ, SCAN_END_MHZ)
    print(
        f"[scanner] Smart Auto-Tune Scanner initialized: "
        f"{SCAN_START_MHZ:.1f}-{SCAN_END_MHZ:.1f} MHz | "
        f"2 MHz centers={[round(c, 3) for c in centers]} | "
        f"IDLE_TIMEOUT_SEC={IDLE_TIMEOUT_SEC}s | BLACKLIST_TIME_SEC={BLACKLIST_TIME_SEC}s",
        flush=True,
    )

    cycle = 1
    try:
        while RUNNING:
            print(f"[scanner] === Smart Auto-Tune Cycle #{cycle} ===", flush=True)
            all_peaks = phase1_sweep_band(centers)
            if not RUNNING:
                break

            center_hz, best_peaks = phase2_select_best_window(all_peaks)
            if not best_peaks or center_hz is None:
                print(
                    "[scanner] No non-blacklisted TETRA carriers available. Retrying in 10s...",
                    flush=True,
                )
                for _ in range(20):
                    if not RUNNING:
                        break
                    time.sleep(0.5)
                cycle += 1
                continue

            phase3_monitor_channels(center_hz, best_peaks)
            cycle += 1
    finally:
        terminate_children()
        print("[scanner] Smart Auto-Tune Scanner stopped cleanly.", flush=True)


if __name__ == "__main__":
    main()
