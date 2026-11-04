#!/usr/bin/env python3
import os
import sys
import csv
import time
import signal
import atexit
import subprocess
import statistics


SCAN_START_MHZ = float(os.environ.get("SCAN_START_MHZ", "410"))
SCAN_END_MHZ = float(os.environ.get("SCAN_END_MHZ", "430"))
SCAN_DWELL_TIME = int(os.environ.get("SCAN_DWELL_TIME", "120"))

RTL_DEVICE_INDEX = int(os.environ.get("RTL_DEVICE_INDEX", "1"))
RTL_GAIN = float(os.environ.get("RTL_GAIN", "24.0"))
RTL_PPM = int(os.environ.get("RTL_PPM", "0"))

SCAN_CSV_PATH = "/tmp/scan.csv"
MAX_PEAKS = 6
MIN_PEAK_SEPARATION_HZ = 25000
DC_SPIKE_GUARD_HZ = 25000
EDGE_GUARD_HZ = 25000
MIN_SNR_ABOVE_FLOOR_DB = float(os.environ.get("SCAN_MIN_SNR_DB", "6.0"))
ABSOLUTE_MIN_DBM = float(os.environ.get("SCAN_MIN_DBM", "-42.0"))

ACTIVE_PROCS = []
RUNNING = True


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


def run_rtl_power_sweep(start_hz, end_hz):
    try:
        if os.path.exists(SCAN_CSV_PATH):
            os.remove(SCAN_CSV_PATH)
    except OSError:
        pass

    cmd = [
        "rtl_power",
        "-d",
        str(RTL_DEVICE_INDEX),
        "-g",
        str(RTL_GAIN),
        "-p",
        str(RTL_PPM),
        "-f",
        f"{start_hz}:{end_hz}:12.5k",
        "-i",
        "5",
        "-1",
        SCAN_CSV_PATH,
    ]
    print(
        f"[scanner] Running spectrum sweep: {' '.join(cmd)}",
        flush=True,
    )
    try:
        res = subprocess.run(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            timeout=25,
            check=False,
        )
        if res.returncode != 0:
            print(
                f"[scanner] rtl_power exited with code {res.returncode}: {res.stderr.strip()}",
                flush=True,
            )
    except Exception as exc:
        print(f"[scanner] Failed running rtl_power: {exc}", flush=True)
        return False

    return os.path.exists(SCAN_CSV_PATH) and os.path.getsize(SCAN_CSV_PATH) > 0


def parse_scan_csv(csv_path, start_hz, end_hz, center_hz):
    if not os.path.exists(csv_path):
        return []

    bin_Accum = {}

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
                    snapped_freq = int(round(raw_freq / 12500.0) * 12500)
                    bin_Accum.setdefault(snapped_freq, []).append(dbm)
    except Exception as exc:
        print(f"[scanner] Error parsing {csv_path}: {exc}", flush=True)
        return []

    if not bin_Accum:
        return []

    avg_bins = {
        freq: sum(vals) / float(len(vals)) for freq, vals in bin_Accum.items()
    }
    all_dbm = list(avg_bins.values())
    noise_floor = statistics.median(all_dbm)
    min_threshold = max(noise_floor + MIN_SNR_ABOVE_FLOOR_DB, ABSOLUTE_MIN_DBM)

    candidates = []
    for freq_hz, dbm in avg_bins.items():
        if abs(freq_hz - center_hz) <= DC_SPIKE_GUARD_HZ:
            continue
        if freq_hz < (start_hz + EDGE_GUARD_HZ) or freq_hz > (end_hz - EDGE_GUARD_HZ):
            continue
        if dbm < min_threshold:
            continue
        candidates.append((freq_hz, dbm))

    candidates.sort(key=lambda item: item[1], reverse=True)

    selected = []
    for freq_hz, dbm in candidates:
        if any(abs(freq_hz - sel_freq) < MIN_PEAK_SEPARATION_HZ for sel_freq, _ in selected):
            continue
        selected.append((freq_hz, round(dbm, 2)))
        if len(selected) >= MAX_PEAKS:
            break

    selected.sort(key=lambda item: item[0])
    print(
        f"[scanner] Sweep {start_hz / 1e6:.3f}-{end_hz / 1e6:.3f} MHz | "
        f"Noise floor: {noise_floor:.1f} dBm | Threshold: {min_threshold:.1f} dBm | "
        f"Peaks found: {len(selected)}",
        flush=True,
    )
    for freq_hz, dbm in selected:
        print(f"[scanner]   Peak: {freq_hz / 1e6:.4f} MHz ({dbm:.1f} dBm)", flush=True)

    return selected


def dwell_on_block(center_hz, peaks):
    global ACTIVE_PROCS, RUNNING

    channels_list = [f"{freq_hz}:{idx}" for idx, (freq_hz, _) in enumerate(peaks, start=1)]
    channels_str = ",".join(channels_list)

    os.environ["CENTER_FREQ"] = str(center_hz)
    os.environ["TETRA_CHANNELS"] = channels_str
    child_env = os.environ.copy()

    print(
        f"[scanner] Dwelling {SCAN_DWELL_TIME}s on CENTER_FREQ={center_hz} Hz "
        f"with TETRA_CHANNELS={channels_str}",
        flush=True,
    )

    try:
        for idx, (freq_hz, _) in enumerate(peaks, start=1):
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
            env=child_env,
        )
        ACTIVE_PROCS.append(parser_proc)

        end_time = time.time() + SCAN_DWELL_TIME
        while RUNNING and time.time() < end_time:
            if gr_proc.poll() is not None:
                print("[scanner] Warning: tetra_multi_rtl.py exited early.", flush=True)
                break
            time.sleep(0.5)
    finally:
        terminate_children()
        time.sleep(1.5)


def build_center_frequencies(start_mhz, end_mhz):
    centers = []
    cur = start_mhz + 1.0
    while cur < end_mhz:
        centers.append(cur)
        cur += 2.0
    if not centers:
        centers.append(start_mhz + 1.0)
    return centers


def main():
    centers = build_center_frequencies(SCAN_START_MHZ, SCAN_END_MHZ)
    print(
        f"[scanner] Starting Auto-Discovery Scanner across {SCAN_START_MHZ}-{SCAN_END_MHZ} MHz "
        f"(2 MHz blocks: {[round(c, 3) for c in centers]}, dwell={SCAN_DWELL_TIME}s)",
        flush=True,
    )

    sweep_round = 1
    try:
        while RUNNING:
            print(f"[scanner] === Starting sweep cycle #{sweep_round} ===", flush=True)
            for center_mhz in centers:
                if not RUNNING:
                    break
                center_hz = int(round(center_mhz * 1_000_000))
                start_hz = int(round((center_mhz - 1.0) * 1_000_000))
                end_hz = int(round((center_mhz + 1.0) * 1_000_000))

                ok = run_rtl_power_sweep(start_hz, end_hz)
                if not ok or not RUNNING:
                    time.sleep(1.0)
                    continue

                peaks = parse_scan_csv(SCAN_CSV_PATH, start_hz, end_hz, center_hz)
                if len(peaks) < 1:
                    print(
                        f"[scanner] No active TETRA carriers above threshold in "
                        f"{start_hz / 1e6:.3f}-{end_hz / 1e6:.3f} MHz, moving to next block.",
                        flush=True,
                    )
                    continue

                dwell_on_block(center_hz, peaks)

            sweep_round += 1
    finally:
        terminate_children()
        print("[scanner] Orchestrator stopped cleanly.", flush=True)


if __name__ == "__main__":
    main()
