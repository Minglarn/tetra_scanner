#!/usr/bin/env python3
import os
import sys
import json
import time
import asyncio
from collections import deque
from datetime import datetime, timezone
import aiofiles
from fastapi import FastAPI, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

_SRC_DIR = os.path.dirname(os.path.abspath(__file__))
if _SRC_DIR not in sys.path:
    sys.path.insert(0, _SRC_DIR)
import db


APP_VERSION = os.environ.get("APP_VERSION", "2026.11.06.05")
LOG_DIR = os.environ.get("LOG_DIR", "/app/logs")
AUDIO_DIR = os.environ.get("AUDIO_DIR", os.path.join(LOG_DIR, "audio"))
os.makedirs(AUDIO_DIR, exist_ok=True)
EVENTS_FILE = os.path.join(LOG_DIR, "tetra_events.jsonl")
NETWORKS_FILE = os.path.join(LOG_DIR, "discovered_networks.jsonl")
BLACKLIST_FILE = os.path.join(LOG_DIR, "blacklist.json")
WHITELIST_FILE = os.path.join(LOG_DIR, "whitelist.json")
RUNTIME_CONFIG_FILE = os.path.join(LOG_DIR, "runtime_config.json")

RELOAD_CONFIG_FLAG = "/tmp/reload_config.flag"
SOFT_CONFIG_FLAG = "/tmp/soft_config.flag"
FORCE_SWEEP_FLAG = "/tmp/force_sweep.flag"

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
TEMPLATES_DIR = os.path.join(BASE_DIR, "templates")

app = FastAPI(title="TETRA Scanner WebUI", version=APP_VERSION)
app.mount("/api/audio", StaticFiles(directory=AUDIO_DIR), name="audio")
templates = Jinja2Templates(directory=TEMPLATES_DIR)
db.init_db()


def _parse_device_spec(val, default=0):
    raw = str(val if val is not None else default).split("#", 1)[0].strip()
    if not raw:
        return default
    if raw.isdigit() and len(raw) < 4:
        return int(raw)
    return raw


def get_default_config():
    def _clean(key, default):
        raw = os.environ.get(key, str(default))
        return str(raw).split("#", 1)[0].strip() or str(default)

    return {
        "RTL_DEVICE_INDEX": _parse_device_spec(_clean("RTL_DEVICE_INDEX", 0), default=0),
        "RTL_SERIAL": _clean("RTL_SERIAL", ""),
        "RTL_GAIN": float(_clean("RTL_GAIN", 24.0)),
        "RTL_PPM": int(float(_clean("RTL_PPM", 0))),
        "MODE": _clean("MODE", "scanner").lower(),
        "SCAN_START_MHZ": float(_clean("SCAN_START_MHZ", 420.0)),
        "SCAN_END_MHZ": float(_clean("SCAN_END_MHZ", 430.0)),
        "SCAN_DWELL_TIME": int(float(_clean("SCAN_DWELL_TIME", 120))),
        "IDLE_TIMEOUT_SEC": int(float(_clean("IDLE_TIMEOUT_SEC", 900))),
        "BLACKLIST_STRIKES": int(float(_clean("BLACKLIST_STRIKES", 3))),
        "BLACKLIST_TIME_SEC": int(float(_clean("BLACKLIST_TIME_SEC", 3600))),
        "CENTER_FREQ": int(float(_clean("CENTER_FREQ", 427750000))),
        "TETRA_CHANNELS": _clean(
            "TETRA_CHANNELS",
            "427000000:71,427300000:17,427550000:23,427850000:1,427950000:16,428500000:7",
        ),
    }


def _read_json_file_sync(path: str, default):
    if not os.path.exists(path):
        return default
    try:
        with open(path, "r", encoding="utf-8", errors="replace") as f:
            content = f.read()
            if not content.strip():
                return default
            data = json.loads(content)
            return data if isinstance(data, type(default)) else default
    except Exception:
        return default


def _write_json_file_sync(path: str, data):
    os.makedirs(os.path.dirname(path) or LOG_DIR, exist_ok=True)
    tmp_path = f"{path}.tmp.{os.getpid()}"
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, sort_keys=True)
    os.replace(tmp_path, path)


def _touch_flag_sync(path: str):
    try:
        with open(path, "w", encoding="utf-8") as f:
            f.write(str(time.time()))
    except Exception:
        pass


def _append_event_sync(sys_event: dict):
    try:
        os.makedirs(LOG_DIR, exist_ok=True)
        with open(EVENTS_FILE, "a", encoding="utf-8") as ef:
            ef.write(json.dumps(sys_event, ensure_ascii=False) + "\n")
            ef.flush()
    except Exception:
        pass
    try:
        db.record_event(sys_event)
    except Exception:
        pass


async def read_runtime_config():
    cfg = get_default_config()
    saved = _read_json_file_sync(RUNTIME_CONFIG_FILE, {})
    if isinstance(saved, dict) and saved:
        cfg.update(saved)
    return cfg


@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    cfg = await read_runtime_config()
    return templates.TemplateResponse(
        "index.html",
        {
            "request": request,
            "version": APP_VERSION,
            "mode": cfg.get("MODE", "scanner"),
        },
    )


@app.get("/api/config", response_class=JSONResponse)
async def get_config():
    cfg = await read_runtime_config()
    return cfg


def compute_optimal_center_hz(freq_hz_list, min_dc_guard_hz=25000):
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


def normalize_channels_and_center(channels_raw: str, center_raw=None):
    entries = [c.strip() for c in str(channels_raw or "").split(",") if c.strip()]
    if not entries:
        raise ValueError("TETRA_CHANNELS cannot be empty")

    normalized_parts = []
    freq_hz_list = []
    for idx, entry in enumerate(entries, start=1):
        parts = entry.split(":", 1)
        f_val = float(parts[0].strip())
        if f_val < 100000.0:
            f_hz = int(round(f_val * 1_000_000))
        else:
            f_hz = int(round(f_val))
        la_str = parts[1].strip() if len(parts) > 1 and parts[1].strip() else str(idx)
        freq_hz_list.append(f_hz)
        normalized_parts.append(f"{f_hz}:{la_str}")

    if center_raw is not None and str(center_raw).strip() != "":
        c_val = float(center_raw)
        if 0 < c_val < 100000.0:
            center_hz = int(round(c_val * 1_000_000))
        elif c_val >= 100000.0:
            center_hz = int(round(c_val))
        else:
            center_hz = compute_optimal_center_hz(freq_hz_list)
        if any(abs(center_hz - f) < 25000 for f in freq_hz_list):
            center_hz = compute_optimal_center_hz(freq_hz_list)
    else:
        center_hz = compute_optimal_center_hz(freq_hz_list)

    return ",".join(normalized_parts), center_hz


@app.post("/api/config", response_class=JSONResponse)
async def update_config(request: Request):
    try:
        body = await request.json()
    except Exception:
        return JSONResponse({"status": "error", "message": "Invalid JSON"}, status_code=400)

    old_cfg = await read_runtime_config()
    new_cfg = dict(old_cfg)

    try:
        if "RTL_DEVICE_INDEX" in body:
            new_cfg["RTL_DEVICE_INDEX"] = _parse_device_spec(
                body["RTL_DEVICE_INDEX"], default=0
            )
            if "RTL_SERIAL" not in body and isinstance(new_cfg["RTL_DEVICE_INDEX"], int):
                new_cfg["RTL_SERIAL"] = ""
        if "RTL_SERIAL" in body:
            new_cfg["RTL_SERIAL"] = str(body["RTL_SERIAL"]).strip()
        if "RTL_GAIN" in body:
            new_cfg["RTL_GAIN"] = float(body["RTL_GAIN"])
        if "RTL_IF_GAIN" in body:
            new_cfg["RTL_IF_GAIN"] = float(body["RTL_IF_GAIN"])
        if "RTL_BB_GAIN" in body:
            new_cfg["RTL_BB_GAIN"] = float(body["RTL_BB_GAIN"])
        if "RTL_PPM" in body:
            new_cfg["RTL_PPM"] = int(body["RTL_PPM"])
        if "TETRA_DECIM" in body:
            new_cfg["TETRA_DECIM"] = max(1, int(body["TETRA_DECIM"]))
        if "TETRA_FILTER_CUTOFF" in body:
            new_cfg["TETRA_FILTER_CUTOFF"] = float(body["TETRA_FILTER_CUTOFF"])
        if "TETRA_FILTER_TRANS" in body:
            new_cfg["TETRA_FILTER_TRANS"] = float(body["TETRA_FILTER_TRANS"])
        if "MODE" in body:
            m = str(body["MODE"]).strip().lower()
            if m in ("scanner", "static", "stopped"):
                new_cfg["MODE"] = m
        if "SCAN_START_MHZ" in body:
            new_cfg["SCAN_START_MHZ"] = float(body["SCAN_START_MHZ"])
        if "SCAN_END_MHZ" in body:
            new_cfg["SCAN_END_MHZ"] = float(body["SCAN_END_MHZ"])
        if new_cfg["SCAN_END_MHZ"] <= new_cfg["SCAN_START_MHZ"]:
            new_cfg["SCAN_END_MHZ"] = new_cfg["SCAN_START_MHZ"] + 2.0
        if "SCAN_DWELL_TIME" in body:
            new_cfg["SCAN_DWELL_TIME"] = max(10, int(body["SCAN_DWELL_TIME"]))
        if "IDLE_TIMEOUT_SEC" in body:
            new_cfg["IDLE_TIMEOUT_SEC"] = max(15, int(body["IDLE_TIMEOUT_SEC"]))
        if "BLACKLIST_STRIKES" in body:
            new_cfg["BLACKLIST_STRIKES"] = max(1, int(body["BLACKLIST_STRIKES"]))
        if "BLACKLIST_TIME_SEC" in body:
            new_cfg["BLACKLIST_TIME_SEC"] = max(10, int(body["BLACKLIST_TIME_SEC"]))
        if "TETRA_CHANNELS" in body and str(body["TETRA_CHANNELS"]).strip():
            norm_ch, norm_center = normalize_channels_and_center(
                body["TETRA_CHANNELS"],
                body.get("CENTER_FREQ", new_cfg.get("CENTER_FREQ")),
            )
            new_cfg["TETRA_CHANNELS"] = norm_ch
            new_cfg["CENTER_FREQ"] = norm_center
        elif "CENTER_FREQ" in body and str(body["CENTER_FREQ"]).strip():
            c_val = float(body["CENTER_FREQ"])
            new_cfg["CENTER_FREQ"] = (
                int(round(c_val * 1_000_000)) if c_val < 100000.0 else int(round(c_val))
            )
    except (ValueError, TypeError) as exc:
        return JSONResponse(
            {"status": "error", "message": f"Invalid parameter value: {exc}"},
            status_code=400,
        )

    _write_json_file_sync(RUNTIME_CONFIG_FILE, new_cfg)

    hard_keys = (
        "RTL_DEVICE_INDEX",
        "RTL_SERIAL",
        "RTL_GAIN",
        "RTL_IF_GAIN",
        "RTL_BB_GAIN",
        "RTL_PPM",
        "TETRA_DECIM",
        "TETRA_FILTER_CUTOFF",
        "TETRA_FILTER_TRANS",
        "MODE",
        "SCAN_START_MHZ",
        "SCAN_END_MHZ",
        "CENTER_FREQ",
        "TETRA_CHANNELS",
    )
    needs_pipeline_restart = bool(body.get("force_restart")) or any(
        old_cfg.get(k) != new_cfg.get(k) for k in hard_keys
    )

    flag_to_touch = RELOAD_CONFIG_FLAG if needs_pipeline_restart else SOFT_CONFIG_FLAG
    _touch_flag_sync(flag_to_touch)

    iso_ts = datetime.now(timezone.utc).isoformat()
    action_str = (
        "restarting radio/sweep pipeline"
        if needs_pipeline_restart
        else "applied live in memory"
    )
    sys_event = {
        "time": iso_ts,
        "timestamp": iso_ts,
        "category": "SYSTEM",
        "freq_mhz": "SYS",
        "human_text": (
            f"Settings updated ({action_str}): Mode={new_cfg['MODE']}, "
            f"Center={new_cfg.get('CENTER_FREQ', 0) / 1e6:.4f} MHz, "
            f"Channels={new_cfg.get('TETRA_CHANNELS', '')}, "
            f"Band={new_cfg['SCAN_START_MHZ']}-{new_cfg['SCAN_END_MHZ']} MHz, "
            f"Gain={new_cfg['RTL_GAIN']} dB, IdleTimeout={new_cfg['IDLE_TIMEOUT_SEC']}s, "
            f"Strikes={new_cfg['BLACKLIST_STRIKES']}."
        ),
    }
    _append_event_sync(sys_event)

    return {
        "ok": True,
        "status": "ok",
        "hard_reload": needs_pipeline_restart,
        "restarted_pipeline": needs_pipeline_restart,
        "config": new_cfg,
    }


@app.post("/api/stop-sdr", response_class=JSONResponse)
async def stop_and_release_sdr():
    old_cfg = await read_runtime_config()
    new_cfg = dict(old_cfg)
    new_cfg["MODE"] = "stopped"

    _write_json_file_sync(RUNTIME_CONFIG_FILE, new_cfg)
    _touch_flag_sync(RELOAD_CONFIG_FLAG)

    iso_ts = datetime.now(timezone.utc).isoformat()
    sys_event = {
        "time": iso_ts,
        "timestamp": iso_ts,
        "category": "SYSTEM",
        "freq_mhz": "SYS",
        "human_text": (
            f"Stop requested: Stopping radio pipeline and releasing RTL-SDR device "
            f"#{new_cfg.get('RTL_DEVICE_INDEX', 0)}."
        ),
    }
    _append_event_sync(sys_event)

    return {
        "ok": True,
        "status": "ok",
        "hard_reload": True,
        "restarted_pipeline": True,
        "config": new_cfg,
    }


@app.post("/api/force-sweep", response_class=JSONResponse)
async def trigger_force_sweep():
    cfg = await read_runtime_config()
    if cfg.get("MODE") == "stopped":
        cfg["MODE"] = "scanner"
        _write_json_file_sync(RUNTIME_CONFIG_FILE, cfg)
        _touch_flag_sync(RELOAD_CONFIG_FLAG)
    _touch_flag_sync(FORCE_SWEEP_FLAG)
    return {"status": "ok", "message": "Force sweep triggered", "config": cfg}


_NETWORKS_CACHE = {"mtime_ns": 0, "size": 0, "data": []}


def _load_and_compact_networks_sync():
    if not os.path.exists(NETWORKS_FILE):
        _NETWORKS_CACHE["mtime_ns"] = 0
        _NETWORKS_CACHE["size"] = 0
        _NETWORKS_CACHE["data"] = []
        return []

    try:
        st = os.stat(NETWORKS_FILE)
        if (
            st.st_mtime_ns == _NETWORKS_CACHE["mtime_ns"]
            and st.st_size == _NETWORKS_CACHE["size"]
        ):
            return list(_NETWORKS_CACHE["data"])
    except OSError:
        return list(_NETWORKS_CACHE["data"])

    networks_by_freq = {}
    line_count = 0
    try:
        with open(NETWORKS_FILE, mode="r", encoding="utf-8", errors="replace") as f:
            for line in f:
                raw = line.strip()
                if not raw:
                    continue
                line_count += 1
                try:
                    item = json.loads(raw)
                except json.JSONDecodeError:
                    continue
                if not isinstance(item, dict):
                    continue
                key = item.get("freq_mhz") or item.get("network_id")
                if not key:
                    continue
                existing = networks_by_freq.get(key)
                if existing is None:
                    networks_by_freq[key] = dict(item)
                else:
                    prev_role = existing.get("role")
                    for k, v in item.items():
                        if v is not None and v != [] and v != {}:
                            existing[k] = v
                    if prev_role == "ACTIVE CELL":
                        existing["role"] = "ACTIVE CELL"
    except Exception:
        return list(_NETWORKS_CACHE["data"])

    result = [
        net
        for net in networks_by_freq.values()
        if not (
            str(net.get("pdu") or "").upper() in ("TCH_S", "UPLANE")
            and net.get("mcc") is None
            and net.get("mnc") is None
            and net.get("cell_id") is None
            and not net.get("neighbours")
        )
    ]
    result.sort(
        key=lambda x: (
            1 if x.get("role") == "ACTIVE CELL" else 0,
            x.get("seen_epoch") or 0,
        ),
        reverse=True,
    )

    if line_count > max(100, len(result) * 4):
        try:
            tmp_path = f"{NETWORKS_FILE}.compact.{os.getpid()}"
            with open(tmp_path, "w", encoding="utf-8") as wf:
                for net in reversed(result):
                    wf.write(json.dumps(net, ensure_ascii=False) + "\n")
            os.replace(tmp_path, NETWORKS_FILE)
            st = os.stat(NETWORKS_FILE)
        except Exception:
            pass

    _NETWORKS_CACHE["mtime_ns"] = st.st_mtime_ns
    _NETWORKS_CACHE["size"] = st.st_size
    _NETWORKS_CACHE["data"] = result
    return list(result)


@app.get("/api/networks", response_class=JSONResponse)
async def get_networks():
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, _load_and_compact_networks_sync)


@app.delete("/api/networks", response_class=JSONResponse)
async def clear_networks():
    try:
        os.makedirs(LOG_DIR, exist_ok=True)
        tmp_path = f"{NETWORKS_FILE}.tmp.{os.getpid()}"
        with open(tmp_path, mode="w", encoding="utf-8") as f:
            f.write("")
        os.replace(tmp_path, NETWORKS_FILE)
        _NETWORKS_CACHE["mtime_ns"] = 0
        _NETWORKS_CACHE["size"] = 0
        _NETWORKS_CACHE["data"] = []
    except Exception:
        pass
    return {"status": "ok", "cleared": True}


@app.get("/api/blacklist", response_class=JSONResponse)
async def get_blacklist():
    data = _read_json_file_sync(BLACKLIST_FILE, {})
    if not data:
        return {}

    now_ts = time.time()
    active = {}
    for freq_str, info in data.items():
        if not isinstance(info, dict):
            continue
        expire_at = float(info.get("expire_at", 0))
        strikes = int(info.get("strikes", 0))
        max_strikes = int(info.get("max_strikes", 3))
        if expire_at > now_ts:
            active[freq_str] = {
                "expire_at": expire_at,
                "strikes": strikes,
                "max_strikes": max_strikes,
                "status": "blacklisted",
                "reason": info.get("reason", "Idle Timeout"),
            }
        elif expire_at == 0 and strikes > 0:
            active[freq_str] = {
                "expire_at": 0,
                "strikes": strikes,
                "max_strikes": max_strikes,
                "status": "warning",
                "reason": info.get("reason", f"Strike {strikes}/{max_strikes}"),
            }
    return active


@app.delete("/api/blacklist/{freq_hz}", response_class=JSONResponse)
async def remove_from_blacklist(freq_hz: str):
    data = _read_json_file_sync(BLACKLIST_FILE, {})
    if freq_hz in data:
        del data[freq_hz]
        try:
            _write_json_file_sync(BLACKLIST_FILE, data)
        except Exception:
            pass
    return {"status": "ok", "removed": freq_hz}


@app.delete("/api/blacklist", response_class=JSONResponse)
async def clear_blacklist():
    try:
        _write_json_file_sync(BLACKLIST_FILE, {})
    except Exception:
        pass
    return {"status": "ok", "cleared": True}


@app.get("/api/whitelist", response_class=JSONResponse)
async def get_whitelist():
    data = _read_json_file_sync(WHITELIST_FILE, {})
    if not data:
        return []

    valid_wl_categories = {
        "SDS",
        "CALL",
        "VOICE",
        "STATUS",
        "ROAMING",
        "NETWORK",
        "NEIGHBOUR_BROADCAST",
    }
    items = [
        v
        for v in data.values()
        if isinstance(v, dict)
        and str(v.get("last_category", "")).upper() in valid_wl_categories
    ]
    items.sort(key=lambda x: x.get("freq_hz") or 0)
    return items


@app.delete("/api/whitelist/{freq_hz}", response_class=JSONResponse)
async def remove_from_whitelist(freq_hz: str):
    data = _read_json_file_sync(WHITELIST_FILE, {})
    if freq_hz in data:
        del data[freq_hz]
        try:
            _write_json_file_sync(WHITELIST_FILE, data)
        except Exception:
            pass
    return {"status": "ok", "removed": freq_hz}


@app.delete("/api/whitelist", response_class=JSONResponse)
async def clear_whitelist():
    try:
        _write_json_file_sync(WHITELIST_FILE, {})
    except Exception:
        pass
    return {"status": "ok", "cleared": True}


@app.post("/api/reset-all", response_class=JSONResponse)
async def reset_all_discovery():
    try:
        os.makedirs(LOG_DIR, exist_ok=True)
        for jf in (WHITELIST_FILE, BLACKLIST_FILE):
            _write_json_file_sync(jf, {})

        tmp_net = f"{NETWORKS_FILE}.tmp.{os.getpid()}"
        with open(tmp_net, mode="w", encoding="utf-8") as f:
            f.write("")
        os.replace(tmp_net, NETWORKS_FILE)
        _NETWORKS_CACHE["mtime_ns"] = 0
        _NETWORKS_CACHE["size"] = 0
        _NETWORKS_CACHE["data"] = []

        cfg = await read_runtime_config()
        cfg["MODE"] = "scanner"
        _write_json_file_sync(RUNTIME_CONFIG_FILE, cfg)

        iso_ts = datetime.now(timezone.utc).isoformat()
        sys_event = {
            "time": iso_ts,
            "timestamp": iso_ts,
            "category": "SYSTEM",
            "freq_mhz": "SYS",
            "human_text": "Factory discovery reset executed: Cleared whitelist, blacklist, and discovered networks. Restarting in Auto-Scanner mode.",
        }
        tmp_ev = f"{EVENTS_FILE}.tmp.{os.getpid()}"
        with open(tmp_ev, mode="w", encoding="utf-8") as ef:
            ef.write(json.dumps(sys_event, ensure_ascii=False) + "\n")
        os.replace(tmp_ev, EVENTS_FILE)
        try:
            db.record_event(sys_event)
        except Exception:
            pass

        if os.path.exists(FORCE_SWEEP_FLAG):
            try:
                os.remove(FORCE_SWEEP_FLAG)
            except Exception:
                pass
        _touch_flag_sync(RELOAD_CONFIG_FLAG)
    except Exception as exc:
        return JSONResponse(
            {"status": "error", "message": f"Reset failed: {exc}"}, status_code=500
        )

    return {"status": "ok", "reset": True, "config": cfg}


@app.post("/api/whitelist/lock", response_class=JSONResponse)
async def lock_to_whitelist(request: Request):
    selected_hz = None
    try:
        body = await request.json()
        if isinstance(body, dict) and isinstance(body.get("frequencies"), list):
            selected_hz = {int(f) for f in body["frequencies"] if str(f).strip()}
    except Exception:
        pass

    wl_items = await get_whitelist()
    if selected_hz:
        wl_items = [item for item in wl_items if int(item.get("freq_hz", 0)) in selected_hz]

    if not wl_items:
        return JSONResponse(
            {"status": "error", "message": "No verified whitelisted frequencies available to lock."},
            status_code=400,
        )

    wl_items.sort(key=lambda x: int(x.get("freq_hz") or 0))
    best_cluster = []
    best_score = (-1, -1)
    for start_item in wl_items:
        f_start = int(start_item.get("freq_hz") or 0)
        f_limit = f_start + 1_800_000
        in_win = [
            it
            for it in wl_items
            if f_start <= int(it.get("freq_hz") or 0) <= f_limit
        ]
        if len(in_win) > 8:
            in_win = sorted(
                in_win,
                key=lambda x: int(x.get("hit_count") or 0),
                reverse=True,
            )[:8]
            in_win.sort(key=lambda x: int(x.get("freq_hz") or 0))
        hits_sum = sum(int(it.get("hit_count") or 1) for it in in_win)
        sc = (len(in_win), hits_sum)
        if sc > best_score:
            best_score = sc
            best_cluster = in_win

    freq_hz_list = [int(it["freq_hz"]) for it in best_cluster]
    center_hz = compute_optimal_center_hz(freq_hz_list)
    ch_parts = []
    for idx, it in enumerate(best_cluster, start=1):
        la_val = it.get("la") if it.get("la") is not None else idx
        ch_parts.append(f"{int(it['freq_hz'])}:{la_val}")
    channels_str = ",".join(ch_parts)

    cfg = await read_runtime_config()
    cfg["MODE"] = "static"
    cfg["CENTER_FREQ"] = center_hz
    cfg["TETRA_CHANNELS"] = channels_str

    _write_json_file_sync(RUNTIME_CONFIG_FILE, cfg)
    _touch_flag_sync(RELOAD_CONFIG_FLAG)

    iso_ts = datetime.now(timezone.utc).isoformat()
    freqs_mhz = ", ".join(f"{hz / 1e6:.4f} MHz" for hz in freq_hz_list)
    sys_event = {
        "time": iso_ts,
        "timestamp": iso_ts,
        "category": "SYSTEM",
        "freq_mhz": "SYS",
        "human_text": (
            f"Locked receiver to {len(freq_hz_list)} whitelisted channels in STATIC mode "
            f"(Center {center_hz / 1e6:.4f} MHz): {freqs_mhz}"
        ),
    }
    _append_event_sync(sys_event)

    return {
        "ok": True,
        "status": "ok",
        "config": cfg,
        "locked_count": len(freq_hz_list),
        "center_hz": center_hz,
        "tetra_channels": channels_str,
    }


@app.get("/api/history", response_class=JSONResponse)
def get_history(
    category: str = "ALL",
    freq_mhz: str = "",
    ssi: str = "",
    q: str = "",
    hours: float = 24.0,
    limit: int = 150,
    offset: int = 0,
    include_system: bool = False,
):
    return db.query_history(
        category=category,
        freq_mhz=freq_mhz,
        ssi=ssi,
        q=q,
        hours=hours,
        limit=limit,
        offset=offset,
        include_system=include_system,
    )


@app.get("/api/analytics", response_class=JSONResponse)
def get_analytics(hours: float = 24.0):
    return db.get_analytics_summary(hours=hours)


_SDR_USB_STRINGS_CACHE = {}


@app.get("/api/sdr-devices", response_class=JSONResponse)
def get_sdr_devices(refresh: int = 0):
    import ctypes

    devices = []
    rtlsdr = None
    for lib_name in ("librtlsdr.so.0", "librtlsdr.so"):
        try:
            rtlsdr = ctypes.CDLL(lib_name)
            break
        except Exception:
            continue

    if rtlsdr is None:
        return {"count": 0, "devices": [], "error": "librtlsdr not available"}

    try:
        rtlsdr.rtlsdr_get_device_name.restype = ctypes.c_char_p
        dev_count = int(rtlsdr.rtlsdr_get_device_count())
        if refresh or len(_SDR_USB_STRINGS_CACHE) != dev_count:
            _SDR_USB_STRINGS_CACHE.clear()
        for idx in range(dev_count):
            name_ptr = rtlsdr.rtlsdr_get_device_name(idx)
            name = (
                name_ptr.decode("utf-8", errors="ignore")
                if name_ptr
                else f"RTL-SDR #{idx}"
            )
            cached = _SDR_USB_STRINGS_CACHE.get(idx)
            if cached and cached.get("name") == name and cached.get("serial"):
                devices.append(dict(cached))
                continue
            mfg = ctypes.create_string_buffer(256)
            prod = ctypes.create_string_buffer(256)
            ser = ctypes.create_string_buffer(256)
            rc = int(rtlsdr.rtlsdr_get_device_usb_strings(idx, mfg, prod, ser))
            entry = {
                "index": idx,
                "name": name,
                "manufacturer": mfg.value.decode("utf-8", errors="ignore").strip()
                if rc == 0
                else (cached.get("manufacturer", "") if cached else ""),
                "product": prod.value.decode("utf-8", errors="ignore").strip()
                if rc == 0
                else (cached.get("product", "") if cached else ""),
                "serial": ser.value.decode("utf-8", errors="ignore").strip()
                if rc == 0
                else (cached.get("serial", "") if cached else ""),
            }
            if rc == 0:
                _SDR_USB_STRINGS_CACHE[idx] = dict(entry)
            devices.append(entry)
    except Exception as exc:
        return {"count": len(devices), "devices": devices, "error": str(exc)}

    return {"count": len(devices), "devices": devices}


def _read_recent_lines_sync(filepath: str, max_lines: int = 100):
    if not os.path.exists(filepath):
        return [], 0
    payload_buf = deque(maxlen=max_lines)
    system_buf = deque(maxlen=25)
    file_size = 0
    try:
        file_size = os.path.getsize(filepath)
        if file_size > 15 * 1024 * 1024:
            try:
                with open(filepath, "rb") as rf:
                    rf.seek(max(0, file_size - 5 * 1024 * 1024))
                    rf.readline()
                    tail_bytes = rf.read()
                tmp_ev = f"{filepath}.trim.{os.getpid()}"
                with open(tmp_ev, "wb") as wf:
                    wf.write(tail_bytes)
                os.replace(tmp_ev, filepath)
                file_size = os.path.getsize(filepath)
            except Exception:
                pass

        seek_pos = max(0, file_size - 256 * 1024)
        with open(filepath, "rb") as f:
            if seek_pos > 0:
                f.seek(seek_pos)
                f.readline()
            chunk = f.read().decode("utf-8", errors="replace")
            end_pos = f.tell()

        idx = 0
        for line in chunk.splitlines():
            s = line.strip()
            if not s:
                continue
            if '"category": "RAW_MAC"' in s or '"category":"RAW_MAC"' in s:
                continue
            idx += 1
            if '"category": "SYSTEM"' in s or '"category":"SYSTEM"' in s:
                system_buf.append((idx, s))
            else:
                payload_buf.append((idx, s))
    except Exception:
        return [], file_size
    merged = sorted(list(payload_buf) + list(system_buf), key=lambda item: item[0])
    return [line_str for _, line_str in merged], end_pos


def _read_tail_chunk_sync(filepath: str, file_pos: int):
    if not os.path.exists(filepath):
        return [], 0
    try:
        current_size = os.path.getsize(filepath)
        if current_size < file_pos:
            file_pos = max(0, current_size - 128 * 1024)
        if current_size <= file_pos:
            return [], current_size
        if (current_size - file_pos) > 256 * 1024:
            file_pos = current_size - 256 * 1024
        with open(filepath, "rb") as f:
            f.seek(file_pos)
            raw_bytes = f.read()
            new_pos = f.tell()
        lines = []
        for line in raw_bytes.decode("utf-8", errors="replace").splitlines():
            clean = line.strip()
            if clean and '"category": "RAW_MAC"' not in clean and '"category":"RAW_MAC"' not in clean:
                lines.append(clean)
        return lines, new_pos
    except Exception:
        return [], file_pos


async def read_recent_lines(filepath: str, max_lines: int = 100):
    loop = asyncio.get_running_loop()
    lines, _ = await loop.run_in_executor(None, _read_recent_lines_sync, filepath, max_lines)
    return lines


@app.websocket("/ws/events")
async def websocket_events(websocket: WebSocket):
    await websocket.accept()
    file_pos = 0

    try:
        if os.path.exists(EVENTS_FILE):
            loop = asyncio.get_running_loop()
            recent, file_pos = await loop.run_in_executor(
                None, _read_recent_lines_sync, EVENTS_FILE, 100
            )
            for line in recent:
                await websocket.send_text(line)

        while True:
            if not os.path.exists(EVENTS_FILE):
                file_pos = 0
                await asyncio.sleep(0.5)
                continue

            new_lines, file_pos = _read_tail_chunk_sync(EVENTS_FILE, file_pos)
            for clean in new_lines:
                await websocket.send_text(clean)

            await asyncio.sleep(0.25)
    except WebSocketDisconnect:
        return
    except Exception:
        return
