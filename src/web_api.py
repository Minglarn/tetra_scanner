#!/usr/bin/env python3
import os
import json
import time
import asyncio
from collections import deque
import aiofiles
from fastapi import FastAPI, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates


APP_VERSION = os.environ.get("APP_VERSION", "2026.11.04.7")
LOG_DIR = os.environ.get("LOG_DIR", "/app/logs")
EVENTS_FILE = os.path.join(LOG_DIR, "tetra_events.jsonl")
NETWORKS_FILE = os.path.join(LOG_DIR, "discovered_networks.jsonl")
BLACKLIST_FILE = os.path.join(LOG_DIR, "blacklist.json")
WHITELIST_FILE = os.path.join(LOG_DIR, "whitelist.json")

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
TEMPLATES_DIR = os.path.join(BASE_DIR, "templates")

app = FastAPI(title="TETRA Scanner WebUI", version=APP_VERSION)
templates = Jinja2Templates(directory=TEMPLATES_DIR)


@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    return templates.TemplateResponse(
        "index.html",
        {
            "request": request,
            "version": APP_VERSION,
            "mode": os.environ.get("MODE", "static"),
        },
    )


@app.get("/api/networks", response_class=JSONResponse)
async def get_networks():
    if not os.path.exists(NETWORKS_FILE):
        return []

    networks_by_id = {}
    try:
        async with aiofiles.open(
            NETWORKS_FILE, mode="r", encoding="utf-8", errors="replace"
        ) as f:
            async for line in f:
                raw = line.strip()
                if not raw:
                    continue
                try:
                    item = json.loads(raw)
                except json.JSONDecodeError:
                    continue
                if not isinstance(item, dict):
                    continue
                net_id = item.get("network_id") or f"{item.get('freq_mhz')}_{item.get('la')}"
                networks_by_id[net_id] = item
    except Exception:
        return []

    result = list(networks_by_id.values())
    result.sort(key=lambda x: x.get("seen_epoch") or 0, reverse=True)
    return result


@app.get("/api/blacklist", response_class=JSONResponse)
async def get_blacklist():
    if not os.path.exists(BLACKLIST_FILE):
        return {}

    try:
        async with aiofiles.open(
            BLACKLIST_FILE, mode="r", encoding="utf-8", errors="replace"
        ) as f:
            content = await f.read()
            if not content.strip():
                return {}
            data = json.loads(content)
            if not isinstance(data, dict):
                return {}
    except Exception:
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
    if not os.path.exists(BLACKLIST_FILE):
        return {"status": "ok", "removed": freq_hz}

    try:
        async with aiofiles.open(
            BLACKLIST_FILE, mode="r", encoding="utf-8", errors="replace"
        ) as f:
            content = await f.read()
            data = json.loads(content) if content.strip() else {}
            if not isinstance(data, dict):
                data = {}
    except Exception:
        data = {}

    if freq_hz in data:
        del data[freq_hz]
        tmp_path = f"{BLACKLIST_FILE}.tmp"
        try:
            async with aiofiles.open(tmp_path, mode="w", encoding="utf-8") as f:
                await f.write(json.dumps(data, indent=2, sort_keys=True))
            os.replace(tmp_path, BLACKLIST_FILE)
        except Exception:
            pass

    return {"status": "ok", "removed": freq_hz}


@app.delete("/api/blacklist", response_class=JSONResponse)
async def clear_blacklist():
    try:
        os.makedirs(LOG_DIR, exist_ok=True)
        tmp_path = f"{BLACKLIST_FILE}.tmp"
        async with aiofiles.open(tmp_path, mode="w", encoding="utf-8") as f:
            await f.write("{}\n")
        os.replace(tmp_path, BLACKLIST_FILE)
    except Exception:
        pass
    return {"status": "ok", "cleared": True}


@app.get("/api/whitelist", response_class=JSONResponse)
async def get_whitelist():
    if not os.path.exists(WHITELIST_FILE):
        return []

    try:
        async with aiofiles.open(
            WHITELIST_FILE, mode="r", encoding="utf-8", errors="replace"
        ) as f:
            content = await f.read()
            if not content.strip():
                return []
            data = json.loads(content)
            if not isinstance(data, dict):
                return []
    except Exception:
        return []

    items = [v for v in data.values() if isinstance(v, dict)]
    items.sort(key=lambda x: x.get("freq_hz") or 0)
    return items


async def read_recent_lines(filepath: str, max_lines: int = 100):
    if not os.path.exists(filepath):
        return []
    buf = deque(maxlen=max_lines)
    try:
        async with aiofiles.open(
            filepath, mode="r", encoding="utf-8", errors="replace"
        ) as f:
            async for line in f:
                s = line.strip()
                if s:
                    buf.append(s)
    except Exception:
        return []
    return list(buf)


@app.websocket("/ws/events")
async def websocket_events(websocket: WebSocket):
    await websocket.accept()
    file_pos = 0

    try:
        if os.path.exists(EVENTS_FILE):
            recent = await read_recent_lines(EVENTS_FILE, max_lines=100)
            for line in recent:
                await websocket.send_text(line)
            try:
                file_pos = os.path.getsize(EVENTS_FILE)
            except OSError:
                file_pos = 0

        while True:
            if not os.path.exists(EVENTS_FILE):
                file_pos = 0
                await asyncio.sleep(0.5)
                continue

            try:
                current_size = os.path.getsize(EVENTS_FILE)
            except OSError:
                await asyncio.sleep(0.5)
                continue

            if current_size < file_pos:
                file_pos = 0

            if current_size > file_pos:
                try:
                    async with aiofiles.open(
                        EVENTS_FILE, mode="r", encoding="utf-8", errors="replace"
                    ) as f:
                        await f.seek(file_pos)
                        while True:
                            line = await f.readline()
                            if not line:
                                break
                            clean = line.strip()
                            if clean:
                                await websocket.send_text(clean)
                        file_pos = await f.tell()
                except OSError:
                    pass

            await asyncio.sleep(0.25)
    except WebSocketDisconnect:
        return
    except Exception:
        return
