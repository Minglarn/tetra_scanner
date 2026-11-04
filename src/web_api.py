#!/usr/bin/env python3
import os
import json
import asyncio
from collections import deque
import aiofiles
from fastapi import FastAPI, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates


APP_VERSION = os.environ.get("APP_VERSION", "2026.11.04.4")
LOG_DIR = os.environ.get("LOG_DIR", "/app/logs")
EVENTS_FILE = os.path.join(LOG_DIR, "tetra_events.jsonl")
NETWORKS_FILE = os.path.join(LOG_DIR, "discovered_networks.jsonl")

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
