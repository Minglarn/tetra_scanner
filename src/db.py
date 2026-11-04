#!/usr/bin/env python3
import os
import json
import time
import sqlite3
import threading
from datetime import datetime, timezone

LOG_DIR = os.environ.get("LOG_DIR", "/app/logs")
DB_FILE = os.environ.get("DB_FILE", os.path.join(LOG_DIR, "tetra_scanner.db"))
EVENTS_JSONL_FILE = os.path.join(LOG_DIR, "tetra_events.jsonl")

_db_lock = threading.Lock()
_initialized = False

# In-memory packet counter buffer to avoid SQLite writes on every single raw MAC frame
_pkt_buffer_lock = threading.Lock()
_pkt_buffer = {}  # (freq_mhz, hour_bucket) -> {"la": la, "total": int, "raw_mac": int, "last_ts": float}
_last_pkt_flush_ts = 0.0


def get_connection():
    os.makedirs(os.path.dirname(DB_FILE), exist_ok=True)
    conn = sqlite3.connect(DB_FILE, timeout=15.0, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("PRAGMA temp_store=MEMORY;")
    return conn


def _parse_iso_to_unix(iso_str):
    if not iso_str:
        return time.time()
    try:
        s = str(iso_str).strip()
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        dt = datetime.fromisoformat(s)
        return dt.timestamp()
    except Exception:
        return time.time()


def _hour_bucket_from_ts(ts_unix):
    return datetime.fromtimestamp(ts_unix, tz=timezone.utc).strftime("%Y-%m-%d %H:00")


def _extract_unit_fields(event_dict):
    train_no = None
    consist_id = None
    device_mac = None
    device_id = None

    parsed = event_dict.get("parsed")
    if isinstance(parsed, dict):
        if parsed.get("train_or_route_no") is not None:
            train_no = str(parsed["train_or_route_no"])
        elif parsed.get("line_or_id") is not None:
            train_no = str(parsed["line_or_id"])
        if parsed.get("consist_or_tx_id") is not None:
            consist_id = str(parsed["consist_or_tx_id"])
        if parsed.get("device_mac"):
            device_mac = str(parsed["device_mac"])
        if parsed.get("device_id"):
            device_id = str(parsed["device_id"])

    unit_ctx = event_dict.get("unit_context")
    if isinstance(unit_ctx, dict):
        if not train_no and unit_ctx.get("train_or_route_no") is not None:
            train_no = str(unit_ctx["train_or_route_no"])
        if not consist_id and unit_ctx.get("consist_or_tx_id") is not None:
            consist_id = str(unit_ctx["consist_or_tx_id"])
        if not device_mac and unit_ctx.get("device_mac"):
            device_mac = str(unit_ctx["device_mac"])
        if not device_id and unit_ctx.get("device_id"):
            device_id = str(unit_ctx["device_id"])

    return train_no, consist_id, device_mac, device_id


def init_db():
    global _initialized
    with _db_lock:
        if _initialized and os.path.exists(DB_FILE):
            return
        try:
            conn = get_connection()
            with conn:
                conn.executescript(
                    """
                    CREATE TABLE IF NOT EXISTS events (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        ts_unix REAL NOT NULL,
                        ts_iso TEXT NOT NULL,
                        category TEXT NOT NULL,
                        freq_mhz TEXT NOT NULL,
                        la TEXT,
                        pdu TEXT,
                        service TEXT,
                        ssi INTEGER,
                        dest_ssi INTEGER,
                        gssi INTEGER,
                        sds_type TEXT,
                        sds_text TEXT,
                        human_text TEXT,
                        audio_url TEXT,
                        duration_s REAL,
                        gps_lat REAL,
                        gps_lon REAL,
                        train_no TEXT,
                        device_mac TEXT,
                        json_payload TEXT
                    );

                    CREATE INDEX IF NOT EXISTS idx_events_ts_unix ON events(ts_unix DESC);
                    CREATE INDEX IF NOT EXISTS idx_events_cat_ts ON events(category, ts_unix DESC);
                    CREATE INDEX IF NOT EXISTS idx_events_freq_ts ON events(freq_mhz, ts_unix DESC);
                    CREATE INDEX IF NOT EXISTS idx_events_ssi_ts ON events(ssi, ts_unix DESC);

                    CREATE TABLE IF NOT EXISTS channel_hourly_stats (
                        freq_mhz TEXT NOT NULL,
                        hour_bucket TEXT NOT NULL,
                        la TEXT,
                        total_packets INTEGER DEFAULT 0,
                        sds_count INTEGER DEFAULT 0,
                        voice_count INTEGER DEFAULT 0,
                        voice_seconds REAL DEFAULT 0.0,
                        call_count INTEGER DEFAULT 0,
                        status_count INTEGER DEFAULT 0,
                        roaming_count INTEGER DEFAULT 0,
                        network_count INTEGER DEFAULT 0,
                        raw_mac_count INTEGER DEFAULT 0,
                        last_active_ts REAL NOT NULL,
                        PRIMARY KEY (freq_mhz, hour_bucket)
                    );

                    CREATE INDEX IF NOT EXISTS idx_ch_stats_hour ON channel_hourly_stats(hour_bucket DESC);

                    CREATE TABLE IF NOT EXISTS units (
                        ssi INTEGER PRIMARY KEY,
                        ssi_type TEXT,
                        first_seen_ts REAL NOT NULL,
                        last_seen_ts REAL NOT NULL,
                        last_freq_mhz TEXT,
                        last_la TEXT,
                        train_or_route_no TEXT,
                        consist_id TEXT,
                        device_mac TEXT,
                        device_id TEXT,
                        sds_count INTEGER DEFAULT 0,
                        voice_count INTEGER DEFAULT 0,
                        voice_seconds REAL DEFAULT 0.0,
                        call_count INTEGER DEFAULT 0,
                        status_count INTEGER DEFAULT 0,
                        roaming_count INTEGER DEFAULT 0,
                        last_lat REAL,
                        last_lon REAL,
                        last_human_text TEXT
                    );

                    CREATE INDEX IF NOT EXISTS idx_units_last_seen ON units(last_seen_ts DESC);

                    CREATE TABLE IF NOT EXISTS rf_sweeps (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        ts_unix REAL NOT NULL,
                        freq_mhz TEXT NOT NULL,
                        freq_hz INTEGER NOT NULL,
                        dbm REAL NOT NULL,
                        snr_db REAL NOT NULL,
                        noise_floor REAL NOT NULL,
                        quality_label TEXT
                    );

                    CREATE INDEX IF NOT EXISTS idx_rf_sweeps_freq_ts ON rf_sweeps(freq_mhz, ts_unix DESC);
                    CREATE INDEX IF NOT EXISTS idx_rf_sweeps_ts ON rf_sweeps(ts_unix DESC);
                    """
                )

                # Backfill existing tetra_events.jsonl if events table is empty
                cur = conn.execute("SELECT COUNT(1) AS cnt FROM events")
                row = cur.fetchone()
                existing_count = int(row["cnt"]) if row else 0

            if existing_count == 0 and os.path.exists(EVENTS_JSONL_FILE):
                _backfill_from_jsonl(conn, EVENTS_JSONL_FILE)

            conn.close()
            _initialized = True
        except Exception as exc:
            print(f"[db] Database init error: {exc}", flush=True)


def _insert_event_with_conn(conn, event_dict, commit_each=False):
    if not isinstance(event_dict, dict):
        return
    category = str(event_dict.get("category") or "OTHER").upper()
    ts_iso = str(
        event_dict.get("timestamp")
        or event_dict.get("time")
        or datetime.now(timezone.utc).isoformat()
    )
    ts_unix = _parse_iso_to_unix(ts_iso)
    freq_mhz = str(event_dict.get("freq_mhz") or "SYS")
    la_val = event_dict.get("la")
    la_str = str(la_val) if la_val is not None and str(la_val) != "?" else None

    if category == "RAW_MAC":
        hour_bucket = _hour_bucket_from_ts(ts_unix)
        conn.execute(
            """
            INSERT INTO channel_hourly_stats (
                freq_mhz, hour_bucket, la, total_packets, raw_mac_count, last_active_ts
            ) VALUES (?, ?, ?, 1, 1, ?)
            ON CONFLICT(freq_mhz, hour_bucket) DO UPDATE SET
                la = COALESCE(excluded.la, channel_hourly_stats.la),
                total_packets = channel_hourly_stats.total_packets + 1,
                raw_mac_count = channel_hourly_stats.raw_mac_count + 1,
                last_active_ts = MAX(channel_hourly_stats.last_active_ts, excluded.last_active_ts)
            """,
            (freq_mhz, hour_bucket, la_str, ts_unix),
        )
        return

    pdu = str(event_dict.get("pdu") or "") or None
    service = str(event_dict.get("service") or "") or None

    def _safe_int(v):
        try:
            if v is None or v == "":
                return None
            iv = int(v)
            return None if iv in (0, 16777215) else iv
        except (ValueError, TypeError):
            return None

    def _safe_float(v):
        try:
            return float(v) if v is not None and v != "" else None
        except (ValueError, TypeError):
            return None

    ssi = _safe_int(event_dict.get("ssi"))
    dest_ssi = _safe_int(event_dict.get("dest_ssi"))
    gssi = _safe_int(event_dict.get("gssi"))
    sds_type = str(event_dict.get("sds_type") or "") or None
    sds_text = str(event_dict.get("sds_text") or "") or None
    human_text = str(
        event_dict.get("human_text")
        or event_dict.get("sds_text")
        or event_dict.get("status_text")
        or event_dict.get("pdu")
        or ""
    )
    audio_url = str(event_dict.get("audio_url") or "") or None
    duration_s = _safe_float(event_dict.get("duration_s")) or 0.0
    gps_lat = _safe_float(event_dict.get("gps_lat"))
    gps_lon = _safe_float(event_dict.get("gps_lon"))
    train_no, consist_id, device_mac, device_id = _extract_unit_fields(event_dict)
    json_payload = json.dumps(event_dict, ensure_ascii=False)

    conn.execute(
        """
        INSERT INTO events (
            ts_unix, ts_iso, category, freq_mhz, la, pdu, service,
            ssi, dest_ssi, gssi, sds_type, sds_text, human_text,
            audio_url, duration_s, gps_lat, gps_lon, train_no, device_mac, json_payload
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            ts_unix,
            ts_iso,
            category,
            freq_mhz,
            la_str,
            pdu,
            service,
            ssi,
            dest_ssi,
            gssi,
            sds_type,
            sds_text,
            human_text,
            audio_url,
            duration_s if duration_s > 0 else None,
            gps_lat,
            gps_lon,
            train_no,
            device_mac,
            json_payload,
        ),
    )

    # Update channel_hourly_stats for real RF frequencies
    if freq_mhz and freq_mhz != "SYS" and not freq_mhz.startswith("?"):
        hour_bucket = _hour_bucket_from_ts(ts_unix)
        sds_inc = 1 if category == "SDS" else 0
        voice_inc = 1 if category == "VOICE" else 0
        voice_sec_inc = duration_s if category == "VOICE" else 0.0
        call_inc = 1 if category == "CALL" else 0
        status_inc = 1 if category == "STATUS" else 0
        roaming_inc = 1 if category == "ROAMING" else 0
        net_inc = 1 if category == "NETWORK" else 0

        conn.execute(
            """
            INSERT INTO channel_hourly_stats (
                freq_mhz, hour_bucket, la, total_packets,
                sds_count, voice_count, voice_seconds, call_count,
                status_count, roaming_count, network_count, raw_mac_count, last_active_ts
            ) VALUES (?, ?, ?, 1, ?, ?, ?, ?, ?, ?, ?, 0, ?)
            ON CONFLICT(freq_mhz, hour_bucket) DO UPDATE SET
                la = COALESCE(excluded.la, channel_hourly_stats.la),
                total_packets = channel_hourly_stats.total_packets + 1,
                sds_count = channel_hourly_stats.sds_count + excluded.sds_count,
                voice_count = channel_hourly_stats.voice_count + excluded.voice_count,
                voice_seconds = channel_hourly_stats.voice_seconds + excluded.voice_seconds,
                call_count = channel_hourly_stats.call_count + excluded.call_count,
                status_count = channel_hourly_stats.status_count + excluded.status_count,
                roaming_count = channel_hourly_stats.roaming_count + excluded.roaming_count,
                network_count = channel_hourly_stats.network_count + excluded.network_count,
                last_active_ts = MAX(channel_hourly_stats.last_active_ts, excluded.last_active_ts)
            """,
            (
                freq_mhz,
                hour_bucket,
                la_str,
                sds_inc,
                voice_inc,
                voice_sec_inc,
                call_inc,
                status_inc,
                roaming_inc,
                net_inc,
                ts_unix,
            ),
        )

    # Update units registry
    if ssi is not None and ssi > 0:
        ssi_type = str(event_dict.get("ssi_type") or "SSI")
        sds_inc = 1 if category == "SDS" else 0
        voice_inc = 1 if category == "VOICE" else 0
        voice_sec_inc = duration_s if category == "VOICE" else 0.0
        call_inc = 1 if category == "CALL" else 0
        status_inc = 1 if category == "STATUS" else 0
        roaming_inc = 1 if category == "ROAMING" else 0

        conn.execute(
            """
            INSERT INTO units (
                ssi, ssi_type, first_seen_ts, last_seen_ts, last_freq_mhz, last_la,
                train_or_route_no, consist_id, device_mac, device_id,
                sds_count, voice_count, voice_seconds, call_count, status_count, roaming_count,
                last_lat, last_lon, last_human_text
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(ssi) DO UPDATE SET
                last_seen_ts = MAX(units.last_seen_ts, excluded.last_seen_ts),
                last_freq_mhz = COALESCE(excluded.last_freq_mhz, units.last_freq_mhz),
                last_la = COALESCE(excluded.last_la, units.last_la),
                train_or_route_no = COALESCE(excluded.train_or_route_no, units.train_or_route_no),
                consist_id = COALESCE(excluded.consist_id, units.consist_id),
                device_mac = COALESCE(excluded.device_mac, units.device_mac),
                device_id = COALESCE(excluded.device_id, units.device_id),
                sds_count = units.sds_count + excluded.sds_count,
                voice_count = units.voice_count + excluded.voice_count,
                voice_seconds = units.voice_seconds + excluded.voice_seconds,
                call_count = units.call_count + excluded.call_count,
                status_count = units.status_count + excluded.status_count,
                roaming_count = units.roaming_count + excluded.roaming_count,
                last_lat = COALESCE(excluded.last_lat, units.last_lat),
                last_lon = COALESCE(excluded.last_lon, units.last_lon),
                last_human_text = excluded.last_human_text
            """,
            (
                ssi,
                ssi_type,
                ts_unix,
                ts_unix,
                freq_mhz if freq_mhz != "SYS" else None,
                la_str,
                train_no,
                consist_id,
                device_mac,
                device_id,
                sds_inc,
                voice_inc,
                voice_sec_inc,
                call_inc,
                status_inc,
                roaming_inc,
                gps_lat,
                gps_lon,
                human_text[:240] if human_text else None,
            ),
        )


def _backfill_from_jsonl(conn, jsonl_path):
    try:
        file_size = os.path.getsize(jsonl_path)
        seek_pos = max(0, file_size - 25 * 1024 * 1024)
        with open(jsonl_path, "r", encoding="utf-8", errors="replace") as f:
            if seek_pos > 0:
                f.seek(seek_pos)
                f.readline()
            with conn:
                for line in f:
                    s = line.strip()
                    if not s:
                        continue
                    try:
                        ev = json.loads(s)
                        if isinstance(ev, dict):
                            _insert_event_with_conn(conn, ev)
                    except Exception:
                        continue
        print(f"[db] Backfilled SQLite database from {jsonl_path}", flush=True)
    except Exception as exc:
        print(f"[db] Backfill warning: {exc}", flush=True)


def record_event(event_dict):
    init_db()
    try:
        with _db_lock:
            conn = get_connection()
            try:
                with conn:
                    _insert_event_with_conn(conn, event_dict)
            finally:
                conn.close()
    except Exception as exc:
        print(f"[db] Failed to record event: {exc}", flush=True)


def record_raw_packet_stat(freq_mhz, la=None, now_ts=None, force_flush=False):
    global _last_pkt_flush_ts
    if not freq_mhz or freq_mhz == "SYS" or str(freq_mhz).startswith("?"):
        return
    ts = now_ts or time.time()
    hour_bucket = _hour_bucket_from_ts(ts)
    la_str = str(la) if la is not None and str(la) != "?" else None
    key = (str(freq_mhz), hour_bucket)

    to_flush = None
    with _pkt_buffer_lock:
        entry = _pkt_buffer.get(key)
        if entry is None:
            entry = {"la": la_str, "total": 0, "raw_mac": 0, "last_ts": ts}
            _pkt_buffer[key] = entry
        if la_str:
            entry["la"] = la_str
        entry["total"] += 1
        entry["raw_mac"] += 1
        entry["last_ts"] = ts

        if force_flush or (ts - _last_pkt_flush_ts >= 5.0):
            to_flush = dict(_pkt_buffer)
            _pkt_buffer.clear()
            _last_pkt_flush_ts = ts

    if to_flush:
        flush_packet_stats_dict(to_flush)


def flush_packet_stats_dict(batch_dict=None):
    global _last_pkt_flush_ts
    if batch_dict is None:
        with _pkt_buffer_lock:
            if not _pkt_buffer:
                return
            batch_dict = dict(_pkt_buffer)
            _pkt_buffer.clear()
            _last_pkt_flush_ts = time.time()

    if not batch_dict:
        return

    init_db()
    try:
        with _db_lock:
            conn = get_connection()
            try:
                with conn:
                    for (freq_mhz, hour_bucket), info in batch_dict.items():
                        conn.execute(
                            """
                            INSERT INTO channel_hourly_stats (
                                freq_mhz, hour_bucket, la, total_packets, raw_mac_count, last_active_ts
                            ) VALUES (?, ?, ?, ?, ?, ?)
                            ON CONFLICT(freq_mhz, hour_bucket) DO UPDATE SET
                                la = COALESCE(excluded.la, channel_hourly_stats.la),
                                total_packets = channel_hourly_stats.total_packets + excluded.total_packets,
                                raw_mac_count = channel_hourly_stats.raw_mac_count + excluded.raw_mac_count,
                                last_active_ts = MAX(channel_hourly_stats.last_active_ts, excluded.last_active_ts)
                            """,
                            (
                                freq_mhz,
                                hour_bucket,
                                info.get("la"),
                                info.get("total", 0),
                                info.get("raw_mac", 0),
                                info.get("last_ts", time.time()),
                            ),
                        )
            finally:
                conn.close()
    except Exception as exc:
        print(f"[db] Failed flushing packet stats: {exc}", flush=True)


def record_rf_sweep_peaks(peaks_list):
    if not peaks_list:
        return
    init_db()
    now_ts = time.time()
    try:
        with _db_lock:
            conn = get_connection()
            try:
                with conn:
                    for pk in peaks_list:
                        freq_hz = int(pk.get("freq_hz", 0))
                        if freq_hz <= 0:
                            continue
                        freq_mhz = f"{freq_hz / 1e6:.4f}"
                        dbm = float(pk.get("dbm", -99.0))
                        snr_db = float(pk.get("snr_db", 0.0))
                        nf = float(pk.get("noise_floor", dbm - snr_db))
                        if dbm >= -12.0:
                            q_lbl = "NEAR SATURATION"
                        elif snr_db >= 8.0:
                            q_lbl = "STRONG"
                        elif snr_db >= 4.0:
                            q_lbl = "GOOD"
                        elif snr_db >= 2.5:
                            q_lbl = "MODERATE"
                        else:
                            q_lbl = "WEAK"
                        conn.execute(
                            """
                            INSERT INTO rf_sweeps (
                                ts_unix, freq_mhz, freq_hz, dbm, snr_db, noise_floor, quality_label
                            ) VALUES (?, ?, ?, ?, ?, ?, ?)
                            """,
                            (now_ts, freq_mhz, freq_hz, dbm, snr_db, nf, q_lbl),
                        )
            finally:
                conn.close()
    except Exception as exc:
        print(f"[db] Failed recording RF sweep peaks: {exc}", flush=True)


def query_history(
    category=None,
    freq_mhz=None,
    ssi=None,
    q=None,
    hours=24,
    limit=150,
    offset=0,
    include_system=False,
):
    init_db()
    where_clauses = []
    params = []

    if hours and float(hours) > 0:
        cutoff = time.time() - float(hours) * 3600.0
        where_clauses.append("ts_unix >= ?")
        params.append(cutoff)

    if category and str(category).upper() not in ("ALL", ""):
        cats = [c.strip().upper() for c in str(category).split(",") if c.strip()]
        if len(cats) == 1:
            where_clauses.append("category = ?")
            params.append(cats[0])
        elif len(cats) > 1:
            placeholders = ",".join("?" for _ in cats)
            where_clauses.append(f"category IN ({placeholders})")
            params.extend(cats)
    elif not include_system:
        where_clauses.append("category != 'SYSTEM'")

    if freq_mhz and str(freq_mhz).strip() not in ("ALL", ""):
        f_clean = str(freq_mhz).strip().replace("MHz", "").strip()
        where_clauses.append("freq_mhz LIKE ?")
        params.append(f"%{f_clean}%")

    if ssi and str(ssi).strip():
        s_clean = str(ssi).strip()
        if s_clean.isdigit():
            s_int = int(s_clean)
            where_clauses.append("(ssi = ? OR dest_ssi = ? OR gssi = ? OR train_no = ?)")
            params.extend([s_int, s_int, s_int, s_clean])
        else:
            where_clauses.append("(train_no LIKE ? OR device_mac LIKE ?)")
            params.extend([f"%{s_clean}%", f"%{s_clean}%"])

    if q and str(q).strip():
        q_clean = f"%{str(q).strip()}%"
        where_clauses.append(
            "(human_text LIKE ? OR sds_text LIKE ? OR freq_mhz LIKE ? OR train_no LIKE ? OR CAST(ssi AS TEXT) LIKE ?)"
        )
        params.extend([q_clean, q_clean, q_clean, q_clean, q_clean])

    where_sql = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""
    safe_limit = max(1, min(int(limit or 150), 500))
    safe_offset = max(0, int(offset or 0))

    try:
        conn = get_connection()
        try:
            cnt_row = conn.execute(
                f"SELECT COUNT(1) AS total FROM events {where_sql}", params
            ).fetchone()
            total_matches = int(cnt_row["total"]) if cnt_row else 0

            rows = conn.execute(
                f"""
                SELECT id, ts_unix, ts_iso, category, freq_mhz, la, pdu, service,
                       ssi, dest_ssi, gssi, sds_type, sds_text, human_text,
                       audio_url, duration_s, gps_lat, gps_lon, train_no, device_mac
                FROM events
                {where_sql}
                ORDER BY ts_unix DESC, id DESC
                LIMIT ? OFFSET ?
                """,
                params + [safe_limit, safe_offset],
            ).fetchall()

            events_out = [dict(r) for r in rows]
            return {
                "total": total_matches,
                "limit": safe_limit,
                "offset": safe_offset,
                "events": events_out,
            }
        finally:
            conn.close()
    except Exception as exc:
        print(f"[db] query_history error: {exc}", flush=True)
        return {"total": 0, "limit": safe_limit, "offset": safe_offset, "events": []}


def get_analytics_summary(hours=24):
    init_db()
    safe_hours = float(hours) if hours and float(hours) > 0 else 24.0
    cutoff_ts = time.time() - safe_hours * 3600.0
    cutoff_bucket = _hour_bucket_from_ts(cutoff_ts)

    try:
        conn = get_connection()
        try:
            # 1. Totals by category from events
            cat_rows = conn.execute(
                """
                SELECT category, COUNT(1) AS cnt, COALESCE(SUM(duration_s), 0.0) AS total_dur
                FROM events
                WHERE ts_unix >= ?
                GROUP BY category
                """,
                (cutoff_ts,),
            ).fetchall()
            counts_by_cat = {r["category"]: int(r["cnt"]) for r in cat_rows}
            total_voice_sec = sum(
                float(r["total_dur"] or 0.0) for r in cat_rows if r["category"] == "VOICE"
            )

            # 2. Per-frequency stats combined with latest RF sweep measurements
            freq_rows = conn.execute(
                """
                SELECT
                    freq_mhz,
                    MAX(la) AS la,
                    SUM(total_packets) AS total_packets,
                    SUM(sds_count) AS sds_count,
                    SUM(voice_count) AS voice_count,
                    ROUND(SUM(voice_seconds), 1) AS voice_seconds,
                    SUM(call_count) AS call_count,
                    SUM(status_count) AS status_count,
                    SUM(roaming_count) AS roaming_count,
                    SUM(network_count) AS network_count,
                    SUM(raw_mac_count) AS raw_mac_count,
                    MAX(last_active_ts) AS last_active_ts
                FROM channel_hourly_stats
                WHERE hour_bucket >= ?
                GROUP BY freq_mhz
                ORDER BY (SUM(sds_count) * 10 + SUM(voice_count) * 15 + SUM(call_count) * 5 + SUM(total_packets)) DESC
                """,
                (cutoff_bucket,),
            ).fetchall()

            rf_rows = conn.execute(
                """
                SELECT r.freq_mhz, r.dbm, r.snr_db, r.noise_floor, r.quality_label, r.ts_unix
                FROM rf_sweeps r
                INNER JOIN (
                    SELECT freq_mhz, MAX(id) AS max_id
                    FROM rf_sweeps
                    GROUP BY freq_mhz
                ) latest ON r.id = latest.max_id
                """
            ).fetchall()
            rf_by_freq = {r["freq_mhz"]: dict(r) for r in rf_rows}

            frequencies = []
            total_packets_period = 0
            for r in freq_rows:
                d = dict(r)
                f_mhz = d["freq_mhz"]
                total_packets_period += int(d.get("total_packets") or 0)
                rf_info = rf_by_freq.get(f_mhz)
                if rf_info:
                    d["last_dbm"] = rf_info["dbm"]
                    d["last_snr_db"] = rf_info["snr_db"]
                    d["last_noise_floor"] = rf_info["noise_floor"]
                    d["rf_quality"] = rf_info["quality_label"]
                else:
                    d["last_dbm"] = None
                    d["last_snr_db"] = None
                    d["last_noise_floor"] = None
                    d["rf_quality"] = None
                d["last_active_iso"] = (
                    datetime.fromtimestamp(d["last_active_ts"], tz=timezone.utc).isoformat()
                    if d.get("last_active_ts")
                    else None
                )
                frequencies.append(d)

            # 3. Hourly activity timeline
            hourly_rows = conn.execute(
                """
                SELECT
                    hour_bucket,
                    SUM(total_packets) AS total_packets,
                    SUM(sds_count) AS sds_count,
                    SUM(voice_count) AS voice_count,
                    SUM(call_count) AS call_count,
                    SUM(status_count) AS status_count,
                    SUM(roaming_count) AS roaming_count
                FROM channel_hourly_stats
                WHERE hour_bucket >= ?
                GROUP BY hour_bucket
                ORDER BY hour_bucket ASC
                """,
                (cutoff_bucket,),
            ).fetchall()
            hourly_timeline = [dict(r) for r in hourly_rows]

            # 4. Active radio units / trains
            unit_rows = conn.execute(
                """
                SELECT
                    ssi, ssi_type, first_seen_ts, last_seen_ts, last_freq_mhz, last_la,
                    train_or_route_no, consist_id, device_mac, device_id,
                    sds_count, voice_count, ROUND(voice_seconds, 1) AS voice_seconds,
                    call_count, status_count, roaming_count,
                    last_lat, last_lon, last_human_text
                FROM units
                WHERE last_seen_ts >= ?
                ORDER BY (voice_count * 10 + sds_count * 3 + call_count * 2 + status_count) DESC, last_seen_ts DESC
                LIMIT 100
                """,
                (cutoff_ts,),
            ).fetchall()
            units_list = []
            for r in unit_rows:
                ud = dict(r)
                ud["last_seen_iso"] = datetime.fromtimestamp(
                    ud["last_seen_ts"], tz=timezone.utc
                ).isoformat()
                units_list.append(ud)

            # 5. Recent voice recordings
            voice_rows = conn.execute(
                """
                SELECT id, ts_unix, ts_iso, freq_mhz, la, ssi, gssi, duration_s, audio_url, human_text, train_no
                FROM events
                WHERE category = 'VOICE' AND ts_unix >= ?
                ORDER BY ts_unix DESC
                LIMIT 50
                """,
                (cutoff_ts,),
            ).fetchall()
            voice_recordings = [dict(r) for r in voice_rows]

            db_size_bytes = os.path.getsize(DB_FILE) if os.path.exists(DB_FILE) else 0

            return {
                "hours": safe_hours,
                "db_size_kb": round(db_size_bytes / 1024.0, 1),
                "totals": {
                    "total_packets": total_packets_period,
                    "sds": counts_by_cat.get("SDS", 0),
                    "voice": counts_by_cat.get("VOICE", 0),
                    "voice_seconds": round(total_voice_sec, 1),
                    "call": counts_by_cat.get("CALL", 0),
                    "status": counts_by_cat.get("STATUS", 0),
                    "roaming": counts_by_cat.get("ROAMING", 0),
                    "active_frequencies": len(frequencies),
                    "active_units": len(units_list),
                },
                "frequencies": frequencies,
                "hourly_timeline": hourly_timeline,
                "units": units_list,
                "voice_recordings": voice_recordings,
            }
        finally:
            conn.close()
    except Exception as exc:
        print(f"[db] get_analytics_summary error: {exc}", flush=True)
        return {
            "hours": safe_hours,
            "db_size_kb": 0,
            "totals": {
                "total_packets": 0,
                "sds": 0,
                "voice": 0,
                "voice_seconds": 0.0,
                "call": 0,
                "status": 0,
                "roaming": 0,
                "active_frequencies": 0,
                "active_units": 0,
            },
            "frequencies": [],
            "hourly_timeline": [],
            "units": [],
            "voice_recordings": [],
        }
