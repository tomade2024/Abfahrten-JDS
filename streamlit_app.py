import io
import json
import os
import sqlite3
import time
import uuid
import hashlib
import hmac
from datetime import datetime, timedelta, time as dtime
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd
import streamlit as st

try:
    from streamlit_autorefresh import st_autorefresh
except Exception:
    def st_autorefresh(interval=15000, key=None):
        return None

st.set_page_config(page_title="Abfahrten V3.1.1", layout="wide")

# Bestehende Daten/Verzeichnisse weiterverwenden
APP_NAME = "AbfahrtenV32"
TZ = ZoneInfo("Europe/Berlin")

COUNTDOWN_START_HOURS = 3
AUTO_COMPLETE_AFTER_MIN = 20
KEEP_COMPLETED_MINUTES = 10
MATERIALIZE_TOURS_HOURS_BEFORE = 12
DISPLAY_WINDOW_HOURS = 12
BLINK_UNDER_MINUTES = 10
CRITICAL_UNDER_MINUTES = 5
AUTO_BACKUP_KEEP = 30

WEEKDAYS_DE = ["Montag", "Dienstag", "Mittwoch", "Donnerstag", "Freitag", "Samstag", "Sonntag"]
WEEKDAY_TO_INT = {d: i for i, d in enumerate(WEEKDAYS_DE)}

ZONE_NAME_MAP = {
    1: "Zone A",
    2: "Zone B",
    3: "Zone C",
    4: "Zone D",
    8: "Wareneingang 1",
    9: "Wareneingang 2",
}

COMBINED_SCREEN_MAP = {
    101: {"name": "Split A + B", "left": 1, "right": 2},
    102: {"name": "Split C + D", "left": 3, "right": 4},
    103: {"name": "Split Wareneingang 1 + 2", "left": 8, "right": 9},
}

DEFAULT_CONFIG = {
    "security": {
        "password_iterations": 200000,
    },
    "display": {
        "overview_groups": {
            "5": [1, 2, 3],
            "6": [4, 8, 9],
            "7": [1, 2, 3, 4, 8, 9],
        }
    },
    "users": {
        "admin": {"password": "admin123", "role": "admin"},
        "dispo": {"password": "dispo123", "role": "viewer"},
    },
}


def get_base_dir() -> Path:
    local_appdata = os.getenv("LOCALAPPDATA")
    if local_appdata:
        base = Path(local_appdata) / APP_NAME
    else:
        base = Path.home() / f".{APP_NAME.lower()}"
    base.mkdir(parents=True, exist_ok=True)
    return base


BASE_DIR = get_base_dir()
DATA_DIR = BASE_DIR / "daten"
BACKUP_DIR = BASE_DIR / "backups"
LOG_DIR = BASE_DIR / "logs"
CONFIG_PATH = BASE_DIR / "config.json"
DB_PATH = DATA_DIR / "abfahrten_v32.db"
APP_LOG_PATH = LOG_DIR / "app.log"

for d in [DATA_DIR, BACKUP_DIR, LOG_DIR]:
    d.mkdir(parents=True, exist_ok=True)


def deep_merge(base: dict, override: dict) -> dict:
    result = dict(base)
    for k, v in override.items():
        if isinstance(v, dict) and isinstance(result.get(k), dict):
            result[k] = deep_merge(result[k], v)
        else:
            result[k] = v
    return result


def load_config() -> dict:
    cfg = dict(DEFAULT_CONFIG)
    if CONFIG_PATH.exists():
        try:
            loaded = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
            if isinstance(loaded, dict):
                cfg = deep_merge(cfg, loaded)
        except Exception:
            pass
    return cfg


def save_config(cfg: dict):
    CONFIG_PATH.write_text(json.dumps(cfg, ensure_ascii=False, indent=2), encoding="utf-8")


APP_CONFIG = load_config()
PASSWORD_ITERATIONS = int(APP_CONFIG.get("security", {}).get("password_iterations", 200000))
OVERVIEW_GROUPS = {int(k): v for k, v in APP_CONFIG.get("display", {}).get("overview_groups", {}).items()}


def now_berlin() -> datetime:
    return datetime.now(TZ)


def ensure_tz(dt):
    if dt is None:
        return None
    if hasattr(dt, "to_pydatetime"):
        dt = dt.to_pydatetime()
    if dt.tzinfo is None:
        return dt.replace(tzinfo=TZ)
    return dt.astimezone(TZ)


def escape_html(text: str) -> str:
    text = "" if text is None else str(text)
    return text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def parse_screen_ids(value) -> list[int]:
    if value is None:
        return []
    s = str(value).strip()
    if not s:
        return []
    return [int(x.strip()) for x in s.split(",") if x.strip().isdigit()]


def fmt_compact(td: timedelta) -> str:
    total = max(0, int(td.total_seconds()))
    h = total // 3600
    m = (total % 3600) // 60
    s = total % 60
    return f"{h:02d}:{m:02d}:{s:02d}" if h > 0 else f"{m:02d}:{s:02d}"


def completion_deadline(dep_dt: datetime) -> datetime:
    return dep_dt + timedelta(minutes=AUTO_COMPLETE_AFTER_MIN)


def time_options_half_hour():
    return [f"{h:02d}:{m:02d}" for h in range(24) for m in (0, 30)]


def next_datetime_for_weekday_time(weekday_name: str, hour: int, minute: int) -> datetime:
    now = now_berlin()
    target = WEEKDAY_TO_INT[weekday_name]
    days_ahead = (target - now.weekday()) % 7
    candidate_date = now.date() + timedelta(days=days_ahead)
    candidate_dt = datetime.combine(candidate_date, dtime(hour=hour, minute=minute)).replace(tzinfo=TZ)
    if candidate_dt <= now:
        candidate_dt += timedelta(days=7)
    return candidate_dt


def df_to_csv_bytes(df: pd.DataFrame) -> bytes:
    out = io.StringIO()
    df.to_csv(out, index=False, sep=";", encoding="utf-8")
    return ("\ufeff" + out.getvalue()).encode("utf-8")


def hash_password(password: str, iterations: int = PASSWORD_ITERATIONS) -> str:
    salt = os.urandom(16).hex()
    dk = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt.encode("utf-8"), iterations)
    return f"pbkdf2_sha256${iterations}${salt}${dk.hex()}"


def verify_password(password: str, stored: str) -> bool:
    if not stored:
        return False
    if stored.startswith("pbkdf2_sha256$"):
        try:
            _, iterations, salt, digest = stored.split("$", 3)
            dk = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt.encode("utf-8"), int(iterations))
            return hmac.compare_digest(dk.hex(), digest)
        except Exception:
            return False
    return hmac.compare_digest(password, stored)


def get_runtime_users() -> dict:
    return load_config().get("users", {})


def save_runtime_users(users: dict):
    cfg = load_config()
    cfg["users"] = users
    save_config(cfg)


def log_event(conn, event_type: str, entity_type: str, entity_id=None, details=None, level="INFO"):
    ts = now_berlin().isoformat(timespec="seconds")
    username = str(st.session_state.get("username") or "SYSTEM")
    payload = {
        "ts": ts,
        "level": level,
        "event_type": event_type,
        "entity_type": entity_type,
        "entity_id": entity_id,
        "username": username,
        "details": details or {},
    }
    try:
        with open(APP_LOG_PATH, "a", encoding="utf-8") as f:
            f.write(json.dumps(payload, ensure_ascii=False) + "\n")
    except Exception:
        pass
    if conn is not None:
        try:
            conn.execute(
                """
                INSERT INTO audit_log (event_time, username, event_type, entity_type, entity_id, details_json)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    ts, username, event_type, entity_type,
                    None if entity_id is None else str(entity_id),
                    json.dumps(details or {}, ensure_ascii=False),
                ),
            )
            conn.commit()
        except Exception:
            pass


def integrity_ok(conn: sqlite3.Connection) -> bool:
    try:
        row = conn.execute("PRAGMA integrity_check;").fetchone()
        return bool(row) and str(row[0]).lower() == "ok"
    except Exception:
        return False


def execute_with_retry(cur: sqlite3.Cursor, sql: str, params: tuple = (), retries: int = 6):
    for i in range(retries):
        try:
            cur.execute(sql, params)
            return
        except sqlite3.OperationalError as e:
            msg = str(e).lower()
            if "locked" in msg or "busy" in msg:
                time.sleep(0.25 * (i + 1))
                continue
            raise


def init_db(conn: sqlite3.Connection):
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS locations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            type TEXT NOT NULL,
            active INTEGER NOT NULL DEFAULT 1,
            color TEXT,
            text_color TEXT
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS departures (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            datetime TEXT NOT NULL,
            location_id INTEGER NOT NULL,
            vehicle TEXT,
            status TEXT NOT NULL DEFAULT 'GEPLANT',
            note TEXT,
            ready_at TEXT,
            completed_at TEXT,
            source_key TEXT,
            created_by TEXT,
            screen_id INTEGER,
            countdown_enabled INTEGER NOT NULL DEFAULT 1,
            cooled_required INTEGER NOT NULL DEFAULT 0,
            FOREIGN KEY(location_id) REFERENCES locations(id)
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS tours (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            weekday TEXT NOT NULL,
            hour INTEGER NOT NULL,
            minute INTEGER NOT NULL DEFAULT 0,
            location_id INTEGER NOT NULL,
            note TEXT,
            active INTEGER NOT NULL DEFAULT 1,
            screen_ids TEXT,
            countdown_enabled INTEGER NOT NULL DEFAULT 0,
            cooled_required INTEGER NOT NULL DEFAULT 0,
            FOREIGN KEY(location_id) REFERENCES locations(id)
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS tour_stops (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            tour_id INTEGER NOT NULL,
            location_id INTEGER NOT NULL,
            position INTEGER NOT NULL DEFAULT 0,
            FOREIGN KEY(tour_id) REFERENCES tours(id),
            FOREIGN KEY(location_id) REFERENCES locations(id)
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS holiday_tours (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            holiday_date TEXT NOT NULL,
            hour INTEGER NOT NULL,
            minute INTEGER NOT NULL DEFAULT 0,
            location_id INTEGER NOT NULL,
            note TEXT,
            active INTEGER NOT NULL DEFAULT 1,
            screen_ids TEXT,
            countdown_enabled INTEGER NOT NULL DEFAULT 0,
            cooled_required INTEGER NOT NULL DEFAULT 0,
            FOREIGN KEY(location_id) REFERENCES locations(id)
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS holiday_tour_stops (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            holiday_tour_id INTEGER NOT NULL,
            location_id INTEGER NOT NULL,
            position INTEGER NOT NULL DEFAULT 0,
            FOREIGN KEY(holiday_tour_id) REFERENCES holiday_tours(id),
            FOREIGN KEY(location_id) REFERENCES locations(id)
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS screens (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            mode TEXT NOT NULL,
            filter_type TEXT NOT NULL DEFAULT 'ALLE',
            filter_locations TEXT,
            refresh_interval_seconds INTEGER NOT NULL DEFAULT 30,
            holiday_flag INTEGER NOT NULL DEFAULT 0,
            special_flag INTEGER NOT NULL DEFAULT 0
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS tickers (
            screen_id INTEGER PRIMARY KEY,
            text TEXT,
            active INTEGER NOT NULL DEFAULT 0
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS audit_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            event_time TEXT NOT NULL,
            username TEXT,
            event_type TEXT NOT NULL,
            entity_type TEXT NOT NULL,
            entity_id TEXT,
            details_json TEXT
        )
    """)

    cur.execute("SELECT COUNT(*) FROM screens")
    if cur.fetchone()[0] == 0:
        defaults = [
            (1, "Zone A", "DETAIL", "ALLE", "", 15, 0, 0),
            (2, "Zone B", "DETAIL", "ALLE", "", 15, 0, 0),
            (3, "Zone C", "DETAIL", "ALLE", "", 15, 0, 0),
            (4, "Zone D", "DETAIL", "ALLE", "", 15, 0, 0),
            (5, "Übersicht Links", "OVERVIEW", "ALLE", "", 20, 0, 0),
            (6, "Übersicht Rechts", "OVERVIEW", "ALLE", "", 20, 0, 0),
            (7, "Lagerstand Übersicht", "WAREHOUSE", "ALLE", "", 20, 0, 0),
            (8, "Wareneingang 1", "DETAIL", "ALLE", "", 15, 0, 0),
            (9, "Wareneingang 2", "DETAIL", "ALLE", "", 15, 0, 0),
        ]
        cur.executemany(
            "INSERT INTO screens (id, name, mode, filter_type, filter_locations, refresh_interval_seconds, holiday_flag, special_flag) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            defaults,
        )

    cur.execute("SELECT id FROM screens")
    for sid in [int(r[0]) for r in cur.fetchall()]:
        cur.execute("INSERT OR IGNORE INTO tickers (screen_id, text, active) VALUES (?, '', 0)", (sid,))

    cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_departures_source_key ON departures(source_key)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_departures_datetime ON departures(datetime)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_departures_screen_id ON departures(screen_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_audit_log_event_time ON audit_log(event_time)")
    conn.commit()


def migrate_db(conn: sqlite3.Connection):
    cur = conn.cursor()

    def table_cols(table: str) -> set[str]:
        try:
            rows = cur.execute(f"PRAGMA table_info({table});").fetchall()
            return {str(r[1]) for r in rows}
        except Exception:
            return set()

    init_db(conn)

    deps = table_cols("departures")
    if "screen_id" not in deps:
        cur.execute("ALTER TABLE departures ADD COLUMN screen_id INTEGER")
    if "source_key" not in deps:
        cur.execute("ALTER TABLE departures ADD COLUMN source_key TEXT")
    if "created_by" not in deps:
        cur.execute("ALTER TABLE departures ADD COLUMN created_by TEXT")
    if "ready_at" not in deps:
        cur.execute("ALTER TABLE departures ADD COLUMN ready_at TEXT")
    if "completed_at" not in deps:
        cur.execute("ALTER TABLE departures ADD COLUMN completed_at TEXT")
    if "countdown_enabled" not in deps:
        cur.execute("ALTER TABLE departures ADD COLUMN countdown_enabled INTEGER NOT NULL DEFAULT 1")
    if "cooled_required" not in deps:
        cur.execute("ALTER TABLE departures ADD COLUMN cooled_required INTEGER NOT NULL DEFAULT 0")

    locs = table_cols("locations")
    if "color" not in locs:
        cur.execute("ALTER TABLE locations ADD COLUMN color TEXT")
    if "text_color" not in locs:
        cur.execute("ALTER TABLE locations ADD COLUMN text_color TEXT")

    tours = table_cols("tours")
    if "minute" not in tours:
        cur.execute("ALTER TABLE tours ADD COLUMN minute INTEGER NOT NULL DEFAULT 0")
    if "screen_ids" not in tours:
        cur.execute("ALTER TABLE tours ADD COLUMN screen_ids TEXT")
    if "countdown_enabled" not in tours:
        cur.execute("ALTER TABLE tours ADD COLUMN countdown_enabled INTEGER NOT NULL DEFAULT 0")
    if "cooled_required" not in tours:
        cur.execute("ALTER TABLE tours ADD COLUMN cooled_required INTEGER NOT NULL DEFAULT 0")

    holiday_cols = table_cols("holiday_tours")
    if "minute" not in holiday_cols:
        cur.execute("ALTER TABLE holiday_tours ADD COLUMN minute INTEGER NOT NULL DEFAULT 0")
    if "screen_ids" not in holiday_cols:
        cur.execute("ALTER TABLE holiday_tours ADD COLUMN screen_ids TEXT")
    if "countdown_enabled" not in holiday_cols:
        cur.execute("ALTER TABLE holiday_tours ADD COLUMN countdown_enabled INTEGER NOT NULL DEFAULT 0")
    if "cooled_required" not in holiday_cols:
        cur.execute("ALTER TABLE holiday_tours ADD COLUMN cooled_required INTEGER NOT NULL DEFAULT 0")

    conn.commit()


@st.cache_resource
def get_connection():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("PRAGMA foreign_keys=ON;")
    conn.execute("PRAGMA busy_timeout=30000;")
    init_db(conn)
    migrate_db(conn)
    if not integrity_ok(conn):
        raise RuntimeError(f"Datenbank beschädigt: {DB_PATH}")
    return conn


def read_df(conn: sqlite3.Connection, query: str, params=()):
    try:
        return pd.read_sql_query(query, conn, params=params)
    except Exception as e:
        raise RuntimeError(f"SQL-Fehler in read_df: {e}\nQuery:\n{query}")


def load_locations(conn):
    return read_df(conn, "SELECT id, name, type, active, color, text_color FROM locations ORDER BY id")


def load_screens(conn):
    return read_df(conn, "SELECT s.*, t.text, t.active AS ticker_active FROM screens s LEFT JOIN tickers t ON t.screen_id=s.id ORDER BY id")


def load_tours(conn):
    return read_df(conn, """
        SELECT t.id, t.name, t.weekday, t.hour, t.minute, t.location_id,
               t.note, t.active, t.screen_ids, t.countdown_enabled, t.cooled_required,
               l.name AS location_name
        FROM tours t
        JOIN locations l ON t.location_id = l.id
        ORDER BY t.id
    """)


def load_tour_stops(conn, tour_id: int):
    return read_df(conn, """
        SELECT ts.location_id, ts.position, l.name AS location_name
        FROM tour_stops ts
        JOIN locations l ON l.id = ts.location_id
        WHERE ts.tour_id = ?
        ORDER BY ts.position
    """, (tour_id,))


def load_holiday_tours(conn):
    return read_df(conn, """
        SELECT h.id, h.name, h.holiday_date, h.hour, h.minute, h.location_id,
               h.note, h.active, h.screen_ids, h.countdown_enabled, h.cooled_required,
               l.name AS location_name
        FROM holiday_tours h
        JOIN locations l ON h.location_id = l.id
        ORDER BY h.holiday_date, h.hour, h.minute, h.id
    """)


def load_holiday_tour_stops(conn, holiday_tour_id: int):
    return read_df(conn, """
        SELECT hs.location_id, hs.position, l.name AS location_name
        FROM holiday_tour_stops hs
        JOIN locations l ON l.id = hs.location_id
        WHERE hs.holiday_tour_id = ?
        ORDER BY hs.position
    """, (holiday_tour_id,))


def load_departures_with_locations(conn):
    try:
        df = read_df(conn, """
            SELECT d.id AS id,
                   d.datetime AS datetime,
                   d.location_id AS location_id,
                   d.vehicle AS vehicle,
                   d.status AS status,
                   d.note AS note,
                   d.ready_at AS ready_at,
                   d.completed_at AS completed_at,
                   d.source_key AS source_key,
                   d.created_by AS created_by,
                   d.screen_id AS screen_id,
                   d.countdown_enabled AS countdown_enabled,
                   d.cooled_required AS cooled_required,
                   l.name AS location_name,
                   l.type AS location_type,
                   l.active AS location_active,
                   l.color AS location_color,
                   l.text_color AS location_text_color
            FROM departures d
            JOIN locations l ON d.location_id = l.id
        """)
    except Exception:
        return pd.DataFrame()

    if not df.empty:
        for col in ["datetime", "ready_at", "completed_at"]:
            df[col] = pd.to_datetime(df[col], errors="coerce")
            df[col] = df[col].apply(lambda x: ensure_tz(x) if pd.notnull(x) else x)
        df["countdown_enabled"] = pd.to_numeric(df["countdown_enabled"], errors="coerce").fillna(1).astype(int)
        df["cooled_required"] = pd.to_numeric(df["cooled_required"], errors="coerce").fillna(0).astype(int)

    return df


def export_backup_json(conn) -> bytes:
    tours = load_tours(conn)
    holiday_tours = load_holiday_tours(conn)

    tour_items = []
    for _, t in tours.iterrows():
        stops_df = load_tour_stops(conn, int(t["id"]))
        tour_items.append({
            "id": int(t["id"]),
            "name": str(t["name"]),
            "weekday": str(t["weekday"]),
            "hour": int(t["hour"]),
            "minute": int(t["minute"] or 0),
            "location_id": int(t["location_id"]),
            "note": str(t["note"] or ""),
            "active": int(t["active"]),
            "screen_ids": str(t["screen_ids"] or ""),
            "countdown_enabled": int(t["countdown_enabled"] or 0),
            "cooled_required": int(t["cooled_required"] or 0),
            "stops": stops_df.to_dict(orient="records"),
        })

    holiday_items = []
    for _, h in holiday_tours.iterrows():
        stops_df = load_holiday_tour_stops(conn, int(h["id"]))
        holiday_items.append({
            "id": int(h["id"]),
            "name": str(h["name"]),
            "holiday_date": str(h["holiday_date"]),
            "hour": int(h["hour"]),
            "minute": int(h["minute"] or 0),
            "location_id": int(h["location_id"]),
            "note": str(h["note"] or ""),
            "active": int(h["active"]),
            "screen_ids": str(h["screen_ids"] or ""),
            "countdown_enabled": int(h["countdown_enabled"] or 0),
            "cooled_required": int(h["cooled_required"] or 0),
            "stops": stops_df.to_dict(orient="records"),
        })

    payload = {
        "version": "3.1.1",
        "exported_at": now_berlin().isoformat(),
        "locations": load_locations(conn).to_dict(orient="records"),
        "tours": tour_items,
        "holiday_tours": holiday_items,
        "screens": load_screens(conn).to_dict(orient="records"),
    }
    return json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")


def save_backup_to_dir(conn, prefix: str = "backup_auto") -> Path:
    stamp = now_berlin().strftime("%Y%m%d_%H%M%S")
    target = BACKUP_DIR / f"{prefix}_{stamp}.json"
    target.write_bytes(export_backup_json(conn))
    return target


def cleanup_old_backups(keep: int = AUTO_BACKUP_KEEP):
    files = sorted(BACKUP_DIR.glob("backup_*.json"), key=lambda p: p.stat().st_mtime, reverse=True)
    for old in files[keep:]:
        try:
            old.unlink()
        except Exception:
            pass


def maybe_run_nightly_backup(conn):
    marker = BACKUP_DIR / ".last_nightly_backup.txt"
    now = now_berlin()
    today = now.strftime("%Y%m%d")
    last_done = st.session_state.get("last_nightly_backup_date")
    if last_done is None and marker.exists():
        try:
            last_done = marker.read_text(encoding="utf-8").strip()
        except Exception:
            last_done = None
    if last_done != today and now.hour >= 2:
        save_backup_to_dir(conn, prefix="backup_nightly")
        cleanup_old_backups()
        st.session_state["last_nightly_backup_date"] = today
        try:
            marker.write_text(today, encoding="utf-8")
        except Exception:
            pass


def import_backup_json(conn, data: dict):
    cur = conn.cursor()

    for loc in data.get("locations", []):
        if not loc.get("name") or not loc.get("type"):
            continue
        loc_id = loc.get("id")
        if loc_id is not None:
            cur.execute("SELECT COUNT(*) FROM locations WHERE id=?", (int(loc_id),))
            if cur.fetchone()[0] > 0:
                cur.execute(
                    "UPDATE locations SET name=?, type=?, active=?, color=?, text_color=? WHERE id=?",
                    (loc["name"], loc["type"], int(loc.get("active", 1)), loc.get("color"), loc.get("text_color"), int(loc_id)),
                )
            else:
                cur.execute(
                    "INSERT INTO locations (id, name, type, active, color, text_color) VALUES (?, ?, ?, ?, ?, ?)",
                    (int(loc_id), loc["name"], loc["type"], int(loc.get("active", 1)), loc.get("color"), loc.get("text_color")),
                )

    for t in data.get("tours", []):
        if not t.get("name"):
            continue
        tour_id = t.get("id")
        if tour_id is not None:
            cur.execute("SELECT COUNT(*) FROM tours WHERE id=?", (int(tour_id),))
            exists = cur.fetchone()[0] > 0
            if exists:
                cur.execute(
                    "UPDATE tours SET name=?, weekday=?, hour=?, minute=?, location_id=?, note=?, active=?, screen_ids=?, countdown_enabled=?, cooled_required=? WHERE id=?",
                    (
                        t["name"], t["weekday"], int(t.get("hour", 0)), int(t.get("minute", 0)),
                        int(t["location_id"]), t.get("note", ""), int(t.get("active", 1)),
                        t.get("screen_ids", ""), int(t.get("countdown_enabled", 0)),
                        int(t.get("cooled_required", 0)), int(tour_id),
                    ),
                )
            else:
                cur.execute(
                    "INSERT INTO tours (id, name, weekday, hour, minute, location_id, note, active, screen_ids, countdown_enabled, cooled_required) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        int(tour_id), t["name"], t["weekday"], int(t.get("hour", 0)),
                        int(t.get("minute", 0)), int(t["location_id"]), t.get("note", ""),
                        int(t.get("active", 1)), t.get("screen_ids", ""),
                        int(t.get("countdown_enabled", 0)), int(t.get("cooled_required", 0)),
                    ),
                )
            cur.execute("DELETE FROM tour_stops WHERE tour_id=?", (int(tour_id),))
            for s in t.get("stops", []):
                cur.execute(
                    "INSERT INTO tour_stops (tour_id, location_id, position) VALUES (?, ?, ?)",
                    (int(tour_id), int(s["location_id"]), int(s.get("position", 0)))
                )

    for h in data.get("holiday_tours", []):
        if not h.get("name"):
            continue
        hid = h.get("id")
        if hid is not None:
            cur.execute("SELECT COUNT(*) FROM holiday_tours WHERE id=?", (int(hid),))
            exists = cur.fetchone()[0] > 0
            if exists:
                cur.execute(
                    "UPDATE holiday_tours SET name=?, holiday_date=?, hour=?, minute=?, location_id=?, note=?, active=?, screen_ids=?, countdown_enabled=?, cooled_required=? WHERE id=?",
                    (
                        h["name"], h["holiday_date"], int(h.get("hour", 0)), int(h.get("minute", 0)),
                        int(h["location_id"]), h.get("note", ""), int(h.get("active", 1)),
                        h.get("screen_ids", ""), int(h.get("countdown_enabled", 0)),
                        int(h.get("cooled_required", 0)), int(hid),
                    ),
                )
            else:
                cur.execute(
                    "INSERT INTO holiday_tours (id, name, holiday_date, hour, minute, location_id, note, active, screen_ids, countdown_enabled, cooled_required) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        int(hid), h["name"], h["holiday_date"], int(h.get("hour", 0)),
                        int(h.get("minute", 0)), int(h["location_id"]), h.get("note", ""),
                        int(h.get("active", 1)), h.get("screen_ids", ""),
                        int(h.get("countdown_enabled", 0)), int(h.get("cooled_required", 0)),
                    ),
                )
            cur.execute("DELETE FROM holiday_tour_stops WHERE holiday_tour_id=?", (int(hid),))
            for s in h.get("stops", []):
                cur.execute(
                    "INSERT INTO holiday_tour_stops (holiday_tour_id, location_id, position) VALUES (?, ?, ?)",
                    (int(hid), int(s["location_id"]), int(s.get("position", 0)))
                )

    if "screens" in data:
        for s in data.get("screens", []):
            cur.execute("""
                INSERT INTO screens (id, name, mode, filter_type, filter_locations, refresh_interval_seconds, holiday_flag, special_flag)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(id) DO UPDATE SET
                    name=excluded.name,
                    mode=excluded.mode,
                    filter_type=excluded.filter_type,
                    filter_locations=excluded.filter_locations,
                    refresh_interval_seconds=excluded.refresh_interval_seconds,
                    holiday_flag=excluded.holiday_flag,
                    special_flag=excluded.special_flag
            """, (
                int(s["id"]), str(s["name"]), str(s["mode"]), str(s["filter_type"]),
                str(s.get("filter_locations") or ""), int(s["refresh_interval_seconds"]),
                int(s["holiday_flag"]), int(s["special_flag"]),
            ))
            cur.execute("INSERT OR REPLACE INTO tickers (screen_id, text, active) VALUES (?, ?, ?)", (
                int(s["id"]), str(s.get("text") or ""), int(s.get("ticker_active", 0) or 0)
            ))

    conn.commit()


def update_departure_statuses(conn: sqlite3.Connection):
    now = now_berlin()
    now_iso = now.isoformat(timespec="seconds")
    df = read_df(conn, "SELECT id, datetime, status FROM departures")
    if df.empty:
        return

    df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce")
    df = df.dropna(subset=["datetime"])
    df["datetime"] = df["datetime"].apply(ensure_tz)

    to_ready = []
    to_done = []

    for _, r in df.iterrows():
        dep_dt = ensure_tz(r["datetime"])
        status = str(r["status"] or "").upper()
        if status in ("STORNIERT", "ABGESCHLOSSEN"):
            continue
        if now >= completion_deadline(dep_dt):
            to_done.append(int(r["id"]))
        elif now >= dep_dt and status != "BEREIT":
            to_ready.append(int(r["id"]))

    if to_ready:
        conn.executemany(
            "UPDATE departures SET status='BEREIT', ready_at=COALESCE(ready_at, ?) WHERE id=?",
            [(now_iso, i) for i in to_ready]
        )
    if to_done:
        conn.executemany(
            "UPDATE departures SET status='ABGESCHLOSSEN', completed_at=COALESCE(completed_at, ?) WHERE id=?",
            [(now_iso, i) for i in to_done]
        )

    if to_ready or to_done:
        conn.commit()


def cleanup_materialized_departures(conn: sqlite3.Connection):
    cutoff_old = (now_berlin() - timedelta(days=2)).isoformat()
    cutoff_completed = (now_berlin() - timedelta(days=1)).isoformat()
    conn.execute("DELETE FROM departures WHERE source_key LIKE 'TOUR:%' AND datetime < ?", (cutoff_old,))
    conn.execute("DELETE FROM departures WHERE source_key LIKE 'HOLIDAY:%' AND datetime < ?", (cutoff_old,))
    conn.execute("DELETE FROM departures WHERE status='ABGESCHLOSSEN' AND completed_at IS NOT NULL AND completed_at < ?", (cutoff_completed,))
    conn.commit()


def materialize_tours_to_departures(conn: sqlite3.Connection):
    now = now_berlin()
    df = read_df(conn, """
        SELECT t.id AS tour_id, t.weekday, t.hour, t.minute, t.note AS tour_note,
               t.active AS tour_active, t.screen_ids AS tour_screen_ids,
               t.countdown_enabled AS tour_countdown_enabled,
               t.cooled_required AS tour_cooled_required,
               ts.location_id, ts.position, l.active AS location_active
        FROM tours t
        JOIN tour_stops ts ON ts.tour_id = t.id
        JOIN locations l ON l.id = ts.location_id
    """)
    if df.empty:
        return

    df = df[(df["tour_active"] == 1) & (df["location_active"] == 1)]
    cur = conn.cursor()

    for _, r in df.iterrows():
        weekday = str(r["weekday"])
        if weekday not in WEEKDAYS_DE:
            continue
        screen_ids = parse_screen_ids(r["tour_screen_ids"])
        if not screen_ids:
            continue

        dep_dt = next_datetime_for_weekday_time(weekday, int(r["hour"]), int(r["minute"]))
        if dep_dt - now > timedelta(hours=MATERIALIZE_TOURS_HOURS_BEFORE):
            continue

        for sid in screen_ids:
            source_key = f"TOUR:{int(r['tour_id'])}:{int(r['position'])}:{sid}:{dep_dt.isoformat()}"
            try:
                execute_with_retry(cur, """
                    INSERT INTO departures (datetime, location_id, vehicle, status, note, source_key, created_by, screen_id, countdown_enabled, cooled_required)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    dep_dt.isoformat(),
                    int(r["location_id"]),
                    "",
                    "GEPLANT",
                    str(r["tour_note"] or ""),
                    source_key,
                    "TOUR_AUTO",
                    sid,
                    int(r["tour_countdown_enabled"] or 0),
                    int(r["tour_cooled_required"] or 0),
                ))
            except sqlite3.IntegrityError:
                pass

    conn.commit()


def materialize_holiday_tours_to_departures(conn: sqlite3.Connection):
    try:
        now = now_berlin()
        window_start = (now - timedelta(minutes=AUTO_COMPLETE_AFTER_MIN)).date()
        window_end = (now + timedelta(hours=MATERIALIZE_TOURS_HOURS_BEFORE)).date()

        df = read_df(conn, """
            SELECT h.id AS holiday_tour_id,
                   h.holiday_date,
                   h.hour,
                   h.minute,
                   h.note AS holiday_note,
                   h.active AS holiday_active,
                   h.screen_ids AS holiday_screen_ids,
                   h.countdown_enabled AS holiday_countdown_enabled,
                   h.cooled_required AS holiday_cooled_required,
                   hs.location_id,
                   hs.position,
                   l.active AS location_active
            FROM holiday_tours h
            JOIN holiday_tour_stops hs ON hs.holiday_tour_id = h.id
            JOIN locations l ON l.id = hs.location_id
        """)
    except Exception as e:
        log_event(conn, "error", "holiday_materialize", details={"message": str(e)}, level="ERROR")
        return

    if df.empty:
        return

    df = df[(df["holiday_active"] == 1) & (df["location_active"] == 1)].copy()
    cur = conn.cursor()

    for _, r in df.iterrows():
        try:
            holiday_date = pd.to_datetime(r["holiday_date"]).date()
        except Exception:
            continue

        if holiday_date < window_start or holiday_date > window_end:
            continue

        screen_ids = parse_screen_ids(r["holiday_screen_ids"])
        if not screen_ids:
            continue

        dep_dt = datetime.combine(holiday_date, dtime(hour=int(r["hour"]), minute=int(r["minute"]))).replace(tzinfo=TZ)

        for sid in screen_ids:
            source_key = f"HOLIDAY:{int(r['holiday_tour_id'])}:{int(r['position'])}:{sid}:{dep_dt.isoformat()}"
            try:
                execute_with_retry(cur, """
                    INSERT INTO departures (datetime, location_id, vehicle, status, note, source_key, created_by, screen_id, countdown_enabled, cooled_required)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    dep_dt.isoformat(),
                    int(r["location_id"]),
                    "",
                    "GEPLANT",
                    str(r["holiday_note"] or ""),
                    source_key,
                    "HOLIDAY_AUTO",
                    int(sid),
                    int(r["holiday_countdown_enabled"] or 0),
                    int(r["holiday_cooled_required"] or 0),
                ))
            except sqlite3.IntegrityError:
                pass

    conn.commit()


def create_manual_departures(
    conn,
    dep_dt: datetime,
    location_id: int,
    screen_ids: list[int],
    note: str,
    created_by: str,
    countdown_enabled: bool,
    cooled_required: bool
):
    cur = conn.cursor()
    note_clean = (note or "").strip()
    for sid in screen_ids:
        source_key = f"MANUAL:{uuid.uuid4().hex}:{sid}:{dep_dt.isoformat()}"
        execute_with_retry(cur, """
            INSERT INTO departures (datetime, location_id, vehicle, status, note, source_key, created_by, screen_id, countdown_enabled, cooled_required)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            dep_dt.isoformat(),
            int(location_id),
            "",
            "GEPLANT",
            note_clean,
            source_key,
            created_by,
            int(sid),
            1 if countdown_enabled else 0,
            1 if cooled_required else 0,
        ))
    conn.commit()


def update_manual_departure(conn, dep_id: int, dep_dt: datetime, location_id: int, screen_id: int, note: str, countdown_enabled: bool, cooled_required: bool):
    conn.execute(
        """
        UPDATE departures
        SET datetime=?, location_id=?, screen_id=?, note=?, countdown_enabled=?, cooled_required=?
        WHERE id=? AND source_key LIKE 'MANUAL:%'
        """,
        (
            dep_dt.isoformat(),
            int(location_id),
            int(screen_id),
            (note or "").strip(),
            1 if countdown_enabled else 0,
            1 if cooled_required else 0,
            int(dep_id),
        ),
    )
    conn.commit()


def is_urgent_countdown(row) -> bool:
    try:
        status = str(row.get("status") or "").upper()
        if status != "GEPLANT":
            return False
        dep_dt = ensure_tz(row.get("datetime"))
        remaining = dep_dt - now_berlin()
        return timedelta(0) <= remaining <= timedelta(minutes=BLINK_UNDER_MINUTES)
    except Exception:
        return False


def is_critical_countdown(row) -> bool:
    try:
        status = str(row.get("status") or "").upper()
        if status != "GEPLANT":
            return False
        dep_dt = ensure_tz(row.get("datetime"))
        remaining = dep_dt - now_berlin()
        return timedelta(0) <= remaining <= timedelta(minutes=CRITICAL_UNDER_MINUTES)
    except Exception:
        return False


def build_info_html(row) -> str:
    note = str(row.get("note") or "")
    line_info = str(row.get("line_info") or "")
    status = str(row.get("status") or "").upper()
    cooled_required = int(row.get("cooled_required", 0) or 0) == 1
    parts = []
    if note:
        parts.append(escape_html(note))
    if line_info:
        line_info_html = escape_html(line_info)
        if status == "BEREIT":
            line_info_html = f"<span class='ready-badge'>{escape_html(line_info)}</span>"
        elif is_critical_countdown(row):
            line_info_html = f"<span class='blink-countdown-critical'>{escape_html(line_info)}</span>"
        elif is_urgent_countdown(row):
            line_info_html = f"<span class='blink-countdown'>{escape_html(line_info)}</span>"
        parts.append(line_info_html)
    if cooled_required:
        parts.append("<span class='cold-badge'>❄ Kühlware mitzunehmen</span>")
    return " · ".join(parts)


def get_screen_data(conn, screen_id: int):
    screens = load_screens(conn)
    if screens.empty or screen_id not in screens["id"].tolist():
        return None, pd.DataFrame()

    screen = screens.loc[screens["id"] == screen_id].iloc[0]
    now = now_berlin()
    end = now + timedelta(hours=DISPLAY_WINDOW_HOURS)
    start = now - timedelta(minutes=AUTO_COMPLETE_AFTER_MIN)

    deps = load_departures_with_locations(conn)
    if deps.empty:
        return screen, pd.DataFrame()

    deps = deps.copy()
    deps = deps[deps["location_active"] == 1]
    deps = deps[(deps["screen_id"].isna()) | (deps["screen_id"] == int(screen_id))]

    if str(screen.get("filter_type", "ALLE")) != "ALLE":
        deps = deps[deps["location_type"] == screen["filter_type"]]

    filter_locations = str(screen.get("filter_locations", "") or "").strip()
    if filter_locations:
        ids = [int(x.strip()) for x in filter_locations.split(",") if x.strip().isdigit()]
        if ids:
            deps = deps[deps["location_id"].isin(ids)]

    deps["datetime"] = pd.to_datetime(deps["datetime"], errors="coerce")
    deps = deps.dropna(subset=["datetime"]).copy()
    deps["datetime"] = deps["datetime"].apply(ensure_tz)
    deps = deps[(deps["datetime"] >= start) & (deps["datetime"] <= end)].copy()

    if deps.empty:
        return screen, deps

    def visible(row):
        status = str(row.get("status") or "").upper()
        if status != "ABGESCHLOSSEN":
            return True
        ca = row.get("completed_at")
        dep_dt = ensure_tz(row["datetime"])
        base = ensure_tz(ca) if pd.notnull(ca) else completion_deadline(dep_dt)
        return now <= base + timedelta(minutes=KEEP_COMPLETED_MINUTES)

    deps = deps[deps.apply(visible, axis=1)].copy()
    if deps.empty:
        return screen, deps

    def build_line_info(row):
        cd_on = int(row.get("countdown_enabled", 1) or 1) == 1
        if not cd_on:
            return ""
        status = str(row.get("status") or "").upper()
        dep_dt = ensure_tz(row["datetime"])

        if status == "GEPLANT":
            delta = dep_dt - now
            if timedelta(0) <= delta <= timedelta(hours=COUNTDOWN_START_HOURS):
                return f"Countdown: {fmt_compact(delta)}"
            return ""

        if status == "BEREIT":
            return f"BEREIT · Abschluss in {fmt_compact(completion_deadline(dep_dt) - now)}"
        return ""

    deps["line_info"] = [build_line_info(r) for _, r in deps.iterrows()]
    deps = deps.sort_values(["datetime", "location_name"], na_position="last").copy()
    return screen, deps


def is_next_departure(row, df: pd.DataFrame) -> bool:
    try:
        future = df[df["status"].fillna("").astype(str).str.upper().isin(["GEPLANT", "BEREIT"])].copy()
        if future.empty:
            return False
        future = future.sort_values("datetime")
        return int(row["id"]) == int(future.iloc[0]["id"])
    except Exception:
        return False


def get_row_display_styles(row, df: pd.DataFrame):
    base_bg = str(row.get("location_color") or "").strip()
    base_text = str(row.get("location_text_color") or "").strip()

    status = str(row.get("status") or "").upper()
    cooled_required = int(row.get("cooled_required", 0) or 0) == 1

    bg = base_bg
    text = base_text
    extra_css = ""

    if status == "BEREIT":
        bg = "#dcfce7"
        text = "#166534"

    if is_critical_countdown(row):
        bg = "#fecaca"
        text = "#7f1d1d"
        extra_css = "font-weight:900;"
    elif is_urgent_countdown(row):
        bg = "#fed7aa"
        text = "#9a3412"

    if cooled_required and not bg:
        bg = "#dbeafe"
        text = "#1e3a8a"

    if is_next_departure(row, df):
        extra_css += "outline:4px solid #f59e0b; outline-offset:-4px;"

    return bg, text, extra_css


def build_display_rows(data: pd.DataFrame):
    rows = []
    row_backgrounds = []
    text_colors = []
    extra_css = []

    if data is None or data.empty:
        return rows, row_backgrounds, text_colors, extra_css

    for _, r in data.iterrows():
        rows.append([
            ensure_tz(r["datetime"]).strftime("%H:%M"),
            r["location_name"],
            build_info_html(r),
        ])
        bg, tc, ex = get_row_display_styles(r, data)
        row_backgrounds.append(bg)
        text_colors.append(tc)
        extra_css.append(ex)

    return rows, row_backgrounds, text_colors, extra_css


def render_big_table_v2(headers, rows, row_backgrounds=None, text_colors=None, extra_row_css=None, html_cols=None):
    html_cols = set(html_cols or [])
    thead = "".join(f"<th>{escape_html(h)}</th>" for h in headers)
    body = ""

    for idx, r in enumerate(rows):
        style_parts = []
        if row_backgrounds and idx < len(row_backgrounds) and row_backgrounds[idx]:
            style_parts.append(f"background-color:{row_backgrounds[idx]};")
        if text_colors and idx < len(text_colors) and text_colors[idx]:
            style_parts.append(f"color:{text_colors[idx]};")
        if extra_row_css and idx < len(extra_row_css) and extra_row_css[idx]:
            style_parts.append(extra_row_css[idx])

        style = f' style="{"".join(style_parts)}"' if style_parts else ""
        cells = []
        for cidx, c in enumerate(r):
            if cidx in html_cols:
                cells.append(f"<td>{c or ''}</td>")
            else:
                cells.append(f"<td>{escape_html(str(c or ''))}</td>")
        body += f"<tr{style}>{''.join(cells)}</tr>"

    st.markdown(
        f"""
        <table class="big-table">
          <thead><tr>{thead}</tr></thead>
          <tbody>{body}</tbody>
        </table>
        """,
        unsafe_allow_html=True,
    )

def render_display_header(title: str | None = None, data: pd.DataFrame | None = None):
    now = now_berlin()
    weekday = WEEKDAYS_DE[now.weekday()]
    line = f"{weekday}, {now.strftime('%d.%m.%Y')} • {now.strftime('%H:%M:%S')}"

    summary_html = ""
    if data is not None and not data.empty:
        status_series = data["status"].fillna("").astype(str).str.upper()
        active = int((status_series == "GEPLANT").sum())
        ready = int((status_series == "BEREIT").sum())
        done = int((status_series == "ABGESCHLOSSEN").sum())

        summary_html = (
            f'<div style="display:flex;gap:10px;flex-wrap:wrap;justify-content:flex-end;margin-top:6px;">'
            f'<span style="background:#dbeafe;color:#1e3a8a;padding:4px 10px;border-radius:10px;font-weight:900;">Aktiv: {active}</span>'
            f'<span style="background:#dcfce7;color:#166534;padding:4px 10px;border-radius:10px;font-weight:900;">Bereit: {ready}</span>'
            f'<span style="background:#e5e7eb;color:#374151;padding:4px 10px;border-radius:10px;font-weight:900;">Fertig: {done}</span>'
            f'</div>'
        )

    html = f"""
<div style="
    display:flex;
    justify-content:space-between;
    align-items:flex-start;
    gap:18px;
    background:#111827;
    color:white;
    padding:10px 16px;
    border-radius:14px;
    margin-bottom:10px;
    font-weight:800;
">
    <div style="font-size:30px;line-height:1.2;">{escape_html(title or '')}</div>
    <div style="text-align:right;">
        <div style="font-size:26px;">{escape_html(line)}</div>
        {summary_html}
    </div>
</div>
"""
    st.markdown(html, unsafe_allow_html=True)



def base_display_css() -> str:
    return """
        <style>
        #MainMenu {visibility:hidden;}
        footer {visibility:hidden;}
        header {visibility:hidden;}

        html, body, [data-testid="stAppViewContainer"], [data-testid="stApp"], .main {
            margin: 0 !important;
            padding: 0 !important;
            width: 100vw !important;
            height: 100vh !important;
            overflow: hidden !important;
            background: #0f172a !important;
            cursor: none !important;
            user-select: none !important;
        }

        .block-container {
            max-width: 100vw !important;
            width: 100vw !important;
            height: 100vh !important;
            padding: 0.2rem 0.35rem 3rem 0.35rem !important;
            margin: 0 !important;
        }

        body, .block-container, .stMarkdown, .stText, div, span {
            font-size: 34px !important;
        }

        .big-table {
            width: 100%;
            border-collapse: collapse;
            background: #fff;
            border-radius: 14px;
            overflow: hidden;
        }

        .big-table th, .big-table td {
            border-bottom: 1px solid #d1d5db;
            padding: 0.78em 1em;
            text-align: left;
            vertical-align: top;
        }

        .big-table th {
            font-weight: 900;
            background: #e5e7eb;
            color: #111827;
        }

        .ticker {
            position: fixed;
            bottom: 0;
            left: 0;
            width: 100%;
            background: #000;
            color: #fff;
            overflow: hidden;
            white-space: nowrap;
            z-index: 9999;
            padding: .25rem 0;
        }

        .ticker__inner {
            display: inline-block;
            padding-left: 100%;
            animation: ticker-scroll 20s linear infinite;
            font-size: 28px !important;
        }

        @keyframes ticker-scroll {
            0% { transform: translateX(0); }
            100% { transform: translateX(-100%); }
        }

        .blink-countdown {
            color:#b91c1c;
            font-weight:900;
            animation: blinkUrgent 1s steps(2, start) infinite;
        }

        .blink-countdown-critical {
            color:#7f1d1d;
            font-weight:900;
            background:#fecaca;
            padding:0 6px;
            border-radius:6px;
            animation: blinkCritical 0.7s steps(2, start) infinite;
        }

        .ready-badge {
            color:#14532d;
            font-weight:900;
            background:#bbf7d0;
            padding:0 6px;
            border-radius:6px;
        }

        .cold-badge {
            color:#1e3a8a;
            font-weight:900;
            background:#bfdbfe;
            padding:0 6px;
            border-radius:6px;
        }

        @keyframes blinkUrgent {
            0% { opacity:1; }
            50% { opacity:0.15; }
            100% { opacity:1; }
        }

        @keyframes blinkCritical {
            0% { opacity:1; }
            50% { opacity:0.05; }
            100% { opacity:1; }
        }

        .split-monitor-card {
            background: #111827;
            padding: 8px 12px 10px 12px;
            min-height: 90vh;
            border: 3px solid #1f2937;
            border-radius: 16px;
            box-shadow: 0 18px 45px rgba(0,0,0,0.28);
        }

        .split-empty {
            background: #1f2937;
            color: #e5e7eb;
            border-radius: 14px;
            padding: 20px;
            font-size: 24px !important;
            border: 1px solid #374151;
        }

        .split-zone-title {
            color: #ffffff;
            font-size: 28px !important;
            font-weight: 900;
            margin-bottom: 10px;
            padding: 8px 10px;
            background: #0f172a;
            border-radius: 10px;
            text-transform: uppercase;
        }

        .zone-overview-card {
            background: #111827;
            border: 2px solid #1f2937;
            border-radius: 18px;
            padding: 16px;
            margin-bottom: 18px;
            box-shadow: 0 10px 24px rgba(0,0,0,0.22);
        }
        </style>
    """


def render_kiosk_hint():
    st.markdown(
        """
        <script>
        document.addEventListener("contextmenu", function(e) { e.preventDefault(); });
        document.addEventListener("dragstart", function(e) { e.preventDefault(); });
        document.addEventListener("selectstart", function(e) { e.preventDefault(); });
        </script>
        """,
        unsafe_allow_html=True,
    )


def get_combined_ticker_text(conn, screen_ids: list[int]) -> str:
    texts = []
    screens = load_screens(conn)
    for sid in screen_ids:
        row = screens.loc[screens["id"] == sid]
        if row.empty:
            continue
        text = str(row.iloc[0].get("text") or "").strip()
        active = bool(int(row.iloc[0].get("ticker_active") or 0))
        if active and text:
            texts.append(text)
    return "   ✦   ".join(dict.fromkeys(texts))


def render_zone_overview_screen(conn, screen_id: int):
    screens = load_screens(conn)
    screen_row = screens.loc[screens["id"] == int(screen_id)].iloc[0]

    zone_ids = OVERVIEW_GROUPS.get(int(screen_id), [1, 2, 3, 4, 8, 9])
    all_rows = []
    row_backgrounds = []
    text_colors = []
    extra_css = []
    combined_df_parts = []

    for zid in zone_ids:
        _, zone_data = get_screen_data(conn, zid)
        zone_name = ZONE_NAME_MAP.get(zid, f"Zone {zid}")
        if zone_data is None or zone_data.empty:
            continue

        combined_df_parts.append(zone_data.copy())
        for _, r in zone_data.iterrows():
            all_rows.append([
                ensure_tz(r["datetime"]).strftime("%H:%M"),
                r["location_name"],
                zone_name,
                build_info_html(r),
            ])
            bg, tc, ex = get_row_display_styles(r, zone_data)
            row_backgrounds.append(bg)
            text_colors.append(tc)
            extra_css.append(ex)

    combined_df = pd.concat(combined_df_parts, ignore_index=True) if combined_df_parts else pd.DataFrame()
    render_display_header(f"{screen_row['name']} (Screen {screen_id})", combined_df)

    st.markdown("<div class='zone-overview-card'>", unsafe_allow_html=True)
    if not all_rows:
        st.info("Keine Abfahrten im Zeitfenster.")
    else:
        render_big_table_v2(
            ["Zeit", "Einrichtung", "Zone", "Hinweis / Countdown"],
            all_rows,
            row_backgrounds=row_backgrounds,
            text_colors=text_colors,
            extra_row_css=extra_css,
            html_cols={3},
        )
    st.markdown("</div>", unsafe_allow_html=True)

    ticker_text = get_combined_ticker_text(conn, zone_ids)
    if ticker_text:
        st.markdown(f"<div class='ticker'><div class='ticker__inner'>{escape_html(ticker_text)}</div></div>", unsafe_allow_html=True)


def render_split_screen(conn, left_screen_id: int, right_screen_id: int, title: str):
    left_screen, left_data = get_screen_data(conn, left_screen_id)
    right_screen, right_data = get_screen_data(conn, right_screen_id)

    left_zone_name = ZONE_NAME_MAP.get(left_screen_id, left_screen["name"] if left_screen is not None else f"Screen {left_screen_id}")
    right_zone_name = ZONE_NAME_MAP.get(right_screen_id, right_screen["name"] if right_screen is not None else f"Screen {right_screen_id}")

    combined_parts = []
    if left_data is not None and not left_data.empty:
        combined_parts.append(left_data)
    if right_data is not None and not right_data.empty:
        combined_parts.append(right_data)
    combined_df = pd.concat(combined_parts, ignore_index=True) if combined_parts else pd.DataFrame()

    render_display_header(title, combined_df)

    col1, col2 = st.columns(2)

    with col1:
        st.markdown("<div class='split-monitor-card'>", unsafe_allow_html=True)
        st.markdown(f"<div class='split-zone-title'>{escape_html(left_zone_name)}</div>", unsafe_allow_html=True)
        if left_data is None or left_data.empty:
            st.markdown("<div class='split-empty'>Keine Abfahrten im Zeitfenster.</div>", unsafe_allow_html=True)
        else:
            rows, row_backgrounds, text_colors, extra_css = build_display_rows(left_data)
            render_big_table_v2(
                ["Zeit", "Einrichtung", "Hinweis / Countdown"],
                rows,
                row_backgrounds=row_backgrounds,
                text_colors=text_colors,
                extra_row_css=extra_css,
                html_cols={2},
            )
        st.markdown("</div>", unsafe_allow_html=True)

    with col2:
        st.markdown("<div class='split-monitor-card'>", unsafe_allow_html=True)
        st.markdown(f"<div class='split-zone-title'>{escape_html(right_zone_name)}</div>", unsafe_allow_html=True)
        if right_data is None or right_data.empty:
            st.markdown("<div class='split-empty'>Keine Abfahrten im Zeitfenster.</div>", unsafe_allow_html=True)
        else:
            rows, row_backgrounds, text_colors, extra_css = build_display_rows(right_data)
            render_big_table_v2(
                ["Zeit", "Einrichtung", "Hinweis / Countdown"],
                rows,
                row_backgrounds=row_backgrounds,
                text_colors=text_colors,
                extra_row_css=extra_css,
                html_cols={2},
            )
        st.markdown("</div>", unsafe_allow_html=True)

    ticker_text = get_combined_ticker_text(conn, [left_screen_id, right_screen_id])
    if ticker_text:
        st.markdown(f"<div class='ticker'><div class='ticker__inner'>{escape_html(ticker_text)}</div></div>", unsafe_allow_html=True)


def show_login():
    st.title("Login")
    users = get_runtime_users()
    if not users:
        st.error("Keine Benutzer konfiguriert.")
        st.stop()

    with st.form("login_form"):
        username = st.text_input("Benutzername")
        password = st.text_input("Passwort", type="password")
        submitted = st.form_submit_button("Einloggen")

    if submitted:
        user = users.get(username)
        if user and verify_password(password, str(user.get("password", ""))):
            st.session_state["logged_in"] = True
            st.session_state["username"] = username
            st.session_state["role"] = user.get("role", "viewer")
            log_event(None, "login", "auth", details={"username": username})
            st.rerun()
        else:
            st.error("Login fehlgeschlagen.")

    st.stop()


def require_login():
    if not st.session_state.get("logged_in"):
        show_login()


def show_admin_departures(conn, can_edit: bool):
    st.subheader("Abfahrten")
    materialize_tours_to_departures(conn)
    materialize_holiday_tours_to_departures(conn)
    update_departure_statuses(conn)

    deps = load_departures_with_locations(conn).sort_values("datetime")

    with st.expander("Filter / Suche", expanded=True):
        c1, c2, c3, c4 = st.columns(4)
        with c1:
            search_text = st.text_input("Suche Einrichtung / Hinweis")
        with c2:
            screen_options = ["ALLE"]
            if not deps.empty:
                screen_options += sorted([str(i) for i in deps["screen_id"].dropna().astype(int).unique().tolist()])
            screen_filter = st.selectbox("Screen", screen_options)
        with c3:
            status_filter = st.selectbox("Status", ["ALLE", "GEPLANT", "BEREIT", "ABGESCHLOSSEN"])
        with c4:
            cold_only = st.checkbox("Nur Kühlware")

    if deps.empty:
        st.info("Noch keine Abfahrten vorhanden.")
    else:
        view = deps.copy()
        if search_text.strip():
            q = search_text.strip().lower()
            view = view[
                view["location_name"].fillna("").astype(str).str.lower().str.contains(q)
                | view["note"].fillna("").astype(str).str.lower().str.contains(q)
            ]
        if screen_filter != "ALLE":
            view = view[view["screen_id"] == int(screen_filter)]
        if status_filter != "ALLE":
            view = view[view["status"].fillna("").astype(str).str.upper() == status_filter]
        if cold_only:
            view = view[view["cooled_required"] == 1]

        view["Quelle"] = view["source_key"].astype(str).apply(
            lambda s: "TOUR" if s.startswith("TOUR:")
            else ("FEIERTAG" if s.startswith("HOLIDAY:")
            else ("MANUELL" if s.startswith("MANUAL:") else "SONST"))
        )
        view["Zeit"] = view["datetime"].apply(lambda d: ensure_tz(d).strftime("%d.%m.%Y %H:%M") if pd.notnull(d) else "")
        st.dataframe(
            view[["id", "Zeit", "screen_id", "location_name", "note", "status", "countdown_enabled", "cooled_required", "Quelle"]],
            use_container_width=True,
            height=320
        )

    if not can_edit:
        return

    locations = load_locations(conn)
    screens = load_screens(conn)

    st.markdown("### Neue manuelle Abfahrt")
    with st.form("manual_dep_form"):
        c1, c2, c3 = st.columns(3)
        with c1:
            loc_id = st.selectbox("Einrichtung", locations["id"].tolist(), format_func=lambda i: locations.loc[locations["id"] == i, "name"].values[0])
            note = st.text_input("Hinweis")
        with c2:
            dep_date = st.date_input("Datum", value=now_berlin().date())
            dep_time = st.selectbox("Uhrzeit", time_options_half_hour(), index=time_options_half_hour().index("08:00"))
        with c3:
            screen_ids = st.multiselect("Screens", options=screens["id"].tolist(), default=[1])
            countdown_enabled = st.checkbox("Countdown aktiv", True)
            cooled_required = st.checkbox("Kühlware mitzunehmen", False)
        submitted = st.form_submit_button("Manuelle Abfahrt speichern")

    if submitted and screen_ids:
        hh, mm = map(int, dep_time.split(":"))
        dep_dt = datetime.combine(dep_date, dtime(hour=hh, minute=mm)).replace(tzinfo=TZ)
        create_manual_departures(
            conn,
            dep_dt,
            int(loc_id),
            [int(s) for s in screen_ids],
            note,
            str(st.session_state.get("username") or "ADMIN"),
            countdown_enabled,
            cooled_required
        )
        save_backup_to_dir(conn)
        cleanup_old_backups()
        st.success("Gespeichert.")
        st.rerun()


def show_admin_locations(conn, can_edit: bool):
    st.subheader("Einrichtungen")
    locations = load_locations(conn)
    st.dataframe(locations, use_container_width=True, height=260)
    st.download_button("Einrichtungen als CSV", data=df_to_csv_bytes(locations), file_name="einrichtungen.csv", mime="text/csv")

    if not can_edit:
        return

    st.markdown("### Neue Einrichtung")
    with st.form("new_location_form"):
        c1, c2, c3 = st.columns(3)
        with c1:
            name = st.text_input("Name")
            typ = st.selectbox("Typ", ["KRANKENHAUS", "ALTENHEIM", "MVZ"])
        with c2:
            active = st.checkbox("Aktiv", True)
            color = st.color_picker("Hintergrundfarbe", "#007bff")
        with c3:
            text_color = st.color_picker("Schriftfarbe", "#000000")
        submitted = st.form_submit_button("Speichern")

    if submitted and name.strip():
        conn.execute(
            "INSERT INTO locations (name, type, active, color, text_color) VALUES (?, ?, ?, ?, ?)",
            (name.strip(), typ, 1 if active else 0, color, text_color)
        )
        conn.commit()
        save_backup_to_dir(conn)
        cleanup_old_backups()
        st.success("Einrichtung gespeichert.")
        st.rerun()

    if locations.empty:
        return

    st.markdown("### Einrichtung bearbeiten / löschen")
    selected = st.selectbox("Einrichtung auswählen", locations["id"].tolist(), key="edit_location_select")
    row = locations.loc[locations["id"] == selected].iloc[0]

    with st.form("edit_location_form"):
        c1, c2, c3 = st.columns(3)
        with c1:
            edit_name = st.text_input("Name", row["name"])
            edit_type = st.selectbox(
                "Typ",
                ["KRANKENHAUS", "ALTENHEIM", "MVZ"],
                index=["KRANKENHAUS", "ALTENHEIM", "MVZ"].index(row["type"]) if row["type"] in ["KRANKENHAUS", "ALTENHEIM", "MVZ"] else 0
            )
        with c2:
            edit_active = st.checkbox("Aktiv", bool(row["active"]))
            edit_color = st.color_picker("Hintergrundfarbe", row["color"] if row["color"] else "#007bff")
        with c3:
            edit_text_color = st.color_picker("Schriftfarbe", row["text_color"] if row["text_color"] else "#000000")
        csave, cdel = st.columns(2)
        save = csave.form_submit_button("Speichern")
        delete = cdel.form_submit_button("Löschen")

    if save and edit_name.strip():
        conn.execute(
            "UPDATE locations SET name=?, type=?, active=?, color=?, text_color=? WHERE id=?",
            (edit_name.strip(), edit_type, 1 if edit_active else 0, edit_color, edit_text_color, int(selected))
        )
        conn.commit()
        save_backup_to_dir(conn)
        cleanup_old_backups()
        st.success("Aktualisiert.")
        st.rerun()

    if delete:
        try:
            conn.execute("DELETE FROM locations WHERE id=?", (int(selected),))
            conn.commit()
            save_backup_to_dir(conn)
            cleanup_old_backups()
            st.success("Gelöscht.")
            st.rerun()
        except Exception as e:
            st.error(str(e))


def export_tours_csv(conn):
    tours_df = read_df(conn, """
        SELECT
            t.id,
            t.name,
            t.weekday,
            t.hour,
            t.minute,
            t.location_id,
            l.name AS location_name,
            t.note,
            t.active,
            t.screen_ids,
            t.countdown_enabled,
            t.cooled_required
        FROM tours t
        LEFT JOIN locations l ON l.id = t.location_id
        ORDER BY
            CASE t.weekday
                WHEN 'Montag' THEN 1
                WHEN 'Dienstag' THEN 2
                WHEN 'Mittwoch' THEN 3
                WHEN 'Donnerstag' THEN 4
                WHEN 'Freitag' THEN 5
                WHEN 'Samstag' THEN 6
                WHEN 'Sonntag' THEN 7
                ELSE 99
            END,
            t.hour,
            t.minute,
            t.name
    """)

    stops_df = read_df(conn, """
        SELECT
            ts.id,
            ts.tour_id,
            t.name AS tour_name,
            ts.position,
            ts.location_id,
            l.name AS location_name
        FROM tour_stops ts
        JOIN tours t ON t.id = ts.tour_id
        JOIN locations l ON l.id = ts.location_id
        ORDER BY ts.tour_id, ts.position
    """)

    return df_to_csv_bytes(tours_df), df_to_csv_bytes(stops_df)


def show_admin_tours(conn, can_edit: bool):
    st.subheader("Touren")
    tours = load_tours(conn)

    with st.expander("Filter / Suche", expanded=True):
        c1, c2 = st.columns(2)
        with c1:
            search_text = st.text_input("Suche Tour / Hinweis")
        with c2:
            weekday_filter = st.selectbox("Wochentag", ["ALLE"] + WEEKDAYS_DE)

    if not tours.empty:
        view = tours.copy()
        if search_text.strip():
            q = search_text.strip().lower()
            view = view[
                view["name"].fillna("").astype(str).str.lower().str.contains(q)
                | view["note"].fillna("").astype(str).str.lower().str.contains(q)
            ]
        if weekday_filter != "ALLE":
            view = view[view["weekday"] == weekday_filter]
        view["Zeit"] = view.apply(lambda r: f"{int(r['hour']):02d}:{int(r['minute']):02d}", axis=1)
        st.dataframe(
            view[["id", "name", "weekday", "Zeit", "countdown_enabled", "cooled_required", "location_name", "note", "active", "screen_ids"]],
            use_container_width=True,
            height=280
        )
    else:
        st.info("Noch keine Touren vorhanden.")

    tours_csv, stops_csv = export_tours_csv(conn)
    c1, c2 = st.columns(2)
    with c1:
        st.download_button("Touren CSV", data=tours_csv, file_name="touren.csv", mime="text/csv")
    with c2:
        st.download_button("Tour-Stopps CSV", data=stops_csv, file_name="tour_stops.csv", mime="text/csv")

    if not can_edit:
        return

    locations = load_locations(conn)
    screens = load_screens(conn)

    st.markdown("### Neue Tour")
    with st.form("new_tour_form"):
        c1, c2, c3 = st.columns(3)
        with c1:
            tour_name = st.text_input("Tour-Name")
            weekday = st.selectbox("Wochentag", WEEKDAYS_DE)
        with c2:
            time_label = st.selectbox("Uhrzeit", time_options_half_hour(), index=time_options_half_hour().index("08:00"))
        with c3:
            screens_new = st.multiselect("Monitore", options=screens["id"].tolist())

        countdown_enabled = st.checkbox("Countdown aktiv", False)
        cooled_required = st.checkbox("Kühlware mitzunehmen", False)
        stops_new = st.multiselect(
            "Stops",
            options=locations["id"].tolist(),
            format_func=lambda i: locations.loc[locations["id"] == i, "name"].values[0]
        )
        note_new = st.text_input("Hinweis")
        active_new = st.checkbox("Aktiv", True)
        submitted = st.form_submit_button("Tour speichern")

    if submitted and tour_name.strip() and screens_new and stops_new:
        hh, mm = map(int, time_label.split(":"))
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO tours (name, weekday, hour, minute, location_id, note, active, screen_ids, countdown_enabled, cooled_required) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                tour_name.strip(),
                weekday,
                hh,
                mm,
                int(stops_new[0]),
                note_new.strip(),
                1 if active_new else 0,
                ",".join(map(str, screens_new)),
                1 if countdown_enabled else 0,
                1 if cooled_required else 0,
            ),
        )
        tour_id = cur.lastrowid
        for pos, loc_id in enumerate(stops_new):
            cur.execute("INSERT INTO tour_stops (tour_id, location_id, position) VALUES (?, ?, ?)", (tour_id, int(loc_id), pos))
        conn.commit()
        save_backup_to_dir(conn)
        cleanup_old_backups()
        st.success("Tour gespeichert.")
        st.rerun()

    if tours.empty:
        return

    st.markdown("### Tour bearbeiten / löschen")
    selected_tour_id = st.selectbox(
        "Tour auswählen",
        tours["id"].tolist(),
        format_func=lambda i: f"{int(i)} – {tours.loc[tours['id'] == i, 'name'].iloc[0]}",
        key="edit_tour_select"
    )

    tour_row = tours.loc[tours["id"] == selected_tour_id].iloc[0]
    stops_df = load_tour_stops(conn, int(selected_tour_id))
    current_stop_ids = stops_df["location_id"].astype(int).tolist() if not stops_df.empty else []
    current_screen_ids = parse_screen_ids(tour_row.get("screen_ids"))

    weekday_options = WEEKDAYS_DE
    weekday_index = weekday_options.index(tour_row["weekday"]) if tour_row["weekday"] in weekday_options else 0

    current_time = f"{int(tour_row['hour']):02d}:{int(tour_row['minute']):02d}"
    time_options = time_options_half_hour()
    time_index = time_options.index(current_time) if current_time in time_options else 0

    with st.form("edit_tour_form"):
        c1, c2, c3 = st.columns(3)
        with c1:
            edit_tour_name = st.text_input("Tour-Name", value=str(tour_row["name"]))
            edit_weekday = st.selectbox("Wochentag", weekday_options, index=weekday_index)
        with c2:
            edit_time_label = st.selectbox("Uhrzeit", time_options, index=time_index)
            edit_screens = st.multiselect("Monitore", options=screens["id"].tolist(), default=current_screen_ids)
        with c3:
            edit_countdown_enabled = st.checkbox("Countdown aktiv", value=bool(int(tour_row.get("countdown_enabled", 0) or 0)))
            edit_cooled_required = st.checkbox("Kühlware mitzunehmen", value=bool(int(tour_row.get("cooled_required", 0) or 0)))
            edit_active = st.checkbox("Aktiv", value=bool(int(tour_row.get("active", 1) or 1)))

        edit_stops = st.multiselect(
            "Stops",
            options=locations["id"].tolist(),
            default=current_stop_ids,
            format_func=lambda i: locations.loc[locations["id"] == i, "name"].values[0]
        )
        edit_note = st.text_input("Hinweis", value=str(tour_row["note"] or ""))

        csave, cdel = st.columns(2)
        save_edit = csave.form_submit_button("Tour aktualisieren")
        delete_tour = cdel.form_submit_button("Tour löschen")

    if save_edit:
        if not edit_tour_name.strip():
            st.error("Tour-Name fehlt.")
        elif not edit_screens:
            st.error("Mindestens ein Monitor muss gewählt werden.")
        elif not edit_stops:
            st.error("Mindestens ein Stop muss gewählt werden.")
        else:
            hh, mm = map(int, edit_time_label.split(":"))
            cur = conn.cursor()
            cur.execute(
                """
                UPDATE tours
                SET name=?, weekday=?, hour=?, minute=?, location_id=?, note=?, active=?, screen_ids=?, countdown_enabled=?, cooled_required=?
                WHERE id=?
                """,
                (
                    edit_tour_name.strip(),
                    edit_weekday,
                    hh,
                    mm,
                    int(edit_stops[0]),
                    edit_note.strip(),
                    1 if edit_active else 0,
                    ",".join(map(str, edit_screens)),
                    1 if edit_countdown_enabled else 0,
                    1 if edit_cooled_required else 0,
                    int(selected_tour_id),
                ),
            )

            cur.execute("DELETE FROM tour_stops WHERE tour_id=?", (int(selected_tour_id),))
            for pos, loc_id in enumerate(edit_stops):
                cur.execute("INSERT INTO tour_stops (tour_id, location_id, position) VALUES (?, ?, ?)", (int(selected_tour_id), int(loc_id), pos))

            conn.execute("DELETE FROM departures WHERE source_key LIKE ?", (f"TOUR:{int(selected_tour_id)}:%",))
            conn.commit()

            materialize_tours_to_departures(conn)
            update_departure_statuses(conn)
            save_backup_to_dir(conn)
            cleanup_old_backups()
            st.success("Tour aktualisiert.")
            st.rerun()

    if delete_tour:
        try:
            cur = conn.cursor()
            cur.execute("DELETE FROM tour_stops WHERE tour_id=?", (int(selected_tour_id),))
            cur.execute("DELETE FROM tours WHERE id=?", (int(selected_tour_id),))
            cur.execute("DELETE FROM departures WHERE source_key LIKE ?", (f"TOUR:{int(selected_tour_id)}:%",))
            conn.commit()
            save_backup_to_dir(conn)
            cleanup_old_backups()
            st.success("Tour gelöscht.")
            st.rerun()
        except Exception as e:
            st.error(str(e))


def export_holiday_tours_csv(conn):
    holiday_tours_df = read_df(conn, """
        SELECT
            h.id,
            h.name,
            h.holiday_date,
            h.hour,
            h.minute,
            h.location_id,
            l.name AS location_name,
            h.note,
            h.active,
            h.screen_ids,
            h.countdown_enabled,
            h.cooled_required
        FROM holiday_tours h
        LEFT JOIN locations l ON l.id = h.location_id
        ORDER BY
            h.holiday_date,
            h.hour,
            h.minute,
            h.name
    """)

    holiday_stops_df = read_df(conn, """
        SELECT
            hs.id,
            hs.holiday_tour_id,
            h.name AS holiday_tour_name,
            hs.position,
            hs.location_id,
            l.name AS location_name
        FROM holiday_tour_stops hs
        JOIN holiday_tours h ON h.id = hs.holiday_tour_id
        JOIN locations l ON l.id = hs.location_id
        ORDER BY
            hs.holiday_tour_id,
            hs.position
    """)

    return df_to_csv_bytes(holiday_tours_df), df_to_csv_bytes(holiday_stops_df)


def show_admin_holiday_tours(conn, can_edit: bool):
    st.subheader("Feiertagsbelieferung")
    holiday_tours = load_holiday_tours(conn)

    with st.expander("Filter / Suche", expanded=True):
        c1, c2 = st.columns(2)
        with c1:
            search_text = st.text_input("Suche Name / Hinweis", key="holiday_search_text")
        with c2:
            date_filter = st.text_input("Datum filtern (YYYY-MM-DD)", key="holiday_date_filter")

    if not holiday_tours.empty:
        view = holiday_tours.copy()

        if search_text.strip():
            q = search_text.strip().lower()
            view = view[
                view["name"].fillna("").astype(str).str.lower().str.contains(q)
                | view["note"].fillna("").astype(str).str.lower().str.contains(q)
            ]

        if date_filter.strip():
            view = view[
                view["holiday_date"].fillna("").astype(str).str.contains(date_filter.strip(), regex=False)
            ]

        view["Zeit"] = view.apply(lambda r: f"{int(r['hour']):02d}:{int(r['minute']):02d}", axis=1)

        st.dataframe(
            view[["id", "name", "holiday_date", "Zeit", "countdown_enabled", "cooled_required", "location_name", "note", "active", "screen_ids"]],
            use_container_width=True,
            height=300
        )
    else:
        st.info("Noch keine Feiertagsbelieferungen vorhanden.")

    holiday_tours_csv, holiday_stops_csv = export_holiday_tours_csv(conn)
    c1, c2 = st.columns(2)
    with c1:
        st.download_button("Feiertagsbelieferung CSV", data=holiday_tours_csv, file_name="feiertagsbelieferung.csv", mime="text/csv")
    with c2:
        st.download_button("Feiertags-Stopps CSV", data=holiday_stops_csv, file_name="feiertags_stops.csv", mime="text/csv")

    if not can_edit:
        return

    locations = load_locations(conn)
    screens = load_screens(conn)

    st.markdown("### Neue Feiertagsbelieferung")
    with st.form("new_holiday_tour_form"):
        c1, c2, c3 = st.columns(3)
        with c1:
            holiday_name = st.text_input("Name")
            holiday_date = st.date_input("Datum")
        with c2:
            holiday_time = st.selectbox("Uhrzeit", time_options_half_hour(), index=time_options_half_hour().index("08:00"))
        with c3:
            holiday_screens = st.multiselect("Monitore", options=screens["id"].tolist())
        holiday_countdown = st.checkbox("Countdown aktiv", False)
        holiday_cooled = st.checkbox("Kühlware mitzunehmen", False)
        holiday_stops = st.multiselect("Stops", options=locations["id"].tolist(), format_func=lambda i: locations.loc[locations["id"] == i, "name"].values[0])
        holiday_note = st.text_input("Hinweis")
        holiday_active = st.checkbox("Aktiv", True)
        create_holiday = st.form_submit_button("Speichern")

    if create_holiday and holiday_name.strip() and holiday_screens and holiday_stops:
        hh, mm = map(int, holiday_time.split(":"))
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO holiday_tours
            (name, holiday_date, hour, minute, location_id, note, active, screen_ids, countdown_enabled, cooled_required)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            holiday_name.strip(), holiday_date.isoformat(), hh, mm, int(holiday_stops[0]),
            holiday_note.strip(), 1 if holiday_active else 0, ",".join(map(str, holiday_screens)),
            1 if holiday_countdown else 0, 1 if holiday_cooled else 0,
        ))
        holiday_tour_id = cur.lastrowid
        for pos, loc_id in enumerate(holiday_stops):
            cur.execute("INSERT INTO holiday_tour_stops (holiday_tour_id, location_id, position) VALUES (?, ?, ?)", (holiday_tour_id, int(loc_id), pos))
        conn.commit()
        save_backup_to_dir(conn)
        cleanup_old_backups()
        st.success("Gespeichert.")
        st.rerun()

    if holiday_tours.empty:
        return

    st.markdown("### Feiertagsbelieferung bearbeiten / löschen")
    selected_holiday_id = st.selectbox(
        "Feiertagsbelieferung auswählen",
        holiday_tours["id"].tolist(),
        format_func=lambda i: f"{int(i)} – {holiday_tours.loc[holiday_tours['id'] == i, 'name'].iloc[0]}",
        key="edit_holiday_tour_select"
    )

    holiday_row = holiday_tours.loc[holiday_tours["id"] == selected_holiday_id].iloc[0]
    holiday_stops_df = load_holiday_tour_stops(conn, int(selected_holiday_id))
    current_holiday_stop_ids = holiday_stops_df["location_id"].astype(int).tolist() if not holiday_stops_df.empty else []
    current_holiday_screen_ids = parse_screen_ids(holiday_row.get("screen_ids"))

    current_holiday_time = f"{int(holiday_row['hour']):02d}:{int(holiday_row['minute']):02d}"
    time_options = time_options_half_hour()
    time_index = time_options.index(current_holiday_time) if current_holiday_time in time_options else 0

    try:
        current_holiday_date = pd.to_datetime(holiday_row["holiday_date"]).date()
    except Exception:
        current_holiday_date = now_berlin().date()

    with st.form("edit_holiday_tour_form"):
        c1, c2, c3 = st.columns(3)
        with c1:
            edit_holiday_name = st.text_input("Name", value=str(holiday_row["name"]))
            edit_holiday_date = st.date_input("Datum", value=current_holiday_date)
        with c2:
            edit_holiday_time = st.selectbox("Uhrzeit", time_options, index=time_index)
            edit_holiday_screens = st.multiselect("Monitore", options=screens["id"].tolist(), default=current_holiday_screen_ids)
        with c3:
            edit_holiday_countdown = st.checkbox("Countdown aktiv", value=bool(int(holiday_row.get("countdown_enabled", 0) or 0)))
            edit_holiday_cooled = st.checkbox("Kühlware mitzunehmen", value=bool(int(holiday_row.get("cooled_required", 0) or 0)))
            edit_holiday_active = st.checkbox("Aktiv", value=bool(int(holiday_row.get("active", 1) or 1)))

        edit_holiday_stops = st.multiselect(
            "Stops",
            options=locations["id"].tolist(),
            default=current_holiday_stop_ids,
            format_func=lambda i: locations.loc[locations["id"] == i, "name"].values[0]
        )
        edit_holiday_note = st.text_input("Hinweis", value=str(holiday_row["note"] or ""))

        csave, cdel = st.columns(2)
        save_holiday_edit = csave.form_submit_button("Feiertagsbelieferung aktualisieren")
        delete_holiday = cdel.form_submit_button("Feiertagsbelieferung löschen")

    if save_holiday_edit:
        if not edit_holiday_name.strip():
            st.error("Name fehlt.")
        elif not edit_holiday_screens:
            st.error("Mindestens ein Monitor muss gewählt werden.")
        elif not edit_holiday_stops:
            st.error("Mindestens ein Stop muss gewählt werden.")
        else:
            hh, mm = map(int, edit_holiday_time.split(":"))
            cur = conn.cursor()
            cur.execute(
                """
                UPDATE holiday_tours
                SET name=?, holiday_date=?, hour=?, minute=?, location_id=?, note=?, active=?, screen_ids=?, countdown_enabled=?, cooled_required=?
                WHERE id=?
                """,
                (
                    edit_holiday_name.strip(),
                    edit_holiday_date.isoformat(),
                    hh,
                    mm,
                    int(edit_holiday_stops[0]),
                    edit_holiday_note.strip(),
                    1 if edit_holiday_active else 0,
                    ",".join(map(str, edit_holiday_screens)),
                    1 if edit_holiday_countdown else 0,
                    1 if edit_holiday_cooled else 0,
                    int(selected_holiday_id),
                ),
            )

            cur.execute("DELETE FROM holiday_tour_stops WHERE holiday_tour_id=?", (int(selected_holiday_id),))
            for pos, loc_id in enumerate(edit_holiday_stops):
                cur.execute(
                    "INSERT INTO holiday_tour_stops (holiday_tour_id, location_id, position) VALUES (?, ?, ?)",
                    (int(selected_holiday_id), int(loc_id), pos)
                )

            conn.execute("DELETE FROM departures WHERE source_key LIKE ?", (f"HOLIDAY:{int(selected_holiday_id)}:%",))
            conn.commit()

            materialize_holiday_tours_to_departures(conn)
            update_departure_statuses(conn)
            save_backup_to_dir(conn)
            cleanup_old_backups()
            st.success("Feiertagsbelieferung aktualisiert.")
            st.rerun()

    if delete_holiday:
        try:
            cur = conn.cursor()
            cur.execute("DELETE FROM holiday_tour_stops WHERE holiday_tour_id=?", (int(selected_holiday_id),))
            cur.execute("DELETE FROM holiday_tours WHERE id=?", (int(selected_holiday_id),))
            cur.execute("DELETE FROM departures WHERE source_key LIKE ?", (f"HOLIDAY:{int(selected_holiday_id)}:%",))
            conn.commit()
            save_backup_to_dir(conn)
            cleanup_old_backups()
            st.success("Feiertagsbelieferung gelöscht.")
            st.rerun()
        except Exception as e:
            st.error(str(e))


def show_admin_cold_goods(conn, can_edit: bool):
    st.subheader("Kühlware")
    st.info("Kühlware wird direkt bei manuellen Abfahrten, Touren und Feiertagsbelieferung gepflegt.")


def show_admin_screens(conn, can_edit: bool):
    st.subheader("Screens / Monitorprofile")
    screens = load_screens(conn)
    st.dataframe(screens, use_container_width=True, height=300)

    st.markdown("### Monitore öffnen")
    button_items = []
    for _, r in screens.iterrows():
        sid = int(r["id"])
        button_items.append((f"Screen {sid} – {r['name']}", f"?mode=display&screenId={sid}"))
    button_items.extend([
        ("Split A + B", "?mode=display&screenId=101"),
        ("Split C + D", "?mode=display&screenId=102"),
        ("Split Wareneingang 1 + 2", "?mode=display&screenId=103"),
    ])

    cols = st.columns(3)
    for idx, (label, url) in enumerate(button_items):
        with cols[idx % 3]:
            st.link_button(label, url, use_container_width=True)

    if not can_edit:
        return

    sid = st.selectbox("Screen wählen", screens["id"].tolist())
    row = screens.loc[screens["id"] == sid].iloc[0]

    with st.form("edit_screen_form"):
        name = st.text_input("Name", row["name"])
        mode = st.selectbox("Modus", ["DETAIL", "OVERVIEW", "WAREHOUSE"], index=["DETAIL", "OVERVIEW", "WAREHOUSE"].index(row["mode"]))
        filter_type = st.selectbox("Filter Typ", ["ALLE", "KRANKENHAUS", "ALTENHEIM", "MVZ"], index=["ALLE", "KRANKENHAUS", "ALTENHEIM", "MVZ"].index(row["filter_type"]))
        filter_locations = st.text_input("Filter Locations", row["filter_locations"] or "")
        refresh = st.number_input("Refresh (Sek.)", min_value=5, max_value=300, value=int(row["refresh_interval_seconds"]))
        holiday = st.checkbox("Feiertagsmodus", value=bool(row["holiday_flag"]))
        special = st.checkbox("Sonderplan", value=bool(row["special_flag"]))
        ticker_text = st.text_area("Ticker-Text", value=row["text"] or "")
        ticker_active = st.checkbox("Ticker aktiv", value=bool(row["ticker_active"]))
        submitted = st.form_submit_button("Speichern")

    if submitted:
        conn.execute(
            "UPDATE screens SET name=?, mode=?, filter_type=?, filter_locations=?, refresh_interval_seconds=?, holiday_flag=?, special_flag=? WHERE id=?",
            (name, mode, filter_type, filter_locations, int(refresh), 1 if holiday else 0, 1 if special else 0, int(sid)),
        )
        conn.execute("INSERT OR REPLACE INTO tickers (screen_id, text, active) VALUES (?, ?, ?)", (int(sid), ticker_text.strip(), 1 if ticker_active else 0))
        conn.commit()
        save_backup_to_dir(conn)
        cleanup_old_backups()
        st.success("Screen gespeichert.")
        st.rerun()


def show_admin_users(conn, can_edit: bool):
    st.subheader("Benutzer")
    users = get_runtime_users()
    rows = []
    for uname, data in users.items():
        rows.append({
            "Benutzername": uname,
            "Rolle": data.get("role", "viewer"),
            "Passwort": "Hash" if str(data.get("password", "")).startswith("pbkdf2_sha256$") else "Klartext/Fallback",
        })
    st.dataframe(pd.DataFrame(rows), use_container_width=True, height=220)

    if not can_edit:
        return

    st.markdown("### Neuer Benutzer")
    with st.form("new_user_form"):
        c1, c2, c3 = st.columns(3)
        with c1:
            new_username = st.text_input("Benutzername")
        with c2:
            new_password = st.text_input("Passwort", type="password")
        with c3:
            new_role = st.selectbox("Rolle", ["viewer", "admin"])
        add_user = st.form_submit_button("Anlegen")

    if add_user:
        if not new_username.strip():
            st.error("Benutzername fehlt.")
        elif not new_password:
            st.error("Passwort fehlt.")
        elif new_username in users:
            st.error("Benutzer existiert bereits.")
        else:
            users[new_username.strip()] = {"password": hash_password(new_password), "role": new_role}
            save_runtime_users(users)
            st.success("Benutzer angelegt.")
            st.rerun()


def show_system_status(conn):
    st.subheader("Systemstatus")

    c1, c2, c3 = st.columns(3)
    with c1:
        st.metric("Datenbank OK", "Ja" if integrity_ok(conn) else "Nein")
        st.caption(str(DB_PATH))
    with c2:
        tours_count = int(read_df(conn, "SELECT COUNT(*) AS c FROM tours").iloc[0]["c"])
        loc_count = int(read_df(conn, "SELECT COUNT(*) AS c FROM locations").iloc[0]["c"])
        holiday_count = int(read_df(conn, "SELECT COUNT(*) AS c FROM holiday_tours").iloc[0]["c"])
        st.metric("Touren", tours_count)
        st.caption(f"Einrichtungen: {loc_count} • Feiertagsbelieferung: {holiday_count}")
    with c3:
        dep_count = int(read_df(conn, "SELECT COUNT(*) AS c FROM departures").iloc[0]["c"])
        st.metric("Abfahrten gesamt", dep_count)

    st.markdown("### Letzte Backups")
    backups = sorted(BACKUP_DIR.glob("backup_*.json"), key=lambda p: p.stat().st_mtime, reverse=True)[:10]
    if backups:
        rows = []
        for p in backups:
            rows.append({
                "Datei": p.name,
                "Zeit": datetime.fromtimestamp(p.stat().st_mtime, TZ).strftime("%d.%m.%Y %H:%M:%S"),
                "Größe KB": round(p.stat().st_size / 1024, 1),
            })
        st.dataframe(pd.DataFrame(rows), use_container_width=True, height=220)
    else:
        st.info("Noch keine Backups vorhanden.")

    if APP_LOG_PATH.exists():
        try:
            last_line = APP_LOG_PATH.read_text(encoding="utf-8").strip().splitlines()[-1]
            st.code(last_line, language="json")
        except Exception:
            pass


def show_admin_mode():
    require_login()
    conn = get_connection()
    cleanup_materialized_departures(conn)
    materialize_tours_to_departures(conn)
    materialize_holiday_tours_to_departures(conn)
    update_departure_statuses(conn)
    maybe_run_nightly_backup(conn)

    role = st.session_state.get("role", "viewer")
    username = st.session_state.get("username", "")
    can_edit = role == "admin"

    st.title("Abfahrten – V3.1.1")
    st.caption(f"Eingeloggt als: {username} ({role}) • DB: {DB_PATH}")

    if st.sidebar.button("Logout"):
        st.session_state.clear()
        st.rerun()

    tabs = st.tabs([
        "Abfahrten",
        "Einrichtungen",
        "Touren",
        "Feiertagsbelieferung",
        "Kühlware",
        "Screens / Monitorprofile",
        "Benutzer",
        "Backup",
        "Änderungsprotokoll",
        "Systemstatus",
    ])

    with tabs[0]:
        show_admin_departures(conn, can_edit)
    with tabs[1]:
        show_admin_locations(conn, can_edit)
    with tabs[2]:
        show_admin_tours(conn, can_edit)
    with tabs[3]:
        show_admin_holiday_tours(conn, can_edit)
    with tabs[4]:
        show_admin_cold_goods(conn, can_edit)
    with tabs[5]:
        show_admin_screens(conn, can_edit)
    with tabs[6]:
        show_admin_users(conn, can_edit)
    with tabs[7]:
        st.subheader("Backup")
        c1, c2 = st.columns(2)
        with c1:
            st.download_button(
                "Backup herunterladen",
                data=export_backup_json(conn),
                file_name=f"backup_abfahrten_{now_berlin().strftime('%Y%m%d_%H%M%S')}.json",
                mime="application/json",
            )
        with c2:
            backup_file = st.file_uploader("Backup importieren", type=["json"])
            if backup_file is not None and can_edit:
                data = json.loads(backup_file.getvalue().decode("utf-8"))
                import_backup_json(conn, data)
                save_backup_to_dir(conn, prefix="backup_import")
                cleanup_old_backups()
                st.success("Backup importiert.")
                st.rerun()
    with tabs[8]:
        st.subheader("Änderungsprotokoll")
        audit_df = read_df(conn, "SELECT event_time, username, event_type, entity_type, entity_id, details_json FROM audit_log ORDER BY id DESC LIMIT 300")
        if audit_df.empty:
            st.info("Noch keine Protokolleinträge vorhanden.")
        else:
            st.dataframe(audit_df, use_container_width=True, height=520)
    with tabs[9]:
        show_system_status(conn)


def show_display_error(message: str):
    st.markdown(
        f"""
        <div style="
            display:flex;
            justify-content:center;
            align-items:center;
            height:100vh;
            width:100%;
            background:#000;
            color:#fff;
            font-size:42px;
            font-weight:900;
            text-align:center;
            padding:40px;
        ">
            SYSTEMFEHLER – {escape_html(message)}
        </div>
        """,
        unsafe_allow_html=True,
    )


def show_display_mode(screen_id: int):
    st.markdown(base_display_css(), unsafe_allow_html=True)
    render_kiosk_hint()

    if not screen_id:
        show_display_error("ScreenId fehlt oder ist ungültig")
        return

    try:
        conn = get_connection()
        cleanup_materialized_departures(conn)
        materialize_tours_to_departures(conn)
        materialize_holiday_tours_to_departures(conn)
        update_departure_statuses(conn)
        screens = load_screens(conn)

        if int(screen_id) in COMBINED_SCREEN_MAP:
            cfg = COMBINED_SCREEN_MAP[int(screen_id)]
            st_autorefresh(interval=15000, key=f"display_refresh_combined_{screen_id}")
            render_split_screen(conn, cfg["left"], cfg["right"], cfg["name"])
            return

        if int(screen_id) in [5, 6, 7]:
            st_autorefresh(interval=15000, key=f"display_refresh_zone_overview_{screen_id}")
            render_zone_overview_screen(conn, int(screen_id))
            return

        if screens.empty or int(screen_id) not in screens["id"].tolist():
            show_display_error(f"Screen {screen_id} ist nicht konfiguriert")
            return

        screen = screens.loc[screens["id"] == int(screen_id)].iloc[0]
        st_autorefresh(interval=int(screen["refresh_interval_seconds"]) * 1000, key=f"display_refresh_{screen_id}")

        if bool(screen["holiday_flag"]) or bool(screen["special_flag"]):
            labels = []
            if bool(screen["holiday_flag"]):
                labels.append("Feiertagsbelieferung")
            if bool(screen["special_flag"]):
                labels.append("Sonderplan")
            st.markdown(
                f"<div style='display:flex;justify-content:center;align-items:center;height:100vh;background:#000;color:#fff;font-size:72px;font-weight:900;text-transform:uppercase;text-align:center;'>{' - '.join(labels)}</div>",
                unsafe_allow_html=True
            )
            return

        _, data = get_screen_data(conn, int(screen_id))
        render_display_header(f"{screen['name']} (Screen {screen_id})", data)

        if data.empty:
            st.info("Keine Abfahrten im nächsten Zeitfenster.")
        else:
            rows, row_backgrounds, text_colors, extra_css = build_display_rows(data)
            render_big_table_v2(
                ["Zeit", "Einrichtung", "Hinweis / Countdown"],
                rows,
                row_backgrounds=row_backgrounds,
                text_colors=text_colors,
                extra_row_css=extra_css,
                html_cols={2},
            )

        if bool(screen.get("ticker_active", 0)) and str(screen.get("text", "") or "").strip():
            st.markdown(f"<div class='ticker'><div class='ticker__inner'>{escape_html(screen['text'])}</div></div>", unsafe_allow_html=True)

    except Exception as e:
        log_event(None, "error", "display_mode", details={"message": str(e), "screen_id": screen_id}, level="ERROR")
        show_display_error(str(e))


def main():
    params = st.query_params
    mode = params.get("mode", "admin")
    screen_id_param = params.get("screenId", None)

    if mode == "display":
        try:
            screen_id = int(screen_id_param) if screen_id_param is not None else None
        except Exception:
            screen_id = None
        show_display_mode(screen_id)
    else:
        show_admin_mode()


if __name__ == "__main__":
    main()
