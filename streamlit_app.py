import streamlit as st
import pandas as pd
import sqlite3
import json
import io
import time
import uuid
import os
import shutil
from datetime import datetime, timedelta, time as dtime
from pathlib import Path
from zoneinfo import ZoneInfo

from streamlit_autorefresh import st_autorefresh

# ==================================================
# KONFIGURATION
# ==================================================

st.set_page_config(page_title="Abfahrten", layout="wide")

APP_NAME = "Abfahrten"
TZ = ZoneInfo("Europe/Berlin")
USE_PORTABLE_MODE = False  # True = Daten im Projektordner, False = AppData

DEFAULT_CONFIG = {
    "security": {
        "allow_default_users": True,
        "require_secrets_in_production": False,
    },
    "users": {
        "admin": {"password": "admin123", "role": "admin"},
        "dispo": {"password": "dispo123", "role": "viewer"},
    },
    "display": {
        "countdown_start_hours": 3,
        "auto_complete_after_min": 20,
        "keep_completed_minutes": 10,
        "materialize_tours_hours_before": 12,
        "display_window_hours": 12,
        "blink_under_minutes": 10,
        "critical_under_minutes": 5,
    },
    "backup": {
        "keep_files": 30,
        "force_daily_if_older_than_hours": 24,
    },
    "overview_groups": {
        "5": [1, 2, 3],
        "6": [4, 8, 9],
        "7": [1, 2, 3, 4, 8, 9],
    },
}

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

WEEKDAYS_DE = ["Montag", "Dienstag", "Mittwoch", "Donnerstag", "Freitag", "Samstag", "Sonntag"]
WEEKDAY_TO_INT = {name: i for i, name in enumerate(WEEKDAYS_DE)}

# ==================================================
# PFADE / CONFIG JSON
# ==================================================


def get_base_dir() -> Path:
    if USE_PORTABLE_MODE:
        base = Path(__file__).resolve().parent
    else:
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
EXPORT_DIR = BASE_DIR / "exporte"
LOG_DIR = BASE_DIR / "logs"
CONFIG_PATH = BASE_DIR / "config.json"

for d in [DATA_DIR, BACKUP_DIR, EXPORT_DIR, LOG_DIR]:
    d.mkdir(parents=True, exist_ok=True)

DB_PATH = DATA_DIR / "abfahrten.db"
OLD_DB = Path("abfahrten.db")
if not DB_PATH.exists() and OLD_DB.exists() and OLD_DB.resolve() != DB_PATH.resolve():
    try:
        shutil.copy2(OLD_DB, DB_PATH)
    except Exception:
        pass


def _deep_merge(base: dict, override: dict) -> dict:
    result = dict(base)
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(result.get(key), dict):
            result[key] = _deep_merge(result[key], value)
        else:
            result[key] = value
    return result


def load_config() -> dict:
    cfg = dict(DEFAULT_CONFIG)
    if CONFIG_PATH.exists():
        try:
            loaded = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
            if isinstance(loaded, dict):
                cfg = _deep_merge(cfg, loaded)
        except Exception:
            pass
    return cfg


APP_CONFIG = load_config()
DEFAULT_USERS = APP_CONFIG["users"]
ALLOW_DEFAULT_USERS = bool(APP_CONFIG.get("security", {}).get("allow_default_users", True))
REQUIRE_SECRETS_IN_PRODUCTION = bool(APP_CONFIG.get("security", {}).get("require_secrets_in_production", False))
APP_LOG_PATH = LOG_DIR / "app.log"

COUNTDOWN_START_HOURS = int(APP_CONFIG["display"]["countdown_start_hours"])
AUTO_COMPLETE_AFTER_MIN = int(APP_CONFIG["display"]["auto_complete_after_min"])
KEEP_COMPLETED_MINUTES = int(APP_CONFIG["display"]["keep_completed_minutes"])
MATERIALIZE_TOURS_HOURS_BEFORE = int(APP_CONFIG["display"]["materialize_tours_hours_before"])
DISPLAY_WINDOW_HOURS = int(APP_CONFIG["display"]["display_window_hours"])
BLINK_UNDER_MINUTES = int(APP_CONFIG["display"]["blink_under_minutes"])
CRITICAL_UNDER_MINUTES = int(APP_CONFIG["display"]["critical_under_minutes"])
AUTO_BACKUP_KEEP = int(APP_CONFIG["backup"]["keep_files"])
FORCE_DAILY_BACKUP_HOURS = int(APP_CONFIG["backup"]["force_daily_if_older_than_hours"])
OVERVIEW_GROUPS = {int(k): v for k, v in APP_CONFIG.get("overview_groups", {}).items()}

# ==================================================
# ZEIT / HELPERS
# ==================================================


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


def next_datetime_for_weekday_time(weekday_name: str, hour: int, minute: int) -> datetime:
    now = now_berlin()
    target = WEEKDAY_TO_INT[weekday_name]
    days_ahead = (target - now.weekday()) % 7
    candidate_date = now.date() + timedelta(days=days_ahead)
    candidate_dt = datetime.combine(candidate_date, dtime(hour=hour, minute=minute)).replace(tzinfo=TZ)
    if candidate_dt <= now:
        candidate_dt += timedelta(days=7)
    return candidate_dt


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


def parse_screen_ids(value) -> list[int]:
    if value is None:
        return []
    s = str(value).strip()
    if not s:
        return []
    return [int(x.strip()) for x in s.split(",") if x.strip().isdigit()]


def escape_html(text: str) -> str:
    return (text or "").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


# ==================================================
# LOGIN / LOGGING
# ==================================================


def log_event(conn: sqlite3.Connection | None, event_type: str, entity_type: str, entity_id=None, details: dict | None = None, level: str = "INFO"):
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
                (ts, username, event_type, entity_type, None if entity_id is None else str(entity_id), json.dumps(details or {}, ensure_ascii=False)),
            )
            conn.commit()
        except Exception:
            pass


def run_safe(label: str, func, *args, **kwargs):
    try:
        return True, func(*args, **kwargs)
    except Exception as e:
        log_event(None, "error", label, details={"message": str(e)}, level="ERROR")
        st.error(f"{label}: {e}")
        return False, None


def get_users():
    try:
        secrets_users = st.secrets.get("users", None)
        if secrets_users:
            out = {}
            for username, data in secrets_users.items():
                out[str(username)] = {
                    "password": str(data.get("password", "")),
                    "role": str(data.get("role", "viewer")),
                }
            if out:
                return out
    except Exception:
        pass

    if REQUIRE_SECRETS_IN_PRODUCTION and not ALLOW_DEFAULT_USERS:
        return {}

    return DEFAULT_USERS if ALLOW_DEFAULT_USERS else {}


def require_login():
    if st.session_state.get("logged_in"):
        return

    st.title("Login")
    users = get_users()
    if not users:
        st.error("Keine Benutzer konfiguriert. Bitte users in secrets oder config.json hinterlegen.")
        st.stop()

    with st.form("login_form"):
        username = st.text_input("Benutzername")
        password = st.text_input("Passwort", type="password")
        submitted = st.form_submit_button("Einloggen")

    if submitted:
        user_entry = users.get(username)
        if user_entry and user_entry["password"] == password:
            st.session_state["logged_in"] = True
            st.session_state["role"] = user_entry["role"]
            st.session_state["username"] = username
            st.success("Erfolgreich eingeloggt.")
            log_event(None, "login", "auth", details={"username": username})
            st.rerun()
        else:
            st.error("Benutzername oder Passwort ist falsch.")
            log_event(None, "login_failed", "auth", details={"username": username}, level="WARNING")

    st.stop()


# ==================================================
# DATENBANK
# ==================================================


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

    conn.commit()


@st.cache_resource
def get_connection():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("PRAGMA foreign_keys=ON;")
    conn.execute("PRAGMA busy_timeout=30000;")
    if not integrity_ok(conn):
        raise RuntimeError(f"Datenbank beschädigt: {DB_PATH}")
    init_db(conn)
    migrate_db(conn)
    return conn


# ==================================================
# DB HELPER
# ==================================================


def read_df(conn: sqlite3.Connection, query: str, params=()):
    return pd.read_sql_query(query, conn, params=params)


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
        df = pd.DataFrame()

    required = {
        "id": None,
        "datetime": None,
        "location_id": None,
        "vehicle": "",
        "status": "GEPLANT",
        "note": "",
        "ready_at": None,
        "completed_at": None,
        "source_key": "",
        "created_by": "",
        "screen_id": None,
        "countdown_enabled": 1,
        "cooled_required": 0,
        "location_name": "",
        "location_type": "",
        "location_active": 1,
        "location_color": "",
        "location_text_color": "",
    }

    for col, default in required.items():
        if col not in df.columns:
            df[col] = default

    if not df.empty:
        for col in ["datetime", "ready_at", "completed_at"]:
            df[col] = pd.to_datetime(df[col], errors="coerce")
            df[col] = df[col].apply(lambda x: ensure_tz(x) if pd.notnull(x) else x)

        df["countdown_enabled"] = pd.to_numeric(df["countdown_enabled"], errors="coerce").fillna(1).astype(int)
        df["cooled_required"] = pd.to_numeric(df["cooled_required"], errors="coerce").fillna(0).astype(int)

    return df


# ==================================================
# EXPORT / IMPORT / BACKUP
# ==================================================


def df_to_csv_bytes(df: pd.DataFrame) -> bytes:
    out = io.StringIO()
    df.to_csv(out, index=False, sep=";", encoding="utf-8")
    return ("\ufeff" + out.getvalue()).encode("utf-8")


def export_locations_csv(conn) -> bytes:
    return df_to_csv_bytes(load_locations(conn))


def export_tours_csv(conn) -> tuple[bytes, bytes]:
    tours = load_tours(conn)
    if tours.empty:
        return df_to_csv_bytes(pd.DataFrame()), df_to_csv_bytes(pd.DataFrame())

    tours_df = tours[["id", "name", "weekday", "hour", "minute", "note", "active", "screen_ids", "countdown_enabled", "cooled_required", "location_id", "location_name"]].copy()
    stop_rows = []
    for _, t in tours.iterrows():
        stops = load_tour_stops(conn, int(t["id"]))
        for _, s in stops.iterrows():
            stop_rows.append({
                "tour_id": int(t["id"]),
                "position": int(s["position"]),
                "location_id": int(s["location_id"]),
                "location_name": str(s["location_name"]),
            })
    stops_df = pd.DataFrame(stop_rows, columns=["tour_id", "position", "location_id", "location_name"])
    return df_to_csv_bytes(tours_df), df_to_csv_bytes(stops_df)


def export_backup_json(conn) -> bytes:
    tours = load_tours(conn)
    items = []
    for _, t in tours.iterrows():
        stops_df = load_tour_stops(conn, int(t["id"]))
        items.append({
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
    payload = {
        "version": 1,
        "exported_at": now_berlin().isoformat(),
        "locations": load_locations(conn).to_dict(orient="records"),
        "tours": items,
    }
    return json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")


def save_backup_to_dir(conn, prefix: str = "backup_auto") -> Path:
    BACKUP_DIR.mkdir(parents=True, exist_ok=True)
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
        return

    files = sorted(BACKUP_DIR.glob("backup_*.json"), key=lambda p: p.stat().st_mtime, reverse=True)
    latest = files[0] if files else None
    if latest is not None:
        age_hours = (time.time() - latest.stat().st_mtime) / 3600.0
        if age_hours >= FORCE_DAILY_BACKUP_HOURS:
            save_backup_to_dir(conn, prefix="backup_safety")
            cleanup_old_backups()


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
        else:
            cur.execute(
                "INSERT INTO locations (name, type, active, color, text_color) VALUES (?, ?, ?, ?, ?)",
                (loc["name"], loc["type"], int(loc.get("active", 1)), loc.get("color"), loc.get("text_color")),
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
                        t["name"],
                        t["weekday"],
                        int(t.get("hour", 0)),
                        int(t.get("minute", 0)),
                        int(t["location_id"]),
                        t.get("note", ""),
                        int(t.get("active", 1)),
                        t.get("screen_ids", ""),
                        int(t.get("countdown_enabled", 0)),
                        int(t.get("cooled_required", 0)),
                        int(tour_id),
                    ),
                )
            else:
                cur.execute(
                    "INSERT INTO tours (id, name, weekday, hour, minute, location_id, note, active, screen_ids, countdown_enabled, cooled_required) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        int(tour_id),
                        t["name"],
                        t["weekday"],
                        int(t.get("hour", 0)),
                        int(t.get("minute", 0)),
                        int(t["location_id"]),
                        t.get("note", ""),
                        int(t.get("active", 1)),
                        t.get("screen_ids", ""),
                        int(t.get("countdown_enabled", 0)),
                        int(t.get("cooled_required", 0)),
                    ),
                )
            cur.execute("DELETE FROM tour_stops WHERE tour_id=?", (int(tour_id),))
            for s in t.get("stops", []):
                cur.execute("INSERT INTO tour_stops (tour_id, location_id, position) VALUES (?, ?, ?)", (int(tour_id), int(s["location_id"]), int(s.get("position", 0))))

    conn.commit()


# ==================================================
# BUSINESS LOGIC
# ==================================================


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
        conn.executemany("UPDATE departures SET status='BEREIT', ready_at=COALESCE(ready_at, ?) WHERE id=?", [(now_iso, i) for i in to_ready])
    if to_done:
        conn.executemany("UPDATE departures SET status='ABGESCHLOSSEN', completed_at=COALESCE(completed_at, ?) WHERE id=?", [(now_iso, i) for i in to_done])

    if to_ready or to_done:
        conn.commit()


def cleanup_materialized_departures(conn: sqlite3.Connection):
    cutoff_old_tours = (now_berlin() - timedelta(days=2)).isoformat()
    cutoff_completed = (now_berlin() - timedelta(days=1)).isoformat()
    conn.execute("DELETE FROM departures WHERE source_key LIKE 'TOUR:%' AND datetime < ?", (cutoff_old_tours,))
    conn.execute("DELETE FROM departures WHERE status='ABGESCHLOSSEN' AND completed_at IS NOT NULL AND completed_at < ?", (cutoff_completed,))
    conn.commit()


def materialize_tours_to_departures(conn: sqlite3.Connection):
    cleanup_materialized_departures(conn)

    now = now_berlin()
    window = timedelta(hours=MATERIALIZE_TOURS_HOURS_BEFORE)
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
    created_any = False

    for _, r in df.iterrows():
        weekday = str(r["weekday"])
        if weekday not in WEEKDAYS_DE:
            continue
        screen_ids = parse_screen_ids(r["tour_screen_ids"])
        if not screen_ids:
            continue
        dep_dt = next_datetime_for_weekday_time(weekday, int(r["hour"]), int(r["minute"]))
        if dep_dt - now > window:
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
                created_any = True
            except sqlite3.IntegrityError:
                pass

    if created_any:
        conn.commit()


def create_manual_departures(conn, dep_dt: datetime, location_id: int, screen_ids: list[int], note: str, created_by: str, countdown_enabled: bool, cooled_required: bool):
    cur = conn.cursor()
    note_clean = (note or "").strip()
    created = 0

    for sid in screen_ids:
        dup = read_df(
            conn,
            """
            SELECT COUNT(*) AS c
            FROM departures
            WHERE source_key LIKE 'MANUAL:%'
              AND datetime=? AND location_id=? AND screen_id=? AND COALESCE(note,'')=?
            """,
            (dep_dt.isoformat(), int(location_id), int(sid), note_clean),
        )
        if not dup.empty and int(dup.iloc[0]["c"]) > 0:
            continue

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
        created += 1

    conn.commit()
    log_event(conn, "create", "manual_departure", details={
        "created": created,
        "datetime": dep_dt.isoformat(),
        "location_id": int(location_id),
        "screen_ids": screen_ids,
        "countdown_enabled": bool(countdown_enabled),
        "cooled_required": bool(cooled_required),
    })


def get_screen_data(conn, screen_id: int):
    screens = load_screens(conn)
    if screens.empty or "id" not in screens.columns or screen_id not in screens["id"].tolist():
        return None, pd.DataFrame()

    screen = screens.loc[screens["id"] == screen_id].iloc[0]
    now = now_berlin()
    end = now + timedelta(hours=DISPLAY_WINDOW_HOURS)
    start = now - timedelta(minutes=AUTO_COMPLETE_AFTER_MIN)

    deps = load_departures_with_locations(conn)
    required_cols = [
        "datetime", "location_active", "screen_id", "location_type", "location_id",
        "status", "completed_at", "countdown_enabled", "cooled_required", "note", "line_info",
        "location_name", "location_color", "location_text_color"
    ]

    if deps is None or deps.empty:
        return screen, pd.DataFrame(columns=required_cols)

    for col in required_cols:
        if col not in deps.columns:
            deps[col] = None

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

    if deps.empty:
        deps["line_info"] = []
        return screen, deps

    deps["datetime"] = deps["datetime"].apply(ensure_tz)
    deps = deps[(deps["datetime"] >= start) & (deps["datetime"] <= end)].copy()

    if deps.empty:
        deps["line_info"] = []
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
        deps["line_info"] = []
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

    deps["line_info"] = [build_line_info(row) for _, row in deps.iterrows()]
    if "zone_name" not in deps.columns:
        deps["zone_name"] = deps["screen_id"].apply(lambda x: ZONE_NAME_MAP.get(int(x), "") if pd.notnull(x) else "")
    deps = deps.sort_values(["datetime", "zone_name", "location_name"], na_position="last").copy()

    return screen, deps


# ==================================================
# RENDERING / HTML
# ==================================================


def is_urgent_countdown(row) -> bool:
    try:
        status = str(row.get("status") or "").upper()
        if status != "GEPLANT":
            return False
        dep_dt = ensure_tz(row.get("datetime"))
        if dep_dt is None:
            return False
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
        if dep_dt is None:
            return False
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
        elif is_critical_countdown(row) and "countdown" in line_info.lower():
            line_info_html = f"<span class='blink-countdown-critical'>{escape_html(line_info)}</span>"
        elif is_urgent_countdown(row) and "countdown" in line_info.lower():
            line_info_html = f"<span class='blink-countdown'>{escape_html(line_info)}</span>"
        parts.append(line_info_html)

    if cooled_required:
        parts.append("<span class='cold-badge'>❄ Kühlware mitzunehmen</span>")

    return " · ".join(parts)


def render_big_table(headers, rows, row_colors=None, text_colors=None, html_cols=None):
    html_cols = set(html_cols or [])
    thead = "".join(f"<th>{escape_html(str(h))}</th>" for h in headers)
    body = ""

    rows = list(rows)
    for idx, r in enumerate(rows):
        style_parts = []
        if row_colors is not None and idx < len(row_colors):
            bg = row_colors[idx] or ""
            if bg:
                style_parts.append(f"background-color:{bg};")
        if text_colors is not None and idx < len(text_colors):
            tc = text_colors[idx] or ""
            if tc:
                style_parts.append(f"color:{tc};")
        style = f' style="{"".join(style_parts)}"' if style_parts else ""

        cells = []
        for cidx, c in enumerate(r):
            if cidx in html_cols:
                cells.append(f"<td>{c or ''}</td>")
            else:
                cells.append(f"<td>{escape_html(str(c or ''))}</td>")
        body += f"<tr{style}>{''.join(cells)}</tr>"

    st.markdown(f"""
        <table class="big-table">
          <thead><tr>{thead}</tr></thead>
          <tbody>{body}</tbody>
        </table>
    """, unsafe_allow_html=True)


def render_display_header(title: str | None = None):
    now = now_berlin()
    weekday = WEEKDAYS_DE[now.weekday()]
    line = f"{weekday}, {now.strftime('%d.%m.%Y')} • {now.strftime('%H:%M:%S')}"

    if title:
        st.markdown(
            f"""
            <div style="
                display:flex;
                justify-content:space-between;
                align-items:center;
                background:#111827;
                color:white;
                padding:10px 16px;
                border-radius:14px;
                margin-bottom:10px;
                font-weight:800;
            ">
                <div style="font-size:30px;">{escape_html(title)}</div>
                <div style="font-size:26px;">{escape_html(line)}</div>
            </div>
            """,
            unsafe_allow_html=True,
        )
    else:
        st.markdown(
            f"""
            <div style="
                display:flex;
                justify-content:flex-end;
                align-items:center;
                background:#111827;
                color:white;
                padding:10px 16px;
                border-radius:14px;
                margin-bottom:10px;
                font-weight:800;
                font-size:26px;
            ">
                {escape_html(line)}
            </div>
            """,
            unsafe_allow_html=True,
        )


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
        }

        .block-container {
            max-width: 100vw !important;
            width: 100vw !important;
            height: 100vh !important;
            padding: 0.2rem 0.4rem 3rem 0.4rem !important;
            margin: 0 !important;
        }

        body,.block-container,.stMarkdown,.stText,div,span {
            font-size: 34px !important;
        }

        .big-table {
            width: 100%;
            border-collapse: collapse;
            background: #fff;
            border-radius: 14px;
            overflow: hidden;
        }

        .big-table th,.big-table td {
            border-bottom: 1px solid #d1d5db;
            padding: 0.72em 1em;
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
            0% {transform:translateX(0);}
            100% {transform:translateX(-100%);}
        }

        .blink-countdown {
            color:#b91c1c;
            font-weight:900;
            animation:blinkUrgent 1s steps(2, start) infinite;
        }

        .blink-countdown-critical {
            color:#7f1d1d;
            font-weight:900;
            background:#fecaca;
            padding:0 6px;
            border-radius:6px;
            animation:blinkCritical 0.7s steps(2, start) infinite;
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
            background:#dbeafe;
            padding:0 6px;
            border-radius:6px;
        }

        @keyframes blinkUrgent {
            0% {opacity:1;}
            50% {opacity:0.15;}
            100% {opacity:1;}
        }

        @keyframes blinkCritical {
            0% {opacity:1;}
            50% {opacity:0.05;}
            100% {opacity:1;}
        }
        </style>
    """


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
    st.markdown(
        """
        <style>
        .zone-overview-card {
            background: #111827;
            border: 2px solid #1f2937;
            border-radius: 18px;
            padding: 16px;
            margin-bottom: 18px;
            box-shadow: 0 10px 24px rgba(0,0,0,0.22);
        }
        </style>
        """,
        unsafe_allow_html=True,
    )

    screens = load_screens(conn)
    screen_row = screens.loc[screens["id"] == int(screen_id)].iloc[0]

    render_display_header(f"{screen_row['name']} (Screen {screen_id})")

    zone_ids = OVERVIEW_GROUPS.get(int(screen_id), [1, 2, 3, 4, 8, 9])
    all_rows = []
    row_colors = []
    text_colors = []

    for zid in zone_ids:
        _, zone_data = get_screen_data(conn, zid)
        zone_name = ZONE_NAME_MAP.get(zid, f"Zone {zid}")

        if zone_data is None or zone_data.empty:
            continue

        for _, r in zone_data.iterrows():
            info_html = build_info_html(r)
            all_rows.append([
                ensure_tz(r["datetime"]).strftime("%H:%M"),
                r["location_name"],
                zone_name,
                info_html,
            ])
            row_colors.append(r.get("location_color") or "")
            text_colors.append(r.get("location_text_color") or "")

    st.markdown("<div class='zone-overview-card'>", unsafe_allow_html=True)

    if not all_rows:
        st.info("Keine Abfahrten im Zeitfenster.")
    else:
        all_rows = sorted(all_rows, key=lambda x: (x[0], x[2], x[1]))
        render_big_table(
            ["Zeit", "Einrichtung", "Zone", "Hinweis / Countdown"],
            all_rows,
            row_colors=row_colors,
            text_colors=text_colors,
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

    st.markdown("""
        <style>
        .split-monitor-card {
            background: #111827;
            padding: 8px 12px 10px 12px;
            min-height: 90vh;
            border: 3px solid #1f2937;
            border-radius: 16px;
            box-shadow: 0 18px 45px rgba(0,0,0,0.28);
        }
        .split-divider {
            min-height: 90vh;
            width: 1px;
            background: #d1d5db;
            border-radius: 1px;
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
        </style>
    """, unsafe_allow_html=True)

    render_display_header(title)

    col1, colmid, col2 = st.columns([1, 0.0006, 1])

    with col1:
        st.markdown("<div class='split-monitor-card'>", unsafe_allow_html=True)
        st.markdown(f"<div class='split-zone-title'>{escape_html(left_zone_name)}</div>", unsafe_allow_html=True)

        if left_data is None or left_data.empty:
            st.markdown("<div class='split-empty'>Keine Abfahrten im Zeitfenster.</div>", unsafe_allow_html=True)
        else:
            rows, row_colors, text_colors = [], [], []
            for _, r in left_data.iterrows():
                info_html = build_info_html(r)
                rows.append([
                    ensure_tz(r["datetime"]).strftime("%H:%M"),
                    r["location_name"],
                    info_html,
                ])
                row_colors.append(r.get("location_color") or "")
                text_colors.append(r.get("location_text_color") or "")
            render_big_table(
                ["Zeit", "Einrichtung", "Hinweis / Countdown"],
                rows,
                row_colors=row_colors,
                text_colors=text_colors,
                html_cols={2},
            )
        st.markdown("</div>", unsafe_allow_html=True)

    with colmid:
        st.markdown("<div class='split-divider'></div>", unsafe_allow_html=True)

    with col2:
        st.markdown("<div class='split-monitor-card'>", unsafe_allow_html=True)
        st.markdown(f"<div class='split-zone-title'>{escape_html(right_zone_name)}</div>", unsafe_allow_html=True)

        if right_data is None or right_data.empty:
            st.markdown("<div class='split-empty'>Keine Abfahrten im Zeitfenster.</div>", unsafe_allow_html=True)
        else:
            rows, row_colors, text_colors = [], [], []
            for _, r in right_data.iterrows():
                info_html = build_info_html(r)
                rows.append([
                    ensure_tz(r["datetime"]).strftime("%H:%M"),
                    r["location_name"],
                    info_html,
                ])
                row_colors.append(r.get("location_color") or "")
                text_colors.append(r.get("location_text_color") or "")
            render_big_table(
                ["Zeit", "Einrichtung", "Hinweis / Countdown"],
                rows,
                row_colors=row_colors,
                text_colors=text_colors,
                html_cols={2},
            )
        st.markdown("</div>", unsafe_allow_html=True)

    ticker_text = get_combined_ticker_text(conn, [left_screen_id, right_screen_id])
    if ticker_text:
        st.markdown(
            f"<div class='ticker'><div class='ticker__inner'>{escape_html(ticker_text)}</div></div>",
            unsafe_allow_html=True
        )


# ==================================================
# ADMIN VIEWS
# ==================================================


def show_admin_departures(conn, can_edit: bool):
    st.subheader("Abfahrten")
    materialize_tours_to_departures(conn)
    update_departure_statuses(conn)

    deps = load_departures_with_locations(conn).sort_values("datetime")
    if deps.empty:
        st.info("Noch keine Abfahrten vorhanden.")
    else:
        view = deps.copy()
        view["Quelle"] = view["source_key"].astype(str).apply(lambda s: "TOUR" if s.startswith("TOUR:") else ("MANUELL" if s.startswith("MANUAL:") else "SONST"))
        view["Zeit"] = view["datetime"].apply(lambda d: ensure_tz(d).strftime("%d.%m.%Y %H:%M") if pd.notnull(d) else "")
        st.dataframe(view[["Zeit", "screen_id", "location_name", "note", "status", "countdown_enabled", "cooled_required", "Quelle"]], use_container_width=True, height=360)

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
        ok, _ = run_safe(
            "Manuelle Abfahrt speichern",
            create_manual_departures,
            conn,
            dep_dt,
            int(loc_id),
            [int(s) for s in screen_ids],
            note,
            str(st.session_state.get("username") or "ADMIN"),
            countdown_enabled,
            cooled_required,
        )
        if ok:
            save_backup_to_dir(conn)
            cleanup_old_backups()
            st.success("Gespeichert.")
            st.rerun()

    st.markdown("### Manuelle Abfahrt löschen")
    manual = deps[deps["source_key"].astype(str).str.startswith("MANUAL:")].copy() if not deps.empty else pd.DataFrame()
    if manual.empty:
        st.info("Keine manuellen Abfahrten vorhanden.")
    else:
        manual["label"] = manual.apply(lambda r: f"ID {int(r['id'])} • {ensure_tz(r['datetime']).strftime('%d.%m.%Y %H:%M')} • Screen {r['screen_id']} • {r['location_name']}", axis=1)
        selected_label = st.selectbox("Manuelle Abfahrt auswählen", manual["label"].tolist(), key="delete_manual_dep_select")
        selected_id = int(manual.loc[manual["label"] == selected_label, "id"].iloc[0])
        if st.button("Ausgewählte manuelle Abfahrt löschen"):
            try:
                conn.execute("DELETE FROM departures WHERE id=? AND source_key LIKE 'MANUAL:%'", (selected_id,))
                conn.commit()
                log_event(conn, "delete", "manual_departure", entity_id=selected_id)
                save_backup_to_dir(conn)
                cleanup_old_backups()
                st.success("Manuelle Abfahrt gelöscht.")
                st.rerun()
            except Exception as e:
                log_event(conn, "error", "manual_departure", entity_id=selected_id, details={"message": str(e)}, level="ERROR")
                st.error(f"Löschen fehlgeschlagen: {e}")


def show_admin_locations(conn, can_edit: bool):
    st.subheader("Einrichtungen")
    locations = load_locations(conn)
    st.dataframe(locations, use_container_width=True, height=300)
    st.download_button("Einrichtungen als CSV", data=export_locations_csv(conn), file_name="einrichtungen.csv", mime="text/csv")

    if not can_edit:
        return

    st.markdown("### Neue Einrichtung")
    with st.form("new_location"):
        c1, c2, c3, c4 = st.columns([1.5, 1, 1, 1])
        with c1:
            name = st.text_input("Name")
        with c2:
            typ = st.selectbox("Typ", ["KRANKENHAUS", "ALTENHEIM", "MVZ"])
        with c3:
            active = st.checkbox("Aktiv", True)
        with c4:
            st.write("&nbsp;")
            st.write("&nbsp;")
            st.caption("Farben unten")
        c5, c6 = st.columns(2)
        with c5:
            color = st.color_picker("Hintergrundfarbe", "#007bff")
        with c6:
            text_color = st.color_picker("Schriftfarbe", "#000000")
        st.markdown(f"<div style='background:{color};color:{text_color};padding:10px;border-radius:8px;font-weight:700;'>Vorschau Einrichtung</div>", unsafe_allow_html=True)
        submitted = st.form_submit_button("Speichern")

    if submitted and name.strip():
        try:
            conn.execute("INSERT INTO locations (name, type, active, color, text_color) VALUES (?, ?, ?, ?, ?)", (name.strip(), typ, 1 if active else 0, color, text_color))
            conn.commit()
            new_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
            log_event(conn, "create", "location", entity_id=new_id, details={"name": name.strip(), "type": typ})
            save_backup_to_dir(conn)
            cleanup_old_backups()
            st.success("Einrichtung gespeichert.")
            st.rerun()
        except Exception as e:
            log_event(conn, "error", "location", details={"message": str(e)}, level="ERROR")
            st.error(f"Speichern fehlgeschlagen: {e}")

    if locations.empty:
        return

    st.markdown("### Einrichtung bearbeiten / löschen")
    selected = st.selectbox("Einrichtung auswählen", locations["id"].tolist(), key="edit_location_select")
    row = locations.loc[locations["id"] == selected].iloc[0]
    with st.form("edit_location"):
        c1, c2, c3 = st.columns(3)
        with c1:
            edit_name = st.text_input("Name", row["name"])
            edit_type = st.selectbox("Typ", ["KRANKENHAUS", "ALTENHEIM", "MVZ"], index=["KRANKENHAUS", "ALTENHEIM", "MVZ"].index(row["type"]) if row["type"] in ["KRANKENHAUS", "ALTENHEIM", "MVZ"] else 0)
        with c2:
            edit_active = st.checkbox("Aktiv", bool(row["active"]))
        with c3:
            edit_color = st.color_picker("Hintergrundfarbe", row["color"] if row["color"] else "#007bff")
            edit_text_color = st.color_picker("Schriftfarbe", row["text_color"] if row["text_color"] else "#000000")
        st.markdown(f"<div style='background:{edit_color};color:{edit_text_color};padding:10px;border-radius:8px;font-weight:700;'>Vorschau Einrichtung</div>", unsafe_allow_html=True)
        csave, cdel = st.columns(2)
        save = csave.form_submit_button("Änderungen speichern")
        delete = cdel.form_submit_button("Einrichtung löschen")
    if save and edit_name.strip():
        try:
            conn.execute("UPDATE locations SET name=?, type=?, active=?, color=?, text_color=? WHERE id=?", (edit_name.strip(), edit_type, 1 if edit_active else 0, edit_color, edit_text_color, int(selected)))
            conn.commit()
            log_event(conn, "update", "location", entity_id=int(selected), details={"name": edit_name.strip(), "type": edit_type, "active": bool(edit_active)})
            save_backup_to_dir(conn)
            cleanup_old_backups()
            st.success("Einrichtung aktualisiert.")
            st.rerun()
        except Exception as e:
            log_event(conn, "error", "location", entity_id=int(selected), details={"message": str(e)}, level="ERROR")
            st.error(f"Aktualisieren fehlgeschlagen: {e}")
    if delete:
        dep_count = read_df(conn, "SELECT COUNT(*) AS c FROM departures WHERE location_id=?", (int(selected),)).iloc[0]["c"]
        tour_count = read_df(conn, "SELECT COUNT(*) AS c FROM tours WHERE location_id=?", (int(selected),)).iloc[0]["c"]
        stop_count = read_df(conn, "SELECT COUNT(*) AS c FROM tour_stops WHERE location_id=?", (int(selected),)).iloc[0]["c"]
        if dep_count or tour_count or stop_count:
            st.error(f"Kann nicht löschen: Abfahrten={dep_count}, Touren={tour_count}, Stops={stop_count}")
        else:
            conn.execute("DELETE FROM locations WHERE id=?", (int(selected),))
            conn.commit()
            log_event(conn, "delete", "location", entity_id=int(selected), details={"name": str(row['name'])})
            save_backup_to_dir(conn)
            cleanup_old_backups()
            st.success("Einrichtung gelöscht.")
            st.rerun()


def show_admin_tours(conn, can_edit: bool):
    st.subheader("Touren")
    tours_csv, tour_stops_csv = export_tours_csv(conn)
    c1, c2 = st.columns(2)
    with c1:
        st.download_button("Touren als CSV", data=tours_csv, file_name="touren.csv", mime="text/csv")
    with c2:
        st.download_button("Tour-Stopps als CSV", data=tour_stops_csv, file_name="tour_stops.csv", mime="text/csv")

    tours = load_tours(conn)
    if not tours.empty:
        view = tours.copy()
        view["Zeit"] = view.apply(lambda r: f"{int(r['hour']):02d}:{int(r['minute']):02d}", axis=1)
        st.dataframe(view[["id", "name", "weekday", "Zeit", "countdown_enabled", "cooled_required", "location_name", "note", "active", "screen_ids"]], use_container_width=True, height=300)
    else:
        st.info("Noch keine Touren vorhanden.")

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
        stops_new = st.multiselect("Stops", options=locations["id"].tolist(), format_func=lambda i: locations.loc[locations["id"] == i, "name"].values[0])
        note_new = st.text_input("Hinweis")
        active_new = st.checkbox("Aktiv", True)
        submitted = st.form_submit_button("Tour speichern")
    if submitted and tour_name.strip() and screens_new and stops_new:
        try:
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
            log_event(conn, "create", "tour", entity_id=tour_id, details={"name": tour_name.strip(), "weekday": weekday, "screens": screens_new, "stops": stops_new, "cooled_required": bool(cooled_required)})
            save_backup_to_dir(conn)
            cleanup_old_backups()
            st.success("Tour gespeichert.")
            st.rerun()
        except Exception as e:
            log_event(conn, "error", "tour", details={"message": str(e)}, level="ERROR")
            st.error(f"Tour speichern fehlgeschlagen: {e}")

    if tours.empty:
        return

    st.markdown("### Tour bearbeiten / löschen")
    selected = st.selectbox("Tour auswählen", tours["id"].tolist(), key="edit_tour_select")
    row = tours.loc[tours["id"] == selected].iloc[0]
    stops_df = load_tour_stops(conn, int(selected))
    default_stops = stops_df["location_id"].tolist() if not stops_df.empty else [int(row["location_id"])]
    default_screens = parse_screen_ids(row["screen_ids"])
    current_time = f"{int(row['hour']):02d}:{int(row['minute']):02d}"
    if current_time not in time_options_half_hour():
        current_time = "08:00"

    with st.form("edit_tour_form"):
        c1, c2, c3 = st.columns(3)
        with c1:
            edit_name = st.text_input("Tour-Name", row["name"])
            edit_weekday = st.selectbox("Wochentag", WEEKDAYS_DE, index=WEEKDAYS_DE.index(row["weekday"]) if row["weekday"] in WEEKDAYS_DE else 0)
        with c2:
            edit_time = st.selectbox("Uhrzeit", time_options_half_hour(), index=time_options_half_hour().index(current_time))
        with c3:
            edit_screens = st.multiselect("Monitore", options=screens["id"].tolist(), default=default_screens)
        edit_countdown = st.checkbox("Countdown aktiv", bool(int(row["countdown_enabled"])))
        edit_cooled_required = st.checkbox("Kühlware mitzunehmen", bool(int(row.get("cooled_required", 0) or 0)))
        edit_stops = st.multiselect("Stops", options=locations["id"].tolist(), default=default_stops, format_func=lambda i: locations.loc[locations["id"] == i, "name"].values[0])
        edit_note = st.text_input("Hinweis", row["note"] or "")
        edit_active = st.checkbox("Aktiv", bool(row["active"]))
        csave, cdel = st.columns(2)
        save = csave.form_submit_button("Änderungen speichern")
        delete = cdel.form_submit_button("Tour löschen")
    if save and edit_name.strip() and edit_screens and edit_stops:
        try:
            hh, mm = map(int, edit_time.split(":"))
            cur = conn.cursor()
            cur.execute(
                "UPDATE tours SET name=?, weekday=?, hour=?, minute=?, location_id=?, note=?, active=?, screen_ids=?, countdown_enabled=?, cooled_required=? WHERE id=?",
                (
                    edit_name.strip(),
                    edit_weekday,
                    hh,
                    mm,
                    int(edit_stops[0]),
                    edit_note.strip(),
                    1 if edit_active else 0,
                    ",".join(map(str, edit_screens)),
                    1 if edit_countdown else 0,
                    1 if edit_cooled_required else 0,
                    int(selected),
                ),
            )
            cur.execute("DELETE FROM tour_stops WHERE tour_id=?", (int(selected),))
            for pos, loc_id in enumerate(edit_stops):
                cur.execute("INSERT INTO tour_stops (tour_id, location_id, position) VALUES (?, ?, ?)", (int(selected), int(loc_id), pos))
            cur.execute("DELETE FROM departures WHERE source_key LIKE ?", (f"TOUR:{int(selected)}:%",))
            conn.commit()
            log_event(conn, "update", "tour", entity_id=int(selected), details={"name": edit_name.strip(), "weekday": edit_weekday, "screens": edit_screens, "stops": edit_stops, "cooled_required": bool(edit_cooled_required)})
            save_backup_to_dir(conn)
            cleanup_old_backups()
            st.success("Tour aktualisiert.")
            st.rerun()
        except Exception as e:
            log_event(conn, "error", "tour", entity_id=int(selected), details={"message": str(e)}, level="ERROR")
            st.error(f"Tour aktualisieren fehlgeschlagen: {e}")
    if delete:
        try:
            cur = conn.cursor()
            cur.execute("DELETE FROM departures WHERE source_key LIKE ?", (f"TOUR:{int(selected)}:%",))
            cur.execute("DELETE FROM tour_stops WHERE tour_id=?", (int(selected),))
            cur.execute("DELETE FROM tours WHERE id=?", (int(selected),))
            conn.commit()
            log_event(conn, "delete", "tour", entity_id=int(selected), details={"name": str(row['name'])})
            save_backup_to_dir(conn)
            cleanup_old_backups()
            st.success("Tour gelöscht.")
            st.rerun()
        except Exception as e:
            log_event(conn, "error", "tour", entity_id=int(selected), details={"message": str(e)}, level="ERROR")
            st.error(f"Tour löschen fehlgeschlagen: {e}")


def show_admin_cold_goods(conn, can_edit: bool):
    st.subheader("Kühlware")

    if not can_edit:
        st.info("Nur Admin kann Kühlware bearbeiten.")
        return

    deps = load_departures_with_locations(conn).sort_values("datetime")
    tours = load_tours(conn)

    tab1, tab2 = st.tabs(["Manuelle / kommende Abfahrten", "Touren"])

    with tab1:
        if deps.empty:
            st.info("Keine Abfahrten vorhanden.")
        else:
            upcoming = deps.copy()
            upcoming["label"] = upcoming.apply(
                lambda r: f"{ensure_tz(r['datetime']).strftime('%d.%m.%Y %H:%M')} • Screen {r['screen_id']} • {r['location_name']}",
                axis=1
            )

            selected = st.selectbox("Abfahrt auswählen", upcoming["label"].tolist(), key="cold_dep_select")
            row = upcoming.loc[upcoming["label"] == selected].iloc[0]
            dep_id = int(row["id"])
            current_value = bool(int(row.get("cooled_required", 0) or 0))

            new_value = st.checkbox("Kühlware mitzunehmen", value=current_value, key=f"cold_dep_chk_{dep_id}")

            if st.button("Kühlware für Abfahrt speichern"):
                try:
                    conn.execute(
                        "UPDATE departures SET cooled_required=? WHERE id=?",
                        (1 if new_value else 0, dep_id)
                    )
                    conn.commit()
                    log_event(conn, "update", "departure_cold_goods", entity_id=dep_id, details={"cooled_required": bool(new_value)})
                    st.success("Kühlware aktualisiert.")
                    st.rerun()
                except Exception as e:
                    log_event(conn, "error", "departure_cold_goods", entity_id=dep_id, details={"message": str(e)}, level="ERROR")
                    st.error(f"Speichern fehlgeschlagen: {e}")

    with tab2:
        if tours.empty:
            st.info("Keine Touren vorhanden.")
        else:
            tours["label"] = tours.apply(
                lambda r: f"ID {int(r['id'])} • {r['name']} • {r['weekday']} {int(r['hour']):02d}:{int(r['minute']):02d}",
                axis=1
            )

            selected_tour = st.selectbox("Tour auswählen", tours["label"].tolist(), key="cold_tour_select")
            row = tours.loc[tours["label"] == selected_tour].iloc[0]
            tour_id = int(row["id"])
            current_value = bool(int(row.get("cooled_required", 0) or 0))

            new_value = st.checkbox("Kühlware mitzunehmen", value=current_value, key=f"cold_tour_chk_{tour_id}")

            if st.button("Kühlware für Tour speichern"):
                try:
                    conn.execute(
                        "UPDATE tours SET cooled_required=? WHERE id=?",
                        (1 if new_value else 0, tour_id)
                    )
                    conn.commit()
                    log_event(conn, "update", "tour_cold_goods", entity_id=tour_id, details={"cooled_required": bool(new_value)})
                    st.success("Kühlware bei Tour aktualisiert.")
                    st.rerun()
                except Exception as e:
                    log_event(conn, "error", "tour_cold_goods", entity_id=tour_id, details={"message": str(e)}, level="ERROR")
                    st.error(f"Speichern fehlgeschlagen: {e}")


def show_admin_screens(conn, can_edit: bool):
    st.subheader("Screens / Ticker")
    screens = load_screens(conn)
    st.dataframe(screens, use_container_width=True, height=300)

    st.markdown("### Monitore öffnen")
    button_items = []
    for _, r in screens.iterrows():
        sid = int(r["id"])
        name = str(r["name"])
        button_items.append((f"Screen {sid} – {name}", f"?mode=display&screenId={sid}"))
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
    with st.form("edit_screen"):
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
        try:
            conn.execute("UPDATE screens SET name=?, mode=?, filter_type=?, filter_locations=?, refresh_interval_seconds=?, holiday_flag=?, special_flag=? WHERE id=?", (name, mode, filter_type, filter_locations, int(refresh), 1 if holiday else 0, 1 if special else 0, int(sid)))
            conn.execute("INSERT OR REPLACE INTO tickers (screen_id, text, active) VALUES (?, ?, ?)", (int(sid), ticker_text.strip(), 1 if ticker_active else 0))
            conn.commit()
            log_event(conn, "update", "screen", entity_id=int(sid), details={"name": name, "mode": mode, "refresh": int(refresh), "ticker_active": bool(ticker_active)})
            save_backup_to_dir(conn)
            cleanup_old_backups()
            st.success("Screen gespeichert.")
            st.rerun()
        except Exception as e:
            log_event(conn, "error", "screen", entity_id=int(sid), details={"message": str(e)}, level="ERROR")
            st.error(f"Screen speichern fehlgeschlagen: {e}")


def show_admin_mode():
    require_login()
    conn = get_connection()
    materialize_tours_to_departures(conn)
    update_departure_statuses(conn)
    maybe_run_nightly_backup(conn)

    role = st.session_state.get("role", "viewer")
    username = st.session_state.get("username", "")

    st.title("Abfahrten – Admin / Disposition")
    st.caption(f"Eingeloggt als: {username} ({role}) • DB: {DB_PATH}")

    if st.sidebar.button("Logout"):
        log_event(conn, "logout", "auth", details={"username": username})
        st.session_state.clear()
        st.rerun()

    can_edit = role == "admin"

    tabs = st.tabs([
        "Abfahrten",
        "Einrichtungen",
        "Touren",
        "Kühlware",
        "Screens / Ticker",
        "Wartung",
        "Backup",
        "Änderungsprotokoll",
    ])

    with tabs[0]:
        show_admin_departures(conn, can_edit)

    with tabs[1]:
        show_admin_locations(conn, can_edit)

    with tabs[2]:
        show_admin_tours(conn, can_edit)

    with tabs[3]:
        show_admin_cold_goods(conn, can_edit)

    with tabs[4]:
        show_admin_screens(conn, can_edit)

    with tabs[5]:
        st.subheader("Wartung")
        c1, c2, c3 = st.columns(3)

        with c1:
            if st.button("DB prüfen"):
                ok = integrity_ok(conn)
                log_event(conn, "maintenance_check", "database", details={"integrity_ok": ok})
                if ok:
                    st.success("Datenbank ist in Ordnung.")
                else:
                    st.error("Datenbankprüfung fehlgeschlagen.")

        with c2:
            if st.button("Alte Tour-Abfahrten bereinigen"):
                try:
                    cleanup_materialized_departures(conn)
                    log_event(conn, "maintenance_cleanup", "departures")
                    st.success("Bereinigung durchgeführt.")
                except Exception as e:
                    log_event(conn, "error", "maintenance_cleanup", details={"message": str(e)}, level="ERROR")
                    st.error(f"Bereinigung fehlgeschlagen: {e}")

        with c3:
            if st.button("Sofort-Backup erstellen"):
                try:
                    target = save_backup_to_dir(conn, prefix="backup_manual")
                    cleanup_old_backups()
                    log_event(conn, "maintenance_backup", "backup", details={"path": str(target)})
                    st.success(f"Backup erstellt: {target.name}")
                except Exception as e:
                    log_event(conn, "error", "backup", details={"message": str(e)}, level="ERROR")
                    st.error(f"Backup fehlgeschlagen: {e}")

    with tabs[6]:
        st.subheader("Backup")
        c1, c2 = st.columns(2)

        with c1:
            st.download_button(
                "Backup herunterladen",
                data=export_backup_json(conn),
                file_name=f"backup_abfahrten_{now_berlin().strftime('%Y%m%d_%H%M%S')}.json",
                mime="application/json"
            )
            st.caption(f"Backup-Ordner: {BACKUP_DIR}")

        with c2:
            backup_file = st.file_uploader("Backup importieren (JSON)", type=["json"], key="backup_import_main")
            if backup_file is not None and role == "admin":
                try:
                    data = json.loads(backup_file.getvalue().decode("utf-8"))
                    import_backup_json(conn, data)
                    save_backup_to_dir(conn, prefix="backup_import")
                    cleanup_old_backups()
                    log_event(conn, "import", "backup", details={"filename": backup_file.name})
                    st.success("Backup importiert.")
                    st.rerun()
                except Exception as e:
                    log_event(conn, "error", "backup_import", details={"message": str(e)}, level="ERROR")
                    st.error(f"Backup-Import fehlgeschlagen: {e}")

    with tabs[7]:
        st.subheader("Änderungsprotokoll")
        audit_df = read_df(
            conn,
            "SELECT event_time, username, event_type, entity_type, entity_id, details_json FROM audit_log ORDER BY id DESC LIMIT 200"
        )
        if audit_df.empty:
            st.info("Noch keine Protokolleinträge vorhanden.")
        else:
            st.dataframe(audit_df, use_container_width=True, height=500)


# ==================================================
# DISPLAY MODE
# ==================================================


def show_display_mode(screen_id: int):
    st.markdown(base_display_css(), unsafe_allow_html=True)

    if not screen_id:
        st.error("Parameter 'screenId' fehlt oder ist ungültig.")
        return

    conn = get_connection()
    materialize_tours_to_departures(conn)
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
        st.error(f"Screen {screen_id} ist nicht konfiguriert.")
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

    render_display_header(f"{screen['name']} (Screen {screen_id})")

    _, data = get_screen_data(conn, int(screen_id))
    if data.empty:
        st.info("Keine Abfahrten im nächsten Zeitfenster.")
    else:
        rows, row_colors, text_colors = [], [], []
        for _, r in data.iterrows():
            info_html = build_info_html(r)
            rows.append([
                ensure_tz(r["datetime"]).strftime("%H:%M"),
                r["location_name"],
                info_html,
            ])
            row_colors.append(r.get("location_color") or "")
            text_colors.append(r.get("location_text_color") or "")
        render_big_table(["Zeit", "Einrichtung", "Hinweis / Countdown"], rows, row_colors=row_colors, text_colors=text_colors, html_cols={2})

    if bool(screen.get("ticker_active", 0)) and str(screen.get("text", "") or "").strip():
        st.markdown(f"<div class='ticker'><div class='ticker__inner'>{escape_html(screen['text'])}</div></div>", unsafe_allow_html=True)


# ==================================================
# MAIN
# ==================================================


def main():
    params = st.query_params
    mode = params.get("mode", "admin")
    screen_id_param = params.get("screenId", None)

    if mode == "display":
        try:
            screen_id = int(screen_id_param) if screen_id_param is not None else None
        except (ValueError, TypeError):
            screen_id = None
        show_display_mode(screen_id)
    else:
        show_admin_mode()


if __name__ == "__main__":
    main()
