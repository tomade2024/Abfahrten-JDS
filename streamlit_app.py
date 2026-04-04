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
# Konfiguration
# ==================================================

st.set_page_config(page_title="Abfahrten", layout="wide")

APP_NAME = "Abfahrten"
TZ = ZoneInfo("Europe/Berlin")
USE_PORTABLE_MODE = False  # True = Daten im Projektordner / USB, False = AppData

DEFAULT_USERS = {
    "admin": {"password": "admin123", "role": "admin"},
    "dispo": {"password": "dispo123", "role": "viewer"},
}

WEEKDAYS_DE = ["Montag", "Dienstag", "Mittwoch", "Donnerstag", "Freitag", "Samstag", "Sonntag"]
WEEKDAY_TO_INT = {name: i for i, name in enumerate(WEEKDAYS_DE)}

COUNTDOWN_START_HOURS = 3
AUTO_COMPLETE_AFTER_MIN = 20
KEEP_COMPLETED_MINUTES = 10
MATERIALIZE_TOURS_HOURS_BEFORE = 12
DISPLAY_WINDOW_HOURS = 12

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

# ==================================================
# Pfade
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
for d in [DATA_DIR, BACKUP_DIR, EXPORT_DIR]:
    d.mkdir(parents=True, exist_ok=True)

DB_PATH = DATA_DIR / "abfahrten.db"
OLD_DB = Path("abfahrten.db")
if not DB_PATH.exists() and OLD_DB.exists() and OLD_DB.resolve() != DB_PATH.resolve():
    try:
        shutil.copy2(OLD_DB, DB_PATH)
    except Exception:
        pass

# ==================================================
# Login
# ==================================================


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
    return DEFAULT_USERS


def require_login():
    if st.session_state.get("logged_in"):
        return

    st.title("Login")
    with st.form("login_form"):
        username = st.text_input("Benutzername")
        password = st.text_input("Passwort", type="password")
        submitted = st.form_submit_button("Einloggen")

    if submitted:
        users = get_users()
        user_entry = users.get(username)
        if user_entry and user_entry["password"] == password:
            st.session_state["logged_in"] = True
            st.session_state["role"] = user_entry["role"]
            st.session_state["username"] = username
            st.success("Erfolgreich eingeloggt.")
            st.rerun()
        else:
            st.error("Benutzername oder Passwort ist falsch.")

    st.stop()

# ==================================================
# Zeit
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
    if h > 0:
        return f"{h:02d}:{m:02d}:{s:02d}"
    return f"{m:02d}:{s:02d}"


def completion_deadline(dep_dt: datetime) -> datetime:
    return dep_dt + timedelta(minutes=AUTO_COMPLETE_AFTER_MIN)


def time_options_half_hour():
    return [f"{h:02d}:{m:02d}" for h in range(24) for m in (0, 30)]

# ==================================================
# DB
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
# DB Helper
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
               t.note, t.active, t.screen_ids, t.countdown_enabled,
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
               l.name AS location_name,
               l.type AS location_type,
               l.active AS location_active,
               l.color AS location_color,
               l.text_color AS location_text_color
        FROM departures d
        JOIN locations l ON d.location_id = l.id
    """)
    if not df.empty:
        for col in ["datetime", "ready_at", "completed_at"]:
            df[col] = pd.to_datetime(df[col], errors="coerce")
            df[col] = df[col].apply(lambda x: ensure_tz(x) if pd.notnull(x) else x)
        df["countdown_enabled"] = pd.to_numeric(df["countdown_enabled"], errors="coerce").fillna(1).astype(int)
    return df


def parse_screen_ids(value):
    if value is None:
        return []
    s = str(value).strip()
    if not s:
        return []
    return [int(x.strip()) for x in s.split(",") if x.strip().isdigit()]

# ==================================================
# Backup / Export
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

    tours_df = tours[["id", "name", "weekday", "hour", "minute", "note", "active", "screen_ids", "countdown_enabled", "location_id", "location_name"]].copy()
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


def cleanup_old_backups(keep: int = 20):
    files = sorted(BACKUP_DIR.glob("backup_*.json"), key=lambda p: p.stat().st_mtime, reverse=True)
    for old in files[keep:]:
        try:
            old.unlink()
        except Exception:
            pass


def maybe_run_nightly_backup(conn):
    today = now_berlin().strftime("%Y%m%d")
    marker = BACKUP_DIR / ".last_nightly_backup.txt"
    last_done = st.session_state.get("last_nightly_backup_date")
    if last_done is None and marker.exists():
        try:
            last_done = marker.read_text(encoding="utf-8").strip()
        except Exception:
            last_done = None
    if last_done == today:
        return
    if now_berlin().hour >= 2:
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
                    "UPDATE tours SET name=?, weekday=?, hour=?, minute=?, location_id=?, note=?, active=?, screen_ids=?, countdown_enabled=? WHERE id=?",
                    (t["name"], t["weekday"], int(t.get("hour", 0)), int(t.get("minute", 0)), int(t["location_id"]), t.get("note", ""), int(t.get("active", 1)), t.get("screen_ids", ""), int(t.get("countdown_enabled", 0)), int(tour_id)),
                )
            else:
                cur.execute(
                    "INSERT INTO tours (id, name, weekday, hour, minute, location_id, note, active, screen_ids, countdown_enabled) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (int(tour_id), t["name"], t["weekday"], int(t.get("hour", 0)), int(t.get("minute", 0)), int(t["location_id"]), t.get("note", ""), int(t.get("active", 1)), t.get("screen_ids", ""), int(t.get("countdown_enabled", 0))),
                )
            cur.execute("DELETE FROM tour_stops WHERE tour_id=?", (int(tour_id),))
            for s in t.get("stops", []):
                cur.execute("INSERT INTO tour_stops (tour_id, location_id, position) VALUES (?, ?, ?)", (int(tour_id), int(s["location_id"]), int(s.get("position", 0))))

    conn.commit()

# ==================================================
# Business Logic
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


def materialize_tours_to_departures(conn: sqlite3.Connection):
    now = now_berlin()
    window = timedelta(hours=MATERIALIZE_TOURS_HOURS_BEFORE)
    df = read_df(conn, """
        SELECT t.id AS tour_id, t.weekday, t.hour, t.minute, t.note AS tour_note,
               t.active AS tour_active, t.screen_ids AS tour_screen_ids,
               t.countdown_enabled AS tour_countdown_enabled,
               ts.location_id, ts.position, l.active AS location_active
        FROM tours t
        JOIN tour_stops ts ON ts.tour_id = t.id
        JOIN locations l ON l.id = ts.location_id
    """)
    if df.empty:
        return
    df = df[(df["tour_active"] == 1) & (df["location_active"
