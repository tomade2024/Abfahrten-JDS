import streamlit as st
import pandas as pd
import sqlite3
import json
import io
import time
import uuid
import os
import shutil
from contextlib import contextmanager
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

USE_PORTABLE_MODE = False  # True = Daten im Projektordner / USB-Stick, False = AppData/Local

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

ZONE_SCREEN_IDS = [1, 2, 3, 4, 8, 9]
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


def get_base_dir() -> Path:
    if USE_PORTABLE_MODE:
        return Path(__file__).resolve().parent

    local_appdata = os.getenv("LOCALAPPDATA")
    if local_appdata:
        app_dir = Path(local_appdata) / APP_NAME
    else:
        app_dir = Path.home() / f".{APP_NAME.lower()}"
    app_dir.mkdir(parents=True, exist_ok=True)
    return app_dir


BASE_DIR = get_base_dir()
DATA_DIR = BASE_DIR / "daten"
BACKUP_DIR = BASE_DIR / "backups"
EXPORT_DIR = BASE_DIR / "exporte"

DATA_DIR.mkdir(parents=True, exist_ok=True)
BACKUP_DIR.mkdir(parents=True, exist_ok=True)
EXPORT_DIR.mkdir(parents=True, exist_ok=True)

DB_PATH = DATA_DIR / "abfahrten.db"

# Alte DB aus dem Arbeitsordner einmalig übernehmen
OLD_DB = Path("abfahrten.db")
if not DB_PATH.exists() and OLD_DB.exists() and OLD_DB.resolve() != DB_PATH.resolve():
    try:
        shutil.copy2(OLD_DB, DB_PATH)
    except Exception:
        pass


# ==================================================
# Benutzer / Login
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
# Zeit Helpers
# ==================================================

def now_berlin() -> datetime:
    return datetime.now(TZ)


def ensure_tz(dt: datetime | None):
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=TZ)
    return dt.astimezone(TZ)


def next_datetime_for_weekday_time(weekday_name: str, hour: int, minute: int) -> datetime:
    now = now_berlin()
    target = WEEKDAY_TO_INT[weekday_name]
    today = now.weekday()
    days_ahead = (target - today) % 7
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
# DB / Migration
# ==================================================

def integrity_ok(conn: sqlite3.Connection) -> bool:
    try:
        r = conn.execute("PRAGMA integrity_check;").fetchone()
        if not r:
            return True
        return str(r[0]).lower() == "ok"
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

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS locations (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            name        TEXT NOT NULL,
            type        TEXT NOT NULL,
            active      INTEGER NOT NULL DEFAULT 1,
            color       TEXT,
            text_color  TEXT
        )
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS departures (
            id                 INTEGER PRIMARY KEY AUTOINCREMENT,
            datetime           TEXT NOT NULL,
            location_id        INTEGER NOT NULL,
            vehicle            TEXT,
            status             TEXT NOT NULL DEFAULT 'GEPLANT',
            note               TEXT,
            ready_at           TEXT,
            completed_at       TEXT,
            source_key         TEXT,
            created_by         TEXT,
            screen_id          INTEGER,
            countdown_enabled  INTEGER NOT NULL DEFAULT 1,
            FOREIGN KEY(location_id) REFERENCES locations(id)
        )
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS tours (
            id                 INTEGER PRIMARY KEY AUTOINCREMENT,
            name               TEXT NOT NULL,
            weekday            TEXT NOT NULL,
            hour               INTEGER NOT NULL,
            minute             INTEGER NOT NULL DEFAULT 0,
            location_id        INTEGER NOT NULL,
            note               TEXT,
            active             INTEGER NOT NULL DEFAULT 1,
            screen_ids         TEXT,
            countdown_enabled  INTEGER NOT NULL DEFAULT 0,
            FOREIGN KEY(location_id) REFERENCES locations(id)
        )
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS tour_stops (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            tour_id     INTEGER NOT NULL,
            location_id INTEGER NOT NULL,
            position    INTEGER NOT NULL DEFAULT 0,
            FOREIGN KEY(tour_id) REFERENCES tours(id),
            FOREIGN KEY(location_id) REFERENCES locations(id)
        )
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS screens (
            id                        INTEGER PRIMARY KEY,
            name                      TEXT NOT NULL,
            mode                      TEXT NOT NULL,
            filter_type               TEXT NOT NULL DEFAULT 'ALLE',
            filter_locations          TEXT,
            refresh_interval_seconds  INTEGER NOT NULL DEFAULT 30,
            holiday_flag              INTEGER NOT NULL DEFAULT 0,
            special_flag              INTEGER NOT NULL DEFAULT 0
        )
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS tickers (
            screen_id INTEGER PRIMARY KEY,
            text      TEXT,
            active    INTEGER NOT NULL DEFAULT 0
        )
        """
    )

    cur.execute("SELECT COUNT(*) FROM screens")
    if cur.fetchone()[0] == 0:
        defaults = [
            (1, "Zone A",               "DETAIL",   "ALLE", "", 15, 0, 0),
            (2, "Zone B",               "DETAIL",   "ALLE", "", 15, 0, 0),
            (3, "Zone C",               "DETAIL",   "ALLE", "", 15, 0, 0),
            (4, "Zone D",               "DETAIL",   "ALLE", "", 15, 0, 0),
            (5, "Übersicht Links",      "OVERVIEW", "ALLE", "", 20, 0, 0),
            (6, "Übersicht Rechts",     "OVERVIEW", "ALLE", "", 20, 0, 0),
            (7, "Lagerstand Übersicht", "WAREHOUSE","ALLE", "", 20, 0, 0),
            (8, "Wareneingang 1",       "DETAIL",   "ALLE", "", 15, 0, 0),
            (9, "Wareneingang 2",       "DETAIL",   "ALLE", "", 15, 0, 0),
        ]
        cur.executemany(
            """
            INSERT INTO screens
            (id, name, mode, filter_type, filter_locations, refresh_interval_seconds, holiday_flag, special_flag)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            defaults,
        )

    cur.execute("SELECT id FROM screens")
    for sid in [int(r[0]) for r in cur.fetchall()]:
        cur.execute("INSERT OR IGNORE INTO tickers (screen_id, text, active) VALUES (?, '', 0)", (sid,))

    cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_departures_source_key ON departures(source_key)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_departures_screen_id ON departures(screen_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_departures_datetime ON departures(datetime)")

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
    if deps:
        if "screen_id" not in deps:
            cur.execute("ALTER TABLE departures ADD COLUMN screen_id INTEGER;")
        if "source_key" not in deps:
            cur.execute("ALTER TABLE departures ADD COLUMN source_key TEXT;")
        if "created_by" not in deps:
            cur.execute("ALTER TABLE departures ADD COLUMN created_by TEXT;")
        if "ready_at" not in deps:
            cur.execute("ALTER TABLE departures ADD COLUMN ready_at TEXT;")
        if "completed_at" not in deps:
            cur.execute("ALTER TABLE departures ADD COLUMN completed_at TEXT;")
        if "countdown_enabled" not in deps:
            cur.execute("ALTER TABLE departures ADD COLUMN countdown_enabled INTEGER NOT NULL DEFAULT 1;")

    locs = table_cols("locations")
    if locs:
        if "color" not in locs:
            cur.execute("ALTER TABLE locations ADD COLUMN color TEXT;")
        if "text_color" not in locs:
            cur.execute("ALTER TABLE locations ADD COLUMN text_color TEXT;")

    tours = table_cols("tours")
    if tours:
        if "screen_ids" not in tours:
            cur.execute("ALTER TABLE tours ADD COLUMN screen_ids TEXT;")
        if "minute" not in tours:
            cur.execute("ALTER TABLE tours ADD COLUMN minute INTEGER NOT NULL DEFAULT 0;")
        if "countdown_enabled" not in tours:
            cur.execute("ALTER TABLE tours ADD COLUMN countdown_enabled INTEGER NOT NULL DEFAULT 0;")

    conn.commit()


@contextmanager
def get_connection_context():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("PRAGMA foreign_keys=ON;")
    conn.execute("PRAGMA busy_timeout=30000;")
    try:
        if not integrity_ok(conn):
            raise RuntimeError(f"Datenbank beschädigt: {DB_PATH}")
        init_db(conn)
        migrate_db(conn)
        yield conn
    finally:
        conn.close()


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


def _ensure_columns(df: pd.DataFrame, required: dict) -> pd.DataFrame:
    for col, default in required.items():
        if col not in df.columns:
            df[col] = default
    return df


def load_departures_with_locations(conn):
    try:
        df = read_df(
            conn,
            """
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
            """,
        )
    except Exception:
        df = pd.DataFrame()

    df = _ensure_columns(df, {
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
        "location_name": "",
        "location_type": "",
        "location_active": 1,
        "location_color": "",
        "location_text_color": "",
    })

    if not df.empty:
        for col in ["datetime", "ready_at", "completed_at"]:
            df[col] = pd.to_datetime(df[col], errors="coerce")
            df[col] = df[col].apply(lambda x: ensure_tz(x.to_pydatetime()) if pd.notnull(x) else x)
        df["countdown_enabled"] = pd.to_numeric(df["countdown_enabled"], errors="coerce").fillna(1).astype(int)

    return df


def load_tours(conn):
    return read_df(
        conn,
        """
        SELECT t.id,
               t.name,
               t.weekday,
               t.hour,
               t.minute,
               t.location_id,
               t.note,
               t.active,
               t.screen_ids,
               t.countdown_enabled,
               l.name AS location_name
        FROM tours t
        JOIN locations l ON t.location_id = l.id
        ORDER BY t.id
        """,
    )


def load_tour_stops(conn, tour_id: int):
    return read_df(
        conn,
        """
        SELECT ts.location_id, ts.position, l.name AS location_name
        FROM tour_stops ts
        JOIN locations l ON l.id = ts.location_id
        WHERE ts.tour_id = ?
        ORDER BY ts.position
        """,
        (tour_id,),
    )


def parse_screen_ids(screen_ids_value):
    if screen_ids_value is None:
        return []
    s = str(screen_ids_value).strip()
    if not s:
        return []
    out = []
    for part in s.split(","):
        p = part.strip()
        if p.isdigit():
            out.append(int(p))
    return out


# ==================================================
# Status / Materialisierung
# ==================================================

def update_departure_statuses(conn: sqlite3.Connection):
    now = now_berlin()
    now_iso = now.isoformat(timespec="seconds")

    df = read_df(conn, "SELECT id, datetime, status, ready_at, completed_at FROM departures")
    if df.empty:
        return

    for col in ["datetime", "ready_at", "completed_at"]:
        df[col] = pd.to_datetime(df[col], errors="coerce")
    df = df.dropna(subset=["datetime"])
    df["datetime"] = df["datetime"].apply(lambda x: ensure_tz(x.to_pydatetime()))

    to_ready = []
    to_done = []
    for _, r in df.iterrows():
        dep_id = int(r["id"])
        dep_dt = ensure_tz(r["datetime"])
        status = str(r["status"] or "").upper()
        if status in ("STORNIERT", "ABGESCHLOSSEN"):
            continue
        if now >= completion_deadline(dep_dt):
            to_done.append(dep_id)
        elif now >= dep_dt and status != "BEREIT":
            to_ready.append(dep_id)

    if to_ready:
        conn.executemany(
            "UPDATE departures SET status='BEREIT', ready_at=COALESCE(ready_at, ?) WHERE id=?",
            [(now_iso, i) for i in to_ready],
        )
    if to_done:
        conn.executemany(
            "UPDATE departures SET status='ABGESCHLOSSEN', completed_at=COALESCE(completed_at, ?) WHERE id=?",
            [(now_iso, i) for i in to_done],
        )
    if to_ready or to_done:
        conn.commit()


def materialize_tours_to_departures(conn: sqlite3.Connection, create_window_hours: int = MATERIALIZE_TOURS_HOURS_BEFORE):
    now = now_berlin()
    window = timedelta(hours=create_window_hours)

    df = read_df(
        conn,
        """
        SELECT t.id AS tour_id,
               t.weekday,
               t.hour,
               t.minute,
               t.note AS tour_note,
               t.active AS tour_active,
               t.screen_ids AS tour_screen_ids,
               t.countdown_enabled AS tour_countdown_enabled,
               ts.location_id,
               ts.position,
               l.active AS location_active
        FROM tours t
        JOIN tour_stops ts ON ts.tour_id = t.id
        JOIN locations l ON l.id = ts.location_id
        """
    )
    if df.empty:
        return

    df = df[(df["tour_active"] == 1) & (df["location_active"] == 1)]
    created_any = False
    cur = conn.cursor()

    for _, r in df.iterrows():
        tour_id = int(r["tour_id"])
        pos = int(r["position"])
        loc_id = int(r["location_id"])
        weekday = str(r["weekday"])
        hour = int(r["hour"]) if str(r["hour"]).isdigit() else 0
        minute = int(r["minute"]) if str(r["minute"]).isdigit() else 0
        minute = 0 if minute not in (0, 30) else minute
        note = (r["tour_note"] or "").strip()
        screen_ids = parse_screen_ids(r["tour_screen_ids"])
        tour_cd = int(pd.to_numeric(r.get("tour_countdown_enabled", 0), errors="coerce") or 0)

        if not screen_ids or weekday not in WEEKDAYS_DE:
            continue

        dep_dt = next_datetime_for_weekday_time(weekday, hour, minute)
        if dep_dt - now > window:
            continue

        for sid in screen_ids:
            source_key = f"TOUR:{tour_id}:{pos}:{sid}:{dep_dt.isoformat()}"
            try:
                execute_with_retry(
                    cur,
                    """
                    INSERT INTO departures (datetime, location_id, vehicle, status, note, source_key, created_by, screen_id, countdown_enabled)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (dep_dt.isoformat(), loc_id, "", "GEPLANT", note, source_key, "TOUR_AUTO", int(sid), int(tour_cd)),
                )
                created_any = True
            except sqlite3.IntegrityError:
                pass

    if created_any:
        conn.commit()


def create_manual_departures(conn, dep_dt: datetime, location_id: int, screen_ids: list[int], note: str, created_by: str, countdown_enabled: bool):
    dep_dt = ensure_tz(dep_dt)
    cur = conn.cursor()
    created_any = False
    for sid in screen_ids:
        sk = f"MANUAL:{uuid.uuid4().hex}:{sid}:{dep_dt.isoformat()}"
        try:
            execute_with_retry(
                cur,
                """
                INSERT INTO departures (datetime, location_id, vehicle, status, note, source_key, created_by, screen_id, countdown_enabled)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (dep_dt.isoformat(), int(location_id), "", "GEPLANT", (note or "").strip(), sk, created_by, int(sid), 1 if countdown_enabled else 0),
            )
            created_any = True
        except sqlite3.IntegrityError:
            pass
    if created_any:
        conn.commit()


# ==================================================
# Screen Daten
# ==================================================

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
        return screen, deps

    deps = deps[deps["location_active"] == 1].copy()
    deps = deps[(deps["screen_id"].isna()) | (deps["screen_id"] == int(screen_id))]

    if screen["filter_type"] != "ALLE":
        deps = deps[deps["location_type"] == screen["filter_type"]]
    if (screen["filter_locations"] or "").strip():
        ids = [int(x.strip()) for x in str(screen["filter_locations"]).split(",") if x.strip().isdigit()]
        if ids:
            deps = deps[deps["location_id"].isin(ids)]

    deps = deps[(deps["datetime"] >= start) & (deps["datetime"] <= end)]

    def visible(row):
        status = str(row.get("status") or "").upper()
        if status != "ABGESCHLOSSEN":
            return True
        ca = row.get("completed_at")
        dep_dt = ensure_tz(row["datetime"])
        base = ensure_tz(ca) if pd.notnull(ca) else completion_deadline(dep_dt)
        return now <= base + timedelta(minutes=KEEP_COMPLETED_MINUTES)

    deps = deps[deps.apply(visible, axis=1)]

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
            return f"Abschluss in {fmt_compact(completion_deadline(dep_dt) - now)}"
        return ""

    deps["line_info"] = deps.apply(build_line_info, axis=1)
    deps = deps.sort_values("datetime")
    return screen, deps


# ==================================================
# HTML Helpers
# ==================================================

def escape_html(text: str) -> str:
    return (text or "").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def render_big_table(headers, rows, row_colors=None, text_colors=None):
    thead = "".join(f"<th>{h}</th>" for h in headers)
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
        tds = "".join(f"<td>{c}</td>" for c in r)
        body += f"<tr{style}>{tds}</tr>"
    st.markdown(
        f"""
        <table class="big-table">
          <thead><tr>{thead}</tr></thead>
          <tbody>{body}</tbody>
        </table>
        """,
        unsafe_allow_html=True,
    )


def render_split_screen(conn, left_screen_id: int, right_screen_id: int, title: str):
    left_screen, left_data = get_screen_data(conn, left_screen_id)
    right_screen, right_data = get_screen_data(conn, right_screen_id)

    st.markdown(
        """
        <style>
        .split-monitor-wrap {
            display: grid;
            grid-template-columns: 1fr 8px 1fr;
            gap: 0;
            width: 100%;
            min-height: 70vh;
            margin-top: 14px;
            border: 3px solid #1f2937;
            border-radius: 20px;
            overflow: hidden;
            box-shadow: 0 18px 45px rgba(0,0,0,0.28);
            background: #0b1220;
        }
        .split-monitor-card {
            background: #111827;
            padding: 18px 18px 12px 18px;
            min-height: 100%;
        }
        .split-divider {
            background: linear-gradient(180deg, #4b5563 0%, #9ca3af 50%, #4b5563 100%);
            min-height: 100%;
        }
        .split-monitor-title {
            margin: 0 0 14px 0;
            font-size: 34px !important;
            font-weight: 900;
            color: #ffffff;
            letter-spacing: 0.3px;
            text-transform: uppercase;
        }
        .split-empty {
            background: #1f2937;
            color: #e5e7eb;
            border-radius: 14px;
            padding: 20px;
            font-size: 24px !important;
            border: 1px solid #374151;
        }
        .split-monitor-card .big-table {
            width: 100%;
            border-collapse: collapse;
            background: #ffffff;
            border-radius: 14px;
            overflow: hidden;
        }
        .split-monitor-card .big-table th,
        .split-monitor-card .big-table td {
            border-bottom: 1px solid #d1d5db;
            padding: 0.55em 0.9em;
            text-align: left;
            vertical-align: top;
        }
        .split-monitor-card .big-table th {
            font-weight: 900;
            background: #e5e7eb;
            color: #111827;
        }
        @media (max-width: 1100px) {
            .split-monitor-wrap {
                grid-template-columns: 1fr;
            }
            .split-divider {
                display: none;
            }
        }
        </style>
        """,
        unsafe_allow_html=True,
    )

    st.markdown(f"## {title}")
    st.caption(f"DE Ortszeit: {now_berlin().strftime('%d.%m.%Y %H:%M:%S')} • Anzeige: nächste {DISPLAY_WINDOW_HOURS}h")

    left_name = left_screen["name"] if left_screen is not None else f"Screen {left_screen_id}"
    right_name = right_screen["name"] if right_screen is not None else f"Screen {right_screen_id}"

    col1, divider, col2 = st.columns([1, 0.02, 1])

    with col1:
        st.markdown(f"<div class='split-monitor-card'><div class='split-monitor-title'>{left_name}</div>", unsafe_allow_html=True)
        if left_data is None or left_data.empty:
            st.markdown("<div class='split-empty'>Keine Abfahrten im Zeitfenster.</div>", unsafe_allow_html=True)
        else:
            rows = []
            row_colors = []
            text_colors = []
            for _, r in left_data.iterrows():
                info = str(r.get("note") or "")
                li = str(r.get("line_info") or "")
                if li:
                    info = (info + " · " if info else "") + li
                rows.append([
                    ensure_tz(r["datetime"]).strftime("%H:%M"),
                    r["location_name"],
                    info,
                ])
                row_colors.append(r.get("location_color") or "")
                text_colors.append(r.get("location_text_color") or "")
            render_big_table(
                ["Zeit", "Einrichtung", "Hinweis / Countdown"],
                rows,
                row_colors=row_colors,
                text_colors=text_colors,
            )
        st.markdown("</div>", unsafe_allow_html=True)

    with divider:
        st.markdown("<div class='split-divider'>&nbsp;</div>", unsafe_allow_html=True)

    with col2:
        st.markdown(f"<div class='split-monitor-card'><div class='split-monitor-title'>{right_name}</div>", unsafe_allow_html=True)
        if right_data is None or right_data.empty:
            st.markdown("<div class='split-empty'>Keine Abfahrten im Zeitfenster.</div>", unsafe_allow_html=True)
        else:
            rows = []
            row_colors = []
            text_colors = []
            for _, r in right_data.iterrows():
                info = str(r.get("note") or "")
                li = str(r.get("line_info") or "")
                if li:
                    info = (info + " · " if info else "") + li
                rows.append([
                    ensure_tz(r["datetime"]).strftime("%H:%M"),
                    r["location_name"],
                    info,
                ])
                row_colors.append(r.get("location_color") or "")
                text_colors.append(r.get("location_text_color") or "")
            render_big_table(
                ["Zeit", "Einrichtung", "Hinweis / Countdown"],
                rows,
                row_colors=row_colors,
                text_colors=text_colors,
            )
        st.markdown("</div>", unsafe_allow_html=True)


def show_display_mode(screen_id: int):
    st.markdown(
        """
        <style>
        #MainMenu {visibility:hidden;} footer {visibility:hidden;} header {visibility:hidden;}
        .block-container {padding-top:0.5rem;padding-bottom:3.2rem;}
        body,.block-container,.stMarkdown,.stText,div,span {font-size:30px !important;}
        .big-table {width:100%;border-collapse:collapse;}
        .big-table th,.big-table td {border-bottom:1px solid #555;padding:.45em .9em;text-align:left;vertical-align:top;}
        .ticker {position:fixed;bottom:0;left:0;width:100%;background:#000;color:#fff;overflow:hidden;white-space:nowrap;z-index:9999;padding:.25rem 0;}
        .ticker__inner {display:inline-block;padding-left:100%;animation:ticker-scroll 20s linear infinite;font-size:28px !important;}
        @keyframes ticker-scroll {0% {transform:translateX(0);} 100% {transform:translateX(-100%);} }
        </style>
        """,
        unsafe_allow_html=True,
    )

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
        msg = " - ".join(labels)
        st.markdown(f"<div style='display:flex;justify-content:center;align-items:center;height:100vh;background:#000;color:#fff;font-size:72px;font-weight:900;text-transform:uppercase;text-align:center;'>{msg}</div>", unsafe_allow_html=True)
        return

    if int(screen["id"]) == 7 or str(screen["mode"]).upper() == "WAREHOUSE":
        st.markdown(f"## {screen['name']} (Screen {screen_id})")
        rows = []
        for zid in [1, 2, 3, 4, 8, 9]:
            _, zrows = get_screen_data(conn, zid)
            first = zrows.iloc[0] if len(zrows) >= 1 else None
            second = zrows.iloc[1] if len(zrows) >= 2 else None
            n1 = f"{ensure_tz(first['datetime']).strftime('%d.%m %H:%M')} – {first['location_name']}" if first is not None else "—"
            n2 = f"{ensure_tz(second['datetime']).strftime('%d.%m %H:%M')} – {second['location_name']}" if second is not None else "—"
            rows.append([ZONE_NAME_MAP.get(zid, str(zid)), n1, n2])
        render_big_table(["Zone", "Nächste Abfahrt", "Übernächste Abfahrt"], rows)
    else:
        st.markdown(f"## {screen['name']} (Screen {screen_id})")
        _, data = get_screen_data(conn, int(screen_id))
        if data.empty:
            st.info("Keine Abfahrten im nächsten Zeitfenster.")
        else:
            rows = []
            for _, r in data.iterrows():
                info = str(r.get("note") or "")
                li = str(r.get("line_info") or "")
                if li:
                    info = (info + " · " if info else "") + li
                rows.append([ensure_tz(r["datetime"]).strftime("%H:%M"), r["location_name"], info])
            render_big_table(["Zeit", "Einrichtung", "Hinweis / Countdown"], rows)

    if bool(screen.get("ticker_active", 0)) and str(screen.get("text", "") or "").strip():
        st.markdown(f"<div class='ticker'><div class='ticker__inner'>{escape_html(screen['text'])}</div></div>", unsafe_allow_html=True)


# ==================================================
# Main
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
