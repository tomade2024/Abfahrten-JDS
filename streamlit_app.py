# streamlit_app.py
import streamlit as st
import pandas as pd
import sqlite3
import json
import io
import time
import uuid
from datetime import datetime, timedelta, time as dtime, date
from pathlib import Path
from zoneinfo import ZoneInfo

from streamlit_autorefresh import st_autorefresh

# ==================================================
# Konfiguration
# ==================================================

st.set_page_config(page_title="Abfahrten", layout="wide")

DB_PATH = Path("abfahrten.db")
TZ = ZoneInfo("Europe/Berlin")

# Login (einfach, lokal in Code)
USERS = {
    "admin": {"password": "admin123", "role": "admin"},
    "dispo": {"password": "dispo123", "role": "viewer"},  # darf sehen, aber nicht löschen/importieren/ändern
}

WEEKDAYS_DE = ["Montag", "Dienstag", "Mittwoch", "Donnerstag", "Freitag", "Samstag", "Sonntag"]
WEEKDAY_TO_INT = {name: i for i, name in enumerate(WEEKDAYS_DE)}  # Montag=0

# Countdown beginnt 3 Stunden vorher (wenn in der Tour aktiviert)
COUNTDOWN_START_HOURS = 3

# Status-Automatik (gilt für Touren- und manuelle Abfahrten)
AUTO_COMPLETE_AFTER_MIN = 20
KEEP_COMPLETED_MINUTES = 10

# Touren werden automatisch zu echten Abfahrten materialisiert
# UND: Touren sollen 12 Stunden vor Abfahrt auf Screens erscheinen
MATERIALIZE_TOURS_HOURS_BEFORE = 12
DISPLAY_WINDOW_HOURS = 12  # Anzeige-Fenster für Screens: jetzt .. +12h

ZONE_SCREEN_IDS = [1, 2, 3, 4, 8, 9]  # Zone A-D + Wareneingang 1/2
ZONE_NAME_MAP = {
    1: "Zone A",
    2: "Zone B",
    3: "Zone C",
    4: "Zone D",
    8: "Wareneingang 1",
    9: "Wareneingang 2",
}

# ==================================================
# Zeit Helpers (DE Ortszeit)
# ==================================================

def now_berlin() -> datetime:
    return datetime.now(TZ)

def ensure_tz(dt: datetime) -> datetime:
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
    total = int(td.total_seconds())
    if total < 0:
        total = 0
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
# Login
# ==================================================

def require_login():
    if st.session_state.get("logged_in"):
        return

    st.title("Login")
    with st.form("login_form"):
        username = st.text_input("Benutzername")
        password = st.text_input("Passwort", type="password")
        submitted = st.form_submit_button("Einloggen")

    if submitted:
        user_entry = USERS.get(username)
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
# DB Robustness / Migration
# ==================================================

def integrity_ok(conn: sqlite3.Connection) -> bool:
    try:
        r = conn.execute("PRAGMA integrity_check;").fetchone()
        if not r:
            return True
        return str(r[0]).lower() == "ok"
    except Exception:
        return False

def execute_with_retry(cur: sqlite3.Cursor, sql: str, params: tuple, retries: int = 6):
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
            mode                      TEXT NOT NULL,   -- DETAIL / OVERVIEW / WAREHOUSE
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
    else:
        for sid, name, mode, refresh in [
            (7, "Lagerstand Übersicht", "WAREHOUSE", 20),
            (8, "Wareneingang 1",       "DETAIL",    15),
            (9, "Wareneingang 2",       "DETAIL",    15),
        ]:
            cur.execute("SELECT COUNT(*) FROM screens WHERE id=?", (sid,))
            if cur.fetchone()[0] == 0:
                cur.execute(
                    """
                    INSERT INTO screens
                    (id, name, mode, filter_type, filter_locations, refresh_interval_seconds, holiday_flag, special_flag)
                    VALUES (?, ?, ?, 'ALLE', '', ?, 0, 0)
                    """,
                    (sid, name, mode, refresh),
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

    try:
        cur.execute("UPDATE screens SET mode='DETAIL' WHERE id IN (8,9) AND mode='WAREHOUSE';")
    except Exception:
        pass

    try:
        cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_departures_source_key ON departures(source_key);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_departures_screen_id ON departures(screen_id);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_departures_datetime ON departures(datetime);")
    except Exception:
        pass

    conn.commit()

@st.cache_resource
def get_connection():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(DB_PATH, check_same_thread=False, timeout=30)
    conn.row_factory = sqlite3.Row

    try:
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        conn.execute("PRAGMA foreign_keys=ON;")
        conn.execute("PRAGMA busy_timeout=30000;")
    except Exception:
        pass

    if not integrity_ok(conn):
        try:
            conn.close()
        except Exception:
            pass

        bad_name = DB_PATH.with_suffix(f".corrupt_{int(now_berlin().timestamp())}.db")
        try:
            if DB_PATH.exists():
                DB_PATH.rename(bad_name)
        except Exception:
            try:
                DB_PATH.unlink(missing_ok=True)
            except Exception:
                pass

        conn = sqlite3.connect(DB_PATH, check_same_thread=False, timeout=30)
        conn.row_factory = sqlite3.Row
        try:
            conn.execute("PRAGMA journal_mode=WAL;")
            conn.execute("PRAGMA synchronous=NORMAL;")
            conn.execute("PRAGMA foreign_keys=ON;")
            conn.execute("PRAGMA busy_timeout=30000;")
        except Exception:
            pass

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
    return read_df(conn, "SELECT * FROM screens ORDER BY id")

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
            SELECT d.id                   AS id,
                   d.datetime             AS datetime,
                   d.location_id          AS location_id,
                   d.vehicle              AS vehicle,
                   d.status               AS status,
                   d.note                 AS note,
                   d.ready_at             AS ready_at,
                   d.completed_at         AS completed_at,
                   d.source_key           AS source_key,
                   d.created_by           AS created_by,
                   d.screen_id            AS screen_id,
                   d.countdown_enabled    AS countdown_enabled,
                   l.name                 AS location_name,
                   l.type                 AS location_type,
                   l.active               AS location_active,
                   l.color                AS location_color,
                   l.text_color           AS location_text_color
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
        df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce")
        df["ready_at"] = pd.to_datetime(df["ready_at"], errors="coerce")
        df["completed_at"] = pd.to_datetime(df["completed_at"], errors="coerce")

        df["datetime"] = df["datetime"].apply(lambda x: ensure_tz(x.to_pydatetime()) if pd.notnull(x) else x)
        df["ready_at"] = df["ready_at"].apply(lambda x: ensure_tz(x.to_pydatetime()) if pd.notnull(x) else x)
        df["completed_at"] = df["completed_at"].apply(lambda x: ensure_tz(x.to_pydatetime()) if pd.notnull(x) else x)

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

def load_ticker_for_screen(conn, screen_id: int):
    df = read_df(conn, "SELECT screen_id, text, active FROM tickers WHERE screen_id=?", (int(screen_id),))
    return df.iloc[0] if not df.empty else None

# ==================================================
# Parsing Screen IDs
# ==================================================

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
# Status Automation
# ==================================================

def update_departure_statuses(conn: sqlite3.Connection):
    now = now_berlin()
    now_iso = now.isoformat(timespec="seconds")

    df = read_df(conn, "SELECT id, datetime, status, ready_at, completed_at FROM departures")
    if df.empty:
        return

    df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce")
    df["ready_at"] = pd.to_datetime(df["ready_at"], errors="coerce")
    df["completed_at"] = pd.to_datetime(df["completed_at"], errors="coerce")
    df = df.dropna(subset=["datetime"])

    df["datetime"] = df["datetime"].apply(lambda x: ensure_tz(x.to_pydatetime()))
    df["ready_at"] = df["ready_at"].apply(lambda x: ensure_tz(x.to_pydatetime()) if pd.notnull(x) else None)
    df["completed_at"] = df["completed_at"].apply(lambda x: ensure_tz(x.to_pydatetime()) if pd.notnull(x) else None)

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

# ==================================================
# Materialize Tours -> Departures (12h vorher)
# ==================================================

def materialize_tours_to_departures(conn: sqlite3.Connection, create_window_hours: int = MATERIALIZE_TOURS_HOURS_BEFORE):
    now = now_berlin()
    window = timedelta(hours=create_window_hours)

    df = read_df(
        conn,
        """
        SELECT t.id                  AS tour_id,
               t.weekday,
               t.hour,
               t.minute,
               t.note                AS tour_note,
               t.active              AS tour_active,
               t.screen_ids          AS tour_screen_ids,
               t.countdown_enabled   AS tour_countdown_enabled,
               ts.location_id,
               ts.position,
               l.active              AS location_active
        FROM tours t
        JOIN tour_stops ts ON ts.tour_id = t.id
        JOIN locations l   ON l.id = ts.location_id
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

        # nur innerhalb Window materialisieren
        if dep_dt - now > window:
            continue
        if now - dep_dt > timedelta(days=1):
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

# ==================================================
# Manuelle Abfahrten anlegen
# ==================================================

def create_manual_departures(conn: sqlite3.Connection, dep_dt: datetime, location_id: int, screen_ids: list[int], note: str, created_by: str, countdown_enabled: bool):
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
# Screen Daten (Tour + Manuell; Anzeige-Fenster 12h)
# ==================================================

def get_screen_data(conn: sqlite3.Connection, screen_id: int):
    screens = load_screens(conn)
    if screens.empty or screen_id not in screens["id"].tolist():
        return None, None

    screen = screens.loc[screens["id"] == screen_id].iloc[0]
    filter_type = screen["filter_type"] or "ALLE"
    filter_locations = (screen["filter_locations"] or "").strip()

    now = now_berlin()
    end = now + timedelta(hours=DISPLAY_WINDOW_HOURS)

    deps = load_departures_with_locations(conn)
    if deps.empty:
        return screen, deps

    deps = deps[deps["location_active"] == 1].copy()

    # nur Abfahrten, die diesem Screen gehören (oder global)
    deps = deps[(deps["screen_id"].isna()) | (deps["screen_id"] == int(screen_id))]

    # Filter Typ / Locations
    if filter_type != "ALLE":
        deps = deps[deps["location_type"] == filter_type]

    if filter_locations:
        ids = [int(x.strip()) for x in filter_locations.split(",") if x.strip().isdigit()]
        if ids:
            deps = deps[deps["location_id"].isin(ids)]

    # Sichtbarkeit:
    # - Zeige Einträge im Fenster: [now-20min .. now+12h]
    window_start = now - timedelta(minutes=AUTO_COMPLETE_AFTER_MIN)
    deps = deps[(deps["datetime"] >= window_start) & (deps["datetime"] <= end)]

    def visible(row):
        status = str(row.get("status") or "").upper()
        if status != "ABGESCHLOSSEN":
            return True
        if KEEP_COMPLETED_MINUTES <= 0:
            return False
        ca = row.get("completed_at")
        dep_dt = ensure_tz(row["datetime"])
        base = ensure_tz(ca) if pd.notnull(ca) else completion_deadline(dep_dt)
        return now <= base + timedelta(minutes=KEEP_COMPLETED_MINUTES)

    deps = deps[deps.apply(visible, axis=1)]

    # Countdown-Info (nur wenn countdown_enabled=1)
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
            parts = []
            ra = row.get("ready_at")
            if pd.notnull(ra):
                since = now - ensure_tz(ra)
                parts.append(f"BEREIT seit {fmt_compact(since)}")
            rem = completion_deadline(dep_dt) - now
            parts.append(f"Abschluss in {fmt_compact(rem)}")
            return " · ".join(parts)
        return ""

    deps["line_info"] = deps.apply(build_line_info, axis=1)

    # Sortiert nach Zeit (alles im Fenster)
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

# ==================================================
# Screen 7 Übersicht: Nächste + Übernächste pro Zone (1–4,8,9)
# ==================================================

def get_next_two_departures_for_zone(conn: sqlite3.Connection, zone_screen_id: int):
    now = now_berlin()
    end = now + timedelta(hours=DISPLAY_WINDOW_HOURS)
    deps = load_departures_with_locations(conn)
    if deps.empty:
        return []

    deps = deps[deps["location_active"] == 1].copy()
    deps = deps[(deps["screen_id"] == int(zone_screen_id))]
    deps = deps[(deps["datetime"] >= now) & (deps["datetime"] <= end)].sort_values("datetime").head(2)

    out = []
    for _, r in deps.iterrows():
        out.append({
            "dt": ensure_tz(r["datetime"]),
            "location": str(r["location_name"]),
            "note": str(r.get("note") or ""),
        })
    return out

# ==================================================
# Display Mode
# ==================================================

def show_display_mode(screen_id: int):
    st.markdown(
        """
        <style>
        #MainMenu {visibility: hidden;}
        footer {visibility: hidden;}
        header {visibility: hidden;}
        .block-container { padding-top: 0.5rem; padding-bottom: 3.2rem; }
        body, .block-container, .stMarkdown, .stText, .stDataFrame, div, span { font-size: 34px !important; }

        .big-table { width: 100%; border-collapse: collapse; }
        .big-table th, .big-table td {
            border-bottom: 1px solid #555;
            padding: 0.45em 0.9em;
            text-align: left;
            vertical-align: top;
        }
        .big-table th { font-weight: 800; }

        .ticker {
            position: fixed; bottom: 0; left: 0; width: 100%;
            background: #000; color: #fff;
            overflow: hidden; white-space: nowrap; z-index: 9999;
            padding: 0.25rem 0;
        }
        .ticker__inner {
            display: inline-block;
            padding-left: 100%;
            animation: ticker-scroll 20s linear infinite;
            font-size: 28px !important;
        }
        @keyframes ticker-scroll { 0% { transform: translateX(0);} 100% { transform: translateX(-100%);} }
        </style>
        """,
        unsafe_allow_html=True,
    )

    if not screen_id:
        st.error("Parameter 'screenId' fehlt oder ist ungültig.")
        return

    conn = get_connection()

    materialize_tours_to_departures(conn, create_window_hours=MATERIALIZE_TOURS_HOURS_BEFORE)
    update_departure_statuses(conn)

    screens = load_screens(conn)
    if screens.empty or int(screen_id) not in screens["id"].tolist():
        st.error(f"Screen {screen_id} ist nicht konfiguriert.")
        return

    screen = screens.loc[screens["id"] == int(screen_id)].iloc[0]
    interval_sec = int(screen.get("refresh_interval_seconds", 30))
    st_autorefresh(interval=interval_sec * 1000, key=f"display_refresh_{screen_id}")

    holiday_active = bool(screen.get("holiday_flag", 0))
    special_active = bool(screen.get("special_flag", 0))

    if holiday_active or special_active:
        st.markdown("<style>body, .block-container { background-color:#000 !important; color:#fff !important; }</style>", unsafe_allow_html=True)
        labels = []
        if holiday_active:
            labels.append("Feiertagsbelieferung")
        if special_active:
            labels.append("Sonderplan")
        msg = " - ".join(l.upper() for l in labels)
        st.markdown(
            f"""
            <div style="display:flex;justify-content:center;align-items:center;height:100vh;width:100%;
                        background:#000;color:#fff;font-size:72px;font-weight:900;text-transform:uppercase;text-align:center;">
              {msg}
            </div>
            """,
            unsafe_allow_html=True,
        )
        return

    # Screen 7: Lagerstand Übersicht (Zone A-D + WE1/WE2)
    if int(screen["id"]) == 7 or str(screen.get("mode", "")).upper() == "WAREHOUSE":
        st.markdown(f"## {screen['name']} (Screen {int(screen['id'])})")
        st.caption(
            f"Aktualisierung alle {interval_sec} Sekunden • DE Ortszeit: {now_berlin().strftime('%d.%m.%Y %H:%M:%S')} • Anzeige: nächste {DISPLAY_WINDOW_HOURS}h"
        )

        rows = []
        for zid in [1, 2, 3, 4, 8, 9]:
            zone_name = ZONE_NAME_MAP.get(zid, f"Screen {zid}")
            nxt = get_next_two_departures_for_zone(conn, zid)
            if not nxt:
                rows.append([zone_name, "—", "—"])
                continue

            def fmt_item(item):
                return f"{item['dt'].strftime('%a, %d.%m %H:%M')} – {escape_html(item['location'])}"

            first = fmt_item(nxt[0]) if len(nxt) >= 1 else "—"
            second = fmt_item(nxt[1]) if len(nxt) >= 2 else "—"
            rows.append([zone_name, first, second])

        render_big_table(["Zone", "Nächste Abfahrt", "Übernächste Abfahrt"], rows)

        ticker = load_ticker_for_screen(conn, int(screen_id))
        if ticker is not None and int(ticker["active"]) == 1 and (ticker["text"] or "").strip():
            st.markdown(f"<div class='ticker'><div class='ticker__inner'>{escape_html(ticker['text'])}</div></div>", unsafe_allow_html=True)
        return

    # DETAIL/OVERVIEW Screens
    st.markdown(f"## {screen['name']} (Screen {screen_id})")
    st.caption(
        f"Modus: {screen['mode']} • DE Ortszeit: {now_berlin().strftime('%d.%m.%Y %H:%M:%S')} • Anzeige: nächste {DISPLAY_WINDOW_HOURS}h"
    )

    screen_obj, data = get_screen_data(conn, int(screen_id))

    if data is None or data.empty:
        st.info("Keine Abfahrten im nächsten Zeitfenster.")
    else:
        if str(screen_obj["mode"]).upper() == "DETAIL" and int(screen_obj["id"]) in ZONE_SCREEN_IDS:
            subset = data[["datetime", "location_name", "note", "line_info", "location_color", "location_text_color"]].copy()
            subset["note"] = subset["note"].fillna("")
            subset["line_info"] = subset["line_info"].fillna("")
            subset["time"] = subset["datetime"].apply(lambda d: ensure_tz(d).strftime("%H:%M") if pd.notnull(d) else "")

            def combine(r):
                parts = [f"<b>{escape_html(r['time'])}</b>"]
                if r["note"]:
                    parts.append(escape_html(r["note"]))
                if r["line_info"]:
                    parts.append(escape_html(r["line_info"]))
                return "<br/>".join([p for p in parts if p])

            subset["combined"] = subset.apply(combine, axis=1)

            rows = subset[["location_name", "combined"]].itertuples(index=False, name=None)
            render_big_table(
                ["Einrichtung", "Hinweis / Countdown"],
                rows,
                row_colors=subset["location_color"].fillna("").tolist(),
                text_colors=subset["location_text_color"].fillna("").tolist(),
            )
        else:
            subset = data.copy()
            for _, row in subset.iterrows():
                note = (row.get("note") or "")
                li = (row.get("line_info") or "")
                dt = row.get("datetime")
                tstr = ensure_tz(dt).strftime("%H:%M") if pd.notnull(dt) else ""
                extra = ""
                if note or li:
                    extra = "<br/><span style='font-size:28px;'>" + escape_html(note)
                    if note and li:
                        extra += " · "
                    extra += escape_html(li) + "</span>"

                main = f"<b>{escape_html(tstr)} – {escape_html(row['location_name'])}</b>"
                bg = row.get("location_color") or ""
                tc = row.get("location_text_color") or ""
                style = "margin-bottom:10px;"
                if bg:
                    style += f"background-color:{bg};padding:0.3em 0.5em;border-radius:0.2em;"
                if tc:
                    style += f"color:{tc};"
                st.markdown(f"<div style='{style}'>{main}{extra}</div>", unsafe_allow_html=True)

    ticker = load_ticker_for_screen(conn, int(screen_id))
    if ticker is not None and int(ticker["active"]) == 1 and (ticker["text"] or "").strip():
        st.markdown(f"<div class='ticker'><div class='ticker__inner'>{escape_html(ticker['text'])}</div></div>", unsafe_allow_html=True)

# ==================================================
# Export/Import/Backup
# ==================================================

def df_to_csv_bytes(df: pd.DataFrame) -> bytes:
    out = io.StringIO()
    df.to_csv(out, index=False, sep=";", encoding="utf-8")
    return ("\ufeff" + out.getvalue()).encode("utf-8")

def export_locations_json(conn) -> bytes:
    df = load_locations(conn)
    payload = {"version": 1, "exported_at": now_berlin().isoformat(), "locations": df.to_dict(orient="records")}
    return json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")

def import_locations_json(conn, data: dict) -> tuple[int, int]:
    locations = data.get("locations", [])
    inserted = 0
    updated = 0
    cur = conn.cursor()

    for loc in locations:
        loc_id = loc.get("id", None)
        name = (loc.get("name") or "").strip()
        typ = (loc.get("type") or "").strip()
        active = 1 if int(loc.get("active", 1)) == 1 else 0
        color = loc.get("color", None)
        text_color = loc.get("text_color", None)

        if not name or not typ:
            continue

        if loc_id is not None:
            cur.execute("SELECT COUNT(*) FROM locations WHERE id=?", (int(loc_id),))
            exists = cur.fetchone()[0] > 0
            if exists:
                cur.execute(
                    "UPDATE locations SET name=?, type=?, active=?, color=?, text_color=? WHERE id=?",
                    (name, typ, active, color, text_color, int(loc_id)),
                )
                updated += 1
            else:
                cur.execute(
                    "INSERT INTO locations (id, name, type, active, color, text_color) VALUES (?, ?, ?, ?, ?, ?)",
                    (int(loc_id), name, typ, active, color, text_color),
                )
                inserted += 1
        else:
            cur.execute(
                "INSERT INTO locations (name, type, active, color, text_color) VALUES (?, ?, ?, ?, ?)",
                (name, typ, active, color, text_color),
            )
            inserted += 1

    conn.commit()
    return inserted, updated

def export_tours_json(conn) -> bytes:
    tours = load_tours(conn)
    items = []
    for _, t in tours.iterrows():
        tour_id = int(t["id"])
        stops_df = load_tour_stops(conn, tour_id)
        stops = []
        if not stops_df.empty:
            for _, s in stops_df.iterrows():
                stops.append({
                    "location_id": int(s["location_id"]),
                    "location_name": str(s["location_name"]),
                    "position": int(s["position"]),
                })

        items.append({
            "id": int(t["id"]),
            "name": str(t["name"]),
            "weekday": str(t["weekday"]),
            "hour": int(t["hour"]),
            "minute": int(t.get("minute", 0) or 0),
            "note": str(t["note"] or ""),
            "active": int(t["active"]),
            "screen_ids": str(t["screen_ids"] or ""),
            "countdown_enabled": int(t.get("countdown_enabled", 0) or 0),
            "primary_location_id": int(t["location_id"]),
            "primary_location_name": str(t["location_name"]),
            "stops": stops,
        })

    payload = {"version": 1, "exported_at": now_berlin().isoformat(), "tours": items}
    return json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")

def _resolve_location_id(conn, location_id, location_name):
    cur = conn.cursor()
    if location_id is not None:
        cur.execute("SELECT id FROM locations WHERE id=?", (int(location_id),))
        r = cur.fetchone()
        if r:
            return int(r[0])

    name = (location_name or "").strip()
    if not name:
        return None
    cur.execute("SELECT id FROM locations WHERE name=?", (name,))
    r = cur.fetchone()
    if r:
        return int(r[0])
    return None

def import_tours_json(conn, data: dict) -> tuple[int, int]:
    tours = data.get("tours", [])
    inserted = 0
    updated = 0
    cur = conn.cursor()

    for t in tours:
        tour_id = t.get("id", None)
        name = (t.get("name") or "").strip()
        weekday = (t.get("weekday") or "").strip()

        hour = int(t.get("hour", 0))
        minute = int(t.get("minute", 0) or 0)
        minute = 0 if minute not in (0, 30) else minute

        note = (t.get("note") or "").strip()
        active = 1 if int(t.get("active", 1)) == 1 else 0
        screen_ids = (t.get("screen_ids") or "").strip()
        cd = 1 if int(t.get("countdown_enabled", 0) or 0) == 1 else 0

        if not name or weekday not in WEEKDAYS_DE or not (0 <= hour <= 23):
            continue

        primary_loc_id = _resolve_location_id(conn, t.get("primary_location_id", None), t.get("primary_location_name", None))

        stops_in = t.get("stops", [])
        stops_in_sorted = sorted(stops_in, key=lambda x: int(x.get("position", 0)))
        resolved_stop_ids = []
        for s in stops_in_sorted:
            sid = _resolve_location_id(conn, s.get("location_id", None), s.get("location_name", None))
            if sid is not None:
                resolved_stop_ids.append(int(sid))

        if primary_loc_id is None and resolved_stop_ids:
            primary_loc_id = resolved_stop_ids[0]
        if primary_loc_id is None:
            continue
        if not resolved_stop_ids:
            resolved_stop_ids = [int(primary_loc_id)]

        if tour_id is not None:
            cur.execute("SELECT COUNT(*) FROM tours WHERE id=?", (int(tour_id),))
            exists = cur.fetchone()[0] > 0
            if exists:
                cur.execute(
                    """
                    UPDATE tours
                    SET name=?, weekday=?, hour=?, minute=?, location_id=?, note=?, active=?, screen_ids=?, countdown_enabled=?
                    WHERE id=?
                    """,
                    (name, weekday, hour, minute, int(primary_loc_id), note, active, screen_ids, cd, int(tour_id)),
                )
                updated += 1
            else:
                cur.execute(
                    """
                    INSERT INTO tours (id, name, weekday, hour, minute, location_id, note, active, screen_ids, countdown_enabled)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (int(tour_id), name, weekday, hour, minute, int(primary_loc_id), note, active, screen_ids, cd),
                )
                inserted += 1

            cur.execute("DELETE FROM tour_stops WHERE tour_id=?", (int(tour_id),))
            for pos, loc_id in enumerate(resolved_stop_ids):
                cur.execute(
                    "INSERT INTO tour_stops (tour_id, location_id, position) VALUES (?, ?, ?)",
                    (int(tour_id), int(loc_id), int(pos)),
                )
        else:
            cur.execute(
                """
                INSERT INTO tours (name, weekday, hour, minute, location_id, note, active, screen_ids, countdown_enabled)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (name, weekday, hour, minute, int(primary_loc_id), note, active, screen_ids, cd),
            )
            new_id = cur.lastrowid
            inserted += 1

            cur.execute("DELETE FROM tour_stops WHERE tour_id=?", (int(new_id),))
            for pos, loc_id in enumerate(resolved_stop_ids):
                cur.execute(
                    "INSERT INTO tour_stops (tour_id, location_id, position) VALUES (?, ?, ?)",
                    (int(new_id), int(loc_id), int(pos)),
                )

    conn.commit()
    return inserted, updated

def export_backup_json(conn) -> bytes:
    payload = {
        "version": 1,
        "exported_at": now_berlin().isoformat(),
        "locations": load_locations(conn).to_dict(orient="records"),
        "tours": json.loads(export_tours_json(conn).decode("utf-8")).get("tours", []),
    }
    return json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")

def import_backup_json(conn, data: dict) -> tuple[tuple[int, int], tuple[int, int]]:
    loc_ins, loc_upd = import_locations_json(conn, {"locations": data.get("locations", [])})
    tour_ins, tour_upd = import_tours_json(conn, {"tours": data.get("tours", [])})
    return (loc_ins, loc_upd), (tour_ins, tour_upd)

def export_locations_csv(conn) -> bytes:
    return df_to_csv_bytes(load_locations(conn))

def export_tours_csv(conn) -> tuple[bytes, bytes]:
    tours = load_tours(conn)
    if tours.empty:
        touren_df = pd.DataFrame(columns=[
            "id", "name", "weekday", "hour", "minute", "note", "active", "screen_ids", "countdown_enabled",
            "location_id", "primary_location_name"
        ])
        stops_df = pd.DataFrame(columns=["tour_id", "position", "location_id", "location_name"])
        return df_to_csv_bytes(touren_df), df_to_csv_bytes(stops_df)

    touren_out = tours.rename(columns={"location_name": "primary_location_name"})[
        ["id", "name", "weekday", "hour", "minute", "note", "active", "screen_ids", "countdown_enabled", "location_id", "primary_location_name"]
    ].copy()

    stops_rows = []
    for _, t in tours.iterrows():
        tid = int(t["id"])
        s = load_tour_stops(conn, tid)
        if s.empty:
            continue
        for _, r in s.iterrows():
            stops_rows.append({
                "tour_id": tid,
                "position": int(r["position"]),
                "location_id": int(r["location_id"]),
                "location_name": str(r["location_name"]),
            })
    stops_df = pd.DataFrame(stops_rows, columns=["tour_id", "position", "location_id", "location_name"])
    return df_to_csv_bytes(touren_out), df_to_csv_bytes(stops_df)

# ==================================================
# Admin: Einrichtungen
# ==================================================

def show_admin_locations(conn, can_edit: bool):
    st.subheader("Einrichtungen")

    locations = load_locations(conn)
    if locations.empty:
        st.info("Noch keine Einrichtungen vorhanden.")
    else:
        st.dataframe(locations, use_container_width=True)

    st.markdown("---")
    st.markdown("### Export (Einrichtungen)")
    c1, c2 = st.columns(2)
    with c1:
        st.download_button("Download einrichtungen.json", data=export_locations_json(conn), file_name="einrichtungen.json", mime="application/json")
    with c2:
        st.download_button("Download einrichtungen.csv", data=export_locations_csv(conn), file_name="einrichtungen.csv", mime="text/csv")

    if not can_edit:
        st.info("Nur Admin kann Einrichtungen anlegen/bearbeiten/löschen/importieren.")
        return

    st.markdown("---")
    st.markdown("### Import (Einrichtungen)")
    up = st.file_uploader("einrichtungen.json importieren", type=["json"], key="upl_locations")
    if up is not None:
        try:
            data = json.loads(up.getvalue().decode("utf-8"))
            ins, upd = import_locations_json(conn, data)
            st.success(f"Import erfolgreich. Neu: {ins}, Aktualisiert: {upd}")
            st.rerun()
        except Exception as e:
            st.error(f"Import fehlgeschlagen: {e}")

    st.markdown("---")
    st.markdown("### Neue Einrichtung anlegen")
    with st.form("new_location"):
        col1, col2, col3 = st.columns(3)
        with col1:
            name = st.text_input("Name", "")
            typ = st.selectbox("Typ", ["KRANKENHAUS", "ALTENHEIM", "MVZ"])
        with col2:
            active = st.checkbox("Aktiv", True)
        with col3:
            color = st.color_picker("Hintergrundfarbe", "#007bff")
            text_color = st.color_picker("Schriftfarbe", "#000000")

        submitted = st.form_submit_button("Speichern")

    if submitted:
        if not name.strip():
            st.error("Name darf nicht leer sein.")
        else:
            conn.execute(
                "INSERT INTO locations (name, type, active, color, text_color) VALUES (?, ?, ?, ?, ?)",
                (name.strip(), typ, 1 if active else 0, color, text_color),
            )
            conn.commit()
            st.success("Einrichtung gespeichert.")
            st.rerun()

    locations = load_locations(conn)
    if locations.empty:
        return

    st.markdown("---")
    st.markdown("### Einrichtung bearbeiten / löschen")
    loc_ids = locations["id"].tolist()
    selected = st.selectbox("Einrichtung auswählen", loc_ids, key="loc_edit_select")
    row = locations.loc[locations["id"] == selected].iloc[0]

    with st.form("edit_location"):
        col1, col2, col3 = st.columns(3)
        with col1:
            name_edit = st.text_input("Name", row["name"])
            types = ["KRANKENHAUS", "ALTENHEIM", "MVZ"]
            typ_edit = st.selectbox("Typ", types, index=types.index(row["type"]) if row["type"] in types else 0)
        with col2:
            active_edit = st.checkbox("Aktiv", bool(row["active"]))
        with col3:
            color_init = row["color"] if isinstance(row["color"], str) and row["color"] else "#007bff"
            text_color_init = row["text_color"] if isinstance(row["text_color"], str) and row["text_color"] else "#000000"
            color_edit = st.color_picker("Hintergrundfarbe", color_init)
            text_color_edit = st.color_picker("Schriftfarbe", text_color_init)

        b1, b2 = st.columns(2)
        save = b1.form_submit_button("Änderungen speichern")
        delete = b2.form_submit_button("Einrichtung löschen")

    if save:
        if not name_edit.strip():
            st.error("Name darf nicht leer sein.")
        else:
            conn.execute(
                "UPDATE locations SET name=?, type=?, active=?, color=?, text_color=? WHERE id=?",
                (name_edit.strip(), typ_edit, 1 if active_edit else 0, color_edit, text_color_edit, int(selected)),
            )
            conn.commit()
            st.success("Einrichtung aktualisiert.")
            st.rerun()

    if delete:
        dep_count = read_df(conn, "SELECT COUNT(*) AS c FROM departures WHERE location_id=?", (int(selected),)).iloc[0]["c"]
        tour_count = read_df(conn, "SELECT COUNT(*) AS c FROM tours WHERE location_id=?", (int(selected),)).iloc[0]["c"]
        stop_count = read_df(conn, "SELECT COUNT(*) AS c FROM tour_stops WHERE location_id=?", (int(selected),)).iloc[0]["c"]
        if dep_count or tour_count or stop_count:
            st.error(f"Kann nicht löschen: Abfahrten={dep_count}, Touren={tour_count}, Tour-Stops={stop_count}")
        else:
            conn.execute("DELETE FROM locations WHERE id=?", (int(selected),))
            conn.commit()
            st.success("Einrichtung gelöscht.")
            st.rerun()

# ==================================================
# Admin: Abfahrten (Tour + Manuell; inkl. manuelles Anlegen)
# ==================================================

def show_admin_departures(conn, can_edit: bool):
    st.subheader("Abfahrten")

    materialize_tours_to_departures(conn, create_window_hours=MATERIALIZE_TOURS_HOURS_BEFORE)
    update_departure_statuses(conn)

    deps = load_departures_with_locations(conn).sort_values("datetime")

    # Anzeige etwas kompakter
    if deps.empty:
        st.info("Noch keine Abfahrten vorhanden.")
    else:
        deps_view = deps.copy()
        deps_view["Quelle"] = deps_view["source_key"].astype(str).apply(lambda s: "TOUR" if s.startswith("TOUR:") else ("MANUELL" if s.startswith("MANUAL:") else "SONST"))
        deps_view["Zeit"] = deps_view["datetime"].apply(lambda d: ensure_tz(d).strftime("%d.%m.%Y %H:%M") if pd.notnull(d) else "")
        st.dataframe(
            deps_view[["Zeit", "screen_id", "location_name", "note", "status", "countdown_enabled", "Quelle", "source_key"]].copy(),
            use_container_width=True
        )

    if not can_edit:
        st.info("Keine Bearbeitungsrechte für manuelle Abfahrten.")
        return

    st.markdown("---")
    st.markdown("### Manuelle Abfahrt anlegen")

    locations = load_locations(conn)
    screens = load_screens(conn)
    if locations.empty:
        st.warning("Bitte zuerst Einrichtungen anlegen.")
        return

    screen_map = {int(r["id"]): r["name"] for _, r in screens.iterrows()}
    screen_options = list(screen_map.keys())

    with st.form("manual_dep_form"):
        c1, c2, c3 = st.columns(3)
        with c1:
            loc_id = st.selectbox(
                "Einrichtung",
                options=locations["id"].tolist(),
                format_func=lambda i: locations.loc[locations["id"] == i, "name"].values[0],
            )
            note = st.text_input("Hinweis (optional)", "")
        with c2:
            dep_date = st.date_input("Datum", value=now_berlin().date())
            time_label = st.selectbox("Uhrzeit (00/30)", time_options_half_hour(), index=time_options_half_hour().index("08:00") if "08:00" in time_options_half_hour() else 0)
            hh, mm = map(int, time_label.split(":"))
        with c3:
            screens_sel = st.multiselect(
                "Auf welchen Screens anzeigen?",
                options=screen_options,
                default=[1],
                format_func=lambda sid: f"{sid}: {screen_map.get(sid)}",
            )
            countdown_enabled = st.checkbox("Countdown aktiv", value=True)

        submitted = st.form_submit_button("Manuelle Abfahrt speichern")

    if submitted:
        if not screens_sel:
            st.error("Bitte mindestens einen Screen auswählen.")
        else:
            dep_dt = datetime.combine(dep_date, dtime(hour=hh, minute=mm)).replace(tzinfo=TZ)
            create_manual_departures(
                conn=conn,
                dep_dt=dep_dt,
                location_id=int(loc_id),
                screen_ids=[int(s) for s in screens_sel],
                note=note,
                created_by=str(st.session_state.get("username") or "ADMIN"),
                countdown_enabled=bool(countdown_enabled),
            )
            st.success("Manuelle Abfahrt angelegt.")
            st.rerun()

    st.markdown("---")
    st.markdown("### Löschen (nur Admin)")
    # Nur manuelle Abfahrten löschbar (sicherer), Tour-Abfahrten regenerieren sich automatisch
    manual = deps[deps["source_key"].astype(str).str.startswith("MANUAL:")] if not deps.empty else pd.DataFrame()
    if manual.empty:
        st.info("Keine manuellen Abfahrten vorhanden.")
        return

    ids = manual["id"].tolist()
    del_id = st.selectbox("Manuelle Abfahrt auswählen", ids, format_func=lambda i: f"ID {i}", key="del_dep_pick")
    if st.button("Ausgewählte manuelle Abfahrt löschen"):
        conn.execute("DELETE FROM departures WHERE id=?", (int(del_id),))
        conn.commit()
        st.success("Manuelle Abfahrt gelöscht.")
        st.rerun()

# ==================================================
# Admin: Touren
# ==================================================

def show_admin_tours(conn, can_edit: bool):
    st.subheader("Touren (feste Touren)")

    locations = load_locations(conn)
    screens = load_screens(conn)
    if locations.empty:
        st.warning("Bitte zuerst Einrichtungen anlegen.")
        return

    screen_map = {int(r["id"]): r["name"] for _, r in screens.iterrows()}
    screen_options = list(screen_map.keys())
    loc_options = locations["id"].tolist()
    time_opts = time_options_half_hour()

    tours = load_tours(conn)
    if tours.empty:
        st.info("Noch keine Touren vorhanden.")
    else:
        view = tours.copy()
        view["Zeit"] = view.apply(lambda r: f"{int(r['hour']):02d}:{int(r.get('minute',0) or 0):02d}", axis=1)
        view["Monitore"] = view["screen_ids"].apply(
            lambda s: ", ".join([f"{i}:{screen_map.get(i, f'Screen {i}')}" for i in parse_screen_ids(s)])
        )
        view["Countdown"] = view["countdown_enabled"].apply(lambda v: "Ja" if int(v) == 1 else "Nein")
        st.dataframe(
            view[["id", "name", "weekday", "Zeit", "Countdown", "location_name", "note", "active", "Monitore"]],
            use_container_width=True
        )

    st.markdown("---")
    st.markdown("### Export (Touren)")
    col_t1, col_t2, col_t3 = st.columns(3)
    with col_t1:
        st.download_button("Download touren.json", data=export_tours_json(conn), file_name="touren.json", mime="application/json")
    touren_csv, stops_csv = export_tours_csv(conn)
    with col_t2:
        st.download_button("Download touren.csv", data=touren_csv, file_name="touren.csv", mime="text/csv")
    with col_t3:
        st.download_button("Download tour_stops.csv", data=stops_csv, file_name="tour_stops.csv", mime="text/csv")

    if not can_edit:
        st.info("Nur Admin kann Touren anlegen/bearbeiten/löschen/importieren.")
        return

    st.markdown("---")
    st.markdown("### Import (Touren)")
    up = st.file_uploader("touren.json importieren", type=["json"], key="upl_tours")
    if up is not None:
        try:
            data = json.loads(up.getvalue().decode("utf-8"))
            ins, upd = import_tours_json(conn, data)
            st.success(f"Import erfolgreich. Neu: {ins}, Aktualisiert: {upd}")
            st.rerun()
        except Exception as e:
            st.error(f"Import fehlgeschlagen: {e}")

    st.markdown("---")
    st.markdown("### Neue Tour anlegen")
    with st.form("new_tour_form"):
        col1, col2, col3 = st.columns(3)
        with col1:
            tour_name = st.text_input("Tour-Name", "")
            weekday = st.selectbox("Wochentag", WEEKDAYS_DE)
        with col2:
            time_label = st.selectbox("Uhrzeit (00/30)", time_opts, index=time_opts.index("08:00") if "08:00" in time_opts else 0)
            hour_int, minute_int = map(int, time_label.split(":"))
        with col3:
            screens_new = st.multiselect(
                "Monitore (Screens) für diese Tour",
                options=screen_options,
                format_func=lambda sid: f"{sid}: {screen_map.get(sid)}",
            )

        countdown_enabled = st.checkbox("Countdown für diese Tour aktiv", value=False)

        stops_new = st.multiselect(
            "Einrichtungen (Stops)",
            options=loc_options,
            format_func=lambda i: locations.loc[locations["id"] == i, "name"].values[0],
        )
        note_new = st.text_input("Hinweis (optional)", "")
        active_new = st.checkbox("Aktiv", True)

        submitted = st.form_submit_button("Tour speichern")

    if submitted:
        if not tour_name.strip():
            st.error("Tour-Name darf nicht leer sein.")
        elif not screens_new:
            st.error("Bitte mindestens einen Screen auswählen.")
        elif not stops_new:
            st.error("Bitte mindestens einen Stop auswählen.")
        else:
            primary_loc = int(stops_new[0])
            screen_ids_str = ",".join(str(s) for s in screens_new)

            cur = conn.cursor()
            cur.execute(
                """
                INSERT INTO tours (name, weekday, hour, minute, location_id, note, active, screen_ids, countdown_enabled)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (tour_name.strip(), weekday, hour_int, minute_int, primary_loc, note_new.strip(),
                 1 if active_new else 0, screen_ids_str, 1 if countdown_enabled else 0),
            )
            tour_id = cur.lastrowid

            for pos, loc_id in enumerate(stops_new):
                cur.execute("INSERT INTO tour_stops (tour_id, location_id, position) VALUES (?, ?, ?)", (tour_id, int(loc_id), pos))

            conn.commit()
            st.success("Tour gespeichert.")
            st.rerun()

    tours = load_tours(conn)
    if tours.empty:
        return

    st.markdown("---")
    st.markdown("### Tour bearbeiten / löschen")
    tour_ids = tours["id"].tolist()
    selected = st.selectbox("Tour auswählen", tour_ids, key="edit_tour_select")
    row = tours.loc[tours["id"] == selected].iloc[0]

    stops_df = read_df(conn, "SELECT location_id FROM tour_stops WHERE tour_id=? ORDER BY position", (int(selected),))
    existing_stop_ids = stops_df["location_id"].tolist() if not stops_df.empty else [int(row["location_id"])]

    existing_screen_ids = parse_screen_ids(row["screen_ids"])
    cur_time = f"{int(row['hour']):02d}:{int(row.get('minute',0) or 0):02d}"
    if cur_time not in time_opts:
        cur_time = "08:00"

    with st.form("edit_tour_form"):
        col1, col2, col3 = st.columns(3)
        with col1:
            name_edit = st.text_input("Tour-Name", row["name"])
            weekday_edit = st.selectbox(
                "Wochentag",
                WEEKDAYS_DE,
                index=WEEKDAYS_DE.index(row["weekday"]) if row["weekday"] in WEEKDAYS_DE else 0
            )
        with col2:
            time_edit = st.selectbox("Uhrzeit (00/30)", time_opts, index=time_opts.index(cur_time))
            hour_edit, minute_edit = map(int, time_edit.split(":"))
        with col3:
            screens_edit = st.multiselect(
                "Monitore (Screens) für diese Tour",
                options=screen_options,
                format_func=lambda sid: f"{sid}: {screen_map.get(sid)}",
                default=existing_screen_ids,
            )

        countdown_edit = st.checkbox("Countdown für diese Tour aktiv", value=(int(row.get("countdown_enabled", 0) or 0) == 1))

        stops_edit = st.multiselect(
            "Einrichtungen (Stops)",
            options=loc_options,
            format_func=lambda i: locations.loc[locations["id"] == i, "name"].values[0],
            default=existing_stop_ids,
        )
        note_edit = st.text_input("Hinweis (optional)", row["note"] or "")
        active_edit = st.checkbox("Aktiv", bool(row["active"]))

        b1, b2 = st.columns(2)
        save = b1.form_submit_button("Änderungen speichern")
        delete = b2.form_submit_button("Tour löschen")

    if save:
        if not name_edit.strip():
            st.error("Tour-Name darf nicht leer sein.")
        elif not screens_edit:
            st.error("Bitte mindestens einen Screen auswählen.")
        elif not stops_edit:
            st.error("Bitte mindestens einen Stop auswählen.")
        else:
            primary_loc = int(stops_edit[0])
            screen_ids_str = ",".join(str(s) for s in screens_edit)

            cur = conn.cursor()
            cur.execute(
                """
                UPDATE tours
                SET name=?, weekday=?, hour=?, minute=?, location_id=?, note=?, active=?, screen_ids=?, countdown_enabled=?
                WHERE id=?
                """,
                (name_edit.strip(), weekday_edit, hour_edit, minute_edit, primary_loc, note_edit.strip(),
                 1 if active_edit else 0, screen_ids_str, 1 if countdown_edit else 0, int(selected)),
            )
            cur.execute("DELETE FROM tour_stops WHERE tour_id=?", (int(selected),))
            for pos, loc_id in enumerate(stops_edit):
                cur.execute("INSERT INTO tour_stops (tour_id, location_id, position) VALUES (?, ?, ?)", (int(selected), int(loc_id), int(pos)))
            conn.commit()
            st.success("Tour aktualisiert.")
            st.rerun()

    if delete:
        cur = conn.cursor()
        cur.execute("DELETE FROM tour_stops WHERE tour_id=?", (int(selected),))
        cur.execute("DELETE FROM tours WHERE id=?", (int(selected),))
        conn.commit()
        st.success("Tour gelöscht.")
        st.rerun()

# ==================================================
# Admin: Screens + Links + Sonderplan/Feiertag + Ticker
# ==================================================

def show_admin_screens(conn, can_edit: bool):
    st.subheader("Screens / Monitore")

    screens = load_screens(conn)
    st.dataframe(screens, use_container_width=True)

    st.markdown("---")
    st.markdown("### Links zu den Monitoren")
    st.info("Diese Links auf den jeweiligen Monitoren öffnen (mit Autorefresh).")
    for _, r in screens.iterrows():
        sid = int(r["id"])
        link = f"?mode=display&screenId={sid}"
        st.markdown(f"- **Screen {sid} – {r['name']}**: [Monitor öffnen]({link}) (Parameter: `{link}`)")

    if not can_edit:
        st.info("Nur Admin kann Screens/Ticker bearbeiten.")
        return

    st.markdown("---")
    st.markdown("### Screen bearbeiten")
    screen_ids = screens["id"].tolist()
    selected = st.selectbox("Screen wählen", screen_ids, key="screen_select")
    row = screens.loc[screens["id"] == selected].iloc[0]

    with st.form("edit_screen"):
        name = st.text_input("Name", row["name"])
        mode = st.selectbox(
            "Modus",
            ["DETAIL", "OVERVIEW", "WAREHOUSE"],
            index=["DETAIL", "OVERVIEW", "WAREHOUSE"].index(row["mode"]) if row["mode"] in ["DETAIL", "OVERVIEW", "WAREHOUSE"] else 0
        )
        filter_type = st.selectbox(
            "Filter Typ",
            ["ALLE", "KRANKENHAUS", "ALTENHEIM", "MVZ"],
            index=["ALLE", "KRANKENHAUS", "ALTENHEIM", "MVZ"].index(row["filter_type"]) if row["filter_type"] in ["ALLE", "KRANKENHAUS", "ALTENHEIM", "MVZ"] else 0
        )
        filter_locations = st.text_input("Filter Locations (IDs, Komma-getrennt)", row["filter_locations"] or "")
        refresh = st.number_input("Refresh-Intervall (Sekunden)", min_value=5, max_value=300, value=int(row["refresh_interval_seconds"]))
        holiday_flag = st.checkbox("Feiertagsbelieferung aktiv (Vollbild)", value=bool(row["holiday_flag"]))
        special_flag = st.checkbox("Sonderplan aktiv (Vollbild)", value=bool(row["special_flag"]))

        submitted = st.form_submit_button("Speichern")

    if submitted:
        conn.execute(
            """
            UPDATE screens
            SET name=?, mode=?, filter_type=?, filter_locations=?,
                refresh_interval_seconds=?, holiday_flag=?, special_flag=?
            WHERE id=?
            """,
            (name, mode, filter_type, filter_locations, int(refresh),
             1 if holiday_flag else 0, 1 if special_flag else 0, int(selected)),
        )
        conn.commit()
        st.success("Screen aktualisiert.")
        st.rerun()

    st.markdown("---")
    st.markdown("### Laufband / Ticker (pro Screen)")
    st.info("Das Laufband läuft nur auf Screens, wo es hier aktiv gesetzt ist. Hintergrund schwarz, Text weiß.")

    trow = load_ticker_for_screen(conn, int(selected))
    with st.form("ticker_form"):
        text = st.text_area("Laufband-Text", value=(trow["text"] or "") if trow is not None else "", height=120)
        active = st.checkbox("Laufband aktiv", value=bool(trow["active"]) if trow is not None else False)
        submit = st.form_submit_button("Speichern")

    if submit:
        conn.execute(
            "INSERT OR REPLACE INTO tickers (screen_id, text, active) VALUES (?, ?, ?)",
            (int(selected), text.strip(), 1 if active else 0),
        )
        conn.commit()
        st.success("Laufband gespeichert.")
        st.rerun()

# ==================================================
# Admin: Backup + Tabs
# ==================================================

def show_admin_mode():
    require_login()

    role = st.session_state.get("role", "viewer")
    username = st.session_state.get("username", "")

    st.title("Abfahrten – Admin / Disposition")
    st.caption(
        f"Eingeloggt als: {username} (Rolle: {role}) • DE Ortszeit: {now_berlin().strftime('%d.%m.%Y %H:%M:%S')}"
    )

    if st.sidebar.button("Logout"):
        st.session_state["logged_in"] = False
        st.session_state["role"] = None
        st.session_state["username"] = None
        st.rerun()

    conn = get_connection()

    materialize_tours_to_departures(conn, create_window_hours=MATERIALIZE_TOURS_HOURS_BEFORE)
    update_departure_statuses(conn)

    can_edit = role == "admin"

    if can_edit:
        st.markdown("### Backup (Einrichtungen + Touren)")
        colb1, colb2 = st.columns(2)

        with colb1:
            st.download_button(
                "Backup herunterladen (backup_abfahrten.json)",
                data=export_backup_json(conn),
                file_name="backup_abfahrten.json",
                mime="application/json",
            )

        with colb2:
            up = st.file_uploader("Backup importieren (backup_abfahrten.json)", type=["json"], key="upl_backup")
            if up is not None:
                try:
                    data = json.loads(up.getvalue().decode("utf-8"))
                    (li, lu), (ti, tu) = import_backup_json(conn, data)
                    st.success(f"Backup importiert. Einrichtungen: Neu {li}, Update {lu} • Touren: Neu {ti}, Update {tu}")
                    st.rerun()
                except Exception as e:
                    st.error(f"Backup-Import fehlgeschlagen: {e}")

        st.markdown("---")

        tabs = st.tabs(["Abfahrten (inkl. manuell)", "Einrichtungen", "Touren", "Screens/Ticker"])
        with tabs[0]:
            show_admin_departures(conn, can_edit=True)
        with tabs[1]:
            show_admin_locations(conn, can_edit=True)
        with tabs[2]:
            show_admin_tours(conn, can_edit=True)
        with tabs[3]:
            show_admin_screens(conn, can_edit=True)

    else:
        st.info("Dispo-Ansicht: Touren und Abfahrten sehen, aber nichts löschen/ändern.")
        tabs = st.tabs(["Abfahrten", "Touren"])
        with tabs[0]:
            show_admin_departures(conn, can_edit=False)
        with tabs[1]:
            tours = load_tours(conn)
            if tours.empty:
                st.info("Noch keine Touren vorhanden.")
            else:
                screens = load_screens(conn)
                screen_map = {int(r["id"]): r["name"] for _, r in screens.iterrows()}
                view = tours.copy()
                view["Zeit"] = view.apply(lambda r: f"{int(r['hour']):02d}:{int(r.get('minute',0) or 0):02d}", axis=1)
                view["Monitore"] = view["screen_ids"].apply(
                    lambda s: ", ".join([f"{i}:{screen_map.get(i, f'Screen {i}')}" for i in parse_screen_ids(s)])
                )
                view["Countdown"] = view["countdown_enabled"].apply(lambda v: "Ja" if int(v) == 1 else "Nein")
                st.dataframe(view[["id", "name", "weekday", "Zeit", "Countdown", "location_name", "note", "active", "Monitore"]],
                             use_container_width=True)

# ==================================================
# Main
# ==================================================

def main():
    params = st.experimental_get_query_params()
    mode = params.get("mode", ["admin"])[0]
    screen_id_param = params.get("screenId", [None])[0]

    if mode == "display":
        try:
            screen_id = int(screen_id_param) if screen_id_param is not None else None
        except ValueError:
            screen_id = None
        show_display_mode(screen_id)
    else:
        show_admin_mode()

if __name__ == "__main__":
    main()
