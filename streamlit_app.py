# streamlit_app.py
import streamlit as st
import pandas as pd
import sqlite3
import json
import io
import time
import uuid
from datetime import datetime, timedelta, time as dtime
from pathlib import Path
from zoneinfo import ZoneInfo

from streamlit_autorefresh import st_autorefresh

# ==================================================
# KONFIGURATION
# ==================================================

st.set_page_config(page_title="Abfahrten", layout="wide")

DB_PATH = Path("abfahrten.db")
TZ = ZoneInfo("Europe/Berlin")

USERS = {
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
SUMMARY_ZONE_IDS = [1, 2, 3, 4, 8, 9]

ZONE_NAME_MAP = {
    1: "Zone A",
    2: "Zone B",
    3: "Zone C",
    4: "Zone D",
    8: "Wareneingang 1",
    9: "Wareneingang 2",
}


# ==================================================
# ZEIT
# ==================================================

def now_berlin() -> datetime:
    return datetime.now(TZ)

def ensure_tz(dt: datetime) -> datetime:
    if dt is None:
        return None
    if getattr(dt, "tzinfo", None) is None:
        return dt.replace(tzinfo=TZ)
    return dt.astimezone(TZ)

def parse_dt(value):
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        return ensure_tz(value)
    try:
        return ensure_tz(datetime.fromisoformat(str(value)))
    except Exception:
        try:
            return ensure_tz(pd.to_datetime(value).to_pydatetime())
        except Exception:
            return None

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
# LOGIN
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
            st.rerun()
        else:
            st.error("Benutzername oder Passwort ist falsch.")

    st.stop()


# ==================================================
# DATENBANK
# ==================================================

def integrity_ok(conn: sqlite3.Connection) -> bool:
    try:
        r = conn.execute("PRAGMA integrity_check;").fetchone()
        return not r or str(r[0]).lower() == "ok"
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
        refresh_interval_seconds INTEGER NOT NULL DEFAULT 20,
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
            (5, "Ãbersicht Links", "OVERVIEW", "ALLE", "", 20, 0, 0),
            (6, "Ãbersicht Rechts", "OVERVIEW", "ALLE", "", 20, 0, 0),
            (7, "Lagerstand Ãbersicht", "WAREHOUSE", "ALLE", "", 20, 0, 0),
            (8, "Wareneingang 1", "DETAIL", "ALLE", "", 15, 0, 0),
            (9, "Wareneingang 2", "DETAIL", "ALLE", "", 15, 0, 0),
        ]
        cur.executemany("""
        INSERT INTO screens
        (id, name, mode, filter_type, filter_locations, refresh_interval_seconds, holiday_flag, special_flag)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, defaults)
    else:
        for sid, name, mode, refresh in [
            (7, "Lagerstand Ãbersicht", "WAREHOUSE", 20),
            (8, "Wareneingang 1", "DETAIL", 15),
            (9, "Wareneingang 2", "DETAIL", 15),
        ]:
            cur.execute("SELECT COUNT(*) FROM screens WHERE id=?", (sid,))
            if cur.fetchone()[0] == 0:
                cur.execute("""
                INSERT INTO screens
                (id, name, mode, filter_type, filter_locations, refresh_interval_seconds, holiday_flag, special_flag)
                VALUES (?, ?, ?, 'ALLE', '', ?, 0, 0)
                """, (sid, name, mode, refresh))

    cur.execute("SELECT id FROM screens")
    for sid in [int(r[0]) for r in cur.fetchall()]:
        cur.execute("INSERT OR IGNORE INTO tickers (screen_id, text, active) VALUES (?, '', 0)", (sid,))

    cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_departures_source_key ON departures(source_key)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_departures_screen_id ON departures(screen_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_departures_datetime ON departures(datetime)")

    conn.commit()

def migrate_db(conn: sqlite3.Connection):
    init_db(conn)
    cur = conn.cursor()

    def table_cols(table: str):
        try:
            return {str(r[1]) for r in cur.execute(f"PRAGMA table_info({table})").fetchall()}
        except Exception:
            return set()

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
    if "screen_ids" not in tours:
        cur.execute("ALTER TABLE tours ADD COLUMN screen_ids TEXT")
    if "minute" not in tours:
        cur.execute("ALTER TABLE tours ADD COLUMN minute INTEGER NOT NULL DEFAULT 0")
    if "countdown_enabled" not in tours:
        cur.execute("ALTER TABLE tours ADD COLUMN countdown_enabled INTEGER NOT NULL DEFAULT 0")

    try:
        cur.execute("UPDATE screens SET mode='DETAIL' WHERE id IN (8,9) AND mode='WAREHOUSE'")
    except Exception:
        pass

    conn.commit()

@st.cache_resource
def get_connection():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False, timeout=30)
    conn.row_factory = sqlite3.Row
    try:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute("PRAGMA busy_timeout=30000")
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

    migrate_db(conn)
    return conn

def read_df(conn: sqlite3.Connection, query: str, params=()):
    return pd.read_sql_query(query, conn, params=params)

def load_locations(conn):
    return read_df(conn, "SELECT id, name, type, active, color, text_color FROM locations ORDER BY id")

def load_screens(conn):
    return read_df(conn, "SELECT * FROM screens ORDER BY id")

def ensure_columns(df: pd.DataFrame, required: dict):
    for c, v in required.items():
        if c not in df.columns:
            df[c] = v
    return df

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

    df = ensure_columns(df, {
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
        for c in ["datetime", "ready_at", "completed_at"]:
            df[c] = pd.to_datetime(df[c], errors="coerce")
            df[c] = df[c].apply(lambda x: ensure_tz(x.to_pydatetime()) if pd.notnull(x) else x)
        df["countdown_enabled"] = pd.to_numeric(df["countdown_enabled"], errors="coerce").fillna(1).astype(int)

    return df

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

def load_ticker_for_screen(conn, screen_id: int):
    df = read_df(conn, "SELECT screen_id, text, active FROM tickers WHERE screen_id=?", (int(screen_id),))
    return df.iloc[0] if not df.empty else None

def parse_screen_ids(screen_ids_value):
    if screen_ids_value is None:
        return []
    out = []
    for part in str(screen_ids_value).split(","):
        p = part.strip()
        if p.isdigit():
            out.append(int(p))
    return out


# ==================================================
# AUTOMATIK
# ==================================================

def update_departure_statuses(conn: sqlite3.Connection):
    now = now_berlin()
    now_iso = now.isoformat(timespec="seconds")
    df = read_df(conn, "SELECT id, datetime, status, ready_at, completed_at FROM departures")
    if df.empty:
        return

    df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce")
    df = df.dropna(subset=["datetime"])
    df["datetime"] = df["datetime"].apply(lambda x: ensure_tz(x.to_pydatetime()))

    to_ready, to_done = [], []
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

def materialize_tours_to_departures(conn, create_window_hours=MATERIALIZE_TOURS_HOURS_BEFORE):
    now = now_berlin()
    window = timedelta(hours=create_window_hours)
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

    df = df[(df["tour_active"] == 1) & (df["location_active"] == 1)]
    created_any = False
    cur = conn.cursor()

    for _, r in df.iterrows():
        weekday = str(r["weekday"])
        if weekday not in WEEKDAYS_DE:
            continue

        tour_id = int(r["tour_id"])
        pos = int(r["position"])
        loc_id = int(r["location_id"])
        hour = int(r["hour"])
        minute = int(r["minute"]) if int(r["minute"]) in (0, 30) else 0
        note = (r["tour_note"] or "").strip()
        screen_ids = parse_screen_ids(r["tour_screen_ids"])
        tour_cd = int(r.get("tour_countdown_enabled", 0) or 0)

        dep_dt = next_datetime_for_weekday_time(weekday, hour, minute)
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
                    VALUES (?, ?, '', 'GEPLANT', ?, ?, 'TOUR_AUTO', ?, ?)
                    """,
                    (dep_dt.isoformat(), loc_id, note, source_key, int(sid), int(tour_cd)),
                )
                created_any = True
            except sqlite3.IntegrityError:
                pass

    if created_any:
        conn.commit()

def create_manual_departures(conn, dep_dt, location_id, screen_ids, note, created_by, countdown_enabled):
    dep_dt = ensure_tz(dep_dt)
    cur = conn.cursor()
    for sid in screen_ids:
        sk = f"MANUAL:{uuid.uuid4().hex}:{sid}:{dep_dt.isoformat()}"
        cur.execute("""
            INSERT INTO departures (datetime, location_id, vehicle, status, note, source_key, created_by, screen_id, countdown_enabled)
            VALUES (?, ?, '', 'GEPLANT', ?, ?, ?, ?, ?)
        """, (
            dep_dt.isoformat(),
            int(location_id),
            (note or "").strip(),
            sk,
            created_by,
            int(sid),
            1 if countdown_enabled else 0,
        ))
    conn.commit()


# ==================================================
# SCREEN-DATEN
# ==================================================

def get_screen_data(conn, screen_id: int):
    screens = load_screens(conn)
    if screens.empty or int(screen_id) not in screens["id"].tolist():
        return None, pd.DataFrame()

    screen = screens.loc[screens["id"] == int(screen_id)].iloc[0]
    filter_type = screen["filter_type"] or "ALLE"
    filter_locations = (screen["filter_locations"] or "").strip()

    now = now_berlin()
    end = now + timedelta(hours=DISPLAY_WINDOW_HOURS)
    deps = load_departures_with_locations(conn)
    if deps.empty:
        return screen, deps

    deps = deps[deps["location_active"] == 1].copy()
    deps = deps[(deps["screen_id"].isna()) | (deps["screen_id"] == int(screen_id))]

    if filter_type != "ALLE":
        deps = deps[deps["location_type"] == filter_type]

    if filter_locations:
        ids = [int(x.strip()) for x in filter_locations.split(",") if x.strip().isdigit()]
        if ids:
            deps = deps[deps["location_id"].isin(ids)]

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

    if not deps.empty:
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
            parts = []
            ra = row.get("ready_at")
            if pd.notnull(ra):
                parts.append(f"BEREIT seit {fmt_compact(now - ensure_tz(ra))}")
            parts.append(f"Abschluss in {fmt_compact(completion_deadline(dep_dt) - now)}")
            return " Â· ".join(parts)
        return ""

    if not deps.empty:
        deps["line_info"] = deps.apply(build_line_info, axis=1)
        deps = deps.sort_values("datetime")

    return screen, deps

def get_next_two_departures_for_zone(conn, zone_screen_id: int):
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
# HTML
# ==================================================

def escape_html(text):
    return ("" if text is None else str(text)).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

def render_big_table(headers, rows, row_colors=None, text_colors=None):
    thead = "".join(f"<th>{escape_html(h)}</th>" for h in headers)
    body = ""
    rows = list(rows)
    for idx, r in enumerate(rows):
        style_parts = []
        if row_colors is not None and idx < len(row_colors) and row_colors[idx]:
            style_parts.append(f"background-color:{row_colors[idx]};")
        if text_colors is not None and idx < len(text_colors) and text_colors[idx]:
            style_parts.append(f"color:{text_colors[idx]};")
        style = f' style="{"".join(style_parts)}"' if style_parts else ""
        tds = "".join(f"<td>{c}</td>" for c in r)
        body += f"<tr{style}>{tds}</tr>"
    st.markdown(f"""
    <table class="big-table">
      <thead><tr>{thead}</tr></thead>
      <tbody>{body}</tbody>
    </table>
    """, unsafe_allow_html=True)


# ==================================================
# DISPLAY-MODUS
# ==================================================

def show_display_mode(screen_id: int):
    st.markdown("""
    <style>
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    header {visibility: hidden;}
    .block-container { padding-top: 0.5rem; padding-bottom: 3.4rem; }
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
    """, unsafe_allow_html=True)

    if not screen_id:
        st.error("Parameter 'screenId' fehlt oder ist ungÃ¼ltig.")
        return

    conn = get_connection()
    materialize_tours_to_departures(conn)
    update_departure_statuses(conn)

    screens = load_screens(conn)
    if screens.empty or int(screen_id) not in screens["id"].tolist():
        st.error(f"Screen {screen_id} ist nicht konfiguriert.")
        return

    screen = screens.loc[screens["id"] == int(screen_id)].iloc[0]
    interval_sec = int(screen.get("refresh_interval_seconds", 20))
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
        st.markdown(f"""
        <div style="display:flex;justify-content:center;align-items:center;height:100vh;width:100%;
                    background:#000;color:#fff;font-size:72px;font-weight:900;text-transform:uppercase;text-align:center;">
          - {escape_html(msg)} -
        </div>
        """, unsafe_allow_html=True)
        return

    # Screen 7 / Warehouse Ãbersicht
    if int(screen["id"]) == 7 or str(screen.get("mode", "")).upper() == "WAREHOUSE":
        st.markdown(f"## {escape_html(screen['name'])} (Screen {int(screen['id'])})")
        st.caption(f"DE Ortszeit: {now_berlin().strftime('%d.%m.%Y %H:%M:%S')} â¢ Anzeige: nÃ¤chste {DISPLAY_WINDOW_HOURS}h")

        rows = []
        for zid in SUMMARY_ZONE_IDS:
            zone_name = ZONE_NAME_MAP.get(zid, f"Screen {zid}")
            nxt = get_next_two_departures_for_zone(conn, zid)
            first = f"{nxt[0]['dt'].strftime('%a, %d.%m %H:%M')} â {escape_html(nxt[0]['location'])}" if len(nxt) >= 1 else "â"
            second = f"{nxt[1]['dt'].strftime('%a, %d.%m %H:%M')} â {escape_html(nxt[1]['location'])}" if len(nxt) >= 2 else "â"
            rows.append([escape_html(zone_name), first, second])
        render_big_table(["Zone", "NÃ¤chste Abfahrt", "ÃbernÃ¤chste Abfahrt"], rows)

        ticker = load_ticker_for_screen(conn, int(screen_id))
        if ticker is not None and int(ticker["active"]) == 1 and (ticker["text"] or "").strip():
            st.markdown(f"<div class='ticker'><div class='ticker__inner'>{escape_html(ticker['text'])}</div></div>", unsafe_allow_html=True)
        return

    st.markdown(f"## {escape_html(screen['name'])} (Screen {screen_id})")
    st.caption(f"DE Ortszeit: {now_berlin().strftime('%d.%m.%Y %H:%M:%S')} â¢ Anzeige: nÃ¤chste {DISPLAY_WINDOW_HOURS}h")

    screen_obj, data = get_screen_data(conn, int(screen_id))

    if data is None or data.empty:
        st.info("Keine Abfahrten im Zeitfenster.")
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
                return "<br/>".join(parts)

            subset["combined"] = subset.apply(combine, axis=1)
            rows = subset[["location_name", "combined"]].itertuples(index=False, name=None)
            render_big_table(
                ["Einrichtung", "Hinweis / Countdown"],
                rows,
                row_colors=subset["location_color"].fillna("").tolist(),
                text_colors=subset["location_text_color"].fillna("").tolist(),
            )
        else:
            for _, row in data.iterrows():
                note = row.get("note") or ""
                li = row.get("line_info") or ""
                dt = row.get("datetime")
                tstr = ensure_tz(dt).strftime("%H:%M") if pd.notnull(dt) else ""
                extra = ""
                if note or li:
                    extra = "<br/><span style='font-size:28px;'>" + escape_html(note)
                    if note and li:
                        extra += " Â· "
                    extra += escape_html(li) + "</span>"
                main = f"<b>{escape_html(tstr)} â {escape_html(row['location_name'])}</b>"
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
# EXPORT / IMPORT / BACKUP
# ==================================================

def df_to_csv_bytes(df: pd.DataFrame) -> bytes:
    out = io.StringIO()
    df.to_csv(out, index=False, sep=";", encoding="utf-8")
    return ("\ufeff" + out.getvalue()).encode("utf-8")

def export_locations_json(conn):
    payload = {
        "version": 1,
        "exported_at": now_berlin().isoformat(),
        "locations": load_locations(conn).to_dict(orient="records"),
    }
    return json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")

def import_locations_json(conn, data):
    cur = conn.cursor()
    inserted = updated = 0
    for loc in data.get("locations", []):
        loc_id = loc.get("id")
        name = (loc.get("name") or "").strip()
        typ = (loc.get("type") or "").strip()
        active = 1 if int(loc.get("active", 1)) == 1 else 0
        color = loc.get("color") or "#007bff"
        text_color = loc.get("text_color") or "#000000"
        if not name or typ not in ["KRANKENHAUS", "ALTENHEIM", "MVZ"]:
            continue
        if loc_id is not None:
            cur.execute("SELECT COUNT(*) FROM locations WHERE id=?", (int(loc_id),))
            if cur.fetchone()[0]:
                cur.execute("UPDATE locations SET name=?, type=?, active=?, color=?, text_color=? WHERE id=?",
                            (name, typ, active, color, text_color, int(loc_id)))
                updated += 1
            else:
                cur.execute("INSERT INTO locations (id, name, type, active, color, text_color) VALUES (?, ?, ?, ?, ?, ?)",
                            (int(loc_id), name, typ, active, color, text_color))
                inserted += 1
        else:
            cur.execute("INSERT INTO locations (name, type, active, color, text_color) VALUES (?, ?, ?, ?, ?)",
                        (name, typ, active, color, text_color))
            inserted += 1
    conn.commit()
    return inserted, updated

def export_tours_json(conn):
    tours = load_tours(conn)
    items = []
    for _, t in tours.iterrows():
        tid = int(t["id"])
        stops_df = load_tour_stops(conn, tid)
        stops = []
        for _, s in stops_df.iterrows():
            stops.append({"location_id": int(s["location_id"]), "position": int(s["position"])})
        items.append({
            "id": tid,
            "name": str(t["name"]),
            "weekday": str(t["weekday"]),
            "hour": int(t["hour"]),
            "minute": int(t["minute"] or 0),
            "note": str(t["note"] or ""),
            "active": int(t["active"]),
            "screen_ids": str(t["screen_ids"] or ""),
            "countdown_enabled": int(t["countdown_enabled"] or 0),
            "location_id": int(t["location_id"]),
            "stops": stops,
        })
    payload = {"version": 1, "exported_at": now_berlin().isoformat(), "tours": items}
    return json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")

def import_tours_json(conn, data):
    cur = conn.cursor()
    inserted = updated = 0
    for t in data.get("tours", []):
        tid = t.get("id")
        name = (t.get("name") or "").strip()
        weekday = (t.get("weekday") or "").strip()
        hour = int(t.get("hour", 0))
        minute = int(t.get("minute", 0) or 0)
        minute = minute if minute in (0, 30) else 0
        note = (t.get("note") or "").strip()
        active = 1 if int(t.get("active", 1)) == 1 else 0
        screen_ids = (t.get("screen_ids") or "").strip()
        cd = 1 if int(t.get("countdown_enabled", 0) or 0) == 1 else 0
        stops = sorted(t.get("stops", []), key=lambda x: int(x.get("position", 0)))

        stop_ids = [int(s["location_id"]) for s in stops if str(s.get("location_id", "")).isdigit()]
        if not stop_ids and t.get("location_id"):
            stop_ids = [int(t["location_id"])]
        if not name or weekday not in WEEKDAYS_DE or not stop_ids:
            continue
        primary_loc = stop_ids[0]

        if tid is not None:
            cur.execute("SELECT COUNT(*) FROM tours WHERE id=?", (int(tid),))
            if cur.fetchone()[0]:
                cur.execute("""
                    UPDATE tours
                    SET name=?, weekday=?, hour=?, minute=?, location_id=?, note=?, active=?, screen_ids=?, countdown_enabled=?
                    WHERE id=?
                """, (name, weekday, hour, minute, primary_loc, note, active, screen_ids, cd, int(tid)))
                updated += 1
            else:
                cur.execute("""
                    INSERT INTO tours (id, name, weekday, hour, minute, location_id, note, active, screen_ids, countdown_enabled)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (int(tid), name, weekday, hour, minute, primary_loc, note, active, screen_ids, cd))
                inserted += 1
            tour_id = int(tid)
        else:
            cur.execute("""
                INSERT INTO tours (name, weekday, hour, minute, location_id, note, active, screen_ids, countdown_enabled)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (name, weekday, hour, minute, primary_loc, note, active, screen_ids, cd))
            tour_id = cur.lastrowid
            inserted += 1

        cur.execute("DELETE FROM tour_stops WHERE tour_id=?", (tour_id,))
        for pos, loc_id in enumerate(stop_ids):
            cur.execute("INSERT INTO tour_stops (tour_id, location_id, position) VALUES (?, ?, ?)", (tour_id, int(loc_id), pos))
    conn.commit()
    return inserted, updated

def export_backup_json(conn):
    payload = {
        "version": 1,
        "exported_at": now_berlin().isoformat(),
        "locations": json.loads(export_locations_json(conn).decode("utf-8")).get("locations", []),
        "tours": json.loads(export_tours_json(conn).decode("utf-8")).get("tours", []),
    }
    return json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")

def import_backup_json(conn, data):
    li, lu = import_locations_json(conn, {"locations": data.get("locations", [])})
    ti, tu = import_tours_json(conn, {"tours": data.get("tours", [])})
    return (li, lu), (ti, tu)


# ==================================================
# ADMIN: EINRICHTUNGEN
# ==================================================

def show_admin_locations(conn, can_edit):
    st.subheader("Einrichtungen")
    locations = load_locations(conn)
    if locations.empty:
        st.info("Noch keine Einrichtungen vorhanden.")
    else:
        st.dataframe(locations, use_container_width=True)

    st.markdown("### Export")
    c1, c2 = st.columns(2)
    with c1:
        st.download_button("Download einrichtungen.json", data=export_locations_json(conn), file_name="einrichtungen.json", mime="application/json")
    with c2:
        st.download_button("Download einrichtungen.csv", data=df_to_csv_bytes(load_locations(conn)), file_name="einrichtungen.csv", mime="text/csv")

    if not can_edit:
        st.info("Nur Admin kann Einrichtungen bearbeiten.")
        return

    st.markdown("### Import")
    up = st.file_uploader("einrichtungen.json importieren", type=["json"], key="upl_locations")
    if up is not None:
        try:
            ins, upd = import_locations_json(conn, json.loads(up.getvalue().decode("utf-8")))
            st.success(f"Import erfolgreich. Neu: {ins}, Aktualisiert: {upd}")
            st.rerun()
        except Exception as e:
            st.error(f"Import fehlgeschlagen: {e}")

    st.markdown("### Neue Einrichtung")
    with st.form("new_location"):
        c1, c2, c3 = st.columns(3)
        with c1:
            name = st.text_input("Name")
            typ = st.selectbox("Typ", ["KRANKENHAUS", "ALTENHEIM", "MVZ"])
        with c2:
            active = st.checkbox("Aktiv", True)
        with c3:
            color = st.color_picker("Hintergrundfarbe", "#007bff")
            text_color = st.color_picker("Schriftfarbe", "#000000")
        submitted = st.form_submit_button("Speichern")

    if submitted:
        if not name.strip():
            st.error("Name darf nicht leer sein.")
        else:
            conn.execute("INSERT INTO locations (name, type, active, color, text_color) VALUES (?, ?, ?, ?, ?)",
                         (name.strip(), typ, 1 if active else 0, color, text_color))
            conn.commit()
            st.success("Einrichtung gespeichert.")
            st.rerun()

    locations = load_locations(conn)
    if locations.empty:
        return

    st.markdown("### Einrichtung bearbeiten / lÃ¶schen")
    selected = st.selectbox("Einrichtung auswÃ¤hlen", locations["id"].tolist(), key="loc_edit_select")
    row = locations.loc[locations["id"] == selected].iloc[0]

    with st.form("edit_location"):
        c1, c2, c3 = st.columns(3)
        with c1:
            name_edit = st.text_input("Name", row["name"])
            typ_options = ["KRANKENHAUS", "ALTENHEIM", "MVZ"]
            typ_edit = st.selectbox("Typ", typ_options, index=typ_options.index(row["type"]) if row["type"] in typ_options else 0)
        with c2:
            active_edit = st.checkbox("Aktiv", bool(row["active"]))
        with c3:
            color_edit = st.color_picker("Hintergrundfarbe", row["color"] if row["color"] else "#007bff")
            text_color_edit = st.color_picker("Schriftfarbe", row["text_color"] if row["text_color"] else "#000000")
        s1, s2 = st.columns(2)
        save = s1.form_submit_button("Ãnderungen speichern")
        delete = s2.form_submit_button("Einrichtung lÃ¶schen")

    if save:
        conn.execute("UPDATE locations SET name=?, type=?, active=?, color=?, text_color=? WHERE id=?",
                     (name_edit.strip(), typ_edit, 1 if active_edit else 0, color_edit, text_color_edit, int(selected)))
        conn.commit()
        st.success("Einrichtung aktualisiert.")
        st.rerun()

    if delete:
        dep_count = read_df(conn, "SELECT COUNT(*) AS c FROM departures WHERE location_id=?", (int(selected),)).iloc[0]["c"]
        tour_count = read_df(conn, "SELECT COUNT(*) AS c FROM tours WHERE location_id=?", (int(selected),)).iloc[0]["c"]
        stop_count = read_df(conn, "SELECT COUNT(*) AS c FROM tour_stops WHERE location_id=?", (int(selected),)).iloc[0]["c"]
        if dep_count or tour_count or stop_count:
            st.error(f"Kann nicht lÃ¶schen: Abfahrten={dep_count}, Touren={tour_count}, Tour-Stops={stop_count}")
        else:
            conn.execute("DELETE FROM locations WHERE id=?", (int(selected),))
            conn.commit()
            st.success("Einrichtung gelÃ¶scht.")
            st.rerun()


# ==================================================
# ADMIN: ABFAHRTEN
# ==================================================

def show_admin_departures(conn, can_edit):
    st.subheader("Abfahrten")
    materialize_tours_to_departures(conn)
    update_departure_statuses(conn)

    st.markdown("### Filter")
    c1, c2, c3, c4 = st.columns(4)
    with c1:
        source_filter = st.selectbox("Quelle", ["Alle", "Manuell", "Tour"])
    with c2:
        screen_filter = st.selectbox("Screen", ["Alle"] + [str(x) for x in load_screens(conn)["id"].tolist()])
    with c3:
        status_filter = st.selectbox("Status", ["Alle", "GEPLANT", "BEREIT", "ABGESCHLOSSEN"])
    with c4:
        hours_back = st.number_input("Stunden zurÃ¼ck", min_value=1, max_value=2000, value=168)

    hours_forward = st.number_input("Stunden voraus", min_value=0, max_value=2000, value=24)

    deps = load_departures_with_locations(conn)
    if not deps.empty:
        now = now_berlin()
        deps = deps[(deps["datetime"] >= now - timedelta(hours=int(hours_back))) & (deps["datetime"] <= now + timedelta(hours=int(hours_forward)))]
        if source_filter == "Manuell":
            deps = deps[deps["source_key"].astype(str).str.startswith("MANUAL:")]
        elif source_filter == "Tour":
            deps = deps[deps["source_key"].astype(str).str.startswith("TOUR:")]
        if screen_filter != "Alle":
            deps = deps[deps["screen_id"] == int(screen_filter)]
        if status_filter != "Alle":
            deps = deps[deps["status"].astype(str).str.upper() == status_filter]
        deps = deps.sort_values("datetime", ascending=False)

    if deps.empty:
        st.info("Keine Abfahrten gefunden.")
    else:
        view = deps.copy()
        view["Zeit"] = view["datetime"].apply(lambda d: ensure_tz(d).strftime("%d.%m.%Y %H:%M") if pd.notnull(d) else "")
        view["Quelle"] = view["source_key"].astype(str).apply(lambda s: "TOUR" if s.startswith("TOUR:") else ("MANUELL" if s.startswith("MANUAL:") else "SONST"))
        st.dataframe(view[["id", "Zeit", "screen_id", "location_name", "note", "status", "countdown_enabled", "Quelle", "source_key"]], use_container_width=True)

    if not can_edit:
        st.info("Keine Bearbeitungsrechte.")
        return

    st.markdown("### Manuelle Abfahrt anlegen")
    locations = load_locations(conn)
    screens = load_screens(conn)
    if locations.empty:
        st.warning("Bitte zuerst Einrichtungen anlegen.")
        return

    screen_map = {int(r["id"]): r["name"] for _, r in screens.iterrows()}

    with st.form("manual_dep_form"):
        c1, c2, c3 = st.columns(3)
        with c1:
            loc_id = st.selectbox("Einrichtung", locations["id"].tolist(), format_func=lambda i: locations.loc[locations["id"] == i, "name"].values[0])
            note = st.text_input("Hinweis")
        with c2:
            dep_date = st.date_input("Datum", value=now_berlin().date())
            time_label = st.selectbox("Uhrzeit", time_options_half_hour(), index=time_options_half_hour().index("08:00"))
            hh, mm = map(int, time_label.split(":"))
        with c3:
            screens_sel = st.multiselect("Screens", list(screen_map.keys()), default=[1], format_func=lambda sid: f"{sid}: {screen_map.get(sid)}")
            countdown_enabled = st.checkbox("Countdown aktiv", value=True)
        submitted = st.form_submit_button("Manuelle Abfahrt speichern")

    if submitted:
        if not screens_sel:
            st.error("Bitte mindestens einen Screen auswÃ¤hlen.")
        else:
            dep_dt = datetime.combine(dep_date, dtime(hour=hh, minute=mm)).replace(tzinfo=TZ)
            create_manual_departures(conn, dep_dt, int(loc_id), [int(s) for s in screens_sel], note, st.session_state.get("username", "ADMIN"), countdown_enabled)
            st.success("Manuelle Abfahrt angelegt.")
            st.rerun()

    st.markdown("### Manuelle Abfahrt lÃ¶schen")
    all_deps = load_departures_with_locations(conn)
    manual = all_deps[all_deps["source_key"].astype(str).str.startswith("MANUAL:")] if not all_deps.empty else pd.DataFrame()
    if manual.empty:
        st.info("Keine manuellen Abfahrten vorhanden.")
    else:
        delete_id = st.selectbox("Manuelle Abfahrt auswÃ¤hlen", manual["id"].tolist(), format_func=lambda i: f"ID {i}")
        if st.button("AusgewÃ¤hlte manuelle Abfahrt lÃ¶schen"):
            conn.execute("DELETE FROM departures WHERE id=?", (int(delete_id),))
            conn.commit()
            st.success("Manuelle Abfahrt gelÃ¶scht.")
            st.rerun()


# ==================================================
# ADMIN: TOUREN
# ==================================================

def show_admin_tours(conn, can_edit):
    st.subheader("Touren")
    locations = load_locations(conn)
    screens = load_screens(conn)

    if locations.empty:
        st.warning("Bitte zuerst Einrichtungen anlegen.")
        return

    screen_map = {int(r["id"]): r["name"] for _, r in screens.iterrows()}
    loc_options = locations["id"].tolist()
    screen_options = list(screen_map.keys())
    time_opts = time_options_half_hour()

    tours = load_tours(conn)
    if tours.empty:
        st.info("Noch keine Touren vorhanden.")
    else:
        view = tours.copy()
        view["Zeit"] = view.apply(lambda r: f"{int(r['hour']):02d}:{int(r.get('minute', 0) or 0):02d}", axis=1)
        view["Monitore"] = view["screen_ids"].apply(lambda s: ", ".join([f"{i}:{screen_map.get(i)}" for i in parse_screen_ids(s)]))
        view["Countdown"] = view["countdown_enabled"].apply(lambda v: "Ja" if int(v) == 1 else "Nein")
        st.dataframe(view[["id", "name", "weekday", "Zeit", "Countdown", "location_name", "note", "active", "Monitore"]], use_container_width=True)

    st.markdown("### Export / Import")
    c1, c2 = st.columns(2)
    with c1:
        st.download_button("Download touren.json", data=export_tours_json(conn), file_name="touren.json", mime="application/json")
    with c2:
        tour_csv = load_tours(conn)
        st.download_button("Download touren.csv", data=df_to_csv_bytes(tour_csv), file_name="touren.csv", mime="text/csv")

    if not can_edit:
        st.info("Nur Admin kann Touren bearbeiten.")
        return

    up = st.file_uploader("touren.json importieren", type=["json"], key="upl_tours")
    if up is not None:
        try:
            ins, upd = import_tours_json(conn, json.loads(up.getvalue().decode("utf-8")))
            st.success(f"Import erfolgreich. Neu: {ins}, Aktualisiert: {upd}")
            st.rerun()
        except Exception as e:
            st.error(f"Import fehlgeschlagen: {e}")

    st.markdown("### Neue Tour")
    with st.form("new_tour_form"):
        c1, c2, c3 = st.columns(3)
        with c1:
            tour_name = st.text_input("Tour-Name")
            weekday = st.selectbox("Wochentag", WEEKDAYS_DE)
        with c2:
            time_label = st.selectbox("Uhrzeit", time_opts, index=time_opts.index("08:00"))
            hour_int, minute_int = map(int, time_label.split(":"))
        with c3:
            screens_new = st.multiselect("Monitore/Screens", screen_options, format_func=lambda sid: f"{sid}: {screen_map.get(sid)}")
        countdown_enabled = st.checkbox("Countdown fÃ¼r diese Tour aktiv", value=False)
        stops_new = st.multiselect("Einrichtungen / Stops", loc_options, format_func=lambda i: locations.loc[locations["id"] == i, "name"].values[0])
        note_new = st.text_input("Hinweis")
        active_new = st.checkbox("Aktiv", True)
        submitted = st.form_submit_button("Tour speichern")

    if submitted:
        if not tour_name.strip():
            st.error("Tour-Name darf nicht leer sein.")
        elif not screens_new:
            st.error("Bitte mindestens einen Screen auswÃ¤hlen.")
        elif not stops_new:
            st.error("Bitte mindestens einen Stop auswÃ¤hlen.")
        else:
            cur = conn.cursor()
            cur.execute("""
                INSERT INTO tours (name, weekday, hour, minute, location_id, note, active, screen_ids, countdown_enabled)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                tour_name.strip(), weekday, hour_int, minute_int, int(stops_new[0]), note_new.strip(),
                1 if active_new else 0, ",".join(str(s) for s in screens_new), 1 if countdown_enabled else 0,
            ))
            tour_id = cur.lastrowid
            for pos, loc_id in enumerate(stops_new):
                cur.execute("INSERT INTO tour_stops (tour_id, location_id, position) VALUES (?, ?, ?)", (tour_id, int(loc_id), pos))
            conn.commit()
            st.success("Tour gespeichert.")
            st.rerun()

    tours = load_tours(conn)
    if tours.empty:
        return

    st.markdown("### Tour bearbeiten / lÃ¶schen")
    selected = st.selectbox("Tour auswÃ¤hlen", tours["id"].tolist(), key="edit_tour_select")
    row = tours.loc[tours["id"] == selected].iloc[0]
    stops_df = read_df(conn, "SELECT location_id FROM tour_stops WHERE tour_id=? ORDER BY position", (int(selected),))
    existing_stop_ids = stops_df["location_id"].tolist() if not stops_df.empty else [int(row["location_id"])]
    existing_screen_ids = parse_screen_ids(row["screen_ids"])
    cur_time = f"{int(row['hour']):02d}:{int(row.get('minute', 0) or 0):02d}"
    if cur_time not in time_opts:
        cur_time = "08:00"

    with st.form("edit_tour_form"):
        c1, c2, c3 = st.columns(3)
        with c1:
            name_edit = st.text_input("Tour-Name", row["name"])
            weekday_edit = st.selectbox("Wochentag", WEEKDAYS_DE, index=WEEKDAYS_DE.index(row["weekday"]) if row["weekday"] in WEEKDAYS_DE else 0)
        with c2:
            time_edit = st.selectbox("Uhrzeit", time_opts, index=time_opts.index(cur_time))
            hour_edit, minute_edit = map(int, time_edit.split(":"))
        with c3:
            screens_edit = st.multiselect("Monitore/Screens", screen_options, default=existing_screen_ids, format_func=lambda sid: f"{sid}: {screen_map.get(sid)}")
        countdown_edit = st.checkbox("Countdown fÃ¼r diese Tour aktiv", value=(int(row.get("countdown_enabled", 0) or 0) == 1))
        stops_edit = st.multiselect("Einrichtungen / Stops", loc_options, default=existing_stop_ids, format_func=lambda i: locations.loc[locations["id"] == i, "name"].values[0])
        note_edit = st.text_input("Hinweis", row["note"] or "")
        active_edit = st.checkbox("Aktiv", bool(row["active"]))
        s1, s2 = st.columns(2)
        save = s1.form_submit_button("Ãnderungen speichern")
        delete = s2.form_submit_button("Tour lÃ¶schen")

    if save:
        if not name_edit.strip():
            st.error("Tour-Name darf nicht leer sein.")
        elif not screens_edit:
            st.error("Bitte mindestens einen Screen auswÃ¤hlen.")
        elif not stops_edit:
            st.error("Bitte mindestens einen Stop auswÃ¤hlen.")
        else:
            cur = conn.cursor()
            cur.execute("""
                UPDATE tours
                SET name=?, weekday=?, hour=?, minute=?, location_id=?, note=?, active=?, screen_ids=?, countdown_enabled=?
                WHERE id=?
            """, (
                name_edit.strip(), weekday_edit, hour_edit, minute_edit, int(stops_edit[0]), note_edit.strip(),
                1 if active_edit else 0, ",".join(str(s) for s in screens_edit), 1 if countdown_edit else 0, int(selected),
            ))
            cur.execute("DELETE FROM tour_stops WHERE tour_id=?", (int(selected),))
            for pos, loc_id in enumerate(stops_edit):
                cur.execute("INSERT INTO tour_stops (tour_id, location_id, position) VALUES (?, ?, ?)", (int(selected), int(loc_id), pos))
            conn.commit()
            st.success("Tour aktualisiert.")
            st.rerun()

    if delete:
        cur = conn.cursor()
        cur.execute("DELETE FROM tour_stops WHERE tour_id=?", (int(selected),))
        cur.execute("DELETE FROM tours WHERE id=?", (int(selected),))
        conn.commit()
        st.success("Tour gelÃ¶scht.")
        st.rerun()


# ==================================================
# ADMIN: SCREENS
# ==================================================

def show_admin_screens(conn, can_edit):
    st.subheader("Screens / Monitore")
    screens = load_screens(conn)
    st.dataframe(screens, use_container_width=True)

    st.markdown("### Links")
    for _, r in screens.iterrows():
        sid = int(r["id"])
        link = f"?mode=display&screenId={sid}"
        st.markdown(f"- **Screen {sid} â {r['name']}**: [Monitor Ã¶ffnen]({link}) (`{link}`)")

    if not can_edit:
        st.info("Nur Admin kann Screens/Ticker bearbeiten.")
        return

    st.markdown("### Screen bearbeiten")
    selected = st.selectbox("Screen wÃ¤hlen", screens["id"].tolist(), key="screen_select")
    row = screens.loc[screens["id"] == selected].iloc[0]

    with st.form("edit_screen"):
        name = st.text_input("Name", row["name"])
        mode_opts = ["DETAIL", "OVERVIEW", "WAREHOUSE"]
        mode = st.selectbox("Modus", mode_opts, index=mode_opts.index(row["mode"]) if row["mode"] in mode_opts else 0)
        filter_opts = ["ALLE", "KRANKENHAUS", "ALTENHEIM", "MVZ"]
        filter_type = st.selectbox("Filter Typ", filter_opts, index=filter_opts.index(row["filter_type"]) if row["filter_type"] in filter_opts else 0)
        filter_locations = st.text_input("Filter Locations (IDs, Komma-getrennt)", row["filter_locations"] or "")
        refresh = st.number_input("Refresh-Intervall (Sekunden)", min_value=5, max_value=300, value=int(row["refresh_interval_seconds"]))
        holiday_flag = st.checkbox("Feiertagsbelieferung aktiv (Vollbild)", value=bool(row["holiday_flag"]))
        special_flag = st.checkbox("Sonderplan aktiv (Vollbild)", value=bool(row["special_flag"]))
        submitted = st.form_submit_button("Speichern")

    if submitted:
        conn.execute("""
            UPDATE screens
            SET name=?, mode=?, filter_type=?, filter_locations=?, refresh_interval_seconds=?, holiday_flag=?, special_flag=?
            WHERE id=?
        """, (name, mode, filter_type, filter_locations, int(refresh), 1 if holiday_flag else 0, 1 if special_flag else 0, int(selected)))
        conn.commit()
        st.success("Screen aktualisiert.")
        st.rerun()

    st.markdown("### Laufband / Ticker")
    trow = load_ticker_for_screen(conn, int(selected))
    with st.form("ticker_form"):
        text = st.text_area("Laufband-Text", value=(trow["text"] or "") if trow is not None else "", height=120)
        active = st.checkbox("Laufband aktiv", value=bool(trow["active"]) if trow is not None else False)
        submit = st.form_submit_button("Speichern")

    if submit:
        conn.execute("INSERT OR REPLACE INTO tickers (screen_id, text, active) VALUES (?, ?, ?)", (int(selected), text.strip(), 1 if active else 0))
        conn.commit()
        st.success("Laufband gespeichert.")
        st.rerun()


# ==================================================
# FRACHTBRIEF
# ==================================================

def _x(condition):
    return "X" if condition else ""

def show_frachtbrief():
    st.subheader("ð Frachtbrief drucken")

    col1, col2, col3 = st.columns(3)
    with col1:
        versanddatum = st.date_input("Versanddatum", value=now_berlin().date())
        wochentag = st.selectbox("Wochentag", WEEKDAYS_DE, index=now_berlin().weekday())
        mitarbeiter = st.text_input("Mitarbeiter")
    with col2:
        fahrtart = st.selectbox("Art der Fahrt", ["Regelfahrt", "Kurier", "Leergut", "Ãberhang"])
        fahrzeug = st.selectbox("Fahrzeug", ["PKW", "LKW 7,5 to", "LKW 12 to"])
        tour = st.selectbox("Tour / Lager", ["Lager 1", "Lager 2", "Lager 3", "Lager 4"])
    with col3:
        frueh_spaet = st.selectbox("FrÃ¼h / SpÃ¤t", ["FrÃ¼h", "SpÃ¤t"])
        abfahrt_lzg = st.text_input("Abfahrt LZG")
        ankunft_lzg = st.text_input("Ankunft LZG")

    st.markdown("### Lieferadresse")
    lieferadresse = st.text_area("Lieferadresse / Hauptadresse", height=90)

    st.markdown("### Warenlieferung")
    waren_rows = []
    for i in range(1, 7):
        with st.expander(f"Warenlieferung Zeile {i}", expanded=(i == 1)):
            c1, c2, c3, c4, c5, c6, c7 = st.columns([2, 2, 1.5, 1, 1, 1, 1])
            waren_rows.append({
                "empfaenger": c1.text_input("EmpfÃ¤nger", key=f"w_emp_{i}"),
                "adresse": c2.text_input("Adresse", key=f"w_adr_{i}"),
                "ort": c3.text_input("Ort", key=f"w_ort_{i}"),
                "rollgitter": c4.text_input("Rollgitter", key=f"w_rg_{i}"),
                "rogiwa": c5.text_input("RoGiWa", key=f"w_rogiwa_{i}"),
                "euro": c6.text_input("Euro-Pal.", key=f"w_euro_{i}"),
                "ladezeit": c7.text_input("Ladezeit", key=f"w_lade_{i}"),
            })

    st.markdown("### Leergut-RÃ¼ckgabe")
    leergut_rows = []
    for i in range(1, 7):
        with st.expander(f"Leergut Zeile {i}", expanded=False):
            c1, c2, c3 = st.columns([3, 1, 1])
            leergut_rows.append({
                "empfaenger": c1.text_input("EmpfÃ¤nger", key=f"l_emp_{i}"),
                "rollgitter": c2.text_input("Rollgitter", key=f"l_rg_{i}"),
                "euro": c3.text_input("Euro-Paletten", key=f"l_euro_{i}"),
            })

    name_druck = st.text_input("Name in Druckbuchstaben")
    frachtfuehrer = st.text_input("Datum / FrachtfÃ¼hrer")
    datum_lzg = st.text_input("Datum / LZG", value=versanddatum.strftime("%d.%m.%Y"))

    if st.button("ð¨ï¸ Druckansicht anzeigen", type="primary"):
        render_frachtbrief_html(
            versanddatum=versanddatum.strftime("%d.%m.%Y"),
            wochentag=wochentag,
            mitarbeiter=mitarbeiter,
            fahrtart=fahrtart,
            fahrzeug=fahrzeug,
            tour=tour,
            frueh_spaet=frueh_spaet,
            abfahrt_lzg=abfahrt_lzg,
            ankunft_lzg=ankunft_lzg,
            lieferadresse=lieferadresse,
            waren_rows=waren_rows,
            leergut_rows=leergut_rows,
            name_druck=name_druck,
            frachtfuehrer=frachtfuehrer,
            datum_lzg=datum_lzg,
        )

def render_frachtbrief_html(
    versanddatum, wochentag, mitarbeiter, fahrtart, fahrzeug, tour, frueh_spaet,
    abfahrt_lzg, ankunft_lzg, lieferadresse, waren_rows, leergut_rows,
    name_druck, frachtfuehrer, datum_lzg
):
    def td(v):
        return escape_html(v)

    waren_html = ""
    for r in waren_rows:
        waren_html += f"""
        <tr>
          <td>{td(r["empfaenger"])}</td>
          <td>{td(r["adresse"])}</td>
          <td>{td(r["ort"])}</td>
          <td>{td(r["rollgitter"])}</td>
          <td>{td(r["rogiwa"])}</td>
          <td>{td(r["euro"])}</td>
          <td>{td(r["ladezeit"])}</td>
        </tr>
        """

    leergut_html = ""
    for r in leergut_rows:
        leergut_html += f"""
        <tr>
          <td>{td(r["empfaenger"])}</td>
          <td>{td(r["rollgitter"])}</td>
          <td>{td(r["euro"])}</td>
        </tr>
        """

    html = f"""
    <!DOCTYPE html>
    <html>
    <head>
    <meta charset="utf-8">
    <style>
      @media print {{
        body {{ margin: 0; background:white; }}
        .no-print {{ display:none; }}
        .page {{ box-shadow:none; margin:0; }}
      }}
      body {{ background:#ddd; font-family:Arial, Helvetica, sans-serif; }}
      .page {{
        width:210mm; min-height:297mm; background:white; margin:20px auto;
        padding:12mm; box-sizing:border-box; color:#000;
      }}
      .top {{ display:grid; grid-template-columns:1.4fr 1fr; gap:20mm; align-items:start; }}
      .title-box {{ border:2px solid #000; font-size:34px; font-weight:900; padding:4px 10px; width:85mm; margin-bottom:8mm; }}
      .copy-text {{ font-size:12px; margin-left:3mm; line-height:1.4; }}
      .logo-box {{ border:2px solid #000; font-size:18px; font-weight:900; padding:8px; width:62mm; text-align:center; margin-left:auto; }}
      .upper-grid {{ display:grid; grid-template-columns:1.45fr 0.78fr; gap:20mm; margin-top:10mm; }}
      .box {{ border:2px solid #000; }}
      .box-title {{ font-size:24px; font-weight:900; border-bottom:2px solid #000; padding:3px 5px; }}
      .box-content {{ min-height:28mm; padding:7px; font-size:14px; white-space:pre-line; }}
      .gray {{ background:#d9d9d9; }}
      .spedition {{ font-size:13px; line-height:1.5; }}
      table {{ border-collapse:collapse; width:100%; table-layout:fixed; }}
      td, th {{ border:1.7px solid #000; padding:3px 4px; font-size:12px; height:7mm; vertical-align:middle; }}
      th {{ font-weight:900; text-align:left; }}
      .mini-table {{ margin-top:5mm; }}
      .mini-table td {{ text-align:center; font-weight:700; }}
      .mini-label {{ font-style:italic; text-align:left !important; font-weight:900 !important; }}
      .section-title {{ font-size:25px; font-weight:900; border:2px solid #000; border-bottom:none; padding:2px 5px; background:#d9d9d9; margin-top:6mm; }}
      .data-head td {{ font-weight:900; }}
      .sum-row td {{ font-weight:900; text-align:right; }}
      .signature {{ margin-top:4mm; }}
      .signature td {{ height:8mm; font-weight:900; }}
      .center {{ text-align:center; }}
      .italic {{ font-style:italic; }}
      .no-print {{ text-align:center; margin:20px; }}
      .print-btn {{ font-size:18px; padding:12px 22px; cursor:pointer; }}
    </style>
    </head>
    <body>
    <div class="no-print"><button class="print-btn" onclick="window.print()">Drucken / als PDF speichern</button></div>
    <div class="page">
      <div class="top">
        <div>
          <div class="title-box">Frachtbrief</div>
          <div class="copy-text">1 Exemplar behÃ¤lt der Spediteur<br>1 Exemplar geht zurÃ¼ck an das LZG</div>
        </div>
        <div class="logo-box">Johannesstift<br>Diakonie<br>Services</div>
      </div>

      <div class="upper-grid">
        <div class="box">
          <div class="box-title">Lieferadresse (Haupt-)</div>
          <div class="box-content">
            <div class="gray" style="padding:4px; margin:-7px -7px 7px -7px;">Adresse markieren</div>
            {td(lieferadresse)}
          </div>
        </div>
        <div class="box">
          <div class="box-title">Spediteur</div>
          <div class="box-content spedition">
            Billhardt Transport und<br>Logistik GmbH<br><br>
            Siemensring 5-9<br><br>
            14641 Nauen<br><br>
            Tel. 030 / 680783340
          </div>
        </div>
      </div>

      <table class="mini-table">
        <tr>
          <td class="mini-label">Art der Fahrt:</td>
          <td>Regelfahrt<br>{_x(fahrtart == "Regelfahrt")}</td>
          <td>Kurier<br>{_x(fahrtart == "Kurier")}</td>
          <td>Leergut<br>{_x(fahrtart == "Leergut")}</td>
          <td>Ãberhang<br>{_x(fahrtart == "Ãberhang")}</td>
          <td>Tour<br>{td(tour)}</td>
          <td>FrÃ¼h<br>{_x(frueh_spaet == "FrÃ¼h")}</td>
          <td>SpÃ¤t<br>{_x(frueh_spaet == "SpÃ¤t")}</td>
        </tr>
        <tr>
          <td class="mini-label">Fahrzeug:</td>
          <td>PKW<br>{_x(fahrzeug == "PKW")}</td>
          <td colspan="2">LKW 7,5 to<br>{_x(fahrzeug == "LKW 7,5 to")}</td>
          <td colspan="2">LKW 12 to<br>{_x(fahrzeug == "LKW 12 to")}</td>
          <td colspan="2"></td>
        </tr>
        <tr>
          <td class="mini-label">Abfahrt LZG:</td>
          <td>{td(abfahrt_lzg)}</td>
          <td class="mini-label" colspan="2">Ankunft LZG:</td>
          <td colspan="2">{td(ankunft_lzg)}</td>
          <td colspan="2"></td>
        </tr>
      </table>

      <div class="section-title">Warenlieferung (vom LZG auszufÃ¼llen)</div>
      <table>
        <tr class="data-head">
          <td style="width:17%;">Versanddatum:</td>
          <td style="width:17%;">{td(versanddatum)}</td>
          <td style="width:17%;">Wochentag:</td>
          <td style="width:18%;">{td(wochentag)}</td>
          <td style="width:15%;">Mitarbeiter:</td>
          <td colspan="2">{td(mitarbeiter)}</td>
        </tr>
      </table>

      <table>
        <tr>
          <th style="width:28%;">EmpfÃ¤nger (Haupt- + weitere)</th>
          <th style="width:16%;">Adresse</th>
          <th style="width:14%;">Ort</th>
          <th style="width:14%;">Rollgitter-<br>wagen</th>
          <th style="width:12%;">RoGiWa<br>(unkompr.)</th>
          <th style="width:9%;">Euro-<br>Paletten</th>
          <th style="width:11%;">Ladezeit<br>Klinik</th>
        </tr>
        {waren_html}
        <tr class="sum-row"><td colspan="3"></td><td colspan="2">Summe:</td><td class="center">0</td><td></td></tr>
      </table>

      <table class="signature">
        <tr>
          <td style="width:28%;" class="italic center">Ware erhalten:</td>
          <td style="width:15%;" class="center">{td(versanddatum)}</td>
          <td style="width:28%;" class="italic center">Name in Druckbuchstaben:</td>
          <td>{td(name_druck)}</td>
        </tr>
      </table>

      <div class="section-title" style="margin-top:10mm;">Leergut - RÃ¼ckgabe (von der Spediteur auszufÃ¼llen)</div>
      <table>
        <tr class="data-head">
          <td style="width:17%;">RÃ¼ckgabedatum:</td>
          <td style="width:17%;">{td(versanddatum)}</td>
          <td style="width:15%;">Mitarbeiter</td>
          <td colspan="2"></td>
        </tr>
      </table>

      <table>
        <tr>
          <th style="width:60%;">EmpfÃ¤nger (Haupt- + weitere)</th>
          <th style="width:20%;">Rollgitter-<br>wagen</th>
          <th style="width:20%;">Euro-<br>Paletten</th>
        </tr>
        {leergut_html}
        <tr class="sum-row"><td>Summe:</td><td></td><td></td></tr>
      </table>

      <table class="signature">
        <tr>
          <td style="width:28%;" class="italic center">Leergut erhalten:</td>
          <td style="width:15%;" class="center">{td(versanddatum)}</td>
          <td style="width:28%;" class="italic center">Datum / FrachtfÃ¼hrer<br>{td(frachtfuehrer)}</td>
          <td class="italic center">Datum / LZG<br>{td(datum_lzg)}</td>
        </tr>
      </table>
    </div>
    </body>
    </html>
    """

    st.components.v1.html(html, height=1200, scrolling=True)


# ==================================================
# ADMIN-MODUS
# ==================================================

def show_admin_mode():
    require_login()

    role = st.session_state.get("role", "viewer")
    username = st.session_state.get("username", "")
    can_edit = role == "admin"

    st.title("Abfahrten â Admin / Disposition")
    st.caption(f"Eingeloggt als: {username} (Rolle: {role}) â¢ DE Ortszeit: {now_berlin().strftime('%d.%m.%Y %H:%M:%S')}")

    if st.sidebar.button("Logout"):
        st.session_state.clear()
        st.rerun()

    conn = get_connection()
    materialize_tours_to_departures(conn)
    update_departure_statuses(conn)

    if can_edit:
        st.markdown("### Backup")
        b1, b2 = st.columns(2)
        with b1:
            st.download_button("Backup herunterladen", data=export_backup_json(conn), file_name="backup_abfahrten.json", mime="application/json")
        with b2:
            up = st.file_uploader("Backup importieren", type=["json"], key="upl_backup")
            if up is not None:
                try:
                    (li, lu), (ti, tu) = import_backup_json(conn, json.loads(up.getvalue().decode("utf-8")))
                    st.success(f"Backup importiert. Einrichtungen: Neu {li}, Update {lu} â¢ Touren: Neu {ti}, Update {tu}")
                    st.rerun()
                except Exception as e:
                    st.error(f"Backup-Import fehlgeschlagen: {e}")

        tabs = st.tabs(["Abfahrten", "Einrichtungen", "Touren", "Screens/Ticker", "Frachtbrief"])
        with tabs[0]:
            show_admin_departures(conn, can_edit=True)
        with tabs[1]:
            show_admin_locations(conn, can_edit=True)
        with tabs[2]:
            show_admin_tours(conn, can_edit=True)
        with tabs[3]:
            show_admin_screens(conn, can_edit=True)
        with tabs[4]:
            show_frachtbrief()
    else:
        st.info("Dispo-Ansicht: Touren und Abfahrten sehen, aber nichts lÃ¶schen/Ã¤ndern.")
        tabs = st.tabs(["Abfahrten", "Touren", "Frachtbrief"])
        with tabs[0]:
            show_admin_departures(conn, can_edit=False)
        with tabs[1]:
            show_admin_tours(conn, can_edit=False)
        with tabs[2]:
            show_frachtbrief()


# ==================================================
# MAIN
# ==================================================

def get_query_param(name, default=None):
    params = st.query_params
    val = params.get(name, default)
    if isinstance(val, list):
        return val[0] if val else default
    return val

def main():
    mode = get_query_param("mode", "admin")
    screen_id_param = get_query_param("screenId", None)

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
