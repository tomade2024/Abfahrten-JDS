import streamlit as st
import pandas as pd
import sqlite3
import json
from datetime import datetime, timedelta, time as dtime
from pathlib import Path
from zoneinfo import ZoneInfo

from streamlit_autorefresh import st_autorefresh

# --------------------------------------------------
# Konfiguration
# --------------------------------------------------

st.set_page_config(page_title="Abfahrten", layout="wide")
DB_PATH = Path("abfahrten.db")

TZ = ZoneInfo("Europe/Berlin")

USERS = {
    "admin": {"password": "admin123", "role": "admin"},
    "dispo": {"password": "dispo123", "role": "viewer"},
}

WEEKDAYS_DE = ["Montag", "Dienstag", "Mittwoch", "Donnerstag", "Freitag", "Samstag", "Sonntag"]
WEEKDAY_TO_INT = {name: i for i, name in enumerate(WEEKDAYS_DE)}  # Montag=0

COUNTDOWN_START_HOURS = 3
AUTO_COMPLETE_AFTER_MIN = 20
KEEP_COMPLETED_MINUTES = 10

MATERIALIZE_TOURS_HOURS_BEFORE = 3


# --------------------------------------------------
# Zeit Helpers (DE Ortszeit)
# --------------------------------------------------

def now_berlin() -> datetime:
    return datetime.now(TZ)

def ensure_tz(dt: datetime) -> datetime:
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=TZ)
    return dt.astimezone(TZ)

def next_datetime_for_weekday_hour(weekday_name: str, hour: int) -> datetime:
    now = now_berlin()
    target = WEEKDAY_TO_INT[weekday_name]
    today = now.weekday()

    days_ahead = (target - today) % 7
    candidate_date = now.date() + timedelta(days=days_ahead)
    candidate_dt = datetime.combine(candidate_date, dtime(hour=hour, minute=0)).replace(tzinfo=TZ)

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


# --------------------------------------------------
# Login
# --------------------------------------------------

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


# --------------------------------------------------
# DB / Init
# --------------------------------------------------

@st.cache_resource
def get_connection():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    init_db(conn)
    return conn


def init_db(conn: sqlite3.Connection):
    cur = conn.cursor()

    # Einrichtungen
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

    # Abfahrten
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS departures (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            datetime     TEXT NOT NULL,
            location_id  INTEGER NOT NULL,
            vehicle      TEXT,
            status       TEXT NOT NULL DEFAULT 'GEPLANT',
            note         TEXT,
            ready_at     TEXT,
            completed_at TEXT,
            source_key   TEXT,
            created_by   TEXT,
            screen_id    INTEGER,
            FOREIGN KEY(location_id) REFERENCES locations(id)
        )
        """
    )
    cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_departures_source_key ON departures(source_key)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_departures_screen_id ON departures(screen_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_departures_datetime ON departures(datetime)")

    # Touren
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS tours (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            name         TEXT NOT NULL,
            weekday      TEXT NOT NULL,
            hour         INTEGER NOT NULL,
            location_id  INTEGER NOT NULL,
            note         TEXT,
            active       INTEGER NOT NULL DEFAULT 1,
            screen_ids   TEXT,
            FOREIGN KEY(location_id) REFERENCES locations(id)
        )
        """
    )

    # Tour-Stopps
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

    # Screens
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

    # Ticker pro Screen
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS tickers (
            screen_id INTEGER PRIMARY KEY,
            text      TEXT,
            active    INTEGER NOT NULL DEFAULT 0
        )
        """
    )

    # Default Screens (1-7)
    cur.execute("SELECT COUNT(*) AS cnt FROM screens")
    if cur.fetchone()[0] == 0:
        defaults = [
            (1, "Zone A",              "DETAIL",   "ALLE", "", 15, 0, 0),
            (2, "Zone B",              "DETAIL",   "ALLE", "", 15, 0, 0),
            (3, "Zone C",              "DETAIL",   "ALLE", "", 15, 0, 0),
            (4, "Zone D",              "DETAIL",   "ALLE", "", 15, 0, 0),
            (5, "Übersicht Links",     "OVERVIEW", "ALLE", "", 20, 0, 0),
            (6, "Übersicht Rechts",    "OVERVIEW", "ALLE", "", 20, 0, 0),
            (7, "Lagerstand Übersicht","WAREHOUSE","ALLE", "", 20, 0, 0),
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
        cur.execute("SELECT COUNT(*) FROM screens WHERE id=7")
        if cur.fetchone()[0] == 0:
            cur.execute(
                """
                INSERT INTO screens
                (id, name, mode, filter_type, filter_locations, refresh_interval_seconds, holiday_flag, special_flag)
                VALUES (7, 'Lagerstand Übersicht', 'WAREHOUSE', 'ALLE', '', 20, 0, 0)
                """
            )

    # Ticker Einträge sicherstellen
    cur.execute("SELECT id FROM screens")
    screen_ids = [int(r[0]) for r in cur.fetchall()]
    for sid in screen_ids:
        cur.execute("INSERT OR IGNORE INTO tickers (screen_id, text, active) VALUES (?, '', 0)", (sid,))

    conn.commit()


# --------------------------------------------------
# DB Helpers
# --------------------------------------------------

def read_df(conn: sqlite3.Connection, query: str, params=()):
    return pd.read_sql_query(query, conn, params=params)

def load_locations(conn):
    return read_df(conn, "SELECT id, name, type, active, color, text_color FROM locations ORDER BY id")

def load_screens(conn):
    return read_df(conn, "SELECT * FROM screens ORDER BY id")

def load_departures_with_locations(conn):
    df = read_df(
        conn,
        """
        SELECT d.id,
               d.datetime,
               d.location_id,
               d.vehicle,
               d.status,
               d.note,
               d.ready_at,
               d.completed_at,
               d.source_key,
               d.created_by,
               d.screen_id,
               l.name       AS location_name,
               l.type       AS location_type,
               l.active     AS location_active,
               l.color      AS location_color,
               l.text_color AS location_text_color
        FROM departures d
        JOIN locations l ON d.location_id = l.id
        """,
    )
    if not df.empty:
        df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce")
        df["ready_at"] = pd.to_datetime(df["ready_at"], errors="coerce")
        df["completed_at"] = pd.to_datetime(df["completed_at"], errors="coerce")

        df["datetime"] = df["datetime"].apply(lambda x: ensure_tz(x.to_pydatetime()) if pd.notnull(x) else x)
        df["ready_at"] = df["ready_at"].apply(lambda x: ensure_tz(x.to_pydatetime()) if pd.notnull(x) else x)
        df["completed_at"] = df["completed_at"].apply(lambda x: ensure_tz(x.to_pydatetime()) if pd.notnull(x) else x)
    return df

def load_tours(conn):
    return read_df(
        conn,
        """
        SELECT t.id,
               t.name,
               t.weekday,
               t.hour,
               t.location_id,
               t.note,
               t.active,
               t.screen_ids,
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


# --------------------------------------------------
# Parsing Screen IDs
# --------------------------------------------------

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


# --------------------------------------------------
# Status Automation
# --------------------------------------------------

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


# --------------------------------------------------
# Materialize Tours -> Departures (per Screen)
# --------------------------------------------------

def materialize_tours_to_departures(conn: sqlite3.Connection, create_window_hours: int = MATERIALIZE_TOURS_HOURS_BEFORE):
    now = now_berlin()
    window = timedelta(hours=create_window_hours)

    df = read_df(
        conn,
        """
        SELECT t.id         AS tour_id,
               t.weekday,
               t.hour,
               t.note       AS tour_note,
               t.active     AS tour_active,
               t.screen_ids AS tour_screen_ids,
               ts.location_id,
               ts.position,
               l.active     AS location_active
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
        note = (r["tour_note"] or "").strip()
        screen_ids = parse_screen_ids(r["tour_screen_ids"])

        if not screen_ids:
            continue

        dep_dt = next_datetime_for_weekday_hour(weekday, hour)

        if dep_dt - now > window:
            continue
        if now - dep_dt > timedelta(days=1):
            continue

        for sid in screen_ids:
            source_key = f"TOUR:{tour_id}:{pos}:{sid}:{dep_dt.isoformat()}"
            try:
                cur.execute(
                    """
                    INSERT INTO departures (datetime, location_id, vehicle, status, note, source_key, created_by, screen_id)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (dep_dt.isoformat(), loc_id, "", "GEPLANT", note, source_key, "TOUR_AUTO", int(sid)),
                )
                created_any = True
            except sqlite3.IntegrityError:
                pass

    if created_any:
        conn.commit()


# --------------------------------------------------
# Screen 7: Next tour per screen 1-4 (from tours)
# --------------------------------------------------

def get_next_tour_for_screen(conn: sqlite3.Connection, screen_id: int):
    tours = load_tours(conn)
    if tours.empty:
        return None

    tours = tours[(tours["active"] == 1)]
    tours = tours[tours["screen_ids"].apply(lambda v: screen_id in parse_screen_ids(v))]
    if tours.empty:
        return None

    best_row = None
    best_dt = None
    for _, t in tours.iterrows():
        try:
            dt = next_datetime_for_weekday_hour(str(t["weekday"]), int(t["hour"]))
        except Exception:
            continue
        if best_dt is None or dt < best_dt:
            best_dt = dt
            best_row = t

    if best_row is None:
        return None

    stops_df = load_tour_stops(conn, int(best_row["id"]))
    stops = stops_df["location_name"].tolist() if not stops_df.empty else [str(best_row.get("location_name") or "")]

    return {
        "tour_id": int(best_row["id"]),
        "tour_name": str(best_row["name"]),
        "next_dt": best_dt,
        "stops": [s for s in stops if s],
        "note": str(best_row.get("note") or ""),
    }


# --------------------------------------------------
# Screen Data (departures)
# --------------------------------------------------

def get_screen_data(conn: sqlite3.Connection, screen_id: int):
    screens = load_screens(conn)
    if screens.empty or screen_id not in screens["id"].tolist():
        return None, None

    screen = screens.loc[screens["id"] == screen_id].iloc[0]
    filter_type = screen["filter_type"] or "ALLE"
    filter_locations = (screen["filter_locations"] or "").strip()

    now = now_berlin()

    deps = load_departures_with_locations(conn)
    if deps.empty:
        return screen, deps

    deps = deps[deps["location_active"] == 1].copy()

    # Touren nur auf zugewiesenem Screen; manuelle screen_id NULL = global
    deps = deps[(deps["screen_id"].isna()) | (deps["screen_id"] == int(screen_id))]

    window_start = now - timedelta(minutes=AUTO_COMPLETE_AFTER_MIN)
    deps = deps[deps["datetime"] >= window_start]

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

    if filter_type != "ALLE":
        deps = deps[deps["location_type"] == filter_type]

    if filter_locations:
        ids = [int(x.strip()) for x in filter_locations.split(",") if x.strip().isdigit()]
        if ids:
            deps = deps[deps["location_id"].isin(ids)]

    def build_line_info(row):
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
    deps = deps.sort_values("datetime")
    return screen, deps


# --------------------------------------------------
# HTML helpers
# --------------------------------------------------

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


# --------------------------------------------------
# Import/Export JSON
# --------------------------------------------------

def export_locations_json(conn) -> bytes:
    df = load_locations(conn)
    payload = {
        "version": 1,
        "exported_at": now_berlin().isoformat(),
        "locations": df.to_dict(orient="records"),
    }
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
            "note": str(t["note"] or ""),
            "active": int(t["active"]),
            "screen_ids": str(t["screen_ids"] or ""),
            "primary_location_id": int(t["location_id"]),
            "primary_location_name": str(t["location_name"]),
            "stops": stops,
        })

    payload = {
        "version": 1,
        "exported_at": now_berlin().isoformat(),
        "tours": items,
    }
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
        note = (t.get("note") or "").strip()
        active = 1 if int(t.get("active", 1)) == 1 else 0
        screen_ids = (t.get("screen_ids") or "").strip()

        if not name or weekday not in WEEKDAYS_DE or not (0 <= hour <= 23):
            continue

        primary_loc_id = _resolve_location_id(
            conn,
            t.get("primary_location_id", None),
            t.get("primary_location_name", None),
        )

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
                    SET name=?, weekday=?, hour=?, location_id=?, note=?, active=?, screen_ids=?
                    WHERE id=?
                    """,
                    (name, weekday, hour, int(primary_loc_id), note, active, screen_ids, int(tour_id)),
                )
                updated += 1
            else:
                cur.execute(
                    """
                    INSERT INTO tours (id, name, weekday, hour, location_id, note, active, screen_ids)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (int(tour_id), name, weekday, hour, int(primary_loc_id), note, active, screen_ids),
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
                INSERT INTO tours (name, weekday, hour, location_id, note, active, screen_ids)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (name, weekday, hour, int(primary_loc_id), note, active, screen_ids),
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


# --------------------------------------------------
# Display Mode
# --------------------------------------------------

def show_display_mode(screen_id: int):
    st.markdown(
        """
        <style>
        #MainMenu {visibility: hidden;}
        footer {visibility: hidden;}
        header {visibility: hidden;}
        .block-container { padding-top: 0.5rem; }
        body, .block-container, .stMarkdown, .stText, .stDataFrame, div, span { font-size: 32px !important; }

        .big-table { width: 100%; border-collapse: collapse; }
        .big-table th, .big-table td {
            border-bottom: 1px solid #555;
            padding: 0.4em 0.8em;
            text-align: left;
            vertical-align: top;
        }
        .big-table th { font-weight: 800; }

        .ticker {
            position: fixed; bottom: 0; left: 0; width: 100%;
            background: #000; color: #fff;
            overflow: hidden; white-space: nowrap; z-index: 9999;
        }
        .ticker__inner {
            display: inline-block;
            padding-left: 100%;
            animation: ticker-scroll 20s linear infinite;
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
        st.markdown(
            "<style>body, .block-container { background-color:#000 !important; color:#fff !important; }</style>",
            unsafe_allow_html=True,
        )
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

    if int(screen["id"]) == 7 or str(screen.get("mode", "")).upper() == "WAREHOUSE":
        st.markdown(f"## {screen['name']} (Screen 7)")
        st.caption(f"Aktualisierung alle {interval_sec} Sekunden • DE Ortszeit: {now_berlin().strftime('%d.%m.%Y %H:%M:%S')}")

        rows = []
        for sid in [1, 2, 3, 4]:
            info = get_next_tour_for_screen(conn, sid)
            if not info:
                rows.append([f"Screen {sid}", "—", "—", "—"])
                continue
            dt_str = info["next_dt"].strftime("%a, %d.%m %H:%M")
            stops = ", ".join(info["stops"][:10]) + (" …" if len(info["stops"]) > 10 else "")
            rows.append([f"Screen {sid}", dt_str, info["tour_name"], stops])

        render_big_table(["Zone/Screen", "Nächste Tour", "Tour", "Stops"], rows)

        ticker = load_ticker_for_screen(conn, 7)
        if ticker is not None and int(ticker["active"]) == 1 and (ticker["text"] or "").strip():
            st.markdown(f"<div class='ticker'><div class='ticker__inner'>{escape_html(ticker['text'])}</div></div>",
                        unsafe_allow_html=True)
        return

    st.markdown(f"## {screen['name']} (Screen {screen_id})")
    st.caption(f"Modus: {screen['mode']} • DE Ortszeit: {now_berlin().strftime('%d.%m.%Y %H:%M:%S')}")

    screen_obj, data = get_screen_data(conn, int(screen_id))

    if data is None or data.empty:
        st.info("Keine Abfahrten.")
    else:
        if str(screen_obj["mode"]).upper() == "DETAIL" and int(screen_obj["id"]) in [1, 2, 3, 4]:
            subset = data[["location_name", "note", "line_info", "location_color", "location_text_color"]].copy()
            subset["note"] = subset["note"].fillna("")
            subset["line_info"] = subset["line_info"].fillna("")
            subset["combined"] = subset.apply(
                lambda r: (r["note"] + ("<br/>" if r["note"] and r["line_info"] else "") + r["line_info"]),
                axis=1,
            )
            rows = subset[["location_name", "combined"]].itertuples(index=False, name=None)
            render_big_table(
                ["Einrichtung", "Hinweis / Status"],
                rows,
                row_colors=subset["location_color"].fillna("").tolist(),
                text_colors=subset["location_text_color"].fillna("").tolist(),
            )
        else:
            grouped = list(data.groupby("location_type")) if "location_type" in data.columns else [("Alle", data)]
            cols = st.columns(3)
            for idx, (typ, group) in enumerate(grouped):
                with cols[idx % 3]:
                    st.markdown(f"### {typ}")
                    for _, row in group.head(10).iterrows():
                        note = (row.get("note") or "")
                        li = (row.get("line_info") or "")
                        extra = ""
                        if note or li:
                            extra = "<br/><span style='font-size:28px;'>" + escape_html(note)
                            if note and li:
                                extra += " · "
                            extra += escape_html(li) + "</span>"

                        main = f"<b>{escape_html(row['location_name'])}</b> · {escape_html(str(row.get('status') or ''))}"
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
        st.markdown(f"<div class='ticker'><div class='ticker__inner'>{escape_html(ticker['text'])}</div></div>",
                    unsafe_allow_html=True)


# --------------------------------------------------
# Admin: Einrichtungen + Import/Export
# --------------------------------------------------

def show_admin_locations(conn, can_edit: bool):
    st.subheader("Einrichtungen")
    locations = load_locations(conn)

    if locations.empty:
        st.info("Noch keine Einrichtungen vorhanden.")
    else:
        st.dataframe(locations, use_container_width=True)

    if not can_edit:
        st.info("Nur Admin kann Einrichtungen anlegen/bearbeiten/löschen.")
        st.markdown("---")
        st.markdown("### Export (Einrichtungen)")
        st.download_button(
            "Download einrichtungen.json",
            data=export_locations_json(conn),
            file_name="einrichtungen.json",
            mime="application/json",
        )
        return

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

    st.markdown("### Einrichtung bearbeiten / löschen")
    loc_ids = locations["id"].tolist()
    selected = st.selectbox("Einrichtung auswählen", loc_ids, key="loc_edit_select")
    row = locations.loc[locations["id"] == selected].iloc[0]

    with st.form("edit_location"):
        col1, col2, col3 = st.columns(3)
        with col1:
            name_edit = st.text_input("Name", row["name"])
            typ_edit = st.selectbox(
                "Typ",
                ["KRANKENHAUS", "ALTENHEIM", "MVZ"],
                index=["KRANKENHAUS", "ALTENHEIM", "MVZ"].index(row["type"]) if row["type"] in ["KRANKENHAUS","ALTENHEIM","MVZ"] else 0,
            )
        with col2:
            active_edit = st.checkbox("Aktiv", bool(row["active"]))
        with col3:
            color_init = row["color"] if isinstance(row["color"], str) and row["color"] else "#007bff"
            text_color_init = row["text_color"] if isinstance(row["text_color"], str) and row["text_color"] else "#000000"
            color_edit = st.color_picker("Hintergrundfarbe", color_init)
            text_color_edit = st.color_picker("Schriftfarbe", text_color_init)

        c1, c2 = st.columns(2)
        save = c1.form_submit_button("Änderungen speichern")
        delete = c2.form_submit_button("Einrichtung löschen")

        if save:
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

    st.markdown("---")
    st.markdown("### Import / Export (Einrichtungen)")
    colx, coly = st.columns(2)

    with colx:
        st.download_button(
            "Download einrichtungen.json",
            data=export_locations_json(conn),
            file_name="einrichtungen.json",
            mime="application/json",
        )

    with coly:
        up = st.file_uploader("Einrichtungen importieren (einrichtungen.json)", type=["json"], key="upl_locations")
        if up is not None:
            try:
                data = json.loads(up.getvalue().decode("utf-8"))
                ins, upd = import_locations_json(conn, data)
                st.success(f"Import erfolgreich. Neu: {ins}, Aktualisiert: {upd}")
                st.rerun()
            except Exception as e:
                st.error(f"Import fehlgeschlagen: {e}")


# --------------------------------------------------
# Admin: Abfahrten
# --------------------------------------------------

def show_admin_departures(conn, can_edit: bool):
    st.subheader("Abfahrten")

    materialize_tours_to_departures(conn, create_window_hours=MATERIALIZE_TOURS_HOURS_BEFORE)
    update_departure_statuses(conn)

    locations = load_locations(conn)
    screens = load_screens(conn)
    if locations.empty:
        st.warning("Bitte zuerst mindestens eine Einrichtung anlegen.")
        return

    if can_edit:
        st.markdown("### Neue Abfahrt anlegen (manuell)")
        with st.form("new_departure"):
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                weekday = st.selectbox("Wochentag", WEEKDAYS_DE)
            with col2:
                hours = [f"{h:02d}:00" for h in range(24)]
                hour_label = st.selectbox("Uhrzeit (volle Stunde)", hours, index=8)
                hour_int = int(hour_label.split(":")[0])
            with col3:
                loc_id = st.selectbox(
                    "Einrichtung",
                    options=locations["id"].tolist(),
                    format_func=lambda i: locations.loc[locations["id"] == i, "name"].values[0],
                )
            with col4:
                screen_id = st.selectbox(
                    "Ziel-Screen (optional)",
                    options=[None] + screens["id"].tolist(),
                    format_func=lambda x: "Global (alle)" if x is None else f"Screen {int(x)}",
                )

            vehicle = st.text_input("Fahrzeug (optional)", "")
            note = st.text_input("Hinweis (optional)", "")
            submitted = st.form_submit_button("Speichern")

            if submitted:
                dt = next_datetime_for_weekday_hour(weekday, hour_int)
                conn.execute(
                    """
                    INSERT INTO departures (datetime, location_id, vehicle, status, note, created_by, screen_id)
                    VALUES (?, ?, ?, 'GEPLANT', ?, ?, ?)
                    """,
                    (dt.isoformat(), int(loc_id), vehicle.strip(), note.strip(),
                     st.session_state.get("username", "MANUAL"),
                     int(screen_id) if screen_id is not None else None),
                )
                conn.commit()
                st.success(f"Abfahrt gespeichert: {dt.strftime('%Y-%m-%d %H:%M')}")
                st.rerun()

    st.markdown("### Bestehende Abfahrten")
    deps = load_departures_with_locations(conn)
    if deps.empty:
        st.info("Noch keine Abfahrten vorhanden.")
        return

    deps = deps.sort_values("datetime")

    for _, row in deps.iterrows():
        dt_str = row["datetime"].strftime("%Y-%m-%d %H:%M") if pd.notnull(row["datetime"]) else "-"
        sk = row.get("source_key") or ""
        auto_tag = " (Tour-Auto)" if sk and str(sk).startswith("TOUR:") else ""
        sid = row.get("screen_id")
        sid_txt = "Global" if pd.isna(sid) else f"Screen {int(sid)}"

        with st.container():
            c1, c2 = st.columns([4, 1])
            with c1:
                st.markdown(f"**{dt_str} – {row['location_name']} ({row['location_type']})**{auto_tag}")
                lines = []
                lines.append(f"Screen: {sid_txt}")
                lines.append(f"Status: {row.get('status')}")
                if row.get("vehicle"):
                    lines.append(f"Fahrzeug: {row.get('vehicle')}")
                if row.get("note"):
                    lines.append(f"Hinweis: {row.get('note')}")
                if row.get("created_by"):
                    lines.append(f"Erstellt durch: {row.get('created_by')}")
                st.markdown("<br>".join(lines), unsafe_allow_html=True)

            with c2:
                if can_edit:
                    if st.button("Löschen", key=f"del_dep_{row['id']}"):
                        conn.execute("DELETE FROM departures WHERE id=?", (int(row["id"]),))
                        conn.commit()
                        st.success("Abfahrt gelöscht.")
                        st.rerun()
                else:
                    st.caption("Keine Löschrechte")


# --------------------------------------------------
# Admin: Touren + Import/Export
# --------------------------------------------------

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

    tours = load_tours(conn)
    if tours.empty:
        st.info("Noch keine Touren vorhanden.")
    else:
        view = tours.copy()
        view["Zeit"] = view["hour"].apply(lambda h: f"{int(h):02d}:00")
        view["Monitore"] = view["screen_ids"].apply(
            lambda s: ", ".join([f"{i}:{screen_map.get(i, f'Screen {i}')}" for i in parse_screen_ids(s)])
        )
        st.dataframe(view[["id", "name", "weekday", "Zeit", "location_name", "note", "active", "Monitore"]],
                     use_container_width=True)

    if not can_edit:
        st.markdown("---")
        st.markdown("### Export (Touren)")
        st.download_button(
            "Download touren.json",
            data=export_tours_json(conn),
            file_name="touren.json",
            mime="application/json",
        )
        return

    st.markdown("### Neue Tour anlegen")
    with st.form("new_tour_form"):
        col1, col2, col3 = st.columns(3)
        with col1:
            tour_name = st.text_input("Tour-Name", "")
            weekday = st.selectbox("Wochentag", WEEKDAYS_DE)
        with col2:
            hours = [f"{h:02d}:00" for h in range(24)]
            hour_label = st.selectbox("Uhrzeit (volle Stunde)", hours, index=8)
            hour_int = int(hour_label.split(":")[0])
        with col3:
            screens_new = st.multiselect(
                "Monitore (Screens) für diese Tour",
                options=screen_options,
                format_func=lambda sid: f"{sid}: {screen_map.get(sid)}",
            )

        stops_new = st.multiselect(
            "Einrichtungen (Stops) in Reihenfolge",
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
                    INSERT INTO tours (name, weekday, hour, location_id, note, active, screen_ids)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (tour_name.strip(), weekday, hour_int, primary_loc, note_new.strip(), 1 if active_new else 0, screen_ids_str),
                )
                tour_id = cur.lastrowid

                for pos, loc_id in enumerate(stops_new):
                    cur.execute(
                        "INSERT INTO tour_stops (tour_id, location_id, position) VALUES (?, ?, ?)",
                        (tour_id, int(loc_id), pos),
                    )

                conn.commit()
                st.success("Tour gespeichert.")
                st.rerun()

    tours = load_tours(conn)
    if not tours.empty:
        st.markdown("### Tour bearbeiten / löschen")
        tour_ids = tours["id"].tolist()
        selected = st.selectbox("Tour auswählen", tour_ids, key="edit_tour_select")
        row = tours.loc[tours["id"] == selected].iloc[0]

        stops_df = read_df(conn, "SELECT location_id FROM tour_stops WHERE tour_id=? ORDER BY position", (int(selected),))
        existing_stop_ids = stops_df["location_id"].tolist() if not stops_df.empty else [int(row["location_id"])]

        existing_screen_ids = parse_screen_ids(row["screen_ids"])

        with st.form("edit_tour_form"):
            col1, col2, col3 = st.columns(3)
            with col1:
                name_edit = st.text_input("Tour-Name", row["name"])
                weekday_edit = st.selectbox(
                    "Wochentag",
                    WEEKDAYS_DE,
                    index=WEEKDAYS_DE.index(row["weekday"]) if row["weekday"] in WEEKDAYS_DE else 0,
                )
            with col2:
                hours = [f"{h:02d}:00" for h in range(24)]
                default_hour = int(row["hour"]) if str(row["hour"]).isdigit() and 0 <= int(row["hour"]) < 24 else 8
                hour_edit_label = st.selectbox("Uhrzeit (volle Stunde)", hours, index=default_hour)
                hour_edit = int(hour_edit_label.split(":")[0])
            with col3:
                screens_edit = st.multiselect(
                    "Monitore (Screens) für diese Tour",
                    options=screen_options,
                    format_func=lambda sid: f"{sid}: {screen_map.get(sid)}",
                    default=existing_screen_ids,
                )

            stops_edit = st.multiselect(
                "Einrichtungen (Stops) in Reihenfolge",
                options=loc_options,
                format_func=lambda i: locations.loc[locations["id"] == i, "name"].values[0],
                default=existing_stop_ids,
            )
            note_edit = st.text_input("Hinweis (optional)", row["note"] or "")
            active_edit = st.checkbox("Aktiv", bool(row["active"]))

            c1, c2 = st.columns(2)
            save = c1.form_submit_button("Änderungen speichern")
            delete = c2.form_submit_button("Tour löschen")

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
                        SET name=?, weekday=?, hour=?, location_id=?, note=?, active=?, screen_ids=?
                        WHERE id=?
                        """,
                        (name_edit.strip(), weekday_edit, hour_edit, primary_loc, note_edit.strip(),
                         1 if active_edit else 0, screen_ids_str, int(selected)),
                    )
                    cur.execute("DELETE FROM tour_stops WHERE tour_id=?", (int(selected),))
                    for pos, loc_id in enumerate(stops_edit):
                        cur.execute(
                            "INSERT INTO tour_stops (tour_id, location_id, position) VALUES (?, ?, ?)",
                            (int(selected), int(loc_id), int(pos)),
                        )
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

    st.markdown("---")
    st.markdown("### Import / Export (Touren)")
    colx, coly = st.columns(2)

    with colx:
        st.download_button(
            "Download touren.json",
            data=export_tours_json(conn),
            file_name="touren.json",
            mime="application/json",
        )

    with coly:
        up = st.file_uploader("Touren importieren (touren.json)", type=["json"], key="upl_tours")
        if up is not None:
            try:
                data = json.loads(up.getvalue().decode("utf-8"))
                ins, upd = import_tours_json(conn, data)
                st.success(f"Import erfolgreich. Neu: {ins}, Aktualisiert: {upd}")
                st.rerun()
            except Exception as e:
                st.error(f"Import fehlgeschlagen: {e}")


# --------------------------------------------------
# Admin: Screens + Ticker (pro Screen)
# --------------------------------------------------

def show_admin_screens(conn, can_edit: bool):
    st.subheader("Screens / Monitore")

    screens = load_screens(conn)
    st.dataframe(screens, use_container_width=True)

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
            index=["DETAIL", "OVERVIEW", "WAREHOUSE"].index(row["mode"]) if row["mode"] in ["DETAIL","OVERVIEW","WAREHOUSE"] else 0,
        )
        filter_type = st.selectbox(
            "Filter Typ",
            ["ALLE", "KRANKENHAUS", "ALTENHEIM", "MVZ"],
            index=["ALLE", "KRANKENHAUS", "ALTENHEIM", "MVZ"].index(row["filter_type"]) if row["filter_type"] in ["ALLE","KRANKENHAUS","ALTENHEIM","MVZ"] else 0,
        )
        filter_locations = st.text_input("Filter Locations (IDs, Komma-getrennt)", row["filter_locations"] or "")
        refresh = st.number_input("Refresh-Intervall (Sekunden)", min_value=5, max_value=300, value=int(row["refresh_interval_seconds"]))
        holiday_flag = st.checkbox("Feiertagsbelieferung aktiv", value=bool(row["holiday_flag"]))
        special_flag = st.checkbox("Sonderplan aktiv", value=bool(row["special_flag"]))

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
    st.info("Das Laufband läuft nur auf Screens, wo es hier aktiv gesetzt ist.")

    trow = load_ticker_for_screen(conn, int(selected))
    with st.form("ticker_form"):
        text = st.text_area("Laufband-Text", value=(trow["text"] or "") if trow is not None else "", height=100)
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


# --------------------------------------------------
# Admin Mode
# --------------------------------------------------

def show_admin_mode():
    require_login()

    role = st.session_state.get("role", "viewer")
    username = st.session_state.get("username", "")

    st.title("Abfahrten – Admin / Disposition")
    st.caption(f"Eingeloggt als: {username} (Rolle: {role}) • DE Ortszeit: {now_berlin().strftime('%d.%m.%Y %H:%M:%S')}")

    if st.sidebar.button("Logout"):
        st.session_state["logged_in"] = False
        st.session_state["role"] = None
        st.session_state["username"] = None
        st.rerun()

    conn = get_connection()

    # konsistent halten
    materialize_tours_to_departures(conn, create_window_hours=MATERIALIZE_TOURS_HOURS_BEFORE)
    update_departure_statuses(conn)

    can_edit = role == "admin"

    if can_edit:
        tabs = st.tabs(["Abfahrten", "Einrichtungen", "Touren", "Screens/Ticker"])
        with tabs[0]:
            show_admin_departures(conn, can_edit=True)
        with tabs[1]:
            show_admin_locations(conn, can_edit=True)
        with tabs[2]:
            show_admin_tours(conn, can_edit=True)
        with tabs[3]:
            show_admin_screens(conn, can_edit=True)
    else:
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
                view["Zeit"] = view["hour"].apply(lambda h: f"{int(h):02d}:00")
                view["Monitore"] = view["screen_ids"].apply(
                    lambda s: ", ".join([f"{i}:{screen_map.get(i, f'Screen {i}')}" for i in parse_screen_ids(s)])
                )
                st.dataframe(view[["id", "name", "weekday", "Zeit", "location_name", "note", "active", "Monitore"]],
                             use_container_width=True)


# --------------------------------------------------
# Main
# --------------------------------------------------

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
