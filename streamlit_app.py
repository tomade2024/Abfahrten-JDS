import streamlit as st
import pandas as pd
import sqlite3
from datetime import datetime, timedelta, time as dtime
from pathlib import Path

from streamlit_autorefresh import st_autorefresh

# --------------------------------------------------
# Konfiguration
# --------------------------------------------------

st.set_page_config(page_title="Abfahrten", layout="wide")
DB_PATH = Path("abfahrten.db")

USERS = {
    "admin": {"password": "admin123", "role": "admin"},
    "dispo": {"password": "dispo123", "role": "viewer"},
}

WEEKDAYS_DE = ["Montag", "Dienstag", "Mittwoch", "Donnerstag", "Freitag", "Samstag", "Sonntag"]
WEEKDAY_TO_INT = {name: i for i, name in enumerate(WEEKDAYS_DE)}  # Montag=0

# Countdown/Automatik
COUNTDOWN_START_HOURS = 3
AUTO_COMPLETE_AFTER_MIN = 20
KEEP_COMPLETED_MINUTES = 10  # Variante A: global

# Touren -> echte Abfahrten
MATERIALIZE_TOURS_HOURS_BEFORE = 3


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
            active      INTEGER NOT NULL DEFAULT 1
        )
        """
    )
    cur.execute("PRAGMA table_info(locations)")
    loc_cols = [r[1] for r in cur.fetchall()]
    if "color" not in loc_cols:
        cur.execute("ALTER TABLE locations ADD COLUMN color TEXT")
    if "text_color" not in loc_cols:
        cur.execute("ALTER TABLE locations ADD COLUMN text_color TEXT")

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
            FOREIGN KEY(location_id) REFERENCES locations(id)
        )
        """
    )

    # Abfahrten Zusatzfelder
    cur.execute("PRAGMA table_info(departures)")
    dep_cols = [r[1] for r in cur.fetchall()]
    if "ready_at" not in dep_cols:
        cur.execute("ALTER TABLE departures ADD COLUMN ready_at TEXT")
    if "completed_at" not in dep_cols:
        cur.execute("ALTER TABLE departures ADD COLUMN completed_at TEXT")
    if "source_key" not in dep_cols:
        cur.execute("ALTER TABLE departures ADD COLUMN source_key TEXT")
    if "created_by" not in dep_cols:
        cur.execute("ALTER TABLE departures ADD COLUMN created_by TEXT")
    cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_departures_source_key ON departures(source_key)")

    # Touren
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS tours (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            name         TEXT NOT NULL,
            weekday      TEXT NOT NULL,
            hour         INTEGER NOT NULL,
            location_id  INTEGER NOT NULL,     -- Haupt-Einrichtung (erste Station)
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
            mode                      TEXT NOT NULL,
            filter_type               TEXT NOT NULL DEFAULT 'ALLE',
            filter_locations          TEXT,
            refresh_interval_seconds  INTEGER NOT NULL DEFAULT 30,
            holiday_flag              INTEGER NOT NULL DEFAULT 0,
            special_flag              INTEGER NOT NULL DEFAULT 0
        )
        """
    )

    # Ticker
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS ticker (
            id     INTEGER PRIMARY KEY,
            text   TEXT,
            active INTEGER NOT NULL DEFAULT 0
        )
        """
    )
    cur.execute("SELECT COUNT(*) FROM ticker")
    if cur.fetchone()[0] == 0:
        cur.execute("INSERT INTO ticker (id, text, active) VALUES (1, '', 0)")

    # Default Screens anlegen, falls leer
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
        # Falls App schon läuft: Screen 7 nachrüsten, falls nicht vorhanden
        cur.execute("SELECT COUNT(*) FROM screens WHERE id = 7")
        if cur.fetchone()[0] == 0:
            cur.execute(
                """
                INSERT INTO screens
                (id, name, mode, filter_type, filter_locations, refresh_interval_seconds, holiday_flag, special_flag)
                VALUES (7, 'Lagerstand Übersicht', 'WAREHOUSE', 'ALLE', '', 20, 0, 0)
                """
            )

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


def load_ticker(conn):
    df = read_df(conn, "SELECT * FROM ticker WHERE id=1")
    return df.iloc[0] if not df.empty else None


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


# --------------------------------------------------
# Zeit / Format
# --------------------------------------------------

def next_datetime_for_weekday_hour(weekday_name: str, hour: int) -> datetime:
    now = datetime.now()
    target = WEEKDAY_TO_INT[weekday_name]
    today = now.weekday()

    days_ahead = (target - today) % 7
    candidate_date = now.date() + timedelta(days=days_ahead)
    candidate_dt = datetime.combine(candidate_date, dtime(hour=hour, minute=0))

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
# Status-Automation
# --------------------------------------------------

def update_departure_statuses(conn: sqlite3.Connection):
    now = datetime.now()
    now_iso = now.isoformat(timespec="seconds")

    df = read_df(conn, "SELECT id, datetime, status, ready_at, completed_at FROM departures")
    if df.empty:
        return

    df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce")
    df["ready_at"] = pd.to_datetime(df["ready_at"], errors="coerce")
    df["completed_at"] = pd.to_datetime(df["completed_at"], errors="coerce")
    df = df.dropna(subset=["datetime"])

    to_ready = []
    to_done = []

    for _, r in df.iterrows():
        dep_id = int(r["id"])
        dep_dt = r["datetime"].to_pydatetime()
        status = str(r["status"] or "").upper()

        if status in ("STORNIERT", "ABGESCHLOSSEN"):
            continue

        if now >= completion_deadline(dep_dt):
            to_done.append(dep_id)
        elif now >= dep_dt:
            if status != "BEREIT":
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
# Touren -> echte Abfahrten (3h vorher)
# --------------------------------------------------

def materialize_tours_to_departures(conn: sqlite3.Connection, create_window_hours: int = MATERIALIZE_TOURS_HOURS_BEFORE):
    now = datetime.now()
    window = timedelta(hours=create_window_hours)

    df = read_df(
        conn,
        """
        SELECT t.id         AS tour_id,
               t.weekday,
               t.hour,
               t.note       AS tour_note,
               t.active     AS tour_active,
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

        dep_dt = next_datetime_for_weekday_hour(weekday, hour)

        if dep_dt - now > window:
            continue

        if now - dep_dt > timedelta(days=1):
            continue

        source_key = f"TOUR:{tour_id}:{pos}:{dep_dt.isoformat()}"

        try:
            cur.execute(
                """
                INSERT INTO departures (datetime, location_id, vehicle, status, note, source_key, created_by)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (dep_dt.isoformat(), loc_id, "", "GEPLANT", note, source_key, "TOUR_AUTO"),
            )
            created_any = True
        except sqlite3.IntegrityError:
            pass

    if created_any:
        conn.commit()


# --------------------------------------------------
# Screen Tour Summary (für Monitor 7)
# --------------------------------------------------

def tour_has_screen(screen_ids_value, screen_id: int) -> bool:
    if screen_ids_value is None:
        return False
    s = str(screen_ids_value).strip()
    if not s:
        return False
    for part in s.split(","):
        p = part.strip()
        if not p:
            continue
        if p.isdigit() and int(p) == int(screen_id):
            return True
    return False


def get_next_tour_for_screen(conn: sqlite3.Connection, screen_id: int):
    """
    Liefert (next_dt, tour_row, stops_list) für die nächste Tour, die diesem Screen zugewiesen ist.
    tour_row enthält: id, name, weekday, hour, note, ...
    """
    tours = load_tours(conn)
    if tours.empty:
        return None

    tours = tours[(tours["active"] == 1) & (tours["screen_ids"].apply(lambda v: tour_has_screen(v, screen_id)))]
    if tours.empty:
        return None

    best = None
    best_dt = None

    for _, t in tours.iterrows():
        weekday = str(t["weekday"])
        hour = int(t["hour"]) if str(t["hour"]).isdigit() else 0
        try:
            dt = next_datetime_for_weekday_hour(weekday, hour)
        except Exception:
            continue

        if best_dt is None or dt < best_dt:
            best_dt = dt
            best = t

    if best is None:
        return None

    stops_df = load_tour_stops(conn, int(best["id"]))
    stops = stops_df["location_name"].tolist() if not stops_df.empty else [str(best.get("location_name") or "")]

    return {
        "tour_id": int(best["id"]),
        "tour_name": str(best["name"]),
        "next_dt": best_dt,
        "stops": [s for s in stops if s],
        "note": str(best.get("note") or ""),
    }


# --------------------------------------------------
# Screen-Daten (Departures)
# --------------------------------------------------

def get_screen_data(conn: sqlite3.Connection, screen_id: int):
    screens = load_screens(conn)
    if screens.empty or screen_id not in screens["id"].tolist():
        return None, None

    screen = screens.loc[screens["id"] == screen_id].iloc[0]
    filter_type = screen["filter_type"] or "ALLE"
    filter_locations = (screen["filter_locations"] or "").strip()

    now = datetime.now()

    deps = load_departures_with_locations(conn)
    if deps.empty:
        return screen, deps

    deps = deps[deps["location_active"] == 1].copy()

    window_start = now - timedelta(minutes=AUTO_COMPLETE_AFTER_MIN)
    deps = deps[deps["datetime"] >= window_start]

    def visible(row):
        status = str(row.get("status") or "").upper()
        if status != "ABGESCHLOSSEN":
            return True
        if KEEP_COMPLETED_MINUTES <= 0:
            return False
        ca = row.get("completed_at")
        dep_dt = row["datetime"].to_pydatetime()
        base = ca.to_pydatetime() if pd.notnull(ca) else completion_deadline(dep_dt)
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
        dep_dt = row["datetime"].to_pydatetime()

        if status == "GEPLANT":
            delta = dep_dt - now
            if timedelta(0) <= delta <= timedelta(hours=COUNTDOWN_START_HOURS):
                return f"Countdown: {fmt_compact(delta)}"
            return ""
        if status == "BEREIT":
            parts = []
            ra = row.get("ready_at")
            if pd.notnull(ra):
                since = now - ra.to_pydatetime()
                parts.append(f"BEREIT seit {fmt_compact(since)}")
            rem = completion_deadline(dep_dt) - now
            parts.append(f"Abschluss in {fmt_compact(rem)}")
            return " · ".join(parts)
        return ""

    deps["line_info"] = deps.apply(build_line_info, axis=1)
    deps = deps.sort_values("datetime")
    return screen, deps


# --------------------------------------------------
# Rendering
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
# Display Mode (Monitore)
# --------------------------------------------------

def show_display_mode(screen_id: int):
    st.markdown(
        """
        <style>
        #MainMenu {visibility: hidden;}
        footer {visibility: hidden;}
        header {visibility: hidden;}

        .block-container { padding-top: 0.5rem; }

        body, .block-container, .stMarkdown, .stText, .stDataFrame, div, span {
            font-size: 32px !important;
        }

        .big-table {
            width: 100%;
            border-collapse: collapse;
        }
        .big-table th, .big-table td {
            border-bottom: 1px solid #555;
            padding: 0.4em 0.8em;
            text-align: left;
            vertical-align: top;
        }
        .big-table th { font-weight: 800; }

        .ticker {
            position: fixed;
            bottom: 0; left: 0;
            width: 100%;
            background: #000;
            color: #fff;
            overflow: hidden;
            white-space: nowrap;
            z-index: 9999;
        }
        .ticker__inner {
            display: inline-block;
            padding-left: 100%;
            animation: ticker-scroll 20s linear infinite;
        }
        @keyframes ticker-scroll {
            0% { transform: translateX(0); }
            100% { transform: translateX(-100%); }
        }
        </style>
        """,
        unsafe_allow_html=True,
    )

    if not screen_id:
        st.error("Parameter 'screenId' fehlt oder ist ungültig.")
        return

    conn = get_connection()

    # Touren materialisieren + Status updaten
    materialize_tours_to_departures(conn, create_window_hours=MATERIALIZE_TOURS_HOURS_BEFORE)
    update_departure_statuses(conn)

    screen, data = get_screen_data(conn, screen_id)

    interval_sec = 30
    if screen is not None:
        try:
            interval_sec = int(screen.get("refresh_interval_seconds", 30))
        except Exception:
            interval_sec = 30

    st_autorefresh(interval=interval_sec * 1000, key=f"display_refresh_{screen_id}")

    if screen is None:
        st.error(f"Screen {screen_id} ist nicht konfiguriert.")
        return

    holiday_active = bool(screen.get("holiday_flag", 0))
    special_active = bool(screen.get("special_flag", 0))

    # Sonderplan/Feiertag Vollbild
    if holiday_active or special_active:
        st.markdown(
            """
            <style>
            body, .block-container { background-color:#000 !important; color:#fff !important; }
            </style>
            """,
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
            <div style="
              display:flex; justify-content:center; align-items:center;
              height:100vh; width:100%;
              background:#000; color:#fff;
              font-size:72px; font-weight:900; text-transform:uppercase;
              text-align:center;">
              {msg}
            </div>
            """,
            unsafe_allow_html=True,
        )
        return

    # --------------------------------------------------
    # Screen 7: Lagerstand Übersicht
    # --------------------------------------------------
    if int(screen["id"]) == 7 or str(screen.get("mode", "")).upper() == "WAREHOUSE":
        st.markdown(f"## {screen['name']} (Screen 7)")
        st.caption(f"Aktualisierung alle {interval_sec} Sekunden • Nächste Touren von Screen 1–4")

        rows = []
        for sid in [1, 2, 3, 4]:
            info = get_next_tour_for_screen(conn, sid)
            if not info:
                rows.append([f"Screen {sid}", "—", "—", "—"])
                continue

            dt = info["next_dt"]
            dt_str = dt.strftime("%a, %d.%m %H:%M")  # kompakt
            tour_name = info["tour_name"]
            stops = ", ".join(info["stops"][:6]) + (" …" if len(info["stops"]) > 6 else "")
            rows.append([f"Screen {sid}", dt_str, tour_name, stops])

        headers = ["Zone/Screen", "Nächste Tour (Zeit)", "Tour", "Stops (Einrichtungen)"]
        render_big_table(headers, rows)

        # Ticker unten
        ticker_row = load_ticker(conn)
        if ticker_row is not None and int(ticker_row["active"]) == 1 and (ticker_row["text"] or "").strip():
            text = escape_html((ticker_row["text"] or "").strip())
            st.markdown(f"<div class='ticker'><div class='ticker__inner'>{text}</div></div>", unsafe_allow_html=True)
        return

    # --------------------------------------------------
    # Normale Screens 1–6
    # --------------------------------------------------
    st.markdown(f"## {screen['name']} (Screen {screen_id})")
    st.caption(f"Modus: {screen['mode']} • Aktualisierung alle {interval_sec} Sekunden • Abgeschlossene sichtbar: {KEEP_COMPLETED_MINUTES} min")

    if data is None or data.empty:
        st.info("Keine Abfahrten.")
    else:
        # Zone A–D: nur Einrichtung + Hinweis/Statuszeile
        if str(screen["mode"]).upper() == "DETAIL" and int(screen["id"]) in [1, 2, 3, 4]:
            subset = data[["location_name", "note", "line_info", "location_color", "location_text_color"]].copy()
            subset["note"] = subset["note"].fillna("")
            subset["line_info"] = subset["line_info"].fillna("")
            subset["combined_note"] = subset.apply(
                lambda r: (r["note"] + ("<br/>" if r["note"] and r["line_info"] else "") + r["line_info"]),
                axis=1,
            )
            headers = ["Einrichtung", "Hinweis / Status"]
            rows = subset[["location_name", "combined_note"]].itertuples(index=False, name=None)
            render_big_table(
                headers,
                rows,
                row_colors=subset["location_color"].fillna("").tolist(),
                text_colors=subset["location_text_color"].fillna("").tolist(),
            )

        elif str(screen["mode"]).upper() == "DETAIL":
            subset = data[["location_name", "location_type", "vehicle", "status", "note", "line_info", "location_color", "location_text_color"]].copy()
            subset["note"] = subset["note"].fillna("")
            subset["line_info"] = subset["line_info"].fillna("")
            subset["note2"] = subset.apply(
                lambda r: (r["note"] + ("<br/>" if r["note"] and r["line_info"] else "") + r["line_info"]),
                axis=1,
            )
            headers = ["Einrichtung", "Typ", "Fahrzeug", "Status", "Hinweis / Info"]
            rows = subset[["location_name", "location_type", "vehicle", "status", "note2"]].itertuples(index=False, name=None)
            render_big_table(
                headers,
                rows,
                row_colors=subset["location_color"].fillna("").tolist(),
                text_colors=subset["location_text_color"].fillna("").tolist(),
            )

        else:
            grouped = list(data.groupby("location_type"))
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

                        main = f"<b>{escape_html(row['location_name'])}</b>"
                        status = str(row.get("status") or "")
                        if status:
                            main += f" · {escape_html(status)}"

                        bg = row.get("location_color") or ""
                        tc = row.get("location_text_color") or ""
                        style = "margin-bottom:10px;"
                        if bg:
                            style += f"background-color:{bg};padding:0.3em 0.5em;border-radius:0.2em;"
                        if tc:
                            style += f"color:{tc};"

                        st.markdown(f"<div style='{style}'>{main}{extra}</div>", unsafe_allow_html=True)

    # Ticker unten
    ticker_row = load_ticker(conn)
    if ticker_row is not None and int(ticker_row["active"]) == 1 and (ticker_row["text"] or "").strip():
        text = escape_html((ticker_row["text"] or "").strip())
        st.markdown(f"<div class='ticker'><div class='ticker__inner'>{text}</div></div>", unsafe_allow_html=True)


# --------------------------------------------------
# Admin: Einrichtungen
# --------------------------------------------------

def show_admin_locations(conn):
    st.subheader("Einrichtungen")

    locations = load_locations(conn)
    if locations.empty:
        st.info("Noch keine Einrichtungen vorhanden.")
    else:
        st.dataframe(locations, use_container_width=True)

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

    if not locations.empty:
        st.markdown("### Einrichtung bearbeiten / löschen")
        loc_ids = locations["id"].tolist()
        selected = st.selectbox("Einrichtung auswählen", loc_ids)
        row = locations.loc[locations["id"] == selected].iloc[0]

        with st.form("edit_location"):
            col1, col2, col3 = st.columns(3)
            with col1:
                name_edit = st.text_input("Name", row["name"])
                typ_edit = st.selectbox("Typ", ["KRANKENHAUS", "ALTENHEIM", "MVZ"], index=["KRANKENHAUS", "ALTENHEIM", "MVZ"].index(row["type"]))
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


# --------------------------------------------------
# Admin: Abfahrten
# --------------------------------------------------

def show_admin_departures(conn, can_edit: bool):
    st.subheader("Abfahrten")

    materialize_tours_to_departures(conn, create_window_hours=MATERIALIZE_TOURS_HOURS_BEFORE)
    update_departure_statuses(conn)

    locations = load_locations(conn)
    if locations.empty:
        st.warning("Bitte zuerst mindestens eine Einrichtung anlegen.")
        return

    if can_edit:
        st.markdown("### Neue Abfahrt anlegen")
        with st.form("new_departure"):
            col1, col2, col3 = st.columns(3)
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

            vehicle = st.text_input("Fahrzeug (optional)", "")
            note = st.text_input("Hinweis (optional)", "")
            submitted = st.form_submit_button("Speichern")

            if submitted:
                dt = next_datetime_for_weekday_hour(weekday, hour_int)
                conn.execute(
                    """
                    INSERT INTO departures (datetime, location_id, vehicle, status, note, created_by)
                    VALUES (?, ?, ?, 'GEPLANT', ?, ?)
                    """,
                    (dt.isoformat(), int(loc_id), vehicle.strip(), note.strip(), st.session_state.get("username", "MANUAL")),
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

        with st.container():
            c1, c2 = st.columns([4, 1])
            with c1:
                st.markdown(f"**{dt_str} – {row['location_name']} ({row['location_type']})**{auto_tag}")
                lines = []
                lines.append(f"Status: {row.get('status')}")
                if row.get("vehicle"):
                    lines.append(f"Fahrzeug: {row.get('vehicle')}")
                if row.get("note"):
                    lines.append(f"Hinweis: {row.get('note')}")
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
# Admin: Touren
# --------------------------------------------------

def show_admin_tours(conn, can_edit: bool):
    st.subheader("Touren (feste Touren)")

    locations = load_locations(conn)
    if locations.empty:
        st.warning("Bitte zuerst Einrichtungen anlegen.")
        return

    screens = load_screens(conn)
    screen_map = {int(r["id"]): r["name"] for _, r in screens.iterrows()}
    screen_options = list(screen_map.keys())
    loc_options = locations["id"].tolist()

    tours = load_tours(conn)
    if tours.empty:
        st.info("Noch keine Touren vorhanden.")
    else:
        view = tours.copy()
        view["Zeit"] = view["hour"].apply(lambda h: f"{int(h):02d}:00")
        st.dataframe(view[["id", "name", "weekday", "Zeit", "location_name", "note", "active", "screen_ids"]], use_container_width=True)

    if not can_edit:
        st.caption("Nur Admin kann Touren bearbeiten.")
        return

    st.markdown("### Neue Tour anlegen")
    col1, col2, col3 = st.columns(3)
    with col1:
        tour_name = st.text_input("Tour-Name", "", key="new_tour_name")
        weekday = st.selectbox("Wochentag", WEEKDAYS_DE, key="new_tour_weekday")
    with col2:
        hours = [f"{h:02d}:00" for h in range(24)]
        hour_label = st.selectbox("Uhrzeit (volle Stunde)", hours, index=8, key="new_tour_hour")
        hour_int = int(hour_label.split(":")[0])
    with col3:
        stops_new = st.multiselect(
            "Einrichtungen (Stops)",
            options=loc_options,
            format_func=lambda i: locations.loc[locations["id"] == i, "name"].values[0],
            key="new_tour_stops",
        )

    note_new = st.text_input("Hinweis (optional)", "", key="new_tour_note")
    active_new = st.checkbox("Aktiv", True, key="new_tour_active")
    screens_new = st.multiselect(
        "Monitore (Screens) für diese Tour",
        options=screen_options,
        format_func=lambda sid: f"{sid}: {screen_map.get(sid)}",
        key="new_tour_screens",
    )

    if st.button("Tour speichern", key="btn_new_tour_save"):
        if not tour_name.strip():
            st.error("Tour-Name darf nicht leer sein.")
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
    if tours.empty:
        return

    st.markdown("### Tour bearbeiten / löschen")
    tour_ids = tours["id"].tolist()
    selected = st.selectbox("Tour auswählen", tour_ids, key="edit_tour_select")
    row = tours.loc[tours["id"] == selected].iloc[0]

    stops_df = read_df(conn, "SELECT location_id FROM tour_stops WHERE tour_id=? ORDER BY position", (int(selected),))
    existing_stop_ids = stops_df["location_id"].tolist() if not stops_df.empty else [int(row["location_id"])]

    existing_screen_ids = []
    if row["screen_ids"]:
        for part in str(row["screen_ids"]).split(","):
            p = part.strip()
            if p.isdigit():
                existing_screen_ids.append(int(p))

    col1, col2, col3 = st.columns(3)
    with col1:
        name_edit = st.text_input("Tour-Name", row["name"], key=f"edit_name_{selected}")
        weekday_edit = st.selectbox(
            "Wochentag",
            WEEKDAYS_DE,
            index=WEEKDAYS_DE.index(row["weekday"]) if row["weekday"] in WEEKDAYS_DE else 0,
            key=f"edit_weekday_{selected}",
        )
    with col2:
        hours = [f"{h:02d}:00" for h in range(24)]
        default_hour = int(row["hour"]) if str(row["hour"]).isdigit() and 0 <= int(row["hour"]) < 24 else 8
        hour_edit_label = st.selectbox("Uhrzeit (volle Stunde)", hours, index=default_hour, key=f"edit_hour_{selected}")
        hour_edit = int(hour_edit_label.split(":")[0])
    with col3:
        stops_edit = st.multiselect(
            "Einrichtungen (Stops)",
            options=loc_options,
            format_func=lambda i: locations.loc[locations["id"] == i, "name"].values[0],
            default=existing_stop_ids,
            key=f"edit_stops_{selected}",
        )

    note_edit = st.text_input("Hinweis (optional)", row["note"] or "", key=f"edit_note_{selected}")
    active_edit = st.checkbox("Aktiv", bool(row["active"]), key=f"edit_active_{selected}")
    screens_edit = st.multiselect(
        "Monitore (Screens) für diese Tour",
        options=screen_options,
        format_func=lambda sid: f"{sid}: {screen_map.get(sid)}",
        default=existing_screen_ids,
        key=f"edit_screens_{selected}",
    )

    c1, c2 = st.columns(2)
    if c1.button("Änderungen speichern", key=f"btn_save_{selected}"):
        if not stops_edit:
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
                (name_edit.strip(), weekday_edit, hour_edit, primary_loc, note_edit.strip(), 1 if active_edit else 0, screen_ids_str, int(selected)),
            )
            cur.execute("DELETE FROM tour_stops WHERE tour_id=?", (int(selected),))
            for pos, loc_id in enumerate(stops_edit):
                cur.execute(
                    "INSERT INTO tour_stops (tour_id, location_id, position) VALUES (?, ?, ?)",
                    (int(selected), int(loc_id), pos),
                )
            conn.commit()
            st.success("Tour aktualisiert.")
            st.rerun()

    if c2.button("Tour löschen", key=f"btn_del_{selected}"):
        cur = conn.cursor()
        cur.execute("DELETE FROM tour_stops WHERE tour_id=?", (int(selected),))
        cur.execute("DELETE FROM tours WHERE id=?", (int(selected),))
        conn.commit()
        st.success("Tour gelöscht.")
        st.rerun()


# --------------------------------------------------
# Admin: Screens + Ticker
# --------------------------------------------------

def show_admin_screens(conn):
    st.subheader("Screens / Monitore")

    screens = load_screens(conn)
    st.dataframe(screens, use_container_width=True)

    st.markdown("### Links zu den Monitoren")
    st.info("Diese Links auf den jeweiligen Monitoren öffnen (mit Autorefresh).")
    for _, r in screens.iterrows():
        sid = int(r["id"])
        link = f"?mode=display&screenId={sid}"
        st.markdown(f"- **Screen {sid} – {r['name']}**: [Monitor öffnen]({link}) (Parameter: `{link}`)")

    st.markdown("---")
    st.markdown("### Screen bearbeiten")

    screen_ids = screens["id"].tolist()
    selected = st.selectbox("Screen wählen", screen_ids, key="screen_select")
    row = screens.loc[screens["id"] == selected].iloc[0]

    with st.form("edit_screen"):
        name = st.text_input("Name", row["name"])
        mode = st.selectbox("Modus", ["DETAIL", "OVERVIEW", "WAREHOUSE"], index=["DETAIL", "OVERVIEW", "WAREHOUSE"].index(row["mode"]) if row["mode"] in ["DETAIL","OVERVIEW","WAREHOUSE"] else 0)
        filter_type = st.selectbox("Filter Typ", ["ALLE", "KRANKENHAUS", "ALTENHEIM", "MVZ"],
                                   index=["ALLE", "KRANKENHAUS", "ALTENHEIM", "MVZ"].index(row["filter_type"]))
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
    st.markdown("### Laufband / Ticker")

    ticker_row = load_ticker(conn)
    with st.form("ticker_form"):
        text = st.text_area("Laufband-Text", value=ticker_row["text"] or "", height=100)
        active = st.checkbox("Laufband aktiv", value=bool(ticker_row["active"]))
        submitted = st.form_submit_button("Laufband speichern")
        if submitted:
            conn.execute("UPDATE ticker SET text=?, active=? WHERE id=1", (text.strip(), 1 if active else 0))
            conn.commit()
            st.success("Laufband aktualisiert.")
            st.rerun()


# --------------------------------------------------
# Admin Mode
# --------------------------------------------------

def show_admin_mode():
    require_login()

    role = st.session_state.get("role", "viewer")
    username = st.session_state.get("username", "")

    st.title("Abfahrten – Admin / Disposition")
    st.caption(f"Eingeloggt als: {username} (Rolle: {role})")

    if st.sidebar.button("Logout"):
        st.session_state["logged_in"] = False
        st.session_state["role"] = None
        st.session_state["username"] = None
        st.rerun()

    conn = get_connection()
    can_edit = role == "admin"

    if can_edit:
        tabs = st.tabs(["Abfahrten", "Einrichtungen", "Screens", "Touren"])
        with tabs[0]:
            show_admin_departures(conn, can_edit=True)
        with tabs[1]:
            show_admin_locations(conn)
        with tabs[2]:
            show_admin_screens(conn)
        with tabs[3]:
            show_admin_tours(conn, can_edit=True)
    else:
        tabs = st.tabs(["Abfahrten", "Touren"])
        with tabs[0]:
            show_admin_departures(conn, can_edit=False)
        with tabs[1]:
            show_admin_tours(conn, can_edit=False)


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
