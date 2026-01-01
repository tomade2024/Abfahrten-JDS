import streamlit as st
import pandas as pd
import sqlite3
from datetime import datetime, timedelta, time as dtime
from pathlib import Path

from streamlit_autorefresh import st_autorefresh

# --------------------------------------------------
# Grundeinstellungen
# --------------------------------------------------

st.set_page_config(page_title="Abfahrten", layout="wide")
DB_PATH = Path("abfahrten.db")

USERS = {
    "admin": {"password": "admin123", "role": "admin"},
    "dispo": {"password": "dispo123", "role": "viewer"},
}

WEEKDAYS_DE = ["Montag", "Dienstag", "Mittwoch", "Donnerstag", "Freitag", "Samstag", "Sonntag"]
WEEKDAY_TO_INT = {name: i for i, name in enumerate(WEEKDAYS_DE)}  # Montag = 0

STATUS_OPTIONS = ["GEPLANT", "BEREIT", "UNTERWEGS", "ABGESCHLOSSEN", "STORNIERT"]


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
# DB
# --------------------------------------------------

@st.cache_resource
def get_connection():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    init_db(conn)
    return conn


def init_db(conn: sqlite3.Connection):
    cur = conn.cursor()

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
    loc_cols = [row[1] for row in cur.fetchall()]
    if "color" not in loc_cols:
        cur.execute("ALTER TABLE locations ADD COLUMN color TEXT")
    if "text_color" not in loc_cols:
        cur.execute("ALTER TABLE locations ADD COLUMN text_color TEXT")

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
            FOREIGN KEY(location_id) REFERENCES locations(id)
        )
        """
    )
    cur.execute("PRAGMA table_info(tours)")
    tour_cols = [row[1] for row in cur.fetchall()]
    if "screen_ids" not in tour_cols:
        cur.execute("ALTER TABLE tours ADD COLUMN screen_ids TEXT")

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
            refresh_interval_seconds  INTEGER NOT NULL DEFAULT 30
        )
        """
    )
    cur.execute("PRAGMA table_info(screens)")
    s_cols = [row[1] for row in cur.fetchall()]
    if "holiday_flag" not in s_cols:
        cur.execute("ALTER TABLE screens ADD COLUMN holiday_flag INTEGER NOT NULL DEFAULT 0")
    if "special_flag" not in s_cols:
        cur.execute("ALTER TABLE screens ADD COLUMN special_flag INTEGER NOT NULL DEFAULT 0")

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

    cur.execute("SELECT COUNT(*) AS cnt FROM screens")
    cnt = cur.fetchone()["cnt"]
    if cnt == 0:
        screens = [
            (1, "Zone A",           "DETAIL",   "ALLE", "", 15, 0, 0),
            (2, "Zone B",           "DETAIL",   "ALLE", "", 15, 0, 0),
            (3, "Zone C",           "DETAIL",   "ALLE", "", 15, 0, 0),
            (4, "Zone D",           "DETAIL",   "ALLE", "", 15, 0, 0),
            (5, "Übersicht Links",  "OVERVIEW", "ALLE", "", 20, 0, 0),
            (6, "Übersicht Rechts", "OVERVIEW", "ALLE", "", 20, 0, 0),
        ]
        cur.executemany(
            """
            INSERT INTO screens
                (id, name, mode, filter_type, filter_locations, refresh_interval_seconds, holiday_flag, special_flag)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            screens,
        )

    conn.commit()


# --------------------------------------------------
# DB Helpers
# --------------------------------------------------

def read_df(conn: sqlite3.Connection, query: str, params=()):
    return pd.read_sql_query(query, conn, params=params)


def load_locations(conn):
    return read_df(conn, "SELECT id, name, type, active, color, text_color FROM locations ORDER BY id")


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
    return df


def load_screens(conn):
    return read_df(conn, "SELECT * FROM screens ORDER BY id")


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
               l.name  AS location_name
        FROM tours t
        JOIN locations l ON t.location_id = l.id
        ORDER BY t.id
        """,
    )


def load_ticker(conn):
    df = read_df(conn, "SELECT * FROM ticker WHERE id = 1")
    return df.iloc[0] if not df.empty else None


# --------------------------------------------------
# Time Helpers
# --------------------------------------------------

def next_datetime_for_weekday_hour(weekday_name: str, hour: int) -> datetime:
    now = datetime.now()
    target_weekday = WEEKDAY_TO_INT[weekday_name]
    today_weekday = now.weekday()

    days_ahead = (target_weekday - today_weekday) % 7
    candidate_date = now.date() + timedelta(days=days_ahead)
    candidate_dt = datetime.combine(candidate_date, dtime(hour=hour, minute=0))

    if candidate_dt <= now:
        candidate_dt = candidate_dt + timedelta(days=7)

    return candidate_dt


def format_hhmmss(delta: timedelta) -> str:
    total = int(delta.total_seconds())
    if total < 0:
        total = 0
    h = total // 3600
    m = (total % 3600) // 60
    s = total % 60
    return f"{h:02d}:{m:02d}:{s:02d}"


# --------------------------------------------------
# Status-Automation
# --------------------------------------------------

def update_departure_statuses(conn: sqlite3.Connection):
    """
    Automatisch:
    - bei Erreichen der Abfahrtzeit: GEPLANT/UNTERWEGS -> BEREIT (wenn nicht STORNIERT/ABGESCHLOSSEN)
    - 20 Minuten nach Abfahrtzeit: -> ABGESCHLOSSEN
    """
    now = datetime.now()
    cur = conn.cursor()

    df = read_df(conn, "SELECT id, datetime, status FROM departures")
    if df.empty:
        return

    df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce")
    df = df.dropna(subset=["datetime"])

    to_ready = []
    to_done = []

    for _, r in df.iterrows():
        dep_id = int(r["id"])
        dep_dt = r["datetime"].to_pydatetime()
        status = str(r["status"] or "").upper()

        if status in ("STORNIERT", "ABGESCHLOSSEN"):
            continue

        if now >= dep_dt + timedelta(minutes=20):
            to_done.append(dep_id)
        elif now >= dep_dt:
            # "Countdown abgelaufen" => BEREIT
            if status != "BEREIT":
                to_ready.append(dep_id)

    if to_ready:
        cur.executemany("UPDATE departures SET status = 'BEREIT' WHERE id = ?", [(i,) for i in to_ready])
    if to_done:
        cur.executemany("UPDATE departures SET status = 'ABGESCHLOSSEN' WHERE id = ?", [(i,) for i in to_done])

    if to_ready or to_done:
        conn.commit()


# --------------------------------------------------
# Screen Data (Departures + Tour-Stops)
# --------------------------------------------------

def get_screen_data(conn: sqlite3.Connection, screen_id: int):
    screens = load_screens(conn)
    if screen_id not in screens["id"].tolist():
        return None, None

    screen = screens.loc[screens["id"] == screen_id].iloc[0]
    filter_type = screen["filter_type"] or "ALLE"
    filter_locations = (screen["filter_locations"] or "").strip()

    now = datetime.now()

    # Normale Abfahrten: wir zeigen auch die letzten 20 Minuten (damit "BEREIT" sichtbar bleibt)
    deps = load_departures_with_locations(conn)
    if deps.empty:
        future = pd.DataFrame(
            columns=[
                "id", "datetime", "location_id", "vehicle", "status", "note",
                "location_name", "location_type", "location_active", "location_color", "location_text_color"
            ]
        )
    else:
        window_start = now - timedelta(minutes=20)
        future = deps[deps["datetime"] >= window_start].copy()
        future = future[future["location_active"] == 1]
        # standardmäßig abgeschlossene/stornierte ausblenden
        future = future[~future["status"].isin(["ABGESCHLOSSEN", "STORNIERT"])]

        if filter_type != "ALLE":
            future = future[future["location_type"] == filter_type]

        if filter_locations:
            ids = [int(x.strip()) for x in filter_locations.split(",") if x.strip().isdigit()]
            if ids:
                future = future[future["location_id"].isin(ids)]

    # Tour-Stopps als virtuelle Abfahrten (nur nächster Termin)
    tour_stops_df = read_df(
        conn,
        """
        SELECT t.id         AS tour_id,
               t.name       AS tour_name,
               t.weekday,
               t.hour,
               t.note       AS tour_note,
               t.active     AS tour_active,
               t.screen_ids AS tour_screen_ids,
               ts.location_id,
               ts.position,
               l.name       AS location_name,
               l.type       AS location_type,
               l.active     AS location_active,
               l.color      AS location_color,
               l.text_color AS location_text_color
        FROM tours t
        JOIN tour_stops ts ON ts.tour_id = t.id
        JOIN locations l   ON ts.location_id = l.id
        """,
    )

    virtual_rows = []

    def tour_has_screen(screen_ids_value):
        if screen_ids_value is None:
            return False
        s = str(screen_ids_value).strip()
        if s == "":
            return False
        for part in s.split(","):
            p = part.strip()
            if not p:
                continue
            try:
                sid = int(p)
            except Exception:
                continue
            if sid == screen_id:
                return True
        return False

    if not tour_stops_df.empty:
        tour_stops_df = tour_stops_df[(tour_stops_df["tour_active"] == 1) & (tour_stops_df["location_active"] == 1)]
        tour_stops_df = tour_stops_df[tour_stops_df["tour_screen_ids"].apply(tour_has_screen)]

        if filter_type != "ALLE":
            tour_stops_df = tour_stops_df[tour_stops_df["location_type"] == filter_type]

        if filter_locations:
            ids = [int(x.strip()) for x in filter_locations.split(",") if x.strip().isdigit()]
            if ids:
                tour_stops_df = tour_stops_df[tour_stops_df["location_id"].isin(ids)]

        for _, row in tour_stops_df.iterrows():
            try:
                hour_int = int(row["hour"])
            except Exception:
                hour_int = 0
            next_dt = next_datetime_for_weekday_hour(row["weekday"], hour_int)

            virtual_rows.append(
                {
                    "id": f"tour_{row['tour_id']}_{row['position']}",
                    "datetime": next_dt,
                    "location_id": row["location_id"],
                    "vehicle": "",
                    "status": "TOUR",
                    "note": (row["tour_note"] or ""),
                    "location_name": row["location_name"],
                    "location_type": row["location_type"],
                    "location_active": row["location_active"],
                    "location_color": row["location_color"],
                    "location_text_color": row["location_text_color"],
                }
            )

    if virtual_rows:
        tours_future = pd.DataFrame(virtual_rows)
        if future.empty:
            future = tours_future
        else:
            future = pd.concat([future, tours_future], ignore_index=True)

    if not future.empty:
        future["datetime"] = pd.to_datetime(future["datetime"], errors="coerce")
        future = future.dropna(subset=["datetime"]).sort_values("datetime")

    return screen, future


# --------------------------------------------------
# Render Helpers
# --------------------------------------------------

def render_big_table(headers, rows, row_colors=None, text_colors=None):
    thead_cells = "".join(f"<th>{h}</th>" for h in headers)
    body_rows = ""
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
        style = ""
        if style_parts:
            style = ' style="' + "".join(style_parts) + '"'
        tds = "".join(f"<td>{c}</td>" for c in r)
        body_rows += f"<tr{style}>{tds}</tr>"

    table_html = f"""
    <table class="big-table">
      <thead><tr>{thead_cells}</tr></thead>
      <tbody>{body_rows}</tbody>
    </table>
    """
    st.markdown(table_html, unsafe_allow_html=True)


def escape_html(text: str) -> str:
    return text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def render_next_departure_countdown(data: pd.DataFrame):
    """
    Zeigt Countdown zur nächsten Abfahrt.
    Countdown startet 3h vorher.
    Nächste Abfahrt = kleinster datetime-Wert in data.
    """
    if data is None or data.empty:
        st.info("Keine Abfahrten.")
        return

    now = datetime.now()
    next_row = data.iloc[0]
    next_dt = pd.to_datetime(next_row["datetime"]).to_pydatetime()

    delta = next_dt - now
    start_window = timedelta(hours=3)

    if delta <= start_window:
        countdown = format_hhmmss(delta)
        st.markdown(
            f"""
            <div style="border:2px solid #444; padding:16px; border-radius:10px; margin-bottom:14px;">
              <div style="font-size:28px; font-weight:700;">Countdown bis zur nächsten Abfahrt</div>
              <div style="font-size:64px; font-weight:900; margin-top:6px;">{countdown}</div>
            </div>
            """,
            unsafe_allow_html=True,
        )
    else:
        st.markdown(
            f"""
            <div style="border:2px solid #444; padding:16px; border-radius:10px; margin-bottom:14px;">
              <div style="font-size:28px; font-weight:700;">Nächste Abfahrt</div>
              <div style="font-size:52px; font-weight:900; margin-top:6px;">{next_dt.strftime('%H:%M')}</div>
            </div>
            """,
            unsafe_allow_html=True,
        )


# --------------------------------------------------
# Display Mode
# --------------------------------------------------

def show_display_mode(screen_id: int):
    display_style = """
    <style>
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    header {visibility: hidden;}

    .block-container { padding-top: 0.5rem; }

    body, .block-container, .stMarkdown, .stText, .stDataFrame, div, span {
        font-size: 32px !important;
    }

    .big-table { width: 100%; border-collapse: collapse; }
    .big-table th, .big-table td {
        border-bottom: 1px solid #555;
        padding: 0.4em 0.8em;
        text-align: left;
    }
    .big-table th { font-weight: 700; }

    .ticker {
        position: fixed;
        bottom: 0; left: 0; width: 100%;
        background: #000; color: #fff;
        overflow: hidden; white-space: nowrap; z-index: 9999;
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
    """
    st.markdown(display_style, unsafe_allow_html=True)

    if not screen_id:
        st.error("Parameter 'screenId' fehlt oder ist ungültig.")
        return

    conn = get_connection()

    # Statusautomation bei jedem Display-Refresh
    update_departure_statuses(conn)

    screen, data = get_screen_data(conn, screen_id)

    interval_sec = 30
    if screen is not None and "refresh_interval_seconds" in screen.index:
        try:
            interval_sec = int(screen["refresh_interval_seconds"])
        except Exception:
            interval_sec = 30

    st_autorefresh(interval=interval_sec * 1000, key=f"display_refresh_{screen_id}")

    if screen is None:
        st.error(f"Screen {screen_id} ist nicht konfiguriert.")
        return

    holiday_active = bool(screen.get("holiday_flag", 0)) if "holiday_flag" in screen.index else False
    special_active = bool(screen.get("special_flag", 0)) if "special_flag" in screen.index else False

    if holiday_active or special_active:
        st.markdown(
            """
            <style>
            body, .block-container { background-color:#000000 !important; color:#ffffff !important; }
            </style>
            """,
            unsafe_allow_html=True,
        )
        parts = []
        if holiday_active:
            parts.append("Feiertagsbelieferung")
        if special_active:
            parts.append("Sonderplan")
        message = " - ".join(p.upper() for p in parts)

        st.markdown(
            f"""
            <div style="
                display:flex; justify-content:center; align-items:center;
                height:100vh; width:100%;
                background-color:#000000; color:#ffffff;
                font-size:72px; font-weight:900;
                text-transform:uppercase; text-align:center;">
                {message}
            </div>
            """,
            unsafe_allow_html=True,
        )
        return

    st.markdown(f"## {screen['name']} (Screen {screen_id})")
    st.caption(f"Modus: {screen['mode']} • Aktualisierung alle {interval_sec} Sekunden")

    # Countdown oben (nur wenn es mindestens einen Eintrag gibt)
    if data is not None and not data.empty:
        render_next_departure_countdown(data)

    if data is None or data.empty:
        st.info("Keine Abfahrten.")
    else:
        # Zone A–D: nur Einrichtung + Hinweis (keine Typen/Fahrzeuge/Status)
        if screen["mode"] == "DETAIL" and screen["id"] in [1, 2, 3, 4]:
            subset = data[["location_name", "note", "location_color", "location_text_color"]].copy()
            subset["location_color"] = subset["location_color"].fillna("")
            subset["location_text_color"] = subset["location_text_color"].fillna("")
            headers = ["Einrichtung", "Hinweis"]
            rows = subset[["location_name", "note"]].itertuples(index=False, name=None)
            render_big_table(headers, rows, row_colors=subset["location_color"].tolist(), text_colors=subset["location_text_color"].tolist())

        elif screen["mode"] == "DETAIL":
            subset = data[["location_name", "location_type", "vehicle", "status", "note", "location_color", "location_text_color"]].copy()
            subset["location_color"] = subset["location_color"].fillna("")
            subset["location_text_color"] = subset["location_text_color"].fillna("")
            headers = ["Einrichtung", "Typ", "Fahrzeug", "Status", "Hinweis"]
            rows = subset[["location_name", "location_type", "vehicle", "status", "note"]].itertuples(index=False, name=None)
            render_big_table(headers, rows, row_colors=subset["location_color"].tolist(), text_colors=subset["location_text_color"].tolist())

        else:
            grouped = list(data.groupby("location_type"))
            if not grouped:
                st.info("Keine Daten für Übersicht.")
            else:
                cols = st.columns(3)
                for idx, (typ, group) in enumerate(grouped):
                    col = cols[idx % 3]
                    with col:
                        st.markdown(f"### {typ}")
                        for _, row in group.head(8).iterrows():
                            txt = f"**{row['location_name']}**"
                            if row.get("status") and row["status"] != "TOUR":
                                txt += f" · {row['status']}"
                            if row.get("note"):
                                txt += f"<br/><span style='font-size:28px;'>{row['note']}</span>"

                            bg = row.get("location_color") or ""
                            tc = row.get("location_text_color") or ""
                            style_str = "margin-bottom:10px;"
                            if bg:
                                style_str += f"background-color:{bg};padding:0.3em 0.5em;border-radius:0.2em;"
                            if tc:
                                style_str += f"color:{tc};"
                            st.markdown(f"<div style='{style_str}'>{txt}</div>", unsafe_allow_html=True)

    ticker_row = load_ticker(conn)
    if ticker_row is not None and ticker_row["active"] and (ticker_row["text"] or "").strip():
        text = escape_html(ticker_row["text"].strip())
        st.markdown(f"<div class='ticker'><div class='ticker__inner'>{text}</div></div>", unsafe_allow_html=True)


# --------------------------------------------------
# Admin: Einrichtungen
# --------------------------------------------------

def show_admin_locations(conn):
    st.subheader("Einrichtungen")

    locations = load_locations(conn)
    st.write("Bestehende Einrichtungen:")
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
            active = st.checkbox("Aktiv", value=True)
        with col3:
            color = st.color_picker("Hintergrundfarbe", "#007bff")
            text_color = st.color_picker("Schriftfarbe", "#000000")

        submitted = st.form_submit_button("Speichern")
        if submitted:
            if not name.strip():
                st.error("Name darf nicht leer sein.")
            else:
                cur = conn.cursor()
                cur.execute(
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
                typ_edit = st.selectbox(
                    "Typ",
                    ["KRANKENHAUS", "ALTENHEIM", "MVZ"],
                    index=["KRANKENHAUS", "ALTENHEIM", "MVZ"].index(row["type"]),
                )
            with col2:
                active_edit = st.checkbox("Aktiv", value=bool(row["active"]))
            with col3:
                color_init = row["color"] if isinstance(row["color"], str) and row["color"] else "#007bff"
                text_color_init = row["text_color"] if isinstance(row["text_color"], str) and row["text_color"] else "#000000"
                color_edit = st.color_picker("Hintergrundfarbe", color_init)
                text_color_edit = st.color_picker("Schriftfarbe", text_color_init)

            col_btn1, col_btn2 = st.columns(2)
            with col_btn1:
                submitted_edit = st.form_submit_button("Änderungen speichern")
            with col_btn2:
                delete_requested = st.form_submit_button("Einrichtung löschen")

            if submitted_edit:
                cur = conn.cursor()
                cur.execute(
                    "UPDATE locations SET name = ?, type = ?, active = ?, color = ?, text_color = ? WHERE id = ?",
                    (name_edit.strip(), typ_edit, 1 if active_edit else 0, color_edit, text_color_edit, int(selected)),
                )
                conn.commit()
                st.success("Einrichtung aktualisiert.")
                st.rerun()

            if delete_requested:
                cur = conn.cursor()
                cur.execute("SELECT COUNT(*) FROM departures WHERE location_id = ?", (int(selected),))
                dep_count = cur.fetchone()[0]
                cur.execute("SELECT COUNT(*) FROM tours WHERE location_id = ?", (int(selected),))
                tour_count = cur.fetchone()[0]
                cur.execute("SELECT COUNT(*) FROM tour_stops WHERE location_id = ?", (int(selected),))
                stops_count = cur.fetchone()[0]

                if dep_count > 0 or tour_count > 0 or stops_count > 0:
                    st.error(
                        f"Einrichtung kann nicht gelöscht werden – Verknüpfungen vorhanden "
                        f"(Abfahrten: {dep_count}, Touren: {tour_count}, Tour-Stops: {stops_count})."
                    )
                else:
                    cur.execute("DELETE FROM locations WHERE id = ?", (int(selected),))
                    conn.commit()
                    st.success("Einrichtung gelöscht.")
                    st.rerun()


# --------------------------------------------------
# Admin: Abfahrten
# --------------------------------------------------

def show_admin_departures(conn, can_edit: bool):
    st.subheader("Abfahrten")

    # Statusautomation auch im Admin
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
                    options=locations["id"],
                    format_func=lambda i: locations.loc[locations["id"] == i, "name"].values[0],
                )

            vehicle = st.text_input("Fahrzeug (optional)", "")
            status = st.selectbox("Status", STATUS_OPTIONS, index=0)
            note = st.text_input("Hinweis (optional)", "")

            submitted = st.form_submit_button("Speichern")
            if submitted:
                dt = next_datetime_for_weekday_hour(weekday, hour_int)
                cur = conn.cursor()
                cur.execute(
                    """
                    INSERT INTO departures (datetime, location_id, vehicle, status, note)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (dt.isoformat(), int(loc_id), vehicle.strip(), status, note.strip()),
                )
                conn.commit()
                st.success(f"Abfahrt gespeichert (nächster Termin: {dt.strftime('%Y-%m-%d %H:%M')}).")
                st.rerun()

    st.markdown("### Bestehende Abfahrten")
    deps = load_departures_with_locations(conn)
    if deps.empty:
        st.info("Noch keine Abfahrten vorhanden.")
        return

    deps = deps.sort_values("datetime")

    for _, row in deps.iterrows():
        with st.container():
            col1, col2 = st.columns([3, 1])

            with col1:
                dt_str = row["datetime"].strftime("%Y-%m-%d %H:%M") if pd.notnull(row["datetime"]) else "-"
                st.markdown(f"**{dt_str} – {row['location_name']} ({row['location_type']})**")
                info = []
                if row.get("vehicle"):
                    info.append(f"Fahrzeug: {row['vehicle']}")
                if row.get("status"):
                    info.append(f"Status: {row['status']}")
                if row.get("note"):
                    info.append(f"Hinweis: {row['note']}")
                if info:
                    st.markdown("<br>".join(info), unsafe_allow_html=True)

            with col2:
                if can_edit:
                    if st.button("Löschen", key=f"del_dep_{row['id']}"):
                        cur = conn.cursor()
                        cur.execute("DELETE FROM departures WHERE id = ?", (int(row["id"]),))
                        conn.commit()
                        st.success("Abfahrt gelöscht.")
                        st.rerun()
                else:
                    st.caption("Keine Löschrechte")


# --------------------------------------------------
# Admin: Touren (Mehrere Einrichtungen + Screen-Zuordnung)
# --------------------------------------------------

def show_admin_tours(conn, can_edit: bool):
    st.subheader("Feste Touren")

    locations = load_locations(conn)
    if locations.empty:
        st.warning("Bitte zuerst Einrichtungen anlegen – Touren brauchen Einrichtungen.")
        return

    screens = load_screens(conn)
    screen_map = {int(r["id"]): r["name"] for _, r in screens.iterrows()} if not screens.empty else {}

    tours = load_tours(conn)

    st.write("Bestehende Touren:")
    if tours.empty:
        st.info("Noch keine Touren angelegt.")
    else:
        view = tours.copy()
        view["Zeit"] = view["hour"].apply(lambda h: f"{int(h):02d}:00")

        def map_screen_ids(s):
            if s is None or str(s).strip() == "":
                return ""
            res = []
            for part in str(s).split(","):
                p = part.strip()
                if not p:
                    continue
                try:
                    sid = int(p)
                except Exception:
                    continue
                res.append(f"{sid}: {screen_map.get(sid, f'Screen {sid}')}")
            return ", ".join(res)

        view["Monitore"] = view["screen_ids"].apply(map_screen_ids)
        st.dataframe(view[["id", "name", "weekday", "Zeit", "location_name", "note", "active", "Monitore"]], use_container_width=True)

    if not can_edit:
        return

    loc_options = list(locations["id"])
    screen_options = list(screen_map.keys())

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
        tour_locations_new = st.multiselect(
            "Einrichtungen (Stops)",
            options=loc_options,
            format_func=lambda i: locations.loc[locations["id"] == i, "name"].values[0],
            key="new_tour_locations",
        )

    note_new = st.text_input("Hinweis (optional)", "", key="new_tour_note")
    active_new = st.checkbox("Aktiv", value=True, key="new_tour_active")

    screens_selected = st.multiselect(
        "Monitore (Screens) für diese Tour",
        options=screen_options,
        format_func=lambda sid: f"{sid}: {screen_map.get(sid, f'Screen {sid}')}",
        key="new_tour_screens",
    )
    screen_ids_str_new = ",".join(str(s) for s in screens_selected)

    if st.button("Tour speichern", key="btn_new_tour_save"):
        if not tour_name.strip():
            st.error("Tour-Name darf nicht leer sein.")
        elif not tour_locations_new:
            st.error("Bitte mindestens eine Einrichtung für die Tour auswählen.")
        else:
            primary_loc = int(tour_locations_new[0])
            cur = conn.cursor()
            cur.execute(
                """
                INSERT INTO tours (name, weekday, hour, location_id, note, active, screen_ids)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (tour_name.strip(), weekday, hour_int, primary_loc, note_new.strip(), 1 if active_new else 0, screen_ids_str_new),
            )
            tour_id = cur.lastrowid
            for pos, loc_id in enumerate(tour_locations_new):
                cur.execute("INSERT INTO tour_stops (tour_id, location_id, position) VALUES (?, ?, ?)",
                            (tour_id, int(loc_id), pos))
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

    stops_df = read_df(conn, "SELECT location_id FROM tour_stops WHERE tour_id = ? ORDER BY position", params=(int(selected),))
    existing_stop_ids = stops_df["location_id"].tolist() if not stops_df.empty else [row["location_id"]]

    col1, col2, col3 = st.columns(3)
    with col1:
        name_edit = st.text_input("Tour-Name", row["name"], key=f"edit_tour_name_{selected}")
        weekday_edit = st.selectbox(
            "Wochentag", WEEKDAYS_DE,
            index=WEEKDAYS_DE.index(row["weekday"]) if row["weekday"] in WEEKDAYS_DE else 0,
            key=f"edit_tour_weekday_{selected}",
        )
    with col2:
        hours = [f"{h:02d}:00" for h in range(24)]
        default_hour_index = int(row["hour"]) if str(row["hour"]).isdigit() and 0 <= int(row["hour"]) < 24 else 8
        hour_edit_label = st.selectbox("Uhrzeit (volle Stunde)", hours, index=default_hour_index, key=f"edit_tour_hour_{selected}")
        hour_edit = int(hour_edit_label.split(":")[0])
    with col3:
        tour_locations_edit = st.multiselect(
            "Einrichtungen (Stops)",
            options=loc_options,
            format_func=lambda i: locations.loc[locations["id"] == i, "name"].values[0],
            default=existing_stop_ids,
            key=f"edit_tour_locations_{selected}",
        )

    note_edit = st.text_input("Hinweis (optional)", row["note"] or "", key=f"edit_tour_note_{selected}")
    active_edit = st.checkbox("Aktiv", value=bool(row["active"]), key=f"edit_tour_active_{selected}")

    existing_screen_ids = []
    if row["screen_ids"] not in (None, "", "None"):
        for part in str(row["screen_ids"]).split(","):
            p = part.strip()
            if p.isdigit():
                existing_screen_ids.append(int(p))

    screens_selected_edit = st.multiselect(
        "Monitore (Screens) für diese Tour",
        options=screen_options,
        format_func=lambda sid: f"{sid}: {screen_map.get(sid, f'Screen {sid}')}",
        default=existing_screen_ids,
        key=f"edit_tour_screens_{selected}",
    )
    screen_ids_str_edit = ",".join(str(s) for s in screens_selected_edit)

    col_btn1, col_btn2 = st.columns(2)
    with col_btn1:
        if st.button("Änderungen speichern", key=f"btn_edit_tour_save_{selected}"):
            if not tour_locations_edit:
                st.error("Bitte mindestens eine Einrichtung für die Tour auswählen.")
            else:
                primary_loc = int(tour_locations_edit[0])
                cur = conn.cursor()
                cur.execute(
                    """
                    UPDATE tours
                    SET name = ?, weekday = ?, hour = ?, location_id = ?, note = ?, active = ?, screen_ids = ?
                    WHERE id = ?
                    """,
                    (name_edit.strip(), weekday_edit, hour_edit, primary_loc, note_edit.strip(),
                     1 if active_edit else 0, screen_ids_str_edit, int(selected)),
                )
                cur.execute("DELETE FROM tour_stops WHERE tour_id = ?", (int(selected),))
                for pos, loc_id in enumerate(tour_locations_edit):
                    cur.execute("INSERT INTO tour_stops (tour_id, location_id, position) VALUES (?, ?, ?)",
                                (int(selected), int(loc_id), pos))
                conn.commit()
                st.success("Tour aktualisiert.")
                st.rerun()

    with col_btn2:
        if st.button("Tour löschen", key=f"btn_edit_tour_delete_{selected}"):
            cur = conn.cursor()
            cur.execute("DELETE FROM tour_stops WHERE tour_id = ?", (int(selected),))
            cur.execute("DELETE FROM tours WHERE id = ?", (int(selected),))
            conn.commit()
            st.success("Tour gelöscht.")
            st.rerun()


# --------------------------------------------------
# Admin: Screens + Ticker
# --------------------------------------------------

def show_admin_screens(conn):
    st.subheader("Screens / Monitore")

    screens = load_screens(conn)
    st.write("Aktuelle Konfiguration:")
    st.dataframe(screens, use_container_width=True)

    if screens.empty:
        st.info("Noch keine Screens vorhanden.")
        return

    st.markdown("### Links zu den Monitoren")
    st.info("Die Links kannst du auf den jeweiligen Monitoren öffnen.")
    for _, row in screens.iterrows():
        screen_id = int(row["id"])
        link = f"?mode=display&screenId={screen_id}"
        st.markdown(f"- **Screen {screen_id} – {row['name']}**: [Monitor öffnen]({link})  (Parameter: `{link}`)")

    st.markdown("---")
    st.markdown("### Screen bearbeiten")

    screen_ids = screens["id"].tolist()
    selected = st.selectbox("Screen wählen", screen_ids)
    row = screens.loc[screens["id"] == selected].iloc[0]

    with st.form("edit_screen"):
        name = st.text_input("Name", row["name"])
        mode = st.selectbox("Modus", ["DETAIL", "OVERVIEW"], index=["DETAIL", "OVERVIEW"].index(row["mode"]))
        filter_type = st.selectbox("Filter Typ", ["ALLE", "KRANKENHAUS", "ALTENHEIM", "MVZ"],
                                   index=["ALLE", "KRANKENHAUS", "ALTENHEIM", "MVZ"].index(row["filter_type"]))
        filter_locations = st.text_input("Filter Locations (IDs, Komma-getrennt)", row["filter_locations"] or "")
        refresh = st.number_input("Refresh-Intervall (Sekunden)", min_value=5, max_value=300, value=int(row["refresh_interval_seconds"]))
        holiday_flag = st.checkbox("Feiertagsbelieferung aktiv", value=bool(row.get("holiday_flag", 0)))
        special_flag = st.checkbox("Sonderplan aktiv", value=bool(row.get("special_flag", 0)))

        submitted = st.form_submit_button("Speichern")
        if submitted:
            cur = conn.cursor()
            cur.execute(
                """
                UPDATE screens
                SET name = ?, mode = ?, filter_type = ?, filter_locations = ?,
                    refresh_interval_seconds = ?, holiday_flag = ?, special_flag = ?
                WHERE id = ?
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
    if ticker_row is None:
        st.error("Ticker-Datensatz fehlt.")
        return

    with st.form("ticker_form"):
        text = st.text_area("Laufband-Text", value=ticker_row["text"] or "", height=100)
        active = st.checkbox("Laufband aktiv", value=bool(ticker_row["active"]))
        submitted_ticker = st.form_submit_button("Laufband speichern")
        if submitted_ticker:
            cur = conn.cursor()
            cur.execute("UPDATE ticker SET text = ?, active = ? WHERE id = 1", (text.strip(), 1 if active else 0))
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

    if role == "admin":
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
