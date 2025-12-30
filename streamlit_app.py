import streamlit as st
import pandas as pd
import sqlite3
from datetime import datetime
from pathlib import Path

st.set_page_config(page_title="Abfahrten", layout="wide")

DB_PATH = Path("abfahrten.db")

# --------------------------------------------------
# DB-Helfer
# --------------------------------------------------

@st.cache_resource
def get_connection():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    init_db(conn)
    return conn


def init_db(conn: sqlite3.Connection):
    cur = conn.cursor()

    # Tabellen anlegen, falls nicht vorhanden
    cur.execute("""
        CREATE TABLE IF NOT EXISTS locations (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            name        TEXT NOT NULL,
            type        TEXT NOT NULL,        -- KRANKENHAUS / ALTENHEIM / MVZ
            active      INTEGER NOT NULL DEFAULT 1
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS departures (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            datetime     TEXT NOT NULL,
            location_id  INTEGER NOT NULL,
            vehicle      TEXT,
            status       TEXT NOT NULL DEFAULT 'GEPLANT',
            note         TEXT,
            FOREIGN KEY(location_id) REFERENCES locations(id)
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS screens (
            id                        INTEGER PRIMARY KEY,
            name                      TEXT NOT NULL,
            mode                      TEXT NOT NULL,   -- DETAIL / OVERVIEW
            filter_type               TEXT NOT NULL DEFAULT 'ALLE',
            filter_locations          TEXT,
            refresh_interval_seconds  INTEGER NOT NULL DEFAULT 30
        )
    """)

    # Standard-Screens anlegen, wenn Tabelle leer ist
    cur.execute("SELECT COUNT(*) AS cnt FROM screens")
    cnt = cur.fetchone()["cnt"]
    if cnt == 0:
        screens = [
            (1, "Krankenhaus Monitor 1", "DETAIL", "KRANKENHAUS", "", 15),
            (2, "Krankenhaus Monitor 2", "DETAIL", "KRANKENHAUS", "", 15),
            (3, "Altenheim Monitor", "DETAIL", "ALTENHEIM", "", 15),
            (4, "MVZ Monitor", "DETAIL", "MVZ", "", 15),
            (5, "Übersicht Links", "OVERVIEW", "ALLE", "", 20),
            (6, "Übersicht Rechts", "OVERVIEW", "ALLE", "", 20),
        ]
        cur.executemany(
            "INSERT INTO screens (id, name, mode, filter_type, filter_locations, refresh_interval_seconds) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            screens,
        )

    conn.commit()


def read_df(conn: sqlite3.Connection, query: str, params=()):
    return pd.read_sql_query(query, conn, params=params)


# --------------------------------------------------
# Daten-Ladefunktionen
# --------------------------------------------------

def load_locations(conn):
    return read_df(conn, "SELECT id, name, type, active FROM locations ORDER BY id")


def load_departures(conn):
    df = read_df(conn, """
        SELECT d.id, d.datetime, d.location_id, d.vehicle, d.status, d.note,
               l.name AS location_name, l.type AS location_type
        FROM departures d
        JOIN locations l ON d.location_id = l.id
    """)
    if not df.empty:
        df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce")
    return df


def load_screens(conn):
    return read_df(conn, "SELECT * FROM screens ORDER BY id")


# --------------------------------------------------
# Monitor-Ansicht (Display)
# --------------------------------------------------

def get_screen_data(conn: sqlite3.Connection, screen_id: int):
    screens = load_screens(conn)
    if screen_id not in screens["id"].tolist():
        return None, None

    screen = screens.loc[screens["id"] == screen_id].iloc[0]
    mode = screen["mode"]
    filter_type = screen["filter_type"] or "ALLE"
    filter_locations = (screen["filter_locations"] or "").strip()

    departures_all = load_departures(conn)
    now = datetime.now()

    if departures_all.empty:
        return screen, pd.DataFrame(columns=["datetime", "location_name", "location_type", "vehicle", "status", "note"])

    future = departures_all[departures_all["datetime"] >= now].copy()

    # Filter aktive Einrichtungen
    locs = load_locations(conn)
    active_ids = locs[locs["active"] == 1]["id"].tolist()
    future = future[future["location_id"].isin(active_ids)]

    # Filter Typ
    if filter_type != "ALLE":
        future = future[future["location_type"] == filter_type]

    # Filter bestimmte Einrichtungen
    if filter_locations:
        ids = [int(x.strip()) for x in filter_locations.split(",") if x.strip().isdigit()]
        if ids:
            future = future[future["location_id"].isin(ids)]

    future = future.sort_values("datetime")

    return screen, future


def show_display_mode(screen_id: int):
    hide_style = """
    <style>
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    header {visibility: hidden;}
    </style>
    """
    st.markdown(hide_style, unsafe_allow_html=True)

    if not screen_id:
        st.error("Parameter 'screenId' fehlt oder ungültig.")
        return

    conn = get_connection()
    screen, data = get_screen_data(conn, screen_id)

    if screen is None:
        st.error(f"Screen {screen_id} ist nicht konfiguriert.")
        return

    st.markdown(f"## {screen['name']} (Screen {screen_id})")
    st.caption(f"Modus: {screen['mode']} • Letzte Aktualisierung: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    if data is None or data.empty:
        st.info("Keine Abfahrten.")
        return

    if screen["mode"] == "DETAIL":
        view = data[["datetime", "location_name", "location_type", "vehicle", "status", "note"]].copy()
        view = view.rename(columns={
            "datetime": "Datum/Uhrzeit",
            "location_name": "Einrichtung",
            "location_type": "Typ",
            "vehicle": "Fahrzeug",
            "status": "Status",
            "note": "Hinweis",
        })
        st.dataframe(view, use_container_width=True, hide_index=True)
    else:
        grouped = list(data.groupby("location_type"))
        if not grouped:
            st.info("Keine Daten für Übersicht.")
            return
        cols = st.columns(3)
        for idx, (typ, group) in enumerate(grouped):
            col = cols[idx % 3]
            with col:
                st.markdown(f"### {typ}")
                for _, row in group.head(5).iterrows():
                    dt_str = row["datetime"].strftime("%H:%M") if pd.notnull(row["datetime"]) else "-"
                    txt = f"**{dt_str}** – {row['location_name']}"
                    if row.get("vehicle"):
                        txt += f" · {row['vehicle']}"
                    if row.get("note"):
                        txt += f"<br/><span style='font-size:14px;color:#ccc;'>{row['note']}</span>"
                    st.markdown(f"<div style='margin-bottom:10px;'>{txt}</div>", unsafe_allow_html=True)


# --------------------------------------------------
# Admin-Ansicht
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
        col1, col2 = st.columns(2)
        with col1:
            name = st.text_input("Name", "")
            typ = st.selectbox("Typ", ["KRANKENHAUS", "ALTENHEIM", "MVZ"])
        with col2:
            active = st.checkbox("Aktiv", value=True)
        submitted = st.form_submit_button("Speichern")
        if submitted:
            if not name.strip():
                st.error("Name darf nicht leer sein.")
            else:
                cur = conn.cursor()
                cur.execute(
                    "INSERT INTO locations (name, type, active) VALUES (?, ?, ?)",
                    (name.strip(), typ, 1 if active else 0),
                )
                conn.commit()
                st.success("Einrichtung gespeichert.")
                st.experimental_rerun()


def show_admin_departures(conn):
    st.subheader("Abfahrten")

    locations = load_locations(conn)
    if locations.empty:
        st.warning("Bitte zuerst Einrichtungen anlegen.")
        return

    st.markdown("### Neue Abfahrt anlegen")
    with st.form("new_departure"):
        col1, col2 = st.columns(2)
        with col1:
            dt_str = st.text_input("Datum & Uhrzeit (YYYY-MM-DD HH:MM)", "")
            loc_id = st.selectbox(
                "Einrichtung",
                options=locations["id"],
                format_func=lambda i: locations.loc[locations["id"] == i, "name"].values[0],
            )
        with col2:
            vehicle = st.text_input("Fahrzeug (optional)", "")
            status = st.selectbox("Status", ["GEPLANT", "UNTERWEGS", "ABGESCHLOSSEN", "STORNIERT"])
        note = st.text_input("Hinweis (optional)", "")

        submitted = st.form_submit_button("Speichern")
        if submitted:
            try:
                dt = pd.to_datetime(dt_str)
            except Exception:
                st.error("Ungültiges Datum/Uhrzeit-Format. Beispiel: 2025-01-10 13:45")
            else:
                cur = conn.cursor()
                cur.execute(
                    "INSERT INTO departures (datetime, location_id, vehicle, status, note) "
                    "VALUES (?, ?, ?, ?, ?)",
                    (dt.isoformat(), int(loc_id), vehicle.strip(), status, note.strip()),
                )
                conn.commit()
                st.success("Abfahrt gespeichert.")
                st.experimental_rerun()

    st.markdown("### Alle Abfahrten")
    deps = load_departures(conn)
    if deps.empty:
        st.info("Noch keine Abfahrten vorhanden.")
    else:
        view = deps[["id", "datetime", "location_name", "location_type", "vehicle", "status", "note"]].copy()
        view = view.sort_values("datetime")
        st.dataframe(view, use_container_width=True)


def show_admin_screens(conn):
    st.subheader("Screens / Monitore")

    screens = load_screens(conn)
    st.dataframe(screens, use_container_width=True)

    st.markdown("### Screen bearbeiten")
    if screens.empty:
        st.info("Noch keine Screens vorhanden.")
        return

    screen_ids = screens["id"].tolist()
    selected = st.selectbox("Screen wählen", screen_ids)
    row = screens.loc[screens["id"] == selected].iloc[0]

    with st.form("edit_screen"):
        name = st.text_input("Name", row["name"])
        mode = st.selectbox("Modus", ["DETAIL", "OVERVIEW"], index=["DETAIL", "OVERVIEW"].index(row["mode"]))
        filter_type = st.selectbox(
            "Filter Typ", ["ALLE", "KRANKENHAUS", "ALTENHEIM", "MVZ"],
            index=["ALLE", "KRANKENHAUS", "ALTENHEIM", "MVZ"].index(row["filter_type"])
        )
        filter_locations = st.text_input("Filter Locations (IDs, Komma-getrennt)", row["filter_locations"] or "")
        refresh = st.number_input(
            "Refresh-Intervall (Sekunden)", min_value=5, max_value=300,
            value=int(row["refresh_interval_seconds"]),
        )
        submitted = st.form_submit_button("Speichern")
        if submitted:
            cur = conn.cursor()
            cur.execute("""
                UPDATE screens
                SET name = ?, mode = ?, filter_type = ?, filter_locations = ?, refresh_interval_seconds = ?
                WHERE id = ?
            """, (name, mode, filter_type, filter_locations, int(refresh), int(selected)))
            conn.commit()
            st.success("Screen aktualisiert.")
            st.experimental_rerun()


def show_admin_mode():
    st.title("Abfahrten – Admin / Disposition")
    conn = get_connection()

    tabs = st.tabs(["Abfahrten", "Einrichtungen", "Screens"])
    with tabs[0]:
        show_admin_departures(conn)
    with tabs[1]:
        show_admin_locations(conn)
    with tabs[2]:
        show_admin_screens(conn)


# --------------------------------------------------
# Einstieg
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
