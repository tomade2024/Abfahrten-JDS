import streamlit as st
import pandas as pd
import sqlite3
from datetime import datetime, timedelta, time as dtime
from pathlib import Path

# --------------------------------------------------
# Grundeinstellungen
# --------------------------------------------------

st.set_page_config(page_title="Abfahrten", layout="wide")
DB_PATH = Path("abfahrten.db")

# Einfache Login-Daten (BITTE anpassen!)
USERS = {
    "admin": "admin123",  # Benutzername: admin, Passwort: admin123
}

WEEKDAYS_DE = ["Montag", "Dienstag", "Mittwoch", "Donnerstag", "Freitag", "Samstag", "Sonntag"]
WEEKDAY_TO_INT = {name: i for i, name in enumerate(WEEKDAYS_DE)}  # Montag = 0, ...


# --------------------------------------------------
# Login-Funktion
# --------------------------------------------------

def require_login():
    """Einfacher Login-Schutz für den Admin-Bereich."""
    if st.session_state.get("logged_in"):
        return

    st.title("Login")

    with st.form("login_form"):
        username = st.text_input("Benutzername")
        password = st.text_input("Passwort", type="password")
        submitted = st.form_submit_button("Einloggen")

        if submitted:
            if username in USERS and USERS[username] == password:
                st.session_state["logged_in"] = True
                st.success("Erfolgreich eingeloggt.")
                st.rerun()
            else:
                st.error("Benutzername oder Passwort ist falsch.")

    st.stop()


# --------------------------------------------------
# DB-Verbindung + Initialisierung
# --------------------------------------------------

@st.cache_resource
def get_connection():
    """Erzeugt einmalig eine DB-Verbindung und initialisiert das Schema."""
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    init_db(conn)
    return conn


def init_db(conn: sqlite3.Connection):
    """Lege Tabellen an und füge Standard-Screens ein, falls noch nicht vorhanden."""
    cur = conn.cursor()

    # Tabelle: Einrichtungen
    cur.execute("""
        CREATE TABLE IF NOT EXISTS locations (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            name        TEXT NOT NULL,
            type        TEXT NOT NULL,        -- KRANKENHAUS / ALTENHEIM / MVZ
            active      INTEGER NOT NULL DEFAULT 1
        )
    """)

    # Tabelle: Abfahrten
    cur.execute("""
        CREATE TABLE IF NOT EXISTS departures (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            datetime     TEXT NOT NULL,       -- ISO-String
            location_id  INTEGER NOT NULL,
            vehicle      TEXT,
            status       TEXT NOT NULL DEFAULT 'GEPLANT',
            note         TEXT,
            FOREIGN KEY(location_id) REFERENCES locations(id)
        )
    """)

    # Tabelle: Screens / Monitore
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

    # Standard-Screens anlegen, falls noch keine vorhanden sind
    cur.execute("SELECT COUNT(*) AS cnt FROM screens")
    cnt = cur.fetchone()["cnt"]
    if cnt == 0:
        screens = [
            (1, "Zone A",           "DETAIL",   "KRANKENHAUS", "", 15),
            (2, "Zone B",           "DETAIL",   "KRANKENHAUS", "", 15),
            (3, "Zone C",           "DETAIL",   "ALTENHEIM",   "", 15),
            (4, "Zone D",           "DETAIL",   "MVZ",         "", 15),
            (5, "Übersicht Links",  "OVERVIEW", "ALLE",        "", 20),
            (6, "Übersicht Rechts", "OVERVIEW", "ALLE",        "", 20),
        ]
        cur.executemany("""
            INSERT INTO screens (id, name, mode, filter_type, filter_locations, refresh_interval_seconds)
            VALUES (?, ?, ?, ?, ?, ?)
        """, screens)

    conn.commit()


# --------------------------------------------------
# Hilfsfunktionen für DB -> DataFrame
# --------------------------------------------------

def read_df(conn: sqlite3.Connection, query: str, params=()):
    return pd.read_sql_query(query, conn, params=params)


def load_locations(conn):
    return read_df(conn, "SELECT id, name, type, active FROM locations ORDER BY id")


def load_departures_with_locations(conn):
    """Abfahrten inkl. Einrichtungsnamen/-typ."""
    df = read_df(conn, """
        SELECT d.id,
               d.datetime,
               d.location_id,
               d.vehicle,
               d.status,
               d.note,
               l.name AS location_name,
               l.type AS location_type,
               l.active AS location_active
        FROM departures d
        JOIN locations l ON d.location_id = l.id
    """)
    if not df.empty:
        df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce")
    return df


def load_screens(conn):
    return read_df(conn, "SELECT * FROM screens ORDER BY id")


# --------------------------------------------------
# Zeit-Helfer: nächster Termin für Wochentag + Stunde
# --------------------------------------------------

def next_datetime_for_weekday_hour(weekday_name: str, hour: int) -> datetime:
    """Berechne den nächsten Termin (ab jetzt) für Wochentag + volle Stunde."""
    now = datetime.now()
    target_weekday = WEEKDAY_TO_INT[weekday_name]  # 0..6
    today_weekday = now.weekday()

    days_ahead = (target_weekday - today_weekday) % 7
    candidate_date = now.date() + timedelta(days=days_ahead)
    candidate_dt = datetime.combine(candidate_date, dtime(hour=hour, minute=0))

    # Falls die Zeit heute schon vorbei ist → eine Woche weiter
    if candidate_dt <= now:
        candidate_dt = candidate_dt + timedelta(days=7)

    return candidate_dt


# --------------------------------------------------
# Monitor-Ansicht (Display-Modus)
# --------------------------------------------------

def get_screen_data(conn: sqlite3.Connection, screen_id: int):
    screens = load_screens(conn)
    if screen_id not in screens["id"].tolist():
        return None, None

    screen = screens.loc[screens["id"] == screen_id].iloc[0]
    filter_type = screen["filter_type"] or "ALLE"
    filter_locations = (screen["filter_locations"] or "").strip()

    deps = load_departures_with_locations(conn)
    now = datetime.now()

    if deps.empty:
        return screen, pd.DataFrame(columns=["datetime", "location_name", "location_type", "vehicle", "status", "note"])

    future = deps[deps["datetime"] >= now].copy()

    # Nur aktive Einrichtungen
    future = future[future["location_active"] == 1]

    # Filter nach Typ (KH/Altenheim/MVZ)
    if filter_type != "ALLE":
        future = future[future["location_type"] == filter_type]

    # Filter nach bestimmten Einrichtungen (IDs)
    if filter_locations:
        ids = [int(x.strip()) for x in filter_locations.split(",") if x.strip().isdigit()]
        if ids:
            future = future[future["location_id"].isin(ids)]

    future = future.sort_values("datetime")

    return screen, future


def show_display_mode(screen_id: int):
    """Anzeige für einen Monitor-Client (Display)."""

    # Streamlit-Menü & Footer ausblenden
    hide_style = """
    <style>
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    header {visibility: hidden;}
    </style>
    """
    st.markdown(hide_style, unsafe_allow_html=True)

    if not screen_id:
        st.error("Parameter 'screenId' fehlt oder ist ungültig.")
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

    # Monitore 1–4: keine Typen, Fahrzeuge oder Status – nur Einrichtung + Hinweis
    if screen["mode"] == "DETAIL" and screen["id"] in [1, 2, 3, 4]:
        view = data[["location_name", "note"]].copy()
        view = view.rename(columns={
            "location_name": "Einrichtung",
            "note": "Hinweis",
        })
        st.dataframe(view, use_container_width=True, hide_index=True)

    elif screen["mode"] == "DETAIL":
        # Standard-Detail-Ansicht (falls später weitere Detail-Screens kommen)
        view = data[["location_name", "location_type", "vehicle", "status", "note"]].copy()
        view = view.rename(columns={
            "location_name": "Einrichtung",
            "location_type": "Typ",
            "vehicle": "Fahrzeug",
            "status": "Status",
            "note": "Hinweis",
        })
        st.dataframe(view, use_container_width=True, hide_index=True)

    else:
        # OVERVIEW: nach Typ gruppieren, max. 3 Spalten, ohne Uhrzeit
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
                    txt = f"**{row['location_name']}**"
                    if row.get("vehicle"):
                        txt += f" · {row['vehicle']}"
                    if row.get("note"):
                        txt += f"<br/><span style='font-size:14px;color:#ccc;'>{row['note']}</span>"
                    st.markdown(f"<div style='margin-bottom:10px;'>{txt}</div>", unsafe_allow_html=True)


# --------------------------------------------------
# Admin-Ansicht: Einrichtungen
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
                st.rerun()


# --------------------------------------------------
# Admin-Ansicht: Abfahrten (inkl. Löschen)
# --------------------------------------------------

def show_admin_departures(conn):
    st.subheader("Abfahrten")

    locations = load_locations(conn)
    if locations.empty:
        st.warning("Bitte zuerst mindestens eine Einrichtung anlegen.")
        return

    # Neue Abfahrt anlegen – mit Wochentag + Stunde
    st.markdown("### Neue Abfahrt anlegen")

    with st.form("new_departure"):
        col1, col2, col3 = st.columns(3)

        with col1:
            weekday = st.selectbox("Wochentag", WEEKDAYS_DE)
        with col2:
            hours = [f"{h:02d}:00" for h in range(24)]
            hour_label = st.selectbox("Uhrzeit (volle Stunde)", hours, index=8)  # Standard: 08:00
            hour_int = int(hour_label.split(":")[0])
        with col3:
            loc_id = st.selectbox(
                "Einrichtung",
                options=locations["id"],
                format_func=lambda i: locations.loc[locations["id"] == i, "name"].values[0],
            )

        vehicle = st.text_input("Fahrzeug (optional)", "")
        status = st.selectbox("Status", ["GEPLANT", "UNTERWEGS", "ABGESCHLOSSEN", "STORNIERT"])
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

    # Bestehende Abfahrten anzeigen + Löschbutton
    st.markdown("### Bestehende Abfahrten")

    deps = load_departures_with_locations(conn)
    if deps.empty:
        st.info("Noch keine Abfahrten vorhanden.")
        return

    deps = deps.sort_values("datetime")

    for _, row in deps.iterrows():
        with st.container():
            col1, col2 = st.columns([3, 1])

            # Linke Seite: Infos
            with col1:
                dt_str = row["datetime"].strftime("%Y-%m-%d %H:%M") if pd.notnull(row["datetime"]) else "-"
                st.markdown(
                    f"**{dt_str} – {row['location_name']} ({row['location_type']})**"
                )
                info = []
                if row.get("vehicle"):
                    info.append(f"Fahrzeug: {row['vehicle']}")
                if row.get("status"):
                    info.append(f"Status: {row['status']}")
                if row.get("note"):
                    info.append(f"Hinweis: {row['note']}")
                if info:
                    st.markdown("<br>".join(info), unsafe_allow_html=True)

            # Rechte Seite: Löschbutton
            with col2:
                if st.button("Löschen", key=f"del_{row['id']}"):
                    cur = conn.cursor()
                    cur.execute("DELETE FROM departures WHERE id = ?", (int(row["id"]),))
                    conn.commit()
                    st.success("Abfahrt gelöscht.")
                    st.rerun()


# --------------------------------------------------
# Admin-Ansicht: Screens (inkl. Links zu Monitoren)
# --------------------------------------------------

def show_admin_screens(conn):
    st.subheader("Screens / Monitore")

    screens = load_screens(conn)

    st.write("Aktuelle Konfiguration:")
    st.dataframe(screens, use_container_width=True)

    if screens.empty:
        st.info("Noch keine Screens vorhanden (werden beim ersten Start automatisch angelegt).")
        return

    st.markdown("### Links zu den Monitoren")

    st.info(
        "Diese Links kannst du anklicken, um die jeweilige Monitoransicht zu öffnen, "
        "oder die URL aus der Adressleiste kopieren und auf dem entsprechenden Monitor verwenden."
    )

    for _, row in screens.iterrows():
        screen_id = int(row["id"])
        name = row["name"]
        link = f"?mode=display&screenId={screen_id}"
        st.markdown(
            f"- **Screen {screen_id} – {name}**: "
            f"[Monitor öffnen]({link})  "
            f"(URL-Parameter: `{link}`)"
        )

    st.markdown("---")
    st.markdown("### Screen bearbeiten")

    screen_ids = screens["id"].tolist()
    selected = st.selectbox("Screen wählen", screen_ids)
    row = screens.loc[screens["id"] == selected].iloc[0]

    with st.form("edit_screen"):
        name = st.text_input("Name", row["name"])
        mode = st.selectbox("Modus", ["DETAIL", "OVERVIEW"],
                            index=["DETAIL", "OVERVIEW"].index(row["mode"]))
        filter_type = st.selectbox(
            "Filter Typ", ["ALLE", "KRANKENHAUS", "ALTENHEIM", "MVZ"],
            index=["ALLE", "KRANKENHAUS", "ALTENHEIM", "MVZ"].index(row["filter_type"])
        )
        filter_locations = st.text_input(
            "Filter Locations (IDs, Komma-getrennt)",
            row["filter_locations"] or "",
            help="Mehrere Einrichtungen: z.B. 1,2,5. Leer lassen = alle Einrichtungen entsprechend Filter Typ.",
        )
        refresh = st.number_input(
            "Refresh-Intervall (Sekunden)",
            min_value=5, max_value=300,
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
            st.rerun()


# --------------------------------------------------
# Admin-Modus (Hauptansicht)
# --------------------------------------------------

def show_admin_mode():
    require_login()  # Login erzwingen

    st.title("Abfahrten – Admin / Disposition")

    # Logout-Button
    if st.sidebar.button("Logout"):
        st.session_state["logged_in"] = False
        st.rerun()

    conn = get_connection()

    tabs = st.tabs(["Abfahrten", "Einrichtungen", "Screens"])
    with tabs[0]:
        show_admin_departures(conn)
    with tabs[1]:
        show_admin_locations(conn)
    with tabs[2]:
        show_admin_screens(conn)


# --------------------------------------------------
# Einstiegspunkt
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
