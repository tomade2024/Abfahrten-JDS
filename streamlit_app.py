import streamlit as st
import pandas as pd
import sqlite3
from datetime import datetime, timedelta, time as dtime
from pathlib import Path

from streamlit_autorefresh import st_autorefresh  # für Autorefresh

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
    # Falls color-Spalte noch nicht existiert -> hinzufügen
    cur.execute("PRAGMA table_info(locations)")
    cols = [row[1] for row in cur.fetchall()]
    if "color" not in cols:
        cur.execute("ALTER TABLE locations ADD COLUMN color TEXT")

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

    # Tabelle: feste Touren
    cur.execute("""
        CREATE TABLE IF NOT EXISTS tours (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            name         TEXT NOT NULL,
            weekday      TEXT NOT NULL,       -- z.B. 'Montag'
            hour         INTEGER NOT NULL,    -- 0-23
            location_id  INTEGER NOT NULL,
            note         TEXT,
            active       INTEGER NOT NULL DEFAULT 1,
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
    return read_df(conn, "SELECT id, name, type, active, color FROM locations ORDER BY id")


def load_departures_with_locations(conn):
    """Abfahrten inkl. Einrichtungsnamen/-typ + Farbe."""
    df = read_df(conn, """
        SELECT d.id,
               d.datetime,
               d.location_id,
               d.vehicle,
               d.status,
               d.note,
               l.name   AS location_name,
               l.type   AS location_type,
               l.active AS location_active,
               l.color  AS location_color
        FROM departures d
        JOIN locations l ON d.location_id = l.id
    """)
    if not df.empty:
        df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce")
    return df


def load_screens(conn):
    return read_df(conn, "SELECT * FROM screens ORDER BY id")


def load_tours(conn):
    return read_df(conn, """
        SELECT t.id,
               t.name,
               t.weekday,
               t.hour,
               t.location_id,
               t.note,
               t.active,
               l.name  AS location_name
        FROM tours t
        JOIN locations l ON t.location_id = l.id
        ORDER BY t.id
    """)


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
        return screen, pd.DataFrame(columns=["datetime", "location_name", "location_type",
                                             "vehicle", "status", "note", "location_color"])

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


def render_big_table(headers, rows, row_colors=None):
    """Hilfsfunktion: große HTML-Tabelle für Monitore, optional mit Zeilenfarben."""
    thead_cells = "".join(f"<th>{h}</th>" for h in headers)
    body_rows = ""

    rows = list(rows)  # Iterator in Liste umwandeln

    for idx, r in enumerate(rows):
        style = ""
        if row_colors is not None and idx < len(row_colors):
            color = row_colors[idx] or ""
            if color:
                style = f' style="background-color: {color};"'
        tds = "".join(f"<td>{c}</td>" for c in r)
        body_rows += f"<tr{style}>{tds}</tr>"

    table_html = f"""
    <table class="big-table">
      <thead><tr>{thead_cells}</tr></thead>
      <tbody>{body_rows}</tbody>
    </table>
    """
    st.markdown(table_html, unsafe_allow_html=True)


def show_display_mode(screen_id: int):
    """Anzeige für einen Monitor-Client (Display) mit Autorefresh & großer Schrift."""

    # Menü & Footer ausblenden + große Schrift und Tabellendesign
    display_style = """
    <style>
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    header {visibility: hidden;}

    .block-container {
        padding-top: 0.5rem;
    }

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
    }
    .big-table th {
        font-weight: 700;
    }
    </style>
    """
    st.markdown(display_style, unsafe_allow_html=True)

    if not screen_id:
        st.error("Parameter 'screenId' fehlt oder ist ungültig.")
        return

    conn = get_connection()
    screen, data = get_screen_data(conn, screen_id)

    # Autorefresh aktivieren (Intervall aus Screen-Konfiguration)
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

    st.markdown(f"## {screen['name']} (Screen {screen_id})")
    st.caption(f"Modus: {screen['mode']} • Aktualisierung alle {interval_sec} Sekunden")

    if data is None or data.empty:
        st.info("Keine Abfahrten.")
        return

    # Monitore 1–4: keine Typen, Fahrzeuge oder Status – nur Einrichtung + Hinweis, farbige Zeilen
    if screen["mode"] == "DETAIL" and screen["id"] in [1, 2, 3, 4]:
        subset = data[["location_name", "note", "location_color"]].copy()
        subset["location_color"] = subset["location_color"].fillna("")
        headers = ["Einrichtung", "Hinweis"]
        rows = subset[["location_name", "note"]].itertuples(index=False, name=None)
        row_colors = subset["location_color"].tolist()
        render_big_table(headers, rows, row_colors=row_colors)

    elif screen["mode"] == "DETAIL":
        # (Optional) Standard-Detail-Ansicht für andere Detail-Screens
        subset = data[["location_name", "location_type", "vehicle", "status", "note", "location_color"]].copy()
        subset["location_color"] = subset["location_color"].fillna("")
        headers = ["Einrichtung", "Typ", "Fahrzeug", "Status", "Hinweis"]
        rows = subset[["location_name", "location_type", "vehicle", "status", "note"]].itertuples(index=False, name=None)
        row_colors = subset["location_color"].tolist()
        render_big_table(headers, rows, row_colors=row_colors)

    else:
        # OVERVIEW: nach Typ gruppieren, Einträge farbig hinterlegt
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
                        txt += f"<br/><span style='font-size:28px;color:#ccc;'>{row['note']}</span>"

                    color = row.get("location_color") or ""
                    style_str = "margin-bottom:10px;"
                    if color:
                        style_str += f"background-color:{color};padding:0.3em 0.5em;border-radius:0.2em;"

                    st.markdown(f"<div style='{style_str}'>{txt}</div>", unsafe_allow_html=True)


# --------------------------------------------------
# Admin-Ansicht: Einrichtungen (voll editierbar + löschen)
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
            color = st.color_picker("Farbe", "#007bff")

        submitted = st.form_submit_button("Speichern")
        if submitted:
            if not name.strip():
                st.error("Name darf nicht leer sein.")
            else:
                cur = conn.cursor()
                cur.execute(
                    "INSERT INTO locations (name, type, active, color) VALUES (?, ?, ?, ?)",
                    (name.strip(), typ, 1 if active else 0, color),
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
                    "Typ", ["KRANKENHAUS", "ALTENHEIM", "MVZ"],
                    index=["KRANKENHAUS", "ALTENHEIM", "MVZ"].index(row["type"])
                )
            with col2:
                active_edit = st.checkbox("Aktiv", value=bool(row["active"]))
            with col3:
                color_init = row["color"] if isinstance(row["color"], str) and row["color"] else "#007bff"
                color_edit = st.color_picker("Farbe", color_init)

            col_btn1, col_btn2 = st.columns(2)
            with col_btn1:
                submitted_edit = st.form_submit_button("Änderungen speichern")
            with col_btn2:
                delete_requested = st.form_submit_button("Einrichtung löschen")

            if submitted_edit:
                cur = conn.cursor()
                cur.execute(
                    "UPDATE locations SET name = ?, type = ?, active = ?, color = ? WHERE id = ?",
                    (name_edit.strip(), typ_edit, 1 if active_edit else 0, color_edit, int(selected)),
                )
                conn.commit()
                st.success("Einrichtung aktualisiert.")
                st.rerun()

            if delete_requested:
                # Prüfen, ob noch Abfahrten oder Touren auf diese Einrichtung zeigen
                cur = conn.cursor()
                cur.execute("SELECT COUNT(*) FROM departures WHERE location_id = ?", (int(selected),))
                dep_count = cur.fetchone()[0]
                cur.execute("SELECT COUNT(*) FROM tours WHERE location_id = ?", (int(selected),))
                tour_count = cur.fetchone()[0]

                if dep_count > 0 or tour_count > 0:
                    st.error(
                        f"Einrichtung kann nicht gelöscht werden – es existieren noch "
                        f"{dep_count} Abfahrten und {tour_count} Tour(en) für diese Einrichtung."
                    )
                else:
                    cur.execute("DELETE FROM locations WHERE id = ?", (int(selected),))
                    conn.commit()
                    st.success("Einrichtung gelöscht.")
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
                if st.button("Löschen", key=f"del_dep_{row['id']}"):
                    cur = conn.cursor()
                    cur.execute("DELETE FROM departures WHERE id = ?", (int(row["id"]),))
                    conn.commit()
                    st.success("Abfahrt gelöscht.")
                    st.rerun()


# --------------------------------------------------
# Admin-Ansicht: feste Touren (CRUD + Abfahrt erzeugen)
# --------------------------------------------------

def show_admin_tours(conn):
    st.subheader("Feste Touren")

    locations = load_locations(conn)
    if locations.empty:
        st.warning("Bitte zuerst Einrichtungen anlegen – Touren brauchen eine Einrichtung.")
        return

    tours = load_tours(conn)

    st.write("Bestehende Touren:")
    if tours.empty:
        st.info("Noch keine Touren angelegt.")
    else:
        view = tours.copy()
        view["Zeit"] = view["hour"].apply(lambda h: f"{int(h):02d}:00")
        st.dataframe(
            view[["id", "name", "weekday", "Zeit", "location_name", "note", "active"]],
            use_container_width=True,
        )

    st.markdown("### Neue Tour anlegen")
    with st.form("new_tour"):
        col1, col2, col3 = st.columns(3)
        with col1:
            tour_name = st.text_input("Tour-Name", "")
            weekday = st.selectbox("Wochentag", WEEKDAYS_DE, key="tour_weekday_new")
        with col2:
            hours = [f"{h:02d}:00" for h in range(24)]
            hour_label = st.selectbox("Uhrzeit (volle Stunde)", hours, index=8, key="tour_hour_new")
            hour_int = int(hour_label.split(":")[0])
        with col3:
            loc_id = st.selectbox(
                "Einrichtung",
                options=locations["id"],
                format_func=lambda i: locations.loc[locations["id"] == i, "name"].values[0],
                key="tour_loc_new",
            )

        note = st.text_input("Hinweis (optional)", key="tour_note_new")
        active = st.checkbox("Aktiv", value=True, key="tour_active_new")

        submitted = st.form_submit_button("Tour speichern")
        if submitted:
            if not tour_name.strip():
                st.error("Tour-Name darf nicht leer sein.")
            else:
                cur = conn.cursor()
                cur.execute(
                    """
                    INSERT INTO tours (name, weekday, hour, location_id, note, active)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (tour_name.strip(), weekday, hour_int, int(loc_id), note.strip(), 1 if active else 0),
                )
                conn.commit()
                st.success("Tour gespeichert.")
                st.rerun()

    # Tour bearbeiten / löschen / Abfahrt erzeugen
    tours = load_tours(conn)
    if tours.empty:
        return

    st.markdown("### Tour bearbeiten / Abfahrt erzeugen / löschen")
    tour_ids = tours["id"].tolist()
    selected = st.selectbox("Tour auswählen", tour_ids)
    row = tours.loc[tours["id"] == selected].iloc[0]

    with st.form("edit_tour"):
        col1, col2, col3 = st.columns(3)
        with col1:
            name_edit = st.text_input("Tour-Name", row["name"])
            weekday_edit = st.selectbox(
                "Wochentag", WEEKDAYS_DE,
                index=WEEKDAYS_DE.index(row["weekday"]) if row["weekday"] in WEEKDAYS_DE else 0,
                key="tour_weekday_edit",
            )
        with col2:
            hours = [f"{h:02d}:00" for h in range(24)]
            hour_edit_label = st.selectbox(
                "Uhrzeit (volle Stunde)", hours,
                index=int(row["hour"]) if 0 <= int(row["hour"]) < 24 else 8,
                key="tour_hour_edit",
            )
            hour_edit = int(hour_edit_label.split(":")[0])
        with col3:
            loc_id_edit = st.selectbox(
                "Einrichtung",
                options=locations["id"],
                format_func=lambda i: locations.loc[locations["id"] == i, "name"].values[0],
                index=locations[locations["id"] == row["location_id"]].index[0],
                key="tour_loc_edit",
            )

        note_edit = st.text_input("Hinweis (optional)", row["note"] or "", key="tour_note_edit")
        active_edit = st.checkbox("Aktiv", value=bool(row["active"]), key="tour_active_edit")

        col_btn1, col_btn2, col_btn3 = st.columns(3)
        with col_btn1:
            submitted_edit = st.form_submit_button("Änderungen speichern")
        with col_btn2:
            create_dep = st.form_submit_button("Abfahrt aus Tour anlegen")
        with col_btn3:
            delete_tour = st.form_submit_button("Tour löschen")

        if submitted_edit:
            cur = conn.cursor()
            cur.execute(
                """
                UPDATE tours
                SET name = ?, weekday = ?, hour = ?, location_id = ?, note = ?, active = ?
                WHERE id = ?
                """,
                (name_edit.strip(), weekday_edit, hour_edit, int(loc_id_edit),
                 note_edit.strip(), 1 if active_edit else 0, int(selected)),
            )
            conn.commit()
            st.success("Tour aktualisiert.")
            st.rerun()

        if create_dep:
            # Abfahrt aus Tour erzeugen
            dt = next_datetime_for_weekday_hour(weekday_edit, hour_edit)
            cur = conn.cursor()
            cur.execute(
                """
                INSERT INTO departures (datetime, location_id, vehicle, status, note)
                VALUES (?, ?, ?, ?, ?)
                """,
                (dt.isoformat(), int(loc_id_edit), "", "GEPLANT", note_edit.strip()),
            )
            conn.commit()
            st.success(f"Abfahrt aus Tour erzeugt (nächster Termin: {dt.strftime('%Y-%m-%d %H:%M')}).")
            st.rerun()

        if delete_tour:
            cur = conn.cursor()
            cur.execute("DELETE FROM tours WHERE id = ?", (int(selected),))
            conn.commit()
            st.success("Tour gelöscht.")
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

    tabs = st.tabs(["Abfahrten", "Einrichtungen", "Screens", "Touren"])
    with tabs[0]:
        show_admin_departures(conn)
    with tabs[1]:
        show_admin_locations(conn)
    with tabs[2]:
        show_admin_screens(conn)
    with tabs[3]:
        show_admin_tours(conn)


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
