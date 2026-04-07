import streamlit as st
import sqlite3
import pandas as pd
from datetime import datetime, timedelta, time as dtime
from zoneinfo import ZoneInfo
import os

# =============================
# CONFIG
# =============================
TZ = ZoneInfo("Europe/Berlin")
DB_PATH = os.path.join(os.path.expanduser("~"), "AppData", "Local", "Abfahrten", "db.sqlite")

os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)

# =============================
# DB CONNECTION
# =============================
@st.cache_resource
def get_connection():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn

# =============================
# DB INIT + MIGRATION
# =============================
def init_db(conn):
    cur = conn.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS locations (
        id INTEGER PRIMARY KEY,
        name TEXT,
        active INTEGER DEFAULT 1
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS departures (
        id INTEGER PRIMARY KEY,
        datetime TEXT,
        location_id INTEGER,
        note TEXT,
        screen_id INTEGER,
        countdown_enabled INTEGER DEFAULT 1,
        cooled_required INTEGER DEFAULT 0
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS holiday_tours (
        id INTEGER PRIMARY KEY,
        name TEXT,
        holiday_date TEXT,
        hour INTEGER,
        minute INTEGER,
        location_id INTEGER,
        screen_ids TEXT,
        active INTEGER DEFAULT 1
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS holiday_tour_stops (
        id INTEGER PRIMARY KEY,
        holiday_tour_id INTEGER,
        location_id INTEGER,
        position INTEGER
    )
    """)

    conn.commit()

def migrate_db(conn):
    init_db(conn)

# =============================
# HELPERS
# =============================
def now():
    return datetime.now(TZ)

def read_df(conn, query, params=()):
    try:
        return pd.read_sql_query(query, conn, params=params)
    except Exception as e:
        st.error(f"SQL Fehler:\n{e}")
        return pd.DataFrame()

def parse_screens(s):
    if not s:
        return []
    return [int(x.strip()) for x in str(s).split(",") if x.strip().isdigit()]

# =============================
# HOLIDAY MATERIALIZER
# =============================
def materialize_holidays(conn):
    df = read_df(conn, """
        SELECT h.*, hs.location_id, hs.position
        FROM holiday_tours h
        JOIN holiday_tour_stops hs ON hs.holiday_tour_id = h.id
    """)

    if df.empty:
        return

    cur = conn.cursor()

    for _, r in df.iterrows():
        dt = datetime.strptime(r["holiday_date"], "%Y-%m-%d").replace(
            hour=int(r["hour"]), minute=int(r["minute"]), tzinfo=TZ
        )

        for sid in parse_screens(r["screen_ids"]):
            cur.execute("""
                INSERT INTO departures (datetime, location_id, note, screen_id)
                VALUES (?, ?, ?, ?)
            """, (
                dt.isoformat(),
                int(r["location_id"]),
                r["name"],
                sid
            ))

    conn.commit()

# =============================
# DATA FETCH
# =============================
def get_departures(conn, screen_id):
    df = read_df(conn, """
        SELECT d.*, l.name as location_name
        FROM departures d
        LEFT JOIN locations l ON l.id = d.location_id
        WHERE d.screen_id = ?
        ORDER BY d.datetime ASC
    """, (screen_id,))

    if df.empty:
        return df

    df["datetime"] = pd.to_datetime(df["datetime"])
    return df

# =============================
# DISPLAY
# =============================
def render_monitor(conn, screen_id):
    st.markdown(f"## Monitor {screen_id}")

    df = get_departures(conn, screen_id)

    if df.empty:
        st.info("Keine Abfahrten")
        return

    now_time = now()

    for _, row in df.iterrows():
        delta = (row["datetime"] - now_time).total_seconds() / 60

        blink = ""
        if 0 <= delta <= 10:
            blink = "⚠️"

        st.markdown(
            f"""
            **{row['location_name']}**  
            {row['datetime'].strftime('%H:%M')}  
            {blink} {row['note']}
            """
        )

# =============================
# SPLIT SCREEN
# =============================
def render_split(conn, left, right):
    col1, col2 = st.columns(2)

    with col1:
        render_monitor(conn, left)

    with col2:
        render_monitor(conn, right)

# =============================
# ADMIN
# =============================
def admin(conn):
    st.title("Admin")

    tab1, tab2, tab3 = st.tabs(["Abfahrten", "Feiertage", "Monitore"])

    with tab1:
        st.subheader("Neue Abfahrt")

        loc = st.number_input("Location ID", 1)
        dt = st.datetime_input("Zeit")
        note = st.text_input("Hinweis")
        screen = st.number_input("Monitor", 1)

        if st.button("Speichern"):
            conn.execute("""
                INSERT INTO departures (datetime, location_id, note, screen_id)
                VALUES (?, ?, ?, ?)
            """, (dt.isoformat(), loc, note, screen))
            conn.commit()
            st.success("Gespeichert")

    with tab2:
        st.subheader("Feiertagstour")

        name = st.text_input("Name")
        date = st.date_input("Datum")
        hour = st.number_input("Stunde", 0, 23)
        minute = st.number_input("Minute", 0, 59)
        loc = st.number_input("Location", 1)
        screens = st.text_input("Screens (1,2,3)")

        if st.button("Feiertag speichern"):
            cur = conn.cursor()
            cur.execute("""
                INSERT INTO holiday_tours (name, holiday_date, hour, minute, location_id, screen_ids)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (name, date.isoformat(), hour, minute, loc, screens))

            hid = cur.lastrowid

            cur.execute("""
                INSERT INTO holiday_tour_stops (holiday_tour_id, location_id, position)
                VALUES (?, ?, 1)
            """, (hid, loc))

            conn.commit()
            st.success("Gespeichert")

    with tab3:
        st.info("Split Monitor z.B. 1+2")

# =============================
# MAIN
# =============================
def main():
    conn = get_connection()
    migrate_db(conn)

    materialize_holidays(conn)

    mode = st.sidebar.selectbox("Modus", ["Monitor", "Split", "Admin"])

    if mode == "Monitor":
        screen = st.sidebar.number_input("Monitor ID", 1)
        render_monitor(conn, screen)

    elif mode == "Split":
        left = st.sidebar.number_input("Links", 1)
        right = st.sidebar.number_input("Rechts", 2)
        render_split(conn, left, right)

    else:
        admin(conn)

if __name__ == "__main__":
    main()
