import streamlit as st
import pandas as pd
from datetime import datetime
from pathlib import Path

# --------------------------------------------------
# Basis-Konfiguration
# --------------------------------------------------

st.set_page_config(
    page_title="Abfahrten",
    layout="wide",
)

EXCEL_PATH = Path("daten.xls")


# --------------------------------------------------
# Excel-Helfer
# --------------------------------------------------

def init_excel_if_missing():
    """Wenn daten.xlsx nicht existiert, Standardstruktur mit Beispielscreens anlegen."""
    if EXCEL_PATH.exists():
        return

    locations = pd.DataFrame(columns=["id", "name", "type", "active"])
    departures = pd.DataFrame(columns=["id", "datetime", "location_id", "vehicle", "status", "note"])
    screens = pd.DataFrame(columns=[
        "id",
        "name",
        "mode",
        "filter_type",
        "filter_locations",
        "refresh_interval_seconds",
    ])

    # 6 Standard-Screens
    example_screens = [
        (1, "Krankenhaus Monitor 1", "DETAIL", "KRANKENHAUS", "", 15),
        (2, "Krankenhaus Monitor 2", "DETAIL", "KRANKENHAUS", "", 15),
        (3, "Altenheim Monitor", "DETAIL", "ALTENHEIM", "", 15),
        (4, "MVZ Monitor", "DETAIL", "MVZ", "", 15),
        (5, "Übersicht Links", "OVERVIEW", "ALLE", "", 20),
        (6, "Übersicht Rechts", "OVERVIEW", "ALLE", "", 20),
    ]
    for s in example_screens:
        screens.loc[len(screens)] = s

    with pd.ExcelWriter(EXCEL_PATH, engine="openpyxl", mode="w") as writer:
        locations.to_excel(writer, sheet_name="locations", index=False)
        departures.to_excel(writer, sheet_name="departures", index=False)
        screens.to_excel(writer, sheet_name="screens", index=False)


@st.cache_data
def load_excel():
    """Excel-Datei laden (mit Cache)."""
    init_excel_if_missing()
    xl = pd.ExcelFile(EXCEL_PATH)
    locations = pd.read_excel(xl, "locations")
    departures = pd.read_excel(xl, "departures")
    screens = pd.read_excel(xl, "screens")

    # Datentypen aufräumen
    if not locations.empty and "id" in locations.columns:
        locations["id"] = locations["id"].astype(int)

    if not departures.empty:
        if "id" in departures.columns:
            departures["id"] = departures["id"].astype(int)
        if "datetime" in departures.columns:
            departures["datetime"] = pd.to_datetime(departures["datetime"], errors="coerce")
        if "location_id" in departures.columns:
            departures["location_id"] = departures["location_id"].astype(int)

    if not screens.empty and "id" in screens.columns:
        screens["id"] = screens["id"].astype(int)

    return locations, departures, screens


def save_excel(locations, departures, screens):
    """Excel-Datei komplett neu schreiben und Cache leeren."""
    with pd.ExcelWriter(EXCEL_PATH, engine="openpyxl", mode="w") as writer:
        locations.to_excel(writer, sheet_name="locations", index=False)
        departures.to_excel(writer, sheet_name="departures", index=False)
        screens.to_excel(writer, sheet_name="screens", index=False)
    load_excel.clear()


# --------------------------------------------------
# Monitor-Logik (Display-Modus)
# --------------------------------------------------

def get_screen_data(screen_id: int):
    """Filtert Abfahrten nach Screen-Konfiguration."""
    locations, departures, screens = load_excel()

    if screen_id not in screens["id"].values:
        return None, None

    screen = screens.loc[screens["id"] == screen_id].iloc[0]

    mode = screen["mode"]  # DETAIL oder OVERVIEW
    filter_type = str(screen.get("filter_type", "ALLE") or "ALLE")
    filter_locations = str(screen.get("filter_locations") or "").strip()

    now = datetime.now()
    if not departures.empty:
        future_dep = departures[departures["datetime"] >= now].copy()
    else:
        future_dep = departures.copy()

    if future_dep.empty:
        merged = pd.DataFrame(columns=["datetime", "name", "type", "vehicle", "status", "note"])
    else:
        merged = future_dep.merge(
            locations,
            left_on="location_id",
            right_on="id",
            suffixes=("_dep", "_loc"),
        )

        # Nur aktive Einrichtungen
        if "active" in locations.columns:
            merged = merged[(merged.get("active", True) != False)]

        # Filter nach type
        if filter_type and filter_type != "ALLE":
            merged = merged[merged["type"] == filter_type]

        # Filter nach expliziten locations
        if filter_locations:
            ids = [int(x.strip()) for x in filter_locations.split(",") if x.strip().isdigit()]
            if ids:
                merged = merged[merged["location_id"].isin(ids)]

        merged = merged.sort_values("datetime")

    # Anzeige-spezifische Spalten
    view = merged[["datetime", "name", "type", "vehicle", "status", "note"]].copy() if not merged.empty else merged

    return screen, view


def show_display_mode(screen_id: int):
    """Anzeige für einen Monitor."""

    # Streamlit-UI möglichst „clean“ machen
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

    screen, data = get_screen_data(screen_id)
    if screen is None:
        st.error(f"Screen {screen_id} ist in 'screens' nicht konfiguriert.")
        return

    st.markdown(f"## {screen['name']} (Screen {screen_id})")
    st.caption(f"Modus: {screen['mode']} • Letzte Aktualisierung: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    if data is None or data.empty:
        st.info("Keine Abfahrten.")
        return

    if screen["mode"] == "DETAIL":
        # Klassische Liste / Tabelle
        st.dataframe(
            data.rename(columns={
                "datetime": "Datum/Uhrzeit",
                "name": "Einrichtung",
                "type": "Typ",
                "vehicle": "Fahrzeug",
                "status": "Status",
                "note": "Hinweis",
            }),
            use_container_width=True,
            hide_index=True,
        )

    else:
        # OVERVIEW: nach type gruppieren, 3 Spalten
        grouped = list(data.groupby("type"))
        if not grouped:
            st.info("Keine Abfahrten für die Übersicht.")
            return

        # maximal 3 Spalten nebeneinander
        cols = st.columns(3)
        for idx, (typ, group) in enumerate(grouped):
            col = cols[idx % 3]
            with col:
                st.markdown(f"### {typ}")
                # nur die nächsten 5 Abfahrten je Typ
                for _, row in group.head(5).iterrows():
                    dt_str = row["datetime"].strftime("%H:%M") if pd.notnull(row["datetime"]) else "-"
                    txt = f"**{dt_str}** – {row['name']}"
                    if isinstance(row.get("vehicle"), str) and row["vehicle"]:
                        txt += f" · {row['vehicle']}"
                    if isinstance(row.get("note"), str) and row["note"]:
                        txt += f"<br/><span style='font-size:14px;color:#ccc;'>{row['note']}</span>"
                    st.markdown(f"<div style='margin-bottom:10px;'>{txt}</div>", unsafe_allow_html=True)


# --------------------------------------------------
# Admin-Ansicht
# --------------------------------------------------

def show_admin_locations(locations, departures, screens):
    st.subheader("Einrichtungen (locations)")

    st.write("Bestehende Einrichtungen:")
    st.dataframe(locations, use_container_width=True)

    st.markdown("### Neue Einrichtung anlegen")
    with st.form("new_location"):
        col1, col2 = st.columns(2)
        with col1:
            loc_name = st.text_input("Name", "")
            loc_type = st.selectbox("Typ", ["KRANKENHAUS", "ALTENHEIM", "MVZ"])
        with col2:
            active = st.checkbox("Aktiv", value=True)

        submitted = st.form_submit_button("Einrichtung speichern")
        if submitted:
            if not loc_name.strip():
                st.error("Name darf nicht leer sein.")
            else:
                new_id = int(locations["id"].max()) + 1 if not locations.empty else 1
                new_row = {
                    "id": new_id,
                    "name": loc_name.strip(),
                    "type": loc_type,
                    "active": active,
                }
                locations = pd.concat([locations, pd.DataFrame([new_row])], ignore_index=True)
                save_excel(locations, departures, screens)
                st.success(f"Einrichtung '{loc_name}' gespeichert.")
                st.experimental_rerun()


def show_admin_departures(locations, departures, screens):
    st.subheader("Abfahrten (departures)")

    if locations.empty:
        st.warning("Es existieren noch keine Einrichtungen. Bitte zuerst unter 'Einrichtungen' anlegen.")
        return

    st.markdown("### Neue Abfahrt anlegen")
    with st.form("new_departure"):
        col1, col2 = st.columns(2)
        with col1:
            dt_input = st.text_input("Datum & Uhrzeit (YYYY-MM-DD HH:MM)", "")
            loc_id = st.selectbox(
                "Einrichtung",
                options=locations["id"],
                format_func=lambda i: locations.loc[locations["id"] == i, "name"].values[0],
            )
        with col2:
            vehicle = st.text_input("Fahrzeug (optional)", "")
            status = st.selectbox("Status", ["GEPLANT", "UNTERWEGS", "ABGESCHLOSSEN", "STORNIERT"])
        note = st.text_input("Hinweis (optional)", "")

        submitted = st.form_submit_button("Abfahrt speichern")
        if submitted:
            try:
                dt = pd.to_datetime(dt_input)
            except Exception:
                st.error("Ungültiges Datum/Uhrzeit-Format. Beispiel: 2025-01-10 13:45")
            else:
                new_id = int(departures["id"].max()) + 1 if not departures.empty else 1
                new_row = {
                    "id": new_id,
                    "datetime": dt,
                    "location_id": int(loc_id),
                    "vehicle": vehicle.strip(),
                    "status": status,
                    "note": note.strip(),
                }
                departures = pd.concat([departures, pd.DataFrame([new_row])], ignore_index=True)
                save_excel(locations, departures, screens)
                st.success(f"Abfahrt gespeichert (ID {new_id}).")
                st.experimental_rerun()

    st.markdown("### Alle Abfahrten")
    if departures.empty:
        st.info("Noch keine Abfahrten vorhanden.")
    else:
        view = departures.copy()
        view = view.sort_values("datetime")
        st.dataframe(view, use_container_width=True)


def show_admin_screens(locations, departures, screens):
    st.subheader("Screens / Monitore (screens)")

    st.write("Konfiguration der Monitore:")
    st.dataframe(screens, use_container_width=True)

    st.markdown("### Screen bearbeiten")
    if screens.empty:
        st.info("Noch keine Screens vorhanden (werden beim ersten Start normalerweise automatisch angelegt).")
        return

    screen_ids = screens["id"].tolist()
    selected_id = st.selectbox("Screen auswählen", screen_ids)

    screen_row = screens.loc[screens["id"] == selected_id].iloc[0]

    with st.form("edit_screen"):
        name = st.text_input("Name", screen_row["name"])
        mode = st.selectbox("Modus", ["DETAIL", "OVERVIEW"], index=["DETAIL", "OVERVIEW"].index(screen_row["mode"]))
        filter_type = st.selectbox("Filter Typ", ["ALLE", "KRANKENHAUS", "ALTENHEIM", "MVZ"],
                                   index=["ALLE", "KRANKENHAUS", "ALTENHEIM", "MVZ"].index(screen_row["filter_type"]))
        filter_locations = st.text_input("Filter Locations (IDs, Komma-getrennt)", str(screen_row["filter_locations"] or ""))
        refresh_interval = st.number_input("Refresh-Intervall (Sekunden)", min_value=5, max_value=300,
                                           value=int(screen_row["refresh_interval_seconds"] or 30))

        submitted = st.form_submit_button("Speichern")
        if submitted:
            screens.loc[screens["id"] == selected_id, ["name", "mode", "filter_type",
                                                       "filter_locations", "refresh_interval_seconds"]] = [
                name, mode, filter_type, filter_locations, int(refresh_interval)
            ]
            save_excel(locations, departures, screens)
            st.success("Screen-Konfiguration gespeichert.")
            st.experimental_rerun()


def show_admin_mode():
    st.title("Abfahrten – Admin / Disposition")

    locations, departures, screens = load_excel()

    tabs = st.tabs(["Abfahrten", "Einrichtungen", "Screens"])
    with tabs[0]:
        show_admin_departures(locations, departures, screens)
    with tabs[1]:
        show_admin_locations(locations, departures, screens)
    with tabs[2]:
        show_admin_screens(locations, departures, screens)


# --------------------------------------------------
# Haupteinstieg
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
