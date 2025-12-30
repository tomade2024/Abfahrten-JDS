import streamlit as st
import pandas as pd
from datetime import datetime
from pathlib import Path

EXCEL_PATH = Path("daten.xlsx")


@st.cache_data
def load_excel():
    xl = pd.ExcelFile(EXCEL_PATH)
    locations = pd.read_excel(xl, "locations")
    departures = pd.read_excel(xl, "departures")
    screens = pd.read_excel(xl, "screens")
    return locations, departures, screens


def save_excel(locations, departures, screens):
    # Achtung: cache invalidieren, wenn du schreibst
    with pd.ExcelWriter(EXCEL_PATH, engine="openpyxl", mode="w") as writer:
        locations.to_excel(writer, sheet_name="locations", index=False)
        departures.to_excel(writer, sheet_name="departures", index=False)
        screens.to_excel(writer, sheet_name="screens", index=False)
    load_excel.clear()  # cache leeren


def show_display_mode(screen_id: int):
    locations, departures, screens = load_excel()

    if screen_id not in screens["id"].values:
        st.error(f"Screen {screen_id} ist in 'screens'-Tabelle nicht konfiguriert.")
        return

    screen = screens.loc[screens["id"] == screen_id].iloc[0]
    mode = screen["mode"]  # DETAIL oder OVERVIEW
    filter_type = str(screen.get("filter_type", "ALLE"))
    filter_locations = str(screen.get("filter_locations") or "").strip()
    refresh_interval = int(screen.get("refresh_interval_seconds") or 30)

    # Auto-Refresh (in Sekunden)
    st_autorefresh = st.experimental_rerun  # Dummy, für Info
    st.write(f"Aktualisierung alle {refresh_interval} Sekunden (bitte später mit st_autorefresh umsetzen).")

    now = datetime.now()
    departures["datetime"] = pd.to_datetime(departures["datetime"])
    future_dep = departures[departures["datetime"] >= now]

    merged = future_dep.merge(
        locations,
        left_on="location_id",
        right_on="id",
        suffixes=("_dep", "_loc"),
    )

    # Filter nach type
    if filter_type and filter_type != "ALLE":
        merged = merged[merged["type"] == filter_type]

    # Filter nach expliziten locations
    if filter_locations:
        ids = [int(x.strip()) for x in filter_locations.split(",") if x.strip().isdigit()]
        if ids:
            merged = merged[merged["location_id"].isin(ids)]

    merged = merged.sort_values("datetime")

    st.markdown(f"### Anzeige für Screen {screen_id}: {screen['name']}")
    if mode == "DETAIL":
        st.markdown("**Modus: DETAIL**")
        st.dataframe(
            merged[["datetime", "name", "type", "vehicle", "status", "note"]],
            use_container_width=True,
        )
    else:
        st.markdown("**Modus: OVERVIEW**")
        # Einfache Übersicht – z. B. gruppiert nach type
        for t, group in merged.groupby("type"):
            st.markdown(f"#### {t}")
            st.table(group[["datetime", "name", "vehicle", "status", "note"]])


def show_admin_mode():
    st.title("Admin / Disposition")

    locations, departures, screens = load_excel()

    st.subheader("Neue Abfahrt anlegen")
    with st.form("new_departure"):
        col1, col2 = st.columns(2)
        with col1:
            dt = st.text_input("Datum & Uhrzeit (YYYY-MM-DD HH:MM)", "")
            loc = st.selectbox(
                "Einrichtung",
                options=locations["id"],
                format_func=lambda i: locations.loc[locations["id"] == i, "name"].values[0],
            )
        with col2:
            vehicle = st.text_input("Fahrzeug", "")
            note = st.text_input("Hinweis", "")

        submitted = st.form_submit_button("Speichern")
        if submitted:
            try:
                pd.to_datetime(dt)  # Einfache Validierung
            except ValueError:
                st.error("Ungültiges Datum/Uhrzeit-Format.")
            else:
                new_id = (departures["id"].max() or 0) + 1
                new_row = {
                    "id": new_id,
                    "datetime": dt,
                    "location_id": loc,
                    "vehicle": vehicle,
                    "status": "GEPLANT",
                    "note": note,
                }
                departures = pd.concat([departures, pd.DataFrame([new_row])], ignore_index=True)
                save_excel(locations, departures, screens)
                st.success("Abfahrt gespeichert.")

    st.subheader("Alle Abfahrten")
    st.dataframe(
        departures.sort_values("datetime"),
        use_container_width=True,
    )

    st.subheader("Screens-Konfiguration (nur Ansicht)")
    st.dataframe(screens, use_container_width=True)


def main():
    # Query-Parameter auslesen: ?screenId=1&mode=display
    params = st.experimental_get_query_params()
    mode = params.get("mode", ["admin"])[0]  # default: admin
    screen_id_param = params.get("screenId", [None])[0]

    # Für echte Monitore: mode=display&screenId=X in der URL verwenden
    if mode == "display" and screen_id_param is not None:
        try:
            screen_id = int(screen_id_param)
        except ValueError:
            st.error("screenId muss eine Zahl sein.")
        else:
            # Display-Ansicht möglichst „clean“ machen
            st.set_page_config(layout="wide")
            show_display_mode(screen_id)
    else:
        # Admin-Modus
        st.set_page_config(layout="wide")
        show_admin_mode()


if __name__ == "__main__":
    main()
