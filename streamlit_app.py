import streamlit as st
import pandas as pd
from datetime import datetime
from pathlib import Path

EXCEL_PATH = Path("daten.xlsx")


@st.cache_data
def load_excel():
    # Wenn die Datei noch nicht existiert: Standard-Struktur anlegen
    if not EXCEL_PATH.exists():
        # Leere DataFrames mit den richtigen Spalten anlegen
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
        # Standard-Screens 1–6 anlegen
        for i in range(1, 7):
            screens.loc[len(screens)] = [
                i,
                f"Screen {i}",
                "DETAIL" if i <= 4 else "OVERVIEW",
                "ALLE",
                "",
                30,
            ]

        # Datei schreiben
        with pd.ExcelWriter(EXCEL_PATH, engine="openpyxl", mode="w") as writer:
            locations.to_excel(writer, sheet_name="locations", index=False)
            departures.to_excel(writer, sheet_name="departures", index=False)
            screens.to_excel(writer, sheet_name="screens", index=False)

        return locations, departures, screens

    # Wenn die Datei existiert: normal laden
    xl = pd.ExcelFile(EXCEL_PATH)
    locations = pd.read_excel(xl, "locations")
    departures = pd.read_excel(xl, "departures")
    screens = pd.read_excel(xl, "screens")
    return locations, departures, screens


def save_excel(locations, departures, screens):
    with pd.ExcelWriter(EXCEL_PATH, engine="openpyxl", mode="w") as writer:
        locations.to_excel(writer, sheet_name="locations", index=False)
        departures.to_excel(writer, sheet_name="departures", index=False)
        screens.to_excel(writer, sheet_name="screens", index=False)
    # Cache leeren, damit beim nächsten load_excel neu gelesen wird
    load_excel.clear()
