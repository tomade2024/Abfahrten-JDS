from flask import Flask, request, render_template, redirect, url_for
import pandas as pd
from datetime import datetime
from pathlib import Path

app = Flask(__name__)

EXCEL_PATH = Path("daten.xlsx")


def load_excel():
    """Excel-Datei komplett einlesen."""
    xl = pd.ExcelFile(EXCEL_PATH)
    locations = pd.read_excel(xl, "locations")
    departures = pd.read_excel(xl, "departures")
    screens = pd.read_excel(xl, "screens")
    return locations, departures, screens


def save_excel(locations, departures, screens):
    """Excel-Datei komplett neu schreiben."""
    with pd.ExcelWriter(EXCEL_PATH, engine="openpyxl", mode="w") as writer:
        locations.to_excel(writer, sheet_name="locations", index=False)
        departures.to_excel(writer, sheet_name="departures", index=False)
        screens.to_excel(writer, sheet_name="screens", index=False)
