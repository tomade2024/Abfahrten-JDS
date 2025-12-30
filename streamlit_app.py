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
@app.route("/display")
def display():
    screen_id = request.args.get("screenId", type=int)
    if not screen_id:
        return "screenId fehlt", 400

    locations, departures, screens = load_excel()

    # Screen-Konfiguration holen
    screen = screens.loc[screens["id"] == screen_id]
    if screen.empty:
        return f"Screen {screen_id} nicht konfiguriert", 404

    screen = screen.iloc[0]
    mode = screen["mode"]  # DETAIL oder OVERVIEW
    filter_type = screen.get("filter_type", "ALLE")
    filter_locations = str(screen.get("filter_locations") or "").strip()
    refresh_interval = int(screen.get("refresh_interval_seconds") or 30)

    # Nur zukünftige/aktuelle Abfahrten (z. B. ab jetzt - 10 min)
    now = datetime.now()
    departures["datetime"] = pd.to_datetime(departures["datetime"])
    future_dep = departures[departures["datetime"] >= (now)]

    # Join mit locations
    merged = future_dep.merge(locations, left_on="location_id", right_on="id", suffixes=("_dep", "_loc"))

    # Filter nach type (KRANKENHAUS/ALTENHEIM/MVZ)
    if filter_type and filter_type != "ALLE":
        merged = merged[merged["type"] == filter_type]

    # Filter nach expliziten locations
    if filter_locations:
        ids = [int(x.strip()) for x in filter_locations.split(",") if x.strip().isdigit()]
        if ids:
            merged = merged[merged["location_id"].isin(ids)]

    # Sortieren nach Zeit
    merged = merged.sort_values("datetime")

    if mode == "DETAIL":
        # einfache Liste
        data = merged.to_dict(orient="records")
        return render_template("display_detail.html",
                               screen=screen,
                               departures=data,
                               refresh_interval=refresh_interval)
    else:
        # "OVERVIEW" – du kannst hier gruppieren, z. B. nach type
        data = merged.to_dict(orient="records")
        return render_template("display_overview.html",
                               screen=screen,
                               departures=data,
                               refresh_interval=refresh_interval)
@app.route("/admin", methods=["GET", "POST"])
def admin():
    locations, departures, screens = load_excel()

    if request.method == "POST":
        # neue Abfahrt anlegen
        # (hier sehr einfach, ohne Validierung)
        new_id = (departures["id"].max() or 0) + 1
        dt_str = request.form["datetime"]  # "2025-01-10 13:45"
        loc_id = int(request.form["location_id"])
        vehicle = request.form.get("vehicle", "")
        note = request.form.get("note", "")

        new_row = {
            "id": new_id,
            "datetime": dt_str,
            "location_id": loc_id,
            "vehicle": vehicle,
            "status": "GEPLANT",
            "note": note,
        }

        departures = pd.concat([departures, pd.DataFrame([new_row])], ignore_index=True)
        save_excel(locations, departures, screens)
        return redirect(url_for("admin"))

    # GET: Admin-Oberfläche rendern
    departures_sorted = departures.sort_values("datetime")
    loc_list = locations.to_dict(orient="records")
    dep_list = departures_sorted.to_dict(orient="records")
    return render_template("admin.html", locations=loc_list, departures=dep_list)
