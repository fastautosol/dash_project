# 2026.10.10 11.00 Flight Radar & Path Tracker
import os
import dash
import dash_bootstrap_components as dbc
import pandas as pd
import plotly.graph_objects as go
from dash import Input, Output, callback, dcc, html
from sqlalchemy import create_engine, text
import logging

dash.register_page(__name__, path="/flight-radar", name="Emirates A380 Radar", icon="fa-plane")

DB_URL = "postgresql://sql_admin:sql_pass@postgresql:5432/n8n"
engine = create_engine(DB_URL, pool_pre_ping=True)

SCHEMA = "bronze"
PATH_HOURS = 24      # ennyi órányi előzményt rajzolunk ki
GAP_MINUTES = 20     # ennél nagyobb időhézagnál megszakítjuk a vonalat (új járat / adathiány)
LIVE_MINUTES = 5     # ennél frissebb pozíciójú gépet tekintünk "élőnek"
REFRESH_SECONDS = 60  # igazodjon a pozíciógyűjtő --loop értékéhez

EMPTY_COLS = ["icao24", "callsign", "latitude", "longitude", "altitude_m", "velocity_mps", "heading_deg", "on_ground", "snapshot_time"]

def load_path_positions(hours: int) -> pd.DataFrame:
    """Történeti pozíciók az útvonalak kirajzolásához."""
    query = text(f"""
        SELECT icao24, callsign, latitude, longitude, altitude_m, velocity_mps, heading_deg, on_ground, snapshot_time
        FROM {SCHEMA}.a380_positions WHERE snapshot_time > now() - make_interval(hours => :h)
        ORDER BY icao24, snapshot_time""")

    try:
        with engine.connect() as conn:
            return pd.read_sql_query(query, conn, params={"h": hours})
    except Exception:
        logging.exception("Hiba az útvonalelőzmények lekérésekor")
        return pd.DataFrame(columns=EMPTY_COLS)


def load_live_positions() -> pd.DataFrame:
    """Aktuális pozíciók a merge-alapú DLT táblából."""
    query = text(f"""
        SELECT icao24, callsign, latitude, longitude, altitude_m, velocity_mps, heading_deg, on_ground, snapshot_at
        FROM {SCHEMA}.opensky_live_positions WHERE snapshot_at >= now() - make_interval(mins => :minutes)""")

    try:
        with engine.connect() as conn:
            df = pd.read_sql_query(query, conn, params={"minutes": LIVE_MINUTES})
        if not df.empty:
            df = df.rename(columns={"snapshot_at": "snapshot_time"})
        return df
        
    except Exception:
        logging.exception("Hiba az élő pozíciók lekérésekor")
        return pd.DataFrame(columns=EMPTY_COLS)



def build_path_lines(df: pd.DataFrame) -> tuple[list, list]:
    """Egyetlen trace-be fűzi az összes útvonalat; a None értékek törik meg a vonalat
    járatok között és időhézagoknál (így nincs 'ugró' egyenes vonal)."""
    lats, lons = [], []
    for _, g in df.groupby("icao24"):
        g = g.sort_values("snapshot_time")
        new_segment = g["snapshot_time"].diff() > pd.Timedelta(minutes=GAP_MINUTES)
        for is_new, lat, lon in zip(new_segment, g["latitude"], g["longitude"]):
            if is_new:
                lats.append(None)
                lons.append(None)
            lats.append(lat)
            lons.append(lon)
        lats.append(None)
        lons.append(None)
    return lats, lons


# ----- Dash Oldal Elrendezés (Layout) -----
layout = dbc.Container([
    dbc.Row([
        dbc.Col([
            html.H3(
                [html.I(className="fas fa-plane-departure me-2 text-warning"), "EMIRATES A380 FLIGHT RADAR"],
                className="text-light mb-2", style={"letterSpacing": "1px"},
            ),
            html.P(
                "Valós idejű óriásgép-követés és útvonalvonal (Flight Path) vizualizáció",
                className="text-muted small mb-1",
            ),
            html.Small(id="radar-status", className="text-muted d-block mb-3"),
        ], width=12)
    ]),

    dbc.Row([
        dbc.Col([
            dbc.Card([
                dbc.CardBody([
                    dcc.Graph(id="live-flight-radar", style={"height": "75vh"}),
                    dcc.Interval(id="radar-update-clock", interval=REFRESH_SECONDS * 1000, n_intervals=0),
                ])
            ], style={"background": "rgba(0,0,0,0.4)", "borderRadius": "15px",
                      "border": "1px solid rgba(255,255,255,0.1)"})
        ], width=12)
    ])
], fluid=True)


# ----- Reaktív Térkép Frissítő Logika (Callback) -----
@callback(
    Output("live-flight-radar", "figure"),
    Output("radar-status", "children"),
    Input("radar-update-clock", "n_intervals"),
)
def update_radar_map(_):
    fig = go.Figure()
    status = "Nincs adat – fut a pozíciógyűjtő? (python opensky_api.py positions --loop 120)"

    df = load_path_positions(PATH_HOURS)
    live_df = load_live_positions()

    if not df.empty:
        # --- 1. RÉTEG: Útvonalak ---
        lats, lons = build_path_lines(df)
        fig.add_trace(go.Scattermap(
            lat=lats, lon=lons, mode="lines",
            line=dict(width=2, color="rgba(255, 193, 7, 0.5)"),
            hoverinfo="skip", name="Útvonalak"))

        # --- 2. RÉTEG: Élő pozíciók (gépenként a legutolsó, ha elég friss és nem áll a földön) ---
        latest = df.sort_values("snapshot_time").groupby("icao24").tail(1)
        fresh = latest["snapshot_time"] >= pd.Timestamp.now(tz="UTC") - pd.Timedelta(minutes=LIVE_MINUTES)
        airborne = ~latest["on_ground"].fillna(False).astype(bool)
        live = latest[fresh & airborne]

        if not live.empty:
            hover = [
                f"<b>Járat:</b> {r.callsign or r.icao24}<br>"
                f"<b>Magasság:</b> {r.altitude_m:.0f} m<br>"
                f"<b>Irányszög:</b> {r.heading_deg:.0f}°<br>"
                f"<b>Sebesség:</b> {r.velocity_mps * 3.6:.0f} km/h"
                for r in live.itertuples()
            ]
            # Kör marker: a 'symbol="airport"' ikon csak akkor jelenik meg, ha a térképstílus
            # sprite-ot tartalmaz, ezért a körös megoldás megbízhatóbb.
            fig.add_trace(go.Scattermap(
                lat=live["latitude"], lon=live["longitude"],
                mode="markers+text",
                marker=dict(size=12, color="#FFC107"),
                text=live["callsign"].fillna(live["icao24"]),
                textposition="top right",
                textfont=dict(color="white", size=10),
                hovertext=hover, hoverinfo="text", name="Élő Járatok"))

        status = (
            f"{len(live)} gép a levegőben · {df['icao24'].nunique()} gép az elmúlt {PATH_HOURS} órában · "
            f"utolsó adat: {df['snapshot_time'].max():%H:%M:%S} UTC")

    # --- 3. TÉRKÉP STÍLUS ÉS ELRENDEZÉS ---
    fig.update_layout(
        margin={"r": 0, "t": 0, "l": 0, "b": 0}, showlegend=False, paper_bgcolor="rgba(0,0,0,0)",
        uirevision="radar",  # frissítéskor megtartja a felhasználó zoom/pan állását
        map=dict(style="carto-darkmatter", center=dict(lat=25.2048, lon=55.2708), zoom=3))
    return fig, status
