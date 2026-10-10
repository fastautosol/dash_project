# 2026.10.10 - OpenSky Flight Radar: departure-airport filter, route curves, aircraft type in the hint box
import logging
import time

import dash
import dash_bootstrap_components as dbc
import numpy as np
import pandas as pd
import plotly.graph_objects as go
from dash import Input, Output, callback, dcc, html
from sqlalchemy import create_engine, text

log = logging.getLogger(__name__)

dash.register_page(__name__, path="/flight-radar", name="OpenSky Radar", icon="fa-plane")

DB_URL = "postgresql://sql_admin:sql_pass@postgresql:5432/n8n"
engine = create_engine(DB_URL, pool_pre_ping=True)

SCHEMA = "bronze"

# Every call of /live-positions appends a snapshot; all aircraft of one call share snapshot_at.
# The route (origin_*/dest_*) and the aircraft type columns are filled by opensky_api.py.
POSITIONS_TABLE = "opensky_live_positions"

# Departure filter chips. Keep in sync with AIRPORTS in opensky_api.py (ICAO code -> city label).
DEPARTURE_AIRPORTS = {"OMDB": "Dubai", "OMAA": "Abu Dhabi", "EDDF": "Frankfurt", "VHHH": "Hong Kong", 
                      "YSSY": "Sydney", "KLAX": "Los Angeles", "EHAM": "Amsterdam", "LHBP": "Budapest"}

REFRESH_SECONDS = 300    # the page only re-reads the DB; data changes a few times a day
CACHE_SECONDS = 60       # filter clicks must not hit the DB every time
ROUTE_POINTS = 40        # points per great-circle segment

ROUTE_COLS = ["origin_icao", "origin_lat", "origin_lon", "dest_icao", "dest_lat", "dest_lon"]
TYPE_COLS = ["aircraft_icao_type", "aircraft_type"]
EMPTY_COLS = ["icao24", "callsign", "latitude", "longitude", "altitude_m", "velocity_mps", "heading_deg", "on_ground", "snapshot_time"] + ROUTE_COLS + TYPE_COLS
NUMERIC_COLS = ["latitude", "longitude", "altitude_m", "velocity_mps", "heading_deg", "origin_lat", "origin_lon", "dest_lat", "dest_lon"]

_cache = {"ts": 0.0, "df": None}

def _query_latest_positions() -> pd.DataFrame:
    """Rows of the NEWEST snapshot (one row per aircraft), including route and type columns."""
    query = text(f"""
        SELECT *
        FROM {SCHEMA}.{POSITIONS_TABLE}
        WHERE snapshot_at = (SELECT max(snapshot_at) FROM {SCHEMA}.{POSITIONS_TABLE})""")
    try:
        with engine.connect() as conn:
            df = pd.read_sql_query(query, conn)
    except Exception as e:
        log.warning("Aktuális pozíciók nem érhetők el (%s.%s): %s", SCHEMA, POSITIONS_TABLE, str(e).splitlines()[0])
        return pd.DataFrame(columns=EMPTY_COLS)

    df = df.rename(columns={"snapshot_at": "snapshot_time"})
    for col in EMPTY_COLS:                       # new columns do not exist before the first enriched run
        if col not in df.columns:
            df[col] = None
    for col in NUMERIC_COLS:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df["snapshot_time"] = pd.to_datetime(df["snapshot_time"], utc=True)
    return df


def load_live_positions() -> pd.DataFrame:
    """Same as _query_latest_positions, but cached for CACHE_SECONDS (empty results are not cached)."""
    now = time.time()
    if _cache["df"] is not None and now - _cache["ts"] < CACHE_SECONDS:
        return _cache["df"].copy()
    df = _query_latest_positions()
    if not df.empty:
        _cache.update(ts=now, df=df)
    return df.copy()


def great_circle(lat1, lon1, lat2, lon2, n=ROUTE_POINTS, anchor_end=False):
    """n points along the great circle (the curved 'straight line' on a globe).
    Longitudes are unwrapped (continuous across +-180 deg) and shifted so the anchored end
    keeps its original longitude: the aircraft marker and the line stay on the same map copy."""
    def xyz(lat, lon):
        lat, lon = np.radians(lat), np.radians(lon)
        return np.array([np.cos(lat) * np.cos(lon), np.cos(lat) * np.sin(lon), np.sin(lat)])

    a, b = xyz(lat1, lon1), xyz(lat2, lon2)
    omega = float(np.arccos(np.clip(np.dot(a, b), -1.0, 1.0)))
    if omega < 1e-6 or omega > np.pi - 1e-6:     # same point (or antipodal): plain segment
        return [lat1, lat2], [lon1, lon2]

    t = np.linspace(0.0, 1.0, n)
    pts = (np.sin((1 - t) * omega)[:, None] * a + np.sin(t * omega)[:, None] * b) / np.sin(omega)
    lats = np.degrees(np.arctan2(pts[:, 2], np.hypot(pts[:, 0], pts[:, 1])))
    lons = np.degrees(np.unwrap(np.arctan2(pts[:, 1], pts[:, 0])))

    ref_idx, ref_lon = (-1, lon2) if anchor_end else (0, lon1)
    lons = lons + 360.0 * round((ref_lon - lons[ref_idx]) / 360.0)
    return lats.tolist(), lons.tolist()


def build_route_lines(flights: pd.DataFrame):
    """Per aircraft with a known route: origin -> aircraft (flown part) and aircraft -> destination
    (remaining part), both great circles through the real position. None separates the aircraft."""
    flown_lat, flown_lon, rest_lat, rest_lon = [], [], [], []
    airports: dict[str, tuple[float, float]] = {}
    drawn = 0

    for r in flights.itertuples():
        coords = (r.origin_lat, r.origin_lon, r.dest_lat, r.dest_lon, r.latitude, r.longitude)
        if any(pd.isna(c) for c in coords):
            continue

        la, lo = great_circle(r.origin_lat, r.origin_lon, r.latitude, r.longitude, anchor_end=True)
        flown_lat += la + [None]
        flown_lon += lo + [None]

        la, lo = great_circle(r.latitude, r.longitude, r.dest_lat, r.dest_lon)
        rest_lat += la + [None]
        rest_lon += lo + [None]

        if pd.notna(r.origin_icao):
            airports[r.origin_icao] = (r.origin_lat, r.origin_lon)
        if pd.notna(r.dest_icao):
            airports[r.dest_icao] = (r.dest_lat, r.dest_lon)
        drawn += 1

    return flown_lat, flown_lon, rest_lat, rest_lon, airports, drawn


def type_label(model, code) -> str:
    """'A380-800 (A388)' style text for the hint box; '–' when the type is unknown."""
    model = None if pd.isna(model) else str(model)
    code = None if pd.isna(code) else str(code)
    if model and code and code not in model:
        return f"{model} ({code})"
    return model or code or "–"


# ----- Dash Oldal Elrendezés (Layout) -----
layout = dbc.Container([
    dbc.Row([
        dbc.Col([
            html.H3(
                [html.I(className="fas fa-plane-departure me-2 text-warning"), "OPENSKY FLIGHT RADAR"],
                className="text-light mb-2", style={"letterSpacing": "1px"}),
            html.P("Indulási repülőtér szerint szűrhető útvonalak, rajtuk a gép utolsó ismert pozíciója", className="text-muted small mb-1"),
            html.Small(id="radar-status", className="text-muted d-block mb-3"),
        ], width=12)
    ]),

    # --- Indulási repülőtér szűrő (jelölők) ---
    dbc.Row([
        dbc.Col([
            html.Div([
                dbc.Checklist(
                    id="departure-filter",
                    options=[{"label": f"{icao} {city}", "value": icao} for icao, city in DEPARTURE_AIRPORTS.items()],
                    value=[], class_name="btn-group flex-wrap",
                    input_class_name="btn-check",
                    label_class_name="btn btn-outline-warning btn-sm",
                    label_checked_class_name="active"),
                    ], className="mb-3"),
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
    Input("departure-filter", "value"),
)
def update_radar_map(_, departures):
    fig = go.Figure()
    live_df = load_live_positions()              # newest snapshot -> aircraft, routes, types
    selected = [d for d in (departures or []) if d in DEPARTURE_AIRPORTS]

    # csak a levegőben lévő gépek
    live = live_df.iloc[0:0]
    if not live_df.empty:
        airborne = ~live_df["on_ground"].fillna(False).astype(bool)
        live = live_df[airborne].copy()

    # Nincs kijelölt reptér: minden gép pontként, útvonal nélkül.
    # Van kijelölt reptér: csak az onnan induló (ismert útvonalú) gépek, útvonallal.
    shown = live[live["origin_icao"].isin(selected)] if selected else live

    routes_drawn = 0
    if not shown.empty:
        if selected:
            # --- 1. RÉTEG: útvonalgörbék (megtett rész erősebb, hátralévő rész halványabb) ---
            flown_lat, flown_lon, rest_lat, rest_lon, airports, routes_drawn = build_route_lines(shown)

            if routes_drawn:
                fig.add_trace(go.Scattermap(
                    lat=rest_lat, lon=rest_lon, mode="lines",
                    line=dict(width=1.5, color="rgba(255, 193, 7, 0.25)"),
                    hoverinfo="skip", name="Hátralévő útvonal"))
                fig.add_trace(go.Scattermap(
                    lat=flown_lat, lon=flown_lon, mode="lines",
                    line=dict(width=2, color="rgba(255, 193, 7, 0.7)"),
                    hoverinfo="skip", name="Megtett útvonal"))

                # --- 2. RÉTEG: reptér pontok ---
                fig.add_trace(go.Scattermap(
                    lat=[v[0] for v in airports.values()], lon=[v[1] for v in airports.values()],
                    mode="markers+text",
                    marker=dict(size=6, color="rgba(255, 255, 255, 0.85)"),
                    text=list(airports.keys()),
                    textposition="bottom center",
                    textfont=dict(color="rgba(255, 255, 255, 0.7)", size=9),
                    hoverinfo="text", hovertext=list(airports.keys()), name="Repterek"))

        # --- 3. RÉTEG: az aktuális gép pozíciója ---
        shown = shown.assign(
            callsign=shown["callsign"].fillna(shown["icao24"]),
            altitude_m=shown["altitude_m"].fillna(0),
            heading_deg=shown["heading_deg"].fillna(0),
            velocity_mps=shown["velocity_mps"].fillna(0),
        )
        hover = [
            (f"<b>Útvonal:</b> {r.origin_icao} → {r.dest_icao}<br>"
             if pd.notna(r.origin_icao) and pd.notna(r.dest_icao) else "")
            + f"<b>Járat:</b> {r.callsign}<br>"
              f"<b>Típus:</b> {type_label(r.aircraft_type, r.aircraft_icao_type)}<br>"
              f"<b>Magasság:</b> {r.altitude_m:.0f} m<br>"
              f"<b>Irányszög:</b> {r.heading_deg:.0f}°<br>"
              f"<b>Sebesség:</b> {r.velocity_mps * 3.6:.0f} km/h"
            for r in shown.itertuples()]

        fig.add_trace(go.Scattermap(
            lat=shown["latitude"], lon=shown["longitude"],
            mode="markers+text",
            marker=dict(size=12, color="#FFC107"),
            text=shown["callsign"],
            textposition="top right",
            textfont=dict(color="white", size=10),
            hovertext=hover, hoverinfo="text", name="Aktuális pozíció"))

    # --- Státusz ---
    if live_df.empty:
        status = f"Nincs adat a {SCHEMA}.{POSITIONS_TABLE} táblában – hívd meg a /live-positions végpontot."
    else:
        snap = live_df["snapshot_time"].max()
        age_min = int((pd.Timestamp.now(tz="UTC") - snap).total_seconds() // 60)
        if selected:
            filter_txt = f"indulás: {', '.join(selected)} · {routes_drawn} útvonal"
        else:
            filter_txt = "válassz indulási repteret az útvonalak megjelenítéséhez"
        status = (f"{len(shown)} / {len(live)} gép a levegőben · {filter_txt} · "
                  f"utolsó felvétel: {snap:%Y-%m-%d %H:%M} UTC ({age_min} perccel ezelőtt)")

    # --- TÉRKÉP STÍLUS ÉS ELRENDEZÉS ---
    fig.update_layout(
        margin={"r": 0, "t": 0, "l": 0, "b": 0}, showlegend=False, paper_bgcolor="rgba(0,0,0,0)",
        uirevision="radar",  # frissítéskor / szűréskor megtartja a felhasználó zoom/pan állását
        map=dict(style="carto-darkmatter", center=dict(lat=25.2048, lon=55.2708), zoom=3))
    return fig, status
