# 2026.10.10 - OpenSky Flight Radar
import logging
import dash
import dash_bootstrap_components as dbc
import pandas as pd
import plotly.graph_objects as go
from dash import Input, Output, callback, dcc, html
from sqlalchemy import create_engine, text

log = logging.getLogger(__name__)

dash.register_page(__name__, path="/flight-radar", name="OpenSky Radar", icon="fa-plane")

DB_URL = "postgresql://sql_admin:sql_pass@postgresql:5432/n8n"
engine = create_engine(DB_URL, pool_pre_ping=True)

SCHEMA = "bronze"

# One table feeds both layers. Every call of /live-positions appends a snapshot:
# all aircraft of that call share the same snapshot_at (key: icao24 + snapshot_at).
#   - history of snapshots  -> trail (PATH)
#   - the newest snapshot   -> markers (LIVE)
POSITIONS_TABLE = "opensky_live_positions"

PATH_HOURS = 48          # how far back the trail goes (with ~5 snapshots/day: ~10 points)
GAP_MINUTES = 720        # snapshots are hours apart -> only break the line after 12 h
REFRESH_SECONDS = 300    # the page only re-reads the DB; data changes a few times a day

EMPTY_COLS = ["icao24", "callsign", "latitude", "longitude", "altitude_m", "velocity_mps", "heading_deg", "on_ground", "snapshot_time"]

_COLUMNS = """icao24, callsign, latitude, longitude, altitude_m, velocity_mps, heading_deg, on_ground, snapshot_at AS snapshot_time"""


def _read(query, params: dict, what: str) -> pd.DataFrame:
    try:
        with engine.connect() as conn:
            df = pd.read_sql_query(query, conn, params=params)
        if not df.empty:
            df["snapshot_time"] = pd.to_datetime(df["snapshot_time"], utc=True)
        return df
    except Exception as e:
        log.warning("%s nem érhető el (%s.%s): %s", what, SCHEMA, POSITIONS_TABLE, str(e).splitlines()[0])
        return pd.DataFrame(columns=EMPTY_COLS)


def load_path_positions(hours: int) -> pd.DataFrame:
    """History: ALL snapshots of the last `hours` hours (many rows per aircraft) -> trail."""
    query = text(f"""
        SELECT {_COLUMNS}
        FROM {SCHEMA}.{POSITIONS_TABLE}
        WHERE snapshot_at > now() - make_interval(hours => :h)
        ORDER BY icao24, snapshot_at""")
    return _read(query, {"h": hours}, "Útvonalelőzmények")


def load_live_positions() -> pd.DataFrame:
    """Latest: only the rows of the NEWEST snapshot (one row per aircraft) -> markers."""
    query = text(f"""
        SELECT {_COLUMNS}
        FROM {SCHEMA}.{POSITIONS_TABLE}
        WHERE snapshot_at = (SELECT max(snapshot_at) FROM {SCHEMA}.{POSITIONS_TABLE})""")
    return _read(query, {}, "Aktuális pozíciók")


def build_path_lines(df: pd.DataFrame) -> tuple[list, list]:
    """Egyetlen trace-be fűzi az összes útvonalat; a None értékek törik meg a vonalat
    gépek között és nagy időhézagnál."""
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
                [html.I(className="fas fa-plane-departure me-2 text-warning"), "OPENSKY FLIGHT RADAR"],
                className="text-light mb-2", style={"letterSpacing": "1px"},
            ),
            html.P(
                "Utolsó pillanatfelvétel pozíciói és a korábbi felvételek nyomvonala",
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

    path_df = load_path_positions(PATH_HOURS)   # history -> trail
    live_df = load_live_positions()             # newest snapshot -> markers

    # --- 1. RÉTEG: Útvonalak (a korábbi pillanatfelvételekből) ---
    if not path_df.empty:
        lats, lons = build_path_lines(path_df)
        fig.add_trace(go.Scattermap(
            lat=lats, lon=lons, mode="lines+markers",
            line=dict(width=1.5, color="rgba(255, 193, 7, 0.45)"),
            marker=dict(size=4, color="rgba(255, 193, 7, 0.6)"),
            hoverinfo="skip", name="Útvonalak"))

    # --- 2. RÉTEG: Utolsó pillanatfelvétel (csak a levegőben lévő gépek) ---
    live = live_df.iloc[0:0]
    if not live_df.empty:
        airborne = ~live_df["on_ground"].fillna(False).astype(bool)
        live = live_df[airborne].copy()

    if not live.empty:
        live = live.assign(
            callsign=live["callsign"].fillna(live["icao24"]),
            altitude_m=live["altitude_m"].fillna(0),
            heading_deg=live["heading_deg"].fillna(0),
            velocity_mps=live["velocity_mps"].fillna(0),
        )
        hover = [
            f"<b>Járat:</b> {r.callsign}<br>"
            f"<b>Magasság:</b> {r.altitude_m:.0f} m<br>"
            f"<b>Irányszög:</b> {r.heading_deg:.0f}°<br>"
            f"<b>Sebesség:</b> {r.velocity_mps * 3.6:.0f} km/h"
            for r in live.itertuples()]

        fig.add_trace(go.Scattermap(
            lat=live["latitude"], lon=live["longitude"],
            mode="markers+text",
            marker=dict(size=12, color="#FFC107"),
            text=live["callsign"],
            textposition="top right",
            textfont=dict(color="white", size=10),
            hovertext=hover, hoverinfo="text", name="Utolsó felvétel"))

    # --- Státusz: mikor készült az utolsó pillanatfelvétel ---
    if live_df.empty:
        status = f"Nincs adat a {SCHEMA}.{POSITIONS_TABLE} táblában – hívd meg a /live-positions végpontot."
    else:
        snap = live_df["snapshot_time"].max()
        age_min = int((pd.Timestamp.now(tz="UTC") - snap).total_seconds() // 60)
        path_n = path_df["icao24"].nunique() if not path_df.empty else 0
        status = (f"{len(live)} gép a levegőben · utolsó felvétel: {snap:%Y-%m-%d %H:%M} UTC "
                  f"({age_min} perccel ezelőtt) · {path_n} gép előzménye az elmúlt {PATH_HOURS} órából")

    # --- 3. TÉRKÉP STÍLUS ÉS ELRENDEZÉS ---
    fig.update_layout(
        margin={"r": 0, "t": 0, "l": 0, "b": 0}, showlegend=False, paper_bgcolor="rgba(0,0,0,0)",
        uirevision="radar",  # frissítéskor megtartja a felhasználó zoom/pan állását
        map=dict(style="carto-darkmatter", center=dict(lat=25.2048, lon=55.2708), zoom=3))
    return fig, status
