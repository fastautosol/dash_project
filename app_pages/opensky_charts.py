# 2026.10.08 - Emirates A380 Flight Radar & Path Tracker
import dash
from dash import html, dcc, callback, Input, Output
import dash_bootstrap_components as dbc
import plotly.graph_objects as go
import pandas as pd
import psycopg2

dash.register_page(__name__, path="/flight-radar", name="Emirates A380 Radar", icon="fa-plane")

DB_PARAMS = "dbname=sales_data user=postgres password=secret host=localhost"

def get_historical_flight_paths() -> pd.DataFrame:
    """Kiolvassa a dlt által mentett korábbi pozíciókat a PostgreSQL-ből, 
    hogy felépíthessük a repülési útvonalakat (Flight Paths)."""
    try:
        conn = psycopg2.connect(DB_PARAMS)
        # Lekérjük az elmúlt órákban mentett koordinátákat időrendben járatonként csoportosítva
        query = """
            SELECT callsign, latitude, longitude, snapshot_time 
            FROM sky_monitor.scheduled_a380_flights 
            WHERE latitude IS NOT NULL AND longitude IS NOT NULL
            ORDER BY callsign, snapshot_time ASC;
        """
        df = pd.read_sql_query(query, conn)
        conn.close()
        return df
    except Exception as e:
        print(f"Hiba az útvonal-előzmények lekérésekor: {e}")
        return pd.DataFrame(columns=["callsign", "latitude", "longitude", "snapshot_time"])

# ----- Dash Oldal Elrendezés (Layout) -----
layout = dbc.Container([
    dbc.Row([
        dbc.Col([
            html.H3([html.I(className="fas fa-plane-departure me-2 text-warning"), "EMIRATES A380 FLIGHT RADAR"], 
                    className="text-light mb-2", style={"letterSpacing": "1px"}),
            html.P("Valós idejű óriásgép-követés és útvonalvonal (Flight Path) vizualizáció", className="text-muted small mb-4")
        ], width=12)
    ]),
    
    dbc.Row([
        dbc.Col([
            dbc.Card([
                dbc.CardBody([
                    # A térkép grafikon
                    dcc.Graph(id="live-flight-radar", style={"height": "75vh"}),
                    
                    # 30 másodperces automatikus frissítési időzítő (Interval)
                    dcc.Interval(
                        id="radar-update-clock",
                        interval=30 * 1000,  # 30.000 ezredmásodperc = 30 mp
                        n_intervals=0
                    )
                ])
            ], style={"background": "rgba(0,0,0,0.4)", "borderRadius": "15px", "border": "1px solid rgba(255,255,255,0.1)"})
        ], width=12)
    ])
], fluid=True)


# ----- Reaktív Térkép Frissítő Logika (Callback) -----
@callback(
    Output("live-flight-radar", "figure"),
    Input("radar-update-clock", "n_intervals")
)
def update_radar_map(n):
    fig = go.Figure()

    # --- 1. RÉTEG: A Történeti Útvonalak (Flight Paths) Kirajzolása ---
    df_paths = get_historical_flight_paths()
    
    if not df_paths.empty:
        # Minden egyedi járathoz rajzolunk egy külön vonalat
        for callsign, group in df_paths.groupby("callsign"):
            fig.add_trace(go.Scattermap(
                lat=group["latitude"],
                lon=group["longitude"],
                mode="lines",
                line=dict(width=2, color="rgba(255, 193, 7, 0.5)"), # Áttetsző sárga vonal az útvonalnak
                name=f"{callsign} Path",
                hoverinfo="skip" # A vonal maga ne legyen kattintható, csak a repülő
            ))

    # --- 2. RÉTEG: Az Aktuális Élő Pozíciók (Sárga Repülők) ---
    # Meghívjuk a belső FastAPI végpontodat, amit az előző lépésben írtunk meg
    import requests
    try:
        response = requests.get("http://localhost:8000/api/flights/live-positions", timeout=5)
        live_data = response.json().get("flights", [])
    except Exception as e:
        print(f"Nem sikerült elérni az élő pozíciók API-t: {e}")
        live_data = []

    if live_data:
        lats = [f["latitude"] for f in live_data]
        lons = [f["longitude"] for f in live_data]
        hover_texts = [
            f"✈️ <b>Járat:</b> {f['callsign']}<br>"
            f"📈 <b>Magasság:</b> {f['altitude_m']:.0f} m<br>"
            f"🧭 <b>Irányszög:</b> {f['heading_deg']}°<br>"
            f"🚀 <b>Sebesség:</b> {f['velocity_mps']*3.6:.0f} km/h"
            for f in live_data
        ]

        # Felrakjuk az aktuális pozíciókat sárga repülő szimbólummal
        fig.add_trace(go.Scattermap(
            lat=lats,
            lon=lons,
            mode="markers+text",
            marker=dict(
                size=14,
                color="#FFC107", # FlightRadar sárga
                symbol="airport" # A Plotly beépített repülőtér/repülő ikonja
            ),
            text=[f["callsign"] for f in live_data],
            textposition="top right",
            textfont=dict(color="white", size=10),
            hovertext=hover_texts,
            hoverinfo="text",
            name="Élő Járatok"
        ))

    # --- 3. TÉRKÉP STÍLUS ÉS ELRENDEZÉS (Sötét mód konfiguráció) ---
    fig.update_layout(
        margin={"r": 0, "t": 0, "l": 0, "b": 0},
        showlegend=False,
        map=dict(
            style="dark", # Mapbox token nélküli beépített Plotly sötét térkép stílus
            center=dict(lat=25.2048, lon=55.2708), # Dubai (OMDB) koordinátáira centerezve indításkor
            zoom=3
        )
    )

    return fig
