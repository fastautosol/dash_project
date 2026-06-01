# 2026.05.22  10.00
# https://www.whaletrades.io/data-hub#coin-sentiment
import pandas as pd
from datetime import datetime
from fastapi import APIRouter
import dash
from dash import dcc, html, dash_table, callback
import dash_bootstrap_components as dbc
from dash.dependencies import Input, Output
import plotly.express as px
import plotly.graph_objects as go
from apis.bybit_api import sql_engine

# ----- 3. THE FRONTEND (Dash Sidebar uses this) -----
dash.register_page(__name__, icon="fa-solid fa-coins", name="Bybit Cryptos", order=1)

# Glassmorphism Card Style
CARD_STYLE = {
    "background": "rgba(255, 255, 255, 0.03)",
    "backdrop-filter": "blur(10px)",
    "border-radius": "15px",
    "border": "1px solid rgba(255, 255, 255, 0.1)",
    "padding": "20px"
}

layout = dbc.Container([
    html.Div([
        html.H2("ByBit Crypto Market", className="text-light fw-bold mb-0"),
    ], className="mb-4"),

    dcc.Interval(id='refresh', interval=60*1000), 

    dbc.Row(id='crypto-ccharts', className="g-3 mb-3"),

    html.Div([
        html.H5("Execution Logs", className="text-light mb-3"),
        html.Div(id='crypto-table', 
            style={"height": "300px", "overflowY": "auto", "overflowX": "hidden", "backgroundColor": "transparent",  "fontSize": "12px"})
    ], style=CARD_STYLE)

], fluid=True)

@callback(
    [Output('crypto-ccharts', 'children'), 
     Output('crypto-table', 'children')],
    [Input('refresh', 'n_intervals')]
)

def update_dashboard(n_intervals):

    with sql_engine.connect() as conn:
        df = pd.read_sql("SELECT symbol, timestamp, open, high, low, close, turnover24h FROM bybit_data.bybit_candles ORDER BY timestamp DESC LIMIT 2500", conn)

    if df.empty:
        return [html.Div("No data found", className="text-light fst-italic"),  None]

    #df["timestamp"] = pd.to_datetime(df["timestamp"])
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True).dt.tz_convert("Europe/Budapest")

    crypto_ccharts = []
    for symbol in df.sort_values("turnover24h", ascending=False)["symbol"].unique()[:16]:
        chart_df = df[df["symbol"] == symbol].sort_values("timestamp")
        
        if chart_df.empty: continue

        fig = go.Figure()

        fig.add_trace(go.Candlestick(
            x=chart_df["timestamp"].dt.strftime("%H:%M"),
            open=chart_df["open"],
            high=chart_df["high"],
            low=chart_df["low"],
            close=chart_df["close"],
            increasing_line_color="#00ff9c",
            decreasing_line_color="#ff4976"
        ))

        fig.update_layout(
            template="plotly_dark",
            paper_bgcolor='rgba(0,0,0,0)',
            plot_bgcolor='rgba(0,0,0,0)',
            margin=dict(l=0, r=0, t=0, b=0), height=220,
            xaxis=dict(showgrid=False, title=""),
            yaxis=dict(showgrid=True, gridcolor='rgba(255,255,255,0.05)', side="right"),
            xaxis_rangeslider_visible=False, showlegend=False
        )
        
        crypto_ccharts.append(
            dbc.Col([
                html.Div([
                    html.H6(symbol, className="text-info mb-1"),
                    dcc.Graph(figure=fig, config={'displayModeBar': False})
                ], style=CARD_STYLE)
            ], width=3, className="mb-1")
        )
  
    # Crypto Table
    display_df = df.copy()
    #display_df.columns = [c.replace('_', ' ').upper() for c in display_df.columns]
    table = dbc.Table.from_dataframe(display_df[:100], striped=False, hover=True, responsive=True, borderless=True, className="text-light m-0", 
        style={"backgroundColor": "transparent",  "--bs-table-bg": "transparent", "--bs-table-accent-bg": "transparent", "color": "white"}
    )

    return crypto_ccharts, table 
