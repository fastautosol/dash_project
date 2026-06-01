# 2026.05.16  18.00
import pandas as pd
from datetime import datetime
from fastapi import APIRouter
import dash
from dash import dcc, html, dash_table, callback
import dash_bootstrap_components as dbc
from dash.dependencies import Input, Output
import plotly.express as px
from sqlalchemy import create_engine

DB_CONFIG = "postgresql+psycopg://sql_admin:sql_pass@postgresql:5432/n8n"
sql_engine = create_engine(DB_CONFIG, pool_size=5, max_overflow=10)

# ----- 3. THE FRONTEND (Dash Sidebar uses this) -----
dash.register_page(__name__, icon="fa-solid fa-coins", name="MEXC Stocks", order=7)

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
        html.H2("MEXC xStocks", className="text-light fw-bold mb-0")], className="mb-4"),
            dcc.Interval(id='refresh', interval=60*1000), 
            dcc.Dropdown(id="chart-count", options=[{"label": str(i), "value": i} for i in [8,12,16,20,24,28,32]],
                value=12, clearable=False, searchable=False, style={"width": "120px"}),
            html.Br(),   
            dbc.Row(id='xstocks-charts', className="g-3 mb-3"),    
], fluid=True)

@callback(
Output('xstocks-charts', 'children'),
Input('refresh', 'n_intervals'),
Input('chart-count', 'value')
)

def update_dashboard(n_intervals, n_charts):

    with sql_engine.connect() as conn:
        df = pd.read_sql("SELECT * FROM mexc_data.mexc_stocks ORDER BY timestamp DESC", conn)

    if df.empty:
        return html.Div("No data found", className="text-light fst-italic")

    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True).dt.tz_convert("Europe/Budapest")
    XSTOCKS = df.sort_values("volume24h", ascending=False)["symbol"].unique()
    xstocks_charts = []
    for symbol in XSTOCKS[:n_charts]:
        chart_df = df[df["symbol"] == symbol].sort_values("timestamp")
        stock_symbol= symbol[:-5]      
        if chart_df.empty: continue

        fig = px.line(chart_df, x="timestamp", y="price", template="plotly_dark")
        fig.update_traces(line_color='#00d1ff', line_width=2)
        fig.update_layout(paper_bgcolor='rgba(0,0,0,0)',  plot_bgcolor='rgba(0,0,0,0)', margin=dict(l=0, r=0, t=0, b=0), height=150,
            xaxis=dict(showgrid=False, title="", showticklabels=True, tickformat="%H:%M"), 
            yaxis=dict(showgrid=True, gridcolor='rgba(255,255,255,0.05)', title="", side="right"))
        
        xstocks_charts.append(
            dbc.Col([
                html.Div([
                    html.H6(f"{stock_symbol}", className="text-success mb-1"),
                    dcc.Graph(figure=fig, config={'displayModeBar': False})
                ], style=CARD_STYLE)
            ], width=3, className="mb-1")
        )
  
    return  xstocks_charts 

