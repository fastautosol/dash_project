# 2026.06.06  18.00
# Lightweight Charts v5

import pandas as pd
import pandas_ta_classic as ta
from sqlalchemy import create_engine, text
import dash
from dash import html, dcc, Input, Output, callback, clientside_callback
import dash_bootstrap_components as dbc

DB_URL = "postgresql://sql_admin:sql_pass@postgresql:5432/n8n"
engine = create_engine(DB_URL)

SYMBOLS = ["BTC/USDT", "ETH/USDT", "SOL/USDT", "BNB/USDT", "AVAX/USDT", "HYPE/USDT", "ZEN/USDT", "XRP/USDT", "SUI/USDT"]

CARD_STYLE = {
    "backgroundColor": "#111111",
    "borderRadius": "10px",
    "padding": "8px",
    "height": "100%",
    "border": "1px solid #222",
}

# ─────────────────────────────────────────────────────────────
# FETCH CANDLES
# ─────────────────────────────────────────────────────────────

def fetch_candles(symbol):

    sql = text("SELECT timestamp, open, high, low, close, volume FROM bybit_data.bybit_candles WHERE symbol = :sym ORDER BY timestamp DESC")

    with engine.connect() as conn:
        df = pd.read_sql(sql, conn, params={"sym": symbol})

    if df.empty:
        return {"candles": [], "indicators": []}

    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")
    df["timestamp"] = df["timestamp"].dt.tz_convert("Europe/Budapest")
    df["timestamp"] = df["timestamp"].dt.tz_localize(None) # Remove timezone (avoids pandas_ta VWAP warning)
    df = df.set_index("timestamp")

    df = df.sort_index()
    df["time"] = df.index.astype("int64") // 10**9
    df = df.drop_duplicates(subset=["time"], keep="last")

    for col in ["open", "high", "low", "close", "volume"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df["sma50"] = ta.sma(df["close"], length=50)
    df["ema50"] = ta.ema(df["close"], length=50).round(4)
    bb = ta.bbands(df["close"], length=50)
    df["bb_upper"] = bb["BBU_50_2.0"]
    df["bb_middle"] = bb["BBM_50_2.0"]
    df["bb_lower"] = bb["BBL_50_2.0"]
    df["vwap"] = ta.vwap(high=df["high"], low=df["low"], close=df["close"], volume=df["volume"])

    # ── CANDLES: drop only if price data is missing ──────────────────────────
    df_clean = df.dropna(subset=["time", "open", "high", "low", "close"])
    candles = df_clean[ ["time", "open", "high", "low", "close"]].to_dict("records")
    
    # ── INDICATORS: keep all rows, convert NaN → None (→ null in JSON) ───────
    ind_cols = ["time", "sma50", "ema50", "bb_upper", "bb_middle", "bb_lower", "vwap"]
    ind_df = df[ind_cols].where(df[ind_cols].notna(), other=None)
    indicators = ind_df.to_dict("records")
    return {"candles": candles, "indicators": indicators}

# ─────────────────────────────────────────────────────────────
# REGISTER PAGE & LAYOUT
# ─────────────────────────────────────────────────────────────

dash.register_page(__name__, path="/bybit-lwcharts", name="Bybit LWCharts", order=3, assets_folder="assets")

layout = dbc.Container(
    [
    html.Div([html.H2("Crypto Multi Charts", className="text-light fw-bold mb-0")], className="mb-4"),
    html.Div(id="page-load-trigger", style={"display": "none"}),
    html.Div(id="lwc-render-trigger", style={"display": "none"}),
    dcc.Interval(id="lwc-timer", interval=15_000, n_intervals=0),
    dcc.Store(id="lwc-store"),

    html.Div(
        [
        dbc.Checklist(id="indicator-selector",
            options=[
                {"label": " SMA50", "value": "sma50"},
                {"label": " EMA50",  "value": "ema50"},
                {"label": " BB50",   "value": "bb50"},
                {"label": " VWAP",   "value": "vwap"},
            ], value=["ema50"], inline=True, switch=True, className="text-light",
            input_checked_style={"backgroundColor": "#198754", "borderColor": "#198754"}),
        ], style={"padding": "10px 0px 18px 0px", "borderBottom": "1px solid #222", "marginBottom": "20px"}),

    dbc.Row(
        [
        dbc.Col(html.Div([
                html.H6(sym, className="text-success mb-2", style={"fontFamily": "monospace"}),
                html.Div(id=f"chart-{sym.replace('/', '-')}", style={"width": "100%", "height": "210px"}),
                ], style=CARD_STYLE), xs=12, sm=6, lg=4)
        for sym in SYMBOLS], className="g-3 mb-3"),

    ], fluid=True, style={"backgroundColor": "--bs-body-bg", "minHeight": "100vh", "paddingBottom": "20px"})

# ─────────────────────────────────────────────────────────────
# CALLBACKS
# ─────────────────────────────────────────────────────────────

@callback(
    Output("lwc-store", "data"),
    Input("page-load-trigger", "children"),
    Input("lwc-timer", "n_intervals"),
    Input("indicator-selector", "value"),
    prevent_initial_call=False,
)
def load_all_charts(_, n, indicators):
    result = {}
    for sym in SYMBOLS:
        result[sym] = fetch_candles(sym)
    return result

clientside_callback(
    """
    function(data, indicators) {
        if (!data) return "";
        return window.LWCharts(data, indicators);
    }
    """,
    Output("lwc-render-trigger", "children"),
    Input("lwc-store", "data"),
    Input("indicator-selector", "value"),
)
