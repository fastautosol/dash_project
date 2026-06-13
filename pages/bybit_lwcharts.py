# 2026.06.13  Fully Extended Multi-Chart Implementation
import pandas as pd
import numpy as np
import pandas_ta_classic as ta
from sqlalchemy import create_engine, text
import dash
from dash import html, dcc, Input, Output, callback, clientside_callback
import dash_bootstrap_components as dbc

DB_URL = "postgresql://sql_admin:sql_pass@postgresql:5432/n8n"
engine = create_engine(DB_URL)

SYMBOLS = ["BTC/USDT", "ETH/USDT", "SOL/USDT", "BNB/USDT", "AVAX/USDT", "HYPE/USDT", "BCH/USDT", "XRP/USDT", "SUI/USDT", "ZEN/USDT", "COMP/USDT", "LINK/USDT",
          "AAPLX/USDT", "TSLAX/USDT", "NVDAX/USDT", "AMZNX/USDT", "COINX/USDT", "CRCLX/USDT", "HOODX/USDT", "GOOGLX/USDT"]

CARD_STYLE = {
    "backgroundColor": "#111111",
    "borderRadius": "10px",
    "padding": "8px",
    "height": "100%",
    "border": "1px solid #222",
}

# ─────────────────────────────────────────────────────────────
# FETCH CANDLES & PROCESS PREMIUM INDICATORS
# ─────────────────────────────────────────────────────────────

def fetch_candles(symbol):
    sql = text("SELECT timestamp, open, high, low, close, volume FROM bybit_data.bybit_candles WHERE symbol = :sym ORDER BY timestamp DESC")

    with engine.connect() as conn:
        df = pd.read_sql(sql, conn, params={"sym": symbol})

    if df.empty:
        return {"candles": [], "indicators": [], "volume_profile": []}

    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")
    df["timestamp"] = df["timestamp"].dt.tz_convert("Europe/Budapest")
    df["timestamp"] = df["timestamp"].dt.tz_localize(None) 
    df = df.set_index("timestamp")

    df = df.sort_index()
    df["time"] = df.index.astype("int64") // 10**9
    df = df.drop_duplicates(subset=["time"], keep="last")

    for col in ["open", "high", "low", "close", "volume"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # --- STANDARD BASELINE INDICATORS ---
    df["sma50"] = ta.sma(df["close"], length=50)
    df["ema50"] = ta.ema(df["close"], length=50).round(4)
    bb = ta.bbands(df["close"], length=50)
    df["bb_upper"] = bb["BBU_50_2.0"]
    df["bb_middle"] = bb["BBM_50_2.0"]
    df["bb_lower"] = bb["BBL_50_2.0"]
    df["vwap"] = ta.vwap(high=df["high"], low=df["low"], close=df["close"], volume=df["volume"])
    df["mfi"] = ta.mfi(df["high"], df["low"], df["close"], df["volume"], length=14)          
    df["buy_vol"]  = df["volume"].where(df["close"] >= df["open"], 0)
    df["sell_vol"] = df["volume"].where(df["close"] <  df["open"], 0)

    # --- CUSTOM VOLUME FLOW INDICATOR (VFI) ---
    try:
        vfi_len, vfi_coef, vfi_vcoef = 130, 0.2, 2.5
        hlc3 = (df["high"] + df["low"] + df["close"]) / 3
        ln_hlc3 = np.log(hlc3)
        v_inter = ln_hlc3.diff(1)
        v_std = v_inter.rolling(vfi_len).std()
        cutoff = vfi_coef * v_std * df["close"]
        v_ma = df["volume"].rolling(vfi_len).mean()
        max_v = v_ma * vfi_vcoef
        
        direction = np.zeros(len(df))
        direction[hlc3 > hlc3.shift(1) + cutoff] = 1
        direction[hlc3 < hlc3.shift(1) - cutoff] = -1
        
        v_clipped = np.minimum(df["volume"], max_v)
        vfi_raw = direction * v_clipped
        df["vfi"] = (vfi_raw.rolling(vfi_len).sum() / v_ma) / 100.0
    except Exception:
        df["vfi"] = None

    # --- CUSTOM VOLUME PROFILE (VP) DATA GRID GENERATION ---
    volume_profile = []
    try:
        num_bins = 25
        min_p, max_p = float(df["low"].min()), float(df["high"].max())
        if max_p > min_p:
            bin_size = (max_p - min_p) / num_bins
            bins = [min_p + i * bin_size for i in range(num_bins + 1)]
            df["price_bin"] = np.digitize(df["close"], bins[:-1]) - 1
            vp_data = df.groupby("price_bin")["volume"].sum().to_dict()
            
            volume_profile = [
                {"price": round(bins[idx] + (bin_size / 2), 4), "volume": float(vp_data.get(idx, 0))}
                for idx in range(num_bins)
            ]
    except Exception:
        volume_profile = []

    # ── CANDLES: Drop only if critical price data is missing ──────────────────────────
    df_clean = df.dropna(subset=["time", "open", "high", "low", "close"])
    candles = df_clean[["time", "open", "high", "low", "close"]].to_dict("records")
    
    # ── INDICATORS: Convert NaN → None (maps cleanly to JSON null) ───────────────────
    ind_cols = ["time", "sma50", "ema50", "bb_upper", "bb_middle", "bb_lower", "vwap", "mfi", "buy_vol", "sell_vol", "vfi"]
    ind_df = df[ind_cols].where(df[ind_cols].notna(), other=None)
    indicators = ind_df.to_dict("records")
    
    return {"candles": candles, "indicators": indicators, "volume_profile": volume_profile}

# ─────────────────────────────────────────────────────────────
# REGISTER PAGE & LAYOUT WITH UPDATED SELECTORS
# ─────────────────────────────────────────────────────────────

dash.register_page(__name__, path="/bybit-lwcharts", name="Bybit LWCharts", order=3, assets_folder="assets")

layout = dbc.Container(
    [
    html.Div([html.H2("Crypto Multi Charts Pro", className="text-light fw-bold mb-0")], className="mb-4"),
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
                {"label": " Volume Δ", "value": "volume_delta"},  
                {"label": " MFI", "value": "mfi"},  
                {"label": " Vol Flow Indicator (VFI) 👑", "value": "vfi"},  
                {"label": " Volume Profile (VP) 👑", "value": "volume_profile"},  
            ], value=["ema50", "volume_profile"], inline=True, switch=True, className="text-light",
            input_checked_style={"backgroundColor": "#198754", "borderColor": "#198754"}),
        ], style={"padding": "10px 0px 15px 0px", "borderBottom": "1px solid #222", "marginBottom": "10px"}),

    dbc.Row(
        [
        dbc.Col(html.Div([
                html.H6(sym, className="text-success mb-2", style={"fontFamily": "monospace"}),
                html.Div(id=f"chart-{sym.replace('/', '-')}", style={"width": "100%", "height": "140px"}),
                ], style=CARD_STYLE), xs=12, sm=6, md=3, lg=3, xl=3)
        for sym in SYMBOLS], className="g-3 mb-3"),

    ], fluid=True, style={"backgroundColor": "--bs-body-bg", "minHeight": "100vh", "paddingBottom": "10px"})

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
