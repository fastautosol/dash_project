
# 2026.10.22  12.00 Lightweight-Charts
import pandas as pd
import pandas_ta_classic as ta
from sqlalchemy import create_engine, text
import dash
from dash import html, dcc, Input, Output, callback, clientside_callback
import dash_bootstrap_components as dbc
import duckdb

DB_URL = "postgresql://sql_admin:sql_pass@postgresql:5432/n8n"
engine = create_engine(DB_URL)

# Globális DuckDB kapcsolat inicializálása (Postgres csatolással)
duck_conn = duckdb.connect()
duck_conn.execute("INSTALL postgres; LOAD postgres;")
duck_conn.execute("ATTACH 'dbname=n8n user=sql_admin password=sql_pass host=postgresql port=5432' AS pg (TYPE POSTGRES);")

SYMBOLS = ["BTC/USDT", "ETH/USDT", "SOL/USDT", "BNB/USDT", "AVAX/USDT", "HYPE/USDT", "BCH/USDT", "XRP/USDT", "SUI/USDT", "ZEN/USDT", "COMP/USDT", "LINK/USDT", "NEAR/USDT", "AAVE/USDT", "LTC/USDT", "ICP/USDT",
          "AAPLX/USDT", "TSLAX/USDT", "NVDAX/USDT", "AMZNX/USDT", "COINX/USDT", "CRCLX/USDT", "HOODX/USDT", "GOOGLX/USDT"]

CARD_STYLE = {
    "backgroundColor": "#111111",
    "borderRadius": "10px",
    "padding": "8px",
    "height": "100%",
    "border": "1px solid #222",
}

# ─────────────────────────────────────────────────────────────
# MÓDOSÍTOTT FETCH CANDLES (Már a memóriából dolgozik)
# ─────────────────────────────────────────────────────────────
def process_symbol_candles(df_symbol):
    if df_symbol.empty:
        return {"candles": [], "indicators": []}

    # Időzóna konverzió (marad a megszokott logikád)
    df_symbol["timestamp"] = pd.to_datetime(df_symbol["timestamp"], utc=True, errors="coerce")
    df_symbol["timestamp"] = df_symbol["timestamp"].dt.tz_convert("Europe/Budapest")
    df_symbol["timestamp"] = df_symbol["timestamp"].dt.tz_localize(None)
    df_symbol = df_symbol.set_index("timestamp").sort_index()

    # UNIX timestamp generálása a Lightweight Charts-nak
    df_symbol["time"] = df_symbol.index.astype("datetime64[ns]").astype("int64") // 10**9
    df_symbol = df_symbol.drop_duplicates(subset=["time"], keep="last")

    for col in ["open", "high", "low", "close", "volume"]:
        df_symbol[col] = pd.to_numeric(df_symbol[col], errors="coerce")

    # Indikátorok számítása a pandas_ta segítségével
    df_symbol["sma50"] = ta.sma(df_symbol["close"], length=50)
    df_symbol["ema50"] = ta.ema(df_symbol["close"], length=50).round(4)
    bb = ta.bbands(df_symbol["close"], length=50)
    df_symbol["bb_upper"] = bb["BBU_50_2.0"]
    df_symbol["bb_middle"] = bb["BBM_50_2.0"]
    df_symbol["bb_lower"] = bb["BBL_50_2.0"]
    df_symbol["vwap"] = ta.vwap(high=df_symbol["high"], low=df_symbol["low"], close=df_symbol["close"], volume=df_symbol["volume"])
    df_symbol["mfi"] = ta.mfi(df_symbol["high"], df_symbol["low"], df_symbol["close"], df_symbol["volume"], length=14)          
    
    df_symbol["buy_vol"]  = df_symbol["volume"].where(df_symbol["close"] >= df_symbol["open"], 0)
    df_symbol["sell_vol"] = df_symbol["volume"].where(df_symbol["close"] <  df_symbol["open"], 0)

    # Gyertyák JSON formátumba hozatala
    df_clean = df_symbol.dropna(subset=["time", "open", "high", "low", "close"])
    candles = df_clean[["time", "open", "high", "low", "close"]].to_dict("records")
    
    # Indikátorok JSON formátumba hozatala (NaN -> None csere)
    ind_cols = ["time", "sma50", "ema50", "bb_upper", "bb_middle", "bb_lower", "vwap", "mfi", "buy_vol", "sell_vol"]
    ind_df = df_symbol[ind_cols].where(df_symbol[ind_cols].notna(), other=None)
    indicators = ind_df.to_dict("records")
    
    return {"candles": candles, "indicators": indicators}

# ─────────────────────────────────────────────────────────────
# REGISTER PAGE & LAYOUT
# ─────────────────────────────────────────────────────────────

dash.register_page(__name__, path="/", name="Bybit Charts", order=0, assets_folder="app_assets")

layout = dbc.Container(
    [
    html.Div([html.H2("Crypto Multi Charts", className="text-light fw-bold mb-0")], className="mb-4"),
    html.Div(id="page-load-trigger", style={"display": "none"}),
    html.Div(id="lwc-render-trigger", style={"display": "none"}),
    dcc.Interval(id="lwc-timer", interval=30_000, n_intervals=0),
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
            ], value=["ema50"], inline=True, switch=True, className="text-light",
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

    all_data_df = duck_conn.execute("SELECT symbol, timestamp, open, high, low, close, volume FROM pg.bybit_data.bybit_candles").df()    
    if all_data_df.empty:
        return {sym: {"candles": [], "indicators": []} for sym in SYMBOLS}
    
    result = {}    
    for sym in SYMBOLS:
        df_symbol = all_data_df[all_data_df["symbol"] == sym].copy()
        result[sym] = process_symbol_candles(df_symbol)
        
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
