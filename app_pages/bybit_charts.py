# 2026.10.22 18.00 Polars + DuckDB Optimized Multi-Charts
import polars as pl
import duckdb
import dash
from dash import html, dcc, Input, Output, callback, clientside_callback
import dash_bootstrap_components as dbc

# Globális DuckDB kapcsolat inicializálása (Postgres csatolással)
duck_conn = duckdb.connect()
duck_conn.execute("INSTALL postgres; LOAD postgres;")
duck_conn.execute("ATTACH 'dbname=n8n user=sql_admin password=sql_pass host=postgresql port=5432' AS pg (TYPE POSTGRES);")

SYMBOLS = [ "BTC/USDT", "ETH/USDT", "SOL/USDT", "BNB/USDT", "AVAX/USDT", 
            "HYPE/USDT", "BCH/USDT", "XRP/USDT", "SUI/USDT", "ZEN/USDT", 
            "COMP/USDT", "LINK/USDT", "NEAR/USDT", "AAVE/USDT", "LTC/USDT", 
            "ICP/USDT", "AAPLX/USDT", "TSLAX/USDT", "NVDAX/USDT", "AMZNX/USDT", 
            "COINX/USDT", "CRCLX/USDT", "HOODX/USDT", "GOOGLX/USDT" ]

CARD_STYLE = {
    "backgroundColor": "#111111",
    "borderRadius": "10px",
    "padding": "8px",
    "height": "100%",
    "border": "1px solid #222",
}

# ─────────────────────────────────────────────────────────────
# POLARS INDIKÁTOR SZÁMÍTÓ MOTOR (PySpark stílusban)
# ─────────────────────────────────────────────────────────────

def process_symbol_candles_polars(df_symbol_pl):
    if df_symbol_pl.height == 0:
        return {"candles": [], "indicators": []}

    # Transzformációk láncolása (Teljesen PySpark/Polars stílusban)
    processed = df_symbol_pl.with_columns([
        # Időzóna átrakása Budapest időre, majd UNIX timestamp (másodperc) generálása
        pl.col("timestamp").dt.convert_time_zone("Europe/Budapest").dt.epoch("s").alias("time"),
        
        # Típusok kényszerítése numerikusra a biztonság kedvéért
        pl.col("open").cast(pl.Float64),
        pl.col("high").cast(pl.Float64),
        pl.col("low").cast(pl.Float64),
        pl.col("close").cast(pl.Float64),
        pl.col("volume").cast(pl.Float64),
    ]).sort("time").unique(subset=["time"], keep="last").with_columns([
        
        # 1. SMA 50 és EMA 50 (rolling ablakokkal)
        pl.col("close").rolling_mean(window_size=50).alias("sma50"),
        pl.col("close").ewm_mean(span=50, adjust=False).alias("ema50"),
        
        # 2. Bollinger Bands alapok (Közép és Szórás)
        pl.col("close").rolling_mean(window_size=50).alias("bb_middle"),
        pl.col("close").rolling_std(window_size=50).alias("bb_std"),
        
        # 3. VWAP (Kumulált Ár * Volumen / Kumulált Volumen)
        ((pl.col("close") * pl.col("volume")).cum_sum() / pl.col("volume").cum_sum()).alias("vwap"),
        
        # 4. Volume Delta (Vevői és Eladói volumen szétválasztása)
        pl.when(pl.col("close") >= pl.col("open")).then(pl.col("volume")).otherwise(0.0).alias("buy_vol"),
        pl.when(pl.col("close") < pl.col("open")).then(pl.col("volume")).otherwise(0.0).alias("sell_vol"),
    ]).with_columns([
        # Bollinger alsó és felső sávok kiszámítása a már kész adatokból
        (pl.col("bb_middle") + (2 * pl.col("bb_std"))).alias("bb_upper"),
        (pl.col("bb_middle") - (2 * pl.col("bb_std"))).alias("bb_lower"),
    ])

    # ── GYERTYÁK FORMÁZÁSA (Csak ott dobjuk el, ahol nincs ár) ─────────────────
    candles_df = processed.drop_nulls(subset=["time", "open", "high", "low", "close"])
    candles = candles_df.select(["time", "open", "high", "low", "close"]).to_dicts()
    
    # ── INDIKÁTOROK FORMÁZÁSA (NaN/Null megtartása -> null lesz a JSON-ban) ───
    ind_cols = ["time", "sma50", "ema50", "bb_upper", "bb_middle", "bb_lower", "vwap", "buy_vol", "sell_vol"]
    indicators = processed.select(ind_cols).to_dicts()
    
    return {"candles": candles, "indicators": indicators}

# ─────────────────────────────────────────────────────────────
# REGISTER PAGE & LAYOUT
# ─────────────────────────────────────────────────────────────

dash.register_page(__name__, path="/", name="Bybit LWCharts", order=0, assets_folder="app_assets")

layout = dbc.Container(
    [
    html.Div([html.H2("Crypto Multi Charts (Polars Engine)", className="text-light fw-bold mb-0")], className="mb-4"),
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
# CALLBACKS (EGYETLEN POLARS BEOLVASÁSSAL)
# ─────────────────────────────────────────────────────────────

@callback(
    Output("lwc-store", "data"),
    Input("page-load-trigger", "children"),
    Input("lwc-timer", "n_intervals"),
    Input("indicator-selector", "value"),
    prevent_initial_call=False,
)
def load_all_charts(_, n, indicators):
    # 1. Egyetlen szupergyors bináris adatátvitel DuckDB-vel -> Azonnal Polars DataFrame lesz (.pl())
            
    all_data_pl = duck_conn.sql("SELECT symbol, timestamp, open, high, low, close, volume FROM pg.bybit_data.bybit_candles").pl()
    if all_data_pl.height == 0:
        return {sym: {"candles": [], "indicators": []} for sym in SYMBOLS}
        
    result = {}  
    for sym in SYMBOLS:
        df_symbol = all_data_pl.filter(pl.col("symbol") == sym)
        result[sym] = process_symbol_candles_polars(df_symbol)
        
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

