# 2026.10.22 Polars-only Multi-Charts (DuckDB nélkül)
import os
import polars as pl
import dash
from dash import html, dcc, Input, Output, callback, clientside_callback
import dash_bootstrap_components as dbc

# connectorx kell hozzá: pip install connectorx
PG_URI = os.getenv("PG_URI", "postgresql://sql_admin:sql_pass@postgresql:5432/n8n")

SYMBOLS = ["BTC/USDT", "ETH/USDT", "SOL/USDT", "BNB/USDT", "AVAX/USDT", "HYPE/USDT", "BCH/USDT", "XRP/USDT", "SUI/USDT", "ZEN/USDT", "COMP/USDT", "LINK/USDT", "NEAR/USDT", "AAVE/USDT", "LTC/USDT", "ICP/USDT",
          "AAPLX/USDT", "TSLAX/USDT", "NVDAX/USDT", "AMZNX/USDT", "COINX/USDT", "CRCLX/USDT", "HOODX/USDT", "GOOGLX/USDT"]

# Csak a megjelenített szimbólumokat kérjük le (konstans lista, nem felhasználói input)
_SYMBOL_LIST_SQL = ", ".join(f"'{s}'" for s in SYMBOLS)
CANDLES_QUERY = f"SELECT symbol, timestamp, open, high, low, close, volume FROM bybit_data.bybit_candles WHERE symbol IN ({_SYMBOL_LIST_SQL})"

CARD_STYLE = {
    "backgroundColor": "#111111",
    "borderRadius": "10px",
    "padding": "8px",
    "height": "100%",
    "border": "1px solid #222"}

CANDLE_COLS = ["time", "open", "high", "low", "close"]
IND_COLS = ["time", "sma50", "ema50", "bb_upper", "bb_middle", "bb_lower", "vwap", "mfi", "buy_vol", "sell_vol"]

# ─────────────────────────────────────────────────────────────
# POLARS INDIKÁTOR MOTOR – egyetlen vektorizált lépés az összes szimbólumra
# ─────────────────────────────────────────────────────────────

def compute_indicators(df: pl.DataFrame) -> pl.DataFrame:
    sym = "symbol"
    pos14 = pl.col("pos_mf").rolling_sum(window_size=14).over(sym)
    neg14 = pl.col("neg_mf").rolling_sum(window_size=14).over(sym)

    return (
        df.with_columns(
            pl.col("timestamp").dt.epoch("s").alias("time"),   # UTC epoch másodperc
            pl.col("open").cast(pl.Float64),
            pl.col("high").cast(pl.Float64),
            pl.col("low").cast(pl.Float64),
            pl.col("close").cast(pl.Float64),
            pl.col("volume").cast(pl.Float64),
        )
        .sort(sym, "time")
        .unique(subset=[sym, "time"], keep="last", maintain_order=True)
        .with_columns(((pl.col("high") + pl.col("low") + pl.col("close")) / 3).alias("tp"))
        .with_columns(
            # SMA / valódi EMA (az eredetiben az EMA is rolling_mean volt)
            pl.col("close").rolling_mean(window_size=50).over(sym).alias("sma50"),
            pl.col("close").ewm_mean(span=50, adjust=False).over(sym).alias("ema50"),
            pl.col("close").rolling_std(window_size=50).over(sym).alias("bb_std"),
            # VWAP (kumulált, szimbólumonként)
            ((pl.col("close") * pl.col("volume")).cum_sum().over(sym)
             / pl.col("volume").cum_sum().over(sym)).alias("vwap"),
            # Volume Delta
            pl.when(pl.col("close") >= pl.col("open")).then(pl.col("volume")).otherwise(0.0).alias("buy_vol"),
            pl.when(pl.col("close") < pl.col("open")).then(pl.col("volume")).otherwise(0.0).alias("sell_vol"),
            # MFI előkészítés (pozitív / negatív money flow)
            pl.when(pl.col("tp").diff().over(sym) > 0).then(pl.col("tp") * pl.col("volume")).otherwise(0.0).alias("pos_mf"),
            pl.when(pl.col("tp").diff().over(sym) < 0).then(pl.col("tp") * pl.col("volume")).otherwise(0.0).alias("neg_mf"),
        )
        .with_columns(
            pl.col("sma50").alias("bb_middle"),
            (pl.col("sma50") + 2 * pl.col("bb_std")).alias("bb_upper"),
            (pl.col("sma50") - 2 * pl.col("bb_std")).alias("bb_lower"),
            # MFI(14) = 100 * pos / (pos + neg)
            (100 * pos14 / (pos14 + neg14)).alias("mfi"),
        )
        # NaN -> null, hogy a JSON-ban null legyen
        .with_columns(pl.col(pl.Float64).fill_nan(None))
    )


def build_payload(df: pl.DataFrame) -> dict:
    result = {s: {"candles": [], "indicators": []} for s in SYMBOLS}
    for key, part in df.partition_by("symbol", as_dict=True, maintain_order=True).items():
        s = key[0] if isinstance(key, tuple) else key
        candles = (part.drop_nulls(subset=["open", "high", "low", "close"])
                       .select(CANDLE_COLS).to_dicts())
        result[s] = {"candles": candles, "indicators": part.select(IND_COLS).to_dicts()}
    return result

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

    ], fluid=True, style={"backgroundColor": "var(--bs-body-bg)", "minHeight": "100vh", "paddingBottom": "10px"})

# ─────────────────────────────────────────────────────────────
# CALLBACKS
# ─────────────────────────────────────────────────────────────

@callback(
    Output("lwc-store", "data"),
    Input("page-load-trigger", "children"),
    Input("lwc-timer", "n_intervals"),   # az indikátor-választó nem kell ide, csak a kliens oldalon számít
    prevent_initial_call=False)
def load_all_charts(_, n):
    df = pl.read_database_uri(CANDLES_QUERY, PG_URI, engine="connectorx")
    if df.height == 0:
        return {s: {"candles": [], "indicators": []} for s in SYMBOLS}
    return build_payload(compute_indicators(df))


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
