# 2026.06.14  12.00 Lightweight-Charts + Order Flow Profile (last candle)
import math
import pandas as pd
import pandas_ta_classic as ta
import ccxt
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
# EXCHANGE (REST) — used only for fetching recent trades for the
# order-flow / volume-profile column of the last candle.
# ─────────────────────────────────────────────────────────────

exchange = ccxt.bybit({"enableRateLimit": True})
try:
    exchange.load_markets()
except Exception as e:
    print(f"load_markets failed: {e}")

# Simple per-symbol cache so we don't hammer the REST API every
# 15s for 20 symbols. Profile is refreshed at most every PROFILE_TTL sec.
_profile_cache = {}
PROFILE_TTL = 15  # seconds

# ─────────────────────────────────────────────────────────────
# ORDER FLOW / VOLUME PROFILE (LAST CANDLE)
# ─────────────────────────────────────────────────────────────

def get_price_tick(symbol):
    """Return the symbol's price tick size, or None if unavailable."""
    market = exchange.markets.get(symbol) if exchange.markets else None
    if not market:
        return None
    tick = market.get("precision", {}).get("price")
    if tick is None:
        return None
    # ccxt's TICK_SIZE precision mode returns the actual tick (e.g. 0.1, 0.01)
    if tick <= 0:
        return None
    return tick


def get_last_candle_profile(symbol, last_candle, max_bins=40):
    """
    Fetch recent trades and bucket buy/sell volume by price for the
    time range of the last candle. Bucket size = symbol tick size,
    widened (by an integer factor) if that would create too many bins.
    Returns a list of {price_low, price_high, buy, sell} dicts.
    """
    import time
    now = time.time()

    cached = _profile_cache.get(symbol)
    if cached and (now - cached["ts"]) < PROFILE_TTL and cached["candle_time"] == last_candle["time"]:
        return cached["profile"]

    tick = get_price_tick(symbol)
    if tick is None:
        _profile_cache[symbol] = {"ts": now, "candle_time": last_candle["time"], "profile": []}
        return []

    since = int(last_candle["time"]) * 1000

    try:
        trades = exchange.fetch_trades(symbol, since=since, limit=1000)
    except Exception as e:
        print(f"fetch_trades failed for {symbol}: {e}")
        _profile_cache[symbol] = {"ts": now, "candle_time": last_candle["time"], "profile": []}
        return []

    if not trades:
        _profile_cache[symbol] = {"ts": now, "candle_time": last_candle["time"], "profile": []}
        return []

    lo, hi = last_candle["low"], last_candle["high"]
    if hi <= lo:
        _profile_cache[symbol] = {"ts": now, "candle_time": last_candle["time"], "profile": []}
        return []

    # Widen the bucket size if the natural tick size would create
    # too many rows to render cleanly.
    n_natural = (hi - lo) / tick
    if n_natural > max_bins:
        factor = math.ceil(n_natural / max_bins)
        bucket_size = tick * factor
    else:
        bucket_size = tick

    bins = {}
    for t in trades:
        price = t.get("price")
        side = t.get("side")
        amount = t.get("amount")
        if price is None or amount is None or side is None:
            continue
        idx = int((price - lo) / bucket_size)
        b = bins.setdefault(idx, {"buy": 0.0, "sell": 0.0})
        if side == "buy":
            b["buy"] += amount
        elif side == "sell":
            b["sell"] += amount

    profile = []
    for idx in sorted(bins.keys()):
        price_low = lo + idx * bucket_size
        b = bins[idx]
        profile.append({
            "price_low": round(price_low, 8),
            "price_high": round(price_low + bucket_size, 8),
            "buy": round(b["buy"], 8),
            "sell": round(b["sell"], 8),
        })

    _profile_cache[symbol] = {"ts": now, "candle_time": last_candle["time"], "profile": profile}
    return profile

# ─────────────────────────────────────────────────────────────
# FETCH CANDLES
# ─────────────────────────────────────────────────────────────

def fetch_candles(symbol, want_profile=False):

    sql = text("SELECT timestamp, open, high, low, close, volume FROM bybit_data.bybit_candles WHERE symbol = :sym ORDER BY timestamp DESC")

    with engine.connect() as conn:
        df = pd.read_sql(sql, conn, params={"sym": symbol})

    if df.empty:
        return {"candles": [], "indicators": [], "profile": []}

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

    df["mfi"] = ta.mfi(df["high"], df["low"], df["close"], df["volume"], length=14)          
    df["buy_vol"]  = df["volume"].where(df["close"] >= df["open"], 0)
    df["sell_vol"] = df["volume"].where(df["close"] <  df["open"], 0)

    # ── CANDLES: drop only if price data is missing ──────────────────────────
    df_clean = df.dropna(subset=["time", "open", "high", "low", "close"])
    candles = df_clean[ ["time", "open", "high", "low", "close"]].to_dict("records")
    
    # ── INDICATORS: keep all rows, convert NaN → None (→ null in JSON) ───────
    ind_cols = ["time", "sma50", "ema50", "bb_upper", "bb_middle", "bb_lower", "vwap", "mfi", "buy_vol", "sell_vol"]
    ind_df = df[ind_cols].where(df[ind_cols].notna(), other=None)
    indicators = ind_df.to_dict("records")

    # ── ORDER FLOW PROFILE (last candle only) ─────────────────────────────────
    profile = []
    if want_profile and candles:
        last_candle = candles[-1]
        profile = get_last_candle_profile(symbol, last_candle)

    return {"candles": candles, "indicators": indicators, "profile": profile}

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
                {"label": " Volume Δ", "value": "volume_delta"},  
                {"label": " MFI", "value": "mfi"},
                {"label": " Order Flow", "value": "profile"},
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
    want_profile = "profile" in (indicators or [])
    result = {}
    for sym in SYMBOLS:
        result[sym] = fetch_candles(sym, want_profile=want_profile)
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
