# 2026.10.26 Polars + DuckDB Multi-Charts + Crypto News panel (silver.cryptonews)
import logging
import polars as pl
import duckdb
import dash
from dash import html, dcc, Input, Output, callback, clientside_callback
import dash_bootstrap_components as dbc

logger = logging.getLogger(__name__)

# Globális DuckDB kapcsolat inicializálása (Postgres csatolással)
# FIGYELEM: a kapcsolatot több callback szál használja, ezért mindenhol duck_conn.cursor()-t használunk.
duck_conn = duckdb.connect()
duck_conn.execute("INSTALL postgres; LOAD postgres;")
duck_conn.execute("ATTACH 'dbname=n8n user=sql_admin password=sql_pass host=postgresql port=5432' AS pg (TYPE POSTGRES);")

MAX_CANDLES = 300

# Hírpanel beállítások
NEWS_HOURS = 48      # ennyi órára visszamenőleg mutat híreket
NEWS_LIMIT = 40      # legfeljebb ennyi hír
NEWS_ORDER = {       # a rendezés fix, a felhasználói érték csak kulcs
    "latest": "ts DESC",
    "top": "importance_score DESC, ts DESC",
}
NEWS_SQL = """
    SELECT provider, kind, source, title, url, urgent, importance_score, like_count, view_count,
           strftime(COALESCE(published_at, fetched_at) AT TIME ZONE 'Europe/Budapest', '%m.%d %H:%M') AS ts_local,
           COALESCE(published_at, fetched_at) AS ts
    FROM pg.silver.cryptonews
    WHERE COALESCE(published_at, fetched_at) >= now() - INTERVAL '{hours} hours'
    ORDER BY {order}
    LIMIT {limit}
"""

SYMBOLS = [ "BTC/USDT", "ETH/USDT", "SOL/USDT", "BNB/USDT", "AVAX/USDT", "HYPE/USDT", "BCH/USDT", "XRP/USDT", "SUI/USDT", "ZEN/USDT", 
            "COMP/USDT", "LINK/USDT", "NEAR/USDT", "AAVE/USDT", "LTC/USDT",  "ICP/USDT", 
           "AAPLX/USDT", "TSLAX/USDT", "NVDAX/USDT", "AMZNX/USDT", "COINX/USDT", "CRCLX/USDT", "HOODX/USDT", "GOOGLX/USDT" ]

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
        pl.col("timestamp").dt.convert_time_zone("Europe/Budapest").dt.epoch("s").alias("time"),
        pl.col("open").cast(pl.Float64),
        pl.col("high").cast(pl.Float64),
        pl.col("low").cast(pl.Float64),
        pl.col("close").cast(pl.Float64),
        pl.col("volume").cast(pl.Float64),
    ]).sort("time").unique(subset=["time"], keep="last", maintain_order=True).tail(MAX_CANDLES).with_columns([
        
        # 1. SMA 50 és EMA 50 (rolling ablakokkal)
        pl.col("close").rolling_mean(window_size=50).alias("sma50"),
        pl.col("close").ewm_mean(span=50, adjust=False).alias("ema50"),
        
        # 2. Bollinger Bands alapok (Közép és Szórás)
        pl.col("close").rolling_mean(window_size=50).alias("bb_middle"),
        pl.col("close").rolling_std(window_size=50).alias("bb_std"),
        pl.col("high").rolling_max(window_size=50).alias("dc_upper"),
        pl.col("low").rolling_min(window_size=50).alias("dc_lower"),
        
        # 3. VWAP (Kumulált Ár * Volumen / Kumulált Volumen)
        ((pl.col("close") * pl.col("volume")).cum_sum() / pl.col("volume").cum_sum()).alias("vwap"),
        
        # 4. Volume Delta (Vevői és Eladói volumen szétválasztása)
        pl.when(pl.col("close") >= pl.col("open")).then(pl.col("volume")).otherwise(0.0).alias("buy_vol"),
        pl.when(pl.col("close") < pl.col("open")).then(pl.col("volume")).otherwise(0.0).alias("sell_vol"),
    ]).with_columns([
        # Bollinger alsó és felső sávok kiszámítása a már kész adatokból
        (pl.col("bb_middle") + (2 * pl.col("bb_std"))).alias("bb_upper"),
        (pl.col("bb_middle") - (2 * pl.col("bb_std"))).alias("bb_lower"),
        ((pl.col("dc_upper") + pl.col("dc_lower")) / 2).alias("dc_middle"),
    ])

    # ── GYERTYÁK FORMÁZÁSA (Csak ott dobjuk el, ahol nincs ár) ─────────────────
    candles_df = processed.drop_nulls(subset=["time", "open", "high", "low", "close"])
    candles = candles_df.select(["time", "open", "high", "low", "close"]).to_dicts()
    
    # ── INDIKÁTOROK FORMÁZÁSA (NaN/Null megtartása -> null lesz a JSON-ban) ───
    ind_cols = ["time", "sma50", "ema50", "bb_upper", "bb_middle", "bb_lower", "dc_upper", "dc_middle", "dc_lower", "vwap", "buy_vol", "sell_vol"]
    indicators = processed.select(ind_cols).to_dicts()
    
    return {"candles": candles, "indicators": indicators}

# ─────────────────────────────────────────────────────────────
# HÍRPANEL SEGÉDEK
# ─────────────────────────────────────────────────────────────

def _safe_url(url):
    """Csak http(s) linket engedünk (javascript: stb. ne kerülhessen href-be)."""
    return url if isinstance(url, str) and url.startswith(("http://", "https://")) else None


def _short_num(n):
    if n is None:
        return None
    n = int(n)
    if n >= 1_000_000:
        return f"{n / 1_000_000:.1f}M"
    if n >= 1_000:
        return f"{n / 1_000:.1f}k"
    return str(n)


def _news_item(r, sort_mode):
    is_twitter = r["provider"] == "twitter"
    source = r["source"] or r["provider"]
    if is_twitter:
        source = f"@{source}"

    accent = "#ef5350" if r["urgent"] else ("#42a5f5" if is_twitter else "#26a69a")

    meta = [
        html.Span(source, className="text-success"),
        html.Span(r["ts_local"] or "", className="text-muted"),
    ]
    if r["urgent"]:
        meta.append(dbc.Badge("URGENT", color="danger", className="ms-1"))
    if is_twitter and (r["like_count"] or r["view_count"]):
        meta.append(html.Span(f"♥ {_short_num(r['like_count']) or 0} · 👁 {_short_num(r['view_count']) or 0}",
                              className="text-muted"))
    if sort_mode == "top":
        meta.append(html.Span(f"★ {r['importance_score']}", className="text-warning"))

    title = (r["title"] or "").strip()
    url = _safe_url(r["url"])
    title_el = (html.A(title, href=url, target="_blank", rel="noopener noreferrer",
                       className="text-light text-decoration-none")
                if url else html.Span(title, className="text-light"))

    return html.Div(
        [html.Div(meta, className="d-flex flex-wrap gap-2 small mb-1", style={"fontFamily": "monospace"}),
         html.Div(title_el, style={"fontSize": "0.85rem", "lineHeight": "1.25"})],
        style={"borderLeft": f"3px solid {accent}", "padding": "4px 8px", "marginBottom": "8px",
               "backgroundColor": "#161616", "borderRadius": "4px"})

# ─────────────────────────────────────────────────────────────
# REGISTER PAGE & LAYOUT
# ─────────────────────────────────────────────────────────────

dash.register_page(__name__, path="/", name="Bybit Charts", order=0, assets_folder="app_assets")

charts_grid = dbc.Row(
    [
    dbc.Col(html.Div([
            html.H6(sym, className="text-success mb-2", style={"fontFamily": "monospace"}),
            html.Div(id=f"chart-{sym.replace('/', '-')}", style={"width": "100%", "height": "140px"}),
            ], style=CARD_STYLE), xs=12, sm=6, md=4, lg=4, xl=4)     # 3 oszlop
    for sym in SYMBOLS], className="g-3 mb-3")

news_panel = html.Div(
    [
    html.Div([
        html.H6("Crypto News", className="text-success mb-0", style={"fontFamily": "monospace"}),
        dbc.RadioItems(id="news-sort",
            options=[{"label": " Latest", "value": "latest"}, {"label": " Top", "value": "top"}],
            value="latest", inline=True, className="text-light small"),
        ], className="d-flex justify-content-between align-items-center mb-2"),
    html.Div(id="news-panel", style={"overflowY": "auto", "flex": "1"}),
    dcc.Interval(id="news-timer", interval=60_000, n_intervals=0),
    ],
    style={**CARD_STYLE, "position": "sticky", "top": "10px", "height": "calc(100vh - 20px)",
           "display": "flex", "flexDirection": "column"})

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
                {"label": " DC50", "value": "dc50"},
                {"label": " VWAP",   "value": "vwap"},
                {"label": " Volume Δ", "value": "volume_delta"},  
            ], value=["ema50"], inline=True, switch=True, className="text-light",
            input_checked_style={"backgroundColor": "#198754", "borderColor": "#198754"}),
        ], style={"padding": "10px 0px 15px 0px", "borderBottom": "1px solid #222", "marginBottom": "10px"}),

    # Bal oldal: 3 oszlopos chart rács, jobb oldal: hírpanel
    dbc.Row(
        [
        dbc.Col(charts_grid, xs=12, lg=9),
        dbc.Col(news_panel, xs=12, lg=3),
        ], className="g-3"),

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
    # 1. Egyetlen szupergyors bináris adatátvitel DuckDB-vel -> Azonnal Polars DataFrame lesz (.pl())
    all_data_pl = duck_conn.cursor().execute(
        "SELECT symbol, timestamp, open, high, low, close, volume FROM pg.bybit_data.bybit_candles").pl()
    if all_data_pl.height == 0:
        return {sym: {"candles": [], "indicators": []} for sym in SYMBOLS}
                        
    parts = all_data_pl.partition_by("symbol", as_dict=True)
    result = {}
    for sym in SYMBOLS:
        df_symbol = parts.get((sym,), all_data_pl.clear())
        result[sym] = process_symbol_candles_polars(df_symbol)
        
    return result


@callback(
    Output("news-panel", "children"),
    Input("news-timer", "n_intervals"),
    Input("news-sort", "value"),
)
def load_news(_, sort_mode):
    sort_mode = sort_mode if sort_mode in NEWS_ORDER else "latest"
    try:
        sql = NEWS_SQL.format(hours=NEWS_HOURS, order=NEWS_ORDER[sort_mode], limit=NEWS_LIMIT)
        rows = duck_conn.cursor().execute(sql).pl().to_dicts()
    except Exception:
        # pl. a silver tábla újraépítése közben pillanatnyilag nem létezik
        logger.exception("Hírek betöltése sikertelen")
        return html.Div("A hírek átmenetileg nem elérhetők.", className="text-muted small")

    if not rows:
        return html.Div(f"Nincs hír az elmúlt {NEWS_HOURS} órából.", className="text-muted small")
    return [_news_item(r, sort_mode) for r in rows]


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
