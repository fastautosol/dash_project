# 2026.11.02 EIA Grid dashboard - US map (opensky_charts stilus)
import time

import dash
import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import dash_bootstrap_components as dbc
from dash import html, Input, Output, callback, dcc, no_update
from sqlalchemy import create_engine, text

# ----- 1. CONFIGURATION -----
DB_CONFIG = "postgresql+psycopg://sql_admin:sql_pass@postgresql:5432/n8n"
sql_engine = create_engine(DB_CONFIG, pool_size=5, max_overflow=10, pool_pre_ping=True, pool_recycle=1800,
    connect_args={'connect_timeout': 5, 'keepalives': 1, 'keepalives_idle': 30, 'keepalives_interval': 10, 'keepalives_count': 5})

dash.register_page(__name__, icon="fa-solid fa-bolt", name="EIA Grid", order=4)

DAILY_TZ = "Eastern"                      # a napi (daily-*) EIA adat idozona-kulcsa
DEFAULT_RESPONDENT = "CISO"
CORE = ["CISO", "ERCO", "PJM", "MISO"]    # az indexelt osszehasonlitashoz
CACHE_SECONDS = 60
MAP_HEIGHT = 530                          # px; a jobb oldali ket kis chart egyutt ennyi
WEEKDAYS = {1: "Mon", 2: "Tue", 3: "Wed", 4: "Thu", 5: "Fri", 6: "Sat", 7: "Sun"}
FUEL_COLORS = {"Nuclear": "#a78bfa", "Coal": "#6b7280", "Natural gas": "#f59e0b", "Petroleum": "#92400e", "Other": "#9ca3af",
               "Hydro": "#3b82f6", "Geothermal": "#14b8a6", "Wind": "#34d399", "Solar": "#facc15"}

# terkep-metrikak: oszlop -> (cimke, szinskala, kozepre igazitott-e, formatum)
MAP_METRICS = {
    "growth_pct":     ("Demand growth, 30d vs same period last year (%)", "RdBu_r", True,  "{:+.1f}%"),
    "carbon_free_pct": ("Carbon-free share, 30d (%)",                      "YlGn",   False, "{:.1f}%"),
    "gas_pct":        ("Natural gas share, 30d (%)",                       "YlOrBr", False, "{:.1f}%"),
    "fe_pct":         ("Day-ahead forecast error, 30d (abs %)",            "YlOrRd", False, "{:.2f}%"),
    "net_export_pct": ("Net export, % of demand (30d)",                    "PuOr",   True,  "{:+.1f}%"),
    "lmp_latest":     ("Latest day-ahead LMP ($/MWh)",                     "Plasma", False, "{:.1f}"),
}

# ---- Glass Card ----
CARD_STYLE = {
    "background": "rgba(255, 255, 255, 0.03)",
    "backdrop-filter": "blur(10px)",
    "border-radius": "15px", "border": "1px solid rgba(255, 255, 255, 0.1)",
    "padding": "15px", "width": "100%"
}

# ----- 2. SQL -----
HOURLY_SQL = """
    SELECT interval_start_utc, local_ts, local_date, local_hour, local_isodow, demand_mwh, lmp_avg, ref_lmp, congestion_avg, ghg_avg
    FROM silver.demand_lmp_hourly
    WHERE respondent = :r AND interval_start_utc >= NOW() - CAST(:d AS integer) * INTERVAL '1 day'
    ORDER BY interval_start_utc
"""
GRID_SQL = """
    SELECT day, respondent, demand_mwh, forecast_mwh, net_generation_mwh, forecast_error_pct, forecast_abs_error_pct, net_export_mwh
    FROM silver.grid_daily
    WHERE "timezone" = :tz AND day >= CURRENT_DATE - CAST(:d AS integer)
    ORDER BY respondent, day
"""
MIX_SQL = """
    SELECT day, fueltype_code, fueltype_name, fuel_group, sort_order, generation_mwh
    FROM silver.generation_mix_daily
    WHERE respondent = :r AND "timezone" = :tz AND day >= CURRENT_DATE - CAST(:d AS integer)
    ORDER BY day, sort_order
"""
# terkep: utolso 30 nap vs. az elozo evi azonos 30 napos ablak (365 nappal korabban)
MAP_SQL = """
    WITH ld AS (SELECT MAX(day) AS d FROM silver.grid_daily WHERE "timezone" = :tz),
    cur AS (
        SELECT g.respondent, AVG(g.demand_mwh) AS dem, AVG(g.forecast_abs_error_pct) AS fe, AVG(g.net_export_mwh) AS ne, COUNT(g.demand_mwh) AS n
        FROM silver.grid_daily g, ld
        WHERE g."timezone" = :tz AND g.day > ld.d - 30 AND g.day <= ld.d
        GROUP BY g.respondent
    ),
    prev AS (
        SELECT g.respondent, AVG(g.demand_mwh) AS dem, COUNT(g.demand_mwh) AS n
        FROM silver.grid_daily g, ld
        WHERE g."timezone" = :tz AND g.day > ld.d - 395 AND g.day <= ld.d - 365
        GROUP BY g.respondent
    )
    SELECT c.respondent,
           c.dem / 24.0 AS avg_mw,
           CASE WHEN c.n >= 20 AND p.n >= 20 AND p.dem > 0 THEN (c.dem / p.dem - 1) * 100 END AS growth_pct,
           c.fe AS fe_pct,
           CASE WHEN c.dem > 0 THEN c.ne / c.dem * 100 END AS net_export_pct
    FROM cur c LEFT JOIN prev p ON p.respondent = c.respondent
"""
MIX_SUMMARY_SQL = """
    WITH ld AS (SELECT MAX(day) AS d FROM silver.generation_mix_daily WHERE "timezone" = :tz)
    SELECT m.respondent,
           SUM(CASE WHEN m.is_carbon_free THEN m.generation_mwh END) / NULLIF(SUM(CASE WHEN m.fuel_group <> 'Storage' THEN m.generation_mwh END), 0) * 100 AS carbon_free_pct,
           SUM(CASE WHEN m.fueltype_code = 'NG' THEN m.generation_mwh END) / NULLIF(SUM(CASE WHEN m.fuel_group <> 'Storage' THEN m.generation_mwh END), 0) * 100 AS gas_pct
    FROM silver.generation_mix_daily m, ld
    WHERE m."timezone" = :tz AND m.day > ld.d - 30 AND m.day <= ld.d
    GROUP BY m.respondent
"""
LMP_LATEST_SQL = """
    WITH last_day AS (
        SELECT respondent, MAX(local_date) AS d FROM silver.demand_lmp_hourly WHERE lmp_avg IS NOT NULL GROUP BY respondent
    )
    SELECT t.respondent, AVG(t.lmp_avg) AS lmp_latest
    FROM silver.demand_lmp_hourly t
    JOIN last_day l ON l.respondent = t.respondent AND t.local_date = l.d
    WHERE t.lmp_avg IS NOT NULL
    GROUP BY t.respondent
"""
DIM_SQL = "SELECT * FROM silver.dim_respondent ORDER BY is_core DESC, respondent_code"


def q(sql: str, **params) -> pd.DataFrame:
    with sql_engine.connect() as conn:
        return pd.read_sql(text(sql), conn, params=params)


# ----- 3. HELPERS -----
def empty_fig(msg="No data"):
    fig = go.Figure()
    fig.add_annotation(text=msg, x=0.5, y=0.5, xref="paper", yref="paper", showarrow=False, font=dict(color="#9ca3af"))
    fig.update_xaxes(visible=False)
    fig.update_yaxes(visible=False)
    fig.update_layout(template="plotly_dark")
    return fig


def make_card(title, content, is_graph=True, legend_bottom=False, md=4, height=230):
    if is_graph:
        content.update_layout(height=height, margin=dict(l=10, r=10, t=15, b=15), paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)", font=dict(color="white"))
        if legend_bottom:
            content.update_layout(legend=dict(orientation="h", y=-0.25, font=dict(size=9), title_text=""), margin=dict(l=10, r=10, t=15, b=45))
    return dbc.Col([
        html.Div([
            html.H6(title, className="mb-1", style={"color": "#f59e0b", "fontWeight": "500"}),
            dcc.Graph(figure=content, config={'displayModeBar': False}, style={"height": f"{height}px"})
            if is_graph else html.Div(content, style={"height": f"{height}px", "overflowY": "auto"})
        ], style=CARD_STYLE)
    ], md=md)


TABLE_STYLE = {"backgroundColor": "transparent", "--bs-table-bg": "transparent", "--bs-table-accent-bg": "transparent", "color": "white"}


def make_table(df_table, small=True):
    """dbc.Table.from_dataframe helyett kezzel epitett tabla, igy a dbc verziojatol fuggetlen."""
    head = html.Thead(html.Tr([html.Th(str(c)) for c in df_table.columns]))
    body = html.Tbody([html.Tr([html.Td("" if pd.isna(v) else str(v)) for v in row]) for row in df_table.itertuples(index=False)])
    return dbc.Table([head, body], striped=False, hover=True, responsive=True, borderless=True,
                     className="text-light small" if small else "text-light m-0", style=TABLE_STYLE)


_cache = {"ts": 0.0, "df": None}


def load_map_data() -> pd.DataFrame:
    """Respondensenkent 1 sor: dim (lat/lon/nev) + terkepes metrikak. CACHE_SECONDS-ig cache-elve."""
    now = time.time()
    if _cache["df"] is not None and now - _cache["ts"] < CACHE_SECONDS:
        return _cache["df"].copy()

    df = q(DIM_SQL)
    for sql, params in ((MAP_SQL, dict(tz=DAILY_TZ)), (MIX_SUMMARY_SQL, dict(tz=DAILY_TZ)), (LMP_LATEST_SQL, {})):
        part = q(sql, **params)
        if "respondent" not in part.columns:     # ures eredmeny -> csak NaN oszlopok
            continue
        df = df.merge(part, left_on="respondent_code", right_on="respondent", how="left").drop(columns=["respondent"])
    for c in ["avg_mw", *MAP_METRICS]:
        df[c] = pd.to_numeric(df.get(c), errors="coerce")
    _cache.update(ts=now, df=df)
    return df.copy()


def build_map(df: pd.DataFrame, metric: str, selected: str) -> go.Figure:
    label, scale, diverging, fmt = MAP_METRICS.get(metric, MAP_METRICS["growth_pct"])
    fig = go.Figure()

    if "lat" not in df.columns or df["lat"].isna().all():
        return empty_fig("silver.dim_respondent has no lat/lon - deploy the new eia_silver.py and re-run the silver step")

    d = df.dropna(subset=["lat", "lon", "avg_mw"]).copy()
    if d.empty:
        return empty_fig("No demand data for the map")

    d["size"] = 9 + 34 * np.sqrt(d["avg_mw"] / d["avg_mw"].max())
    d["hover"] = [
        f"<b>{r.respondent_code}</b> · {r.respondent_name}<br>"
        f"Avg demand: {r.avg_mw:,.0f} MW<br>"
        + "<br>".join(f"{MAP_METRICS[k][0].split(',')[0].split(' (')[0]}: {'–' if pd.isna(getattr(r, k)) else MAP_METRICS[k][3].format(getattr(r, k))}" for k in MAP_METRICS)
        for r in d.itertuples()]

    # kijelolt respondent: halvany halo a buborek alatt
    sel = d[d["respondent_code"] == selected]
    if not sel.empty:
        fig.add_trace(go.Scattermap(lat=sel["lat"], lon=sel["lon"], mode="markers", hoverinfo="skip",
                                    marker=dict(size=sel["size"] + 14, color="rgba(245, 158, 11, 0.35)")))

    has = d[d[metric].notna()]
    miss = d[d[metric].isna()]
    if not miss.empty:   # nincs adat -> szurke
        fig.add_trace(go.Scattermap(lat=miss["lat"], lon=miss["lon"], mode="markers", customdata=miss["respondent_code"],
                                    hovertext=miss["hover"], hoverinfo="text",
                                    marker=dict(size=miss["size"], color="rgba(156,163,175,0.55)")))
    if not has.empty:
        vals = has[metric]
        if diverging:
            m = max(float(np.nanpercentile(vals.abs(), 95)), 1e-6)
            rng = dict(cmin=-m, cmax=m, cmid=0)
        else:
            lo, hi = float(np.nanpercentile(vals, 5)), float(np.nanpercentile(vals, 95))
            rng = dict(cmin=lo, cmax=hi if hi > lo else lo + 1e-6)
        fig.add_trace(go.Scattermap(
            lat=has["lat"], lon=has["lon"], mode="markers+text", customdata=has["respondent_code"],
            text=has["respondent_code"], textposition="top right", textfont=dict(color="rgba(255,255,255,0.75)", size=9),
            hovertext=has["hover"], hoverinfo="text",
            marker=dict(size=has["size"], color=vals, colorscale=scale, opacity=0.85, **rng,
                        colorbar=dict(title=dict(text=label.split(" (")[0].split(",")[0], font=dict(size=10, color="white")),
                                      thickness=10, len=0.55, x=0.99, tickfont=dict(size=9, color="white")))))

    fig.update_layout(
        margin={"r": 0, "t": 0, "l": 0, "b": 0}, showlegend=False, paper_bgcolor="rgba(0,0,0,0)",
        uirevision="eia-map",   # frissitesnel / metrikavaltasnal megtartja a zoom/pan allast
        map=dict(style="carto-darkmatter", center=dict(lat=38.5, lon=-96.5), zoom=3.1))
    return fig


# ----- 4. LAYOUT -----
layout = dbc.Container([

    html.Div([
        html.H2("EIA Grid Dashboard", className="text-light fw-bold mb-0"),
        html.P(id='eia-metrics-update', className="text-muted small"),
    ], className="mb-3"),

    dcc.Interval(id='eia-refresh', interval=600000),   # 10 perc: az adat 3 oranként / naponta frissül

    dbc.Row([
        dbc.Col(dbc.Select(id="eia-respondent", options=[{"label": DEFAULT_RESPONDENT, "value": DEFAULT_RESPONDENT}], value=DEFAULT_RESPONDENT), md=4),
        dbc.Col(dbc.Select(id="eia-days", options=[{"label": f"Last {d} days", "value": d} for d in (30, 90, 180, 400)], value=90), md=2),
        dbc.Col(dbc.Select(id="eia-map-metric", options=[{"label": f"Map colour: {v[0]}", "value": k} for k, v in MAP_METRICS.items()], value="growth_pct"), md=6),
    ], className="g-3 mb-3"),

    # ---- MAP (2x szeles, 2x magas) + jobbra 2 kis chart ----
    dbc.Row([
        dbc.Col(html.Div([
            html.H6("US Grid Map  (bubble size = avg demand, click = select)", className="mb-1", style={"color": "#f59e0b", "fontWeight": "500"}),
            dcc.Graph(id="eia-map", config={'displayModeBar': False}, style={"height": f"{MAP_HEIGHT}px"}),
        ], style=CARD_STYLE), md=8),
        dbc.Col(dbc.Row(id="eia-side", className="g-3"), md=4),
    ], className="g-3 mb-3"),

    # ---- 3 KIS CHART ----
    dbc.Row(id="eia-mini-charts", className="g-3 mb-3"),

    # ---- 3 KIS TABLA ----
    dbc.Row(id="eia-mini-tables", className="g-3 mb-3"),

    # ---- LOG TABLE ----
    html.Div([
        html.H5("Hourly Demand & Day-ahead LMP (latest 100)", className="mb-2", style={"color": "#f59e0b", "fontWeight": "500"}),
        html.Div(id='eia-log-table', style={"height": "300px", "overflowY": "auto", "fontSize": "12px"})
    ], style=CARD_STYLE)

], fluid=True)


# ----- 5. DASHBOARD BUILDER (callbacktol fuggetlen, tesztelheto) -----
def build_dashboard(resp: str, days: int):
    hourly = q(HOURLY_SQL, r=resp, d=days)
    grid = q(GRID_SQL, tz=DAILY_TZ, d=days)
    mix = q(MIX_SQL, r=resp, tz=DAILY_TZ, d=days)
    try:
        mp = load_map_data()
    except Exception:
        mp = pd.DataFrame(columns=["respondent_code", "growth_pct", "fe_pct", "carbon_free_pct"])

    if not hourly.empty:
        hourly["interval_start_utc"] = pd.to_datetime(hourly["interval_start_utc"], utc=True)
        hourly["local_ts"] = pd.to_datetime(hourly["local_ts"])
    if not grid.empty:
        grid["day"] = pd.to_datetime(grid["day"])
    if not mix.empty:
        mix["day"] = pd.to_datetime(mix["day"])

    dem = hourly.dropna(subset=["demand_mwh"]) if not hourly.empty else hourly

    # ---- jobb oldal: 2 chart a terkep mellett ----
    if dem.empty:
        fig = empty_fig(f"No hourly demand for {resp}")
    else:
        last = dem["interval_start_utc"].max()
        d7 = dem[dem["interval_start_utc"] >= last - pd.Timedelta(days=7)]
        fig = px.line(d7, x="local_ts", y="demand_mwh", template="plotly_dark", labels={"local_ts": "", "demand_mwh": "MWh"})
    side = [make_card(f"{resp} Hourly Demand (7d, local time)", fig, md=12, height=MAP_HEIGHT // 2 - 55)]

    mx = mix[mix["fuel_group"] != "Storage"] if not mix.empty else mix
    if mx.empty:
        fig = empty_fig(f"No generation mix for {resp}")
    else:
        order = mx.drop_duplicates("fueltype_name").sort_values("sort_order")["fueltype_name"].tolist()
        fig = px.area(mx, x="day", y="generation_mwh", color="fueltype_name", color_discrete_map=FUEL_COLORS,
                      category_orders={"fueltype_name": order}, template="plotly_dark", labels={"day": "", "generation_mwh": "MWh"})
    side.append(make_card(f"{resp} Generation Mix (daily MWh)", fig, legend_bottom=True, md=12, height=MAP_HEIGHT // 2 - 55))

    # ---- 3 kis chart a terkep alatt ----
    mini_charts = []
    if dem.empty:
        fig = empty_fig("No data")
    else:
        hm = dem.pivot_table(index="local_isodow", columns="local_hour", values="demand_mwh", aggfunc="mean")
        hm.index = [WEEKDAYS.get(int(i), str(i)) for i in hm.index]
        fig = px.imshow(hm, aspect="auto", color_continuous_scale="Viridis", template="plotly_dark", labels=dict(x="hour (local)", y="", color="MWh"))
        fig.update_coloraxes(showscale=False)
    mini_charts.append(make_card(f"{resp} Avg Demand: Weekday x Hour", fig))

    pr = hourly.dropna(subset=["lmp_avg"]) if not hourly.empty else hourly
    if pr.empty:
        fig = empty_fig(f"No day-ahead LMP for {resp}")
    else:
        byh = pr.groupby("local_hour")[["lmp_avg", "ref_lmp"]].mean().reset_index()
        fig = px.line(byh, x="local_hour", y=["lmp_avg", "ref_lmp"], template="plotly_dark", labels={"local_hour": "hour (local)", "value": "$/MWh"})
    mini_charts.append(make_card("Day-ahead LMP by Hour ($/MWh)", fig, legend_bottom=True))

    parts = []
    if not grid.empty:
        for r in dict.fromkeys(CORE + [resp]):
            s = grid[grid["respondent"] == r].sort_values("day").set_index("day")["demand_mwh"].rolling(7, min_periods=4).mean().dropna()
            if len(s) > 0 and s.iloc[0] != 0:
                parts.append(pd.DataFrame({"day": s.index, "respondent": r, "index": s.values / s.iloc[0] * 100}))
    fig = (px.line(pd.concat(parts), x="day", y="index", color="respondent", template="plotly_dark", labels={"day": "", "index": "start = 100"})
           if parts else empty_fig("No data"))
    mini_charts.append(make_card("Demand Index (7d avg, start=100)", fig, legend_bottom=True))

    # ---- 3 kis tabla (a terkep adataibol) ----
    def top(col, name, asc, n=25):
        if col not in mp.columns or mp[col].notna().sum() == 0:
            return pd.DataFrame(columns=["respondent", name])
        t = mp.dropna(subset=[col]).sort_values(col, ascending=asc).head(n)[["respondent_code", col]].copy()
        t[col] = t[col].round(2)
        t.columns = ["respondent", name]
        return t

    mini_tables = [
        make_card("Demand Growth 30d vs last year (%)", make_table(top("growth_pct", "growth_pct", False)), is_graph=False),
        make_card("Forecast Error 30d (worst first)", make_table(top("fe_pct", "abs_err_pct", False)), is_graph=False),
        make_card("Carbon-free % (30d)", make_table(top("carbon_free_pct", "carbon_free_pct", False)), is_graph=False),
    ]

    # ---- log ----
    if dem.empty:
        log_tbl = html.Div("No hourly data", className="text-muted")
    else:
        log = dem.sort_values("interval_start_utc", ascending=False).head(100)[
            ["interval_start_utc", "demand_mwh", "lmp_avg", "ref_lmp", "congestion_avg", "ghg_avg"]].copy()
        log["interval_start_utc"] = log["interval_start_utc"].dt.strftime("%Y-%m-%d %H:%M")
        log_tbl = make_table(log.round(2), small=False)

    last_h = dem["interval_start_utc"].max().strftime("%Y-%m-%d %H:%M UTC") if not dem.empty else "-"
    last_d = grid["day"].max().strftime("%Y-%m-%d") if not grid.empty else "-"
    return f"Hourly → {last_h} · Daily → {last_d}", side, mini_charts, mini_tables, log_tbl


# ----- 6. CALLBACKS -----
@callback(
    Output('eia-respondent', 'options'),
    Input('eia-refresh', 'n_intervals'),
)
def load_respondents(_):
    try:
        df = q("SELECT respondent_code, respondent_name FROM silver.dim_respondent ORDER BY is_core DESC, respondent_code")
    except Exception:
        return [{"label": r, "value": r} for r in CORE]
    return [{"label": f"{r.respondent_code} · {r.respondent_name}", "value": r.respondent_code} for r in df.itertuples()]


@callback(
    Output('eia-map', 'figure'),
    Input('eia-refresh', 'n_intervals'),
    Input('eia-map-metric', 'value'),
    Input('eia-respondent', 'value'),
)
def update_map(_, metric, resp):
    try:
        return build_map(load_map_data(), metric or "growth_pct", resp or DEFAULT_RESPONDENT)
    except Exception as exc:
        return empty_fig(f"Map error: {str(exc).splitlines()[0]}")


@callback(
    Output('eia-respondent', 'value'),
    Input('eia-map', 'clickData'),
    prevent_initial_call=True,
)
def select_from_map(click):
    try:
        cd = click["points"][0]["customdata"]
        code = cd[0] if isinstance(cd, (list, tuple)) else cd
        return code if code else no_update
    except Exception:
        return no_update


@callback(
    Output('eia-metrics-update', 'children'),
    Output('eia-side', 'children'),
    Output('eia-mini-charts', 'children'),
    Output('eia-mini-tables', 'children'),
    Output('eia-log-table', 'children'),
    Input('eia-refresh', 'n_intervals'),
    Input('eia-respondent', 'value'),
    Input('eia-days', 'value'),
)
def load_data_render(_, resp, days):
    try:
        return build_dashboard(resp or DEFAULT_RESPONDENT, int(days or 90))
    except Exception as exc:
        return f"DB error: {exc}", [], [], [], None
