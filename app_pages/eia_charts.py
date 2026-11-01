# 2026.11.01 EIA Grid dashboard (silver: demand_lmp_hourly, grid_daily, generation_mix_daily)
import dash
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import dash_bootstrap_components as dbc
from dash import html, Input, Output, callback, dcc
from sqlalchemy import create_engine, text

# ----- 1. CONFIGURATION -----
DB_CONFIG = "postgresql+psycopg://sql_admin:sql_pass@postgresql:5432/n8n"
sql_engine = create_engine(DB_CONFIG, pool_size=5, max_overflow=10, pool_pre_ping=True, pool_recycle=1800,
    connect_args={'connect_timeout': 5, 'keepalives': 1, 'keepalives_idle': 30, 'keepalives_interval': 10, 'keepalives_count': 5})

dash.register_page(__name__, icon="fa-solid fa-bolt", name="EIA Grid", order=4)

DAILY_TZ = "Eastern"                      # a napi (daily-*) EIA adat idozona-kulcsa
DEFAULT_RESPONDENT = "CISO"
CORE = ["CISO", "ERCO", "PJM", "MISO"]    # az indexelt osszehasonlitashoz
WEEKDAYS = {1: "Mon", 2: "Tue", 3: "Wed", 4: "Thu", 5: "Fri", 6: "Sat", 7: "Sun"}
FUEL_COLORS = {"Nuclear": "#a78bfa", "Coal": "#6b7280", "Natural gas": "#f59e0b", "Petroleum": "#92400e", "Other": "#9ca3af",
               "Hydro": "#3b82f6", "Geothermal": "#14b8a6", "Wind": "#34d399", "Solar": "#facc15"}

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
CARBON_FREE_SQL = """
    SELECT respondent,
           ROUND(CAST(SUM(CASE WHEN is_carbon_free THEN generation_mwh END)
                      / NULLIF(SUM(CASE WHEN fuel_group <> 'Storage' THEN generation_mwh END), 0) * 100 AS numeric), 1) AS carbon_free_pct
    FROM silver.generation_mix_daily
    WHERE "timezone" = :tz AND day >= CURRENT_DATE - 30
    GROUP BY respondent
    ORDER BY carbon_free_pct DESC NULLS LAST
    LIMIT 25
"""
PRICE_SNAPSHOT_SQL = """
    WITH last_day AS (
        SELECT respondent, MAX(local_date) AS d
        FROM silver.demand_lmp_hourly WHERE lmp_avg IS NOT NULL GROUP BY respondent
    )
    SELECT t.respondent, l.d AS local_date,
           ROUND(CAST(AVG(t.lmp_avg) AS numeric), 1) AS avg_lmp,
           ROUND(CAST(MIN(t.lmp_min) AS numeric), 1) AS min_lmp,
           ROUND(CAST(MAX(t.lmp_max) AS numeric), 1) AS max_lmp
    FROM silver.demand_lmp_hourly t
    JOIN last_day l ON l.respondent = t.respondent AND t.local_date = l.d
    WHERE t.lmp_avg IS NOT NULL
    GROUP BY t.respondent, l.d
    ORDER BY t.respondent
"""
RESPONDENTS_SQL = "SELECT respondent_code, respondent_name FROM silver.dim_respondent ORDER BY is_core DESC, respondent_code"


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


def make_card(title, content, is_graph=True, legend_bottom=False):
    if is_graph:
        content.update_layout(height=230, margin=dict(l=10, r=10, t=15, b=15), paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)", font=dict(color="white"))
        if legend_bottom:
            content.update_layout(legend=dict(orientation="h", y=-0.25, font=dict(size=9), title_text=""), margin=dict(l=10, r=10, t=15, b=45))
    return dbc.Col([
        html.Div([
            html.H6(title, className="mb-1", style={"color": "#f59e0b", "fontWeight": "500"}),
            dcc.Graph(figure=content, config={'displayModeBar': False}, style={"height": "230px"})
            if is_graph else html.Div(content, style={"height": "230px", "overflowY": "auto"})
        ], style=CARD_STYLE)
    ], md=4)


TABLE_STYLE = {"backgroundColor": "transparent", "--bs-table-bg": "transparent", "--bs-table-accent-bg": "transparent", "color": "white"}


def make_table(df_table, small=True):
    """dbc.Table.from_dataframe helyett kezzel epitett tabla, igy a dbc verziojatol fuggetlen."""
    head = html.Thead(html.Tr([html.Th(str(c)) for c in df_table.columns]))
    body = html.Tbody([html.Tr([html.Td("" if pd.isna(v) else str(v)) for v in row]) for row in df_table.itertuples(index=False)])
    return dbc.Table([head, body], striped=False, hover=True, responsive=True, borderless=True,
                     className="text-light small" if small else "text-light m-0", style=TABLE_STYLE)


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
    ], className="g-3 mb-3"),

    # ---- 6 MINI CHART GRID ----
    dbc.Row(id="eia-mini-charts", className="g-3 mb-3"),

    # ---- 3 SMALL TABLE ----
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
    carbon = q(CARBON_FREE_SQL, tz=DAILY_TZ)
    snap = q(PRICE_SNAPSHOT_SQL)

    if not hourly.empty:
        hourly["interval_start_utc"] = pd.to_datetime(hourly["interval_start_utc"], utc=True)
        hourly["local_ts"] = pd.to_datetime(hourly["local_ts"])
    if not grid.empty:
        grid["day"] = pd.to_datetime(grid["day"])
    if not mix.empty:
        mix["day"] = pd.to_datetime(mix["day"])

    dem = hourly.dropna(subset=["demand_mwh"]) if not hourly.empty else hourly
    mini_charts = []

    # 1. Hourly demand, last 7 days (helyi ido)
    if dem.empty:
        fig = empty_fig(f"No hourly demand for {resp}")
    else:
        last = dem["interval_start_utc"].max()
        d7 = dem[dem["interval_start_utc"] >= last - pd.Timedelta(days=7)]
        fig = px.line(d7, x="local_ts", y="demand_mwh", template="plotly_dark", labels={"local_ts": "", "demand_mwh": "MWh"})
    mini_charts.append(make_card(f"{resp} Hourly Demand (7d, local time)", fig))

    # 2. Heatmap: ora x hetnap
    if dem.empty:
        fig = empty_fig("No data")
    else:
        hm = dem.pivot_table(index="local_isodow", columns="local_hour", values="demand_mwh", aggfunc="mean")
        hm.index = [WEEKDAYS.get(int(i), str(i)) for i in hm.index]
        fig = px.imshow(hm, aspect="auto", color_continuous_scale="Viridis", template="plotly_dark", labels=dict(x="hour (local)", y="", color="MWh"))
        fig.update_coloraxes(showscale=False)
    mini_charts.append(make_card("Avg Demand: Weekday x Hour", fig))

    # 3. Day-ahead LMP az orak szerint
    pr = hourly.dropna(subset=["lmp_avg"]) if not hourly.empty else hourly
    if pr.empty:
        fig = empty_fig(f"No day-ahead LMP for {resp}")
    else:
        byh = pr.groupby("local_hour")[["lmp_avg", "ref_lmp"]].mean().reset_index()
        fig = px.line(byh, x="local_hour", y=["lmp_avg", "ref_lmp"], template="plotly_dark", labels={"local_hour": "hour (local)", "value": "$/MWh"})
    mini_charts.append(make_card("Day-ahead LMP by Hour ($/MWh)", fig, legend_bottom=True))

    # 4. Termelesi mix (halmozott terulet, taroloktol eltekintve)
    mx = mix[mix["fuel_group"] != "Storage"] if not mix.empty else mix
    if mx.empty:
        fig = empty_fig(f"No generation mix for {resp}")
    else:
        order = mx.drop_duplicates("fueltype_name").sort_values("sort_order")["fueltype_name"].tolist()
        fig = px.area(mx, x="day", y="generation_mwh", color="fueltype_name", color_discrete_map=FUEL_COLORS,
                      category_orders={"fueltype_name": order}, template="plotly_dark", labels={"day": "", "generation_mwh": "MWh"})
    mini_charts.append(make_card(f"{resp} Generation Mix (daily MWh)", fig, legend_bottom=True))

    # 5. Elorejelzesi hiba
    g = grid[grid["respondent"] == resp].sort_values("day").copy() if not grid.empty else grid
    if g.empty or g["forecast_abs_error_pct"].notna().sum() == 0:
        fig = empty_fig(f"No forecast error for {resp}")
    else:
        g["abs_err_7d_avg"] = g["forecast_abs_error_pct"].rolling(7, min_periods=3).mean()
        fig = px.line(g, x="day", y=["forecast_abs_error_pct", "abs_err_7d_avg"], template="plotly_dark", labels={"day": "", "value": "% of demand"})
    mini_charts.append(make_card(f"{resp} Day-ahead Forecast Error (abs %)", fig, legend_bottom=True))

    # 6. Fogyasztas-index (7 napos atlag, az ablak elejen = 100): core + kivalasztott
    parts = []
    if not grid.empty:
        for r in dict.fromkeys(CORE + [resp]):
            s = grid[grid["respondent"] == r].sort_values("day").set_index("day")["demand_mwh"].rolling(7, min_periods=4).mean().dropna()
            if len(s) > 0 and s.iloc[0] != 0:
                parts.append(pd.DataFrame({"day": s.index, "respondent": r, "index": s.values / s.iloc[0] * 100}))
    if parts:
        fig = px.line(pd.concat(parts), x="day", y="index", color="respondent", template="plotly_dark", labels={"day": "", "index": "start = 100"})
    else:
        fig = empty_fig("No data")
    mini_charts.append(make_card("Demand Index (7d avg, start=100)", fig, legend_bottom=True))

    # -------------------
    # 3 SMALL TABLE
    # -------------------
    if grid.empty:
        fe = pd.DataFrame(columns=["respondent", "abs_err_pct"])
    else:
        recent = grid[grid["day"] >= grid["day"].max() - pd.Timedelta(days=30)]
        fe = (recent.groupby("respondent")["forecast_abs_error_pct"].mean().round(2).sort_values(ascending=False).head(25).reset_index())
        fe.columns = ["respondent", "abs_err_pct"]

    mini_tables = [
        make_card("Forecast Error 30d (worst first)", make_table(fe), is_graph=False),
        make_card("Carbon-free % (30d)", make_table(carbon), is_graph=False),
        make_card("Latest Day-ahead LMP ($/MWh)", make_table(snap), is_graph=False),
    ]

    # -------------------
    # LOG TABLE
    # -------------------
    if dem.empty:
        log_tbl = html.Div("No hourly data", className="text-muted")
    else:
        log = dem.sort_values("interval_start_utc", ascending=False).head(100)[
            ["interval_start_utc", "demand_mwh", "lmp_avg", "ref_lmp", "congestion_avg", "ghg_avg"]].copy()
        log["interval_start_utc"] = log["interval_start_utc"].dt.strftime("%Y-%m-%d %H:%M")
        log = log.round(2)
        log_tbl = make_table(log, small=False)

    last_h = dem["interval_start_utc"].max().strftime("%Y-%m-%d %H:%M UTC") if not dem.empty else "-"
    last_d = grid["day"].max().strftime("%Y-%m-%d") if not grid.empty else "-"
    return f"Hourly → {last_h} · Daily → {last_d}", mini_charts, mini_tables, log_tbl


# ----- 6. CALLBACKS -----
@callback(
    Output('eia-respondent', 'options'),
    Input('eia-refresh', 'n_intervals'),
)
def load_respondents(_):
    try:
        df = q(RESPONDENTS_SQL)
    except Exception:
        return [{"label": r, "value": r} for r in CORE]
    return [{"label": f"{r.respondent_code} · {r.respondent_name}", "value": r.respondent_code} for r in df.itertuples()]


@callback(
    Output('eia-metrics-update', 'children'),
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
        return f"DB error: {exc}", [], [], None
