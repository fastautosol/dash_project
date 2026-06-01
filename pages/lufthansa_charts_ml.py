# 2026.06.01  11.00
import dash
import pandas as pd
from dash import html, dcc, Input, Output, State, callback, no_update
import dash_bootstrap_components as dbc
import plotly.express as px
import plotly.graph_objects as go
from sqlalchemy import create_engine
from datetime import datetime

# ML module (same directory)
import pages.lufthansa_ml as lm

# ----- 1. CONFIGURATION -----
DB_CONFIG = "postgresql+psycopg://sql_admin:sql_pass@postgresql:5432/n8n"
sql_engine = create_engine(
    DB_CONFIG, pool_size=5, max_overflow=10, pool_pre_ping=True, pool_recycle=1800,
    connect_args={'connect_timeout': 5, 'keepalives': 1, 'keepalives_idle': 30,
                  'keepalives_interval': 10, 'keepalives_count': 5})

dash.register_page(__name__, path="/lufthansa-ml", icon="fa-solid fa-plane", name="Lufthansa Flight ML", order=5)

# ---- Glass Card ----
CARD_STYLE = {
    "background": "rgba(255, 255, 255, 0.03)",
    "backdrop-filter": "blur(10px)",
    "border-radius": "15px", "border": "1px solid rgba(255, 255, 255, 0.1)",
    "padding": "15px", "width": "100%"
}
ML_ACCENT     = "#f59e0b"   
ML_PRED_COLOR = "#38bdf8"   
ML_ACT_COLOR  = "#34d399"   

# ------------------- LAYOUT -------------------
layout = dbc.Container([
    html.Div([
        html.H2("Lufthansa Dashboard", className="text-light fw-bold mb-0"),
        html.P(id='lh-metrics-update', className="text-muted small"),
    ], className="mb-3"),

    dcc.Interval(id='refresh', interval=60000),
    dcc.Store(id="lh-df-store"),
    dcc.Store(id="lh-pred-store", data=None),

    dbc.Row(id="lh-mini-charts", className="g-3 mb-3"),

    dbc.Row([
        dbc.Col([
            html.Div([
                html.Div([
                    html.H5("Arrival Delay · HistGBR Prediction",
                            className="mb-0",
                            style={"color": ML_ACCENT, "fontWeight": "500", "fontSize": "13px", "letterSpacing": "0.03em"}),
                    dbc.Button(
                        "Run ML Prediction",
                        id="lh-delay-toggle",
                        size="sm",
                        color="warning",
                        outline=True,
                        className="ms-auto",
                        style={"fontSize": "11px", "padding": "2px 12px", "borderRadius": "20px", "fontWeight": "600"}
                    ),
                ], className="d-flex align-items-center mb-2"),

                dcc.Graph(id="lh-delay-chart", config={"displayModeBar": False}, style={"height": "260px"}),
                html.Div(id="lh-model-metrics", className="mt-1",
                         style={"fontSize": "11px", "color": "#94a3b8", "display": "flex", "gap": "14px", "flexWrap": "wrap"}),
            ], style=CARD_STYLE),
        ], md=6),

        dbc.Col([
            html.Div([
                html.H5("ML Prediction · Latest Flights",
                        className="mb-2",
                        style={"color": ML_ACCENT, "fontWeight": "500", "fontSize": "13px", "letterSpacing": "0.03em"}),
                html.Div(
                    id="lh-pred-table",
                    children=html.P("Loading flight records...", className="text-muted small mt-3", style={"paddingLeft": "4px"}),
                    style={"height": "280px", "overflowY": "auto", "fontSize": "11px"}
                ),
            ], style=CARD_STYLE),
        ], md=6),
    ], className="g-3 mb-3"),

    dbc.Row(id="lh-mini-tables", className="g-3 mb-3"),

    html.Div([
        html.H5("Lufthansa Logs", className="mb-2", style={"color": ML_ACCENT, "fontWeight": "500"}),
        html.Div(id='lh-log-table', style={"height": "300px", "overflowY": "auto", "fontSize": "12px"})
    ], style=CARD_STYLE)
], fluid=True)

# ===================================================================
# CALLBACK 1 – data load
# ===================================================================
@callback(
    Output('lh-metrics-update', 'children'),
    Output('lh-df-store', 'data'),
    Output('lh-mini-charts', 'children'),
    Output('lh-mini-tables', 'children'),
    Output('lh-log-table', 'children'),
    Output('lh-delay-chart', 'figure'),   
    Output('lh-pred-table', 'children'), 
    Output('lh-pred-store', 'data', allow_duplicate=True),
    Input('refresh', 'n_intervals'),
    State('lh-pred-store', 'data'),
    prevent_initial_call='initial_duplicate'
)
def load_data_render(_, existing_pred):
    with sql_engine.connect() as conn:
        df = pd.read_sql("SELECT * FROM silver.lh_flights", conn)
        
    if df.empty:
        empty = _empty_fig("No data")
        if existing_pred is not None:
            return "No data", None, [], [], None, no_update, no_update
        return "No data", None, [], [], None, empty, html.P("No data", className="text-muted small mt-3", style={"paddingLeft": "4px"})

    df.columns = df.columns.str.strip()

    # ---- computed columns ----
    df["dep_delay_min"] = (df["dep_act_ts"] - df["dep_sch_ts"]).dt.total_seconds() / 60
    df["arr_delay_min"] = (df["arr_act_ts"] - df["arr_sch_ts"]).dt.total_seconds() / 60
    df["dep_hour"] = df["dep_sch_ts"].dt.hour
    df["_ingested_at"] = pd.to_datetime(df["_ingested_at"]).dt.strftime("%Y-%m-%d %H:%M:%S")

    # -------------------
    # 6 MINI CHARTS
    # -------------------
    mini_charts = []

    def make_card(title, content, is_graph=True):
        if is_graph:
            content.update_layout(
                 height=200, margin=dict(l=10, r=10, t=15, b=15),
                paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                font=dict(color="white"))
        return dbc.Col([
            html.Div([
                html.H6(title, className="mb-1",
                        style={"color": ML_ACCENT, "fontWeight": "500"}),
                dcc.Graph(figure=content, config={'displayModeBar': False},
                          style={"height": "200px"})
                if is_graph else html.Div(content,
                                          style={"height": "200px", "overflowY": "auto"})
            ], style=CARD_STYLE)
        ], md=4)

    # 1 Daily
    daily_df = df.groupby(df["dep_sch_ts"].dt.floor("D")).size().reset_index(name="count")
    fig_daily = px.bar(daily_df, x="dep_sch_ts", y="count", template="plotly_dark")
    fig_daily.update_layout(height=250, plot_bgcolor='rgba(0,0,0,0)',
                             paper_bgcolor='rgba(0,0,0,0)', margin=dict(l=20, r=20, t=10, b=10))
    mini_charts.append(make_card("Daily Chart", fig_daily))

    # 2 Dep delay dist
    df_dd = df[df["dep_delay_min"].notna() & (df["dep_delay_min"] <= 100)]
    mini_charts.append(make_card("Departure Delay Dist",
        px.histogram(df_dd, x="dep_delay_min", nbins=25, template="plotly_dark")))

    # 3 Route delay
    route = df.groupby("route_key")["dep_delay_min"].mean().reset_index()
    mini_charts.append(make_card("Top Delay Routes",
        px.bar(route.sort_values("dep_delay_min", ascending=False).head(15),
               x="route_key", y="dep_delay_min", template="plotly_dark")))

    # 4 Airport traffic
    airport = df["arrival__airport_code"].value_counts().head(15).reset_index()
    airport.columns = ["airport", "count"]
    mini_charts.append(make_card("Arrival Airports",
        px.bar(airport, x="airport", y="count", template="plotly_dark")))

    # 5 Status
    status = df["status__description"].value_counts().reset_index()
    status.columns = ["status", "count"]
    mini_charts.append(make_card("Status",
        px.pie(status, names="status", values="count", hole=0.4)))

    # 6 Aircraft
    aircraft = df["equipment__aircraft_code"].value_counts().reset_index()
    aircraft.columns = ["aircraft", "count"]
    mini_charts.append(make_card("Aircraft Usage",
        px.bar(aircraft.head(10), x="aircraft", y="count", template="plotly_dark")))

    # -------------------
    # 3 SMALL TABLES
    # -------------------
    dep_tbl = (df.groupby("route_key")["dep_delay_min"].mean().round(2)
                 .sort_values(ascending=False).head(25).reset_index())
    arr_tbl = (df.groupby("route_key")["arr_delay_min"].mean().round(2)
                 .sort_values(ascending=False).head(25).reset_index())
    route_cnt = df["route_key"].value_counts().head(10).reset_index()
    route_cnt.columns = ["route_key", "count"]

    def make_table(df_t):
        return dbc.Table.from_dataframe(
            df_t, striped=False, hover=True, responsive=True,
            borderless=True, className="text-light small",
            style={"backgroundColor": "transparent",
                    "--bs-table-bg": "transparent",
                    "--bs-table-accent-bg": "transparent", "color": "white"})

    mini_tables = [
        make_card("Dep Delay", make_table(dep_tbl), is_graph=False),
        make_card("Arr Delay", make_table(arr_tbl), is_graph=False),
        make_card("Route Volume", make_table(route_cnt), is_graph=False),
    ]

    # -------------------
    # LOG TABLE
    # -------------------
    status_cols = [0, 4, 5, 6, 7, 10, 11]
    # Safe fallback if columns are fewer than expected
    cols_to_use = [c for c in status_cols if c < len(df.columns)]
    log_table = dbc.Table.from_dataframe(
         df.iloc[-100:, cols_to_use], striped=False, hover=True,
        responsive=True, borderless=True,
        className="text-light m-0",
        style={"backgroundColor": "transparent",
                "--bs-table-bg": "transparent",
                "--bs-table-accent-bg": "transparent", "color": "white"})

    # ---------------------------------------------
    # FIX: PREVENT OVERWRITING ML RESULTS
    # ---------------------------------------------
    if existing_pred is not None:
        return (
        f"Updated → {df['_ingested_at'].iloc[-1]}",
        df.to_dict("records"),
        mini_charts, mini_tables, log_table,
        no_update, no_update,
        existing_pred,    # ← pass through, don't wipe
        )

    # Callback 1 — final return (no predictions yet):
    return (
        f"Updated → {df['_ingested_at'].iloc[-1]}",
        df.to_dict("records"),
        mini_charts, mini_tables, log_table,
        fig_act, initial_pred_table,
        None,    # ← no predictions yet
    )

    # ---------------------------------------------
    # INITIAL DELAY CHART & TABLE (actual only)
    # ---------------------------------------------
    ml_display_df = df.dropna(subset=["dep_sch_ts", "arr_delay_min"]).sort_values("dep_sch_ts").tail(50).copy()
    ml_display_df["dep_sched"] = ml_display_df["dep_sch_ts"].dt.strftime("%m-%d %H:%M")
    ml_display_df["dep_delay"] = ml_display_df["dep_delay_min"].round(1)
    ml_display_df["arr_delay"] = ml_display_df["arr_delay_min"].round(1)
    ml_display_df["pred_arr_delay"] = None
    ml_display_df["error_min"] = None

    fig_act = go.Figure()
    fig_act.add_trace(go.Bar(
        x=ml_display_df["dep_sched"], y=ml_display_df["arr_delay"],
        name="Actual Arrival Delay", marker_color=ML_ACT_COLOR, opacity=0.85,
    ))
    fig_act.add_hline(y=0, line_color="rgba(255,255,255,0.15)", line_width=1)
    _style_delay_fig(fig_act, "Actual Arrival Delay (min) – latest 50 flights")

    initial_pred_table = _build_pred_table(ml_display_df)

    return (
        f"Updated → {df['_ingested_at'].iloc[-1]}",
        df.to_dict("records"),
        mini_charts,
        mini_tables,
        log_table,
        fig_act,
        initial_pred_table,
    )

# ===================================================================
# CALLBACK 2 – ML button press
# ===================================================================
@callback(
    Output('lh-delay-chart', 'figure', allow_duplicate=True),
    Output('lh-delay-toggle', 'children'),
    Output('lh-delay-toggle', 'color'),
    Output('lh-delay-toggle', 'disabled'),
    Output('lh-pred-store', 'data'),
    Output('lh-pred-table', 'children', allow_duplicate=True),
    Output('lh-model-metrics', 'children'),
    Input('lh-delay-toggle', 'n_clicks'),
    State('lh-df-store', 'data'),
    State('lh-pred-store', 'data'),
    State('lh-delay-toggle', 'children'),
    prevent_initial_call=True,
)
def run_ml(n_clicks, raw_records, existing_pred):
    if not raw_records:
        return (no_update,) * 7
        
    df = pd.DataFrame(raw_records)
    for col in ["dep_sch_ts", "dep_act_ts", "arr_sch_ts", "arr_act_ts"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")

    ml_df = lm.prepare(df)

    if existing_pred is not None:
        pred_df = pd.DataFrame(existing_pred)
        #show_combined = (n_clicks % 2 == 1)
        show_combined = (btn_label != "Hide Predicted")
        fig = _build_delay_fig(pred_df, show_combined)
        btn_label = "Hide Predicted" if show_combined else "Show Predicted"
        return fig, btn_label, "warning", False, no_update, no_update, no_update

    model, metrics = lm.train(ml_df)
