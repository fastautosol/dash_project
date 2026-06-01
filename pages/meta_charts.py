# 2026.05.19  16:00
import dash
import pandas as pd
import numpy as np

from dash import html, dcc, Input, Output, callback
import dash_bootstrap_components as dbc
import plotly.express as px
import plotly.graph_objects as go

from sqlalchemy import create_engine
from datetime import datetime

# -------------------------------------------------
# CONFIG
# -------------------------------------------------
DB_CONFIG = "postgresql+psycopg://sql_admin:sql_pass@postgresql:5432/n8n"

sql_engine = create_engine(DB_CONFIG, pool_size=5, max_overflow=10, pool_pre_ping=True, pool_recycle=1800,
    connect_args={'connect_timeout': 5, 'keepalives': 1, 'keepalives_idle': 30, 'keepalives_interval': 10, 'keepalives_count': 5})

# Register as a page in your multi-page application setup
dash.register_page(__name__, icon="fa-brands fa-meta", name="Meta Charts", order=6)

# -------------------------------------------------
# STYLE
# -------------------------------------------------
CARD_STYLE = {
    "background": "rgba(255, 255, 255, 0.03)",
    "backdrop-filter": "blur(10px)",
    "border-radius": "15px",
    "border": "1px solid rgba(255, 255, 255, 0.1)",
    "padding": "15px",
    "width": "100%"
}

DASH_ID_TAG = "meta"
META_BLUE = "#1877F2"

# -------------------------------------------------
# LAYOUT
# -------------------------------------------------
layout = dbc.Container([

    html.Div([
        html.H2("Meta Lead Generation Dashboard",
            className="text-light fw-bold mb-0")
    ], className="mb-3"),

    dcc.Interval(id=f"{DASH_ID_TAG}-refresh", interval=60000),

    dcc.Store(id=f"{DASH_ID_TAG}-df-store"),

    # MINI CHARTS (Distribution, Trends, Forms)
    dbc.Row(id=f"{DASH_ID_TAG}-mini-charts",
        className="g-3 mb-3"),

    # MINI TABLES (Recent Leads, Form Lead Volumes)
    dbc.Row(id=f"{DASH_ID_TAG}-mini-tables",
        className="g-3 mb-3"),

    # AUDIT TRAIL / DETAILED LEDGER
    html.Div([
        html.H5("Live Lead Audit Ledger", className="mb-2",
            style={"color": META_BLUE, "fontWeight": "500"}),

        html.Div(
            id=f"{DASH_ID_TAG}-log-table",
            style={"height": "350px", "overflowY": "auto", "fontSize": "12px"})
            
    ], style=CARD_STYLE)

], fluid=True)

# -------------------------------------------------
# HELPERS
# -------------------------------------------------
def make_card(title, content, is_graph=True, md_col=3):
    if is_graph:
        content.update_layout(
            height=220, 
            margin=dict(l=10, r=10, t=30, b=10), 
            paper_bgcolor="rgba(0,0,0,0)", 
            plot_bgcolor="rgba(0,0,0,0)", 
            font=dict(color="white")
        )
    return dbc.Col([
        html.Div([
            html.H6(title, className="mb-2", style={"color": META_BLUE, "fontWeight": "500"}),
            dcc.Graph(figure=content, config={"displayModeBar": False}, style={"height": "240px"})
            if is_graph else html.Div(content, style={"height": "240px", "overflowY": "auto"})
        ], style=CARD_STYLE)
    ], md=md_col)

def make_table(df_table):
    return dbc.Table.from_dataframe(
        df_table, striped=False, hover=True, responsive=True, borderless=True, className="text-light small",
        style={"backgroundColor": "transparent", "--bs-table-bg": "transparent", "--bs-table-accent-bg": "transparent", "color": "white"}
    )

# -------------------------------------------------
# CALLBACK
# -------------------------------------------------
@callback(
    Output(f"{DASH_ID_TAG}-df-store", "data"),
    Output(f"{DASH_ID_TAG}-mini-charts", "children"),
    Output(f"{DASH_ID_TAG}-mini-tables", "children"),
    Output(f"{DASH_ID_TAG}-log-table", "children"),
    Input(f"{DASH_ID_TAG}-refresh", "n_intervals")
)
def load_meta_data(_):
    # Query from your newly assigned bronze.meta_leads table
    query = "SELECT * FROM bronze.meta_leads ORDER BY _ingested_at DESC LIMIT 1000"
    with sql_engine.connect() as conn:
        df = pd.read_sql(query, conn)
    if df.empty:
        return None, [], [], None

    # Handle standard string/case initialization
    df.columns = [c.lower() for c in df.columns]
    
    # Clean datetime fields safely
    df["created_at"] = pd.to_datetime(df["created_at"], errors="coerce")
    df["_ingested_at"] = pd.to_datetime(df["_ingested_at"], errors="coerce")

    # Deduplicate matching identities using the unique lead_id primary key 
    df = df.sort_values("_ingested_at").drop_duplicates(subset=["lead_id"], keep="last")

    # Filter out pure technical error logs from standard marketing data charts
    clean_df = df[df["source_channel"] != "error_log"].copy()

    # -------------------------------------------------
    # MINI CHARTS
    # -------------------------------------------------
    mini_charts = []

    # Chart 1: Omnichannel Volume Breakdowns (Facebook vs Instagram Pie Chart)
    channel_vol = clean_df.groupby("source_channel").size().reset_index(name="lead_count")
    fig1 = px.pie(
        channel_vol, names="source_channel", values="lead_count", 
        template="plotly_dark", color_discrete_sequence=["#1877F2", "#E1306C"]
    )
    mini_charts.append(make_card("Leads by Channel Source", fig1, md_col=4))

    # Chart 2: Time-Series Lead Acquisition Speeds
    trend_df = clean_df.groupby(clean_df["created_at"].dt.date).size().reset_index(name="lead_count")
    fig2 = px.line(trend_df, x="created_at", y="lead_count", markers=True, template="plotly_dark")
    fig2.update_traces(line_color=META_BLUE)
    mini_charts.append(make_card("Daily Lead Velocity Trend", fig2, md_col=4))

    # Chart 3: Capture Forms Performance Matrix
    form_df = clean_df.groupby("form_id").size().reset_index(name="lead_count").sort_values("lead_count", ascending=False).head(10)
    fig3 = px.bar(form_df, x="form_id", y="lead_count", template="plotly_dark")
    fig3.update_traces(marker_color=META_BLUE)
    fig3.update_xaxes(type='category', tickangle=-25)
    mini_charts.append(make_card("Top Converting Forms", fig3, md_col=4))

    # -------------------------------------------------
    # MINI TABLES
    # -------------------------------------------------
    # Table 1: Most Recent Profiles Captured
    recent_leads = clean_df[["full_name", "email", "source_channel", "created_at"]].sort_values("created_at", ascending=False).head(15)
    if not recent_leads.empty:
        recent_leads["created_at"] = recent_leads["created_at"].dt.strftime("%m-%d %H:%M")
    
    # Table 2: Source Page Distribution Volume
    page_dist = clean_df.groupby(["page_id", "source_channel"]).size().reset_index(name="total_leads").sort_values("total_leads", ascending=False).head(15)

    mini_tables = [
        make_card("Latest Profile Submissions", make_table(recent_leads), is_graph=False, md_col=6),
        make_card("Active Meta Pages Performance", make_table(page_dist), is_graph=False, md_col=6)
    ]

    # -------------------------------------------------
    # LEAD LOG LEDGER (Bottom Section)
    # -------------------------------------------------
    ledger_rows = []
    for _, row in df.iterrows():
        # Captures error handling logs directly inside your monitoring interface
        if row["source_channel"] == "error_log":
            ledger_rows.append({
                "timestamp": row["_ingested_at"],
                "channel": "SYSTEM_ERROR",
                "identity": f"Page: {row['page_id']}",
                "contact_detail": "N/A",
                "form_or_message": str(row["error"])[:150]
            })
            continue

        # Parses dynamic fields context smoothly 
        raw = row.get("raw_fields") or {}
        # Checks if there are any distinct custom questions submitted in the form payload
        custom_notes = ", ".join([f"{k}:{v}" for k, v in raw.items() if k not in ['email', 'full_name', 'phone_number']])
        
        ledger_rows.append({
            "timestamp": row["created_at"],
            "channel": str(row["source_channel"]).upper(),
            "identity": row["full_name"] or "Anonymous",
            "contact_detail": row["email"] or row["phone_number"] or "Hidden",
            "form_or_message": custom_notes if custom_notes else f"Standard Form ({row['form_id']})"
        })

    ledger_df = pd.DataFrame(ledger_rows)

    if not ledger_df.empty:
        ledger_df["timestamp"] = pd.to_datetime(ledger_df["timestamp"], errors="coerce")
        ledger_df = ledger_df.sort_values("timestamp", ascending=False).head(100)
        ledger_df["timestamp"] = ledger_df["timestamp"].dt.strftime("%Y-%m-%d %H:%M")

    log_table = dbc.Table.from_dataframe(
        ledger_df, striped=False, hover=True, responsive=True, borderless=True, 
        className="text-light text-success small",
        style={"backgroundColor": "transparent", "--bs-table-bg": "transparent", "--bs-table-accent-bg": "transparent", "color": "white", "fontSize": "11px"}
    )

    return df.to_dict("records"), mini_charts, mini_tables, log_table
