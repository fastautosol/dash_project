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

dash.register_page(__name__, icon="fa-brands fa-youtube", name="Youtube Charts", order=8)

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

DASH_ID_TAG = "youtube"
# -------------------------------------------------
# LAYOUT
# -------------------------------------------------
layout = dbc.Container([

    html.Div([
        html.H2("YouTube Metrics Dashboard",
            className="text-light fw-bold mb-0")
    ], className="mb-3"),

    dcc.Interval(id='refresh', interval=60000),

    dcc.Store(id=f"{DASH_ID_TAG}-df-store"),

    # MINI CHARTS
    dbc.Row(id=f"{DASH_ID_TAG}-mini-charts",
        className="g-3 mb-3"),

    # MINI TABLES
    dbc.Row(id=f"{DASH_ID_TAG}-mini-tables",
        className="g-3 mb-3"),

    # COMMENT LOG
    html.Div([

        html.H5( "Latest Comments", className="text-success mb-2",
            style={ "color": "#ef4444", "fontWeight": "500"}),

        html.Div(
            id=f"{DASH_ID_TAG}-log-table",
            style={"height": "350px", "overflowY": "auto",  "fontSize": "12px"})

    ], style=CARD_STYLE)

], fluid=True)

# -------------------------------------------------
# HELPERS
# -------------------------------------------------

def make_card(title, content, is_graph=True, md_col=3):

    if is_graph:
        content.update_layout(height=220, margin=dict(l=10, r=10, t=30, b=10), paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)", font=dict(color="white"))
    return dbc.Col([
        html.Div([
            html.H6(title, className="text-success mb-2", style={ "color": "#ef4444", "fontWeight": "500"} ),
            dcc.Graph( figure=content, config={"displayModeBar": False}, style={"height": "240px"})
            if is_graph else html.Div(content, style={ "height": "240px", "overflowY": "auto"})
        ], style=CARD_STYLE)
    ], md=md_col)

def make_table(df_table):
    return dbc.Table.from_dataframe(df_table, striped=False, hover=True, responsive=True, borderless=True, className="text-light small",
        style={"backgroundColor": "transparent",  "--bs-table-bg": "transparent", "--bs-table-accent-bg": "transparent", "color": "white"})

# -------------------------------------------------
# CALLBACK
# -------------------------------------------------

@callback(
    Output(f"{DASH_ID_TAG}-df-store", "data"),
    Output(f"{DASH_ID_TAG}-mini-charts", "children"),
    Output(f"{DASH_ID_TAG}-mini-tables", "children"),
    Output(f"{DASH_ID_TAG}-log-table", "children"),
    Input("refresh", "n_intervals")
)

def load_youtube_data(_):

    query = "SELECT * FROM bronze.youtube_metrics ORDER BY _ingested_at DESC LIMIT 500"
    with sql_engine.connect() as conn:
        df = pd.read_sql(query, conn)
    if df.empty:
        return None, [], [], None

    df.columns = [c.lower() for c in df.columns]
    numeric_cols = ["view_count", "like_count", "comment_count", "duration_sec"]
    for c in numeric_cols:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    df["upload_date"] = pd.to_datetime(df["upload_date"], errors="coerce")
    df["_ingested_at"] = pd.to_datetime(df["_ingested_at"], errors="coerce")

    df = df.sort_values("_ingested_at").drop_duplicates(subset=["video_id"], keep="last")

    df["engagement_rate"] = ((df["like_count"] + df["comment_count"]) / df["view_count"].replace(0, np.nan)) * 100
    df["duration_min"] = (df["duration_sec"] / 60).round(1)

    # -------------------------------------------------
    # MINI CHARTS
    # -------------------------------------------------

    mini_charts = []

    ch_views = (df.groupby("channel")["view_count"].sum().sort_values(ascending=False).head(10).reset_index())
    fig1 = px.bar(ch_views, x="channel", y="view_count", template="plotly_dark")
    fig1.update_xaxes(tickangle=-25)
    mini_charts.append(make_card("Views by Channel", fig1, md_col=4))

    trend_df = (df.groupby(df["upload_date"].dt.date).agg({"video_id": "count","view_count": "sum"}).reset_index())
    fig2 = px.line(trend_df, x="upload_date", y="view_count", markers=True, template="plotly_dark")
    mini_charts.append(make_card("Daily Views Trend", fig2, md_col=4))

    eng_df = (df.groupby("channel")["engagement_rate"].mean().sort_values(ascending=False).head(10).reset_index())
    fig3 = px.bar(eng_df, x="channel", y="engagement_rate", template="plotly_dark")
    fig3.update_xaxes(tickangle=-25)
    mini_charts.append(make_card("Avg Engagement %", fig3, md_col=4))

    # -------------------------------------------------
    # MINI TABLES
    # -------------------------------------------------

    # Top videos
    top_videos = (df[["channel", "title", "view_count", "like_count", "comment_count"]].sort_values("view_count", ascending=False).head(15))
    top_videos["title"] = top_videos["title"].apply(lambda x: str(x)[:55] + "..." if len(str(x)) > 55 else str(x))

    # Best engagement
    best_eng = (df[["channel", "title", "engagement_rate", "view_count"]].sort_values("engagement_rate", ascending=False).head(15))
    best_eng["engagement_rate"] = best_eng["engagement_rate"].round(2)
    best_eng["title"] = best_eng["title"].apply(lambda x: str(x)[:55] + "..." if len(str(x)) > 55 else str(x))

    mini_tables = [
        make_card("Top Videos", make_table(top_videos), is_graph=False, md_col=6),
        make_card("Best Engagement", make_table(best_eng), is_graph=False, md_col=6)
    ]

    # -------------------------------------------------
    # COMMENTS LOG TABLE
    # -------------------------------------------------

    comment_rows = []
    for _, row in df.iterrows():
        
        comments = row.get("comments")
        if not comments:
            continue

        if isinstance(comments, list):
            for c in comments:
                comment_rows.append({
                    "channel": row["channel"],
                    "video": str(row["title"])[:45],
                    "author": c.get("c_author"),
                    "comment": str(c.get("c_text"))[:120],
                    "likes": c.get("c_like_count"),
                    "published": c.get("c_published")
                })

    comments_df = pd.DataFrame(comment_rows)

    if not comments_df.empty:

        comments_df["published"] = pd.to_datetime(comments_df["published"], errors="coerce")
        comments_df = comments_df.sort_values("published", ascending=False).head(150)
        comments_df["published"] = comments_df["published"].dt.strftime("%Y-%m-%d %H:%M")

    log_table = dbc.Table.from_dataframe(comments_df, striped=False, hover=True, responsive=True, borderless=True, className="text-light text-success small",
        style={"backgroundColor": "transparent", "--bs-table-bg": "transparent", "--bs-table-accent-bg": "transparent", "color": "white", "fontSize": "11px"})

    return df.to_dict("records"), mini_charts, mini_tables, log_table
