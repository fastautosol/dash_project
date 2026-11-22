# 2026.11.22  17.00
import os
import json
import dash
import pandas as pd
import numpy as np

from dash import html, dcc, Input, Output, callback
import dash_bootstrap_components as dbc
import plotly.express as px

from sqlalchemy import create_engine

# -------------------------------------------------
# CONFIG
# -------------------------------------------------
PG_USER = os.getenv("POSTGRES_USER") or os.getenv("DB_USER", "sql_admin")
PG_PASS = os.getenv("POSTGRES_PASSWORD") or os.getenv("DB_PASSWORD", "sql_pass")
PG_HOST = os.getenv("POSTGRES_HOST") or os.getenv("DB_HOST", "postgresql")
PG_PORT = os.getenv("POSTGRES_PORT") or os.getenv("DB_PORT", "5432")
PG_DB = os.getenv("POSTGRES_DB") or os.getenv("DB_NAME", "n8n")

DB_CONFIG = f"postgresql+psycopg://{PG_USER}:{PG_PASS}@{PG_HOST}:{PG_PORT}/{PG_DB}"
TABLE = "bronze.youtube_videos_raw"

sql_engine = create_engine(
    DB_CONFIG,
    pool_size=5,
    max_overflow=10,
    pool_pre_ping=True,
    pool_recycle=1800,
    connect_args={
        "connect_timeout": 5,
        "keepalives": 1,
        "keepalives_idle": 30,
        "keepalives_interval": 10,
        "keepalives_count": 5,
    },
)

dash.register_page(__name__, icon="fa-brands fa-youtube", name="Youtube Charts", order=6)

# -------------------------------------------------
# STYLE
# -------------------------------------------------
CARD_STYLE = {
    "background": "rgba(255, 255, 255, 0.03)",
    "backdrop-filter": "blur(10px)",
    "border-radius": "15px",
    "border": "1px solid rgba(255, 255, 255, 0.1)",
    "padding": "15px",
    "width": "100%",
}

DASH_ID_TAG = "youtube"

# -------------------------------------------------
# LAYOUT
# -------------------------------------------------
layout = dbc.Container([

    html.Div([
        html.H2("YouTube Metrics Dashboard", className="text-light fw-bold mb-0"),
        html.P(id=f"{DASH_ID_TAG}-metrics-update", className="text-muted small"),
    ], className="mb-3"),

    dcc.Interval(id=f"{DASH_ID_TAG}-refresh", interval=60000),
    dcc.Store(id=f"{DASH_ID_TAG}-df-store"),

    # MINI CHARTS
    dbc.Row(id=f"{DASH_ID_TAG}-mini-charts", className="g-3 mb-3"),

    # EXTRA CHARTS (store-link / duration)
    dbc.Row(id=f"{DASH_ID_TAG}-extra-charts", className="g-3 mb-3"),

    # MINI TABLES
    dbc.Row(id=f"{DASH_ID_TAG}-mini-tables", className="g-3 mb-3"),

    # COMMENT LOG
    html.Div([
        html.H5("Latest Comments", className="text-success mb-2", style={"color": "#ef4444", "fontWeight": "500"}),
        html.Div(id=f"{DASH_ID_TAG}-log-table", style={"height": "350px", "overflowY": "auto", "fontSize": "12px"}),
    ], style=CARD_STYLE),

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
            font=dict(color="white"),
        )
    return dbc.Col([
        html.Div([
            html.H6(title, className="text-success mb-2", style={"color": "#ef4444", "fontWeight": "500"}),
            dcc.Graph(figure=content, config={"displayModeBar": False}, style={"height": "240px"})
            if is_graph else html.Div(content, style={"height": "240px", "overflowY": "auto"}),
        ], style=CARD_STYLE)
    ], md=md_col)


def make_table(df_table):
    return dbc.Table.from_dataframe(
        df_table,
        striped=False,
        hover=True,
        responsive=True,
        borderless=True,
        className="text-light small",
        style={"backgroundColor": "transparent", "--bs-table-bg": "transparent", "--bs-table-accent-bg": "transparent", "color": "white"},
    )


def short(x, n=55):
    x = str(x)
    return x[:n] + "..." if len(x) > n else x


def to_list(x):
    """jsonb -> python list (psycopg parses jsonb already; also tolerate JSON strings / NULL)."""
    if isinstance(x, list):
        return x
    if isinstance(x, str):
        try:
            v = json.loads(x)
            return v if isinstance(v, list) else []
        except Exception:
            return []
    return []


def to_bool(s):
    """Robust bool: works for real booleans, 'true'/'false' strings and NULLs."""
    return s.map(lambda v: v if isinstance(v, bool) else str(v).strip().lower() in ("true", "t", "1", "yes"))


# -------------------------------------------------
# CALLBACK
# -------------------------------------------------

@callback(
    Output(f"{DASH_ID_TAG}-metrics-update", "children"),
    Output(f"{DASH_ID_TAG}-df-store", "data"),
    Output(f"{DASH_ID_TAG}-mini-charts", "children"),
    Output(f"{DASH_ID_TAG}-extra-charts", "children"),
    Output(f"{DASH_ID_TAG}-mini-tables", "children"),
    Output(f"{DASH_ID_TAG}-log-table", "children"),
    Input(f"{DASH_ID_TAG}-refresh", "n_intervals"),
)
def load_youtube_data(_):
    # Csak a szükséges oszlopok (a nagy méretű transcript_text / transcript_segments nélkül)
    query = f"""
        SELECT video_id, video_title, channel, channel_title, upload_date,
               has_captions, video_definition, view_count, like_count, comment_count,
               description_snippet, has_store_link, duration_sec, comments,
               transcript_status, transcript_language, transcript_is_generated,
               processed, _ingested_at
        FROM {TABLE}
        ORDER BY upload_date DESC
        LIMIT 5000
    """
    try:
        with sql_engine.connect() as conn:
            df = pd.read_sql(query, conn)
    except Exception as exc:
        return f"DB error ({PG_HOST}:{PG_PORT}): {exc}", None, [], [], [], None

    if df.empty:
        return "No data", None, [], [], [], None

    df.columns = [c.lower() for c in df.columns]
    for c in ["view_count", "like_count", "comment_count", "duration_sec"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    df["upload_date"] = pd.to_datetime(df["upload_date"], utc=True, errors="coerce")
    df["has_store_link"] = to_bool(df["has_store_link"]) if "has_store_link" in df.columns else False

    # Display channel: human-readable title, fall back to the @handle
    df["channel_name"] = df["channel_title"].fillna(df["channel"])

    # -------------------------------------------------
    # VIDEO-LEVEL DATAFRAME
    # -------------------------------------------------
    video_df = df.sort_values("_ingested_at", ascending=False).drop_duplicates(subset=["video_id"], keep="first").copy()
    video_df["engagement_rate"] = ((video_df["like_count"] + video_df["comment_count"]) / video_df["view_count"].replace(0, np.nan)) * 100
    video_df["duration_min"] = (video_df["duration_sec"] / 60).round(1)
    video_df["comments_loaded"] = video_df["comments"].apply(lambda x: len(to_list(x))) if "comments" in video_df.columns else 0

    # -------------------------------------------------
    # MINI CHARTS
    # -------------------------------------------------
    mini_charts = []

    ch_views = video_df.groupby("channel_name")["view_count"].sum().sort_values(ascending=False).head(10).reset_index()
    fig1 = px.bar(ch_views, x="channel_name", y="view_count", template="plotly_dark")
    fig1.update_xaxes(tickangle=-25)
    mini_charts.append(make_card("Views by Channel", fig1, md_col=4))

    trend_df = (
        video_df.dropna(subset=["upload_date"])
        .groupby(video_df["upload_date"].dt.date)
        .agg({"video_id": "count", "view_count": "sum"})
        .reset_index()
    )
    fig2 = px.line(trend_df, x="upload_date", y="view_count", markers=True, template="plotly_dark")
    mini_charts.append(make_card("Daily Views Trend", fig2, md_col=4))

    eng_df = (
        video_df.dropna(subset=["engagement_rate"])
        .groupby("channel_name")["engagement_rate"]
        .mean()
        .sort_values(ascending=False)
        .head(10)
        .reset_index()
    )
    fig3 = px.bar(eng_df, x="channel_name", y="engagement_rate", template="plotly_dark")
    fig3.update_xaxes(tickangle=-25)
    mini_charts.append(make_card("Avg Engagement %", fig3, md_col=4))

    # -------------------------------------------------
    # EXTRA CHARTS — has_store_link, duration_sec
    # -------------------------------------------------
    extra_charts = []

    store_counts = video_df["has_store_link"].value_counts().rename({True: "Store link", False: "No store link"}).reset_index()
    store_counts.columns = ["label", "count"]
    fig4 = px.pie(store_counts, names="label", values="count", template="plotly_dark", hole=0.5)
    extra_charts.append(make_card("Videos with Store Links", fig4, md_col=4))

    store_views = video_df.groupby("has_store_link")["view_count"].mean().rename({True: "Store link", False: "No store link"}).reset_index()
    store_views.columns = ["label", "avg_views"]
    fig5 = px.bar(store_views, x="label", y="avg_views", template="plotly_dark")
    extra_charts.append(make_card("Avg Views: Store-Link vs Not", fig5, md_col=4))

    fig6 = px.histogram(video_df, x="duration_min", nbins=20, template="plotly_dark")
    extra_charts.append(make_card("Video Duration Distribution (min)", fig6, md_col=4))

    # -------------------------------------------------
    # MINI TABLES
    # -------------------------------------------------
    top_videos = video_df[["channel_name", "video_title", "view_count", "like_count", "comment_count"]].sort_values("view_count", ascending=False).head(15).copy()
    top_videos["video_title"] = top_videos["video_title"].apply(short)
    top_videos = top_videos.rename(columns={"channel_name": "channel", "video_title": "title"})

    best_eng = video_df[["channel_name", "video_title", "engagement_rate", "view_count"]].sort_values("engagement_rate", ascending=False).head(15).copy()
    best_eng["engagement_rate"] = best_eng["engagement_rate"].round(2)
    best_eng["video_title"] = best_eng["video_title"].apply(short)
    best_eng = best_eng.rename(columns={"channel_name": "channel", "video_title": "title"})

    store_videos = video_df[video_df["has_store_link"]][["channel_name", "video_title", "view_count"]].sort_values("view_count", ascending=False).head(15).copy()
    store_videos["video_title"] = store_videos["video_title"].apply(short)
    store_videos = store_videos.rename(columns={"channel_name": "channel", "video_title": "title"})

    mini_tables = [
        make_card("Top Videos", make_table(top_videos), is_graph=False, md_col=4),
        make_card("Best Engagement", make_table(best_eng), is_graph=False, md_col=4),
        make_card("Top Store-Link Videos", make_table(store_videos), is_graph=False, md_col=4),
    ]

    # -------------------------------------------------
    # COMMENTS LOG TABLE
    # -------------------------------------------------
    rows = []
    if "comments" in video_df.columns:
        for _, r in video_df.iterrows():
            for c in to_list(r["comments"]):
                if not isinstance(c, dict) or not c.get("comment_text"):
                    continue
                rows.append({
                    "channel": r["channel_name"],
                    "video": short(r["video_title"], 45),
                    "comment": short(c.get("comment_text", ""), 120),
                    "likes": c.get("comment_like_count", 0) or 0,
                    "published": pd.to_datetime(c.get("comment_published_at"), utc=True, errors="coerce"),
                })

    comments_df = pd.DataFrame(rows, columns=["channel", "video", "comment", "likes", "published"])
    if comments_df.empty:
        log_table = html.P("No comments loaded yet.", className="text-muted small m-0")
    else:
        comments_df = comments_df.sort_values("published", ascending=False).head(150)
        comments_df["published"] = comments_df["published"].dt.strftime("%Y-%m-%d %H:%M")
        log_table = dbc.Table.from_dataframe(
            comments_df,
            striped=False,
            hover=True,
            responsive=True,
            borderless=True,
            className="text-light text-success small",
            style={"backgroundColor": "transparent", "--bs-table-bg": "transparent", "--bs-table-accent-bg": "transparent", "color": "white", "fontSize": "11px"},
        )

    # Store only light, JSON-safe columns (no jsonb lists / Timestamps)
    store_df = video_df.drop(columns=["comments"], errors="ignore").copy()
    store_df["upload_date"] = store_df["upload_date"].dt.strftime("%Y-%m-%d %H:%M:%S")
    ingested_ts = pd.to_datetime(store_df["_ingested_at"], utc=True, errors="coerce")
    store_df["_ingested_at"] = ingested_ts.dt.strftime("%Y-%m-%d %H:%M:%S")

    latest_str = store_df["_ingested_at"].dropna().iloc[0] if not store_df["_ingested_at"].dropna().empty else "-"
    return f"Updated → {latest_str}", store_df.to_dict("records"), mini_charts, extra_charts, mini_tables, log_table
