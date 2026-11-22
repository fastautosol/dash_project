# 2026.11.22 - TMDB movies dashboard
import os
import json
import logging
import dash
import numpy as np
import pandas as pd
from dash import html, dcc, Input, Output, callback
import dash_bootstrap_components as dbc
import plotly.express as px
from sqlalchemy import create_engine

logger = logging.getLogger(__name__)

# -------------------------------------------------
# CONFIG
# -------------------------------------------------
PG_USER = os.getenv("POSTGRES_USER", "sql_admin")
PG_PASS = os.getenv("POSTGRES_PASSWORD", "sql_pass")
PG_HOST = os.getenv("POSTGRES_HOST", "postgresql")
PG_PORT = os.getenv("POSTGRES_PORT", "5432")
PG_DB = os.getenv("POSTGRES_DB", "n8n")

DB_CONFIG = f"postgresql+psycopg://{PG_USER}:{PG_PASS}@{PG_HOST}:{PG_PORT}/{PG_DB}"
TABLE = "bronze.tmdb_movies_raw"

SELECT_COLS = ["movie_id", "title", "budget", "revenue", "release_date", "vote_average", "vote_count", "popularity", "runtime", "original_language", "genres", "director", "user_reviews", "updated_at"]

sql_engine = create_engine(
    DB_CONFIG,
    pool_size=5,
    max_overflow=10,
    pool_pre_ping=True,
    pool_recycle=1800,
    connect_args={"connect_timeout": 5, "keepalives": 1, "keepalives_idle": 30, "keepalives_interval": 10, "keepalives_count": 5})

dash.register_page(__name__, icon="fa-solid fa-film", name="TMDB Charts", order=5)

# -------------------------------------------------
# STYLE & LAYOUT
# -------------------------------------------------
CARD_STYLE = {
    "background": "rgba(255, 255, 255, 0.03)",
    "backdrop-filter": "blur(10px)",
    "border-radius": "15px",
    "border": "1px solid rgba(255, 255, 255, 0.1)",
    "padding": "15px",
    "width": "100%"}

ACCENT = "#01b4e4"
DASH_ID_TAG = "tmdb"

layout = dbc.Container([
    html.Div([
        html.H2("TMDB Movies Dashboard", className="text-light fw-bold mb-0"),
        html.P(id=f"{DASH_ID_TAG}-metrics-update", className="text-muted small"),
    ], className="mb-3"),

    dcc.Interval(id=f"{DASH_ID_TAG}-refresh", interval=60000),
    dbc.Row(id=f"{DASH_ID_TAG}-mini-charts", className="g-3 mb-3"),
    dbc.Row(id=f"{DASH_ID_TAG}-extra-charts", className="g-3 mb-3"),
    dbc.Row(id=f"{DASH_ID_TAG}-mini-tables", className="g-3 mb-3"),

    html.Div([
        html.H5("Latest User Reviews", className="mb-2", style={"color": ACCENT, "fontWeight": "500"}),
        html.Div(id=f"{DASH_ID_TAG}-log-table", style={"height": "350px", "overflowY": "auto", "fontSize": "12px"}),
    ], style=CARD_STYLE),
], fluid=True)


# -------------------------------------------------
# HELPERS
# -------------------------------------------------
def make_card(title, content, is_graph=True, md_col=4):
    if is_graph:
        content.update_layout(height=220, margin=dict(l=10, r=10, t=30, b=10), paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)", font=dict(color="white"), template="plotly_dark")
    return dbc.Col([
        html.Div([
            html.H6(title, className="mb-2", style={"color": ACCENT, "fontWeight": "500"}),
            dcc.Graph(figure=content, config={"displayModeBar": False}, style={"height": "240px"})
            if is_graph else html.Div(content, style={"height": "240px", "overflowY": "auto"}),
        ], style=CARD_STYLE)
    ], md=md_col)


def make_table(df_table):
    return dbc.Table.from_dataframe(df_table, striped=False, hover=True, responsive=True, borderless=True, className="text-light small",
        style={"backgroundColor": "transparent", "--bs-table-bg": "transparent", "--bs-table-accent-bg": "transparent", "color": "white"})

def short(x, n=45):
    x = str(x)
    return x[:n] + "..." if len(x) > n else x

def to_list(x):
    if isinstance(x, list):
        return x
    if isinstance(x, str):
        try:
            v = json.loads(x)
            return v if isinstance(v, list) else []
        except Exception:
            return []
    return []


def first_or(x, default="Unknown"):
    lst = to_list(x)
    if not lst:
        return default
    v = lst[0]
    if isinstance(v, dict):
        v = v.get("name", default)
    return str(v) if v else default


# -------------------------------------------------
# CALLBACK
# -------------------------------------------------
@callback(
    Output(f"{DASH_ID_TAG}-metrics-update", "children"),
    Output(f"{DASH_ID_TAG}-mini-charts", "children"),
    Output(f"{DASH_ID_TAG}-extra-charts", "children"),
    Output(f"{DASH_ID_TAG}-mini-tables", "children"),
    Output(f"{DASH_ID_TAG}-log-table", "children"),
    Input(f"{DASH_ID_TAG}-refresh", "n_intervals"))

def load_tmdb_data(_):
    query = f"SELECT {', '.join(SELECT_COLS)} FROM {TABLE} ORDER BY updated_at DESC LIMIT 50000"
    try:
        with sql_engine.connect() as conn:
            df = pd.read_sql(query, conn)
    except Exception as exc:
        logger.exception("TMDB DB lekérdezési hiba: %s", exc)
        return f"DB connection error ({PG_HOST}:{PG_PORT})", [], [], [], None

    if df.empty:
        return "No data", [], [], [], None

    df.columns = [c.lower() for c in df.columns]
    for c in ["budget", "revenue", "vote_average", "vote_count", "popularity", "runtime"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    for c in ["budget", "revenue", "runtime"]:
        df[c] = df[c].replace(0, np.nan)

    df["release_date"] = pd.to_datetime(df["release_date"], errors="coerce")
    df["updated_at"] = pd.to_datetime(df["updated_at"], utc=True, errors="coerce")
    df["year"] = df["release_date"].dt.year
    df["decade"] = (df["year"] // 10 * 10).astype("Int64")

    df = df.sort_values("updated_at", ascending=False).drop_duplicates(subset=["movie_id"], keep="first").copy()

    df["genre"] = df["genres"].apply(first_or)
    df["director"] = df["director"].fillna("Unknown").replace("", "Unknown")
    df["profit"] = df["revenue"] - df["budget"]

    # MINI CHARTS
    mini_charts = []

    g_cnt = df["genre"].value_counts().head(12).reset_index()
    g_cnt.columns = ["genre", "count"]
    fig1 = px.bar(g_cnt, x="genre", y="count", template="plotly_dark")
    fig1.update_xaxes(tickangle=-25)
    mini_charts.append(make_card("Movies by Genre", fig1))

    dec = df.dropna(subset=["decade"]).groupby("decade").size().reset_index(name="count")
    dec["decade"] = dec["decade"].astype(int).astype(str) + "s"
    fig2 = px.bar(dec, x="decade", y="count", template="plotly_dark")
    mini_charts.append(make_card("Releases per Decade", fig2))

    g_rate = df.groupby("genre").agg(avg_rating=("vote_average", "mean"), n=("movie_id", "count")).reset_index()
    g_rate = g_rate[g_rate["n"] >= 2].sort_values("avg_rating", ascending=False).head(12)
    g_rate["avg_rating"] = g_rate["avg_rating"].round(2)
    fig3 = px.bar(g_rate, x="genre", y="avg_rating", template="plotly_dark")
    fig3.update_xaxes(tickangle=-25)
    mini_charts.append(make_card("Avg Rating by Genre", fig3))

    br = df.dropna(subset=["budget", "revenue"])
    fig4 = px.scatter(br, x="budget", y="revenue", hover_name="title", color="genre", log_x=True, log_y=True, template="plotly_dark")
    fig4.update_layout(showlegend=False)
    mini_charts.append(make_card("Budget vs Revenue", fig4))

    fig5 = px.histogram(df.dropna(subset=["runtime"]), x="runtime", nbins=20, template="plotly_dark")
    mini_charts.append(make_card("Runtime Distribution (min)", fig5))

    lang = df["original_language"].fillna("??").value_counts().head(8).reset_index()
    lang.columns = ["language", "count"]
    fig6 = px.pie(lang, names="language", values="count", hole=0.4, template="plotly_dark")
    mini_charts.append(make_card("Original Language", fig6))

    # EXTRA CHARTS
    extra_charts = []

    fig7 = px.histogram(df.dropna(subset=["vote_average"]), x="vote_average", nbins=20, template="plotly_dark")
    extra_charts.append(make_card("Vote Average Distribution", fig7))

    fig8 = px.scatter(df, x="vote_count", y="popularity", hover_name="title", color="genre", log_x=True, log_y=True, template="plotly_dark")
    fig8.update_layout(showlegend=False)
    extra_charts.append(make_card("Popularity vs Vote Count", fig8))

    top_dir = df[df["director"] != "Unknown"]["director"].value_counts().head(10).reset_index()
    top_dir.columns = ["director", "count"]
    fig9 = px.bar(top_dir, x="director", y="count", template="plotly_dark")
    fig9.update_xaxes(tickangle=-25)
    extra_charts.append(make_card("Top Directors (movies)", fig9))

    # MINI TABLES
    popular = df.sort_values("popularity", ascending=False).head(15)[["title", "year", "genre", "popularity", "vote_count"]].copy()
    popular["title"] = popular["title"].apply(short)
    popular["popularity"] = popular["popularity"].round(1)
    popular["year"] = popular["year"].astype("Int64")

    best = df[df["vote_count"] >= 100].sort_values("vote_average", ascending=False).head(15)[["title", "year", "genre", "vote_average", "vote_count"]].copy()
    best["title"] = best["title"].apply(short)
    best["year"] = best["year"].astype("Int64")

    profit = df.dropna(subset=["profit"]).sort_values("profit", ascending=False).head(15)[["title", "year", "budget", "revenue", "profit"]].copy()
    profit["title"] = profit["title"].apply(short)
    profit["year"] = profit["year"].astype("Int64")
    for c in ["budget", "revenue", "profit"]:
        profit[c] = (profit[c] / 1e6).round(1)
    profit = profit.rename(columns={"budget": "budget M$", "revenue": "revenue M$", "profit": "profit M$"})

    mini_tables = [
        make_card("Most Popular", make_table(popular), is_graph=False),
        make_card("Best Rated (100+ votes)", make_table(best), is_graph=False),
        make_card("Biggest Profit", make_table(profit), is_graph=False),
    ]

    # REVIEW LOG TABLE
    rows = []
    for _, r in df.iterrows():
        for rev in to_list(r["user_reviews"]):
            if isinstance(rev, dict):
                rev = rev.get("content") or rev.get("review") or ""
            rev = str(rev).strip().replace("**", "").replace("_", "")
            if rev:
                rows.append({"movie": short(r["title"], 35), "year": r["year"], "genre": r["genre"], "review": short(rev, 180)})
        if len(rows) >= 150:
            break

    reviews_df = pd.DataFrame(rows[:150], columns=["movie", "year", "genre", "review"])
    if not reviews_df.empty:
        reviews_df["year"] = reviews_df["year"].astype("Int64")

    log_table = dbc.Table.from_dataframe(reviews_df, striped=False, hover=True, responsive=True, borderless=True, className="text-light small",
        style={"backgroundColor": "transparent", "--bs-table-bg": "transparent", "--bs-table-accent-bg": "transparent", "color": "white", "fontSize": "11px"})

    last = df["updated_at"].max()
    stamp = last.strftime("%Y-%m-%d %H:%M:%S") if pd.notna(last) else "n/a"

    return f"Updated → {stamp}  |  {len(df)} movies", mini_charts, extra_charts, mini_tables, log_table
