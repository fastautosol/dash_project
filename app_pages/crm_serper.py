# 2026.05.16  18.00
import dash
import pandas as pd
from dash import html, dcc, Input, Output, State, callback
import dash_bootstrap_components as dbc
import plotly.express as px
import plotly.graph_objects as go
from sqlalchemy import create_engine, text
from datetime import datetime

# ----- 1. CONFIGURATION -----
DB_CONFIG = "postgresql+psycopg://sql_admin:sql_pass@postgresql:5432/n8n"
sql_engine = create_engine(DB_CONFIG, pool_size=5, max_overflow=10, pool_pre_ping=True, pool_recycle=1800,      
    connect_args={'connect_timeout': 5, 'keepalives': 1, 'keepalives_idle': 30, 'keepalives_interval': 10, 'keepalives_count': 5})

dash.register_page(__name__, icon="fa-solid fa-chart-line", name="CRM Serper", order=4)

# ---- Glass Card ----
CARD_STYLE = { "background": "rgba(255, 255, 255, 0.03)", "backdrop-filter": "blur(10px)",
    "border-radius": "15px", "border": "1px solid rgba(255, 255, 255, 0.1)", "padding": "15px", "width": "100%"}

# -------------------
# LAYOUT
# -------------------
DASH_ID_TAG = 'serper'
layout = dbc.Container([

    html.Div([
        html.H2("CRM Serper (Google) Dashboard", className="text-light fw-bold mb-0"),
    ], className="mb-3"),

    dcc.Interval(id='refresh', interval=60000),
    dcc.Store(id=f"{DASH_ID_TAG}-df-store"),

    # ---- 6 MINI CHART GRID ----
    dbc.Row(id=f"{DASH_ID_TAG}-mini-charts", className="g-3 mb-3"),

    # ---- 3 SMALL KPI TABLE ----
    dbc.Row(id=f"{DASH_ID_TAG}-mini-tables", className="g-3 mb-3"),

    # ---- LOG TABLE ----
    html.Div([
        html.H5("CRM Serper Logs", className="mb-2", style={"color": "#f59e0b", "fontWeight": "500"}),
        html.Div(id=f"{DASH_ID_TAG}-log-table", style={"height": "300px", "overflowY": "auto", "fontSize": "12px"})
    ], style=CARD_STYLE)

], fluid=True)


# -------------------
# DATA CALLBACK
# -------------------
@callback(
    Output(f"{DASH_ID_TAG}-df-store", "data"),
    Output(f"{DASH_ID_TAG}-mini-charts", "children"),
    Output(f"{DASH_ID_TAG}-mini-tables", "children"),
    Output(f"{DASH_ID_TAG}-log-table", "children"),
    Input("refresh", "n_intervals"),
)
def load_data_render(_):

    with sql_engine.connect() as conn:
        df = pd.read_sql("SELECT * FROM serper.companies_email", conn)

    if df.empty:
        return None, [], [], None

    # -------------------
    # CLEAN DATA
    # -------------------
    df.columns = [c.lower() for c in df.columns]

    # normalize UNKNOWN
    df = df.replace("UNKNOWN", pd.NA)

    df["rating"] = pd.to_numeric(df["rating"], errors="coerce")
    df["reviews"] = pd.to_numeric(df["reviews"], errors="coerce")

    df["_ingested_at"] = pd.to_datetime(df["_ingested_at"], errors="coerce")

    df["email"] = df["email"].fillna("").astype(str).str.replace(" ", "", regex=False)   
    df["email_count"] = df["email"].apply(lambda x: len([e for e in x.split(",") if e.strip()]))

    df["has_website"] = df["website"].notna()
    df["has_phone"] = df["phone_number"].notna()
    df = df.sort_values("_ingested_at").drop_duplicates(subset=["cid"], keep="last")
    
    # -------------------
    # MINI CHARTS
    # -------------------
    mini_charts = []
    def make_card(title, content, is_graph=True, md_col=4):
        if is_graph:
            content.update_layout(height=220, margin=dict(l=10, r=10, t=25, b=10), paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)", font=dict(color="white"))
        return dbc.Col([
            html.Div([
                html.H6(title, className="mb-2", style={"color": "#f59e0b", "fontWeight": "500"}),
                dcc.Graph(figure=content, config={"displayModeBar": False}, style={"height": "250px"}) 
                if is_graph else html.Div(content, style={"height": "250px", "overflowY": "auto"})
            ], style=CARD_STYLE)
        ], md=md_col)

    # 1 Top Categories
    #df["category"] = df["category"].astype(str).str[:12]  #.str.slice(0, 30)   
    cat_df = df["category"].value_counts().head(15).reset_index()
    cat_df["category"] = cat_df["category"].apply(lambda x: str(x)[:12] + "..." if len(str(x)) > 12 else str(x))   
    cat_df.columns = ["category", "count"]
    mini_charts.append(make_card("Top Categories", px.bar(cat_df, x="category", y="count", template="plotly_dark")))

    # 2 Top Cities
    city_df = df["city"].value_counts().head(15).reset_index()
    city_df.columns = ["city", "count"]
    mini_charts.append(make_card("Top Cities", px.bar(city_df, x="city", y="count", template="plotly_dark")))

    # 3 Ratings Distribution
    rating_df = df[df["rating"].notna()]
    mini_charts.append(make_card("Ratings Distribution", px.histogram(rating_df, x="rating", nbins=20, template="plotly_dark")))

    # 4 Review Leaders
    review_df = df[["name", "reviews"]].dropna().sort_values("reviews", ascending=False).head(15)
    review_df["name"] = review_df["name"].apply(lambda x: str(x)[:12] + "..." if len(str(x)) > 12 else str(x))  
    mini_charts.append(make_card("Most Reviewed", px.bar(review_df, x="name", y="reviews", template="plotly_dark")))

    # 5 Email Availability
    email_stats = pd.DataFrame({"type": ["Has Email", "No Email"], "count": [df["email"].astype(bool).sum(), (~df["email"].astype(bool)).sum()]})
    fig = px.pie(email_stats, names="type", values="count", hole=0.4, template="plotly_dark")
    fig.update_traces(textinfo="percent+value", textposition="inside")
    mini_charts.append(make_card("Email Coverage", fig))

    # 6 Avg Rating by Category
    avg_rating_df = df.groupby("category")["rating"].mean().dropna().sort_values(ascending=False).head(15).reset_index() 
    fig = px.bar(avg_rating_df, x="category", y="rating", template="plotly_dark")
    fig.update_layout(xaxis=dict(
        tickmode="array", tickvals=avg_rating_df["category"],
        ticktext=[label[:15] + "…" if len(label) > 15 else label for label in avg_rating_df["category"]]))
    mini_charts.append(make_card("Avg Rating by Category", fig))
    
    # -------------------
    # MINI TABLES
    # -------------------
    def make_table(df_table):

        return dbc.Table.from_dataframe(df_table, striped=False, hover=True, responsive=True, borderless=True, className="text-light small",
            style={"backgroundColor": "transparent", "--bs-table-bg": "transparent", "--bs-table-accent-bg": "transparent", "color": "white"})

    top_rated_df = df[["name", "city", "rating", "reviews"]].dropna(subset=["rating"]).sort_values(["rating", "reviews"], ascending=False).head(25)
    top_rated_df["name"] = top_rated_df["name"].apply(lambda x: str(x)[:30] + "..." if len(str(x)) > 30 else str(x))
    top_rated_df["rating"] = top_rated_df["rating"].apply(lambda x: "{:.1f}".format(x))
    
    most_reviews_df = df[["name", "city", "reviews"]].dropna(subset=["reviews"]).sort_values("reviews", ascending=False).head(25)
    most_reviews_df["name"] = most_reviews_df["name"].apply(lambda x: str(x)[:30] + "..." if len(str(x)) > 30 else str(x))  
    
    mini_tables = [
        make_card("Top Rated", make_table(top_rated_df), is_graph=False, md_col=6),
        make_card("Most Reviews", make_table(most_reviews_df), is_graph=False, md_col=6),
    ]

    # -------------------
    # LOG TABLE
    # -------------------
    log_cols = ["name", "city", "category", "rating", "reviews", "email", "phone_number", "website", "_ingested_at"]
    log_df = df[log_cols].sort_values("_ingested_at", ascending=False).head(100) 
    log_df["_ingested_at"] = pd.to_datetime(log_df["_ingested_at"]).dt.strftime('%Y-%m-%d %H:%M:%S')
    log_df = log_df.map(lambda x: str(x)[:40] if isinstance(x, str) else x)
    
    table = dbc.Table.from_dataframe(log_df, striped=False, hover=True, responsive=True, borderless=True, className="text-light small",
        style={"backgroundColor": "transparent", "--bs-table-bg": "transparent", "--bs-table-accent-bg": "transparent", "color": "white", "fontSize": "11px"})

    return df.to_dict("records"), mini_charts, mini_tables, table
