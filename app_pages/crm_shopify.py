# 2026.05.16  18.00
import dash
import pandas as pd
from dash import html, dcc, Input, Output, State, callback
import dash_bootstrap_components as dbc
import plotly.express as px
from sqlalchemy import create_engine
import requests

DB_CONFIG = "postgresql+psycopg://sql_admin:sql_pass@postgresql:5432/n8n"
sql_engine = create_engine(DB_CONFIG, pool_size=5, max_overflow=10, pool_pre_ping=True)

dash.register_page(__name__, icon="fa-solid fa-shopping-bag", name="CRM Shopify", order=3)

N8N_WEBHOOK = "https://n8n.petrosofteu.cloud/webhook/rag-query"

def call_n8n_rag(query: str, top_k: int = 5):
    try:
        payload = {"query": query, "top_k": top_k}
        res = requests.post(N8N_WEBHOOK, json=payload, timeout=30, headers={"Content-Type": "application/json"})
        return res.json()

    except requests.exceptions.Timeout:
        return {"error": "Timeout from n8n"}

    except requests.exceptions.ConnectionError:
        return {"error": "Cannot reach n8n"}


CARD_STYLE = {
    "background": "rgba(255, 255, 255, 0.03)",
    "backdropFilter": "blur(10px)",
    "borderRadius": "15px",
    "border": "1px solid rgba(255, 255, 255, 0.1)",
    "padding": "15px",
    "width": "100%"
}

layout = dbc.Container([

    html.Div([
        html.H2("Shopify CRM Dashboard", className="text-light fw-bold mb-0"),
        html.P(id='crm-rag-metrics-update', className="text-muted small"),
    ], className="mb-3"),

    dcc.Interval(id='crm-rag-refresh', interval=60000, n_intervals=0),
    dcc.Store(id="crm-rag-df-store"),

    html.Div(id="crm-rag-header-metrics", className="mb-3"),

    dbc.Row([
        dbc.Col(
            html.Div(
                dcc.Input(id="crm-rag-query", placeholder="Type query", type="text", className="mb-0", style={"color": "black"}), style=CARD_STYLE), md=9),
        dbc.Col(
            html.Div(
                html.Button("Search", id="crm-rag-btn", n_clicks=0, className="btn btn-warning w-100"), style=CARD_STYLE), md=3),
    ], className="g-3 mb-3"),

    dbc.Row([
        dbc.Col(
            html.Div(
                html.Div(id="crm-rag-results", style={"maxHeight": "200px", "overflowY": "auto"}),  style=CARD_STYLE), md=8),
        dbc.Col(
            html.Div(
                html.Div(id="crm-rag-answer", style={"maxHeight": "200px", "overflowY": "auto"}), style=CARD_STYLE ),  md=4),
    ], className="g-3 mb-3"),

    dbc.Row(id="crm-rag-mini-charts", className="g-3 mb-3"),
    dbc.Row(id="crm-rag-mini-tables", className="g-3 mb-3"),

    html.Div([
        html.H5("CRM Logs", className="mb-2", style={"color": "#f59e0b", "fontWeight": "500"}),
        html.Div( id='crm-rag-log-table', style={"height": "300px", "overflowY": "auto", "fontSize": "12px"})
    ], style=CARD_STYLE)

], fluid=True)


@callback(
    Output("crm-rag-results", "children"),
    Output("crm-rag-answer", "children"),
    Input("crm-rag-btn", "n_clicks"),
    State("crm-rag-query", "value"),
    prevent_initial_call=True
)
def run_rag_search(n_clicks, query):

    if not query:
        return "Enter a query...", ""

    data = call_n8n_rag(query)

    if "error" in data:
        return data["error"], ""

    answer = data.get("answer", "No answer returned.")
    results = data.get("results", [])

    cards = []

    for r in results:
        ticketid = r.get("ticket_id") or r.get("metadata", {}).get("ticket_id", "unknown ticketid")
        intent = r.get("intent") or r.get("metadata", {}).get("intent", "unknown intent")
        text = r.get("text") or r.get("pageContent") or "No content"

        cards.append(
            html.Div([html.Div(f"Id:{ticketid} / {intent}", className="text-info small"),
                html.Div(text[:200] + "...", className="text-light small"),
                html.Hr() ]))

    return cards, answer

@callback(
    Output('crm-rag-metrics-update', 'children'),
    Output('crm-rag-df-store', 'data'),
    Output('crm-rag-header-metrics', 'children'),
    Output('crm-rag-mini-charts', 'children'),
    Output('crm-rag-mini-tables', 'children'),
    Output('crm-rag-log-table', 'children'),
    Input('crm-rag-refresh', 'n_intervals'),
)
def load_data_render(_):

    with sql_engine.connect() as conn:
        try:
            df = pd.read_sql("SELECT * FROM crm_shopify.tickets LIMIT 5000", conn)
        except Exception as e:
            print("SQL ERROR:", e)
            return f"SQL error: {e}", None, None, [], [], None

    if df.empty:
        return "No data", "No data", None, [], [], None

    df = df.copy()

    # -----------------------------
    # DATA CLEANING
    # -----------------------------
    df["created_at"] = pd.to_datetime(df["created_at"], errors="coerce")
    df = df.dropna(subset=["created_at"])
    df["day"] = df["created_at"].dt.date
    df["hour"] = df["created_at"].dt.hour
    df["total_price"] = pd.to_numeric( df["total_price"], errors="coerce").fillna(0)
    for col in ["intent", "financial_status", "customer"]:
        if col in df.columns:
            df[col] = df[col].fillna("").astype(str)

    # -----------------------------
    # CUSTOMER PARSING
    # -----------------------------
    spent_extract = df["customer"].str.extract(r'total_spent["\\\': ]+([0-9.]+)', expand=False)
    df["customer_total_spent"] = pd.to_numeric(spent_extract, errors="coerce").fillna(df["total_price"])
    orders_extract = df["customer"].str.extract( r'(?:orders_count|orders)["\\\': ]+([0-9]+)', expand=False)
    df["customer_orders"] = pd.to_numeric(orders_extract, errors="coerce").fillna(1)
    country_extract = df["customer"].str.extract( r'country["\\\': ]+([A-Za-z]{2,})', expand=False)
    df["customer_country"] = country_extract.fillna("Unknown").replace("", "Unknown")
    email_extract = df["customer"].str.extract(r'([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})', expand=False)
    df["customer_email"] = email_extract.fillna("Anonymous").replace("", "Anonymous")
    anonymous_mask = df["customer_email"] == "Anonymous"
    df.loc[anonymous_mask, "customer_email"] = "guest_" + df.loc[anonymous_mask, "ticket_id"].astype(str)
    df["intent"] = df["intent"].replace("", "unknown")
    df["financial_status"] = df["financial_status"].replace("", "unknown")
    df["processed"] = df["processed"].fillna(False)

    # -----------------------------
    # KPI
    # -----------------------------
    total_revenue = df["total_price"].sum()
    spam_ratio = (df["intent"] == "spam or irrelevant message").mean()
    header_metrics = html.Div([
        html.Span(f"Revenue: ${total_revenue:,.0f}", className="me-3"),
        html.Span(f"Spam Ratio: {spam_ratio:.2%}")
    ], className="text-warning small")

    # -----------------------------
    # CHARTS
    # -----------------------------
    mini_charts = []
    def make_card(title, content, is_graph=True):
        if is_graph:
            content.update_layout( height=200, margin=dict(l=10, r=10, t=15, b=15), paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)", font=dict(color="white"))

        return dbc.Col([
            html.Div([
                html.H6(title, className="mb-1", style={"color": "#f59e0b","fontWeight": "500"}),
                dcc.Graph(figure=content, config={'displayModeBar': False}, style={"height": "200px"}) 
                if is_graph else html.Div(content, style={ "height": "200px", "overflowY": "auto"})
            ], style=CARD_STYLE)
        ], md=4)

    # Tickets Over Time
    ts = (df.groupby("day").size().reset_index(name="tickets").sort_values("day"))
    mini_charts.append(make_card("Tickets Over Time", px.line(ts, x="day", y="tickets", markers=True, template="plotly_dark")))

    # Order Value Distribution
    mini_charts.append(make_card("Order Value Dist", px.histogram( df,x="total_price", nbins=30,template="plotly_dark" )))

    # Intent Breakdown
    intent_df = df["intent"].value_counts().reset_index()
    intent_df.columns = ["intent", "count"]
    mini_charts.append(make_card("Intent Breakdown", px.pie( intent_df, names="intent", values="count", hole=0.4)))

    # Financial Status
    fin = df["financial_status"].value_counts().reset_index()
    fin.columns = ["status", "count"]
    mini_charts.append(make_card("Financial Status", px.bar(fin, x="status", y="count", template="plotly_dark")))

    # Customer Value
    customer_value_df = (df.groupby("customer_email", as_index=False).agg({ "customer_orders": "max","customer_total_spent": "max","total_price": "sum"}))
    customer_value_df = customer_value_df[(customer_value_df["customer_orders"] > 0) | (customer_value_df["customer_total_spent"] > 0)]
    mini_charts.append(make_card("Customer Value", px.scatter(customer_value_df, x="customer_orders", y="customer_total_spent", size="total_price", hover_name="customer_email", template="plotly_dark")))

    # Tickets by Hour
    hour = (df.groupby("hour").size().reset_index(name="tickets"))
    mini_charts.append(make_card("Tickets by Hour", px.line(hour, x="hour", y="tickets", template="plotly_dark")))

    # -----------------------------
    # TABLES
    # -----------------------------
    def make_table(df_table):
        return dbc.Table.from_dataframe(df_table, striped=False, hover=True, responsive=True, borderless=True, className="text-light small",
            style={ "backgroundColor": "transparent", "--bs-table-bg": "transparent", "--bs-table-accent-bg": "transparent", "color": "white"})

    # Top intents
    intent_tbl = df["intent"].value_counts().head(10).reset_index()
    intent_tbl.columns = ["intent", "count"]

    # Top customers
    cust_tbl = (df.groupby("customer_email", as_index=False).agg({"total_price": "sum", "customer_orders": "max", "customer_total_spent": "max"}).sort_values("total_price", ascending=False).head(10))
    cust_tbl["total_price"] = cust_tbl["total_price"].round(2)
    cust_tbl["customer_total_spent"] = cust_tbl["customer_total_spent"].round(2)

    # Top countries
    country_tbl = (df[df["customer_country"] != "Unknown"]["customer_country"].value_counts().head(10).reset_index())
    country_tbl.columns = ["country", "count"]

    mini_tables = [
        make_card("Top Intents", make_table(intent_tbl), is_graph=False),
        make_card("Top Customers", make_table(cust_tbl), is_graph=False),
        make_card("Top Countries", make_table(country_tbl), is_graph=False),
    ]

    # -----------------------------
    # LOG TABLE
    # -----------------------------
    log_cols = ["ticket_id", "customer_email", "total_price", "intent", "financial_status", "created_at"]
    table = dbc.Table.from_dataframe(df[log_cols].tail(100), striped=False, hover=True, responsive=True, borderless=True, className="text-light m-0",
        style={"backgroundColor": "transparent", "--bs-table-bg": "transparent", "--bs-table-accent-bg": "transparent", "color": "white"})

    return f"Updated → {df['created_at'].max()}", df.to_dict("records"), header_metrics, mini_charts, mini_tables, table
