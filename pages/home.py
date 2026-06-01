# 2026.05.20  18.00
import dash
from dash import html, dcc, callback, Output, Input
from datetime import datetime
import dash_bootstrap_components as dbc

dash.register_page(__name__, path="/", icon="fa-solid fa-home", name="Home", order=0)

CARD_STYLE = {
    "background": "rgba(255, 255, 255, 0.03)",
    "backdropFilter": "blur(10px)",
    "borderRadius": "15px",
    "border": "1px solid rgba(255, 255, 255, 0.1)",
    "padding": "15px",
    "width": "100%",
    "height": "100%",
}

CARDS = [
    {"icon": "fa-solid fa-chart-line",  "title": "Analytics",     "img": "https://placehold.co/400x160/1a1a2e/7eb3ff?text=Analytics",    "desc": "Track key metrics and trends across your trading systems in real time."},
    {"icon": "fa-solid fa-robot",       "title": "Bots",          "img": "https://placehold.co/400x160/1a1a2e/7eb3ff?text=Bots",          "desc": "Manage and monitor your automated trading bots and their performance."},
    {"icon": "fa-solid fa-database",    "title": "Database",      "img": "https://placehold.co/400x160/1a1a2e/7eb3ff?text=Database",      "desc": "Inspect PostgreSQL tables, run queries and review stored data."},
    {"icon": "fa-solid fa-webhook",     "title": "Webhooks",      "img": "https://placehold.co/400x160/1a1a2e/7eb3ff?text=Webhooks",      "desc": "Incoming signal endpoints, logs and webhook health status."},
    {"icon": "fa-solid fa-server",      "title": "VPS SysInfo",   "img": "https://placehold.co/400x160/1a1a2e/7eb3ff?text=SysInfo",       "desc": "Live CPU, memory and disk stats for the VPS host and containers."},
    {"icon": "fa-solid fa-gear",        "title": "Settings",      "img": "https://placehold.co/400x160/1a1a2e/7eb3ff?text=Settings",      "desc": "Configure API keys, notifications and application preferences."},
]


def make_card(c):
    return dbc.Card([
        dbc.CardHeader([
            html.I(className=f"{c['icon']} me-2 text-info"),
            html.Span(c["title"], className="fw-semibold text-light"),
        ], style={"background": "rgba(255,255,255,0.05)", "border": "none"}),
        html.Img(src=c["img"], style={"width": "100%", "height": "160px", "objectFit": "cover"}),
        dbc.CardBody(
            html.P(c["desc"], className="text-muted small m-0"),
        ),
    ], style=CARD_STYLE)


layout = dbc.Container([

    # ── Top-centre logo ───
    dbc.Row(
    dbc.Col(
        html.Img(src="/assets/fastautosol_header.jpg", style={"height": "70%", "width": "auto"}), 
        className="text-center"), className="mb-4",
    ),

    # ── 2 × 3 info cards ───
    dbc.Row([
        dbc.Col(make_card(CARDS[i]), width=6, className="g-4 mb-4") 
        for i in range(6)
    ]),

], fluid=True, className="py-3")
