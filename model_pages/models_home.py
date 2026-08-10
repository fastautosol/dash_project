# 2026.07.08  18.00
import dash
from dash import html, dcc
import dash_bootstrap_components as dbc
from model_pages.models import MODELS

dash.register_page(__name__, path="/", name="Home")

CARD_STYLE = {
    "background": "rgba(255,255,255,0.03)",
    "backdropFilter": "blur(10px)",
    "borderRadius": "15px",
    "border": "1px solid rgba(255,255,255,0.1)",
    "padding": "15px",
    "width": "100%",
    "height": "100%",
    "display": "flex",
    "flexDirection": "column",
}


def make_influencer_card(model):

    if model["cover"]:
        cover_el = html.Img(
            src=model["cover"],
            style={"width": "100%", "aspectRatio": "3 / 4", "objectFit": "cover", "borderRadius": "10px 10px 0 0"})
    else:
        cover_el = html.Div(
            html.I(className="fa-solid fa-image fa-2x text-muted"),
            style={"width": "100%", "aspectRatio": "3 / 4", "borderRadius": "10px 10px 0 0", "background": "rgba(255,255,255,0.05)",
                "display": "flex", "alignItems": "center", "justifyContent": "center"})

    return dbc.Card([
        dcc.Link(cover_el, href=f"/model/{model['model_slug']}"),

        dbc.CardBody([
            dcc.Link(
                html.H5(model["name"], className="fw-bold text-light mb-1"),
                href=f"/model/{model['model_slug']}", className="text-decoration-none"),

            html.P(model["niche"], className="text-info small mb-3"),

            html.Div([
                html.Span(
                    [html.I(className="fa-solid fa-users me-1"), f"{model['reach']} Reach"], className="badge bg-secondary text-light")
            ], className="d-flex justify-content-between align-items-center mt-auto"),
        ],
        className="px-2 pt-3 pb-1"),
    ],
    style=CARD_STYLE)

layout = dbc.Container([

    dbc.Row(
        dbc.Col([
            html.H2("FastAutoSol Creator Network", className="text-light fw-bold"),
            html.P("Empowering brand sponsorships through high-engagement virtual AI models and creators", className="text-muted lead"),
            html.A("Chat Privately with our models and exclusive content on Fanvue", href="https://www.fanvue.com/fastmedia.aimodels", target="_blank",
                rel="noopener noreferrer", className="text-info mb-1 d-block"),  
        ], className="text-center py-4"), className="mb-4"),

    dbc.Row(
        [
            dbc.Col(make_influencer_card(model), xs=12, sm=6, lg=3, className="mb-4 d-flex align-items-stretch")
            for model in MODELS
        ], className="g-4"),

], fluid=True, className="px-4")
