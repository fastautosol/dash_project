# 2026.07.08  18.00
import dash
from dash import html
import dash_bootstrap_components as dbc
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.wsgi import WSGIMiddleware

#from model_pages.models import MODELS, MODELS_BY_ID

# ----- Dash App -----
app = dash.Dash(__name__, use_pages=True, pages_folder="model_pages", suppress_callback_exceptions=True,
    external_stylesheets=[dbc.themes.DARKLY, "https://cdnjs.cloudflare.com/ajax/libs/font-awesome/7.0.1/css/all.min.css"],
    meta_tags=[{"name": "impact-site-verification", "content": "894da505-1ec1-4124-8ec6-bfd26d04f2d4"}])

# ----- Footer -----
FOOTER = html.Footer([
    html.Hr(
        style={
            "borderTop": "1px solid rgba(255,255,255,0.1)",
            "marginTop": "3rem",
        }
    ),

    dbc.Row([
        dbc.Col(
            html.P(
                "© 2026 FastAutoSol Media Group. All rights reserved.",
                className="text-muted small",
            ),
            md=6,
        ),

        dbc.Col(
            html.Div([
                html.A(
                    "Privacy Policy",
                    href="/privacy-policy",
                    target="_blank",
                    rel="noopener noreferrer",
                    className="text-muted small me-3 text-decoration-none",
                ),

                html.A(
                    "Terms of Service",
                    href="/terms-of-service",
                    target="_blank",
                    rel="noopener noreferrer",
                    className="text-muted small me-3 text-decoration-none",
                ),

                html.A(
                    "Contact Us",
                    href="/contact-us",
                    target="_blank",
                    rel="noopener noreferrer",
                    className="text-muted small text-decoration-none",
                ),
            ],
            className="text-md-end"),
            md=6,
        ),
    ],
    className="pb-5 px-4"),
])

# ----- Global Layout -----

app.layout = html.Div(
    [dash.page_container, FOOTER],
    style={"background": "linear-gradient(135deg, #0f0c29, #302b63, #24243e)", "minHeight": "100vh"})

# ----- FastAPI Wrapper -----
server = FastAPI(title="Dash Home App")

@server.get("/health")
def health():
    return {"status": "ok"}

server.mount("/model_assets", StaticFiles(directory="model_assets"), name="model_assets")

server.mount("/", WSGIMiddleware(app.server))
