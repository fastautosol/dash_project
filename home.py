# 2026.08.28  18.00
import dash
from dash import html
import dash_bootstrap_components as dbc
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.wsgi import WSGIMiddleware

# ----- Dash App -----
app = dash.Dash(__name__, use_pages=True, pages_folder="model_pages", suppress_callback_exceptions=True,
    external_stylesheets=[dbc.themes.DARKLY, "https://cdnjs.cloudflare.com/ajax/libs/font-awesome/7.0.1/css/all.min.css"],
    meta_tags=[{"name": "impact-site-verification", "content": "9743b3d8-1d27-4867-9c0b-064292508489"}])

# ----- Footer -----
FOOTER = html.Footer([
dbc.Container([
    html.Hr(style={"borderTop": "1px solid rgba(255,255,255,0.1)", "marginTop": "3rem"}),
    dbc.Row([
        dbc.Col(
            html.P("© 2026 FastAutoSol Media Group. All rights reserved", className="text-muted small"), md=6),
        dbc.Col(
            html.Div([
                html.A("Privacy Policy", href="/privacy-policy", target="_blank", rel="noopener noreferrer", className="text-muted small me-3"),
                html.A("Terms of Service", href="/terms-of-service", target="_blank", rel="noopener noreferrer", className="text-muted small me-3"),
                html.A("Contact Us", href="/contact-us", target="_blank", rel="noopener noreferrer", className="text-muted small"),
            ], className="text-md-end"), md=6),
    ], className="pb-2 px-1"),
], fluid=True)
])

# ----- Global Layout -----
app.layout = html.Div(
    [
        html.Div(html.Img(src="/model_assets/fastautosol_logo_small.jpg", style={"width": "120px"}),
        style={"position": "fixed", "top": "10px", "right": "10px", "padding": "5px", "borderRadius": "5px",
        "background": "rgba(255,255,255,0.05)", "backdropFilter": "blur(12px)", "boxShadow": "0 8px 32px rgba(0,0,0,0.3)", "zIndex": "9999"}),
        dash.page_container, FOOTER
    ],
    style={"background": "linear-gradient(135deg, #0f0c29, #302b63, #24243e)", "minHeight": "100vh"})

# ----- FastAPI Wrapper -----
server = FastAPI(title="Dash Home App")

@server.get("/health")
def health():
    return {"status": "ok"}

server.mount("/model_assets", StaticFiles(directory="model_assets"), name="model_assets")
server.mount("/", WSGIMiddleware(app.server))
