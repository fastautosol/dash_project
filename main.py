# 2026.6.03  10.00
import dash
from dash import html, dcc
import dash_bootstrap_components as dbc
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.wsgi import WSGIMiddleware

import apis.crm_shopify_api as crm_shopify_api
import apis.kraken_api as kraken
import apis.lufthansa_api as lufthansa_api
import apis.serper_places_api as serper_places
import apis.serper_places_api_email as serper_places_email
import apis.youtube_api as youtube_api

# ----- 1. Initalize Dash -----
app = dash.Dash(__name__, use_pages=True, suppress_callback_exceptions=True, #assets_folder="assets", assets_url_path="/assets",
    external_stylesheets=[dbc.themes.DARKLY, "https://cdnjs.cloudflare.com/ajax/libs/font-awesome/7.0.1/css/all.min.css"],
    #external_scripts=["https://unpkg.com/lightweight-charts/dist/lightweight-charts.standalone.production.js"]
    external_scripts=["https://unpkg.com/lightweight-charts@5.2.0/dist/lightweight-charts.standalone.production.js"])

# ----- 2. NOW IMPORT YOUR PAGES -----
#from pages import home, bybit_ccharts, bybit_lcharts, crm_serper, crm_shopify, lufthansa_charts, meta_charts, mexc_charts, youtube_charts, vps_sysinfo

# ----- 3. FASTAPI WRAPPER -----
server = FastAPI(title="Dash Main App")

# ----- 4. API ROUTERS -----
server.include_router(crm_shopify_api.router, prefix="/api/crm_shopify",   tags=["CRM Shopify"])
server.include_router(bybit.router,         prefix="/api/bybit",         tags=["Bybit"])
server.include_router(cryptonews_api.router,         prefix="/api/cryptonews",         tags=["Crypto News"])
server.include_router(meta_api.router,         prefix="/api/meta",         tags=["Facebook Insta Leads"])
server.include_router(kraken.router,        prefix="/api/kraken",        tags=["Kraken"])
server.include_router(lufthansa_api.router,     prefix="/api/lufthansa",     tags=["Lufthansa"])
server.include_router(serper_places.router, prefix="/api/serper",        tags=["Serper Places"])
server.include_router(serper_places_email.router, prefix="/api/serper_email",        tags=["Serper Places Email"])
server.include_router(movies_api.router,    prefix="/api/movies",    tags=["OMDB Movies"])
server.include_router(youtube_api.router,       prefix="/api/youtube",       tags=["Youtube Single/Multi Channel"])

# ----- 5. HEALTH ENDPOINT -----
@server.get("/health")
def health():
    return {"status": "ok"}

# ----- 6. Mount Dash to FastAPI -----
#server.mount("/assets", StaticFiles(directory="assets"), name="assets")
server.mount("/", WSGIMiddleware(app.server))

# ----- 7. SIDEBAR & LAYOUT  (Your Modern Layout) -----
SIDEBAR_STYLE = {
    "position": "fixed", "top": "15px", "left": "15px", "bottom": "15px",
    "width": "220px", "padding": "2rem 1rem",
    "background": "rgba(255, 255, 255, 0.1)",
    "backdrop-filter": "blur(15px)",
    "border-radius": "20px",
    "border": "1px solid rgba(255, 255, 255, 0.1)",
    "box-shadow": "0 8px 32px 0 rgba(0, 0, 0, 0.5)"
}

sidebar = html.Div([
    html.H5("FASTAUTOSOL CLOUD", className="text-center mb-4", style={"letterSpacing": "2px", "color": "ivory"}),
    
    html.Div([
        html.Div([
            html.I(className="fas fa-user-circle fa-2x text-info"),
            html.Div([
                html.P("Admin Console", className="mb-0", style={"fontSize": "14px", "fontWeight": "bold"}),
                html.P("8GB - 2vCPU", className="text-muted small mb-0")
            ], className="ms-2")
        ], className="d-flex align-items-center p-2", style={"background": "rgba(0,0,0,0.3)", "borderRadius": "15px"})
    ], className="mb-4"),

    html.Hr(style={"color": "rgba(255,255,255,0.3)"}),

    dbc.Nav([
        dbc.NavLink([
            html.Div([
                html.I(className=f"{page.get('icon', 'fa-solid fa-chart-line')} me-2"),
                html.Span(page["name"]),
            ], className="d-flex align-items-center")
        ], href=page["relative_path"], active="exact", className="mb-2 py-2 ps-2 rounded-3 text-light")
        for page in dash.page_registry.values()
    ], vertical=True, pills=True),
], style=SIDEBAR_STYLE)

app.layout = html.Div([
    sidebar,
    html.Div(dash.page_container, style={
        "marginLeft": "250px", "padding": "2rem",
        "background": "linear-gradient(135deg, #0f0c29, #302b63, #24243e)",
        "minHeight": "100vh"
    })
])
