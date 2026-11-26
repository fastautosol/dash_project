# 2026.11.26  14.00
import dash
from dash import html, dcc
import dash_bootstrap_components as dbc
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.wsgi import WSGIMiddleware

import apis.lufthansa_api as lufthansa_api
import apis.serper_places_api as serper_places
import apis.youtube_api as youtube_api
import apis.tmdb_api as tmdb_api
import apis.opensky_api as opensky_api
import apis.crucix_api as crucix_api
import apis.twitter_api as twitter_api
import apis.eia_api as eia_api
import apis.ingest_azureblob_api as azureblob_api
import apis.ingest_mediaads_api as ingest_mediaads_api
import apis.ingest_webshops_api as ingest_webshops_api
import apis.finance_api as finance_api
import apis.finance_silver_api as finance_silver_api
import apis.ingest_crm_api as ingest_crm_api
import apis.kkv_api as kkv_api
import apis.shopify_rag_api as shopify_rag_api

# ----- 1. Initialize Dash -----
app = dash.Dash(__name__, use_pages=True, pages_folder="app_pages", assets_folder="app_assets", suppress_callback_exceptions=True,
    external_stylesheets=[dbc.themes.DARKLY, "https://cdnjs.cloudflare.com/ajax/libs/font-awesome/7.3.1/css/all.min.css"],
    external_scripts=["https://unpkg.com/lightweight-charts@5.2.1/dist/lightweight-charts.standalone.production.js"])

# ----- 2. SIDEBAR & LAYOUT — must be defined BEFORE the WSGI mount -----
SIDEBAR_STYLE = {
    "position": "fixed", "top": "15px", "left": "15px", "bottom": "15px", "width": "220px", "padding": "2rem 1rem",
    "background": "rgba(255, 255, 255, 0.1)", "backdrop-filter": "blur(15px)",
    "border-radius": "20px", "border": "1px solid rgba(255, 255, 255, 0.1)", "box-shadow": "0 8px 32px 0 rgba(0, 0, 0, 0.5)"}

sidebar = html.Div([
    
    html.H5("FASTAUTOSOL CLOUD", className="text-center mb-4", style={"letterSpacing": "2px", "color": "ivory"}),

    html.Div([
        html.Div([
            html.I(className="fas fa-user-circle fa-2x text-info"),
            html.Div([
                html.P("Admin Console", className="mb-0", style={"fontSize": "14px", "fontWeight": "bold"}),
                html.P("8GB - 2vCPU", className="text-muted small mb-0"),
            ], className="ms-2"),
        ], className="d-flex align-items-center p-2", style={"background": "rgba(0,0,0,0.3)", "borderRadius": "15px"}),
    ], className="mb-4"),

    html.Hr(style={"color": "rgba(255,255,255,0.3)"}),

    dbc.Nav([
        dbc.NavLink([
            html.Div([html.I(className=f"{page.get('icon', 'fa-solid fa-chart-line')} me-2"), html.Span(page["name"])], className="d-flex align-items-center")
        ], href=page["relative_path"], active="exact", className="mb-2 py-2 ps-2 rounded-3 text-light")
        for page in dash.page_registry.values()
    ], vertical=True, pills=True),
], style=SIDEBAR_STYLE)

app.layout = html.Div([
    sidebar,
    html.Div(dash.page_container, style={
        "marginLeft": "250px", "padding": "2rem",
        "background": "linear-gradient(135deg, #0f0c29, #302b63, #24243e)",
        "minHeight": "100vh",
    }),
])


server = FastAPI(title="Dash Demo App")

# ----- 4. API routers -----
server.include_router(azureblob_api.router,    prefix="/api/rest",           tags=["Ingest Azure Blob"])
server.include_router(ingest_crm_api.router,   prefix="/api/crm",            tags=["Ingest CRM API"])
server.include_router(finance_api.router,      prefix="/api/finance",        tags=["Finance API"])
server.include_router(ingest_mediaads_api.router, prefix="/api/mediaads",    tags=["Ingest MediaAds API"])
server.include_router(ingest_webshops_api.router, prefix="/api/webshops",    tags=["Ingest WebShops API"])
server.include_router(shopify_rag_api.router,   prefix="/api/shopify",       tags=["Shopify RAG"])
server.include_router(kkv_api.router,           prefix="/api/kkv",           tags=["KKV API"])

server.include_router(lufthansa_api.router,     prefix="/api/lufthansa",     tags=["Lufthansa"])
server.include_router(serper_places.router,     prefix="/api/serper",        tags=["Serper Places"])
server.include_router(youtube_api.router,       prefix="/api/youtube",       tags=["Youtube Single"])
server.include_router(tmdb_api.router,          prefix="/api/tmdb",          tags=["TMDB Movies"])
server.include_router(opensky_api.router,       prefix="/api/opensky",       tags=["Opensky"])
server.include_router(crucix_api.router,        prefix="/api/crucix",        tags=["Crucix"])
server.include_router(twitter_api.router,       prefix="/api/twitter",       tags=["Twitter"])
server.include_router(eia_api.router,           prefix="/api/eia",           tags=["EIA Energy"])

# ----- 5. Health endpoint -----
@server.get("/health")
def health():
    return {"status": "ok"}

# ----- 6. Mount Dash, after layout is set and assets are mounted -----
server.mount("/app_assets", StaticFiles(directory="app_assets"), name="app_assets")
server.mount("/", WSGIMiddleware(app.server))
