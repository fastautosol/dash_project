# 2026.07.08  16.00
import dash
from dash import html, dcc
import dash_bootstrap_components as dbc
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.wsgi import WSGIMiddleware
from pathlib import Path

# ----- 0. Model dataset -----
# Photo convention: for each model "modelN", upload photos to
#   /assets/modelN/img_01.jpg, img_02.jpg, ...
# Photo count is discovered automatically from whatever files exist —
# models don't need to have the same number of photos.
# (jpg assumed — change PHOTO_EXT below if you're using png/webp instead.)
PHOTO_EXT = "jpg"
ASSETS_DIR = Path(__file__).resolve().parent / "assets"

MODELS = [
    {"id": "model1", "name": "Amara Vance",   "niche": "Virtual Fashion & Styling",   "reach": "145K"},
    {"id": "model2", "name": "Chloe Thorne",  "niche": "Cyberpunk Lifestyle & Tech",  "reach": "320K"},
    {"id": "model3", "name": "Yuki Tanaka",   "niche": "Streetwear & Tokyo Culture",  "reach": "95K"},
    {"id": "model4", "name": "Sienna Brooks", "niche": "Eco-Travel & Digital Nomad",  "reach": "210K"},
    {"id": "model5", "name": "Nova Sterling", "niche": "Futuristic Fitness & Health", "reach": "185K"},
    {"id": "model6", "name": "Elena Rostova", "niche": "High-End Luxury & Runway",    "reach": "410K"},
    {"id": "model7", "name": "Maya Lin",      "niche": "Minimalist Design & Art",     "reach": "125K"},
    {"id": "model8", "name": "Zuri Jones",    "niche": "Afrofuturism & Music Vibe",   "reach": "300K"},
    {"id": "model9", "name": "Aria Wilde",    "niche": "Alternative Rock & Gaming",   "reach": "240K"},
]


def _discover_photos(model_id):
    model_dir = ASSETS_DIR / model_id
    if not model_dir.is_dir():
        return []
    files = sorted(model_dir.glob(f"img_*.{PHOTO_EXT}"))
    return [f"/assets/{model_id}/{f.name}" for f in files]


for _m in MODELS:
    _m["photos"] = _discover_photos(_m["id"])
    _m["cover"] = _m["photos"][0] if _m["photos"] else None

MODELS_BY_ID = {m["id"]: m for m in MODELS}

# ----- 1. Initialize Dash (multipage) -----
app = dash.Dash(__name__, use_pages=True, pages_folder="models_pages", suppress_callback_exceptions=True,
    external_stylesheets=[dbc.themes.DARKLY, "https://cdnjs.cloudflare.com/ajax/libs/font-awesome/7.0.1/css/all.min.css"],
    meta_tags=[{"name": "impact-site-verification", "content": "d73bf68a-2290-414c-858c-fa9dadcd2fd9"}])

# ----- 2. Global footer (shown on every page: home, model profiles, legal pages) -----
FOOTER = html.Footer([
    html.Hr(style={"borderTop": "1px solid rgba(255,255,255,0.1)", "marginTop": "3rem"}),
    dbc.Row([
        dbc.Col(html.P("© 2026 FastAutoSol Media Group. All rights reserved.", className="text-muted small"), md=6),
        dbc.Col(
            html.Div([
                html.A("Privacy Policy", href="/privacy-policy", target="_blank", rel="noopener noreferrer", className="text-muted small me-3 text-decoration-none"),
                html.A("Terms of Service", href="/terms-of-service", target="_blank", rel="noopener noreferrer", className="text-muted small me-3 text-decoration-none"),
                html.A("Contact Us", href="/contact-us", target="_blank", rel="noopener noreferrer", className="text-muted small text-decoration-none"),
            ], className="text-md-end"),
            md=6,
        ),
    ], className="pb-5 px-4"),
])
# dcc.Link("Privacy Policy", href="/privacy-policy", className="text-muted small me-3 text-decoration-none"),
# dcc.Link("Terms of Service", href="/terms-of-service", className="text-muted small me-3 text-decoration-none"),
# dcc.Link("Contact Us", href="/contact-us", className="text-muted small text-decoration-none"),

app.layout = html.Div([dash.page_container, FOOTER], 
    style={"background": "linear-gradient(135deg, #0f0c29, #302b63, #24243e)", "minHeight": "100vh"})

# ----- 3. FastAPI app (ASGI wrapper — required by gunicorn's UvicornWorker) -----
server = FastAPI(title="Dash Home App")

# ----- 4. Health endpoint -----
@server.get("/health")
def health():
    return {"status": "ok"}

# ----- 5. Mount assets, then mount Dash — Dash mount LAST -----
server.mount("/assets", StaticFiles(directory="assets"), name="assets")
server.mount("/", WSGIMiddleware(app.server))
