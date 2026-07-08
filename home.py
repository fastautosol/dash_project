# 2026.07.08  15.00
import dash
from dash import html, dcc
import dash_bootstrap_components as dbc
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.wsgi import WSGIMiddleware

# ----- 0. Model dataset -----
# Photo convention: for each model "modelN", upload 10 photos to
#   /assets/modelN/img_01.jpg ... /assets/modelN/img_10.jpg
# (jpg assumed — change PHOTO_EXT below if you're using png/webp instead.)
PHOTO_COUNT = 10
PHOTO_EXT = "jpg"

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

for _m in MODELS:
    _m["photos"] = [f"/assets/{_m['id']}/img_{i:02d}.{PHOTO_EXT}" for i in range(1, PHOTO_COUNT + 1)]
    _m["cover"] = _m["photos"][0]

MODELS_BY_ID = {m["id"]: m for m in MODELS}

# ----- 1. Initialize Dash (multipage) -----
# Uses its own pages_folder ("home_pages") so it doesn't collide with
# app.py's use_pages=True, which reads from the default "pages" folder.
# NOTE: MODELS / MODELS_BY_ID above must stay defined before this call —
# page discovery happens inside dash.Dash(...), and home_pages/*.py
# import them back via "from home import MODELS_BY_ID".
app = dash.Dash(__name__, use_pages=True, pages_folder="models_pages", suppress_callback_exceptions=True,
    external_stylesheets=[dbc.themes.DARKLY, "https://cdnjs.cloudflare.com/ajax/libs/font-awesome/7.0.1/css/all.min.css"],
    meta_tags=[{"name": "impact-site-verification", "content": "d73bf68a-2290-414c-858c-fa9dadcd2fd9"}],
)

# ----- 2. Global footer (shown on every page: home, model profiles, legal pages) -----
FOOTER = html.Footer([
    html.Hr(style={"borderTop": "1px solid rgba(255,255,255,0.1)", "marginTop": "3rem"}),
    dbc.Row([
        dbc.Col(html.P("© 2026 FastAutoSol Media Group. All rights reserved.", className="text-muted small"), md=6),
        dbc.Col(
            html.Div([
                dcc.Link("Privacy Policy", href="/privacy-policy", className="text-muted small me-3 text-decoration-none"),
                dcc.Link("Terms of Service", href="/terms-of-service", className="text-muted small me-3 text-decoration-none"),
                dcc.Link("Contact Us", href="/contact-us", className="text-muted small text-decoration-none"),
            ], className="text-md-end"),
            md=6,
        ),
    ], className="pb-5 px-4"),
])

app.layout = html.Div([
    dash.page_container,
    FOOTER,
], style={
    "background": "linear-gradient(135deg, #0f0c29, #302b63, #24243e)",
    "minHeight": "100vh",
})

# ----- 3. FastAPI app (ASGI wrapper — required by gunicorn's UvicornWorker) -----
server = FastAPI(title="Dash Home App")

# ----- 4. Health endpoint -----
@server.get("/health")
def health():
    return {"status": "ok"}

# ----- 5. Mount assets, then mount Dash — Dash mount LAST -----
server.mount("/assets", StaticFiles(directory="assets"), name="assets")
server.mount("/", WSGIMiddleware(app.server))
