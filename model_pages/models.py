# 2026.07.09  16.00
from pathlib import Path
import re

PHOTO_EXT = "jpg"
BASE_DIR = Path(__file__).resolve().parent.parent
ASSETS_DIR = BASE_DIR / "model_assets"

MODELS = [
    {"id": "model1", "name": "Amara Vance",   "niche": "Virtual Fashion & Styling", 
     "caption": "Enjoying a peaceful sunset by the Greek1 coastline.", "reach": "145K"},
    {"id": "model2", "name": "Chloe Thorne",  "niche": "Cyberpunk Lifestyle & Tech",  
     "caption": "Enjoying a peaceful sunset by the Greek2 coastline.", "reach": "320K"},
    {"id": "model3", "name": "Yuki Tanaka",   "niche": "Streetwear & Tokyo Culture",
     "caption": "Enjoying a peaceful sunset by the Greek3 coastline.", "reach": "95K"},
    {"id": "model4", "name": "Sienna Brooks", "niche": "Eco-Travel & Digital Nomad",
     "caption": "Enjoying a peaceful sunset by the Greek4 coastline.", "reach": "210K"},
    {"id": "model5", "name": "Nova Sterling", "niche": "Futuristic Fitness & Health",
     "caption": "Enjoying a peaceful sunset by the Greek5 coastline.", "reach": "185K"},
    {"id": "model6", "name": "Elena Rostova", "niche": "High-End Luxury & Runway",
     "caption": "Enjoying a peaceful sunset by the Greek6 coastline.", "reach": "410K"},
    {"id": "model7", "name": "Maya Lin",      "niche": "Minimalist Design & Art",
     "caption": "Enjoying a peaceful sunset by the Greek7 coastline.", "reach": "125K"},
    {"id": "model8", "name": "Zuri Jones",    "niche": "Afrofuturism & Music Vibe",
     "caption": "Enjoying a peaceful sunset by the Greek8 coastline.", "reach": "300K"},
]

def slugify(name):
    return re.sub(r"[^a-z0-9]+", "-", name.lower()).strip("-")

def _discover_photos(model_id):
    model_dir = ASSETS_DIR / model_id
    if not model_dir.is_dir():
        return []
    files = sorted(model_dir.glob(f"img_*.{PHOTO_EXT}"))
    return [f"/assets/{model_id}/{f.name}" for f in files]

for model in MODELS:
    model["model_slug"] = slugify(model["name"])
    model["photos"] = _discover_photos(model["id"])
    model["cover"] = model["photos"][0] if model["photos"] else None

MODELS_BY_ID = {
    model["id"]: model
    for model in MODELS
}

MODELS_BY_SLUG = {
    model["model_slug"]: model
    for model in MODELS
}
