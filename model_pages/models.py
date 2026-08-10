# 2026.07.24  12.00
from pathlib import Path
import re

PHOTO_EXT = "jpg"
BASE_DIR = Path(__file__).resolve().parent.parent
ASSETS_DIR = BASE_DIR / "model_assets"

MODELS = [
    {"id": "model1", "name": "Amara Vance",   "niche": "Virtual Fashion & Styling", "fanvue": "https://www.fanvue.com/fastmedia.aimodels/media/fvml-11",
     "caption": "Enjoying a peaceful sunset by the Greek1 coastline.", "reach": "145K"},
    {"id": "model2", "name": "Sofia Vega",  "niche": "Spanish Lifestyle & Fashion", "fanvue": "https://www.fanvue.com/fastmedia.aimodels/media/fvml-14",
     "caption": "Enjoying a peaceful sunset by the Spanish coastline.", "reach": "320K"}, 
    {"id": "model3", "name": "Sienna Brooks", "niche": "Streetwear & City Culture", "fanvue": "https://www.fanvue.com/fastmedia.aimodels/media/fvml-12",
     "caption": "Enjoying a peaceful sunset by the Greek3 coastline.", "reach": "95K"},
    {"id": "model4", "name": "Zoe Wilder", "niche": "Eco-Travel & Digital Nomad", "fanvue": "https://www.fanvue.com/fastmedia.aimodels/media/fvml-13",
     "caption": "Enjoying a peaceful sunset by the Greek4 coastline.", "reach": "210K"},
    {"id": "model5", "name": "Ruby Valentine", "niche": "Streetwear & City Culture", "fanvue": "Private Photoset Soon",
     "caption": "Enjoying a peaceful sunset by the Greek3 coastline.", "reach": "95K"},  
    {"id": "model6", "name": "Maya Lin", "niche": "Afrofuturism & Music Vibe", "fanvue": "https://www.fanvue.com/fastmedia.aimodels/media/fvml-16",
     "caption": "Enjoying a peaceful sunset by the Greek8 coastline.", "reach": "300K"},
    {"id": "model7", "name": "Chloe Thorne", "niche": "Modern Lifestyle & Tech", "fanvue": "https://www.fanvue.com/fastmedia.aimodels/media/fvml-15",
     "caption": "Enjoying a peaceful sunset by the Greek2 coastline.", "reach": "320K"},
    {"id": "model8", "name": "Aria Luna",  "niche": "Minimalist Design & Art", "fanvue": "Private Photoset Soon",
     "caption": "Enjoying a peaceful sunset by the Greek7 coastline.", "reach": "125K"},
    
]

def slugify(name):
    return re.sub(r"[^a-z0-9]+", "-", name.lower()).strip("-")

def _discover_photos(model_id):
    model_dir = ASSETS_DIR / model_id
    if not model_dir.is_dir():
        return []
    files = sorted(model_dir.glob(f"img_*.{PHOTO_EXT}"))
    return [f"/model_assets/{model_id}/{f.name}" for f in files]

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
