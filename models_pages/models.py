# 2026.07.08  18.00
from pathlib import Path

PHOTO_EXT = "jpg"
BASE_DIR = Path(__file__).resolve().parent.parent
ASSETS_DIR = BASE_DIR / "assets"

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


for model in MODELS:
    model["photos"] = _discover_photos(model["id"])
    model["cover"] = model["photos"][0] if model["photos"] else None


MODELS_BY_ID = {
    model["id"]: model
    for model in MODELS
}

MODELS_BY_SLUG = {
    model["slug"]: model
    for model in MODELS
}
