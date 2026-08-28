# 2026.08.28  17.00
import dash
from dash import html, dcc, callback, Input, Output, State, MATCH, ALL, ctx, no_update, clientside_callback
import dash_bootstrap_components as dbc
from model_pages.models import MODELS_BY_SLUG

dash.register_page(__name__, path_template="/model/<model_slug>", name="Model Profile")

def layout(model_slug=None, **kwargs):
    model = MODELS_BY_SLUG.get(model_slug)

    if model is None:
        return dbc.Container([
            html.H3("Model not found", className="text-light text-center mt-5"),
            html.Div(dcc.Link("<-- Back to all models", href="/", className="text-info"), className="text-center mt-3"),
        ], className="py-5")

    if model["photos"]:
        thumbnails = [
            dbc.Col(
                html.Img(src=photo, id={ "type": "model-thumb", "model": model_slug, "index": i}, n_clicks=0,
                    style={"width": "100%", "aspectRatio": "3 / 4", "objectFit": "cover", "borderRadius": "10px", "cursor": "pointer"},
                ), xs=6, sm=4, md=3, className="mb-3") for i, photo in enumerate(model["photos"])
        ]
    else:
        thumbnails = [dbc.Col(html.P("No photos uploaded yet", className="text-muted text-center py-5"), width=12)]

    return dbc.Container(
        [
            # Két rejtett komponens az adatok átadásához a JavaScriptnek
            html.Div(id="trigger-page-view", style={"display": "none"}),
            dcc.Store(id="current-model-slug", data=model_slug),

            dcc.Link("<-- Back to all models", href="/", className="text-muted small"),
            html.Div(
                [
                html.H2(model["name"], className="text-light fw-bold mb-1"),
                html.P(model["niche"], className="text-info mb-1"),
                html.Div(    
                    html.A(f"{model['name']}'s Private Club & DMs",  href=model["fanvue"], id="fanvue-link-btn",
                        target="_blank", rel="noopener noreferrer", className="btn btn-info btn-lg fw-bold", style={"width": "30%"}), className="text-center"),                                     
                ], className="text-center py-4",
            ),

            # --- VIDEO PREVIEW STRIP (5 clips, link out to Fanvue) ---
            html.Div(
                [html.A(
                        [html.Img(src=vid, style={"width": "150px", "height": "80px", "objectFit": "cover", "borderRadius": "8px", "display": "block"}),
                        html.I(className="fa-solid fa-play", style={
                            "position": "absolute", "top": "50%", "left": "50%", "transform": "translate(-50%, -50%)",
                            "color": "white", "fontSize": "20px", "textShadow": "0 0 6px rgba(0,0,0,0.8)", "pointerEvents": "none"}),
                        ], href=model["fanvue"], target="_blank", rel="noopener noreferrer", style={"position": "relative", "display": "inline-block", "cursor": "pointer"},
                    )
                    for vid in model["video_thumbs"]
                ],
                className="d-flex justify-content-center gap-2 flex-wrap mb-4",
            ) if model["video_thumbs"] else None,

            dbc.Row(thumbnails, className="g-3"),

            dbc.Modal(
                [dbc.ModalHeader( dbc.ModalTitle(model["name"]), close_button=True),
                dbc.ModalBody(
                    [html.Img(id={ "type": "model-modal-img","model": model_slug}, style={"width": "100%", "maxHeight": "100vh", "objectFit": "contain", "borderRadius": "10px"}),
                    html.Hr(),
                    html.Div([html.I(className="fa-solid fa-heart text-danger me-2"), html.Span("12.5K Likes", className="fw-bold")], className="mb-3"),
                    html.Div([html.Strong(model["name"], className="text-info me-2"), html.Span(id={"type": "model-modal-caption", "model": model_slug})], className="mb-3"),
                    html.Div([dbc.Badge("#travel", color="info", className="me-1"), dbc.Badge("#aiinfluencer", color="secondary", className="me-1"), dbc.Badge("#lifestyle", color="primary")])]),
                ], id={"type": "model-modal", "model": model_slug}, size="xl", is_open=False, centered=True),
        ], fluid=True, className="px-4 py-4")


# --- PIXEL KLIENSOLDALI CALLBACKEK ---
# 1. PageView mérése automatikusan az oldal betöltődésekor
clientside_callback(
    """
    function(slug) {
        if (slug && window.trackModelPage) {
            window.trackModelPage(slug);
        }
        return "";
    }
    """,
    Output("trigger-page-view", "children"),
    Input("current-model-slug", "data")
)

# 2. Fanvue gombkattintás mérése
clientside_callback(
    """
    function(n_clicks, slug) {
        if (n_clicks && window.trackFanvueClick) {
            window.trackFanvueClick(slug);
        }
        return no_update;
    }
    """,
    Output("fanvue-link-btn", "id"), # Technikai kimenet, nem változtat semmit
    Input("fanvue-link-btn", "n_clicks"),
    State("current-model-slug", "data"),
    prevent_initial_call=True
)


# --- EREDETI FOTÓ MODAL CALLBACK ---
@callback(
    Output({"type": "model-modal", "model": MATCH}, "is_open"),
    Output({"type": "model-modal-img", "model": MATCH}, "src"),
    Output({"type": "model-modal-caption", "model": MATCH}, "children"),
    Input({"type": "model-thumb", "model": MATCH, "index": ALL}, "n_clicks"),
    State({"type": "model-thumb", "model": MATCH, "index": ALL}, "id"),
    prevent_initial_call=True,
)
def open_photo_modal(n_clicks_list, thumb_ids):
    triggered = ctx.triggered_id
    if not triggered or not any(n_clicks_list):
        return no_update, no_update, no_update

    model_slug = triggered["model"]
    index = triggered["index"]
    model = MODELS_BY_SLUG[model_slug]
    photo = model["photos"][index]
    caption = model.get("caption", "Enjoying another beautiful day and creating unforgettable memories.")

    return (True, photo, caption)
