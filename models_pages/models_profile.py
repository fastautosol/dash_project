# 2026.07.08  16.00
import dash
from dash import html, dcc, callback, Input, Output, State, MATCH, ALL, ctx, no_update
import dash_bootstrap_components as dbc
from models import MODELS_BY_ID

dash.register_page(__name__, path_template="/model/<model_id>", name="Model Profile")

def layout(model_id=None, **kwargs):

    model = MODELS_BY_ID.get(model_id)

    if model is None:
        return dbc.Container([
            html.H3(
                "Model not found",
                className="text-light text-center mt-5",
            ),

            html.Div(
                dcc.Link(
                    "← Back to all models",
                    href="/",
                    className="text-info",
                ),
                className="text-center mt-3",
            ),
        ],
        className="py-5")

    if model["photos"]:

        thumbnails = [
            dbc.Col(
                html.Img(
                    src=photo,
                    id={
                        "type": "model-thumb",
                        "model": model_id,
                        "index": i,
                    },
                    n_clicks=0,
                    style={
                        "width": "100%",
                        "aspectRatio": "3 / 4",
                        "objectFit": "cover",
                        "borderRadius": "10px",
                        "cursor": "pointer",
                        "border": "1px solid rgba(255,255,255,0.1)",
                    },
                ),
                xs=6,
                sm=4,
                md=3,
                className="mb-3",
            )
            for i, photo in enumerate(model["photos"])
        ]

    else:

        thumbnails = [
            dbc.Col(
                html.P(
                    "No photos uploaded yet.",
                    className="text-muted text-center py-5",
                ),
                width=12,
            )
        ]

    return dbc.Container([

        dcc.Link(
            "← Back to all models",
            href="/",
            className="text-muted small",
        ),

        html.Div([
            html.H2(
                model["name"],
                className="text-light fw-bold mb-1",
            ),

            html.P(
                model["niche"],
                className="text-info mb-1",
            ),

            html.Span(
                [
                    html.I(
                        className="fa-solid fa-users me-1"
                    ),
                    f"{model['reach']} Reach",
                ],
                className="badge bg-secondary text-light",
            ),
        ],
        className="text-center py-4"),

        dbc.Row(
            thumbnails,
            className="g-3",
        ),

        dbc.Modal([
            dbc.ModalHeader(
                dbc.ModalTitle(model["name"]),
                close_button=True,
            ),

            dbc.ModalBody(
                html.Img(
                    id={
                        "type": "model-modal-img",
                        "model": model_id,
                    },
                    style={
                        "width": "100%",
                        "maxHeight": "100vh",
                        "objectFit": "contain",
                        "borderRadius": "10px",
                        "display": "block",
                        "margin": "0 auto",
                    },
                )
            ),
        ],
        id={
            "type": "model-modal",
            "model": model_id,
        },
        size="lg",
        is_open=False,
        centered=True),

    ],
    fluid=True,
    className="px-4 py-4")


@callback(
    Output(
        {"type": "model-modal", "model": MATCH},
        "is_open",
    ),
    Output(
        {"type": "model-modal-img", "model": MATCH},
        "src",
    ),
    Input(
        {"type": "model-thumb", "model": MATCH, "index": ALL},
        "n_clicks",
    ),
    State(
        {"type": "model-thumb", "model": MATCH, "index": ALL},
        "id",
    ),
    prevent_initial_call=True,
)
def open_photo_modal(n_clicks_list, thumb_ids):

    triggered = ctx.triggered_id

    if not triggered or not any(n_clicks_list):
        return no_update, no_update

    model_id = triggered["model"]
    index = triggered["index"]

    photo = MODELS_BY_ID[model_id]["photos"][index]

    return True, photo

