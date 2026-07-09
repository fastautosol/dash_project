# 2026.07.09  18.00

import dash
from dash import  html, dcc, callback, Input, Output, State, MATCH, ALL, ctx, no_update
import dash_bootstrap_components as dbc
from model_pages.models import MODELS_BY_SLUG

dash.register_page(__name__, path_template="/model/<model_slug>", name="Model Profile")

def layout(model_slug=None, **kwargs):

    model = MODELS_BY_SLUG.get(model_slug)

    if model is None:
        return dbc.Container(
            [
                html.H3("Model not found", className="text-light text-center mt-5"),
                html.Div(dcc.Link("<-- Back to all models", href="/", className="text-info"), className="text-center mt-3"),
            ], className="py-5")

    if model["photos"]:

        thumbnails = [
            dbc.Col(
                html.Img(
                    src=photo, id={ "type": "model-thumb", "model": model_slug, "index": i}, n_clicks=0,
                    style={"width": "100%", "aspectRatio": "3 / 4", "objectFit": "cover", "borderRadius": "10px", "cursor": "pointer"},
                ), xs=6, sm=4, md=3, className="mb-3") for i, photo in enumerate(model["photos"])
        ]

    else:

        thumbnails = [dbc.Col(html.P("No photos uploaded yet", className="text-muted text-center py-5"), width=12)]

    return dbc.Container(
        [
            dcc.Link("<-- Back to all models", href="/", className="text-muted small"),
            html.Div(
                [
                    html.H2(model["name"], className="text-light fw-bold mb-1"),
                    html.P( model["niche"], className="text-info mb-1"),
                    html.Span([html.I(className="fa-solid fa-users me-1"), f"{model['reach']} Reach"], className="badge bg-secondary text-light"),
                ], className="text-center py-4",
            ),

            dbc.Row(thumbnails, className="g-3"),

            dbc.Row(thumbnails, className="g-3"),

dbc.Modal(
    [
        dbc.ModalHeader(
            dbc.ModalTitle(model["name"]),
            close_button=True,
        ),

        dbc.ModalBody(
            [
                dbc.Row(

                    [

                        # LEFT SIDE - PHOTO
                        dbc.Col(
                            html.Img(
                                id={
                                    "type": "model-modal-img",
                                    "model": model_slug,
                                },
                                style={
                                    "width": "100%",
                                    "maxHeight": "75vh",
                                    "objectFit": "contain",
                                    "borderRadius": "10px",
                                },
                            ),
                            md=7,
                            className="mb-3 mb-md-0",
                        ),

                        # RIGHT SIDE - STORY
                        dbc.Col(
                            html.Div(
                                [

                                    html.H4(
                                        model["name"],
                                        className="fw-bold text-info mb-3",
                                    ),

                                    html.Div(
                                        id={
                                            "type": "model-modal-caption",
                                            "model": model_slug,
                                        },
                                        className="text-light mb-4",
                                        style={
                                            "lineHeight": "1.8",
                                            "fontSize": "1.05rem",
                                        },
                                    ),

                                    html.Hr(),

                                    html.Div(
                                        [
                                            html.I(
                                                className="fa-solid fa-heart text-danger me-2"
                                            ),
                                            html.Span(
                                                "12.5K Likes",
                                                className="fw-bold",
                                            ),
                                        ],
                                        className="mb-3",
                                    ),

                                    html.Div(
                                        [
                                            dbc.Badge(
                                                "#travel",
                                                color="info",
                                                className="me-1",
                                            ),
                                            dbc.Badge(
                                                "#greece",
                                                color="secondary",
                                                className="me-1",
                                            ),
                                            dbc.Badge(
                                                "#summer",
                                                color="primary",
                                            ),
                                        ]
                                    ),

                                ],
                                style={
                                    "background": "rgba(255,255,255,0.05)",
                                    "padding": "20px",
                                    "borderRadius": "12px",
                                    "height": "100%",
                                },
                            ),
                            md=5,
                        ),

                    ]
                )
            ]
        ),
    ],
    id={
        "type": "model-modal",
        "model": model_slug,
    },
    size="xl",
    is_open=False,
    centered=True,
)


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
