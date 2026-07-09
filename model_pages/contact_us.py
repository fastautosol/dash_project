import dash
from dash import html
import dash_bootstrap_components as dbc

dash.register_page(__name__, path="/contact-us", name="Contact Us")

layout = dbc.Container([
    html.H2("Contact Us", className="text-light fw-bold text-center py-4"),

    html.Div([
        html.P(
            "For brand partnerships, sponsorship inquiries, affiliate program questions, or general "
            "support, reach out to us at the email below and we'll get back to you as soon as possible.",
            className="text-light text-center",
        ),

        html.Div([
            html.I(className="fa-solid fa-envelope me-2 text-info"),
            html.A("fastautosol@gmail.com", href="mailto:fastautosol@gmail.com", className="text-info fs-5"),
        ], className="text-center my-4"),

        html.P(
            "FastAutoSol Media Group",
            className="text-muted small text-center",
        ),
    ], style={"maxWidth": "600px", "margin": "0 auto"}, className="py-4"),

], fluid=True, className="px-4 py-5")
