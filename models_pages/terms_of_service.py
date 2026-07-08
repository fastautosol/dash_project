import dash
from dash import html
import dash_bootstrap_components as dbc

dash.register_page(__name__, path="/terms-of-service", name="Terms of Service")

SECTION_STYLE = {"marginBottom": "1.75rem"}

layout = dbc.Container([
    html.H2("Terms of Service", className="text-light fw-bold text-center py-4"),
    html.P("Last updated: July 2026", className="text-muted small text-center mb-5"),

    html.Div([
        html.H5("1. Acceptance of Terms", className="text-info"),
        html.P(
            "By accessing or using fastautosol.com (the \"Site\"), operated by FastAutoSol Media Group, "
            "you agree to be bound by these Terms of Service. If you do not agree, please do not use the Site.",
            className="text-light",
        ),
    ], style=SECTION_STYLE),

    html.Div([
        html.H5("2. AI-Generated Content Disclosure", className="text-info"),
        html.P(
            "The creator profiles, images, and personas featured on this Site are fictional, "
            "AI-generated virtual personas and do not depict real individuals. Any resemblance to "
            "actual persons is coincidental. Names, biographies, follower counts, and engagement "
            "figures shown are illustrative and used for demonstration, marketing, or brand-partnership "
            "purposes.",
            className="text-light",
        ),
    ], style=SECTION_STYLE),

    html.Div([
        html.H5("3. Use License", className="text-info"),
        html.P(
            "We grant you a limited, non-exclusive, non-transferable license to access and use the Site "
            "for personal, non-commercial purposes. You may not reproduce, distribute, or create "
            "derivative works from Site content without our prior written consent.",
            className="text-light",
        ),
    ], style=SECTION_STYLE),

    html.Div([
        html.H5("4. Affiliate Links & Advertising", className="text-info"),
        html.P(
            "The Site may contain affiliate links and third-party advertisements. We may earn a "
            "commission when you interact with these links, at no additional cost to you. See our "
            "Privacy Policy for more detail.",
            className="text-light",
        ),
    ], style=SECTION_STYLE),

    html.Div([
        html.H5("5. Third-Party Links", className="text-info"),
        html.P(
            "The Site may link to third-party websites not owned or controlled by us. We are not "
            "responsible for the content, terms, or practices of any third-party site.",
            className="text-light",
        ),
    ], style=SECTION_STYLE),

    html.Div([
        html.H5("6. Disclaimer & Limitation of Liability", className="text-info"),
        html.P(
            "The Site and its content are provided \"as is\" without warranties of any kind, express "
            "or implied. To the fullest extent permitted by law, FastAutoSol Media Group shall not be "
            "liable for any indirect, incidental, or consequential damages arising from your use of "
            "the Site.",
            className="text-light",
        ),
    ], style=SECTION_STYLE),

    html.Div([
        html.H5("7. Changes to These Terms", className="text-info"),
        html.P(
            "We may revise these Terms at any time. Continued use of the Site after changes are "
            "posted constitutes acceptance of the revised Terms.",
            className="text-light",
        ),
    ], style=SECTION_STYLE),

    html.Div([
        html.H5("8. Governing Law", className="text-info"),
        html.P(
            "These Terms are governed by applicable law in the jurisdiction in which FastAutoSol "
            "Media Group operates, without regard to conflict-of-law principles.",
            className="text-light",
        ),
    ], style=SECTION_STYLE),

    html.Div([
        html.H5("9. Contact Us", className="text-info"),
        html.P([
            "Questions about these Terms can be sent to ",
            html.A("fastautosol@gmail.com", href="mailto:fastautosol@gmail.com", className="text-info"),
            ".",
        ], className="text-light"),
    ], style=SECTION_STYLE),

    html.P(
        "This is a general-purpose template and is not legal advice. We recommend having these Terms "
        "reviewed by a qualified professional, particularly regarding AI-content disclosure "
        "requirements and advertising/affiliate compliance in your target markets.",
        className="text-muted small fst-italic mt-5",
    ),

], fluid=True, className="px-4 py-4", style={"maxWidth": "800px", "margin": "0 auto"})
