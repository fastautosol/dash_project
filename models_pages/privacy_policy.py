import dash
from dash import html
import dash_bootstrap_components as dbc

dash.register_page(__name__, path="/privacy-policy", name="Privacy Policy")

SECTION_STYLE = {"marginBottom": "1.75rem"}

layout = dbc.Container([
    html.H2("Privacy Policy", className="text-light fw-bold text-center py-4"),
    html.P("Last updated: July 2026", className="text-muted small text-center mb-5"),

    html.Div([
        html.H5("1. Overview", className="text-info"),
        html.P(
            "This Privacy Policy explains how FastAutoSol Media Group (\"we\", \"us\", or \"our\") "
            "collects, uses, and protects information when you visit fastautosol.com (the \"Site\"). "
            "By using the Site, you agree to the practices described in this policy.",
            className="text-light",
        ),
    ], style=SECTION_STYLE),

    html.Div([
        html.H5("2. Information We Collect", className="text-info"),
        html.P(
            "We may collect information you voluntarily provide, such as your name and email address "
            "when you contact us. We may also automatically collect technical information such as your "
            "IP address, browser type, device information, and pages visited, typically through cookies "
            "and similar tracking technologies.",
            className="text-light",
        ),
    ], style=SECTION_STYLE),

    html.Div([
        html.H5("3. How We Use Your Information", className="text-info"),
        html.P(
            "We use collected information to operate and improve the Site, respond to inquiries, "
            "monitor site performance and security, and — where applicable — to measure the "
            "performance of advertising and affiliate marketing campaigns.",
            className="text-light",
        ),
    ], style=SECTION_STYLE),

    html.Div([
        html.H5("4. Cookies & Tracking Technologies", className="text-info"),
        html.P(
            "The Site may use cookies, pixels, or similar technologies from analytics, advertising, "
            "and affiliate marketing providers to understand site usage and attribute referrals. "
            "You can control or disable cookies through your browser settings; doing so may affect "
            "some Site functionality.",
            className="text-light",
        ),
    ], style=SECTION_STYLE),

    html.Div([
        html.H5("5. Affiliate & Advertising Disclosure", className="text-info"),
        html.P(
            "The Site may participate in affiliate marketing programs and may earn commissions from "
            "qualifying purchases or referrals made through links on the Site. This does not affect "
            "the price you pay. The Site may also display third-party advertisements.",
            className="text-light",
        ),
    ], style=SECTION_STYLE),

    html.Div([
        html.H5("6. Third-Party Links", className="text-info"),
        html.P(
            "The Site may contain links to third-party websites. We are not responsible for the "
            "privacy practices or content of those external sites, and we encourage you to review "
            "their respective privacy policies.",
            className="text-light",
        ),
    ], style=SECTION_STYLE),

    html.Div([
        html.H5("7. Data Security", className="text-info"),
        html.P(
            "We take reasonable measures to protect information collected through the Site. However, "
            "no method of transmission or storage is completely secure, and we cannot guarantee "
            "absolute security.",
            className="text-light",
        ),
    ], style=SECTION_STYLE),

    html.Div([
        html.H5("8. Children's Privacy", className="text-info"),
        html.P(
            "The Site is not directed to individuals under the age of 18, and we do not knowingly "
            "collect personal information from children.",
            className="text-light",
        ),
    ], style=SECTION_STYLE),

    html.Div([
        html.H5("9. Changes to This Policy", className="text-info"),
        html.P(
            "We may update this Privacy Policy from time to time. Changes will be posted on this page "
            "with an updated \"Last updated\" date.",
            className="text-light",
        ),
    ], style=SECTION_STYLE),

    html.Div([
        html.H5("10. Contact Us", className="text-info"),
        html.P([
            "If you have questions about this Privacy Policy, please contact us at ",
            html.A("fastautosol@gmail.com", href="mailto:fastautosol@gmail.com", className="text-info"),
            ".",
        ], className="text-light"),
    ], style=SECTION_STYLE),

    html.P(
        "This is a general-purpose template and is not legal advice. We recommend having this policy "
        "reviewed by a qualified professional before relying on it, particularly for jurisdiction-specific "
        "requirements (e.g. GDPR, CCPA) and affiliate/advertising disclosure rules.",
        className="text-muted small fst-italic mt-5",
    ),

], fluid=True, className="px-4 py-4", style={"maxWidth": "800px", "margin": "0 auto"})
