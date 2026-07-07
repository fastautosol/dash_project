# 2026.07.07  18.00
import dash
from dash import html, dcc
import dash_bootstrap_components as dbc

app = dash.Dash(__name__, external_stylesheets=[dbc.themes.DARKLY],
meta_tags=[{"name": "impact-site-verification", "content": "d73bf68a-2290-414c-858c-fa9dadcd2fd9"}])
server = app.server

CARD_STYLE = {
    "background": "rgba(255, 255, 255, 0.03)",
    "backdropFilter": "blur(10px)",
    "borderRadius": "15px",
    "border": "1px solid rgba(255, 255, 255, 0.1)",
    "padding": "15px",
    "width": "100%",
    "height": "100%",
    "display": "flex",
    "flexDirection": "column",
}

# 12 AI Models/Influencers Profile Mock Data
AI_GIRLS = [
    {"name": "Amara Vance",    "niche": "Virtual Fashion & Styling",  "img": "https://unsplash.com", "reach": "145K"},
    {"name": "Chloe Thorne",   "niche": "Cyberpunk Lifestyle & Tech", "img": "https://unsplash.com", "reach": "320K"},
    {"name": "Yuki Tanaka",    "niche": "Streetwear & Tokyo Culture", "img": "https://unsplash.com", "reach": "95K"},
    {"name": "Sienna Brooks",  "niche": "Eco-Travel & Digital Nomad", "img": "https://unsplash.com", "reach": "210K"},
    {"name": "Nova Sterling",  "niche": "Futuristic Fitness & Health","img": "https://unsplash.com", "reach": "185K"},
    {"name": "Elena Rostova",  "niche": "High-End Luxury & Runway",   "img": "https://unsplash.com", "reach": "410K"},
    {"name": "Maya Lin",       "niche": "Minimalist Design & Art",    "img": "https://unsplash.com", "reach": "125K"},
    {"name": "Zuri Jones",     "niche": "Afrofuturism & Music Vibe",  "img": "https://unsplash.com", "reach": "300K"},
    {"name": "Aria Wilde",     "niche": "Alternative Rock & Gaming",  "img": "https://unsplash.com", "reach": "240K"},
    {"name": "Leila Kincaid",  "niche": "Coastal Living & Wellness",  "img": "https://unsplash.com", "reach": "165K"},
    {"name": "Iris Dubois",    "niche": "Parisian Beauty & Skincare", "img": "https://unsplash.com", "reach": "190K"},
    {"name": "Tessa Vance",    "niche": "Skateboarding & Street Art", "img": "https://unsplash.com", "reach": "115K"}
]

def make_influencer_card(girl):
    return dbc.Card([
        html.Img(
            src=girl["img"], 
            style={"width": "100%", "height": "280px", "objectFit": "cover", "borderRadius": "10px 10px 0 0"}
        ),
        dbc.CardBody([
            html.H5(girl["name"], className="fw-bold text-light mb-1"),
            html.P(girl["niche"], className="text-info small mb-3"),
            html.Div([
                html.Span([html.I(className="fa-solid fa-users me-1"), f"{girl['reach']} Reach"], className="badge bg-secondary text-light")
            ], className="d-flex justify-content-between align-items-center mt-auto")
        ], className="px-2 pt-3 pb-1")
    ], style=CARD_STYLE)

layout = dbc.Container([

    # ── Header & Logo Section ───
    dbc.Row(
        dbc.Col([
            html.Img(src="/assets/fastautosol_header.jpg", style={"maxHeight": "120px", "width": "auto"}, className="mb-3"), 
            html.H2("FastAutoSol Creator Network", className="text-light fw-bold"),
            html.P("Empowering brand sponsorships through high-engagement virtual AI models and creators.", className="text-muted lead")
        ], className="text-center py-4"), 
        className="mb-4"
    ),

    # ── 3 Column × 4 Row Grid ───
    # row-cols-md-3 automatically splits 12 items into 4 rows of 3 columns each on desktop view
    dbc.Row([
        dbc.Col(
            make_influencer_card(girl), 
            xs=12, sm=6, md=4, 
            className="mb-4 d-flex align-items-stretch"
        ) for girl in AI_GIRLS
    ], className="g-4"),

    # ── Compliance Footer ───
    html.Hr(style={"borderTop": "1px solid rgba(255,255,255,0.1)", "marginTop": "5rem"}),
    dbc.Row([
        dbc.Col(html.P("© 2026 FastAutoSol Media Group. All rights reserved.", className="text-muted small"), md=6),
        dbc.Col(
            html.Div([
                html.A("Privacy Policy", href="#", className="text-muted small me-3 text-decoration-none"),
                html.A("Terms of Service", href="#", className="text-muted small me-3 text-decoration-none"),
                html.A("Contact Us", href="#", className="text-muted small text-decoration-none")
            ], className="text-md-end"), 
            md=6
        )
    ], className="pb-5")

], fluid=True, className="px-4")

@server.route('/health')
def health_check():
    return "OK", 200

if __name__ == "__main__":
    app.run_server(host="0.0.0.0", port=8000)
