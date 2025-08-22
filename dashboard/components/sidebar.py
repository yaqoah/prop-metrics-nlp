import dash_bootstrap_components as dbc
from dash import html

def create_sidebar():
    return html.Div([
        html.H3("Analytics", className="text-center mb-4"),
        dbc.Nav([
            dbc.NavLink([
                html.I(className="fas fa-chart-line me-2"),
                "Main Dashboard"
            ], href="/", active="exact", className="mb-2"),
            dbc.NavLink([
                html.I(className="fas fa-search me-2"),
                "Semantic Explorer"
            ], href="/semantic-explorer", active="exact", className="mb-2"),
            dbc.NavLink([
                html.I(className="fas fa-table me-2"),
                "Competitive Landscape"
            ], href="/competitive-landscape", active="exact", className="mb-2"),
        ], vertical=True, pills=True)
    ], className="p-3")