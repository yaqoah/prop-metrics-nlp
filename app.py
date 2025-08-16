import dash
from dash import dcc, html, Input, Output, callback
from dashboard.pages import main_dashboard, semantic_explorer, competitive_landscape
import dash_bootstrap_components as dbc
from dashboard.components.sidebar import create_sidebar
from src.utils.logger import get_logger
from src.analytics.engine import Engine

logger = get_logger("app")
logger.info("Application starting up...")

logger.info("Initializing the main analytics engine. This may take a moment...")
engine = Engine()
logger.info("Analytics engine initialized successfully.")

app = dash.Dash(__name__, 
                external_stylesheets=[dbc.themes.BOOTSTRAP],
                suppress_callback_exceptions=True)

app.title = "Analytics Dashboard"

# Main layout with sidebar and content area
app.layout = dbc.Container([
    dcc.Location(id="url", refresh=False),
    dbc.Row([
        dbc.Col([
            create_sidebar()
        ], width=2, className="bg-light"),
        dbc.Col([
            html.Div(id="page-content")
        ], width=10)
    ], className="h-100")
], fluid=True, className="h-100")

# Page routing callback
@app.callback(
    Output("page-content", "children"),
    Input("url", "pathname")
)
def display_page(pathname):
    if pathname == "/semantic-explorer":
        return semantic_explorer.layout
    elif pathname == "/competitive-landscape":
        return competitive_landscape.layout
    else: 
        return main_dashboard.layout

if __name__ == '__main__':
    app.run(debug=True)