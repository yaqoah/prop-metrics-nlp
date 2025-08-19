from dash import dcc, html, Input, Output, State, callback
import dash_bootstrap_components as dbc
from src.utils.logger import get_logger

logger = get_logger("semantic explorer")


# Page layout
layout = dbc.Container([
    html.H1("Semantic Explorer", className="mb-4"),

    # Search Interface
    dbc.Row([
        dbc.Col([
            dbc.Card([
                dbc.CardHeader("Search Reviews by Meaning"),
                dbc.CardBody([
                    dbc.InputGroup([
                        dbc.Input(
                            id="search-input",
                            placeholder="Enter your search query (e.g., 'poor customer service', 'fast delivery')",
                            type="text",
                            value="",
                            debounce=True
                        ),
                        dbc.Button("Search", id="search-button", color="primary", n_clicks=0)
                    ], className="mb-3"),

                    dbc.Row([
                        dbc.Col([
                            html.Label("Filter by Prop Firm:"),
                            dcc.Dropdown(
                                id="firm-filter-dropdown",
                                options=[],
                                value=[],
                                multi=True,
                                placeholder="All Firms"
                            )
                        ], width=12, lg=4, className="mb-3 mb-lg-0"),

                        dbc.Col([
                            html.Label("Similarity Threshold:"),
                            dcc.Slider(
                                id="similarity-threshold",
                                min=0.1,
                                max=1.0,
                                step=0.05,
                                value=0.7,
                                marks={i/10: f"{i/10:.1f}" for i in range(1, 11, 2)},
                                tooltip={"placement": "bottom", "always_visible": True}
                            )
                        ], width=12, lg=4, className="mb-3 mb-lg-0"),
                        
                        dbc.Col([
                            html.Label("Max Results:"),
                            dcc.Dropdown(
                                id="max-results",
                                options=[
                                    {"label": "10", "value": 10},
                                    {"label": "25", "value": 25},
                                    {"label": "50", "value": 50},
                                    {"label": "100", "value": 100}
                                ],
                                value=25
                            )
                        ], width=12, lg=4)
                    ])
                ])
            ])
        ], width=12)
    ], className="mb-4"),
    
    dbc.Row([
        dbc.Col([
            dcc.Loading(
                id="loading-search",
                children=[html.Div(id="search-results")],
                type="default"
            )
        ], width=12)
    ])
], fluid=True)


@callback(
    Output("firm-filter-dropdown", "options"),
    Input("firm-filter-dropdown", "id") 
)
def populate_firm_filter(_):
    from app import engine

    try:
        firms = engine.get_firms()
        if firms and firms[0] != "<No firms available>":
            return [{"label": firm, "value": firm} for firm in firms]
    except Exception as e:
        logger.error(f"Error populating firm filter: {e}")
    return []


# Search callback
@callback(
    Output("search-results", "children"),
    [
        Input("search-button", "n_clicks"),
        Input("similarity-threshold", "value"),
        Input("max-results", "value"),
        Input("firm-filter-dropdown", "value")
    ],
    [
        State("search-input", "value")
    ],
    prevent_initial_call=True
)
def perform_search(n_clicks, similarity_threshold, max_results, selected_firms, search_query):
    from app import engine

    if not isinstance(search_query, str) or not search_query.strip():
        return dbc.Alert("Enter a search query and click Search to find similar reviews.", color="info")
    
    try:
        # Perform semantic search
        results = engine.find_similar_reviews(
            query_text=search_query.strip(),
            limit=max_results,
            similarity_threshold=similarity_threshold,
            firm_filter=selected_firms if selected_firms else None
        )
        
        if not results:
            return dbc.Alert(
                f"No reviews found with similarity above {similarity_threshold:.2f}. Try lowering the threshold.",
                color="warning"
            )
        
        # Display results
        result_cards = [html.H4(f"Found {len(results)} similar reviews", className="mb-3")]
        
        # Individual result cards
        for i, result in enumerate(results, 1):
            similarity_percent = result['similarity'] * 100
            sentiment_color = "success" if result['sentiment_score'] > 0.6 else "danger" if result['sentiment_score'] < 0.4 else "warning"
            
            card = dbc.Card([
                dbc.CardBody([
                    dbc.Row([
                        dbc.Col([
                            html.H6(f"#{i} - {result['firm_name']}", className="card-title"),
                            dbc.Badge(f"{similarity_percent:.1f}% similar", color="primary"),
                        ], width=8),
                        dbc.Col([
                            dbc.Badge(f"Sentiment: {result['sentiment_score']:.3f}", color=sentiment_color, className="me-2"),
                            html.Small(f"Date: {result['date_posted']}", className="text-muted")
                        ], width=4, className="text-end")
                    ]),
                    html.Hr(),
                    html.P(result['content'], className="card-text")
                ])
            ], className="mb-2")
            
            result_cards.append(card)
        
        return result_cards
        
    except Exception as e:
        return dbc.Alert(f"Search error: {str(e)}", color="danger")

@callback(
    Output("search-button", "n_clicks"),
    Input("search-input", "n_submit"),
    State("search-button", "n_clicks"),
    prevent_initial_call=True
)
def search_on_enter(n_submit, current_clicks):
    return (current_clicks or 0) + 1