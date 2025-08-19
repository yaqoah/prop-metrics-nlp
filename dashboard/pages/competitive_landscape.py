import dash
from dash import dcc, html, Input, Output, State, callback
import dash_bootstrap_components as dbc
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
from src.utils.logger import get_logger

logger = get_logger("competitive landscape")

def get_sentiment_label(score):
    if score is None or pd.isna(score):
        return "N/A"
    if score > 0.75:
        return "Highly Positive"
    if score > 0.64:
        return "Positive"
    if score > 0.53:
        return "Slightly Positive / Neutral"
    return "Negative"

layout = dbc.Container([
    html.Div(id="competitive-landscape-content", children=[
        html.H1("Competitive Landscape", className="mb-4"),
    dbc.Row([
        dbc.Col([
            dbc.Card([
                dbc.CardBody([
                    dbc.Row([
                        dbc.Col([
                            html.Label("Select Firms:"),
                            dcc.Dropdown(
                                id="firm-selector",
                                multi=True,
                                placeholder="Select firms to compare..."
                            )
                        ], width=12, lg=5),
                        dbc.Col([
                            html.Label("Time Period (days):"),
                            dcc.Dropdown(
                                id="time-period",
                                options=[
                                    {"label": "30 days", "value": 30},
                                    {"label": "90 days", "value": 90},
                                    {"label": "180 days", "value": 180},
                                    {"label": "365 days", "value": 365}
                                ],
                                value=90
                            )
                        ], width=6, lg=4),
                        dbc.Col([
                             dbc.Checkbox(
                                id="hide-topics-checkbox",
                                label="Hide Topic Labels",
                                value=False,
                            )
                        ], width=6, lg=3, className="d-flex align-items-end")
                    ])
                ])
            ])
        ], width=12)
    ], className="mb-4"),
    

    dbc.Row([
        dbc.Col([
            dcc.Loading(
                dcc.Graph(id="competitive-heatmap", style={"height": "600px"})
            )
        ], width=12)
    ], className="mb-4"),

    ]),
    
    # Modal for detailed reviews
    dbc.Modal([
        dbc.ModalHeader(dbc.ModalTitle(id="modal-title")),
        dbc.ModalBody(id="modal-body"),
        dbc.ModalFooter(
            dbc.Button("Close", id="close-modal", className="ms-auto", n_clicks=0)
        )
    ], id="detail-modal", is_open=False, size="xl"),
    
    dcc.Store(id="click-data-store")
], fluid=True)

# Populate firm selector on page load
@callback(
    Output("firm-selector", "options"),
    Input("competitive-landscape-content", "id")
)
def populate_firm_options(_):
    from app import engine
    logger.info("Populating firm selector...")

    try:
        firms = engine.get_firms()
        
        if not firms or firms == ["<No firms available>"]:
            logger.warning("No firms found to populate dropdown.")
            return []
            
        logger.info(f"Found {len(firms)} firms.")
        return [{"label": firm, "value": firm} for firm in firms]
    except Exception as e:
        logger.error(f"Error populating firm selector: {e}")
        return [{"label": "Error loading firms", "value": ""}]

# Update heatmap based on selections
@callback(
    Output("competitive-heatmap", "figure"),
    [Input("firm-selector", "value"),
     Input("time-period", "value"),
     Input("hide-topics-checkbox", "value")]
)
def update_heatmap(selected_firms, time_period, hide_topics):
    from app import engine

    if not selected_firms:
        fig = go.Figure()
        fig.update_layout(
            height=600,
            xaxis_visible=False,
            yaxis_visible=False,
            annotations=[dict(text="Select one or more firms to begin analysis", xref="paper", yref="paper", showarrow=False, font=dict(size=20, color="grey"))]
        )
        return fig
    
    try:
        df = engine.get_topic_sentiment(firms=selected_firms, days=time_period)
        
        if df.empty:
            fig = go.Figure()
            fig.update_layout(title="No data available for the selected firms and time period", height=600, xaxis_visible=False, yaxis_visible=False)
            return fig
        
        fig = px.imshow(df, color_continuous_scale='RdYlGn', color_continuous_midpoint=0.5, aspect="auto", text_auto='.3f')
        

        labels_df = df.map(get_sentiment_label)
        
        fig.update_traces(
            customdata=labels_df,
            hovertemplate="<b>%{y}</b><br>" + "Topic: %{x}<br>" + "Sentiment: %{customdata}<br>" + "Score: %{z:.3f}" + "<extra></extra>"
        )
        
        fig.update_layout(
            title=f"Competitive Sentiment Landscape ({time_period} days)",
            xaxis_title="Topics", yaxis_title="Firms", height=600,
            xaxis={'side': 'bottom'}, font=dict(size=10)
        )
        
        fig.update_xaxes(tickangle=45)
        if hide_topics:
            fig.update_xaxes(showticklabels=False, title_text=None)
        
        return fig
        
    except Exception as e:
        logger.error(f"Failed to generate heatmap: {e}")
        fig = go.Figure()
        fig.update_layout(
            title_text=f"An error occurred: {str(e)}",
            height=600,
            xaxis_visible=False,
            yaxis_visible=False
        )
        return fig

# Handle heatmap cell clicks
@callback(
    [Output("detail-modal", "is_open"),
     Output("modal-title", "children"),
     Output("modal-body", "children")],
    [Input("competitive-heatmap", "clickData"),
     Input("close-modal", "n_clicks")],
    [State("detail-modal", "is_open")]
)
def handle_cell_click(clickData, close_clicks, is_open):
    from app import engine

    ctx = dash.callback_context
    
    if not ctx.triggered:
        return False, "", ""
    
    trigger_id = ctx.triggered[0]['prop_id'].split('.')[0]
    
    if trigger_id == "close-modal":
        return False, "", ""
    
    if trigger_id == "competitive-heatmap" and clickData:
        try:
            point = clickData['points'][0]
            firm_name = point['y']
            topic_name = point['x']
            sentiment_score = point['z']
            
            sentiment_label = get_sentiment_label(sentiment_score)
            modal_title = f"{firm_name} - {topic_name} (Sentiment: {sentiment_label})"

            candidate_reviews = engine.find_similar_reviews(
                query_text=topic_name,
                firm_filter=[firm_name],
                limit=50, 
                similarity_threshold=0.3
            )
            
            if not candidate_reviews:
                modal_body = dbc.Alert("No detailed reviews available for this combination.", color="info")
                return True, modal_title, modal_body

            candidate_reviews.sort(key=lambda r: abs(r['sentiment_score'] - sentiment_score))

            display_reviews = candidate_reviews[:10]

            review_cards = []
            for review in display_reviews:
                sentiment_color = "success" if review['sentiment_score'] > 0.6 else "danger" if review['sentiment_score'] < 0.4 else "warning"
                card = dbc.Card(dbc.CardBody(dbc.Row([
                    dbc.Col([
                        dbc.Badge(f"Sentiment: {review['sentiment_score']:.3f}", color=sentiment_color),
                        html.Br(),
                        html.Small(f"Similarity: {review['similarity']:.3f}", className="text-muted")
                    ], width=12, lg=2, className="text-center mb-2 mb-lg-0"),
                    dbc.Col([
                        html.P(review['content'], className="mb-1"),
                        html.Small(f"Date: {review['date_posted']}", className="text-muted")
                    ], width=12, lg=10)
                ])), className="mb-2")
                review_cards.append(card)
            
            modal_body = html.Div([
                html.H6(f"Showing {len(display_reviews)} most representative reviews:"),
                html.Div(review_cards, style={"max-height": "400px", "overflow-y": "auto"})
            ])
            
            return True, modal_title, modal_body
            
        except Exception as e:
            logger.error(f"Error handling cell click: {e}")
            return True, "Error", dbc.Alert(f"Error loading details: {str(e)}", color="danger")
    
    return is_open, "", ""