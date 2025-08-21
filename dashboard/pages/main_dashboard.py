from dash import dcc, html, Input, Output, State, callback, no_update, ctx, ALL
import dash_bootstrap_components as dbc
import plotly.express as px
import plotly.graph_objects as go
import pycountry
import pandas as pd
import re
from src.utils.logger import get_logger

logger = get_logger("main dashboard")

# --- Helper Functions ---
def get_sentiment_label(score):
    if score is None: return "N/A"
    if score > 0.75: return "Highly Positive"
    if score > 0.64: return "Positive"
    if score > 0.53: return "Slightly Positive / Neutral"
    return "Negative"

# --- Page Layout ---
layout = dbc.Container([
    html.H1("Main Dashboard", className="mb-4"),

    html.Div(id="main-dashboard-content", children=[
        # Controls Row
        dbc.Row([
            dbc.Col([
                dbc.Card([
                    dbc.CardBody([
                        dbc.Row([
                            dbc.Col([
                                html.Label("Select Prop Trading Firm:", className="fw-bold"),
                                dcc.Dropdown(id="firm-dropdown", value="ALL", clearable=False)
                            ], width=8),
                            dbc.Col([
                                html.Label("Time Period:", className="fw-bold"),
                                dcc.Dropdown(
                                    id="time-period-dropdown",
                                    options=[
                                        {"label": "Last 30 days", "value": 30},
                                        {"label": "Last 90 days", "value": 90},
                                        {"label": "Last 180 days", "value": 180},
                                        {"label": "All time", "value": -1}
                                    ],
                                    value=30,
                                    clearable=False
                                )
                            ], width=4)
                        ])
                    ])
                ])
            ], width=12)
        ], className="mb-4"),

        # KPI Cards Row
        dbc.Row(id="kpi-cards", className="mb-4"),
        
        # Charts Row
        dbc.Row([
            dbc.Col(dbc.Card([dbc.CardHeader("Global Sentiment Map"), dbc.CardBody(dcc.Graph(id="geo-sentiment-map"))]), width=6),
            # MODIFICATION: CardHeader now has an ID for dynamic updates
            dbc.Col(dbc.Card([
                dbc.CardHeader(id="voice-feed-header"), 
                dbc.CardBody(html.Div(id="voice-feed", style={"height": "400px", "overflow-y": "auto"}))
            ]), width=6)
        ], className="mb-4"),
        
        # Topic Explorer Row
        dbc.Row([
            dbc.Col(dbc.Card([
                dbc.CardHeader("Interactive Topic Explorer"),
                dbc.CardBody([
                    html.Label("Filter by Sentiment Score:"),
                    dcc.RangeSlider(id="sentiment-range-slider", min=0, max=1, step=0.1, marks={i/10: f"{i/10:.1f}" for i in range(0, 11, 2)}, value=[0, 1], className="mb-3"),
                    dcc.Graph(id="topic-bubble-chart")
                ])
            ]), width=12)
        ]),
    ]),
    
    # NEW: Store for holding the selected country state
    dcc.Store(id='country-filter-store', data=None),
    
    dcc.Interval(id="interval-component", interval=5*60*1000, n_intervals=0) # Set to 5 minutes
], fluid=True)


# --- Callbacks ---

# Callback 1: Populate the firm dropdown on load
@callback(
    Output("firm-dropdown", "options"),
    Input("main-dashboard-content", "id")
)
def populate_firm_dropdown(_):
    from app import engine
    try:
        firms = engine.get_firms()
        options = [{"label": f"All Prop Firms ({len(firms)} total)", "value": "ALL"}]
        options.extend([{"label": firm, "value": firm} for firm in firms])
        return options
    except Exception as e:
        logger.error(f"Error populating firm dropdown: {e}")
        return [{"label": "Error loading firms", "value": "ERROR"}]

# Callback 2: Update the hidden country store when the map is clicked or reset button is clicked
@callback(
    Output('country-filter-store', 'data'),
    Input({'type': 'reset-country-filter-button', 'index': ALL}, 'n_clicks'),
    Input('geo-sentiment-map', 'clickData'),
    prevent_initial_call=True
)
def update_country_filter(reset_clicks, clickData):
    triggered_id = ctx.triggered_id
    
    # Check if the reset button was the trigger
    if triggered_id and isinstance(triggered_id, dict) and triggered_id.get('type') == 'reset-country-filter-button':
        return None  # Reset the filter by setting store data to None

    # Check if the map was the trigger
    if triggered_id == 'geo-sentiment-map' and clickData:
        country_iso3 = clickData['points'][0].get('location')
        try:
            # Find the country by its ISO3 code to get the full country object
            country = pycountry.countries.get(alpha_3=country_iso3)
            if country:
                # Store the ISO2 code (for the engine) and the full name (for display)
                return {'code': country.alpha_2, 'name': country.name}
        except (AttributeError, KeyError):
            return no_update # Ignore clicks on areas without a valid country code
    
    return no_update

# Callback 3: Update KPI cards
@callback(
    Output("kpi-cards", "children"),
    [Input("firm-dropdown", "value"),
     Input("time-period-dropdown", "value")]
)
def update_kpis(selected_firm, time_period):
    # Guard clause to prevent errors on initial load
    if not selected_firm: return no_update

    from app import engine
    firm_to_query = None if selected_firm == "ALL" else selected_firm
    
    try:
        kpi_data = engine.get_kpi(selected_firm=firm_to_query, days=time_period)
        momentum_color = "text-success" if kpi_data['sentiment_momentum']['raw_value'] >= 0 else "text-danger"
        firm_display_text = f"({selected_firm if firm_to_query else 'All Firms'})"
        
        return [
            dbc.Col(dbc.Card(dbc.CardBody([html.H4(kpi_data['total_reviews']['value']), html.P("Total Reviews"), html.Small(firm_display_text, className="text-muted")])), width=3),
            dbc.Col(dbc.Card(dbc.CardBody([html.H4(kpi_data['avg_sentiment']['value'], className="text-success"), html.P("Avg. Sentiment"), html.Small(firm_display_text, className="text-muted")])), width=3),
            dbc.Col(dbc.Card(dbc.CardBody([html.H4(kpi_data['sentiment_momentum']['value'], className=momentum_color), html.P("Sentiment Momentum"), html.Small(f"vs previous {time_period} days", className="text-muted")])), width=3),
            dbc.Col(dbc.Card(dbc.CardBody([html.H4(kpi_data['unique_topics']['value'], className="text-info"), html.P("Unique Topics"), html.Small(firm_display_text, className="text-muted")])), width=3),
        ]
    except Exception as e:
        logger.error(f"Failed to update KPIs: {e}")
        return [dbc.Col(dbc.Alert("Error loading KPI data.", color="danger"), width=12)]

# Callback 4: Update the Geo Map
@callback(
    Output("geo-sentiment-map", "figure"),
    [Input("firm-dropdown", "value"),
     Input("time-period-dropdown", "value")]
)
def update_geo_map(selected_firm, time_period):
    if not selected_firm: return no_update

    from app import engine
    firm_to_query = None if selected_firm == "ALL" else selected_firm

    try:
        geo_data = engine.get_geographic_sentiment(selected_firm=firm_to_query, days=time_period)
        
        if not geo_data:
            fig_map = go.Figure(data=go.Choropleth(
                locations=[],
                z=[],
                locationmode='ISO-3',
                showscale=False
            ))
            fig_map.update_layout(
                title="Global Sentiment Distribution<br><sub>(No review data for selected filters)</sub>",
                geo=dict(showframe=False, showcoastlines=True), 
                height=400
            )
            return fig_map
        iso2_to_iso3 = {country.alpha_2: country.alpha_3 for country in pycountry.countries}
        
        locations_iso3, sentiments, hover_data = [], [], []
        for item in geo_data:
            iso3 = iso2_to_iso3.get(item['location'])
            if iso3:
                locations_iso3.append(iso3)
                sentiments.append(item['sentiment'])
                country_name = pycountry.countries.get(alpha_3=iso3).name
                sentiment_label = get_sentiment_label(item['sentiment'])
                hover_data.append(f"<b>{country_name}</b><br>Sentiment: {sentiment_label}<br>Reviews: {item['review_count']}<extra></extra>")

        fig_map = go.Figure(data=go.Choropleth(
            locations=locations_iso3,
            z=sentiments,
            locationmode='ISO-3', 
            colorscale='RdYlGn', 
            zmid=0.5,
            hovertemplate='%{customdata}',
            customdata=hover_data,
            colorbar_title="Sentiment"
        ))
        
        fig_map.update_layout(title="Global Sentiment Distribution", geo=dict(showframe=False, showcoastlines=True), height=400)
        return fig_map
    except Exception as e:
        logger.error(f"Failed to update geo map: {e}")
        return go.Figure().update_layout(title="Error generating map")

# Callback 5: Update the Topic Bubble Chart
@callback(
    Output("topic-bubble-chart", "figure"),
    [Input("firm-dropdown", "value"),
     Input("time-period-dropdown", "value"),
     Input("sentiment-range-slider", "value")]
)
def update_bubble_chart(selected_firm, time_period, sentiment_range):
    if not selected_firm: return no_update

    from app import engine
    firm_to_query = None if selected_firm == "ALL" else selected_firm
    
    try:
        bubble_data = engine.get_topic_bubble_data(selected_firm=firm_to_query, days=time_period)
        filtered_data = [d for d in bubble_data if sentiment_range[0] <= d['sentiment'] <= sentiment_range[1]]

        if not filtered_data:
            return go.Figure().update_layout(title="No topics found for the selected filters", height=400)
        
        fig_bubble = px.scatter(
            pd.DataFrame(filtered_data),
            x="volume",
            y="sentiment",
            size="volume",
            color="sentiment",
            hover_name="topic",
            color_continuous_scale=px.colors.diverging.RdYlGn,
            range_color=[0, 1],
            title="Topic Explorer: Volume vs Sentiment",
            labels={"volume": "Review Volume", "sentiment": "Average Sentiment Score"}
        )
        fig_bubble.update_layout(height=400)
        return fig_bubble
    except Exception as e:
        logger.error(f"Failed to update bubble chart: {e}")
        return go.Figure().update_layout(title="Error generating bubble chart")

# Callback 6: Update the Voice of the Customer feed and its header
@callback(
    [Output("voice-feed", "children"),
     Output("voice-feed-header", "children")],
    [Input("firm-dropdown", "value"),
     Input("time-period-dropdown", "value"),
     Input("country-filter-store", "data")] # This callback now listens to the country store
)
def update_voice_feed(selected_firm, time_period, country_data):
    if not selected_firm: return no_update, no_update

    from app import engine
    firm_to_query = None if selected_firm == "ALL" else selected_firm
    country_code = country_data['code'] if country_data else None
    country_name = country_data['name'] if country_data else None

    # Part 1: Generate the dynamic header
    if country_name:
        header_content = dbc.Row([
            dbc.Col(html.H5(f"Voice of the Customer ({country_name})")),
            dbc.Col(dbc.Button("Reset", id={'type': 'reset-country-filter-button', 'index': 'reset'}, size="sm", color="secondary"), width="auto")
        ], align="center", justify="between")
    else:
        header_content = html.H5("Voice of the Customer")

    # Part 2: Generate the feed items based on filters
    try:
        high_rev = engine.get_extreme_sentiment_reviews(selected_firm=firm_to_query, days=time_period, limit=3, mode='highest', country_code=country_code)
        low_rev = engine.get_extreme_sentiment_reviews(selected_firm=firm_to_query, days=time_period, limit=3, mode='lowest', country_code=country_code)
        
        feed_items = []
        if high_rev:
            feed_items.append(html.H6("🟢 Recent Highlights", className="text-success"))
            for r in high_rev:
                feed_items.append(dbc.Alert([html.Strong(r['firm_name']), html.P(f"\"{r['content']}\"", className="mt-2 mb-1 fst-italic"), html.Small(f"Date: {r['date_posted']}", className="text-muted")], color="success", className="mb-2"))
        
        if low_rev:
            feed_items.append(html.H6("🔴 Recent Concerns", className="text-danger"))
            for r in low_rev:
                feed_items.append(dbc.Alert([html.Strong(r['firm_name']), html.P(f"\"{r['content']}\"", className="mt-2 mb-1 fst-italic"), html.Small(f"Date: {r['date_posted']}", className="text-muted")], color="danger", className="mb-2"))

        if not feed_items:
            message = "No recent feedback available to display."
            if country_name:
                message += f" for {country_name} with the current filters."
            feed_items = [html.P(message, className="text-muted")]

        return feed_items, header_content
    except Exception as e:
        logger.error(f"Failed to update voice feed: {e}")
        return [dbc.Alert("Error updating voice feed.", color="danger")], "Voice of the Customer"