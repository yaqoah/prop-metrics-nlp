import pandas as pd
import torch
import os
from sentence_transformers import SentenceTransformer
from datetime import datetime, timedelta
from database.connection import SupabaseConnection
from src.analytics.helpers import calculate_date_range, format_kpi_value
from src.utils.logger import get_logger

os.environ["TORCH_FORCE_WEIGHTS_ONLY"] = "0" 

class Engine:
    def __init__(self):
        self.logger = get_logger("analytics engine")
        self.logger.info(f"PyTorch version: {torch.__version__}")
        self.device = "cpu"
        self.logger.info("Loading SentenceTransformer model to CPU...")
        self.model = SentenceTransformer("all-MiniLM-L6-v2", device=self.device)
        self.logger.info("Model successfully loaded.")
        self.logger.info(f"Model device: {next(self.model.parameters()).device}")
        self.db = SupabaseConnection()

    def _execute(self, query, params=None):
        if self.db.use_fallback:
            self.logger.error("Analytics requires direct PostgreSQL connection. Fallback mode not supported.")
            return pd.DataFrame() 

        try:
            with self.db.get_db_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(query, params or ())
                    columns = [desc[0] for desc in cur.description]
                    data = cur.fetchall()
                    df = pd.DataFrame(data, columns=columns)
                    self.logger.debug(f"Query executed successfully, returned {len(df)} rows")
                    return df
        except Exception as e:
            self.logger.error(f"Failed to execute analytics query: {e}")
            self.logger.debug(f"Query: {query}")
            self.logger.debug(f"Params: {params}")
            return pd.DataFrame() 

    # 1. Aggregation functions
    def get_firms(self):
        query = """
        SELECT DISTINCT firm_name 
        FROM reviews 
        WHERE date_posted >= CURRENT_DATE - INTERVAL '180 days'
        ORDER BY firm_name
        """

        try:
            df = self._execute(query)

            if hasattr(df, "empty"):
                if df.empty:
                    self.logger.warning("No firms returned from query.")
                    return ["<No firms available>"]
                return df['firm_name'].dropna().tolist()

            if isinstance(df, list):
                return [row[0] for row in df if row and len(row) > 0]

            self.logger.warning(f"Unexpected result type from _execute: {type(df)}")
            return ["<No firms available>"]

        except Exception as e:
            self.logger.error(f"get_firms() failed: {e}")
            self.logger.info(f"Type of _execute result: {type(df)}")
            self.logger.info(f"First 3 rows from query: {df[:3] if hasattr(df, '__getitem__') else df.head(3) if hasattr(df, 'head') else df}")
            return ["<No firms available>"]

    def get_kpi(self, selected_firm=None, days=30):
        period_log_msg = f"{days} days" if days != -1 else "All time"
        self.logger.info(f"Getting KPI data for period: {period_log_msg}, for firm: {selected_firm or 'All'}")
        
        main_filters = []
        main_params = []
        
        if selected_firm:
            main_filters.append("firm_name = %s")
            main_params.append(selected_firm)
            
        if days != -1:
            start_date, end_date = calculate_date_range(days)
            main_filters.append("date_posted BETWEEN %s AND %s")
            main_params.extend([start_date, end_date])

        main_where_clause = f"WHERE {' AND '.join(main_filters)}" if main_filters else ""
        main_stats_query = f"""
        SELECT 
            COUNT(*) as total_reviews,
            AVG(sentiment_score) as avg_sentiment,
            STDDEV(sentiment_score) as sentiment_std,
            COUNT(DISTINCT firm_name) as unique_firms,
            COUNT(DISTINCT primary_topic_id) as unique_topics
        FROM reviews
        {main_where_clause}
        """
        
        main_stats_df = self._execute(main_stats_query, tuple(main_params))
        
        # Handle empty results (your existing code is fine)
        if main_stats_df.empty or main_stats_df.iloc[0]['total_reviews'] == 0:
            return {
                'total_reviews': {'value': '0', 'raw_value': 0},
                'avg_sentiment': {'value': '0.00', 'raw_value': 0.0},
                'sentiment_momentum': {'value': 'N/A', 'raw_value': 0.0},
                'unique_firms': {'value': '0', 'raw_value': 0},
                'unique_topics': {'value': '0', 'raw_value': 0},
                'top_topics': [],
                'period_days': days
            }
        
        main_stats = main_stats_df.iloc[0]
        self.logger.info(f"Found {main_stats['total_reviews']} reviews for KPI calculation")
        
        topic_filters_prefixed = [f"r.{f}" for f in main_filters]
        topic_where_clause = f"WHERE {' AND '.join(topic_filters_prefixed)}" if topic_filters_prefixed else ""

        top_topics_query = f"""
        SELECT 
            t.topic_name,
            COUNT(*) as review_count,
            AVG(r.sentiment_score) as avg_sentiment
        FROM reviews r
        JOIN topics t ON r.primary_topic_id = t.id
        {topic_where_clause}
        GROUP BY t.topic_name, r.primary_topic_id
        ORDER BY review_count DESC
        LIMIT 3
        """
        
        top_topics_df = self._execute(top_topics_query, tuple(main_params))
        
        top_topics = top_topics_df['topic_name'].tolist() if not top_topics_df.empty else []
        
        if days == -1:
            momentum = 0.0
            self.logger.info("Period is 'All time', skipping momentum calculation.")
        else:
            momentum = self.get_sentiment_momentum(selected_firm, days)
        
        kpi_data = {
            'total_reviews': {
                'value': format_kpi_value(main_stats['total_reviews'], 'count'),
                'raw_value': int(main_stats['total_reviews'])
            },
            'avg_sentiment': {
                'value': format_kpi_value(main_stats['avg_sentiment'], 'score'),
                'raw_value': float(main_stats['avg_sentiment'] or 0)
            },
            'sentiment_momentum': {
                'value': f"{momentum:+.1f}%" if days != -1 else "N/A",
                'raw_value': momentum
            },
            'unique_firms': {
                'value': format_kpi_value(main_stats['unique_firms'], 'count'),
                'raw_value': int(main_stats['unique_firms'])
            },
            'unique_topics': {
                'value': format_kpi_value(main_stats['unique_topics'], 'count'),
                'raw_value': int(main_stats['unique_topics'])
            },
            'top_topics': top_topics,
            'period_days': days
        }

        self.logger.info("KPI data calculated successfully")
        return kpi_data

    def get_topic_sentiment(self, firms, days=90):
        start_date, end_date = calculate_date_range(days)
        self.logger.info(f"Getting topic sentiment matrix for {len(firms) if firms else 'all'} firms")
        
        firm_filter = ""
        params = [start_date, end_date]
        
        if firms:
            firm_placeholders = ','.join(['%s'] * len(firms))
            firm_filter = f"AND r.firm_name IN ({firm_placeholders})"
            params.extend(firms)
        
        query = f"""
        SELECT 
            r.firm_name,
            t.display_name, -- Use the clean display name
            AVG(r.sentiment_score) as avg_sentiment,
            COUNT(*) as review_count,
            STDDEV(r.sentiment_score) as sentiment_std
        FROM reviews r
        JOIN topics t ON r.primary_topic_id = t.id
        WHERE r.date_posted BETWEEN %s AND %s
        {firm_filter}
        GROUP BY r.firm_name, t.display_name -- Group by the clean display name
        HAVING COUNT(*) >= 3 
        ORDER BY r.firm_name, t.display_name
        """
        
        df = self._execute(query, tuple(params))
        
        if df.empty:
            self.logger.warning("Failed to produce topic sentiment")
            return pd.DataFrame()
        
        matrix = df.pivot(index='firm_name', columns='display_name', values='avg_sentiment')
        self.logger.info(f"Topic sentiment matrix created: {matrix.shape}")
        
        return matrix

    def get_geographic_sentiment(self,selected_firm=None, days=90):
        self.logger.info(f"Getting geographic sentiment data for {days} days")

        filters = []
        params = []
        
        if selected_firm:
            filters.append("firm_name = %s")
            params.append(selected_firm)
            
        if days != -1:
            start_date, end_date = calculate_date_range(days)
            filters.append("date_posted BETWEEN %s AND %s")
            params.extend([start_date, end_date])
        
        where_clause = f"WHERE {' AND '.join(filters)}" if filters else ""

        query = f"""
        SELECT country_code AS location, AVG(sentiment_score) as sentiment, COUNT(review_id) as review_count
        FROM reviews
        {where_clause}
        GROUP BY country_code
        HAVING COUNT(review_id) > 1 -- CHANGE THIS LINE FROM > 10 to > 1
        """
        
        df = self._execute(query, tuple(params))

        if df is None or df.empty:
            self.logger.warning("No geographic sentiment data produced.")
            return []


        geo_data = []
        for _, row in df.iterrows():
            geo_data.append({
                'location': row['location'],
                'sentiment': float(row['sentiment']),
                'review_count': int(row['review_count']),

            })

        self.logger.info(f"Found geographic data for {len(geo_data)} locations")
        return geo_data

    def get_topic_bubble_data(self, selected_firm=None, days=30):
        self.logger.info(f"Getting topic bubble data for {days} days")

        filters = []
        params = []
        
        if selected_firm:
            filters.append("r.firm_name = %s")
            params.append(selected_firm)
            
        if days != -1:
            start_date, end_date = calculate_date_range(days)
            filters.append("r.date_posted BETWEEN %s AND %s")
            params.extend([start_date, end_date])
        
        where_clause = f"WHERE {' AND '.join(filters)}" if filters else ""

        query = f"""
        SELECT 
            -- If the display_name is null (because the join failed), label it 'Uncategorized'.
            COALESCE(t.display_name, 'Uncategorized') as topic,
            AVG(r.sentiment_score) as sentiment,
            COUNT(r.review_id) as volume,
            STDDEV(r.sentiment_score) as variance,
            -- Firm coverage for uncategorized reviews is still meaningful
            COUNT(DISTINCT r.firm_name) as firm_coverage
        FROM 
            reviews r
        -- Use a LEFT JOIN to keep all reviews, even those without a topic
        LEFT JOIN topics t ON r.primary_topic_id = t.id
        {where_clause}
        -- Group by the potentially coalesced name
        GROUP BY topic
        -- Keep your requirement to only show topics with more than 1 review
        HAVING COUNT(r.review_id) > 1
        """
        
        df = self._execute(query, tuple(params))

        if df.empty:
            self.logger.warning("No topic bubble data found")
            return []
        
        bubble_data = []
        for _, row in df.iterrows():
            bubble_data.append({
                'topic': row['topic'],
                'sentiment': float(row['sentiment']),
                'volume': int(row['volume']),
                'variance': float(row['variance']) if pd.notna(row['variance']) else 0.0,
                'firm_coverage': int(row['firm_coverage'])
            })
        
        self.logger.info(f"Generated bubble data for {len(bubble_data)} topics")
        return bubble_data

    def get_extreme_sentiment_reviews(self, selected_firm=None, days=30, limit=3, mode='highest', country_code=None):
        self.logger.info(f"Getting {limit} reviews with {mode} sentiment for period: {days} days")
        
        filters = []
        params = []
        
        if selected_firm:
            filters.append("firm_name = %s")
            params.append(selected_firm)
            
        if days != -1:
            start_date, end_date = calculate_date_range(days)
            filters.append("date_posted BETWEEN %s AND %s")
            params.extend([start_date, end_date])

        if country_code:
            filters.append("country_code = %s")
            params.append(country_code)
        
        where_clause = f"WHERE {' AND '.join(filters)}" if filters else ""
        
        order_direction = "DESC" if mode == 'highest' else "ASC"
        
        query = f"""
        SELECT 
            review_id,
            firm_name,
            date_posted,
            sentiment_score,
            content
        FROM reviews
        {where_clause}
        ORDER BY sentiment_score {order_direction}
        LIMIT %s
        """
        params.append(limit)
        
        df = self._execute(query, tuple(params))

        if df.empty:
            return []
        
        results = []
        for _, row in df.iterrows():
            review = {
                'review_id': str(row['review_id']), # Cast ID to string
                'firm_name': str(row['firm_name']), # Cast firm name to string
                'sentiment_score': float(row['sentiment_score']), # Ensure it's a standard float
                'content': str(row['content']) # Cast content to string
            }
            
            # Safely handle the date
            if pd.notna(row['date_posted']):
                review['date_posted'] = row['date_posted'].strftime('%Y-%m-%d')
            else:
                review['date_posted'] = 'N/A'
                
            results.append(review)
                
        return results

    # 2. Trend & Anomolies functions
    def get_sentiment_momentum(self, selected_firm=None, current_period=30, comparison_period=30):
        current_start, current_end = calculate_date_range(current_period)
        comparison_end_date = datetime.now() - timedelta(days=current_period)
        comparison_start_date = comparison_end_date - timedelta(days=comparison_period)
        comparison_start = comparison_start_date.strftime('%Y-%m-%d')
        comparison_end = comparison_end_date.strftime('%Y-%m-%d')

        self.logger.info(f"Calculating sentiment momentum: current({current_start} to {current_end}) vs previous({comparison_start} to {comparison_end})")
        
        current_filters = ["date_posted BETWEEN %s AND %s"]
        current_params = [current_start, current_end]

        previous_filters = ["date_posted BETWEEN %s AND %s"]
        previous_params = [comparison_start, comparison_end]
        
        if selected_firm:
            current_filters.append("firm_name = %s")
            current_params.append(selected_firm)
            
            previous_filters.append("firm_name = %s")
            previous_params.append(selected_firm)

        current_where_clause = " AND ".join(current_filters)
        previous_where_clause = " AND ".join(previous_filters)
        
        final_params = tuple(current_params + previous_params)

        query = f"""
        WITH current_sentiment AS (
            SELECT AVG(sentiment_score) as avg_sentiment
            FROM reviews
            WHERE {current_where_clause}
        ),
        previous_sentiment AS (
            SELECT AVG(sentiment_score) as avg_sentiment
            FROM reviews
            WHERE {previous_where_clause}
        )
        SELECT 
            c.avg_sentiment as current_avg,
            p.avg_sentiment as previous_avg,
            CASE 
                WHEN p.avg_sentiment IS NULL OR p.avg_sentiment = 0 THEN 0
                ELSE ((c.avg_sentiment - p.avg_sentiment) / p.avg_sentiment) * 100
            END as momentum
        FROM current_sentiment c, previous_sentiment p
        """
        
        df = self._execute(query, final_params)

        if df.empty or df.iloc[0]['momentum'] is None:
            self.logger.warning("Failed to calculate sentiment momentum for")
            return 0.0
        
        momentum = float(df.iloc[0]['momentum'])
        self.logger.info(f"Sentiment momentum: {momentum:+.1f}%")
        return momentum
    
    def detect_sentiment_anomalies(self, firm_name=None, days= 90, std_threshold=2.0):
        start_date, end_date = calculate_date_range(days)
        self.logger.info(f"Detecting sentiment anomalies for {firm_name or 'all firms'}")

        
        if firm_name:
            query = """
            WITH daily_sentiment AS (
                SELECT 
                    DATE(date_posted) as review_date,
                    AVG(sentiment_score) as daily_avg_sentiment,
                    COUNT(*) as daily_review_count
                FROM reviews
                WHERE firm_name = %s AND date_posted BETWEEN %s AND %s
                GROUP BY DATE(date_posted)
                HAVING COUNT(*) >= 3  -- Minimum reviews per day
            ),
            sentiment_stats AS (
                SELECT 
                    AVG(daily_avg_sentiment) as overall_avg,
                    STDDEV(daily_avg_sentiment) as overall_std
                FROM daily_sentiment
            )
            SELECT 
                d.review_date,
                d.daily_avg_sentiment,
                d.daily_review_count,
                s.overall_avg,
                s.overall_std,
                ABS(d.daily_avg_sentiment - s.overall_avg) / s.overall_std as z_score
            FROM daily_sentiment d
            CROSS JOIN sentiment_stats s
            WHERE ABS(d.daily_avg_sentiment - s.overall_avg) / s.overall_std > %s
            ORDER BY z_score DESC
            """
            params = (firm_name, start_date, end_date, std_threshold)
        else:
            query = """
            WITH firm_sentiment AS (
                SELECT 
                    firm_name,
                    AVG(sentiment_score) as firm_avg_sentiment,
                    COUNT(*) as firm_review_count
                FROM reviews
                WHERE date_posted BETWEEN %s AND %s
                GROUP BY firm_name
                HAVING COUNT(*) >= 10  -- Minimum reviews per firm
            ),
            sentiment_stats AS (
                SELECT 
                    AVG(firm_avg_sentiment) as overall_avg,
                    STDDEV(firm_avg_sentiment) as overall_std
                FROM firm_sentiment
            )
            SELECT 
                f.firm_name,
                f.firm_avg_sentiment,
                f.firm_review_count,
                s.overall_avg,
                s.overall_std,
                ABS(f.firm_avg_sentiment - s.overall_avg) / s.overall_std as z_score
            FROM firm_sentiment f
            CROSS JOIN sentiment_stats s
            WHERE ABS(f.firm_avg_sentiment - s.overall_avg) / s.overall_std > %s
            ORDER BY z_score DESC
            """
            params = (start_date, end_date, std_threshold)
        
        df = self._execute(query, params)
        
        anomalies = []
        for _, row in df.iterrows():
            if firm_name:
                anomalies.append({
                    'type': 'daily_sentiment',
                    'firm_name': firm_name,
                    'date': row['review_date'].strftime('%Y-%m-%d'),
                    'value': float(row['daily_avg_sentiment']),
                    'expected': float(row['overall_avg']),
                    'z_score': float(row['z_score']),
                    'review_count': int(row['daily_review_count']),
                    'severity': 'high' if row['z_score'] > 3.0 else 'medium'
                })
            else:
                anomalies.append({
                    'type': 'firm_sentiment',
                    'firm_name': row['firm_name'],
                    'value': float(row['firm_avg_sentiment']),
                    'expected': float(row['overall_avg']),
                    'z_score': float(row['z_score']),
                    'review_count': int(row['firm_review_count']),
                    'severity': 'high' if row['z_score'] > 3.0 else 'medium'
                })
        
        self.logger.info(f"Found {len(anomalies)} sentiment anomalies")
        return anomalies

    def get_trending_topics(self, days=30, min_growth_rate=0.2):
        current_start, current_end = calculate_date_range(days)
        
        previous_end_date = datetime.now() - timedelta(days=days)
        previous_start_date = previous_end_date - timedelta(days=days)
        previous_start = previous_start_date.strftime('%Y-%m-%d')
        previous_end = previous_end_date.strftime('%Y-%m-%d')

        self.logger.info(f"Getting trending topics with min growth rate: {min_growth_rate}")

        query = """
        WITH current_topics AS (
            SELECT 
                t.topic_name,
                t.primary_topic_id,
                COUNT(*) as current_count,
                AVG(r.sentiment_score) as current_sentiment
            FROM reviews r
            JOIN topics t ON r.primary_topic_id = t.id
            WHERE r.date_posted BETWEEN %s AND %s
            GROUP BY t.topic_name, r.primary_topic_id
        ),
        previous_topics AS (
            SELECT 
                t.topic_name,
                t.primary_topic_id,
                COUNT(*) as previous_count,
                AVG(r.sentiment_score) as previous_sentiment
            FROM reviews r
            JOIN topics t ON r.primary_topic_id = t.id
            WHERE r.date_posted BETWEEN %s AND %s
            GROUP BY t.topic_name, r.primary_topic_id
        )
        SELECT 
            c.topic_name,
            c.current_count,
            COALESCE(p.previous_count, 0) as previous_count,
            c.current_sentiment,
            COALESCE(p.previous_sentiment, c.current_sentiment) as previous_sentiment,
            CASE 
                WHEN COALESCE(p.previous_count, 0) = 0 THEN 1.0
                ELSE (c.current_count::float - COALESCE(p.previous_count, 0)) / COALESCE(p.previous_count, 1)
            END as growth_rate,
            c.current_sentiment - COALESCE(p.previous_sentiment, c.current_sentiment) as sentiment_change
        FROM current_topics c
        LEFT JOIN previous_topics p ON c.primary_topic_id = p.primary_topic_id
        WHERE CASE 
            WHEN COALESCE(p.previous_count, 0) = 0 THEN 1.0
            ELSE (c.current_count::float - COALESCE(p.previous_count, 0)) / COALESCE(p.previous_count, 1)
        END > %s
        ORDER BY growth_rate DESC
        """
        
        df = self._execute(query, (current_start, current_end, previous_start, previous_end, min_growth_rate))
        
        trending_topics = []
        for _, row in df.iterrows():
            trending_topics.append({
                'topic_name': row['topic_name'],
                'current_count': int(row['current_count']),
                'previous_count': int(row['previous_count']),
                'growth_rate': float(row['growth_rate']),
                'current_sentiment': float(row['current_sentiment']),
                'sentiment_change': float(row['sentiment_change']),
                'trend_strength': 'strong' if row['growth_rate'] > 0.5 else 'moderate'
            })
        
        self.logger.info(f"Found {len(trending_topics)} trending topics")
        return trending_topics

    def get_competitive_alerts(self, firm_name, days=30):
        start_date, end_date = calculate_date_range(days)
        self.logger.info(f"Getting competitive alerts for {firm_name}")
        
        query = """
        WITH firm_performance AS (
            SELECT 
                t.topic_name,
                AVG(CASE WHEN r.firm_name = %s THEN r.sentiment_score END) as target_sentiment,
                AVG(CASE WHEN r.firm_name != %s THEN r.sentiment_score END) as competitor_avg,
                COUNT(CASE WHEN r.firm_name = %s THEN 1 END) as target_reviews,
                COUNT(CASE WHEN r.firm_name != %s THEN 1 END) as competitor_reviews
            FROM reviews r
            JOIN topics t ON r.primary_topic_id = t.id
            WHERE r.date_posted BETWEEN %s AND %s
            GROUP BY t.topic_name
            HAVING COUNT(CASE WHEN r.firm_name = %s THEN 1 END) >= 3
            AND COUNT(CASE WHEN r.firm_name != %s THEN 1 END) >= 10
        )
        SELECT 
            topic_name,
            target_sentiment,
            competitor_avg,
            target_reviews,
            competitor_reviews,
            target_sentiment - competitor_avg as sentiment_gap,
            CASE 
                WHEN target_sentiment - competitor_avg > 0.1 THEN 'outperforming'
                WHEN target_sentiment - competitor_avg < -0.1 THEN 'underperforming'
                ELSE 'competitive'
            END as performance_status
        FROM firm_performance
        WHERE ABS(target_sentiment - competitor_avg) > 0.05  -- Minimum significant difference
        ORDER BY ABS(target_sentiment - competitor_avg) DESC
        """
        
        params = (firm_name, firm_name, firm_name, firm_name, start_date, end_date, firm_name, firm_name)
        df = self._execute(query, params)
        
        alerts = []
        for _, row in df.iterrows():
            alert_type = 'opportunity' if row['sentiment_gap'] > 0 else 'threat'
            
            alerts.append({
                'type': alert_type,
                'topic': row['topic_name'],
                'firm_sentiment': float(row['target_sentiment']),
                'competitor_avg': float(row['competitor_avg']),
                'sentiment_gap': float(row['sentiment_gap']),
                'performance_status': row['performance_status'],
                'firm_reviews': int(row['target_reviews']),
                'competitor_reviews': int(row['competitor_reviews']),
                'priority': 'high' if abs(row['sentiment_gap']) > 0.2 else 'medium'
            })
        
        self.logger.info(f"Generated {len(alerts)} competitive alerts for {firm_name}")
        return alerts

    # 3. Semantic search functions
    def find_similar_reviews(self, query_text, limit=None, similarity_threshold=None, 
                           firm_filter=None, date_range=None):
        limit = limit or 100
        similarity_threshold = similarity_threshold or 0.7

        self.logger.info(f"Searching for reviews similar to: '{query_text[:50]}...'")

        try:
            query_embedding = self.model.encode(query_text)
            query_vector = query_embedding.tolist()
        except Exception as e:
            self.logger.error(f"Failed to generate embedding for query: {e}")
            return []
        
        params = [query_vector]
        
        filters = []
        
        if firm_filter:
            firm_placeholders = ','.join(['%s'] * len(firm_filter))
            filters.append(f"r.firm_name IN ({firm_placeholders})")
            params.extend(firm_filter)
        
        if date_range:
            filters.append("r.date_posted BETWEEN %s AND %s")
            params.extend(list(date_range))
        
        params.extend([similarity_threshold, limit])

        where_clause = ""
        if filters:
            where_clause = "WHERE " + " AND ".join(filters)

        query = f"""
        SELECT *
        FROM (
            SELECT 
                r.review_id,
                r.firm_name,
                r.date_posted,
                r.sentiment_score,
                t.topic_name,
                r.content,
                1 - (r.embedding <=> %s::vector) AS similarity -- Vector goes here
            FROM reviews r
            JOIN topics t ON r.primary_topic_id = t.id
            {where_clause}  -- Optional filters (firm, date) are applied here
        ) AS results
        WHERE results.similarity > %s -- Threshold is applied here
        ORDER BY results.similarity DESC
        LIMIT %s -- Limit is applied last
        """
        
        df = self._execute(query, tuple(params))

        if df.empty:
            self.logger.warning("No similar reviews found")
            return []
        
        results = df.to_dict('records')
        for r in results:
            r['sentiment_score'] = float(r['sentiment_score'])
            r['similarity'] = round(float(r['similarity']), 4)
            if 'date_posted' in r and r['date_posted']:
                r['date_posted'] = r['date_posted'].strftime('%Y-%m-%d')
        
        self.logger.info(f"Found {len(results)} similar reviews")
        return results
    
    def get_search_suggestions(self, partial_query: str, limit: int = 5):
        self.logger.info(f"Getting search suggestions for: '{partial_query}'")

        query = """
        SELECT topic_name, COUNT(*) as frequency
        FROM topics t
        JOIN reviews r ON t.primary_topic_id = r.primary_topic_id
        WHERE LOWER(t.topic_name) LIKE LOWER(%s)
        GROUP BY topic_name
        ORDER BY frequency DESC
        LIMIT %s
        """
        
        search_pattern = f"%{partial_query}%"
        df = self._execute(query, (search_pattern, limit))

        if df.empty:
            suggestions = []
        else:
            suggestions = df['topic_name'].tolist()
        
        suggestions = df['topic_name'].tolist()
        
        if not suggestions and len(partial_query) > 2:
            common_patterns = [
                f"positive {partial_query}",
                f"negative {partial_query}",
                f"{partial_query} experience",
                f"{partial_query} service",
                f"{partial_query} quality"
            ]
            suggestions = common_patterns[:limit]
            self.logger.info(f"Using fallback suggestions for '{partial_query}'")
        
        self.logger.info(f"Generated {len(suggestions)} search suggestions")
        return suggestions
