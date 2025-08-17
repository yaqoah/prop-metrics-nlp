from datetime import datetime, timedelta

def calculate_date_range(days):
    end_date = datetime.now()
    start_date = end_date - timedelta(days=days)
    
    return start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d')

def format_kpi_value(value, metric_type):
        if metric_type == 'percentage':
            return f"{value:.1f}%"
        elif metric_type == 'count':
            return f"{int(value):,}"
        elif metric_type == 'score':
            return f"{value:.2f}"
        
        return str(value)

