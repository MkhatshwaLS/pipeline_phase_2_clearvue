from datetime import datetime, timedelta
import pandas as pd


def get_last_saturday_of_month(year, month):
    # Get last day of the month
    if month == 12:
        next_month = datetime(year + 1, 1, 1)
    else:
        next_month = datetime(year, month + 1, 1)
    last_day = next_month - timedelta(days=1)
    
    # Walk backward to find last Saturday (weekday 5)
    offset = (last_day.weekday() - 5) % 7
    return last_day - timedelta(days=offset)

def get_last_friday_of_month(year, month):
    if month == 12:
        next_month = datetime(year + 1, 1, 1)
    else:
        next_month = datetime(year, month + 1, 1)
    last_day = next_month - timedelta(days=1)
    
    offset = (last_day.weekday() - 4) % 7  # Friday = 4
    return last_day - timedelta(days=offset)

def assign_financial_period(sale_date):
    dt = pd.to_datetime(sale_date)
    year = dt.year
    month = dt.month

    # Financial Month starts on last Saturday of PREVIOUS month
    if month == 1:  # January? Look back to December of prior year
        fm_start = get_last_saturday_of_month(year - 1, 12)
    else:
        fm_start = get_last_saturday_of_month(year, month - 1)

    # Financial Month ends on last Friday of CURRENT month
    fm_end = get_last_friday_of_month(year, month)

    # Determine Financial Year (assume FY starts in February → FY = fm_start.year)
    financial_year = fm_start.year

    # Assign Financial Month Number (1-12) — assuming FY starts in February
    # So Feb = FM1, Mar = FM2, ..., Jan = FM12
    if month == 2:
        fm_number = 1
    elif month > 2:
        fm_number = month - 1
    else:  # Jan (month=1) → FM12 of prior FY
        fm_number = 12
        financial_year -= 1  # Adjust FY for January sales

    # Assign Financial Quarter (1-4)
    fm_quarter_map = {1:1, 2:1, 3:1, 4:2, 5:2, 6:2, 7:3, 8:3, 9:3, 10:4, 11:4, 12:4}
    financial_quarter = fm_quarter_map[fm_number]

    return pd.Series({
        'financial_year': financial_year,
        'financial_month': fm_number,
        'financial_quarter': financial_quarter,
        'financial_month_start': fm_start,
        'financial_month_end': fm_end
    })

# Apply to sales data
sales_df[['financial_year', 'financial_month', 'financial_quarter', 'financial_month_start', 'financial_month_end']] = \
    sales_df['sale_date'].apply(assign_financial_period)