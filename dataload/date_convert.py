import pandas as pd

def extract_date_year(df: pd.DataFrame, date_col: str, prefix: str) -> pd.DataFrame:
    """Converts a string date column in a DataFrame to a datetime object,
    extracts month, year, quarter, day, and week day, and adds them as new columns
    with an optional prefix.

    Args:
        df (pd.DataFrame): The DataFrame containing the date column.
        date_col (str): The name of the column containing the string date.
        prefix (str, optional): A prefix to add before the extracted date components.
                                Defaults to an empty string ("").

    Returns:
        pd.DataFrame: The modified DataFrame with new columns for date components.

    Raises:
        ValueError: If the date format in the 'date_col' is invalid.
    """

  # Extract and add month, year, quarter, day, and week day as new columns
    df[prefix + '_month'] = df[date_col].dt.month
    df[prefix + '_year'] = df[date_col].dt.year
    df[prefix + '_quarter'] = df[date_col].dt.quarter
    df[prefix + '_day'] = df[date_col].dt.day
    df[prefix + '_week_day'] = df[date_col].dt.strftime('%A')

    return df