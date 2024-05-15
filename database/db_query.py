from sqlalchemy import text
import pandas as pd


def query_table(query_statement, connection): 
    """
    Execute a SQL query statement and return the result as a DataFrame.

    Args:
        query_statement (str): SQL query statement to execute.
        connection (Connection): SQLAlchemy connection object.

    Returns:
        DataFrame: DataFrame containing the query result.

    Raises:
        Exception: If an error occurs during the query execution.
    """
    try: 
        # Execute the SQL query statement
        result = connection.execute(text(query_statement))
        
        # Fetch all rows and column names from the query result
        df_table = pd.DataFrame(result.fetchall(), columns=result.keys())
        return df_table
    except Exception as e: 
        # Raise an exception if an error occurs
        raise Exception(f"Error querying table: {e}")
        