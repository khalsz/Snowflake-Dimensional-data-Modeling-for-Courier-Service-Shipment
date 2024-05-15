from sqlalchemy import text
import pandas as pd


def query_table(query_statement: str, connection) -> pd.DataFrame:
  """
  Executes a SQL query statement and returns the result as a DataFrame.

  Args:
      query_statement (str): SQL query statement to execute.
      connection (Any): A database connection object (can vary depending on the database library).

  Returns:
      pd.DataFrame: DataFrame containing the query result.

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
      # Raise an exception if an error occurs with a more informative message
      raise Exception(f"Error executing query: {query_statement}. Details: {e}")
        