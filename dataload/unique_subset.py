import pandas as pd

def create_unique_id_with_subset(df: pd.DataFrame, subset_cols: list[str], unique_col: str, 
                                  new_column: str, start_pk_num: int) -> pd.DataFrame:
    
  """Creates a new column with unique IDs assigned within groups defined by a subset of columns.

  This function takes a DataFrame, a list of column names (subset_cols), a column name 
  representing a unique identifier within groups (unique_col), a name for the new ID column 
  (new_column), and a starting value for the IDs (start_pk_num). It returns a new DataFrame 
  with the original columns and a new column containing unique IDs for each group defined 
  by the subset of columns.

  Args:
      df (pd.DataFrame): The DataFrame containing the data.
      subset_cols (list[str]): A list of column names to define the groups for unique IDs.
      unique_col (str): The name of a column that uniquely identifies rows within each group.
      new_column (str): The name of the new column to store the unique IDs.
      start_pk_num (int): The starting value for the unique IDs.

  Returns:
      pd.DataFrame: The modified DataFrame with a new column for unique IDs within groups.
  """

  # Subset DataFrame based on columns and keep the first occurrence for each unique group
  new_df = df[subset_cols].drop_duplicates(subset=unique_col, keep="first").reset_index(drop=True)

  # Create a new column with unique IDs based on DataFrame length and unique column value within the group
  new_df[new_column] = range(start_pk_num, start_pk_num + len(new_df))

  return new_df