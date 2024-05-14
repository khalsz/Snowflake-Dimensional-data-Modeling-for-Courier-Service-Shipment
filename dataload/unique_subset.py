def create_unique_id_with_subset(df, subset_cols, unique_col, new_column): 
    
    # subsetting dataframe based on columns and unique set of a column 
    new_df = df[subset_cols].drop_duplicates(subset = unique_col, keep="first").reset_index(drop = True)
    
    # creating index column based on new dataframe lenght + unique column value.
    new_df[new_column] = range(1, 1+len(new_df))
    new_df[new_column] = new_df[new_column].astype(str) + new_df[unique_col].astype(str)
    return new_df

