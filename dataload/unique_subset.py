def create_unique_id_with_subset(df, subset_cols, unique_col, new_column, start_pk_num): 
    
    # subsetting dataframe based on columns and unique set of a column 
    new_df = df[subset_cols].drop_duplicates(subset = unique_col, keep="first").reset_index(drop = True)
    
    # creating index column based on new dataframe lenght + unique column value.
    new_df[new_column] = range(start_pk_num, start_pk_num+len(new_df))
    # if type(unique_col) == list: 
    #     new_df[new_column] = new_df[new_column].astype(str) + new_df[unique_col[0]].astype(str)
    # else:     
    #     new_df[new_column] = new_df[new_column].astype(str) + new_df[unique_col].astype(str)
    return new_df

