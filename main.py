from Extract import *
from Transform import *
from Load import *

########################################## Printing Options ##########################################

# Print all columns.
pd.set_option('display.max_columns', None)

def main():
    ####################################### EXTRACT #######################################
    df_names_list = create_names(2000,2021)
    df_by_season = create_dataframes(df_names_list)

    ####################################### TRANSFORM #######################################
    # Rename Header, remove empty columns and drop betting-related columns.
    for dfs in range(0,len(df_by_season)):
        df_by_season[dfs] = rename_header(df_by_season[dfs])
        df_by_season[dfs] = remove_nan_cols(df_by_season[dfs])
        df_by_season[dfs] = drop_bad_cols(df_by_season[dfs])
        try:
            good_cols = get_intersect(good_cols,df_by_season[dfs].columns.values)
        except:
            good_cols = df_by_season[dfs].columns.values

    # Keep only columns that are present in all seasons.
    for dfs in range(0,len(df_by_season)):
        df_by_season[dfs] = drop_unique_cols(df_by_season[dfs],good_cols)
    
    # Add field "season" to all dataframes
    df_by_season = addfield_season(df_by_season)

    # Combine all seasons into one dataframe.
    epl_data = combine_dataframes(df_by_season)

    # Rename columns.
    rename_cols(epl_data)

    # Convert dtypes.
    epl_data = change_col_dtype(epl_data)

    # Split data frame into tables based on ERD.
    tables = split_dataframe(epl_data)

    ####################################### LOAD #######################################
    cursor, connection = create_connection('localhost','pyShern','123456','epl_db')
    for table in tables:
        insert(table,cursor,connection)
    connection.close()


if __name__ == "__main__":
    main()