from Extract import *
from Transform import *
from Load import *

# Setup connection.
cursor, connection = create_connection('localhost','pyShern','123456','epl_db')

def main(file, season):
    ####################################### EXTRACT #######################################
    df = extract_updated(file)

    ####################################### TRANSFORM #######################################
    # Rename Header, remove empty columns and drop betting-related columns.
    df = update_keep_cols(df)
    
    # Add field "season" to all dataframes
    df['Season'] = season

    # Rename columns.
    rename_cols(df)

    # Convert dtypes.
    df = change_col_dtype(df)

    # Filter table for all games after latest game in DB.
    # Retrieve info for latest game in database.
    latest_game = latest_info(cursor)[0]

    # Index for latest game.
    ind_latest_game = df.loc[(df['HomeTeam']==latest_game[1]) & (df['AwayTeam'] == latest_game[2]) & 
    (df['Season'] == latest_game[-1])].index.values
    
    start = ind_latest_game[0] + 1


    # Index for last entry.
    end = df.shape[0] - 1

    # Subset table to include only games not already present in DB.
    df = df.loc[start:end,df.columns.tolist()]

    # Split data frame into tables based on ERD.
    tables = split_dataframe(df)

    # ####################################### LOAD #######################################
    
    for table in tables:
        update(table,cursor,connection)
    connection.close()


if __name__ == "__main__":
    main('E0.csv', '2020_2021')