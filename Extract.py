import pandas as pd

# Assign dataset names with the format: <start season>_<end season>
def create_names(start_season, end_season):
    df_names =[]
    while start_season != end_season:
        df_names.append('_'.join([str(start_season),str(start_season+1)]))
        start_season+=1
    return df_names

# Append datasets into the list
def create_dataframes(list_of_names):
    dataframes_by_season = []
    for i in range(len(list_of_names)):
        temp_df = pd.read_csv("EPL Datasets/"+list_of_names[i]+".csv",sep=',',names = list(range(0,110)))
        dataframes_by_season.append(temp_df)
    return dataframes_by_season
        


