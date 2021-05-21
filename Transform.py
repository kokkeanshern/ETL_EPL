from typing import AsyncIterable
import pandas as pd
from pandas.core.construction import create_series_with_explicit_dtype
import numpy as np

# Reassigns the first row of data as the header
def rename_header(df):
    # Uses the first row as header
    new_header = df.iloc[0]
    # Sets the data as the row after the header onwards
    df = df[1:]
    #Sets the header row as the df header
    df.columns = new_header

    return df

# Drop the columns which are named Nan
def remove_nan_cols(df):
    df = df.loc[:, df.columns.notnull()]
    return df

# Remove the columns which are not required for analysis.
def drop_bad_cols(df):
    bad_cols = ['B365H','B365D','B365A','BSH','BSD','BSA','BWH','BWD','BWA','GBH','GBD','GBA','IWH','IWD',
                'IWA','LBH','LBD','LBA','PSH','PSD','PSA','SOH','SOD','SOA','SBH','SBD','SBA','SJH','SJD',
                'SJA','SYH','SYD','SYA','VCH','VCD','VCA','WHH','WHD','WHA','Bb1X2','BbMxH','BbAvH','BbMxD',
                'BbAvD','BbMxA','BbAvA','MaxH','MaxD','MaxA','AvgH','AvgD','AvgA','BbOU','BbMx>2.5','BbAv>2.5',
                'BbMx<2.5','BbAv<2.5','GB>2.5','GB<2.5','B365>2.5','B365<2.5','P>2.5','P<2.5','Max>2.5','Max<2.5',
                'Avg>2.5','Avg<2.5','BbAH','BbAHh','AHh','BbMxAHH','BbAvAHH','BbMxAHA','BbAvAHA','GBAHH','GBAHA',
                'GBAH','LBAHH','LBAHA','LBAH','B365AHH','B365AHA','B365AH','PAHH','PAHA','MaxAHH','MaxAHA','AvgAHH',
                'AvgAHA','Div','B365C>2.5','WHCA','B365CD','PSCH','WHCD','PCAHH','BWCH','PSCA','AHCh','IWCA',
                'AvgCAHA','MaxCAHA' ,'VCCH' ,'WHCH' ,'BWCD' ,'B365CAHH' ,'PSCD','B365CA' ,'B365CAHA' ,
                'PCAHA' ,'B365CH' ,'BWCA' ,'MaxCA' ,'PC<2.5' ,'IWCD','VCCA' ,'AvgCA' ,'IWCH' ,'AvgCH' ,
                'VCCD' ,'AvgCD' ,'AvgCAHH' ,'AvgC>2.5','MaxCAHH' ,'MaxCD' ,'MaxC<2.5' ,'AvgC<2.5' ,
                'MaxC>2.5' ,'PC>2.5' ,'B365C<2.5','MaxCH']
    good_cols = list(set(df.columns.values) - set(bad_cols))
    df = df[good_cols]
    return df

# Get the columns that are shared among every single season.
def get_intersect(colnamesA, colnamesB):
    good_cols = list(set(colnamesA) & set(colnamesB))
    return good_cols

# Remove columns that are not present in all dataframes.
def drop_unique_cols(df, good_calls):
    df = df[good_calls]
    return df

# Merge all seasons into one dataframe.
def combine_dataframes(df_by_season):
    return pd.concat(df_by_season)

# Add season as a column.
def addfield_season(df_by_season):
    start_season = 2000
    for dfs in range(0,len(df_by_season)):
        df_by_season[dfs]['Season'] = '_'.join([str(start_season),str(start_season+1)])
        start_season += 1
    return df_by_season

# Rename columns.
def rename_cols(epl_data):
    epl_data.rename(columns = {"HF":"HomeFouls","AR":"AwayRedCards","HTHG":"HalfTimeHomeGoals",
                               "FTHG":"FullTimeHomeGoals","HTR":"HalfTimeResult","FTAG":"FullTimeAwayGoals",
                               "HS":"HomeShots","AS":"AwayShots","AF":"AwayFouls", "HY":"HomeYellowCards",
                               "AC":"AwayCorners","HTAG":"HalfTimeAwayGoals","FTR":"FullTimeResult",
                               "HR":"HomeRedCards","AY":"AwayYellowCards","HC":"HomeCorners",
                               "HST":"HomeShotsOnTarget","AST":"AwayShotsOnTarget"}, inplace = True)

# Change column data types.
def change_col_dtype(epl_data):
    epl_data['HalfTimeAwayGoals'] = epl_data['HalfTimeAwayGoals'].astype('int')
    epl_data['HalfTimeHomeGoals'] = epl_data['HalfTimeHomeGoals'].astype('int')
    epl_data['AwayFouls'] = epl_data['AwayFouls'].astype('int')
    epl_data['HomeCorners'] = epl_data['HomeCorners'].astype('int')
    epl_data['HomeFouls'] = epl_data['HomeFouls'].astype('int')
    epl_data['FullTimeAwayGoals'] = epl_data['FullTimeAwayGoals'].astype('int')
    epl_data['HomeShotsOnTarget'] = epl_data['HomeShotsOnTarget'].astype('int')
    epl_data['AwayShots'] = epl_data['AwayShots'].astype('int')
    epl_data['AwayCorners'] = epl_data['AwayCorners'].astype('int')
    epl_data['HomeShots'] = epl_data['HomeShots'].astype('int')
    epl_data['HomeRedCards'] = epl_data['HomeRedCards'].astype('int')
    epl_data['AwayShotsOnTarget'] = epl_data['AwayShotsOnTarget'].astype('int')
    epl_data['AwayRedCards'] = epl_data['AwayRedCards'].astype('int')
    epl_data['AwayYellowCards'] = epl_data['AwayYellowCards'].astype('int')
    epl_data['HomeYellowCards'] = epl_data['HomeYellowCards'].astype('int')
    epl_data['FullTimeHomeGoals'] = epl_data['FullTimeHomeGoals'].astype('int')

    epl_data['Date1'] = pd.to_datetime(epl_data['Date'],format="%d/%m/%y",errors='coerce')
    bool_isnull = epl_data['Date1'].isnull()
    epl_data.loc[bool_isnull,'Date1'] = pd.to_datetime(epl_data[bool_isnull]['Date'],format = "%d/%m/%Y")

    epl_data = epl_data.drop(columns = ['Date'])
    epl_data.rename(columns = {"Date1":"Date"},inplace = True)
    return epl_data

# Split dataframe based on ERD.
def split_dataframe(epl_data):
    # Create ID primary key.
    id = [i for i in range(1,epl_data.shape[0]+1)]
    epl_data['ID'] = id

    # Split into many tables.
    games = epl_data[['ID','HomeTeam','AwayTeam','Date','Season']]
    cards = epl_data[['ID','HomeYellowCards','AwayYellowCards','HomeRedCards',
                      'AwayRedCards']]
    results = epl_data[['ID','HalfTimeResult','FullTimeResult']]
    goals = epl_data[['ID','HalfTimeHomeGoals','HalfTimeAwayGoals',
                      'FullTimeHomeGoals','FullTimeAwayGoals']]
    referee = epl_data[['ID','Referee']]
    shots = epl_data[['ID','HomeShots','HomeShotsOnTarget','AwayShots','AwayShotsOnTarget']]
    fouls = epl_data[['ID','HomeFouls','AwayFouls']]
    corners = epl_data[['ID','HomeCorners','AwayCorners']]

    # Assign names to each table.
    games.name = 'games'
    cards.name = 'cards'
    results.name = 'results'
    goals.name = 'goals'
    referee.name = 'referee'
    shots.name = 'shots'
    fouls.name = 'fouls'
    corners.name = 'corners'

    tables = [games, cards, results, goals, referee, shots, fouls, corners]

    return tables

# Keep only these columns in the update module.
def update_keep_cols(df):
    good_cols = ["HF","AR","HTHG","FTHG","HTR","FTAG","HS","AS","AF","HY","AC",
                 "HTAG","FTR","HR","AY","HC","HST","AST",'Date','HomeTeam','AwayTeam',
                 'Referee']
    df = df[good_cols]
    return df
