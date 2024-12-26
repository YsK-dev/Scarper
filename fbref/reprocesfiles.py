import numpy as np

import pandas as pd


# File paths 
filePaths = [
    '/Users/ysk/Desktop/fbref/super lig/super-lig_2020-2021_fixture_data.csv',
    '/Users/ysk/Desktop/fbref/super lig/super-lig_2019-2020_fixture_data.csv',
    '/Users/ysk/Desktop/fbref/super lig/super-lig_2018-2019_fixture_data.csv',
    '/Users/ysk/Desktop/fbref/super lig/super-lig_2017-2018_fixture_data.csv',
    '/Users/ysk/Desktop/fbref/super lig/super-lig_2021-2022_fixture_data.csv',
    '/Users/ysk/Desktop/fbref/super lig/super-lig_2022-2023_fixture_data.csv',
    '/Users/ysk/Desktop/fbref/super lig/super-lig_2023-2024_fixture_data.csv',
    '/Users/ysk/Desktop/fbref/super lig/super-lig_2024-2025_fixture_data.csv',

]

# Retry processing with corrected game_id assignment
processedFiles = []
targetColumns = ['Wk', 'Day', 'Date', 'Time', 'Home', 'Away', 'xG', 'xG.1', 'Score', 'season', 'game_id']#for every columns to be same 

for i, filePath in enumerate(filePaths):
    # Load the data to the csv
    df = pd.read_csv(filePath)
    
    # Ensure the necessary columns exist because in scarped data there is no xg value for super lig
    for col in ['xG', 'xG.1']:
        if col not in df.columns:
            # Assign xG values  having wrong value is better for  demonstration
            df[col] = [round(x, 2) for x in np.random.uniform(0.5, 3.0, len(df))]
    
    # Ensure all target columns are present; fill missing ones with NaN or appropriate data
    for col in targetColumns:
        if col not in df.columns:
            df[col] = pd.NA
    
    # Reorder columns
    df = df[targetColumns]
    
    # Assign season and game_id if missing it was missing in some csv
    season = f"{2017 + i}-{2018 + i}"
    df['season'] = df['season'].fillna(season)
    df['game_id'] = df['game_id'].fillna(pd.Series(range(len(df))))
    
    # Save the processed file to a new path
    processed_file_path = filePath.replace('.csv', '_processed.csv')
    df.to_csv(processed_file_path, index=False)
    processedFiles.append(processed_file_path)

