# Import necessary packages
import pandas as pd
import os

# Navigate to the folder where your CSV files are 
os.chdir('/Users/ysk/Desktop/fbref/data/allfixture')

# Create an empty dataframe
df = pd.DataFrame([])

# Read all CSV files and append them to df
for root, dirs, files in os.walk("."):
   for name in files:
       df_temp = pd.read_csv(name, encoding='latin1')

       df = pd.concat([df,df_temp])

# Save df to a CSV file
df.to_csv('all-csv-files.csv')