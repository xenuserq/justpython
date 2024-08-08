#Imports
import sys,os
import pandas as pd


def cleandt(input_csv_path, output_csv_path):
    print("Current working directory:", os.getcwd())
#import data
    dfunclean=pd.read_csv(input_csv_path) #Load CSV file check null entries and types

    print(dfunclean.info()) 
    print(dfunclean.head())

# Drop reference column and other not relevant columns
    dfunclean.drop(columns=['reference', 'mileage2','engine size2','fuel type2'], inplace=True)
    print(dfunclean.head())

# r'[^\d]' matches any character that is not a digit (0-9) and replaces it with an empty string '', effectively removing all non-digit characters.
    dfunclean['price'] = dfunclean['price'].str.replace(r'[^\d]', '', regex=True)
# Convert values to numeric, setting errors to 'coerce' which will convert invalid parsing to NaN
    dfunclean['price'] = pd.to_numeric(dfunclean['price'], errors='coerce')
    dfunclean = dfunclean.dropna(subset=['price'])
    dfunclean['price'] = dfunclean['price'].astype(int)


    dfunclean['mileage'] = dfunclean['mileage'].str.replace(r'[^\d.]', '', regex=True)
    dfunclean['mileage'] = pd.to_numeric(dfunclean['mileage'], errors='coerce')
# Calculate the mean of the 'mileage' column
    median_mean = dfunclean['mileage'].mean()

# Replace NaN values with the median value
    dfunclean['mileage'] = dfunclean['mileage'].fillna(median_mean).astype(int)

#Engine size
    dfunclean['engine size'] = dfunclean['engine size'].str.replace(r'[^\d.]', '', regex=True)
    dfunclean['engine size'] = pd.to_numeric(dfunclean['engine size'], errors='coerce')
    dfunclean = dfunclean.dropna(subset=['engine size'])

#Normalize Values: for better understading I will divide to 3 lambda expressions but this affects speed of calculation
    dfunclean['engine size'] = dfunclean['engine size'].apply(lambda x: x / 1000 if x >= 900 else x)

    dfunclean['engine size'] = dfunclean['engine size'].apply(lambda x: x / 100 if 100 <= x < 331 else x)

    dfunclean['engine size'] = dfunclean['engine size'].apply(lambda x: x / 10 if 10 <= x < 31 else x)

# Drop rows where 'engine size' is 0
    dfunclean = dfunclean[dfunclean['engine size'] != 0]
# Round 'engine size' to one decimal
    dfunclean['engine size'] = dfunclean['engine size'].round(1)

#year
    dfunclean['year'] = pd.to_numeric(dfunclean['year'], errors='coerce')
    dfunclean = dfunclean.dropna(subset=['year'])
    dfunclean['year'] = dfunclean['year'].astype(int)

#fuel type (under construction)
    dfunclean['numeric_conversion'] = pd.to_numeric(dfunclean['fuel type'], errors='coerce')
# # Keep rows where 'numeric_conversion' is NaN, meaning the original value was a string or NaN
    dfunclean = dfunclean[dfunclean['numeric_conversion'].isna()]

# Drop the helper column
    dfunclean = dfunclean.drop(columns=['numeric_conversion'])
 
    dfunclean = dfunclean.dropna(subset=['fuel type'])


#price outlier , change





#reset dataframe default index and drop null values
    dfunclean = dfunclean.reset_index(drop=True)
    dfunclean = dfunclean.dropna()

#Check if all values belong to a valid range in the context of the dataset. 
    print(dfunclean['year'].min())
    print(dfunclean['year'].max())
    print(dfunclean['transmission'].value_counts())
    print(dfunclean['fuel type'].value_counts())
    print(dfunclean['engine size'].value_counts())
    print(dfunclean['model'].value_counts())
    print(dfunclean['price'].value_counts())
    print(dfunclean['price'].max())
    print(dfunclean.head(50))
    print(dfunclean.info())

#save clean data to new CSV file
    dfunclean.to_csv(output_csv_path, index=False)
    
    







