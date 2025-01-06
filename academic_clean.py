import pandas as pd
import numpy as np

years = ['14-15', '15-16', '16-17', '17-18']

# Read the CSV file
def process_year(year):
    df = pd.read_csv(f'./data/{year}/academic_performance_{year}.csv')
    
    # Identify numeric columns (excluding sedaadmin and subject)
    numeric_columns = df.select_dtypes(include=[np.number]).columns
    columns_to_avg = [col for col in numeric_columns if col not in ['sedaadmin']]

    # Group by sedaadmin and subject, then calculate means for numeric columns only
    grouped_df = df.groupby(['sedaadmin', 'subject'])[columns_to_avg].mean().reset_index()

    # Write to new CSV file
    grouped_df.to_csv(f'./data/{year}/academic_performance_{year}_averaged.csv', index=False)

# Process each year
for year in years:
    process_year(year)