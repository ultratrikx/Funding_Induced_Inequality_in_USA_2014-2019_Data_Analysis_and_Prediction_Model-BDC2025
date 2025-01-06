import pandas as pd

# Read the CSV files
fiscal_df = pd.read_csv('./data/18-19/fiscal.csv')
master_df = pd.read_csv('./data/18-19/merged_district_data.csv')

# Merge the dataframes on LEAID
merged_df = pd.merge(
    master_df,
    fiscal_df,
    on='LEAID',
    how='left'
)

# Save the merged result
merged_df.to_csv('merged_output.csv', index=False)
