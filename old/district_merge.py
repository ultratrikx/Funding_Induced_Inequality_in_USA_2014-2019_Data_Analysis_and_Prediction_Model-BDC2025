import pandas as pd

# Read the CSV files with low_memory=False
ell_df = pd.read_csv('data/18-19/District/ELL.csv', low_memory=False)
disabilities_df = pd.read_csv('data/18-19/District/Disabilities.csv', low_memory=False) 
directory_df = pd.read_csv('data/18-19/District/Directory District.csv', low_memory=False)
# staff_df = pd.read_csv('data/18-19/District/Staff District.csv')

# Merge all dataframes using LEAID as the key
# First merge ELL and disabilities
merged_df = pd.merge(ell_df, disabilities_df, on='LEAID', how='outer')

# marged_df = pd.merge(merged_df, staff_df, how='outer')

# Then merge with directory
final_df = pd.merge(merged_df, directory_df, on='LEAID', how='outer')

# Remove duplicate columns if any exist
final_df = final_df.loc[:,~final_df.columns.duplicated()]

# Save to new CSV file
final_df.to_csv('data/18-19/District/merged_district_data.csv', index=False)

print(f"Merged CSV created with {len(final_df.columns)} columns and {len(final_df)} rows")
