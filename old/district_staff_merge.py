import pandas as pd

# Read the existing merged district data
final_df = pd.read_csv('data/18-19/District/merged_district_data.csv')

# Read staff data
staff_df = pd.read_csv('data/18-19/District/Staff District.csv')

# Pivot staff data to convert staff types to columns using correct column names
staff_pivoted = staff_df.pivot(
    index='LEAID',
    columns='STAFF',
    values='STAFF_COUNT'
).reset_index()

# Merge pivoted staff data with district data
final_df = pd.merge(final_df, staff_pivoted, on='LEAID', how='left')

# Remove any duplicate columns
final_df = final_df.loc[:,~final_df.columns.duplicated()]

# Save merged data
final_df.to_csv('data/18-19/District/merged_district_data.csv', index=False)

print(f"Added {len(staff_pivoted.columns)-1} staff type columns to district data")  # -1 to exclude LEAID
print(f"Final dataset has {len(final_df.columns)} columns and {len(final_df)} rows")

