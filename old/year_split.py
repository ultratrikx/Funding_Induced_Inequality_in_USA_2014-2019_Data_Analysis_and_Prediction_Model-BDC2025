import pandas as pd

# Read the CSV file
df = pd.read_csv('./data/2009-2019 Academic Performance.csv')

# Convert year column to int if needed (adjust column name as necessary)
# Assuming your year column is named 'year'
df['year'] = pd.to_numeric(df['year'], errors='coerce')

# Group by year and save to separate files
for year, year_df in df.groupby('year'):
    output_file = f'academic_performance_{int(year)}.csv'
    year_df.to_csv(output_file, index=False)
    print(f'Created file: {output_file}')
