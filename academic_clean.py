import pandas as pd
import numpy as np

# Read the CSV file
df = pd.read_csv('./data/18-19/academic_performance_2018.csv')

# Identify numeric columns (excluding sedaadmin and subject)
numeric_columns = df.select_dtypes(include=[np.number]).columns
columns_to_avg = [col for col in numeric_columns if col not in ['sedaadmin']]

# Group by sedaadmin and subject, then calculate means for numeric columns only
grouped_df = df.groupby(['sedaadmin', 'subject'])[columns_to_avg].mean().reset_index()

# Write to new CSV file
grouped_df.to_csv('./data/18-19/academic_performance_2018_averaged.csv', index=False)
