import pandas as pd

# Read the Excel file
df = pd.read_excel('Data_file/import_PO_update.xlsx')

# Display the column names
print("\nColumns in the Excel file:")
print(df.columns.tolist())

# Display unique values in picking_type_id column
print("\nUnique values in picking_type_id column:")
print(df['picking_type_id'].unique())

# Display first few rows of relevant columns
print("\nFirst few rows with picking_type_id:")
print(df[['name', 'picking_type_id']].head())