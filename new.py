# /Users/ball/Git_apcball/Project1/new.py

import pandas as pd

# Define the input and output file paths
input_file = '/Users/ball/Git_apcball/Project1/Data_file/import_bom.xlsx'
output_file = '/Users/ball/Git_apcball/Project1/Data_file/flat_data_template.xlsx'

# Step 1: Read the Excel file
try:
    df = pd.read_excel(input_file)
except FileNotFoundError:
    print(f"Error: The file {input_file} does not exist.")
    exit(1)

# Step 2: Ensure the columns match the required format
# Assuming the original data has columns: 'Product Template', 'Product Variant', 'Component', 'Quantity', 'UoM', 'Operation'
# If the columns are different, rename them to match the required format

# Example of renaming columns
df = df.rename(columns={
    'Part Number': 'Product Template',
    'Variant': 'Product Variant',
    'Component': 'Component',
    'Qty': 'Quantity',
    'Unit': 'UoM',
    'Operation': 'Operation'
})

# Step 3: Create a copy of the data (no transformation needed)
flat_df = df.copy()

# Step 4: Write the flat data to a new Excel file
flat_df.to_excel(output_file, index=False)

print(f"Flat data has been written to {output_file}")