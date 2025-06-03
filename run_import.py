import pandas as pd

# Read Excel file
df = pd.read_excel('Data_file/import_fifo_stock_ob.xlsx')

# Print detailed information about the DataFrame
print("DataFrame Information:")
print("Columns:", list(df.columns))
print("\nFirst few rows:\n", df.head())
print("\nData Types:\n", df.dtypes)
print("\nTotal Rows:", len(df))