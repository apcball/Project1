#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pandas as pd

# Read the Excel file
df = pd.read_excel('Import_SO/Template_SO.xlsx')

print("Warehouse ID values:")
print(df['warehouse_id'].value_counts())

print("\nUnique warehouse_id values:")
for val in df['warehouse_id'].unique():
    print(f"Value: {repr(val)}")
    print(f"Type: {type(val)}")
    print(f"Length: {len(str(val))}")
    print("---")