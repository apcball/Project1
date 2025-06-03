#!/usr/bin/env python3
import pandas as pd
import os
from datetime import datetime

# Excel file paths
INPUT_FILE = 'Data_file/import_fifo_stock_ob.xlsx'
OUTPUT_FILE = 'Data_file/import_fifo_stock_ob_fixed.xlsx'

def fix_excel_file():
    print(f"Reading Excel file: {INPUT_FILE}")
    if not os.path.exists(INPUT_FILE):
        print(f"Error: File not found: {INPUT_FILE}")
        return False
    
    # Read the Excel file
    df = pd.read_excel(INPUT_FILE)
    print(f"Original columns: {list(df.columns)}")
    print(f"Number of rows: {len(df)}")
    
    # Display sample data
    print("\nSample data (first 3 rows):")
    print(df.head(3))
    
    # Fix scheduled_date if needed
    if 'scheduled_date' in df.columns:
        print("\nFixing scheduled_date column...")
        print(f"Original scheduled_date data type: {df['scheduled_date'].dtype}")
        # Convert to datetime if not already
        if not pd.api.types.is_datetime64_any_dtype(df['scheduled_date']):
            try:
                df['scheduled_date'] = pd.to_datetime(df['scheduled_date'], errors='coerce')
                print("Converted scheduled_date to datetime format")
            except Exception as e:
                print(f"Error converting scheduled_date: {str(e)}")
        # Format the date properly
        df['scheduled_date'] = df['scheduled_date'].dt.strftime('%Y-%m-%d %H:%M:%S')
        print(f"Fixed scheduled_date data type: {df['scheduled_date'].dtype}")
    
    # Fix location_dest_id if needed
    if 'location_dest_id' in df.columns:
        print("\nFixing location_dest_id column...")
        print(f"Original location_dest_id values: {df['location_dest_id'].unique()}")
        # Here you can add specific replacements if needed
        # For example: df['location_dest_id'] = df['location_dest_id'].replace('Wrong Name', 'Correct Name')
        print("Please check the location_dest_id values and update as needed")
    
    # Save the fixed file
    df.to_excel(OUTPUT_FILE, index=False)
    print(f"\nFixed Excel file saved to: {OUTPUT_FILE}")
    return True

def main():
    print("Starting Excel file fix process...")
    success = fix_excel_file()
    if success:
        print("Process completed successfully.")
    else:
        print("Process failed.")

if __name__ == "__main__":
    main()