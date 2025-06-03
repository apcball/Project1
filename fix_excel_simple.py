#!/usr/bin/env python3
import pandas as pd
from datetime import datetime
import os

# Excel file paths
INPUT_FILE = 'Data_file/import_fifo_stock_ob.xlsx'
OUTPUT_FILE = 'Data_file/import_fifo_stock_ob_fixed.xlsx'

def fix_excel_file():
    # Check if file exists
    if not os.path.exists(INPUT_FILE):
        print(f"Error: File not found: {INPUT_FILE}")
        return False
    
    # Read the Excel file
    print(f"Reading Excel file: {INPUT_FILE}")
    df = pd.read_excel(INPUT_FILE)
    
    # Display original data
    print(f"\nOriginal data (first 3 rows):")
    print(df.head(3))
    print(f"\nOriginal columns: {list(df.columns)}")
    
    # 1. Fix scheduled_date - Make sure it's in the correct format
    if 'scheduled_date' in df.columns:
        print("\nFixing scheduled_date...")
        # Convert to datetime and then to string in the correct format
        df['scheduled_date'] = pd.to_datetime(df['scheduled_date'], errors='coerce')
        # Replace any NaT (Not a Time) values with current date
        df.loc[df['scheduled_date'].isna(), 'scheduled_date'] = datetime.now()
        # Convert to string in the format expected by Odoo
        df['scheduled_date'] = df['scheduled_date'].dt.strftime('%Y-%m-%d %H:%M:%S')
    
    # 2. Fix location_dest_id - Make sure it matches exactly what's in Odoo
    if 'location_dest_id' in df.columns:
        print("\nFixing location_dest_id...")
        print(f"Original location values: {df['location_dest_id'].unique()}")
        
        # You can add specific replacements here if needed
        # For example:
        # df['location_dest_id'] = df['location_dest_id'].replace('Wrong Name', 'Correct Name')
        
        # Make sure there are no leading/trailing spaces
        df['location_dest_id'] = df['location_dest_id'].str.strip()
    
    # 3. Fix date_done if present
    if 'date_done' in df.columns:
        print("\nFixing date_done...")
        df['date_done'] = pd.to_datetime(df['date_done'], errors='coerce')
        df.loc[df['date_done'].isna(), 'date_done'] = datetime.now()
        df['date_done'] = df['date_done'].dt.strftime('%Y-%m-%d %H:%M:%S')
    
    # 4. Fix picking_type_id if needed
    if 'picking_type_id' in df.columns:
        print("\nFixing picking_type_id...")
        df['picking_type_id'] = df['picking_type_id'].replace('My Company: OB FIFO', 'OB FIFO')
    
    # Save the fixed file
    df.to_excel(OUTPUT_FILE, index=False)
    print(f"\nFixed Excel file saved to: {OUTPUT_FILE}")
    
    # Display fixed data
    print(f"\nFixed data (first 3 rows):")
    print(df.head(3))
    
    return True

def main():
    print("Starting Excel file fix process...")
    success = fix_excel_file()
    if success:
        print("\nProcess completed successfully.")
        print(f"You can now use the fixed file: {OUTPUT_FILE}")
        print("Run your import script with this file:")
        print(f"python import_fifo_stock_ob.py")
    else:
        print("Process failed.")

if __name__ == "__main__":
    main()