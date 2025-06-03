#!/usr/bin/env python3
import pandas as pd
import os
from datetime import datetime

# Excel file paths
INPUT_FILE = 'Data_file/import_fifo_stock_ob.xlsx'
OUTPUT_FILE = 'Data_file/import_fifo_stock_ob_fixed.xlsx'

# Common location name corrections
LOCATION_CORRECTIONS = {
    # Add your specific corrections here, for example:
    'FG50/Stock': 'FG50/Stock',  # Keep as is if correct
    # If you need to fix specific location names, uncomment and modify these lines:
    # 'Wrong Name': 'Correct Name',
}

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
    
    # 1. Fix scheduled_date - Convert to proper datetime format
    if 'scheduled_date' in df.columns:
        print("\nFixing scheduled_date column...")
        print(f"Original scheduled_date data type: {df['scheduled_date'].dtype}")
        
        # Set all dates to today if they're invalid
        today = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        df['scheduled_date'] = pd.to_datetime(df['scheduled_date'], errors='coerce')
        df.loc[df['scheduled_date'].isna(), 'scheduled_date'] = pd.to_datetime(today)
        
        # Format all dates consistently
        df['scheduled_date'] = df['scheduled_date'].dt.strftime('%Y-%m-%d %H:%M:%S')
        print(f"Fixed scheduled_date data type: {df['scheduled_date'].dtype}")
        print(f"Sample values after fix: {df['scheduled_date'].head(3)}")
    
    # 2. Fix location_dest_id - Apply corrections if needed
    if 'location_dest_id' in df.columns:
        print("\nFixing location_dest_id column...")
        unique_locations = df['location_dest_id'].unique()
        print(f"Original unique location_dest_id values: {unique_locations}")
        
        # Apply corrections from the dictionary
        for old_name, new_name in LOCATION_CORRECTIONS.items():
            df['location_dest_id'] = df['location_dest_id'].replace(old_name, new_name)
            
        # After corrections
        unique_locations_after = df['location_dest_id'].unique()
        print(f"Location values after corrections: {unique_locations_after}")
    
    # 3. Fix date_done if present
    if 'date_done' in df.columns:
        print("\nFixing date_done column...")
        df['date_done'] = pd.to_datetime(df['date_done'], errors='coerce')
        df.loc[df['date_done'].isna(), 'date_done'] = pd.to_datetime(today)
        df['date_done'] = df['date_done'].dt.strftime('%Y-%m-%d %H:%M:%S')
        print(f"Fixed date_done data type: {df['date_done'].dtype}")
    
    # Save the fixed file
    df.to_excel(OUTPUT_FILE, index=False)
    print(f"\nFixed Excel file saved to: {OUTPUT_FILE}")
    
    # Show summary of changes
    print("\nSummary of fixed data (first 3 rows):")
    print(df.head(3))
    
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