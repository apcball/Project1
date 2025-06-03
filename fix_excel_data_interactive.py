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
    
    # Fix scheduled_date
    if 'scheduled_date' in df.columns:
        print("\nFixing scheduled_date column...")
        print(f"Original scheduled_date data type: {df['scheduled_date'].dtype}")
        print(f"Sample values: {df['scheduled_date'].head(3)}")
        
        fix_option = input("\nDo you want to:\n1. Convert all scheduled_date values to today's date\n2. Enter a specific date for all rows\n3. Keep current values but ensure proper format\nEnter choice (1/2/3): ")
        
        if fix_option == '1':
            today = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            df['scheduled_date'] = today
            print(f"All scheduled_date values set to: {today}")
        elif fix_option == '2':
            date_str = input("Enter date in format YYYY-MM-DD: ")
            try:
                date_obj = datetime.strptime(date_str, '%Y-%m-%d')
                formatted_date = date_obj.strftime('%Y-%m-%d %H:%M:%S')
                df['scheduled_date'] = formatted_date
                print(f"All scheduled_date values set to: {formatted_date}")
            except ValueError:
                print("Invalid date format. Using current values but ensuring proper format.")
                df['scheduled_date'] = pd.to_datetime(df['scheduled_date'], errors='coerce')
                df['scheduled_date'] = df['scheduled_date'].dt.strftime('%Y-%m-%d %H:%M:%S')
        else:  # Option 3 or any other input
            df['scheduled_date'] = pd.to_datetime(df['scheduled_date'], errors='coerce')
            df['scheduled_date'] = df['scheduled_date'].dt.strftime('%Y-%m-%d %H:%M:%S')
            print("Kept current values but ensured proper format")
    
    # Fix location_dest_id
    if 'location_dest_id' in df.columns:
        print("\nFixing location_dest_id column...")
        unique_locations = df['location_dest_id'].unique()
        print(f"Current unique location_dest_id values: {unique_locations}")
        
        fix_option = input("\nDo you want to:\n1. Replace a specific location value\n2. Set all locations to a single value\n3. Keep current values\nEnter choice (1/2/3): ")
        
        if fix_option == '1':
            old_value = input("Enter the location value to replace: ")
            new_value = input("Enter the new location value: ")
            df['location_dest_id'] = df['location_dest_id'].replace(old_value, new_value)
            print(f"Replaced '{old_value}' with '{new_value}'")
        elif fix_option == '2':
            new_value = input("Enter the location value to use for all rows: ")
            df['location_dest_id'] = new_value
            print(f"All location_dest_id values set to: {new_value}")
        else:  # Option 3 or any other input
            print("Kept current location values")
    
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