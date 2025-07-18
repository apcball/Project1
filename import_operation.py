#!/usr/bin/env python3
import xmlrpc.client
import pandas as pd
import sys
from datetime import datetime

# Odoo connection parameters
HOST = 'http://mogth.work:8069'
DB = 'MOG_LIVE'
USERNAME = 'apichart@mogen.co.th'
PASSWORD = '471109538'

def connect_to_odoo():
    """Establish connection to Odoo server"""
    print("Connecting to Odoo...")
    try:
        common = xmlrpc.client.ServerProxy(f'{HOST}/xmlrpc/2/common')
        uid = common.authenticate(DB, USERNAME, PASSWORD, {})
        if not uid:
            print("Authentication failed")
            return None, None
        models = xmlrpc.client.ServerProxy(f'{HOST}/xmlrpc/2/object')
        print("Connected to Odoo successfully")
        return uid, models
    except Exception as e:
        print(f"Failed to connect to Odoo: {str(e)}")
        return None, None

def read_excel_file(file_path):
    """Read the Excel file and return a cleaned pandas DataFrame"""
    try:
        # Read Excel file
        print(f"\nReading Excel file: {file_path}")
        df = pd.read_excel(file_path)
        
        # Print original columns for debugging
        print("\nOriginal Excel columns:", df.columns.tolist())
        
        # Verify required columns exist
        required_columns = ['Reference', 'operation', 'work_center', 'default_durations']
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            print(f"Error: Missing required columns: {missing_columns}")
            return None
            
        # Create a clean DataFrame with only required columns
        clean_df = pd.DataFrame({
            'product_id': df['Reference'],
            'name': df['operation'],
            'workcenter_id': df['work_center'],
            'time_cycle_manual': df['default_durations']
        })
        
        # Forward fill product_id (Reference)
        clean_df['product_id'] = clean_df['product_id'].fillna(method='ffill')
        
        # Remove rows where operation or workcenter is missing
        clean_df = clean_df.dropna(subset=['name', 'workcenter_id'])
        
        # Convert time_cycle_manual to float, replace NaN with 0.0
        clean_df['time_cycle_manual'] = pd.to_numeric(clean_df['time_cycle_manual'], errors='coerce').fillna(0.0)
        
        # Remove any remaining rows with NaN values
        clean_df = clean_df.dropna()
        
        print(f"\nProcessed {len(clean_df)} valid rows")
        print("\nSample of processed data:")
        print(clean_df.head())
        
        return clean_df
        
    except Exception as e:
        print(f"Error reading Excel file: {str(e)}")
        return None

def update_operation_bom(uid, models, data):
    """Update operation BOM based on product_id"""
    if data is None or len(data) == 0:
        print("No valid data to process")
        return 0, 0, 0
        
    success_count = 0
    error_count = 0
    skipped_count = 0
    
    current_product = None
    sequence_counter = 10
    
    # Get all workcenters once
    try:
        all_workcenters = models.execute_kw(DB, uid, PASSWORD,
            'mrp.workcenter', 'search_read',
            [[]], {'fields': ['id', 'name']}
        )
        workcenter_dict = {wc['name']: wc['id'] for wc in all_workcenters}
    except Exception as e:
        print(f"Error fetching workcenters: {str(e)}")
        return 0, 0, 0
    
    total_rows = len(data)
    print(f"\nProcessing {total_rows} operations...")
    
    for index, row in data.iterrows():
        try:
            product_code = str(row['product_id']).strip()
            operation_name = str(row['name']).strip()
            workcenter_name = str(row['workcenter_id']).strip()
            
            print(f"\nProcessing {index + 1}/{total_rows}:")
            print(f"Product: {product_code}")
            print(f"Operation: {operation_name}")
            print(f"Workcenter: {workcenter_name}")
            
            # Reset sequence for new product
            if current_product != product_code:
                current_product = product_code
                sequence_counter = 10
            
            # Find BOM
            bom_ids = models.execute_kw(DB, uid, PASSWORD,
                'mrp.bom', 'search',
                [[['code', '=', product_code]]]
            )
            
            if not bom_ids:
                print(f"No BOM found for product code: {product_code}")
                error_count += 1
                continue
            
            bom_id = bom_ids[0]
            
            # Get workcenter_id
            workcenter_id = workcenter_dict.get(workcenter_name)
            if not workcenter_id:
                print(f"Workcenter not found: {workcenter_name}")
                error_count += 1
                continue
            
            # Prepare operation values
            operation_vals = {
                'name': operation_name,
                'workcenter_id': workcenter_id,
                'time_cycle_manual': float(row['time_cycle_manual']),
                'sequence': sequence_counter,
                'bom_id': bom_id,
            }
            
            # Check for existing operation
            existing_ops = models.execute_kw(DB, uid, PASSWORD,
                'mrp.routing.workcenter', 'search_read',
                [[['bom_id', '=', bom_id], 
                  ['name', '=', operation_name],
                  ['workcenter_id', '=', workcenter_id]]],
                {'fields': ['id']}
            )
            
            if existing_ops:
                # Update existing operation
                op_id = existing_ops[0]['id']
                models.execute_kw(DB, uid, PASSWORD,
                    'mrp.routing.workcenter', 'write',
                    [[op_id], operation_vals]
                )
                print("Updated existing operation")
            else:
                # Create new operation
                models.execute_kw(DB, uid, PASSWORD,
                    'mrp.routing.workcenter', 'create',
                    [operation_vals]
                )
                print("Created new operation")
            
            success_count += 1
            sequence_counter += 10
            
        except Exception as e:
            print(f"Error processing row: {str(e)}")
            error_count += 1
            continue
    
    return success_count, error_count, skipped_count

def main():
    # ระบุ path ของไฟล์ Excel ตรงนี้ได้เลย
    excel_file = r"C:\Users\Ball\Documents\Git_apcball\Project1\Data_file\operation_bu2_นุ่น04062025.xlsx"
    
    # Connect to Odoo
    uid, models = connect_to_odoo()
    if not uid or not models:
        return
    
    # Read and process Excel file
    data = read_excel_file(excel_file)
    if data is None:
        return
    
    # Update operations
    print("\nUpdating operations in Odoo...")
    success_count, error_count, skipped_count = update_operation_bom(uid, models, data)
    
    # Print summary
    print("\nImport Summary:")
    print(f"Successfully processed: {success_count}")
    print(f"Errors: {error_count}")
    print(f"Skipped: {skipped_count}")
    print(f"Total records processed: {success_count + error_count + skipped_count}")

if __name__ == "__main__":
    main()