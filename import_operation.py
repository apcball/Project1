#!/usr/bin/env python3
import xmlrpc.client
import pandas as pd
from datetime import datetime

# Odoo connection parameters
HOST = 'http://mogth.work:8069'
DB = 'MOG_LIVE'
USERNAME = 'apichart@mogen.co.th'
PASSWORD = '471109538'

def connect_to_odoo():
    """Establish connection to Odoo server"""
    common = xmlrpc.client.ServerProxy(f'{HOST}/xmlrpc/2/common')
    uid = common.authenticate(DB, USERNAME, PASSWORD, {})
    models = xmlrpc.client.ServerProxy(f'{HOST}/xmlrpc/2/object')
    return uid, models

def read_excel_file(file_path):
    """Read the Excel file and return a cleaned pandas DataFrame"""
    try:
        # Read Excel file
        df = pd.read_excel(file_path)
        
        # Print original columns and first few rows for debugging
        print("\nOriginal Excel columns:", df.columns.tolist())
        print("\nFirst few rows of raw data:")
        print(df.head())
        
        # Fill NaN values in product_id column
        df['product_id'] = df['product_id'].fillna(method='ffill')
        
        # Remove rows where all required fields are NaN
        df = df.dropna(subset=['product_id', 'name', 'workcenter_id'], how='all')
        
        return df
    except Exception as e:
        print(f"Error reading Excel file: {str(e)}")
        return None

def update_operation_bom(uid, models, data):
    """Update operation BOM based on product_id from BOM"""
    success_count = 0
    error_count = 0
    skipped_count = 0
    
    current_product = None
    sequence_counter = 10
    
    for index, row in data.iterrows():
        try:
            product_code = str(row['product_id']).strip()
            
            # Skip if product_id is empty or nan
            if not product_code or product_code.lower() == 'nan':
                print(f"Skipping row {index + 2}: Empty or invalid product_id")
                skipped_count += 1
                continue
            
            # Reset sequence counter for new product
            if current_product != product_code:
                current_product = product_code
                sequence_counter = 10
            
            # Print the row data for debugging
            print(f"\nProcessing row {index + 2}:")
            print(f"Product Code: {product_code}")
            print(f"Operation Name: {row.get('name', 'MISSING')}")
            print(f"Workcenter: {row.get('workcenter_id', 'MISSING')}")
            
            # Search for the product in BOM
            bom_ids = models.execute_kw(DB, uid, PASSWORD,
                'mrp.bom', 'search',
                [[['code', '=', product_code]]]
            )
            
            if not bom_ids:
                print(f"BOM not found for product code: {product_code}")
                error_count += 1
                continue
            
            # Get the BOM data
            bom_data = models.execute_kw(DB, uid, PASSWORD,
                'mrp.bom', 'read',
                [bom_ids[0]], {'fields': ['id', 'product_id']}
            )
            
            if not bom_data or not bom_data[0].get('product_id'):
                print(f"Product not found in BOM: {product_code}")
                error_count += 1
                continue
            
            bom_id = bom_data[0]['id']
            
            # Skip if operation name or workcenter is missing
            if pd.isna(row.get('name')) or pd.isna(row.get('workcenter_id')):
                print(f"Skipping row {index + 2}: Missing operation name or workcenter")
                skipped_count += 1
                continue
            
            # Get workcenter_id
            workcenter_ids = models.execute_kw(DB, uid, PASSWORD,
                'mrp.workcenter', 'search',
                [[['name', '=', str(row['workcenter_id'])]]]
            )
            
            if not workcenter_ids:
                print(f"Workcenter not found: {row['workcenter_id']}")
                error_count += 1
                continue
            
            workcenter_id = workcenter_ids[0]
            
            # Search for existing operation in the BOM
            operation_ids = models.execute_kw(DB, uid, PASSWORD,
                'mrp.routing.workcenter', 'search',
                [[['bom_id', '=', bom_id]]]
            )
            
            # Prepare operation values
            operation_vals = {
                'name': str(row['name']),
                'workcenter_id': workcenter_id,
                'time_cycle_manual': float(row['time_cycle_manual']) if pd.notna(row.get('time_cycle_manual')) else 0.0,
                'sequence': sequence_counter,
                'bom_id': bom_id,
            }
            
            matching_operation = None
            if operation_ids:
                # Find matching operation by name and workcenter
                operations_data = models.execute_kw(DB, uid, PASSWORD,
                    'mrp.routing.workcenter', 'read',
                    [operation_ids], {'fields': ['id', 'name', 'workcenter_id']}
                )
                for op in operations_data:
                    if op['name'] == str(row['name']) and op['workcenter_id'][0] == workcenter_id:
                        matching_operation = op['id']
                        break
            
            if matching_operation:
                # Update existing operation
                models.execute_kw(DB, uid, PASSWORD,
                    'mrp.routing.workcenter', 'write',
                    [matching_operation, operation_vals]
                )
                print(f"Updated operation for BOM: {product_code}")
            else:
                # Create new operation
                models.execute_kw(DB, uid, PASSWORD,
                    'mrp.routing.workcenter', 'create',
                    [operation_vals]
                )
                print(f"Created new operation for BOM: {product_code}")
            
            success_count += 1
            sequence_counter += 10
            
        except Exception as e:
            print(f"Error processing row {index + 2}: {str(e)}")
            error_count += 1
    
    return success_count, error_count, skipped_count

def main():
    # Connect to Odoo
    print("Connecting to Odoo...")
    uid, models = connect_to_odoo()
    
    if not uid:
        print("Failed to connect to Odoo")
        return
    
    print("Connected to Odoo successfully")
    
    # Read Excel file
    excel_file = "Data_file/operation_bu2_น่น_2803.xlsx"
    print(f"Reading Excel file: {excel_file}")
    data = read_excel_file(excel_file)
    
    if data is None:
        print("Failed to read Excel file")
        return
    
    if len(data) == 0:
        print("No valid data found in Excel file after cleaning")
        return
    
    print(f"Found {len(data)} valid records in Excel file")
    
    # Process the data
    print("\nStarting to process operations...")
    success_count, error_count, skipped_count = update_operation_bom(uid, models, data)
    
    # Print summary
    print("\nImport Summary:")
    print(f"Successfully processed: {success_count}")
    print(f"Errors: {error_count}")
    print(f"Skipped: {skipped_count}")
    print(f"Total records processed: {success_count + error_count + skipped_count}")
    
    if error_count > 0 or skipped_count > 0:
        print("\nPlease check the log messages above for details on errors and skipped records.")

if __name__ == "__main__":
    main()