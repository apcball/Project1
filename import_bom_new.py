import xmlrpc.client
import pandas as pd
import numpy as np
from datetime import datetime

# Odoo connection parameters
HOST = 'http://mogth.work:8069'
DB = 'MOG_DEV'
USERNAME = 'apichart@mogen.co.th'
PASSWORD = '471109538'

def read_excel_template():
    """Read and validate the Excel template"""
    try:
        # Read the Excel file
        df = pd.read_excel('Data_file/import_bom_2.xlsx')
        
        # Clean up the data
        df = df.fillna('')  # Replace NaN with empty string
        
        # Clean up column names
        df.columns = [str(col).strip() for col in df.columns]
        
        print("\nColumns found in Excel:")
        print(df.columns.tolist())
        
        return df
    except Exception as e:
        print(f"Error reading Excel file: {str(e)}")
        return None

def connect_odoo():
    """Establish connection to Odoo"""
    try:
        # Common endpoint for authentication
        common = xmlrpc.client.ServerProxy(f'{HOST}/xmlrpc/2/common')
        uid = common.authenticate(DB, USERNAME, PASSWORD, {})
        
        # Object endpoint for model operations
        models = xmlrpc.client.ServerProxy(f'{HOST}/xmlrpc/2/object')
        
        return uid, models
    except Exception as e:
        print(f"Error connecting to Odoo: {str(e)}")
        return None, None

def search_product_by_code(models, uid, default_code):
    """Search for a product using default_code or old_product_code"""
    if not default_code or not str(default_code).strip():
        return False
    
    try:
        default_code = str(default_code).strip()
        
        # First try to find by default_code
        product_ids = models.execute_kw(DB, uid, PASSWORD,
            'product.product', 'search',
            [[['default_code', '=', default_code]]])
        
        if product_ids:
            return product_ids[0]
        
        # If not found, try to find by old_product_code
        product_ids = models.execute_kw(DB, uid, PASSWORD,
            'product.product', 'search',
            [[['old_product_code', '=', default_code]]])
        
        if product_ids:
            return product_ids[0]
        else:
            print(f"Product not found by default_code or old_product_code: {default_code}")
    except Exception as e:
        print(f"Error searching product {default_code}: {str(e)}")
    return False

def get_bom_type(type_value):
    """Convert Excel type value to valid BOM type"""
    if not type_value or pd.isna(type_value):
        return 'normal'
    
    type_value = str(type_value).strip().lower()
    
    # Map Excel values to Odoo BOM types
    type_mapping = {
        'kit': 'phantom',
        'phantom': 'phantom',
        'normal': 'normal',
        'manufacture': 'normal',
        'subcontract': 'subcontract'
    }
    
    return type_mapping.get(type_value, 'normal')

def create_bom(models, uid, product_tmpl_id, bom_lines, bom_name, bom_type='normal'):
    """Create a Bill of Materials"""
    if not bom_lines:
        print(f"No BOM lines to create for {bom_name}")
        return False

    bom_vals = {
        'product_tmpl_id': product_tmpl_id,
        'product_qty': 1.0,
        'type': bom_type,
        'code': bom_name,
        'bom_line_ids': [(0, 0, line) for line in bom_lines]
    }
    
    try:
        # Check if BOM already exists
        existing_bom = models.execute_kw(DB, uid, PASSWORD,
            'mrp.bom', 'search',
            [[['code', '=', bom_name]]])
        
        if existing_bom:
            print(f"BOM already exists for {bom_name} (type: {bom_type}), updating...")
            models.execute_kw(DB, uid, PASSWORD,
                'mrp.bom', 'write',
                [existing_bom[0], {
                    'type': bom_type,
                    'bom_line_ids': [(5, 0, 0)] + [(0, 0, line) for line in bom_lines]
                }])
            return existing_bom[0]
        else:
            bom_id = models.execute_kw(DB, uid, PASSWORD,
                'mrp.bom', 'create',
                [bom_vals])
            print(f"Created new BOM for {bom_name} (type: {bom_type})")
            return bom_id
    except Exception as e:
        print(f"Error creating/updating BOM: {str(e)}")
        return False

def clean_code(code):
    """Clean and validate product code"""
    if not code or pd.isna(code) or str(code).strip() == '':
        return None
    return str(code).strip()

def process_bom_group(group, uid, models):
    """Process a group of rows that belong to the same BOM"""
    if not group:
        return False
    
    try:
        # First row contains the main product
        main_row = group[0]
        main_product_code = clean_code(main_row['default_dode'])
        bom_type = get_bom_type(main_row.get('type', ''))
        
        if not main_product_code:
            return False
        
        print(f"\nProcessing BOM for {main_product_code} (type: {bom_type})")
        
        # Get main product template ID
        product_id = search_product_by_code(models, uid, main_product_code)
        if not product_id:
            print(f"Main product not found: {main_product_code}")
            return False
        
        product_data = models.execute_kw(DB, uid, PASSWORD,
            'product.product', 'read',
            [product_id], {'fields': ['product_tmpl_id']})
        product_tmpl_id = product_data[0]['product_tmpl_id'][0]
        
        # Process components
        bom_lines = []
        for row in group:
            component_code = clean_code(row['component_code'])
            if not component_code:
                continue
            
            component_id = search_product_by_code(models, uid, component_code)
            if not component_id:
                print(f"Component not found: {component_code}")
                continue
            
            # Get quantity
            try:
                quantity = float(row['product_qty']) if row['product_qty'] else 1.0
            except (ValueError, TypeError):
                quantity = 1.0
            
            bom_lines.append({
                'product_id': component_id,
                'product_qty': quantity
            })
            print(f"Added component {component_code} (qty: {quantity})")
        
        if bom_lines:
            create_bom(models, uid, product_tmpl_id, bom_lines, main_product_code, bom_type)
            return True
        
    except Exception as e:
        print(f"Error processing BOM group: {str(e)}")
    
    return False

def process_bom_data(df, uid, models):
    """Process BOM data from dataframe"""
    if df is None or uid is None or models is None:
        return

    processed_count = 0
    error_count = 0
    
    print("\nProcessing BOMs...")
    
    # Group rows by default_dode
    current_group = []
    
    for index, row in df.iterrows():
        try:
            default_code = clean_code(row['default_dode'])
            component_code = clean_code(row['component_code'])
            
            # Skip empty rows
            if not default_code and not component_code:
                continue
            
            # If we have a default_code, it's a main product
            if default_code:
                # Process previous group if exists
                if current_group:
                    success = process_bom_group(current_group, uid, models)
                    if success:
                        processed_count += 1
                    else:
                        error_count += 1
                    current_group = []
                
                # Start new group
                current_group = [row]
            else:
                # Add to current group if exists
                if current_group:
                    current_group.append(row)
        
        except Exception as e:
            print(f"Error processing row {index + 2}: {str(e)}")
            error_count += 1
    
    # Process last group
    if current_group:
        success = process_bom_group(current_group, uid, models)
        if success:
            processed_count += 1
        else:
            error_count += 1
    
    return processed_count, error_count, len(df)

def main():
    print("Starting BOM import process...")
    
    # Read Excel template
    df = read_excel_template()
    if df is None:
        return
    
    # Connect to Odoo
    uid, models = connect_odoo()
    if uid is None or models is None:
        return
    
    # Process BOM data
    processed_count, error_count, total_rows = process_bom_data(df, uid, models)
    
    # Print summary
    print("\nImport Summary:")
    print(f"Total rows processed: {total_rows}")
    print(f"Successfully processed BOMs: {processed_count}")
    print(f"Errors encountered: {error_count}")

if __name__ == "__main__":
    main()
    print("\nBOM import process completed!")