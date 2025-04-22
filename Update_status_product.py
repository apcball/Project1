#!/usr/bin/env python3
import xmlrpc.client
import pandas as pd
import logging
from datetime import datetime
import os

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'update_product_log_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'),
        logging.StreamHandler()
    ]
)

# --- Connection Settings ---
url = 'http://mogth.work:8069'
db = 'MOG_LIVE'
username = 'apichart@mogen.co.th'
password = '471109538'

def connect_to_odoo():
    """Establish connection to Odoo server"""
    try:
        common = xmlrpc.client.ServerProxy(f'{url}/xmlrpc/2/common')
        uid = common.authenticate(db, username, password, {})
        models = xmlrpc.client.ServerProxy(f'{url}/xmlrpc/2/object')
        logging.info("Successfully connected to Odoo server")
        return models, uid
    except Exception as e:
        logging.error(f"Failed to connect to Odoo: {str(e)}")
        raise

def read_excel_file():
    """Read the Excel file containing product internal references and can be sold status"""
    try:
        # Using the correct file path
        excel_path = os.path.join('Data_file', 'Update_status_product.xlsx')
        if not os.path.exists(excel_path):
            raise FileNotFoundError(f"Excel file not found at: {excel_path}")
            
        df = pd.read_excel(excel_path)
        
        # Get all column names for logging
        columns = df.columns.tolist()
        logging.info(f"Excel columns found: {columns}")
        
        # Try to find the correct column names
        internal_ref_column = None
        can_be_sold_column = None
        
        # Possible column names for internal reference
        ref_possible_names = ['internal reference', 'Internal Reference', 'default_code', 'Default Code', 'Internal_Reference']
        # Possible column names for can be sold
        sold_possible_names = ['can be sold', 'Can be Sold', 'sale_ok', 'Sale Ok', 'Can_be_Sold']
        
        for col in ref_possible_names:
            if col in columns:
                internal_ref_column = col
                break
                
        for col in sold_possible_names:
            if col in columns:
                can_be_sold_column = col
                break
        
        if internal_ref_column is None or can_be_sold_column is None:
            raise ValueError(f"Could not find required columns. Available columns: {columns}")
            
        # Get the data as a dictionary
        products_data = []
        for _, row in df.iterrows():
            if pd.notna(row[internal_ref_column]):  # Only include rows with internal reference
                # Convert the can_be_sold value to boolean
                can_be_sold_value = row[can_be_sold_column]
                if isinstance(can_be_sold_value, str):
                    can_be_sold_value = can_be_sold_value.upper() in ['TRUE', '1', 'YES', 'Y']
                else:
                    can_be_sold_value = bool(can_be_sold_value)
                
                products_data.append({
                    'default_code': str(row[internal_ref_column]).strip(),
                    'sale_ok': can_be_sold_value
                })
                
                # Log the read values
                logging.info(f"Read from Excel - Product [{str(row[internal_ref_column]).strip()}] "
                           f"Can be sold: {can_be_sold_value}")
        
        logging.info(f"Successfully read {len(products_data)} products from Excel file")
        return products_data
    except Exception as e:
        logging.error(f"Failed to read Excel file: {str(e)}")
        raise

def update_products(models, uid, products_data):
    """Update can_be_sold status for products"""
    try:
        # Get all internal references
        internal_refs = [p['default_code'] for p in products_data]
        
        # Search for products by default_code (internal reference)
        products = models.execute_kw(db, uid, password,
            'product.template', 'search_read',
            [[('default_code', 'in', internal_refs)]],
            {'fields': ['id', 'default_code', 'sale_ok']}
        )
        
        if not products:
            logging.warning("No products found with the provided internal references")
            return
            
        # Create a mapping of internal reference to product data
        product_map = {p['default_code']: p for p in products}
        excel_map = {p['default_code']: p for p in products_data}
        
        # Update products
        updated_count = 0
        for internal_ref, excel_data in excel_map.items():
            if internal_ref in product_map:
                product = product_map[internal_ref]
                current_status = product['sale_ok']
                new_status = excel_data['sale_ok']
                
                if current_status != new_status:
                    result = models.execute_kw(db, uid, password,
                        'product.template', 'write',
                        [[product['id']], {'sale_ok': new_status}]
                    )
                    if result:
                        updated_count += 1
                        logging.info(f"Product [{internal_ref}] updated - "
                                   f"Can be sold: {current_status} -> {new_status}")
                else:
                    logging.info(f"Product [{internal_ref}] no change needed - "
                               f"Current status: {current_status}")
        
        # Log summary
        logging.info(f"Successfully updated {updated_count} products")
        
        # Log products that were not found
        not_found = [ref for ref in internal_refs if ref not in product_map]
        if not_found:
            logging.warning(f"The following products were not found: {not_found}")
            
    except Exception as e:
        logging.error(f"Failed to update products: {str(e)}")
        raise

def main():
    try:
        # Connect to Odoo
        models, uid = connect_to_odoo()
        
        # Read product data from Excel
        products_data = read_excel_file()
        
        if not products_data:
            logging.error("No product data found in the Excel file")
            return
            
        # Update products
        update_products(models, uid, products_data)
        
        logging.info("Product update process completed successfully")
        
    except Exception as e:
        logging.error(f"Process failed: {str(e)}")

if __name__ == "__main__":
    main()