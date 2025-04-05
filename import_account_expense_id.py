#!/usr/bin/env python3
import xmlrpc.client
import pandas as pd
from datetime import datetime
import logging
import sys

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename=f'account_expense_import_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'
)
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(message)s')
console_handler.setFormatter(formatter)
logging.getLogger().addHandler(console_handler)

# --- Connection Settings ---
url = 'http://mogth.work:8069'
db = 'MOG_Training'
username = 'apichart@mogen.co.th'
password = '471109538'

def connect_to_odoo():
    """Establish connection to Odoo server"""
    try:
        common = xmlrpc.client.ServerProxy(f'{url}/xmlrpc/2/common')
        uid = common.authenticate(db, username, password, {})
        if not uid:
            raise Exception("Authentication failed")
        models = xmlrpc.client.ServerProxy(f'{url}/xmlrpc/2/object')
        return uid, models
    except Exception as e:
        logging.error(f"Connection error: {str(e)}")
        raise

def read_excel_data(file_path):
    """Read the Excel file containing product and account data."""
    try:
        df = pd.read_excel(file_path)
        
        # Clean the data
        df['default_code'] = df['default_code'].astype(str).str.strip()
        # Extract only the account number from the property_account_expense_id column
        df['account_code'] = df['property_account_expense_id'].astype(str).str.extract('(\d+)').iloc[:, 0]
        
        return df.to_dict('records')
    except Exception as e:
        logging.error(f"Error reading Excel file: {str(e)}")
        raise

def update_product_account_expense(uid, models, data):
    """Update products with their account expense IDs."""
    success_count = 0
    error_count = 0
    skipped_count = 0
    
    for row in data:
        try:
            # Skip empty rows
            if pd.isna(row['default_code']) or pd.isna(row['account_code']):
                skipped_count += 1
                continue

            # Search for the product using default_code
            product_ids = models.execute_kw(db, uid, password,
                'product.template', 'search',
                [[['default_code', '=', row['default_code']]]],
            )
            
            if not product_ids:
                logging.warning(f"Product with code '{row['default_code']}' not found")
                error_count += 1
                continue
            
            # Search for the account using account code
            account_ids = models.execute_kw(db, uid, password,
                'account.account', 'search',
                [[['code', '=', row['account_code']]]],
            )
            
            if not account_ids:
                logging.warning(f"Account with code '{row['account_code']}' not found")
                error_count += 1
                continue
            
            # Update the product's property_account_expense_id
            models.execute_kw(db, uid, password,
                'product.template', 'write',
                [product_ids[0], {
                    'property_account_expense_id': account_ids[0]
                }]
            )
            
            success_count += 1
            logging.info(f"Updated product '{row['default_code']}' with expense account '{row['account_code']}'")
            
        except Exception as e:
            error_count += 1
            logging.error(f"Error updating product '{row['default_code']}': {str(e)}")
    
    return success_count, error_count, skipped_count

def main():
    try:
        # Connect to Odoo
        uid, models = connect_to_odoo()
        
        # Test connection
        version_info = models.execute_kw(db, uid, password, 'ir.module.module', 'search_read',
            [[['name', '=', 'base']]],
            {'fields': ['latest_version']}
        )
        if version_info:
            logging.info(f"Connected to Odoo {version_info[0]['latest_version']}")
        
        # Read Excel data
        file_path = 'Data_file/import_account_expense_id.xlsx'
        data = read_excel_data(file_path)
        
        if not data:
            logging.error("No data found in the Excel file")
            return
        
        # Update products
        logging.info(f"Starting to process {len(data)} records...")
        success_count, error_count, skipped_count = update_product_account_expense(uid, models, data)
        
        # Log summary
        logging.info(f"""
Import Summary:
--------------
Total records in file: {len(data)}
Successfully updated: {success_count}
Errors: {error_count}
Skipped: {skipped_count}
        """)
        
    except Exception as e:
        logging.error(f"Error during execution: {str(e)}")
        raise

if __name__ == '__main__':
    main()