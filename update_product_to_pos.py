import xmlrpc.client
import pandas as pd
import logging
from datetime import datetime
import numpy as np

# Configuration
HOST = 'http://mogth.work:8069'
DB = 'MOG_LIVE'
USERNAME = 'apichart@mogen.co.th'
PASSWORD = '471109538'
EXCEL_FILE = 'Data_file/available_in_pos.xlsx'

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename=f'pos_update_log_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'
)
logger = logging.getLogger(__name__)

def connect_to_odoo():
    """Establish connection to Odoo server"""
    try:
        common = xmlrpc.client.ServerProxy('{}/xmlrpc/2/common'.format(HOST))
        uid = common.authenticate(DB, USERNAME, PASSWORD, {})
        models = xmlrpc.client.ServerProxy('{}/xmlrpc/2/object'.format(HOST))
        return uid, models
    except Exception as e:
        logger.error(f"Failed to connect to Odoo: {str(e)}")
        raise

def read_excel_data():
    """Read data from Excel file"""
    try:
        # Read Excel file
        df = pd.read_excel(EXCEL_FILE)
        
        # Print column names for debugging
        print("Available columns in Excel:", df.columns.tolist())
        
        # Find the correct column names (case-insensitive)
        default_code_col = None
        pos_col = None
        
        for col in df.columns:
            col_lower = col.lower()
            if 'default_code' in col_lower or 'default code' in col_lower:
                default_code_col = col
            elif 'pos' in col_lower or 'available' in col_lower:
                pos_col = col
        
        if default_code_col is None or pos_col is None:
            raise ValueError(f"Required columns not found. Looking for 'default_code' and POS availability column. Found columns: {df.columns.tolist()}")
        
        # Convert string 'TRUE'/'FALSE' to boolean
        df[pos_col] = df[pos_col].astype(str).str.upper().map({'TRUE': True, 'FALSE': False})
        
        # Return the data as list of tuples
        return df[[default_code_col, pos_col]].values.tolist()
    except Exception as e:
        logger.error(f"Failed to read Excel file: {str(e)}")
        raise

def update_product_pos_availability():
    """Update product POS availability based on Excel data"""
    uid, models = connect_to_odoo()
    products_data = read_excel_data()
    
    success_count = 0
    error_count = 0
    not_found = []
    
    for default_code, available_in_pos in products_data:
        try:
            # Convert to string and strip any whitespace
            default_code = str(default_code).strip()
            
            # Search for product by default_code or old_product_code
            product_ids = models.execute_kw(
                DB, uid, PASSWORD,
                'product.template',
                'search',
                [[
                    '|',  # This is an OR operator in Odoo domain
                    ['default_code', '=', default_code],
                    ['old_product_code', '=', default_code]
                ]]
            )
            
            if not product_ids:
                logger.warning(f"Product with code {default_code} not found in both default_code and old_product_code")
                not_found.append(default_code)
                error_count += 1
                continue
            
            # If multiple products found, log a warning
            if len(product_ids) > 1:
                logger.warning(f"Multiple products found for code {default_code}. Updating all instances.")
                
            # Update product's available_in_pos field
            models.execute_kw(
                DB, uid, PASSWORD,
                'product.template',
                'write',
                [product_ids, {'available_in_pos': available_in_pos}]
            )
            
            logger.info(f"Successfully updated {len(product_ids)} product(s) with code {default_code} - POS availability: {available_in_pos}")
            success_count += len(product_ids)
            
        except Exception as e:
            logger.error(f"Error updating product {default_code}: {str(e)}")
            error_count += 1
    
    return {
        'success_count': success_count,
        'error_count': error_count,
        'total_processed': len(products_data),
        'not_found_products': not_found,
        'total_products_updated': success_count  # This might be higher than total_processed due to multiple matches
    }

if __name__ == '__main__':
    try:
        logger.info("Starting POS availability update process")
        results = update_product_pos_availability()
        logger.info(f"Update process completed. Results: {results}")
        print(f"""
Update Process Completed:
- Total product codes processed: {results['total_processed']}
- Total products updated: {results['total_products_updated']}
- Successfully updated: {results['success_count']}
- Errors encountered: {results['error_count']}
- Products not found: {', '.join(results['not_found_products']) if results['not_found_products'] else 'None'}
        """)
    except Exception as e:
        logger.error(f"Process failed: {str(e)}")
        print(f"Process failed: {str(e)}")