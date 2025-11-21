import xmlrpc.client
import pandas as pd
import sys
import re
from datetime import datetime
import csv
import os
import time
import logging
from typing import List, Dict, Any, Tuple
from concurrent.futures import ThreadPoolExecutor
import threading
from functools import lru_cache
import gc
import threading
from contextlib import contextmanager

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Constants
BATCH_SIZE = 50
MAX_WORKERS = 6
CACHE_SIZE = 5000
MAX_RETRIES = 3
RETRY_DELAY = 1
CONNECTION_TIMEOUT = 30
CONNECTION_POOL_SIZE = 8
KEEPALIVE_INTERVAL = 20
MAX_IDLE_TIME = 180

# Memory optimization settings
CHUNK_SIZE = 1000
GC_THRESHOLD = 5000

# Create log directory if it doesn't exist
if not os.path.exists('logs'):
    os.makedirs('logs')

# Initialize lists to store successful and failed imports with thread safety
failed_imports_lock = threading.Lock()
failed_imports = []
error_messages = []

# --- Connection Settings ---
url = 'http://mogth.work:8069'
db = 'MOG_SETUP'
username = 'napaporn@mogen.co.th'
password = 'mogen'

class Logger:
    def __init__(self):
        self.failed_imports = []
        self.error_messages = []
        self.missing_vendors = []
        self.missing_products = []
        self.bo_errors = []
        self.timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        # Create logs directory if it doesn't exist
        if not os.path.exists('logs'):
            os.makedirs('logs')
            
        # Initialize file paths
        self.missing_vendors_file = f'logs/missing_vendors_{self.timestamp}.xlsx'
        self.missing_products_file = f'logs/missing_products_{self.timestamp}.xlsx'
        self.bo_errors_file = f'logs/bo_errors_{self.timestamp}.xlsx'

    def log_error(self, bo_name, line_number, item_code, error_message, error_type='error'):
        """Log blanket order error"""
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        # Store error information
        error_data = {
            'Date Time': timestamp,
            'BO Number': bo_name,
            'Line Number': line_number,
            'Item Code': item_code,
            'Error Type': error_type,
            'Error Message': error_message
        }
        self.bo_errors.append(error_data)
        self.error_messages.append(f"Error in BO {bo_name}, Line {line_number}: {error_message}")

    def log_missing_vendor(self, bo_name, line_number, vendor_code, partner_id=None):
        """Log missing vendor"""
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        # Store missing vendor information
        vendor_data = {
            'Date Time': timestamp,
            'BO Number': bo_name,
            'Line Number': line_number,
            'Vendor Code': vendor_code,
            'Partner ID': partner_id if partner_id else 'N/A'
        }
        self.missing_vendors.append(vendor_data)

    def log_missing_product(self, bo_name, line_number, product_code, product_id=None):
        """Log missing product"""
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        # Store missing product information
        product_data = {
            'Date Time': timestamp,
            'BO Number': bo_name,
            'Line Number': line_number,
            'Product Code': product_code,
            'Product ID': product_id if product_id else 'N/A'
        }
        self.missing_products.append(product_data)

    def save_logs(self):
        """Save all logs to Excel files"""
        try:
            # Save missing vendors log
            if self.missing_vendors:
                df_missing = pd.DataFrame(self.missing_vendors)
                with pd.ExcelWriter(self.missing_vendors_file, engine='xlsxwriter') as writer:
                    df_missing.to_excel(writer, sheet_name='Missing Vendors', index=False)
                    
                    # Format the worksheet
                    workbook = writer.book
                    worksheet = writer.sheets['Missing Vendors']
                    
                    # Add formats
                    header_format = workbook.add_format({
                        'bold': True,
                        'bg_color': '#D3D3D3',
                        'border': 1
                    })
                    
                    # Format headers
                    for col_num, value in enumerate(df_missing.columns.values):
                        worksheet.write(0, col_num, value, header_format)
                        worksheet.set_column(col_num, col_num, len(value) + 5)
                
                print(f"\nMissing vendors log saved to: {self.missing_vendors_file}")

            # Save missing products log
            if self.missing_products:
                df_missing = pd.DataFrame(self.missing_products)
                with pd.ExcelWriter(self.missing_products_file, engine='xlsxwriter') as writer:
                    df_missing.to_excel(writer, sheet_name='Missing Products', index=False)
                    
                    # Format the worksheet
                    workbook = writer.book
                    worksheet = writer.sheets['Missing Products']
                    
                    # Add formats
                    header_format = workbook.add_format({
                        'bold': True,
                        'bg_color': '#D3D3D3',
                        'border': 1
                    })
                    
                    # Format headers
                    for col_num, value in enumerate(df_missing.columns.values):
                        worksheet.write(0, col_num, value, header_format)
                        worksheet.set_column(col_num, col_num, len(value) + 5)
                
                print(f"\nMissing products log saved to: {self.missing_products_file}")

            # Save BO errors log
            if self.bo_errors:
                df_errors = pd.DataFrame(self.bo_errors)
                with pd.ExcelWriter(self.bo_errors_file, engine='xlsxwriter') as writer:
                    df_errors.to_excel(writer, sheet_name='BO Errors', index=False)
                    
                    # Format the worksheet
                    workbook = writer.book
                    worksheet = writer.sheets['BO Errors']
                    
                    # Add formats
                    header_format = workbook.add_format({
                        'bold': True,
                        'bg_color': '#D3D3D3',
                        'border': 1
                    })
                    
                    # Format headers
                    for col_num, value in enumerate(df_errors.columns.values):
                        worksheet.write(0, col_num, value, header_format)
                        worksheet.set_column(col_num, col_num, len(value) + 5)
                
                print(f"\nBO errors log saved to: {self.bo_errors_file}")
                
                # Print error summary
                print("\nError Summary:")
                error_summary = df_errors['Error Type'].value_counts()
                for error_type, count in error_summary.items():
                    print(f"{error_type}: {count} occurrences")
                    
        except Exception as e:
            print(f"Warning: Error saving log files: {e}")

# Create global logger instance
logger_instance = Logger()

def save_error_log():
    """Save all logs to files"""
    logger_instance.save_logs()

def log_error(bo_name, line_number, item_code, error_message, error_type='error', row_index=None, row_data=None):
    """Log error details for failed imports with thread safety and complete row data"""
    with failed_imports_lock:
        error_entry = {
            'BO Number': bo_name,
            'Line Number': line_number,
            'Item Code': item_code,
            'Error Message': error_message,
            'Date Time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'Excel Row': f'Row {row_index}' if row_index is not None else 'N/A'
        }
        
        # Add complete row data if available
        if row_data is not None and isinstance(row_data, pd.Series):
            for column, value in row_data.items():
                error_entry[f'Original_{column}'] = value
                
        failed_imports.append(error_entry)
        error_messages.append(f"Error in BO {bo_name}, Line {line_number}, Row {row_index}: {error_message}")
        
        # Enhanced logging with row data
        log_message = f"Import error - BO: {bo_name}, Line: {line_number}, Row: {row_index}, Error: {error_message}"
        if row_data is not None:
            log_message += f"\nComplete Row Data: {dict(row_data)}"
        logger.error(log_message)

# Function to connect to Odoo
def connect_to_odoo():
    """Create a new connection to Odoo"""
    try:
        common = xmlrpc.client.ServerProxy(f'{url}/xmlrpc/2/common')
        uid = common.authenticate(db, username, password, {})
        if not uid:
            print("Authentication failed: invalid credentials or insufficient permissions.")
            return None, None
        
        models = xmlrpc.client.ServerProxy(f'{url}/xmlrpc/2/object')
        print(f"Authentication successful, uid = {uid}")
        return uid, models
    except Exception as e:
        print(f"Error during connection/authentication: {e}")
        return None, None

def read_excel_file():
    """Read and validate Excel file"""
    try:
        # Try to find the Excel file in current directory or Import_BO subdirectory
        file_path = 'Tempate_BO.xlsx'
        if not os.path.exists(file_path):
            file_path = 'Import_BO/Tempate_BO.xlsx'
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"Excel file not found at {file_path}")
        
        # Read Excel file
        df = pd.read_excel(file_path)
        
        # Print total number of rows for debugging
        print(f"Total rows in Excel: {len(df)}")
        
        # Print column names for debugging
        print("Excel columns:", df.columns.tolist())
        
        # Print first few rows of raw data
        print("\nFirst few rows of raw data:")
        print(df.head().to_string())
        
        # Remove rows where all values are NaN
        df = df.dropna(how='all')
        print(f"Rows after removing empty rows: {len(df)}")
        
        # Map Excel columns to standard field names
        column_mapping = {
            'เลขที่เอกสาร': 'Reference',
            'ชื่อผู้ขอ': 'User_id',
            'วันที่มีผลกับราคา': 'date_end',
            'วันที่ต้องการของ': 'ordering_date',
            'รหัส Vender': 'old_partner_code',
            'ชื่อ Veder': 'vender_id',
            'วันที่จัดส่ง': 'delivery_date',
            'เอกสารอ้างอิง': 'origin',
            'รหัสสินค้า': 'Defalut_code',
            'ชื่อสินค้า': 'product_id',
            'จำนวน': 'product_qty',
            'ราคา': 'price_unit'
        }
        
        # Apply column mapping
        df = df.rename(columns=column_mapping)
        
        # Validate required columns
        required_columns = ['Reference', 'Defalut_code', 'product_qty', 'price_unit']
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            raise ValueError(f"Missing required columns: {missing_columns}")
        
        print(f"Excel file read successfully. Number of rows = {len(df)}")
        return df
        
    except Exception as e:
        print(f"Error reading Excel file: {e}")
        return None

def search_vendor(old_partner_code=None, partner_code=None, default_code=None, old_product_code=None):
    """Search for vendor in Odoo with fallback hierarchy"""
    try:
        # Step 1: Search by old_partner_code
        if old_partner_code and not pd.isna(old_partner_code):
            vendor_ids = models.execute_kw(
                db, uid, password, 'res.partner', 'search',
                [[['ref', '=', str(old_partner_code).strip()], ['supplier_rank', '>', 0]]]
            )
            if vendor_ids:
                print(f"Found vendor with old_partner_code: {old_partner_code}")
                return vendor_ids[0]

        # Step 2: Search by partner_code
        if partner_code and not pd.isna(partner_code):
            vendor_ids = models.execute_kw(
                db, uid, password, 'res.partner', 'search',
                [[['ref', '=', str(partner_code).strip()], ['supplier_rank', '>', 0]]]
            )
            if vendor_ids:
                print(f"Found vendor with partner_code: {partner_code}")
                return vendor_ids[0]
        
        # Step 2.5: Search by vendor name (from vender_id column)
        # This is a fallback when the above code searches don't work
        if partner_code and not pd.isna(partner_code):
            vendor_ids = models.execute_kw(
                db, uid, password, 'res.partner', 'search',
                [[['name', 'ilike', str(partner_code).strip()], ['supplier_rank', '>', 0]]]
            )
            if vendor_ids:
                print(f"Found vendor by name: {partner_code}")
                return vendor_ids[0]

        # Step 3: Search by default_code (product vendor)
        if default_code and not pd.isna(default_code):
            product_ids = models.execute_kw(
                db, uid, password, 'product.product', 'search',
                [[['default_code', '=', str(default_code).strip()]]]
            )
            if product_ids:
                product = models.execute_kw(
                    db, uid, password, 'product.product', 'read',
                    [product_ids[0]], {'fields': ['seller_ids']}
                )[0]
                
                if product.get('seller_ids'):
                    seller = models.execute_kw(
                        db, uid, password, 'product.supplierinfo', 'read',
                        [product['seller_ids'][0]], {'fields': ['partner_id']}
                    )[0]
                    vendor_id = seller['partner_id'][0]
                    print(f"Found vendor through product default_code: {default_code}")
                    return vendor_id

        # Step 4: Search by old_product_code
        if old_product_code and not pd.isna(old_product_code):
            product_ids = models.execute_kw(
                db, uid, password, 'product.product', 'search',
                [[['old_product_code', '=', str(old_product_code).strip()]]]
            )
            if product_ids:
                product = models.execute_kw(
                    db, uid, password, 'product.product', 'read',
                    [product_ids[0]], {'fields': ['seller_ids']}
                )[0]
                
                if product.get('seller_ids'):
                    seller = models.execute_kw(
                        db, uid, password, 'product.supplierinfo', 'read',
                        [product['seller_ids'][0]], {'fields': ['partner_id']}
                    )[0]
                    vendor_id = seller['partner_id'][0]
                    print(f"Found vendor through product old_product_code: {old_product_code}")
                    return vendor_id
        
        print(f"Vendor not found for codes: old_partner_code={old_partner_code}, partner_code={partner_code}, default_code={default_code}, old_product_code={old_product_code}")
        return None
        
    except Exception as e:
        error_msg = str(e)
        print(f"Error in search_vendor: {error_msg}")
        log_error('N/A', 'N/A', 'N/A', f"Vendor Search Error: {error_msg}", 'vendor_error')
        return None

def search_product(default_code=None, old_product_code=None):
    """Search for product in Odoo with fallback hierarchy"""
    try:
        # Step 1: Search by default_code
        if default_code and not pd.isna(default_code):
            product_ids = models.execute_kw(
                db, uid, password, 'product.product', 'search',
                [[['default_code', '=', str(default_code).strip()]]]
            )
            if product_ids:
                print(f"Found product with default_code: {default_code}")
                return product_ids[0]

        # Step 2: Search by old_product_code (if default_code didn't find anything)
        if old_product_code and not pd.isna(old_product_code):
            product_ids = models.execute_kw(
                db, uid, password, 'product.product', 'search',
                [[['old_product_code', '=', str(old_product_code).strip()]]]
            )
            if product_ids:
                print(f"Found product with old_product_code: {old_product_code}")
                return product_ids[0]
        
        # Step 3: If default_code was provided but not found, try searching by old_product_code field
        # with the default_code value (some products might have old_product_code equal to default_code)
        if default_code and not pd.isna(default_code):
            product_ids = models.execute_kw(
                db, uid, password, 'product.product', 'search',
                [[['old_product_code', '=', str(default_code).strip()]]]
            )
            if product_ids:
                print(f"Found product with old_product_code matching default_code: {default_code}")
                return product_ids[0]
        
        print(f"Product not found for codes: default_code={default_code}, old_product_code={old_product_code}")
        return None
        
    except Exception as e:
        error_msg = str(e)
        print(f"Error searching product: {error_msg}")
        log_error('N/A', 'N/A', str(default_code or old_product_code), f"Product Search Error: {error_msg}", 'system_error')
        return None

def convert_date(pd_timestamp):
    """Convert pandas timestamp to string"""
    if pd.notnull(pd_timestamp):
        if isinstance(pd_timestamp, str):
            return pd_timestamp
        return pd_timestamp.strftime('%Y-%m-%d')
    return False

def get_currency_id(currency_code):
    """Get currency ID from currency code"""
    if not currency_code or pd.isna(currency_code):
        return False
    
    try:
        currency_ids = models.execute_kw(
            db, uid, password, 'res.currency', 'search',
            [[['name', '=', str(currency_code).strip().upper()]]]
        )
        if currency_ids:
            print(f"Found currency: {currency_code}")
            return currency_ids[0]
        
        # Default to company currency if not found
        company_currency = models.execute_kw(
            db, uid, password, 'res.company', 'search_read',
            [[['id', '=', 1]]], {'fields': ['currency_id']}
        )
        if company_currency:
            return company_currency[0]['currency_id'][0]
        
        return False
    except Exception as e:
        print(f"Error getting currency ID: {e}")
        return False

def safe_float_convert(value):
    """Safely convert value to float"""
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        value = value.strip()
        if value in ['-', '', 'N/A', 'NA', 'None', 'null']:
            return 0.0
        try:
            value = value.replace(',', '')
            return float(value)
        except ValueError:
            print(f"Could not convert '{value}' to float, using 0.0")
            return 0.0
    return 0.0

def create_blanket_order(bo_data):
    """Create a blanket order in Odoo"""
    try:
        bo_name = bo_data['name']
        
        # Check if blanket order already exists
        bo_ids = models.execute_kw(
            db, uid, password, 'purchase.requisition', 'search',
            [[['name', '=', bo_name]]]
        )
        
        if bo_ids:
            print(f"Blanket order {bo_name} already exists, updating...")
            bo_id = bo_ids[0]
            
            # First, clear existing lines by using (5, line_id) command for each line
            existing_bo = models.execute_kw(
                db, uid, password, 'purchase.requisition', 'read',
                [bo_id], {'fields': ['line_ids']}
            )[0]
            
            # Prepare commands to delete existing lines
            delete_lines = [(5, line_id) for line_id in existing_bo.get('line_ids', [])]
            
            # Combine delete and new lines
            update_lines = delete_lines + bo_data['line_ids']
            
            # Update existing blanket order with new lines
            update_data = {
                'vendor_id': bo_data['vendor_id'],
                'ordering_date': bo_data['ordering_date'],
                'currency_id': bo_data['currency_id'],
                'origin': bo_data.get('origin', ''),
                'line_ids': update_lines
            }
            
            # Add date_end only if it's not False
            if bo_data.get('date_end'):
                update_data['date_end'] = bo_data['date_end']
            
            models.execute_kw(
                db, uid, password, 'purchase.requisition', 'write',
                [bo_id, update_data]
            )
            print(f"Successfully updated blanket order: {bo_name}")
            return True
        else:
            print(f"Creating new blanket order: {bo_name}")
            # Create new blanket order
            bo_id = models.execute_kw(
                db, uid, password, 'purchase.requisition', 'create',
                [bo_data]
            )
            print(f"Successfully created blanket order: {bo_name} (ID: {bo_id})")
            return True
            
    except Exception as e:
        error_msg = str(e)
        print(f"Error creating/updating blanket order: {error_msg}")
        log_error(bo_data.get('name', 'N/A'), 'N/A', 'N/A', f"BO Creation/Update Error: {error_msg}", 'system_error')
        return False

def process_single_bo(bo_group):
    """Process a single blanket order"""
    success = False
    bo_name = bo_group.iloc[0]['Reference']
    
    try:
        print(f"\nProcessing Blanket Order: {bo_name}")
        
        # Get first row for BO header data
        first_row = bo_group.iloc[0]
        
        # Find vendor
        vendor_id = search_vendor(
            old_partner_code=first_row.get('old_partner_code'),
            partner_code=first_row.get('vender_id'),  # Use vender_id column which contains vendor name
            default_code=first_row.get('Defalut_code'),
            old_product_code=first_row.get('old_product_code')
        )
        
        if not vendor_id:
            logger_instance.log_missing_vendor(bo_name, 1, first_row.get('old_partner_code', 'N/A'))
            log_error(bo_name, 'N/A', 'N/A', "Vendor not found", 'vendor_error')
            return False
        
        # Get currency ID
        currency_id = get_currency_id(first_row.get('currency_id'))
        
        
        # Prepare BO data
        date_order = convert_date(first_row['ordering_date']) if pd.notna(first_row.get('ordering_date')) else datetime.now().strftime('%Y-%m-%d')
        date_end = convert_date(first_row['date_end']) if pd.notna(first_row.get('date_end')) else False
        bo_data = {
            'name': bo_name,
            'vendor_id': vendor_id,  # Changed from partner_id to vendor_id for purchase.requisition
            'ordering_date': date_order,
            'date_end': date_end,
            'currency_id': currency_id,
            'origin': str(first_row.get('origin', '')) if pd.notna(first_row.get('origin')) else '',
            'line_ids': []
        }
        
        
        # Process BO lines
        all_products_found = True
        bo_lines = []
        
        for idx, line in bo_group.iterrows():
            # Search for product
            product_id = search_product(
                default_code=line.get('Defalut_code'),
                old_product_code=line.get('old_product_code')
            )
            
            if not product_id:
                all_products_found = False
                logger_instance.log_missing_product(bo_name, idx + 1, line.get('Defalut_code', 'N/A'))
                log_error(bo_name, idx + 1, line.get('Defalut_code', 'N/A'), "Product not found", 'product_error')
                continue
            
            # Convert quantity and price
            quantity = safe_float_convert(line['product_qty'])
            price_unit = safe_float_convert(line['price_unit'])
            
            if quantity <= 0:
                print(f"Warning: Zero or negative quantity ({quantity}) for product {line.get('Defalut_code')}")
                log_error(bo_name, idx + 1, line.get('Defalut_code', 'N/A'), f"Invalid quantity: {quantity}", 'quantity_error')
                continue
            
            # Prepare line data - REMOVED 'name' field as it doesn't exist
            # Changed 'date_planned' to 'schedule_date' as that's the correct field name
            line_data = {
                'product_id': product_id,
                'product_qty': quantity,
                'price_unit': price_unit,
                'schedule_date': convert_date(line.get('delivery_date')) if pd.notna(line.get('delivery_date')) else date_order,
            }
            
            bo_lines.append((0, 0, line_data))
        
        if bo_lines:
            bo_data['line_ids'] = bo_lines
            success = create_blanket_order(bo_data)
            if success:
                print(f"Successfully processed blanket order: {bo_name}")
            else:
                log_error(bo_name, 'N/A', 'N/A', "Failed to create/update blanket order", 'bo_creation_error')
        else:
            print(f"No valid lines found for blanket order {bo_name}")
            log_error(bo_name, 'N/A', 'N/A', "No valid lines to process", 'no_valid_lines')
            
    except Exception as e:
        print(f"Error processing blanket order {bo_name}: {str(e)}")
        log_error(bo_name, 'N/A', 'N/A', f"Processing Error: {str(e)}", 'system_error')
        success = False
    
    return success

def main():
    total_success = 0
    total_errors = 0
    
    try:
        # Read Excel file
        df = read_excel_file()
        if df is None:
            print("Failed to read Excel file")
            return
        
        print(f"\nExcel columns after mapping: {df.columns.tolist()}")
        
        # Process each blanket order individually
        for bo_name, bo_group in df.groupby('Reference'):
            print(f"\n{'='*50}")
            print(f"Processing Blanket Order: {bo_name}")
            print(f"{'='*50}")
            
            if process_single_bo(bo_group):
                total_success += 1
                print(f"[SUCCESS] Successfully processed blanket order: {bo_name}")
            else:
                total_errors += 1
                print(f"[FAILED] Failed to process blanket order: {bo_name}")
            
            # Optional: Add a small delay between BOs to prevent overloading
            time.sleep(0.5)
        
        # Print final summary
        print("\n" + "="*50)
        print("Final Import Summary:")
        print("="*50)
        print(f"Total Successful Blanket Orders: {total_success}")
        print(f"Total Failed Blanket Orders: {total_errors}")
        print(f"Total Processed: {total_success + total_errors}")
        
    except Exception as e:
        print(f"Error in main function: {e}")
        log_error('N/A', 'N/A', 'N/A', f"Main Function Error: {str(e)}", 'system_error')
    
    finally:
        # Save and close the log file
        save_error_log()
        print("\nImport process completed.")

if __name__ == "__main__":
    # Initialize global connection
    uid, models = connect_to_odoo()
    if not uid or not models:
        print("Failed to connect to Odoo")
        sys.exit(1)
    
    main()