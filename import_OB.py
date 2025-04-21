import xmlrpc.client
import pandas as pd
import sys
import re
from datetime import datetime
import csv
import os
import os.path
import time
import json
from typing import Dict, List, Any, Tuple

# Create necessary directories
if not os.path.exists('logs'):
    os.makedirs('logs')
if not os.path.exists('state'):
    os.makedirs('state')

# Initialize lists to store successful and failed imports
failed_imports = []
error_messages = []
successful_imports = []

# State tracking
STATE_FILE = 'state/import_state.json'

def save_state(current_row: int, file_name: str) -> None:
    """Save the current import state to a file"""
    state = {
        'last_processed_row': current_row,
        'file_name': file_name,
        'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    }
    with open(STATE_FILE, 'w') as f:
        json.dump(state, f)

def load_state() -> Tuple[int, str]:
    """Load the last import state"""
    try:
        if os.path.exists(STATE_FILE):
            with open(STATE_FILE, 'r') as f:
                state = json.load(f)
                return state.get('last_processed_row', 0), state.get('file_name', '')
        return 0, ''
    except Exception as e:
        print(f"Error loading state: {e}")
        return 0, ''

def clear_state() -> None:
    """Clear the current import state"""
    if os.path.exists(STATE_FILE):
        os.remove(STATE_FILE)

def log_error(po_name, line_number, product_code, error_message, row_index=None, row_data=None):
    """Log error details for failed imports with full row data"""
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    error_entry = {
        'PO Number': po_name,
        'Line Number': line_number,
        'Product Code': product_code,
        'Error Message': error_message,
        'Date Time': timestamp,
        'Row Index': row_index
    }
    
    # Add all row data if available
    if row_data is not None:
        for key, value in row_data.items():
            if key not in error_entry:
                error_entry[key] = value
    
    failed_imports.append(error_entry)
    error_messages.append(f"Error in PO {po_name}, Line {line_number}, Row {row_index}: {error_message}")
    
    # Save to Excel immediately
    try:
        current_date = datetime.now().strftime('%Y%m%d')
        error_file = f'logs/import_errors_{current_date}.xlsx'
        
        # If file exists, read it and append
        if os.path.exists(error_file):
            existing_df = pd.read_excel(error_file)
            updated_df = pd.concat([existing_df, pd.DataFrame([error_entry])], ignore_index=True)
        else:
            updated_df = pd.DataFrame([error_entry])
        
        # Save with all columns
        updated_df.to_excel(error_file, index=False)
        print(f"\nError logged to: {error_file}")
        
    except Exception as e:
        print(f"Warning: Could not save error log to file: {str(e)}")
    
    return error_entry

def log_success(po_name, line_number, product_code, row_index=None):
    """Log successful imports"""
    success_entry = {
        'PO Number': po_name,
        'Line Number': line_number,
        'Product Code': product_code,
        'Date Time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'Row Index': row_index
    }
    successful_imports.append(success_entry)
    return success_entry

def save_import_logs():
    """Save both error and success logs to Excel files"""
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    # Save error log
    if failed_imports:
        df_errors = pd.DataFrame(failed_imports)
        error_log_file = f'logs/import_errors_{timestamp}.xlsx'
        df_errors.to_excel(error_log_file, index=False)
        print(f"\nError log saved to: {error_log_file}")
        
        print("\nError Summary:")
        for msg in error_messages:
            print(msg)
    
    # Save success log
    if successful_imports:
        df_success = pd.DataFrame(successful_imports)
        success_log_file = f'logs/import_success_{timestamp}.xlsx'
        df_success.to_excel(success_log_file, index=False)
        print(f"\nSuccess log saved to: {success_log_file}")
        print(f"Successfully imported {len(successful_imports)} records")

# --- Connection Settings ---
url = 'http://119.59.102.189:8069/'
db = 'MOG_LIVE'
username = 'parinya@mogen.co.th'
password = 'mogen'

def connect_to_odoo():
    """Create a new connection to Odoo with timeout handling"""
    try:
        # Create custom transport class with timeout
        class TimeoutTransport(xmlrpc.client.Transport):
            def make_connection(self, host):
                connection = super().make_connection(host)
                connection.timeout = 30  # timeout in seconds
                return connection

        # Create connection with custom transport
        common = xmlrpc.client.ServerProxy(
            f'{url}/xmlrpc/2/common',
            transport=TimeoutTransport()
        )
        
        # Test connection before authentication
        try:
            common.version()
        except Exception as e:
            print(f"Server connection test failed: {e}")
            return None, None
        
        # Authenticate
        try:
            uid = common.authenticate(db, username, password, {})
            if not uid:
                print("Authentication failed: Check credentials or permissions")
                return None, None
        except Exception as e:
            print(f"Authentication error: {e}")
            return None, None
        
        # Create models proxy with timeout
        models = xmlrpc.client.ServerProxy(
            f'{url}/xmlrpc/2/object',
            transport=TimeoutTransport()
        )
        
        print("Connection successful, uid =", uid)
        return uid, models
        
    except ConnectionRefusedError:
        print("Connection refused: Server not responding")
        return None, None
    except xmlrpc.client.ProtocolError as e:
        print(f"Protocol error: {e}")
        return None, None
    except Exception as e:
        print(f"Unexpected connection error: {e}")
        return None, None

class RetryConfig:
    """Configuration for retry mechanisms"""
    CONNECT_MAX_RETRIES = 3  # ลดจาก 5 เป็น 3
    IMPORT_MAX_RETRIES = 2   # ลดจาก 3 เป็น 2
    INITIAL_RETRY_DELAY = 2  # ลดจาก 5 เป็น 2 วินาที
    MAX_RETRY_DELAY = 30    # ลดจาก 60 เป็น 30 วินาที

def ensure_connection():
    """Ensure connection is active, attempt to reconnect if needed"""
    global uid, models
    max_retries = RetryConfig.CONNECT_MAX_RETRIES
    initial_retry_delay = RetryConfig.INITIAL_RETRY_DELAY
    max_retry_delay = RetryConfig.MAX_RETRY_DELAY
    
    for attempt in range(max_retries):
        if attempt > 0:
            # Use exponential backoff for retry delay
            retry_delay = min(initial_retry_delay * (2 ** (attempt - 1)), max_retry_delay)
            print(f"Attempting to reconnect... (Attempt {attempt + 1}/{max_retries}, waiting {retry_delay} seconds)")
            time.sleep(retry_delay)
        
        try:
            new_uid, new_models = connect_to_odoo()
            if new_uid and new_models:
                uid = new_uid
                models = new_models
                # Test connection with a simple command
                try:
                    models.execute_kw(db, uid, password, 'res.users', 'search_count', [[]])
                    return True
                except Exception as e:
                    print(f"Connection test failed: {e}")
                    continue
        except Exception as e:
            print(f"Connection attempt failed: {e}")
            continue
    
    print("Failed to establish a stable connection after multiple attempts")
    return False

# Initial connection
uid, models = connect_to_odoo()
if not uid or not models:
    print("Initial connection failed")
    sys.exit(1)

def search_vendor(partner_name=None, partner_code=None, partner_id=None):
    """Search for vendor in Odoo. If not found, create a new one."""
    try:
        if not partner_id or pd.isna(partner_id):
            print("No vendor information provided")
            return False

        vendor_name = str(partner_id).strip()
        
        # Search for existing vendor
        try:
            vendor_ids = models.execute_kw(
                db, uid, password, 'res.partner', 'search',
                [[['name', '=', vendor_name]]]
            )
        except Exception as e:
            print(f"Error searching vendor: {e}")
            if not ensure_connection():
                return False
            return False
        
        if vendor_ids:
            print(f"Found existing vendor: {vendor_name}")
            return vendor_ids[0]
        
        # If vendor not found, create a new one
        print(f"Vendor not found: {vendor_name}. Creating new vendor...")
        vendor_data = {
            'name': vendor_name,
            'company_type': 'company',
            'supplier_rank': 1,
            'customer_rank': 0,
            'is_company': True,
        }
        
        try:
            new_vendor_id = models.execute_kw(
                db, uid, password, 'res.partner', 'create', [vendor_data]
            )
            print(f"Successfully created new vendor: {vendor_name} (ID: {new_vendor_id})")
            return new_vendor_id
        except Exception as create_error:
            print(f"Failed to create vendor: {vendor_name}")
            print(f"Creation error: {str(create_error)}")
            if not ensure_connection():
                return False
            return False
        
    except Exception as e:
        error_msg = str(e)
        print(f"Error in search_vendor: {error_msg}")
        log_error('N/A', 'N/A', 'N/A', f"Vendor Search Error: {error_msg}")
        return False

def retry_operation(operation_func, *args, max_retries=None, **kwargs):
    """Generic retry mechanism for operations"""
    if max_retries is None:
        max_retries = RetryConfig.IMPORT_MAX_RETRIES
    
    for attempt in range(max_retries):
        try:
            if attempt > 0:
                retry_delay = min(RetryConfig.INITIAL_RETRY_DELAY * (2 ** (attempt - 1)), 
                                RetryConfig.MAX_RETRY_DELAY)
                print(f"Retrying operation... (Attempt {attempt + 1}/{max_retries}, "
                      f"waiting {retry_delay} seconds)")
                time.sleep(retry_delay)
            
            result = operation_func(*args, **kwargs)
            if result is not False:  # Consider False as failure
                return result
        except Exception as e:
            print(f"Operation failed on attempt {attempt + 1}: {str(e)}")
            if not ensure_connection():
                print("Failed to re-establish connection")
                return False
    return False

def prepare_order_line(line_data):
    """Prepare a single order line"""
    return [(0, 0, line_data)]

def create_or_update_po(po_data):
    """Create or update purchase order with individual lines"""
    try:
        po_name = po_data['name']
        order_line = po_data.get('order_line', [])
        
        # Search for existing PO
        try:
            po_ids = models.execute_kw(
                db, uid, password, 'purchase.order', 'search',
                [[['name', '=', po_name]]]
            )
        except Exception as e:
            print(f"Error searching PO {po_name}: {e}")
            if not ensure_connection():
                return False
            return False

        if po_ids:
            print(f"Updating existing PO: {po_name}")
            po_id = po_ids[0]
            
            try:
                # Get PO state
                po_state = models.execute_kw(
                    db, uid, password, 'purchase.order', 'read',
                    [po_id], {'fields': ['state']}
                )[0]['state']
                
                # Only modify if in draft state
                if po_state != 'draft':
                    print(f"Cannot modify PO {po_name} in state: {po_state}")
                    return False
                
                # Add new line to existing PO
                models.execute_kw(
                    db, uid, password, 'purchase.order', 'write',
                    [po_id, {
                        'partner_id': po_data['partner_id'],
                        'partner_ref': po_data.get('partner_ref', ''),
                        'date_order': po_data['date_order'],
                        'date_planned': po_data['date_planned'],
                        'picking_type_id': po_data['picking_type_id'],
                        'notes': po_data.get('notes', ''),
                        'order_line': order_line
                    }]
                )
                print(f"Successfully added new line to PO")
                return True
                
            except Exception as e:
                print(f"Error updating PO {po_name}: {e}")
                if not ensure_connection():
                    return False
                return False
                
        else:
            print(f"Creating new PO: {po_name}")
            try:
                # Create new PO with single line
                po_id = models.execute_kw(
                    db, uid, password, 'purchase.order', 'create',
                    [po_data]
                )
                print(f"Successfully created new PO with line")
                return True
            except Exception as e:
                print(f"Error creating PO: {e}")
                if not ensure_connection():
                    return False
                return False
                
    except Exception as e:
        error_msg = str(e)
        print(f"Error creating/updating PO: {error_msg}")
        log_error(po_data.get('name', 'N/A'), 'N/A', 'N/A', f"PO Creation/Update Error: {error_msg}")
        return False

def truncate_description(text, max_length=500):
    """Truncate description text to specified maximum length"""
    if not text:
        return ""
    if len(text) <= max_length:
        return text
    return text[:max_length-3] + "..."

def search_picking_type(picking_type_value):
    """Search for picking type in Odoo"""
    def get_default_picking_type():
        try:
            picking_type_ids = models.execute_kw(
                db, uid, password, 'stock.picking.type', 'search',
                [[['code', '=', 'incoming'], ['warehouse_id', '!=', False]]],
                {'limit': 1}
            )
            if picking_type_ids:
                print("Using default Purchase picking type")
                return picking_type_ids[0]
            return False
        except Exception as e:
            print(f"Error getting default picking type: {e}")
            if not ensure_connection():
                return False
            return False

    if not picking_type_value or pd.isna(picking_type_value):
        return get_default_picking_type()

    picking_type_value = str(picking_type_value).strip()
    
    try:
        # Get all picking types and their warehouses
        try:
            all_picking_types = models.execute_kw(
                db, uid, password, 'stock.picking.type', 'search_read',
                [[['code', '=', 'incoming']]],
                {'fields': ['name', 'warehouse_id']}
            )

            # Get warehouse details
            warehouse_ids = list(set([pt['warehouse_id'][0] for pt in all_picking_types if pt['warehouse_id']]))
            warehouses = models.execute_kw(
                db, uid, password, 'stock.warehouse', 'search_read',
                [[['id', 'in', warehouse_ids]]],
                {'fields': ['id', 'name']}
            )
            warehouse_dict = {w['id']: w['name'] for w in warehouses}
            
            # Try exact match on picking type name
            for pt in all_picking_types:
                if pt['name'].lower() == picking_type_value.lower():
                    print(f"Found picking type by exact name: {picking_type_value}")
                    return pt['id']
                    
            # Try partial match on picking type name
            for pt in all_picking_types:
                if picking_type_value.lower() in pt['name'].lower():
                    print(f"Found picking type by partial name: {picking_type_value}")
                    return pt['id']
            
            # Try warehouse name match
            for pt in all_picking_types:
                if pt['warehouse_id']:
                    warehouse_name = warehouse_dict.get(pt['warehouse_id'][0], '')
                    if picking_type_value.lower() in warehouse_name.lower():
                        print(f"Found picking type by warehouse name: {picking_type_value}")
                        return pt['id']
                    
        except Exception as e:
            print(f"Error searching picking types: {e}")
            if not ensure_connection():
                return False

        # If no match found, get default picking type
        print(f"\nCould not find picking type for value: {picking_type_value}")
        return get_default_picking_type()
        
    except Exception as e:
        error_msg = str(e)
        print(f"Error in search_picking_type: {error_msg}")
        return get_default_picking_type()

def search_product(product_value):
    """Search for product in Odoo using multiple search strategies"""
    if not isinstance(product_value, str):
        product_value = str(product_value)
    
    product_value = product_value.strip()
    
    try:
        # Function to safely execute search
        def safe_search(domain):
            try:
                return models.execute_kw(
                    db, uid, password, 'product.product', 'search',
                    [domain]
                )
            except Exception as e:
                print(f"Error in product search: {e}")
                if not ensure_connection():
                    return []
                return []

        # 1. Try exact match on default_code
        product_ids = safe_search([['default_code', '=', product_value]])
        if product_ids:
            print(f"Found product with default_code: {product_value}")
            return product_ids

        # 2. Try exact match on old_product_code
        product_ids = safe_search([['old_product_code', '=', product_value]])
        if product_ids:
            print(f"Found product with old_product_code: {product_value}")
            return product_ids

        # 3. Try case-insensitive match on default_code
        product_ids = safe_search([['default_code', 'ilike', product_value]])
        if product_ids:
            print(f"Found product with similar default_code: {product_value}")
            return product_ids

        # 4. Try case-insensitive match on old_product_code
        product_ids = safe_search([['old_product_code', 'ilike', product_value]])
        if product_ids:
            print(f"Found product with similar old_product_code: {product_value}")
            return product_ids

        # 5. For BG- codes, try searching without the prefix
        if product_value.upper().startswith('BG-'):
            code_without_prefix = product_value[3:]
            product_ids = safe_search([
                '|',
                ['default_code', 'ilike', code_without_prefix],
                ['old_product_code', 'ilike', code_without_prefix]
            ])
            if product_ids:
                print(f"Found product matching code without BG- prefix: {product_value}")
                return product_ids

        # 6. For MAC codes, try flexible matching
        if 'MAC' in product_value.upper():
            # Remove any spaces and try matching
            clean_code = product_value.upper().replace(' ', '')
            product_ids = safe_search([
                '|',
                ['default_code', 'ilike', clean_code],
                ['old_product_code', 'ilike', clean_code]
            ])
            if product_ids:
                print(f"Found product with cleaned MAC code: {product_value}")
                return product_ids

            # Try searching with wildcards for partial matches
            product_ids = safe_search([
                '|',
                ['default_code', 'ilike', f"%{product_value}%"],
                ['old_product_code', 'ilike', f"%{product_value}%"]
            ])
            if product_ids:
                print(f"Found product with partial code match: {product_value}")
                return product_ids

        print(f"Product not found: {product_value}")
        return []
        
    except Exception as e:
        error_msg = str(e)
        print(f"Error searching product: {error_msg}")
        if not ensure_connection():
            return []
        return []

def convert_date(pd_timestamp):
    """Convert pandas timestamp to string"""
    if pd.notnull(pd_timestamp):
        if isinstance(pd_timestamp, str):
            try:
                # Try to parse string date with explicit dayfirst=True for Thai date format
                parsed_date = pd.to_datetime(pd_timestamp, dayfirst=True)
                return parsed_date.strftime('%Y-%m-%d %H:%M:%S')
            except:
                return datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        return pd_timestamp.strftime('%Y-%m-%d %H:%M:%S')
    return datetime.now().strftime('%Y-%m-%d %H:%M:%S')  # Return current date if no date provided

def safe_float_conversion(value):
    """Safely convert various input formats to float"""
    if pd.isna(value):
        return 0.0
    try:
        if isinstance(value, (int, float)):
            return float(value)
        # Remove any currency symbols, spaces and commas
        clean_value = str(value).strip().replace('฿', '').replace(',', '').strip()
        if not clean_value:
            return 0.0
        return float(clean_value)
    except (ValueError, TypeError):
        return 0.0

# Initialize product cache
product_cache = {}

def search_product_with_cache(product_code):
    """Search for product with caching to avoid repeated searches"""
    if not product_code or pd.isna(product_code):
        return []
        
    # Check cache first
    if product_code in product_cache:
        return product_cache[product_code]
        
    # Search for product
    product_ids = search_product(product_code)
    
    # Cache the result
    product_cache[product_code] = product_ids
    
    return product_ids

def process_po_batch(batch_df, batch_num, total_batches):
    """Process a batch of purchase orders"""
    print(f"\nProcessing batch {batch_num}/{total_batches} ({len(batch_df)} rows)")
    
    success_count = 0
    error_count = 0
    
    # Process each row individually
    for index, row in batch_df.iterrows():
        try:
            po_name = row['name']
            print(f"\nProcessing row {index + 1} for PO: {po_name}")
            
            # Find vendor
            vendor_id = search_vendor(
                partner_name=None,
                partner_code=None,
                partner_id=row['partner_id'] if pd.notna(row['partner_id']) else None
            )
            
            if not vendor_id:
                error_count += 1
                log_error(po_name, str(index), 'N/A', "Vendor not found or could not be created", index, row.to_dict())
                continue
            
            # Get picking type
            picking_type_id = search_picking_type(row['picking_type_id'] if pd.notna(row.get('picking_type_id')) else None)
            if not picking_type_id:
                error_count += 1
                log_error(po_name, str(index), 'N/A', "Could not find or create picking type", index, row.to_dict())
                continue
            
            # Try to find product using cache
            product_ids = search_product_with_cache(row['default_code']) if pd.notna(row.get('default_code')) else []
            
            # If not found by default_code, try old_product_code
            if not product_ids and pd.notna(row.get('old_product_code')):
                product_ids = search_product_with_cache(row['old_product_code'])
            
            if not product_ids:
                error_count += 1
                product_code = row.get('default_code', row.get('old_product_code', 'N/A'))
                log_error(po_name, str(index), product_code, "Product not found", index, row.to_dict())
                continue
            
            # Process quantity with improved validation
            quantity = safe_float_conversion(row['product_qty'])
            if quantity <= 0:
                print(f"Warning: Zero or negative quantity ({row['product_qty']}) for product {row.get('old_product_code', 'N/A')}")
                error_count += 1
                log_error(po_name, str(index), row.get('old_product_code', 'N/A'), 
                         f"Invalid quantity: {row['product_qty']}", index, row.to_dict())
                continue
            
            # Process date_planned
            date_planned = convert_date(row['date_planned']) if pd.notna(row.get('date_planned')) else convert_date(None)
            
            # Prepare the description (simplified and truncated)
            base_description = str(row['description']) if pd.notna(row.get('description')) else row.get('old_product_code', '')
            
            # Add essential information only
            description_parts = [base_description]
            if pd.notna(row.get('note')):
                description_parts.append(f"Note: {row['note']}")
            
            # Join parts and truncate
            description = "\n".join(description_parts)
            description = truncate_description(description)

            # Create line data
            line_data = {
                'product_id': product_ids[0],
                'name': description,
                'product_qty': quantity,
                'price_unit': safe_float_conversion(row['price_unit']),
                'date_planned': date_planned,
            }
            
            # Prepare PO data for this line
            po_data = {
                'name': po_name,
                'partner_id': vendor_id,
                'partner_ref': row.get('partner_ref', ''),
                'date_order': convert_date(row['date_order']),
                'date_planned': convert_date(row['date_planned']),
                'picking_type_id': picking_type_id,
                'order_line': prepare_order_line(line_data)
            }
            
            if create_or_update_po(po_data):
                success_count += 1
                print(f"Successfully processed line for PO: {po_name}")
                log_success(po_name, str(index), row.get('default_code', row.get('old_product_code', 'N/A')), index)
            else:
                error_count += 1
                print(f"Failed to process line for PO: {po_name}")
            
        except Exception as e:
            error_count += 1
            print(f"Error processing row {index + 1}: {str(e)}")
            log_error(row.get('name', 'N/A'), str(index), 'N/A', 
                     f"Processing Error: {str(e)}", index, row.to_dict())
    
    return success_count, error_count

def main():
    total_success = 0
    total_errors = 0
    
    try:
        # Read Excel file
        excel_file = 'Data_file/import_OB6.xlsx'
        df = pd.read_excel(excel_file)
        print(f"\nOriginal Excel columns: {df.columns.tolist()}")
        print(f"\nExcel file '{excel_file}' read successfully. Number of rows = {len(df)}")
        
        # Process in smaller batches
        batch_size = 10  # Reduced batch size from 50 to 10
        total_rows = len(df)
        total_batches = (total_rows + batch_size - 1) // batch_size
        
        print(f"\nProcessing {total_rows} rows in {total_batches} batches (batch size: {batch_size})")
        
        # Process each batch
        for batch_num in range(total_batches):
            start_idx = batch_num * batch_size
            end_idx = min(start_idx + batch_size, total_rows)
            batch_df = df.iloc[start_idx:end_idx]
            
            success_count, error_count = process_po_batch(batch_df, batch_num + 1, total_batches)
            total_success += success_count
            total_errors += error_count
            
            # Print batch summary
            print(f"\nBatch {batch_num + 1} Summary:")
            print(f"Successful lines: {success_count}")
            print(f"Failed lines: {error_count}")
            
            # Minimal delay between batches
            if batch_num < total_batches - 1:
                time.sleep(0.5)  # ลดเวลารอเหลือ 0.5 วินาที
        
        # Print final summary
        print("\nFinal Import Summary:")
        print(f"Total Successful lines: {total_success}")
        print(f"Total Failed lines: {total_errors}")
        print(f"Total Processed: {total_success + total_errors}")
        
    except Exception as e:
        print(f"Error in main function: {e}")
        log_error('N/A', 'N/A', 'N/A', f"Main Function Error: {str(e)}")
    
    finally:
        # Save logs
        save_import_logs()
        print("\nImport process completed.")

if __name__ == "__main__":
    main()