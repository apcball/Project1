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
    CONNECT_MAX_RETRIES = 3
    IMPORT_MAX_RETRIES = 2
    INITIAL_RETRY_DELAY = 2
    MAX_RETRY_DELAY = 30

def ensure_connection():
    """Ensure connection is active, attempt to reconnect if needed"""
    global uid, models
    max_retries = RetryConfig.CONNECT_MAX_RETRIES
    delay = RetryConfig.INITIAL_RETRY_DELAY
    
    for attempt in range(max_retries):
        if attempt > 0:
            print(f"\nRetrying connection (attempt {attempt + 1}/{max_retries})...")
            time.sleep(delay)
            delay = min(delay * 2, RetryConfig.MAX_RETRY_DELAY)
        
        uid, models = connect_to_odoo()
        if uid and models:
            return True
    
    print("\nFailed to establish connection after multiple attempts")
    return False

def import_purchase_order(file_path: str) -> None:
    """Import purchase orders from Excel file with batch processing and improved error handling"""
    
    if not ensure_connection():
        print("Cannot proceed with import due to connection issues")
        return
    
    try:
        # Load Excel file
        df = pd.read_excel(file_path)
        total_rows = len(df)
        print(f"\nLoaded {total_rows} rows from {file_path}")
        
        # Load previous state
        start_row, last_file = load_state()
        
        # If processing a new file, start from beginning
        if last_file != file_path:
            start_row = 0
            clear_state()
        
        print(f"Starting from row {start_row}")
        
        # Process in batches of 50
        BATCH_SIZE = 50
        current_batch = []
        batch_data = []
        
        # Process each row and prepare batches
        for index, row in df.iloc[start_row:].iterrows():
            try:
                # Extract and validate required fields
                po_name = str(row.get('PO Number', '')).strip()
                line_number = row.get('Line Number', '')
                product_code = str(row.get('Product Code', '')).strip()
                
                # Skip row if any required field is missing
                if not all([po_name, line_number, product_code]):
                    error_msg = "Missing required fields"
                    log_error(po_name, line_number, product_code, error_msg, index, row.to_dict())
                    continue
                
                # Prepare row data for batch processing
                row_data = {
                    'index': index,
                    'po_name': po_name,
                    'line_number': line_number,
                    'product_code': product_code,
                    'ob_qty': float(row.get('OB Qty', 0)),
                    'ob_date': row.get('OB Date', '').strftime('%Y-%m-%d') if pd.notnull(row.get('OB Date')) else False,
                    'row_dict': row.to_dict()
                }
                
                current_batch.append(row_data)
                
                # Process batch when it reaches BATCH_SIZE or at the end
                if len(current_batch) >= BATCH_SIZE or index == len(df) - 1:
                    # Collect all PO numbers and product codes in batch
                    po_names = list(set(d['po_name'] for d in current_batch))
                    product_codes = list(set(d['product_code'] for d in current_batch))
                    
                    # Bulk fetch POs
                    po_domain = [('name', 'in', po_names)]
                    pos = models.execute_kw(db, uid, password, 'purchase.order', 'search_read', 
                                         [po_domain], {'fields': ['name', 'id']})
                    po_map = {po['name']: po['id'] for po in pos}
                    
                    # Bulk fetch products
                    product_domain = [('default_code', 'in', product_codes)]
                    products = models.execute_kw(db, uid, password, 'product.product', 'search_read',
                                              [product_domain], {'fields': ['default_code', 'id']})
                    product_map = {prod['default_code']: prod['id'] for prod in products}
                    
                    # Process each row in batch
                    for row_data in current_batch:
                        try:
                            po_id = po_map.get(row_data['po_name'])
                            product_id = product_map.get(row_data['product_code'])
                            
                            if not po_id:
                                error_msg = f"Purchase Order {row_data['po_name']} not found"
                                log_error(row_data['po_name'], row_data['line_number'], 
                                        row_data['product_code'], error_msg, row_data['index'], 
                                        row_data['row_dict'])
                                continue
                                
                            if not product_id:
                                error_msg = f"Product with code {row_data['product_code']} not found"
                                log_error(row_data['po_name'], row_data['line_number'],
                                        row_data['product_code'], error_msg, row_data['index'],
                                        row_data['row_dict'])
                                continue
                            
                            # Get PO line
                            line_domain = [
                                ('order_id', '=', po_id),
                                ('product_id', '=', product_id),
                                ('sequence', '=', row_data['line_number'])
                            ]
                            
                            line_ids = models.execute_kw(db, uid, password, 'purchase.order.line',
                                                       'search', [line_domain])
                            
                            if not line_ids:
                                error_msg = f"Line {row_data['line_number']} not found for PO {row_data['po_name']} and product {row_data['product_code']}"
                                log_error(row_data['po_name'], row_data['line_number'],
                                        row_data['product_code'], error_msg, row_data['index'],
                                        row_data['row_dict'])
                                continue
                            
                            # Prepare update values
                            ob_values = {
                                'ob_qty': row_data['ob_qty'],
                                'ob_date': row_data['ob_date']
                            }
                            
                            # Add to batch update
                            batch_data.append((line_ids[0], ob_values))
                            
                        except Exception as e:
                            error_msg = f"Error processing row: {str(e)}"
                            log_error(row_data['po_name'], row_data['line_number'],
                                    row_data['product_code'], error_msg, row_data['index'],
                                    row_data['row_dict'])
                    
                    # Perform batch update with retry mechanism
                    if batch_data:
                        retry_count = 0
                        success = False
                        
                        while retry_count < RetryConfig.IMPORT_MAX_RETRIES and not success:
                            try:
                                for line_id, values in batch_data:
                                    models.execute_kw(db, uid, password, 'purchase.order.line',
                                                    'write', [line_id, values])
                                success = True
                                
                                # Log successful updates
                                for row_data in current_batch:
                                    log_success(row_data['po_name'], row_data['line_number'],
                                              row_data['product_code'], row_data['index'])
                                
                            except Exception as e:
                                retry_count += 1
                                if retry_count == RetryConfig.IMPORT_MAX_RETRIES:
                                    error_msg = f"Failed to update batch after {retry_count} attempts: {str(e)}"
                                    print(f"\nError: {error_msg}")
                                else:
                                    time.sleep(RetryConfig.INITIAL_RETRY_DELAY * (2 ** retry_count))
                                    ensure_connection()
                    
                    # Save state and show progress
                    save_state(index + 1, file_path)
                    progress = ((index + 1) / total_rows) * 100
                    print(f"\rProgress: {progress:.1f}% ({index + 1}/{total_rows} rows)", end="")
                    
                    # Clear batch data
                    current_batch = []
                    batch_data = []
            
            except Exception as e:
                error_msg = f"Unexpected error: {str(e)}"
                log_error(po_name if 'po_name' in locals() else '',
                         line_number if 'line_number' in locals() else '',
                         product_code if 'product_code' in locals() else '',
                         error_msg, index, row.to_dict() if 'row' in locals() else None)
                continue
        
        # Clear state file after successful completion
        clear_state()
        
    except Exception as e:
        print(f"Critical error during import: {str(e)}")
    finally:
        # Save final logs
        save_import_logs()

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python import_OB.py <excel_file_path>")
        sys.exit(1)
        
    file_path = sys.argv[1]
    if not os.path.exists(file_path):
        print(f"File not found: {file_path}")
        sys.exit(1)
    
    import_purchase_order(file_path)