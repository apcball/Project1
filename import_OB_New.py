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

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Constants
BATCH_SIZE = 100  # Number of records to process in each batch
MAX_WORKERS = 4   # Number of concurrent threads
CACHE_SIZE = 1000 # Size of LRU cache for vendor lookups

# Create log directory if it doesn't exist
if not os.path.exists('logs'):
    os.makedirs('logs')

# Initialize lists to store successful and failed imports with thread safety
failed_imports_lock = threading.Lock()
failed_imports = []
error_messages = []

# Performance monitoring
class PerformanceMonitor:
    def __init__(self):
        self.start_time = None
        self.records_processed = 0
        self.lock = threading.Lock()

    def start(self):
        self.start_time = time.time()

    def increment(self, count=1):
        with self.lock:
            self.records_processed += count

    def get_stats(self):
        if self.start_time is None:
            return "Processing not started"
        elapsed_time = time.time() - self.start_time
        records_per_second = self.records_processed / elapsed_time if elapsed_time > 0 else 0
        return f"Processed {self.records_processed} records in {elapsed_time:.2f} seconds ({records_per_second:.2f} records/sec)"

performance_monitor = PerformanceMonitor()

def log_error(po_name, line_number, product_code, error_message):
    """Log error details for failed imports with thread safety"""
    with failed_imports_lock:
        failed_imports.append({
            'PO Number': po_name,
            'Line Number': line_number,
            'Product Code': product_code,
            'Error Message': error_message,
            'Date Time': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        })
        error_messages.append(f"Error in PO {po_name}, Line {line_number}: {error_message}")
        logger.error(f"Import error - PO: {po_name}, Line: {line_number}, Error: {error_message}")

def save_error_log():
    """Save error log to Excel file with memory optimization"""
    if failed_imports:
        try:
            # Create DataFrame in chunks to optimize memory
            chunk_size = 1000
            chunks = [failed_imports[i:i + chunk_size] for i in range(0, len(failed_imports), chunk_size)]
            
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            log_file = f'logs/import_errors_{timestamp}.xlsx'
            
            # Write first chunk with header
            pd.DataFrame(chunks[0]).to_excel(log_file, index=False)
            
            # Append remaining chunks
            if len(chunks) > 1:
                with pd.ExcelWriter(log_file, mode='a', engine='openpyxl') as writer:
                    for chunk in chunks[1:]:
                        pd.DataFrame(chunk).to_excel(writer, index=False, header=False)
            
            logger.info(f"Error log saved to: {log_file}")
            
            # Print error summary
            logger.info("\nError Summary:")
            for msg in error_messages:
                logger.info(msg)
                
        except Exception as e:
            logger.error(f"Error saving log file: {str(e)}")
            
        finally:
            # Clear memory
            gc.collect()

# --- Connection Settings ---
url = 'http://mogth.work:8069/'
db = 'MOG_LIVE'
username = 'apichart@mogen.co.th'
password = '471109538'

# --- Data File Settings ---
excel_file = 'Data_file/import_OB5.xlsx'

class OdooConnection:
    def __init__(self):
        self.url = url
        self.db = db
        self.username = username
        self.password = password
        self.uid = None
        self.models = None
        self._connection_lock = threading.Lock()
        self._last_activity = time.time()
        self.timeout = 30
        
    def _create_transport(self):
        """Create custom transport class with timeout"""
        class TimeoutTransport(xmlrpc.client.Transport):
            def __init__(self):
                super().__init__()
                self.timeout = 30

            def make_connection(self, host):
                connection = super().make_connection(host)
                if hasattr(connection, '_conn'):
                    connection._conn.timeout = self.timeout
                else:
                    connection.timeout = self.timeout
                return connection
        return TimeoutTransport()

    def connect(self):
        """Create a new connection to Odoo with timeout handling"""
        try:
            # Create connection with custom transport
            common = xmlrpc.client.ServerProxy(
                f'{self.url}/xmlrpc/2/common',
                transport=self._create_transport()
            )
            
            # Test connection
            common.version()
            
            # Authenticate
            self.uid = common.authenticate(self.db, self.username, self.password, {})
            if not self.uid:
                print("Authentication failed")
                return False
            
            # Create models proxy
            self.models = xmlrpc.client.ServerProxy(
                f'{self.url}/xmlrpc/2/object',
                transport=self._create_transport()
            )
            
            self._last_activity = time.time()
            print(f"Connection successful, uid = {self.uid}")
            return True
            
        except Exception as e:
            print(f"Connection error: {str(e)}")
            return False

    def ensure_connected(self):
        """Ensure connection is active and fresh"""
        with self._connection_lock:
            # Check if connection is stale (inactive for more than 5 minutes)
            if time.time() - self._last_activity > 300:
                print("Connection stale, reconnecting...")
                return self.connect()
            return True

    def execute(self, model, method, *args, **kwargs):
        """Execute Odoo method with automatic reconnection"""
        max_retries = 3
        retry_delay = 1
        
        for attempt in range(max_retries):
            try:
                if not self.ensure_connected():
                    raise Exception("Connection failed")
                    
                result = self.models.execute_kw(
                    self.db, self.uid, self.password,
                    model, method, args, kwargs
                )
                self._last_activity = time.time()
                return result
                
            except Exception as e:
                if attempt < max_retries - 1:
                    print(f"Attempt {attempt + 1} failed: {str(e)}")
                    time.sleep(retry_delay * (2 ** attempt))
                else:
                    raise

# Initialize global connection for backward compatibility
odoo_connection = OdooConnection()
uid, models = None, None

def connect_to_odoo():
    """Create a new connection to Odoo with timeout handling - Legacy support"""
    global uid, models
    if odoo_connection.connect():
        uid = odoo_connection.uid
        models = odoo_connection.models
        return uid, models
    return None, None

def ensure_connection():
    """Ensure connection is active - Legacy support"""
    global uid, models
    if odoo_connection.ensure_connected():
        uid = odoo_connection.uid
        models = odoo_connection.models
        return True
    return False

# Initial connection
uid, models = connect_to_odoo()
if not uid or not models:
    logger.error("Initial connection failed")
    sys.exit(1)

@lru_cache(maxsize=1000)
def search_vendor(partner_name=None, partner_code=None, partner_id=None):
    """Search for vendor in Odoo. If not found, create a new one."""
    try:
        if not partner_id or pd.isna(partner_id):
            logger.warning("No vendor information provided")
            return False

        vendor_name = str(partner_id).strip()
        
        # Search for existing vendor
        try:
            vendor_ids = models.execute_kw(
                db, uid, password, 'res.partner', 'search',
                [[['name', '=', vendor_name]]]
            )
        except Exception as e:
            logger.error(f"Error searching vendor: {e}")
            if not ensure_connection():
                return False
            return False
        
        if vendor_ids:
            logger.info(f"Found existing vendor: {vendor_name}")
            return vendor_ids[0]
        
        # If vendor not found, create a new one
        logger.info(f"Vendor not found: {vendor_name}. Creating new vendor...")
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
            logger.info(f"Successfully created new vendor: {vendor_name} (ID: {new_vendor_id})")
            return new_vendor_id
        except Exception as create_error:
            logger.error(f"Failed to create vendor: {vendor_name}")
            logger.error(f"Creation error: {str(create_error)}")
            if not ensure_connection():
                return False
            return False
        
    except Exception as e:
        error_msg = str(e)
        logger.error(f"Error in search_vendor: {error_msg}")
        log_error('N/A', 'N/A', 'N/A', f"Vendor Search Error: {error_msg}")
        return False

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
        
            # 7. Try searching with wildcards for partial matches
            product_ids = safe_search([
                '|',
                ['default_code', 'ilike', f"%{product_value}%"],
                ['old_product_code', 'ilike', f"%{product_value}%"]
            ])
            if product_ids:
                print(f"Found product with partial code match: {product_value}")
                return product_ids
        
        print(f"Product not found: {product_value}")
        log_error('N/A', 'N/A', product_value, f"Product not found in system: {product_value}")
        return []
        
    except Exception as e:
        error_msg = str(e)
        print(f"Error searching product: {error_msg}")
        log_error('N/A', 'N/A', product_value, f"Product Search Error: {error_msg}")
        if not ensure_connection():
            return []
        return []

def convert_date(pd_timestamp):
    """Convert pandas timestamp to string"""
    if pd.notnull(pd_timestamp):
        if isinstance(pd_timestamp, str):
            try:
                # Try to parse string date
                parsed_date = pd.to_datetime(pd_timestamp)
                return parsed_date.strftime('%Y-%m-%d %H:%M:%S')
            except:
                return datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        return pd_timestamp.strftime('%Y-%m-%d %H:%M:%S')
    return datetime.now().strftime('%Y-%m-%d %H:%M:%S')  # Return current date if no date provided

def get_tax_id(tax_value):
    """Get tax ID from value"""
    if not tax_value or pd.isna(tax_value):
        return False

    try:
        all_taxes = models.execute_kw(
            db, uid, password, 'account.tax', 'search_read',
            [[['type_tax_use', 'in', ['purchase', 'all']], ['active', '=', True]]],
            {'fields': ['id', 'name', 'amount', 'type_tax_use']}
        )

        if isinstance(tax_value, str):
            tax_value = tax_value.strip()
            if tax_value.endswith('%'):
                tax_percentage = float(tax_value.rstrip('%'))
            else:
                tax_percentage = float(tax_value) * 100
        else:
            tax_percentage = float(tax_value) * 100

        matching_taxes = [tax for tax in all_taxes if abs(tax['amount'] - tax_percentage) < 0.01]
        if matching_taxes:
            tax_id = matching_taxes[0]['id']
            print(f"Found purchase tax {tax_percentage}% with ID: {tax_id}")
            return tax_id

        print(f"Tax not found: {tax_value}")
        return False
    except Exception as e:
        print(f"Error getting tax ID: {e}")
        if not ensure_connection():
            return False
        return False

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

        # If no match found, get all picking types and print them for debugging
        try:
            all_picking_types = models.execute_kw(
                db, uid, password, 'stock.picking.type', 'search_read',
                [[['code', '=', 'incoming']]],
                {'fields': ['name', 'warehouse_id']}
            )
            print(f"\nAvailable picking types:")
            for pt in all_picking_types:
                print(f"- {pt['name']} (ID: {pt['id']})")
        except Exception as e:
            print(f"Error getting all picking types: {e}")
            if not ensure_connection():
                return False

        print(f"\nCould not find picking type for value: {picking_type_value}")
        return get_default_picking_type()
        
    except Exception as e:
        error_msg = str(e)
        print(f"Error in search_picking_type: {error_msg}")
        return get_default_picking_type()

def create_or_update_po(po_data):
    """Create or update a purchase order in Odoo"""
    try:
        po_name = po_data['name']
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
                existing_lines = models.execute_kw(
                    db, uid, password, 'purchase.order.line', 'search_read',
                    [[['order_id', '=', po_id]]],
                    {'fields': ['id', 'product_id', 'product_qty', 'price_unit', 'taxes_id']}
                )
            except Exception as e:
                print(f"Error reading PO lines for {po_name}: {e}")
                if not ensure_connection():
                    return False
                return False
            
            try:
                models.execute_kw(
                    db, uid, password, 'purchase.order', 'write',
                    [po_id, {
                        'partner_id': po_data['partner_id'],
                        'partner_ref': po_data.get('partner_ref', ''),
                        'date_order': po_data['date_order'],
                        'date_planned': po_data['date_planned'],
                        'picking_type_id': po_data['picking_type_id'],
                        'notes': po_data.get('notes', ''),
                    }]
                )
            except Exception as e:
                print(f"Error updating PO {po_name}: {e}")
                if not ensure_connection():
                    return False
                return False
            
            # Process all lines as new lines
            for line in po_data['order_line']:
                try:
                    product_id = line[2].get('product_id')
                    if not product_id:
                        print(f"Warning: Missing product ID in line data for PO {po_name}")
                        continue

                    # Create new line for each entry
                    line_data = line[2].copy()
                    line_data['order_id'] = po_id
                    
                    # Validate quantity before create
                    if 'product_qty' in line_data:
                        qty = safe_float_conversion(line_data['product_qty'])
                        if qty <= 0:
                            print(f"Warning: Invalid quantity in new line: {line_data['product_qty']}")
                            continue
                        line_data['product_qty'] = qty

                    new_line_id = models.execute_kw(
                        db, uid, password, 'purchase.order.line', 'create',
                        [line_data]
                    )
                except Exception as e:
                    print(f"Error processing line for PO {po_name}: {e}")
                    if not ensure_connection():
                        return False
                    continue

            # Remove lines that were not updated (optional - uncomment if needed)
            # unused_lines = [line['id'] for line in existing_lines if line['id'] not in updated_line_ids]
            # if unused_lines:
            #     try:
            #         models.execute_kw(
            #             db, uid, password, 'purchase.order.line', 'unlink',
            #             [unused_lines]
            #         )
            #     except Exception as e:
            #         print(f"Error removing unused lines for PO {po_name}: {e}")
            #         if not ensure_connection():
            #             return False
            
            print(f"Successfully updated PO: {po_name}")
            return True
        else:
            print(f"Creating new PO: {po_name}")
            try:
                po_id = models.execute_kw(
                    db, uid, password, 'purchase.order', 'create',
                    [po_data]
                )
                print(f"Successfully created PO: {po_name}")
                return True
            except Exception as e:
                print(f"Error creating PO {po_name}: {e}")
                if not ensure_connection():
                    return False
                return False
    except Exception as e:
        error_msg = str(e)
        print(f"Error creating/updating PO: {error_msg}")
        log_error(po_data.get('name', 'N/A'), 'N/A', 'N/A', f"PO Creation/Update Error: {error_msg}")
        return False

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

def process_po_batch(batch_df, batch_num, total_batches):
    """Process a batch of purchase orders"""
    print(f"\nProcessing batch {batch_num}/{total_batches} ({len(batch_df)} rows)")
    
    success_count = 0
    error_count = 0
    MAX_LINES_PER_PO = 500  # Maximum lines per PO
    
    # Group by PO number within the batch
    for po_name, po_group in batch_df.groupby('name'):
        try:
            print(f"\nProcessing PO: {po_name}")
            
            # Get first row for PO header data
            first_row = po_group.iloc[0]
            
            # Find vendor
            vendor_id = search_vendor(
                partner_name=None,
                partner_code=None,
                partner_id=first_row['partner_id'] if pd.notna(first_row['partner_id']) else None
            )
            
            if not vendor_id:
                print(f"Warning: Vendor not found for PO {po_name}")
                continue
            
            # Get picking type
            picking_type_id = search_picking_type(first_row['picking_type_id'] if pd.notna(first_row.get('picking_type_id')) else None)
            if not picking_type_id:
                print(f"Warning: Could not find picking type for PO {po_name}")
                continue
            
            # Process all lines first to check products
            all_lines = []
            valid_lines_count = 0
            
            for _, line in po_group.iterrows():
                # Try to find product by default_code first
                product_ids = search_product(line['default_code']) if pd.notna(line.get('default_code')) else []
                
                # If not found by default_code, try old_product_code
                if not product_ids:
                    product_ids = search_product(line['old_product_code'])
                
                if not product_ids:
                    print(f"Product not found: {line.get('default_code', line['old_product_code'])}")
                    continue
                
                # Process quantity with improved validation
                quantity = safe_float_conversion(line['product_qty'])
                if quantity <= 0:
                    print(f"Warning: Zero or negative quantity ({line['product_qty']}) for product {line['old_product_code']}")
                    continue
                
                valid_lines_count += 1
                
                # Prepare the description with note and date_planned if available
                description = str(line['description']) if 'description' in line and pd.notna(line['description']) else line['old_product_code']
                
                # Add date_planned to description if available
                date_planned = convert_date(line['date_planned']) if pd.notna(line['date_planned']) else False
                if date_planned:
                    description = f"{description}\nExpected Arrival: {date_planned}"
                
                # Add note if available
                if 'note' in line and pd.notna(line['note']):
                    description = f"{description}\nNote: {line['note']}"

                line_data = {
                    'product_id': product_ids[0],
                    'name': description,
                    'product_qty': quantity,
                    'price_unit': float(line['price_unit']) if pd.notna(line['price_unit']) else 0.0,
                    'date_planned': date_planned,
                    'taxes_id': [(6, 0, [])]  # Set empty tax (VAT = 0)
                }
                
                all_lines.append((0, 0, line_data))
            
            if valid_lines_count == 0:
                print(f"Warning: No valid lines found for PO {po_name}")
                continue
            
            # Split into multiple POs if needed
            po_count = (len(all_lines) + MAX_LINES_PER_PO - 1) // MAX_LINES_PER_PO
            
            for po_index in range(po_count):
                start_idx = po_index * MAX_LINES_PER_PO
                end_idx = start_idx + MAX_LINES_PER_PO
                current_lines = all_lines[start_idx:end_idx]
                
                # Create PO name with suffix if split
                current_po_name = po_name if po_count == 1 else f"{po_name}-{po_index + 1}"
                
                # Prepare PO data
                po_data = {
                    'name': current_po_name,
                    'partner_id': vendor_id,
                    'partner_ref': first_row.get('partner_ref', ''),
                    'date_order': convert_date(first_row['date_order']),
                    'date_planned': convert_date(first_row['date_planned']),
                    'picking_type_id': picking_type_id,
                    'order_line': current_lines
                }
                
                if create_or_update_po(po_data):
                    success_count += 1
                    print(f"Successfully created PO: {current_po_name} with {len(current_lines)} lines")
                else:
                    error_count += 1
                    log_error(current_po_name, 'N/A', 'N/A', f"Failed to add order lines to PO")
                    print(f"Failed to create/update PO: {current_po_name}")
                
        except Exception as e:
            error_count += 1
            print(f"Error processing PO {po_name}: {str(e)}")
            log_error(po_name, 'N/A', 'N/A', f"Processing Error: {str(e)}")
    
    return success_count, error_count

def main():
    total_success = 0
    total_errors = 0
    
    try:
        # Read Excel file
        df = pd.read_excel(excel_file)
        print(f"\nOriginal Excel columns: {df.columns.tolist()}")
        print(f"\nExcel file '{excel_file}' read successfully. Number of rows = {len(df)}")
        
        # Process in batches
        batch_size = 50  # Number of rows per batch
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
            print(f"Successful POs: {success_count}")
            print(f"Failed POs: {error_count}")
            
            # Optional: Add a small delay between batches to prevent overloading
            if batch_num < total_batches - 1:
                time.sleep(1)  # 1 second delay between batches
        
        # Print final summary
        print("\nFinal Import Summary:")
        print(f"Total Successful POs: {total_success}")
        print(f"Total Failed POs: {total_errors}")
        print(f"Total Processed: {total_success + total_errors}")
        
    except Exception as e:
        print(f"Error in main function: {e}")
        log_error('N/A', 'N/A', 'N/A', f"Main Function Error: {str(e)}")
    
    finally:
        # Save error log if there were any errors
        save_error_log()
        print("\nImport process completed.")

if __name__ == "__main__":
    main()