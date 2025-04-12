import xmlrpc.client
import pandas as pd
import sys
import re
from datetime import datetime
import csv
import os
import time

# Create log directory if it doesn't exist
if not os.path.exists('logs'):
    os.makedirs('logs')

# Initialize lists to store successful and failed imports
failed_imports = []
error_messages = []

def log_error(po_name, line_number, product_code, error_message):
    """Log error details for failed imports"""
    failed_imports.append({
        'PO Number': po_name,
        'Line Number': line_number,
        'Product Code': product_code,
        'Error Message': error_message,
        'Date Time': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    })
    error_messages.append(f"Error in PO {po_name}, Line {line_number}: {error_message}")

def save_error_log():
    """Save error log to Excel file"""
    if failed_imports:
        # Create DataFrame from failed imports
        df_errors = pd.DataFrame(failed_imports)
        
        # Generate filename with timestamp
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        log_file = f'logs/import_errors_{timestamp}.xlsx'
        
        # Save to Excel
        df_errors.to_excel(log_file, index=False)
        print(f"\nError log saved to: {log_file}")
        
        # Print error summary
        print("\nError Summary:")
        for msg in error_messages:
            print(msg)

# --- Connection Settings ---
url = 'http://mogth.work:8069'
db = 'MOG_LIVE'
username = 'apichart@mogen.co.th'
password = '471109538'

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

def ensure_connection():
    """Ensure connection is active, attempt to reconnect if needed"""
    global uid, models
    max_retries = 5
    initial_retry_delay = 5  # seconds
    max_retry_delay = 60  # maximum delay in seconds
    
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
            
            # Process existing lines more safely
            existing_lines_dict = {
                (line['product_id'][0] if line['product_id'] else None): line 
                for line in existing_lines
            }

            # Track which existing lines were updated
            updated_line_ids = set()

            # Process new/updated lines
            for line in po_data['order_line']:
                try:
                    product_id = line[2].get('product_id')
                    if not product_id:
                        print(f"Warning: Missing product ID in line data for PO {po_name}")
                        continue

                    # Check if line exists for this product
                    existing_line = existing_lines_dict.get(product_id)
                    
                    if existing_line:
                        # Update existing line
                        line_data = line[2].copy()
                        line_data['order_id'] = po_id
                        
                        # Validate quantity before update
                        if 'product_qty' in line_data:
                            qty = safe_float_conversion(line_data['product_qty'])
                            if qty <= 0:
                                print(f"Warning: Invalid quantity in update: {line_data['product_qty']}")
                                continue
                            line_data['product_qty'] = qty

                        models.execute_kw(
                            db, uid, password, 'purchase.order.line', 'write',
                            [existing_line['id'], line_data]
                        )
                        updated_line_ids.add(existing_line['id'])
                    else:
                        # Create new line
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
                error_count += 1
                log_error(po_name, 'N/A', 'N/A', "Vendor not found or could not be created")
                continue
            
            # Get picking type
            picking_type_id = search_picking_type(first_row['picking_type_id'] if pd.notna(first_row.get('picking_type_id')) else None)
            if not picking_type_id:
                error_count += 1
                log_error(po_name, 'N/A', 'N/A', "Could not find or create picking type")
                continue
            
            # Process all lines first to check products
            all_lines = []
            all_products_found = True
            
            for _, line in po_group.iterrows():
                # Try to find product by default_code first
                product_ids = search_product(line['default_code']) if pd.notna(line.get('default_code')) else []
                
                # If not found by default_code, try old_product_code
                if not product_ids:
                    product_ids = search_product(line['old_product_code'])
                
                if not product_ids:
                    all_products_found = False
                    error_count += 1
                    product_code = line.get('default_code', line['old_product_code'])
                    log_error(po_name, line.name, product_code, "Product not found - Skipping entire PO")
                    break
                
                # Process quantity with improved validation
                quantity = safe_float_conversion(line['product_qty'])
                if quantity <= 0:
                    print(f"Warning: Zero or negative quantity ({line['product_qty']}) for product {line['old_product_code']}")
                    error_count += 1
                    log_error(po_name, line.name, line['old_product_code'], f"Invalid quantity: {line['product_qty']}")
                    continue
                
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
                }
                
                all_lines.append((0, 0, line_data))
            
            if not all_products_found:
                error_count += 1
                print(f"Skipping PO {po_name} due to missing products")
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
                    print(f"Failed to create PO: {current_po_name}")
                
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
        excel_file = 'Data_file/import_OB2.xlsx'
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