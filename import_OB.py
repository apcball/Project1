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
db = 'MOG_Training'
username = 'apichart@mogen.co.th'
password = '471109538'

# --- Authenticate with Odoo ---
try:
    common = xmlrpc.client.ServerProxy(f'{url}/xmlrpc/2/common')
    uid = common.authenticate(db, username, password, {})
    if not uid:
        print("Authentication failed: invalid credentials or insufficient permissions.")
        sys.exit(1)
    else:
        print("Authentication successful, uid =", uid)
except Exception as e:
    print("Error during connection/authentication:", e)
    sys.exit(1)

# --- Create XML-RPC models proxy ---
try:
    models = xmlrpc.client.ServerProxy(f'{url}/xmlrpc/2/object')
except Exception as e:
    print("Error creating XML-RPC models proxy:", e)
    sys.exit(1)

def search_vendor(partner_name=None, partner_code=None, partner_id=None):
    """Search for vendor in Odoo"""
    try:
        if partner_name and not pd.isna(partner_name):
            vendor_ids = models.execute_kw(
                db, uid, password, 'res.partner', 'search',
                [[['name', '=', str(partner_name).strip()]]]
            )
            if vendor_ids:
                print(f"Found existing vendor with name: {partner_name}")
                return vendor_ids[0]

        if partner_code and not pd.isna(partner_code):
            vendor_ids = models.execute_kw(
                db, uid, password, 'res.partner', 'search',
                [[['ref', '=', str(partner_code).strip()]]]
            )
            if vendor_ids:
                print(f"Found existing vendor with code: {partner_code}")
                return vendor_ids[0]

        if partner_id and not pd.isna(partner_id):
            vendor_data = {
                'name': str(partner_id).strip(),
                'ref': str(partner_code).strip() if partner_code and not pd.isna(partner_code) else None,
                'company_type': 'company',
                'supplier_rank': 1,
                'customer_rank': 0,
            }
            
            new_vendor_id = models.execute_kw(
                db, uid, password, 'res.partner', 'create', [vendor_data]
            )
            print(f"Created new vendor from partner_id: {partner_id}" + (f" (Code: {partner_code})" if partner_code and not pd.isna(partner_code) else ""))
            return new_vendor_id
        
        return None
        
    except Exception as e:
        error_msg = str(e)
        print(f"Error in search_vendor: {error_msg}")
        log_error('N/A', 'N/A', 'N/A', f"Vendor Search Error: {error_msg}")
        return None

def search_product(product_value):
    """Search for product in Odoo"""
    if not isinstance(product_value, str):
        product_value = str(product_value)
    
    product_value = product_value.strip()
    
    try:
        product_ids = models.execute_kw(
            db, uid, password, 'product.product', 'search',
            [[['default_code', '=', product_value]]]
        )
        if product_ids:
            print(f"Found product with default_code: {product_value}")
            return product_ids

        product_ids = models.execute_kw(
            db, uid, password, 'product.product', 'search',
            [[['old_product_code', '=', product_value]]]
        )
        if product_ids:
            print(f"Found product with old_product_code: {product_value}")
            return product_ids
        
        print(f"Product not found: {product_value}")
        log_error('N/A', 'N/A', product_value, f"Product not found in system: {product_value}")
        return []
        
    except Exception as e:
        error_msg = str(e)
        print(f"Error searching product: {error_msg}")
        log_error('N/A', 'N/A', product_value, f"Product Search Error: {error_msg}")
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
        return False

def search_picking_type(picking_type_value):
    """Search for picking type in Odoo"""
    def get_default_picking_type():
        picking_type_ids = models.execute_kw(
            db, uid, password, 'stock.picking.type', 'search',
            [[['code', '=', 'incoming'], ['warehouse_id', '!=', False]]],
            {'limit': 1}
        )
        if picking_type_ids:
            print("Using default Purchase picking type")
            return picking_type_ids[0]
        return False

    if not picking_type_value or pd.isna(picking_type_value):
        return get_default_picking_type()

    picking_type_value = str(picking_type_value).strip()
    
    try:
        # First try to find by exact name match
        picking_type_ids = models.execute_kw(
            db, uid, password, 'stock.picking.type', 'search',
            [[['name', '=', picking_type_value], ['code', '=', 'incoming']]]
        )
        if picking_type_ids:
            print(f"Found picking type by exact name: {picking_type_value}")
            return picking_type_ids[0]

        # Try to find by partial name match
        picking_type_ids = models.execute_kw(
            db, uid, password, 'stock.picking.type', 'search',
            [[['name', 'ilike', picking_type_value], ['code', '=', 'incoming']]]
        )
        if picking_type_ids:
            print(f"Found picking type by partial name: {picking_type_value}")
            return picking_type_ids[0]

        # Try to find by warehouse name
        picking_type_ids = models.execute_kw(
            db, uid, password, 'stock.picking.type', 'search',
            [[['warehouse_id.name', 'ilike', picking_type_value], ['code', '=', 'incoming']]]
        )
        if picking_type_ids:
            print(f"Found picking type by warehouse name: {picking_type_value}")
            return picking_type_ids[0]

        # If no match found, get all picking types and print them for debugging
        all_picking_types = models.execute_kw(
            db, uid, password, 'stock.picking.type', 'search_read',
            [[['code', '=', 'incoming']]],
            {'fields': ['name', 'warehouse_id']}
        )
        print(f"\nAvailable picking types:")
        for pt in all_picking_types:
            print(f"- {pt['name']} (ID: {pt['id']})")

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
        po_ids = models.execute_kw(
            db, uid, password, 'purchase.order', 'search',
            [[['name', '=', po_name]]]
        )

        if po_ids:
            print(f"Updating existing PO: {po_name}")
            po_id = po_ids[0]
            
            existing_lines = models.execute_kw(
                db, uid, password, 'purchase.order.line', 'search_read',
                [[['order_id', '=', po_id]]],
                {'fields': ['id', 'product_id', 'product_qty', 'price_unit', 'taxes_id']}
            )
            
            models.execute_kw(
                db, uid, password, 'purchase.order', 'write',
                [po_id, {
                    'partner_id': po_data['partner_id'],
                    'date_order': po_data['date_order'],
                    'date_planned': po_data['date_planned'],
                    'picking_type_id': po_data['picking_type_id'],
                    'notes': po_data['notes'],
                }]
            )
            
            if existing_lines:
                existing_line_ids = [line['id'] for line in existing_lines]
                models.execute_kw(
                    db, uid, password, 'purchase.order.line', 'unlink',
                    [existing_line_ids]
                )

            for line in po_data['order_line']:
                line[2]['order_id'] = po_id
                new_line_id = models.execute_kw(
                    db, uid, password, 'purchase.order.line', 'create',
                    [line[2]]
                )
            
            print(f"Successfully updated PO: {po_name}")
            return True
        else:
            print(f"Creating new PO: {po_name}")
            po_id = models.execute_kw(
                db, uid, password, 'purchase.order', 'create',
                [po_data]
            )
            print(f"Successfully created PO: {po_name}")
            return True
    except Exception as e:
        error_msg = str(e)
        print(f"Error creating/updating PO: {error_msg}")
        log_error(po_data.get('name', 'N/A'), 'N/A', 'N/A', f"PO Creation/Update Error: {error_msg}")
        return False

def process_po_batch(batch_df, batch_num, total_batches):
    """Process a batch of purchase orders"""
    print(f"\nProcessing batch {batch_num}/{total_batches} ({len(batch_df)} rows)")
    
    success_count = 0
    error_count = 0
    
    # Group by PO number within the batch
    for po_name, po_group in batch_df.groupby('name'):
        try:
            print(f"\nProcessing PO: {po_name}")
            
            # Get first row for PO header data
            first_row = po_group.iloc[0]
            
            # Find vendor
            vendor_id = search_vendor(
                partner_name=first_row.get('partner_name') if 'partner_name' in first_row else None,
                partner_code=first_row['partner_code'] if 'partner_code' in first_row and pd.notna(first_row['partner_code']) else None,
                partner_id=first_row['partner_id'] if 'partner_id' in first_row and pd.notna(first_row['partner_id']) else None
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
                
            # Prepare PO data
            po_data = {
                'name': po_name,
                'partner_id': vendor_id,
                'date_order': convert_date(first_row['date_order']),  # Always provide a date
                'date_planned': convert_date(first_row['date_planned']),  # Always provide a date
                'picking_type_id': picking_type_id,
                'order_line': []
            }
            
            # Process PO lines
            all_products_found = True
            po_lines = []
            
            # First check if all products exist
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
                
                try:
                    quantity_str = str(line['product_qty']).strip()
                    quantity_str = ''.join(c for c in quantity_str if c.isdigit() or c == '.')
                    quantity = float(quantity_str) if quantity_str else 0.0
                    
                    if quantity <= 0:
                        print(f"Warning: Zero or negative quantity ({quantity}) for product {line['old_product_code']}")
                except (ValueError, TypeError, AttributeError) as e:
                    print(f"Error converting quantity value: {line['product_qty']} for product {line['old_product_code']}")
                    quantity = 0.0
                
                line_data = {
                    'product_id': product_ids[0],
                    'name': str(line['description']) if 'description' in line and pd.notna(line['description']) else line['old_product_code'],
                    'product_qty': quantity,
                    'price_unit': float(line['price_unit']) if pd.notna(line['price_unit']) else 0.0,
                    'date_planned': convert_date(line['date_planned']) if pd.notna(line['date_planned']) else False,
                }
                
                po_lines.append((0, 0, line_data))
            
            if all_products_found:
                po_data['order_line'] = po_lines
                if create_or_update_po(po_data):
                    success_count += 1
                else:
                    error_count += 1
            else:
                error_count += 1
                print(f"Skipping PO {po_name} due to missing products")
                
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
        excel_file = 'Data_file/import_OB.xlsx'
        df = pd.read_excel(excel_file)
        print(f"\nOriginal Excel columns: {df.columns.tolist()}")
        print(f"\nExcel file '{excel_file}' read successfully. Number of rows = {len(df)}")
        
        # Map Excel columns to Odoo fields
        column_mapping = {
            'name': 'name',
            'date_order': 'date_order',
            'partner_code': 'partner_code',
            'partner_id': 'partner_id',
            'date_planned': 'date_planned',
            'old_product_code': 'old_product_code',
            'default_code': 'default_code',
            'product_id': 'product_id',
            'price_unit': 'price_unit',
            'product_qty': 'product_qty',
            'picking_type_id': 'picking_type_id'
        }
        
        # Rename columns based on mapping
        df = df.rename(columns=column_mapping)
        print(f"\nExcel columns after mapping: {df.columns.tolist()}")
        
        # Print first few rows for verification
        print("\nFirst few rows:")
        print(df[['name', 'partner_code', 'old_product_code', 'product_qty', 'default_code']].head())
        
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
    