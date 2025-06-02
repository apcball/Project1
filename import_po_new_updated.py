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

class Logger:
    def __init__(self):
        self.failed_imports = []
        self.error_messages = []
        self.missing_products = []
        self.po_errors = []
        self.timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        # Create logs directory if it doesn't exist
        if not os.path.exists('logs'):
            os.makedirs('logs')
            
        # Initialize file paths
        self.missing_products_file = f'logs/missing_products_{self.timestamp}.xlsx'
        self.po_errors_file = f'logs/po_errors_{self.timestamp}.xlsx'

    def log_error(self, po_name, line_number, product_code, error_message, error_type='error'):
        """Log PO error"""
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        # Don't log if it's a locked PO
        if "PO is locked" in error_message:
            print(f"Info: Skipping log for locked PO {po_name}")
            return

        # Store error information
        error_data = {
            'Date Time': timestamp,
            'PO Number': po_name,
            'Line Number': line_number,
            'Product Code': product_code,
            'Error Type': error_type,
            'Error Message': error_message
        }
        self.po_errors.append(error_data)
        self.error_messages.append(f"Error in PO {po_name}, Line {line_number}: {error_message}")

    def log_missing_product(self, po_name, line_number, default_code, product_id=None):
        """Log missing product"""
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        # Store missing product information
        product_data = {
            'Date Time': timestamp,
            'PO Number': po_name,
            'Line Number': line_number,
            'Default Code': default_code,
            'Product ID': product_id if product_id else 'N/A'
        }
        self.missing_products.append(product_data)

    def save_logs(self):
        """Save all logs to Excel files"""
        try:
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

            # Save PO errors log
            if self.po_errors:
                df_errors = pd.DataFrame(self.po_errors)
                with pd.ExcelWriter(self.po_errors_file, engine='xlsxwriter') as writer:
                    df_errors.to_excel(writer, sheet_name='PO Errors', index=False)
                    
                    # Format the worksheet
                    workbook = writer.book
                    worksheet = writer.sheets['PO Errors']
                    
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
                
                print(f"\nPO errors log saved to: {self.po_errors_file}")
                
                # Print error summary
                print("\nError Summary:")
                error_summary = df_errors['Error Type'].value_counts()
                for error_type, count in error_summary.items():
                    print(f"{error_type}: {count} occurrences")
                    
        except Exception as e:
            print(f"Warning: Error saving log files: {e}")



# Create global logger instance
logger = Logger()

def save_error_log():
    """Save all logs to files"""
    logger.save_logs()

def main():
    total_success = 0
    total_errors = 0
    
    try:
        # Read Excel file
        excel_file = 'Data_file/import_PO_05.xlsx'
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
            'product_id': 'product_id',
            'price_unit': 'price_unit',
            'product_qty': 'product_qty',
            'picking_type_id': 'picking_type_id',
            'texs_id': 'texs_id',
            'notes': 'notes',
            'description': 'description',
        }
        
        # Rename columns based on mapping
        df = df.rename(columns=column_mapping)
        print(f"\nExcel columns after mapping: {df.columns.tolist()}")
        
        # Process each PO individually
        for po_name, po_group in df.groupby('name'):
            print(f"\n{'='*50}")
            print(f"Processing PO: {po_name}")
            print(f"{'='*50}")
            
            if process_single_po(po_group):
                total_success += 1
                print(f"✓ Successfully processed PO: {po_name}")
            else:
                total_errors += 1
                print(f"✗ Failed to process PO: {po_name}")
            
            # Optional: Add a small delay between POs to prevent overloading
            time.sleep(0.5)
        
        # Print final summary
        print("\n" + "="*50)
        print("Final Import Summary:")
        print("="*50)
        print(f"Total Successful POs: {total_success}")
        print(f"Total Failed POs: {total_errors}")
        print(f"Total Processed: {total_success + total_errors}")
        
    except Exception as e:
        print(f"Error in main function: {e}")
        logger.log_error('N/A', 'N/A', 'N/A', f"Main Function Error: {str(e)}", 'system_error')
    
    finally:
        # Save and close the log file
        save_error_log()
        print("\nImport process completed.")

# --- Connection Settings ---
url = 'http://mogth.work:8069'
db = 'MOG_LIVE'
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
        logger.log_error('N/A', 'N/A', 'N/A', f"Vendor Search Error: {error_msg}", 'vendor_error')
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
        return []
        
    except Exception as e:
        error_msg = str(e)
        print(f"Error searching product: {error_msg}")
        logger.log_error('N/A', 'N/A', product_value, f"Product Search Error: {error_msg}", 'system_error')
        return []

def convert_date(pd_timestamp):
    """Convert pandas timestamp to string"""
    if pd.notnull(pd_timestamp):
        if isinstance(pd_timestamp, str):
            return pd_timestamp
        return pd_timestamp.strftime('%Y-%m-%d %H:%M:%S')
    return False

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
    """Search for picking type in Odoo with enhanced search capabilities"""
    def get_default_picking_type():
        # First try to find a picking type with 'Purchase' in the name
        picking_type_ids = models.execute_kw(
            db, uid, password, 'stock.picking.type', 'search',
            [[
                '|',
                ('name', 'ilike', 'Purchase'),
                ('name', 'ilike', 'Receipts'),
                ('code', '=', 'incoming'),
                ('warehouse_id', '!=', False)
            ]],
            {'limit': 1}
        )
        
        # If not found, try any incoming type
        if not picking_type_ids:
            picking_type_ids = models.execute_kw(
                db, uid, password, 'stock.picking.type', 'search',
                [[
                    ('code', '=', 'incoming'),
                    ('warehouse_id', '!=', False)
                ]],
                {'limit': 1}
            )
        if picking_type_ids:
            picking_type = models.execute_kw(
                db, uid, password, 'stock.picking.type', 'read',
                [picking_type_ids[0]], {'fields': ['name', 'code']}
            )[0]
            print(f"Using default picking type: {picking_type['name']} (code: {picking_type['code']})")
            return picking_type_ids[0]
        return False

    if not picking_type_value or pd.isna(picking_type_value):
        return get_default_picking_type()

    picking_type_value = str(picking_type_value).strip()
    
    try:
        # First try exact ID match if numeric
        if str(picking_type_value).isdigit():
            picking_type_ids = models.execute_kw(
                db, uid, password, 'stock.picking.type', 'search',
                [[['id', '=', int(picking_type_value)]]]
            )
            if picking_type_ids:
                print(f"Found picking type by ID: {picking_type_value}")
                return picking_type_ids[0]

        # Try multiple search conditions at once
        picking_type_ids = models.execute_kw(
            db, uid, password, 'stock.picking.type', 'search',
            [[
                '|', '|', '|', '|',
                ('name', '=', picking_type_value),
                ('name', 'ilike', picking_type_value),
                ('code', '=', picking_type_value),
                ('code', 'ilike', picking_type_value),
                ('warehouse_id.name', 'ilike', picking_type_value)
            ]]
        )
        
        if picking_type_ids:
            picking_type = models.execute_kw(
                db, uid, password, 'stock.picking.type', 'read',
                [picking_type_ids[0]], {'fields': ['name', 'code', 'warehouse_id']}
            )[0]
            print(f"Found picking type: {picking_type['name']} (code: {picking_type['code']})")
            return picking_type_ids[0]

        # If still not found, try splitting the value and search for parts
        parts = picking_type_value.split()
        if len(parts) > 1:
            for part in parts:
                if len(part) > 3:  # Only search for parts longer than 3 characters
                    picking_type_ids = models.execute_kw(
                        db, uid, password, 'stock.picking.type', 'search',
                        [[
                            '|', '|',
                            ('name', 'ilike', part),
                            ('code', 'ilike', part),
                            ('warehouse_id.name', 'ilike', part)
                        ]]
                    )
                    if picking_type_ids:
                        picking_type = models.execute_kw(
                            db, uid, password, 'stock.picking.type', 'read',
                            [picking_type_ids[0]], {'fields': ['name', 'code']}
                        )[0]
                        print(f"Found picking type by partial match: {picking_type['name']} (code: {picking_type['code']})")
                        return picking_type_ids[0]

        # If still not found, get default
        default_id = get_default_picking_type()
        if default_id:
            print(f"Using default picking type as '{picking_type_value}' was not found")
            return default_id
            
        return False
        
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
            po_id = po_ids[0]
            
            # Check PO state before updating
            po_state = models.execute_kw(
                db, uid, password, 'purchase.order', 'read',
                [po_id], {'fields': ['state']}
            )[0]['state']
            
            # Skip if PO is locked (done, purchase, or cancel state)
            if po_state in ['done', 'purchase']:
                print(f"Skipping locked PO {po_name} (state: {po_state})")
                logger.log_error(po_name, 'N/A', 'N/A', f"PO is locked (state: {po_state}) - Update skipped", 'locked_po')
                return False
                
            print(f"Adding lines to existing PO: {po_name}")
            
            # First cancel the PO if it's not in draft state
            if po_state != 'draft':
                try:
                    models.execute_kw(
                        db, uid, password, 'purchase.order', 'button_cancel',
                        [po_id]
                    )
                    print(f"Reset PO {po_name} to draft state")
                except Exception as e:
                    print(f"Error resetting PO state: {str(e)}")
                    logger.log_error(po_name, 'N/A', 'N/A', f"Error resetting PO state: {str(e)}", 'state_reset_error')
                    return False
            
            # Set state to draft for the update
            po_data['state'] = 'draft'

            # Get existing lines with their products
            existing_product_lines = models.execute_kw(
                db, uid, password, 'purchase.order.line', 'search_read',
                [[['order_id', '=', po_id]]],
                {'fields': ['id', 'product_id']}
            )

            # Create a set of existing product IDs
            existing_product_ids = {line['product_id'][0] for line in existing_product_lines}

            # Filter out lines that already exist in the PO
            new_lines = []
            for line in po_data['order_line']:
                product_id = line[2]['product_id']
                if product_id not in existing_product_ids:
                    line[2]['order_id'] = po_id
                    new_lines.append(line[2])
                    print(f"Adding new line for product ID: {product_id}")
                else:
                    print(f"Skipping existing product ID: {product_id}")

            # Create new lines (only for products that don't exist in the PO)
            for line_data in new_lines:
                try:
                    new_line_id = models.execute_kw(
                        db, uid, password, 'purchase.order.line', 'create',
                        [line_data]
                    )
                    print(f"Created new line for product ID: {line_data['product_id']}")
                except Exception as e:
                    print(f"Error creating line: {str(e)}")
                    logger.log_error(po_name, 'N/A', str(line_data['product_id']), 
                                   f"Error creating line: {str(e)}", 'line_creation_error')
                    continue  # Continue with other lines even if one fails
            
            print(f"Successfully added lines to PO: {po_name}")
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
                print(f"Error creating PO: {str(e)}")
                logger.log_error(po_name, 'N/A', 'N/A', f"Error creating PO: {str(e)}", 'po_creation_error')
                return False
    except Exception as e:
        error_msg = str(e)
        print(f"Error creating/updating PO: {error_msg}")
        logger.log_error(po_data.get('name', 'N/A'), 'N/A', 'N/A', f"PO Creation/Update Error: {error_msg}", 'system_error')
        return False

def process_single_po(po_group):
    """Process a single purchase order"""
    success = False
    po_name = po_group.iloc[0]['name']
    
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
            logger.log_error(po_name, 'N/A', 'N/A', "Vendor not found or could not be created", 'vendor_error')
            return False
        
        # Get picking type
        picking_type_id = search_picking_type(first_row['picking_type_id'] if pd.notna(first_row.get('picking_type_id')) else None)
        if not picking_type_id:
            logger.log_error(po_name, 'N/A', 'N/A', "Could not find or create picking type", 'picking_type_error')
            return False
            
        # Prepare PO data
        po_data = {
            'name': po_name,
            'partner_id': vendor_id,
            'date_order': convert_date(first_row['date_order']) if pd.notna(first_row['date_order']) else datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'date_planned': convert_date(first_row['date_planned']) if pd.notna(first_row['date_planned']) else datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'picking_type_id': picking_type_id,
            'notes': str(first_row['notes']) if pd.notna(first_row['notes']) else '',
            'state': 'draft',
            'order_line': []
        }
        
        # Process PO lines
        all_products_found = True
        po_lines = []
        
        # Process each line
        for idx, line in po_group.iterrows():
            product_ids = search_product(line['old_product_code'])
            if not product_ids:
                all_products_found = False
                logger.log_missing_product(po_name, idx, line['old_product_code'])
                continue
            
            try:
                quantity_str = str(line['product_qty']).strip()
                quantity_str = ''.join(c for c in quantity_str if c.isdigit() or c == '.')
                quantity = float(quantity_str) if quantity_str else 0.0
                
                if quantity <= 0:
                    print(f"Warning: Zero or negative quantity ({quantity}) for product {line['old_product_code']}")
                    logger.log_error(po_name, idx, line['old_product_code'], 
                             f"Invalid quantity: {quantity}", 'quantity_error')
                    continue
            except (ValueError, TypeError, AttributeError) as e:
                print(f"Error converting quantity value: {line['product_qty']} for product {line['old_product_code']}")
                logger.log_error(po_name, idx, line['old_product_code'], 
                         f"Invalid quantity format: {line['product_qty']}", 'quantity_error')
                continue
            
            line_data = {
                'product_id': product_ids[0],
                'name': f"{str(line['description']) if pd.notna(line.get('description')) else line['old_product_code']} - Product ID: {line['product_id']}" if pd.notna(line.get('product_id')) else str(line['description']) if pd.notna(line.get('description')) else line['old_product_code'],
                'product_qty': quantity,
                'price_unit': float(line['price_unit']) if pd.notna(line['price_unit']) else 0.0,
                'date_planned': convert_date(line['date_planned']) if pd.notna(line['date_planned']) else datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            }
            
            tax_id = get_tax_id(line['texs_id']) if pd.notna(line['texs_id']) else False
            if tax_id:
                line_data['taxes_id'] = [(6, 0, [tax_id])]
                print(f"Adding tax {tax_id} to line")
            
            po_lines.append((0, 0, line_data))
        
        if po_lines:  # Only proceed if we have valid lines
            po_data['order_line'] = po_lines
            success = create_or_update_po(po_data)
            if success:
                print(f"Successfully processed PO: {po_name}")
            else:
                logger.log_error(po_name, 'N/A', 'N/A', "Failed to create/update PO", 'po_creation_error')
        else:
            print(f"No valid lines found for PO {po_name}")
            logger.log_error(po_name, 'N/A', 'N/A', "No valid lines to process", 'no_valid_lines')
            
    except Exception as e:
        print(f"Error processing PO {po_name}: {str(e)}")
        logger.log_error(po_name, 'N/A', 'N/A', f"Processing Error: {str(e)}", 'system_error')
        success = False
    
    return success

def main():
    total_success = 0
    total_errors = 0
    
    try:
        # Read Excel file
        excel_file = 'Data_file/import_PO_04.xlsx'
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
            'product_id': 'product_id',
            'price_unit': 'price_unit',
            'product_qty': 'product_qty',
            'picking_type_id': 'picking_type_id',
            'texs_id': 'texs_id',
            'notes': 'notes',
            'description': 'description',
        }
        
        # Rename columns based on mapping
        df = df.rename(columns=column_mapping)
        print(f"\nExcel columns after mapping: {df.columns.tolist()}")
        
        # Process each PO individually
        for po_name, po_group in df.groupby('name'):
            print(f"\n{'='*50}")
            print(f"Processing PO: {po_name}")
            print(f"{'='*50}")
            
            if process_single_po(po_group):
                total_success += 1
                print(f"✓ Successfully processed PO: {po_name}")
            else:
                total_errors += 1
                print(f"✗ Failed to process PO: {po_name}")
            
            # Optional: Add a small delay between POs to prevent overloading
            time.sleep(0.5)
        
        # Print final summary
        print("\n" + "="*50)
        print("Final Import Summary:")
        print("="*50)
        print(f"Total Successful POs: {total_success}")
        print(f"Total Failed POs: {total_errors}")
        print(f"Total Processed: {total_success + total_errors}")
        
    except Exception as e:
        print(f"Error in main function: {e}")
        logger.log_error('N/A', 'N/A', 'N/A', f"Main Function Error: {str(e)}", 'system_error')
    
    finally:
        # Save and close the log file
        save_error_log()
        print("\nImport process completed.")

if __name__ == "__main__":
    main()