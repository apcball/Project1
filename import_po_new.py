import xmlrpc.client
import pandas as pd
import sys
import re
from datetime import datetime
import csv
import os

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
    """
    ค้นหา vendor ตามลำดับ:
    1. ค้นหาจาก partner_name ในระบบ (ชื่อ vendor)
    2. ค้นหาจาก partner_code
    3. ถ้าไม่พบ สร้าง vendor ใหม่โดยใช้ชื่อจาก partner_id
    """
    try:
        # 1. ค้นหาจากชื่อ vendor (partner_name)
        if partner_name and not pd.isna(partner_name):
            vendor_ids = models.execute_kw(
                db, uid, password, 'res.partner', 'search',
                [[['name', '=', str(partner_name).strip()]]]
            )
            if vendor_ids:
                print(f"Found existing vendor with name: {partner_name}")
                return vendor_ids[0]

        # 2. ค้นหาจาก partner_code
        if partner_code and not pd.isna(partner_code):
            vendor_ids = models.execute_kw(
                db, uid, password, 'res.partner', 'search',
                [[['ref', '=', str(partner_code).strip()]]]
            )
            if vendor_ids:
                print(f"Found existing vendor with code: {partner_code}")
                return vendor_ids[0]

        # 3. สร้าง vendor ใหม่ถ้าไม่พบ โดยใช้ partner_id
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
    """
    ค้นหาผลิตภัณฑ์ใน Odoo โดย:
    1. ค้นหาจาก default_code (รหัสสินค้า)
    2. ค้นหาจาก old_product_code
    """
    if not isinstance(product_value, str):
        product_value = str(product_value)
    
    product_value = product_value.strip()
    
    # 1. ค้นหาด้วย default_code
    product_ids = models.execute_kw(
        db, uid, password, 'product.product', 'search',
        [[['default_code', '=', product_value]]]
    )
    if product_ids:
        return product_ids

    # 2. ค้นหาจาก old_product_code
    product_ids = models.execute_kw(
        db, uid, password, 'product.product', 'search',
        [[['old_product_code', '=', product_value]]]
    )
    if product_ids:
        return product_ids
    
    # If not found, create a new product
    try:
        product_data = {
            'name': f"Product {product_value}",
            'default_code': product_value,
            'type': 'product',
            'purchase_ok': True,
        }
        
        new_product_id = models.execute_kw(
            db, uid, password, 'product.product', 'create', [product_data]
        )
        print(f"Created new product with code: {product_value}")
        return [new_product_id]
    except Exception as e:
        error_msg = str(e)
        print(f"Error creating product: {error_msg}")
        log_error('N/A', 'N/A', product_value, f"Product Creation Error: {error_msg}")
        return []

def convert_date(pd_timestamp):
    """Convert pandas timestamp to string"""
    if pd.notnull(pd_timestamp):
        if isinstance(pd_timestamp, str):
            return pd_timestamp
        return pd_timestamp.strftime('%Y-%m-%d %H:%M:%S')
    return False

def search_picking_type(picking_type_value):
    """
    Search for a picking type in Odoo using the value from Excel
    """
    if not picking_type_value or pd.isna(picking_type_value):
        return False

    picking_type_value = str(picking_type_value).strip()
    
    try:
        # Try to find by ID if the value is numeric
        if str(picking_type_value).isdigit():
            picking_type_ids = models.execute_kw(
                db, uid, password, 'stock.picking.type', 'search',
                [[['id', '=', int(picking_type_value)]]]
            )
            if picking_type_ids:
                print(f"Found picking type by ID: {picking_type_value}")
                return picking_type_ids[0]

        # Try to find by name
        picking_type_ids = models.execute_kw(
            db, uid, password, 'stock.picking.type', 'search',
            [[['name', '=', picking_type_value]]]
        )
        if picking_type_ids:
            print(f"Found picking type by name: {picking_type_value}")
            return picking_type_ids[0]

        print(f"Picking type not found: {picking_type_value}")
        return False
    except Exception as e:
        error_msg = str(e)
        print(f"Error searching picking type: {error_msg}")
        return False

def check_existing_po(po_name):
    """Check if PO exists and return its ID"""
    try:
        po_ids = models.execute_kw(
            db, uid, password, 'purchase.order', 'search',
            [[['name', '=', po_name]]]
        )
        return po_ids[0] if po_ids else None
    except Exception as e:
        error_msg = str(e)
        print(f"Error checking existing PO: {error_msg}")
        log_error(po_name, 'N/A', 'N/A', f"PO Check Error: {error_msg}")
        return None

def update_po_lines(po_id, lines, po_name):
    """Update PO lines for existing PO"""
    try:
        # Get existing PO lines
        existing_lines = models.execute_kw(
            db, uid, password, 'purchase.order.line', 'search_read',
            [[['order_id', '=', po_id]]],
            {'fields': ['product_id', 'product_qty', 'price_unit']}
        )
        
        # Search for 7% VAT tax
        tax_ids = models.execute_kw(
            db, uid, password, 'account.tax',
            'search',
            [[['type_tax_use', '=', 'purchase'], ['amount', '=', 7.0]]]
        )
        
        # Update existing lines and create new ones
        for line in lines:
            product_ids = search_product(line['product_code'])
            if not product_ids:
                error_msg = f"Product not found: {line['product_code']}"
                log_error(po_name, 'N/A', line['product_code'], error_msg)
                error_products.append(line['product_code'])
                continue
                
            product_id = product_ids[0]
            
            # Check if line exists for this product
            existing_line = next(
                (l for l in existing_lines if l['product_id'][0] == product_id),
                None
            )
            
            line_vals = {
                'product_id': product_id,
                'product_qty': line['product_qty'],
                'price_unit': line['price_unit'],
                'order_id': po_id,
                'taxes_id': [(6, 0, tax_ids)] if tax_ids else False,
            }
            
            try:
                if existing_line:
                    # Update existing line
                    models.execute_kw(
                        db, uid, password, 'purchase.order.line', 'write',
                        [[existing_line['id']], line_vals]
                    )
                else:
                    # Create new line
                    models.execute_kw(
                        db, uid, password, 'purchase.order.line', 'create',
                        [line_vals]
                    )
            except Exception as e:
                error_msg = str(e)
                log_error(po_name, 'N/A', line['product_code'], f"Line Creation Error: {error_msg}")
                continue
                
        return True
    except Exception as e:
        error_msg = str(e)
        print(f"Error updating PO lines: {error_msg}")
        log_error(po_name, 'N/A', 'N/A', f"Line Update Error: {error_msg}")
        return False

# --- Read Excel File ---
excel_file = 'Data_file/import_PO1.xlsx'
try:
    # Read Excel file
    df = pd.read_excel(excel_file)
    
    # Get the column names from the first row
    columns = df.columns.tolist()
    print("Original Excel columns:", columns)
    
    # Create mapping based on position
    column_mapping = {
        'Document No.': 'name',  # PO Number
        'Document Date': 'date_order',  # Order Date
        'Vendor Code': 'partner_code',  # Vendor Code
        'Vendor Name': 'partner_id',  # Vendor Name
        'Due Date': 'date_planned',  # Planned Date
        'Item No.': 'old_product_code',  # Product Code
        'Description': 'product_id',  # Product Name
        'Unit Price': 'price_unit',  # Unit Price
        'Quantity': 'product_qty',  # Quantity
        'Operation Type': 'picking_type_id',  # Picking Type
        'VAT': 'tax_id',  # VAT
        'Remark': 'notes'  # Notes
    }
    
    # Rename the columns
    df = df.rename(columns=column_mapping)
    
    # Remove any rows where all values are NaN
    df = df.dropna(how='all')
    
    # Reset the index after dropping rows
    df = df.reset_index(drop=True)
    
    print(f"\nExcel file '{excel_file}' read successfully. Number of rows = {len(df)}")
    print("Excel columns after mapping:", df.columns.tolist())
    print("\nFirst few rows with notes:")
    print(df[['name', 'partner_code', 'old_product_code', 'product_qty', 'notes']].head())
    
except Exception as e:
    print("Failed to read Excel file:", e)
    sys.exit(1)

# --- First, get first occurrence of each PO for header data ---
po_headers = {}
for index, row in df.iterrows():
    try:
        if pd.isna(row['name']):
            continue
            
        po_name = str(row['name']).strip()
        if po_name not in po_headers:
            # Get notes from the first row of this PO
            notes = str(row['notes']) if pd.notnull(row['notes']) else ''
            notes = notes.split('\n')[0].strip() if notes else ''
            
            # Get picking type from the first row of this PO
            picking_type = str(row['picking_type_id']).strip() if pd.notnull(row['picking_type_id']) else None
            picking_type_id = search_picking_type(picking_type) if picking_type else False
            
            po_headers[po_name] = {
                'vendor_name': str(row['partner_id']).strip() if pd.notnull(row['partner_id']) else None,
                'vendor_code': str(row['partner_code']).strip() if pd.notnull(row['partner_code']) else None,
                'order_date': convert_date(row['date_order']),
                'planned_date': convert_date(row['date_planned']),
                'first_row_index': index,
                'notes': notes,
                'picking_type_id': picking_type_id
            }
    except Exception as e:
        error_msg = str(e)
        print(f"Error processing header row: {error_msg}")
        log_error(row['name'] if pd.notnull(row['name']) else 'Unknown PO',
                 index + 1,
                 row['old_product_code'] if pd.notnull(row['old_product_code']) else 'N/A',
                 f"Header Processing Error: {error_msg}")
        continue

# --- Then collect all lines for each PO ---
po_groups = {}
for index, row in df.iterrows():
    try:
        if pd.isna(row['name']) or pd.isna(row['old_product_code']):
            continue
            
        po_name = str(row['name']).strip()
        if po_name not in po_groups:
            # Initialize lines array for this PO
            po_groups[po_name] = []
        
        # Add line items
        product_code = str(row['old_product_code']).strip()
        product_qty = float(row['product_qty']) if pd.notnull(row['product_qty']) else 0.0
        price_unit = float(row['price_unit']) if pd.notnull(row['price_unit']) else 0.0
        
        # Only add line if product code and quantity are valid
        if product_code and product_qty > 0:
            po_groups[po_name].append({
                'product_code': product_code,
                'product_qty': product_qty,
                'price_unit': price_unit,
            })
    except Exception as e:
        error_msg = str(e)
        print(f"Error processing line row: {error_msg}")
        log_error(row['name'] if pd.notnull(row['name']) else 'Unknown PO',
                 index + 1,
                 row['old_product_code'] if pd.notnull(row['old_product_code']) else 'N/A',
                 f"Line Processing Error: {error_msg}")
        continue

# Process POs using header data and collected lines
for po_name in po_headers.keys():
    try:
        print(f"\nProcessing PO: {po_name}")
        header_data = po_headers[po_name]
        lines_data = po_groups.get(po_name, [])
        
        # Skip if no lines
        if not lines_data:
            error_msg = "No valid lines found"
            print(f"Skipping PO {po_name} - {error_msg}")
            log_error(po_name, 'N/A', 'N/A', error_msg)
            continue
            
        # Check if PO already exists
        existing_po_id = check_existing_po(po_name)
        
        if existing_po_id:
            print(f"Updating existing PO: {po_name}")
            # Update existing PO lines
            if update_po_lines(existing_po_id, lines_data, po_name):
                print(f"Successfully updated PO lines: {po_name}")
            else:
                error_msg = "Failed to update PO lines"
                print(f"{error_msg}: {po_name}")
                log_error(po_name, 'N/A', 'N/A', error_msg)
        else:
            # Create new PO
            vendor_id = search_vendor(
                partner_name=header_data['vendor_name'],
                partner_code=header_data['vendor_code'],
                partner_id=header_data['vendor_name']
            )
            if not vendor_id:
                error_msg = "Could not find or create vendor"
                print(f"{error_msg} for PO: {po_name}")
                log_error(po_name, 'N/A', 'N/A', error_msg)
                continue
            
            po_vals = {
                'name': po_name,
                'partner_id': vendor_id,
                'date_order': header_data['order_date'],
                'date_planned': header_data['planned_date'],
                'state': 'draft',
                'notes': header_data.get('notes', ''),
            }

            # Add picking type if found
            if header_data.get('picking_type_id'):
                po_vals['picking_type_id'] = header_data['picking_type_id']
                print(f"Setting picking type ID: {header_data['picking_type_id']} for PO: {po_name}")
            
            try:
                print(f"Creating Purchase Order: {po_name}")
                new_po_id = models.execute_kw(
                    db, uid, password, 'purchase.order', 'create',
                    [po_vals]
                )
                print(f"Created PO with ID: {new_po_id}")
                
                # Create PO lines
                if update_po_lines(new_po_id, lines_data, po_name):
                    print(f"Successfully created PO lines for: {po_name}")
                else:
                    error_msg = "Failed to create PO lines"
                    print(f"{error_msg} for: {po_name}")
                    log_error(po_name, 'N/A', 'N/A', error_msg)
                    
            except Exception as e:
                error_msg = str(e)
                print(f"Error creating PO {po_name}: {error_msg}")
                log_error(po_name, 'N/A', 'N/A', f"PO Creation Error: {error_msg}")
                continue
                
    except Exception as e:
        error_msg = str(e)
        print(f"Error processing PO {po_name}: {error_msg}")
        log_error(po_name, 'N/A', 'N/A', f"PO Processing Error: {error_msg}")
        continue

# Save error log if there were any errors
save_error_log()