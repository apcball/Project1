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
    หากไม่พบสินค้า จะ return [] และบันทึกในlog
    """
    if not isinstance(product_value, str):
        product_value = str(product_value)
    
    product_value = product_value.strip()
    
    try:
        # 1. ค้นหาด้วย default_code
        product_ids = models.execute_kw(
            db, uid, password, 'product.product', 'search',
            [[['default_code', '=', product_value]]]
        )
        if product_ids:
            print(f"Found product with default_code: {product_value}")
            return product_ids

        # 2. ค้นหาจาก old_product_code
        product_ids = models.execute_kw(
            db, uid, password, 'product.product', 'search',
            [[['old_product_code', '=', product_value]]]
        )
        if product_ids:
            print(f"Found product with old_product_code: {product_value}")
            return product_ids
        
        # If product not found, log it and return empty list
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
            return pd_timestamp
        return pd_timestamp.strftime('%Y-%m-%d %H:%M:%S')
    return False

def search_picking_type(picking_type_value):
    """
    Search for a picking type in Odoo using multiple search strategies:
    1. Search by ID if numeric
    2. Search by exact name match
    3. Search by code
    4. Search by warehouse name
    5. Search by Thai keywords
    6. If not found, return default Purchase picking type
    """
    def get_default_picking_type():
        """Get default incoming picking type"""
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
        # 1. Try to find by ID if the value is numeric
        if str(picking_type_value).isdigit():
            picking_type_ids = models.execute_kw(
                db, uid, password, 'stock.picking.type', 'search',
                [[['id', '=', int(picking_type_value)]]]
            )
            if picking_type_ids:
                print(f"Found picking type by ID: {picking_type_value}")
                return picking_type_ids[0]

        # 2. Try to find by exact name match
        picking_type_ids = models.execute_kw(
            db, uid, password, 'stock.picking.type', 'search',
            [[['name', '=', picking_type_value]]]
        )
        if picking_type_ids:
            print(f"Found picking type by name: {picking_type_value}")
            return picking_type_ids[0]

        # 3. Try to find by code
        picking_type_ids = models.execute_kw(
            db, uid, password, 'stock.picking.type', 'search',
            [[['code', '=', picking_type_value]]]
        )
        if picking_type_ids:
            print(f"Found picking type by code: {picking_type_value}")
            return picking_type_ids[0]

        # 4. Try to find by warehouse name
        picking_type_ids = models.execute_kw(
            db, uid, password, 'stock.picking.type', 'search',
            [[['warehouse_id.name', 'ilike', picking_type_value]]]
        )
        if picking_type_ids:
            print(f"Found picking type by warehouse name: {picking_type_value}")
            return picking_type_ids[0]

        # 5. Try to find by common warehouse keywords in Thai
        thai_keywords = ['คลัง', 'วัตถุดิบ', 'สินค้า', 'ผลิต', 'สำเร็จรูป', 'รับเข้า', 'จ่ายออก']
        for keyword in thai_keywords:
            if keyword in picking_type_value:
                picking_type_ids = models.execute_kw(
                    db, uid, password, 'stock.picking.type', 'search',
                    [[['name', 'ilike', keyword]]]
                )
                if picking_type_ids:
                    print(f"Found picking type containing keyword '{keyword}': {picking_type_value}")
                    return picking_type_ids[0]

        # 6. If not found, return default picking type
        return get_default_picking_type()
        
    except Exception as e:
        error_msg = str(e)
        print(f"Error in search_picking_type: {error_msg}")
        return get_default_picking_type()

def get_tax_id(tax_value):
    """Get tax ID from value"""
    if not tax_value or pd.isna(tax_value):
        return False

    try:
        # Get all purchase taxes first
        all_taxes = models.execute_kw(
            db, uid, password, 'account.tax', 'search_read',
            [[['type_tax_use', 'in', ['purchase', 'all']], ['active', '=', True]]],
            {'fields': ['id', 'name', 'amount', 'type_tax_use']}
        )

        # Convert tax_value to float, handling percentage sign
        if isinstance(tax_value, str):
            tax_value = tax_value.strip()
            if tax_value.endswith('%'):
                tax_percentage = float(tax_value.rstrip('%'))
            else:
                tax_percentage = float(tax_value) * 100
        else:
            tax_percentage = float(tax_value) * 100

        # Search for purchase tax with this amount
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

def create_or_update_po(po_data):
    """
    Create or update a purchase order in Odoo
    """
    try:
        # Check if PO already exists
        po_name = po_data['name']
        po_ids = models.execute_kw(
            db, uid, password, 'purchase.order', 'search',
            [[['name', '=', po_name]]]
        )

        if po_ids:
            # Update existing PO
            print(f"Updating existing PO: {po_name}")
            po_id = po_ids[0]
            
            # Get existing PO lines
            existing_lines = models.execute_kw(
                db, uid, password, 'purchase.order.line', 'search_read',
                [[['order_id', '=', po_id]]],
                {'fields': ['id', 'product_id', 'product_qty', 'price_unit', 'taxes_id']}
            )
            print(f"Existing lines: {existing_lines}")
            
            # Update PO header
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
            
            # Update or create PO lines
            for line in po_data['order_line']:
                # Check if line exists
                matching_lines = [l for l in existing_lines if l['product_id'][0] == line[2]['product_id']]
                
                if matching_lines:
                    # Update existing line
                    line_id = matching_lines[0]['id']
                    print(f"Updating line {line_id} with data: {line[2]}")
                    models.execute_kw(
                        db, uid, password, 'purchase.order.line', 'write',
                        [line_id, line[2]]
                    )
                else:
                    # Create new line
                    line[2]['order_id'] = po_id
                    print(f"Creating new line with data: {line[2]}")
                    new_line_id = models.execute_kw(
                        db, uid, password, 'purchase.order.line', 'create',
                        [line[2]]
                    )
                    print(f"Created new line with ID: {new_line_id}")
            
            print(f"Successfully updated PO lines: {po_name}")
            return po_id
        else:
            # Create new PO
            print(f"Creating new PO: {po_name}")
            print(f"PO Data: {po_data}")
            po_id = models.execute_kw(
                db, uid, password, 'purchase.order', 'create',
                [po_data]
            )
            print(f"Successfully created PO: {po_name}")
            return po_id
    except Exception as e:
        error_msg = str(e)
        print(f"Error creating/updating PO: {error_msg}")
        log_error(po_data.get('name', 'N/A'), 'N/A', 'N/A', f"PO Creation/Update Error: {error_msg}")
        return False

def main():
    try:
        # Read Excel file
        excel_file = 'Data_file/import_PO.xlsx'
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
            'quantity': 'quantity',
            'picking_type_id': 'picking_type_id',
            'texs_id': 'texs_id',
            'notes': 'notes',
        }
        
        # Rename columns based on mapping
        df = df.rename(columns=column_mapping)
        print(f"\nExcel columns after mapping: {df.columns.tolist()}")
        
        # Print first few rows with notes for verification
        print("\nFirst few rows with notes:")
        print(df[['name', 'partner_code', 'old_product_code', 'product_qty', 'notes']].head())
        
        # Group by PO number
        for po_name, po_group in df.groupby('name'):
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
                log_error(po_name, 'N/A', 'N/A', "Vendor not found or could not be created")
                continue
            
            # Get picking type
            picking_type_id = search_picking_type(first_row['picking_type_id'] if pd.notna(first_row.get('picking_type_id')) else None)
            if not picking_type_id:
                log_error(po_name, 'N/A', 'N/A', "Could not find or create picking type")
                continue
                
            # Prepare PO data
            po_data = {
                'name': po_name,
                'partner_id': vendor_id,
                'date_order': convert_date(first_row['date_order']) if pd.notna(first_row['date_order']) else False,
                'date_planned': convert_date(first_row['date_planned']) if pd.notna(first_row['date_planned']) else False,
                'picking_type_id': picking_type_id,
                'notes': str(first_row['notes']) if pd.notna(first_row['notes']) else '',
                'order_line': []
            }
            
            # Process PO lines
            all_products_found = True
            po_lines = []
            
            # First check if all products exist
            for _, line in po_group.iterrows():
                product_ids = search_product(line['old_product_code'])
                if not product_ids:
                    all_products_found = False
                    log_error(po_name, line.name, line['old_product_code'], "Product not found - Skipping entire PO")
                    break
                
                # Prepare line data with taxes
                line_data = {
                    'product_id': product_ids[0],
                    'name': line['old_product_code'],
                    'product_qty': float(line['product_qty']) if pd.notna(line['product_qty']) else 0.0,
                    'price_unit': float(line['price_unit']) if pd.notna(line['price_unit']) else 0.0,
                    'date_planned': convert_date(line['date_planned']) if pd.notna(line['date_planned']) else False,
                }
                
                # Add taxes using command 6 (replace entire list)
                tax_id = get_tax_id(line['texs_id']) if pd.notna(line['texs_id']) else False
                if tax_id:
                    line_data['taxes_id'] = [(6, 0, [tax_id])]
                    print(f"Adding tax {tax_id} to line")
                
                po_lines.append((0, 0, line_data))
            
            # Only create/update PO if all products were found
            if all_products_found:
                po_data['order_line'] = po_lines
                create_or_update_po(po_data)
            else:
                print(f"Skipping PO {po_name} due to missing products")
        
        # Save error log if there were any errors
        save_error_log()
        
    except Exception as e:
        print(f"Error in main function: {e}")
        log_error('N/A', 'N/A', 'N/A', f"Main Function Error: {str(e)}")
        save_error_log()

if __name__ == "__main__":
    main()