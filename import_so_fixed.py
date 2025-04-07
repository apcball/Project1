import xmlrpc.client
import pandas as pd
import sys
from datetime import datetime
import os

# Initialize lists to store error logs and missing products
error_logs = []
missing_products = []

def log_error(so_name, row_number, error_type, error_message, row_data=None):
    """บันทึก error log"""
    error_logs.append({
        'SO Number': so_name,
        'Row Number': row_number,
        'Error Type': error_type,
        'Error Message': error_message,
        'Row Data': str(row_data) if row_data is not None else ''
    })

def log_missing_product(product_id, product_name):
    """บันทึกรายการสินค้าที่ไม่พบในระบบ"""
    # Check if product is already in the list to avoid duplicates
    if not any(p['Product ID'] == product_id for p in missing_products):
        missing_products.append({
            'Product ID': product_id,
            'Product Name': product_name
        })

def export_error_logs():
    """Export error logs and missing products to Excel files"""
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    try:
        # Create logs directory if it doesn't exist
        log_dir = 'logs'
        if not os.path.exists(log_dir):
            os.makedirs(log_dir)
        
        # Export error logs
        if error_logs:
            error_log_file = os.path.join(log_dir, f'import_errors_{timestamp}.xlsx')
            df_errors = pd.DataFrame(error_logs)
            df_errors.to_excel(error_log_file, index=False)
            print(f"\nError log exported to: {error_log_file}")
            print(f"Total errors logged: {len(error_logs)}")
        
        # Export missing products
        if missing_products:
            missing_products_file = os.path.join(log_dir, f'missing_products_{timestamp}.xlsx')
            df_missing = pd.DataFrame(missing_products)
            df_missing.to_excel(missing_products_file, index=False)
            print(f"\nMissing products exported to: {missing_products_file}")
            print(f"Total missing products: {len(missing_products)}")
            
    except Exception as e:
        print(f"Failed to export logs: {e}")

# --- ตั้งค่าการเชื่อมต่อ Odoo ---
url = 'http://mogth.work:8069'
db = 'MOG_LIVE'
username = 'apichart@mogen.co.th'
password = '471109538'

# --- Authenticate ---
try:
    print(f"Connecting to {url}...")
    common = xmlrpc.client.ServerProxy(f'{url}/xmlrpc/2/common')
    
    # Try authentication
    print(f"Authenticating user {username} on database {db}...")
    uid = common.authenticate(db, username, password, {})
    if not uid:
        print("Authentication failed: invalid credentials or insufficient permissions.")
        sys.exit(1)
    
    # Get server version to verify connection
    server_version = common.version()
    print(f"Connected to Odoo server version {server_version.get('server_version', 'unknown')}")
    print(f"Authentication successful, uid = {uid}")

except ConnectionRefusedError:
    print(f"Error: Could not connect to server at {url}. Please verify the server is running and accessible.")
    sys.exit(1)
except xmlrpc.client.Fault as e:
    if "database" in str(e).lower():
        print(f"Database error: The database '{db}' might not exist or is not accessible.")
    else:
        print(f"XMLRPC Error: {str(e)}")
    sys.exit(1)
except Exception as e:
    print("Error during connection/authentication:", str(e))
    sys.exit(1)

# --- สร้าง models proxy ---
try:
    models = xmlrpc.client.ServerProxy(f'{url}/xmlrpc/2/object')
except Exception as e:
    print("Error creating XML-RPC models proxy:", e)
    sys.exit(1)

def format_date(date_str):
    """แปลงรูปแบบวันที่ให้ตรงกับ Odoo format"""
    if pd.isna(date_str):
        return False
    try:
        if isinstance(date_str, datetime):
            return date_str.strftime('%Y-%m-%d')
        elif isinstance(date_str, str):
            try:
                return datetime.strptime(date_str, '%Y-%m-%d').strftime('%Y-%m-%d')
            except ValueError:
                return False
        return False
    except Exception:
        return False



def get_partner_data(partner_name):
    """ค้นหาข้อมูลลูกค้าจากชื่อ"""
    if pd.isna(partner_name):
        return None
    
    try:
        partner_name = str(partner_name).strip()
        # Search for partner by name
        partner_ids = models.execute_kw(
            db, uid, password, 'res.partner', 'search',
            [[['name', '=', partner_name]]]
        )
        
        if partner_ids:
            partner_data = models.execute_kw(
                db, uid, password, 'res.partner', 'read',
                [partner_ids[0]], 
                {'fields': ['id', 'name']}
            )[0]
            
            return partner_data
            
        # If not found, try creating new partner
        partner_vals = {
            'name': partner_name,
            'company_type': 'company',
            'is_company': True
        }
        partner_id = models.execute_kw(
            db, uid, password, 'res.partner', 'create',
            [partner_vals]
        )
        
        if partner_id:
            return {
                'id': partner_id,
                'name': partner_name
            }
            
        return None
    except Exception as e:
        print(f"Error processing partner {partner_name}: {e}")
        return None

def get_shipping_address(address_name, parent_id):
    """ค้นหาหรือสร้าง Shipping Address"""
    if pd.isna(address_name):
        return None
    
    try:
        address_name = str(address_name).strip()
        # Search for existing shipping address
        address_ids = models.execute_kw(
            db, uid, password, 'res.partner', 'search',
            [[
                ['name', '=', address_name],
                ['type', '=', 'delivery']
            ]]
        )
        
        if address_ids:
            address_data = models.execute_kw(
                db, uid, password, 'res.partner', 'read',
                [address_ids[0]], 
                {'fields': ['id', 'name']}
            )[0]
            return address_data
            
        # Create new shipping address if not found
        address_id = models.execute_kw(
            db, uid, password, 'res.partner', 'create',
            [{
                'name': address_name,
                'parent_id': parent_id,
                'type': 'delivery',
                'company_type': 'person',
                'is_company': False
            }]
        )
        
        if address_id:
            return {
                'id': address_id,
                'name': address_name
            }
            
        return None
    except Exception as e:
        print(f"Error processing shipping address {address_name}: {e}")
        return None

def get_user_data(user_name):
    """ค้นหา Salesperson จากชื่อ"""
    if pd.isna(user_name):
        return None
    
    try:
        user_name = str(user_name).strip()
        # Search for user by name
        user_ids = models.execute_kw(
            db, uid, password, 'res.users', 'search',
            [[['name', 'ilike', user_name]]]
        )
        
        if user_ids:
            user_data = models.execute_kw(
                db, uid, password, 'res.users', 'read',
                [user_ids[0]], 
                {'fields': ['id', 'name']}
            )[0]
            return user_data
            
        return None
    except Exception as e:
        print(f"Error processing user {user_name}: {e}")
        return None

def get_team_data(team_name):
    """ค้นหา Sales Team จากชื่อ"""
    if pd.isna(team_name):
        return None
    
    try:
        team_name = str(team_name).strip()
        # Search for team by name
        team_ids = models.execute_kw(
            db, uid, password, 'crm.team', 'search',
            [[['name', 'ilike', team_name]]]
        )
        
        if team_ids:
            team_data = models.execute_kw(
                db, uid, password, 'crm.team', 'read',
                [team_ids[0]], 
                {'fields': ['id', 'name']}
            )[0]
            return team_data
            
        return None
    except Exception as e:
        print(f"Error processing team {team_name}: {e}")
        return None

def get_product_data(product_code, product_name=None):
    """ค้นหาข้อมูลสินค้าจากรหัส"""
    if pd.isna(product_code):
        return None
    
    try:
        product_code = str(product_code).strip()
        # Search for product by default_code (SKU)
        product_ids = models.execute_kw(
            db, uid, password, 'product.product', 'search',
            [[['default_code', '=', product_code]]]
        )
        
        if product_ids:
            product_data = models.execute_kw(
                db, uid, password, 'product.product', 'read',
                [product_ids[0]], 
                {'fields': ['id', 'name', 'default_code', 'list_price', 'uom_id']}
            )[0]
            return product_data
        
        # If product not found, log it
        log_missing_product(product_code, product_name if product_name else 'N/A')
        return None
            
    except Exception as e:
        print(f"Error processing product {product_code}: {e}")
        return None

def get_warehouse_data(warehouse_name):
    """ค้นหา Warehouse จากชื่อ"""
    if pd.isna(warehouse_name):
        return None
    
    try:
        warehouse_name = str(warehouse_name).strip()
        # Search for warehouse by name
        warehouse_ids = models.execute_kw(
            db, uid, password, 'stock.warehouse', 'search',
            [[['name', 'ilike', warehouse_name]]]
        )
        
        if warehouse_ids:
            warehouse_data = models.execute_kw(
                db, uid, password, 'stock.warehouse', 'read',
                [warehouse_ids[0]], 
                {'fields': ['id', 'name']}
            )[0]
            return warehouse_data
            
        return None
    except Exception as e:
        print(f"Error processing warehouse {warehouse_name}: {e}")
        return None



def create_sale_order(row, row_number):
    """สร้าง Sale Order จากข้อมูลในแถว Excel"""
    try:
        # Get partner data
        partner_data = get_partner_data(row['partner_id'])
        if not partner_data:
            log_error(row['name'], row_number, 'Partner Error', f"Partner not found: {row['partner_id']}", row)
            return None
        
        # Get shipping address data
        shipping_data = get_shipping_address(row['partner_shipping_id'], partner_data['id'])
        if not shipping_data:
            log_error(row['name'], row_number, 'Shipping Address Error', 
                     f"Shipping address not found: {row['partner_shipping_id']}", row)
            return None
        
        # Get warehouse data
        warehouse_data = get_warehouse_data(row['warehouse_id'])
        if not warehouse_data:
            log_error(row['name'], row_number, 'Warehouse Error', 
                     f"Warehouse not found: {row['warehouse_id']}", row)
            return None
        
        # Get user data (optional)
        user_data = get_user_data(row['user_id'])
        if not user_data:
            print(f"Warning: User not found for SO {row['name']}: {row['user_id']}")
        
        # Get team data (optional)
        team_data = get_team_data(row['team_id'])
        if not team_data:
            print(f"Warning: Team not found for SO {row['name']}: {row['team_id']}")
        
        # Get product data
        product_data = get_product_data(row['product_id'], row['product_name'] if not pd.isna(row['product_name']) else None)
        if not product_data:
            log_error(row['name'], row_number, 'Product Error', 
                     f"Product not found: {row['product_id']}", row)
            return None
        
        # Prepare SO values
        so_vals = {
            'name': row['name'],
            'date_order': format_date(row['date_order']),
            'partner_id': partner_data['id'],
            'partner_shipping_id': shipping_data['id'],
            'warehouse_id': warehouse_data['id'],
            'note': row['note'] if not pd.isna(row['note']) else False,
            'order_line': [(0, 0, {
                'product_id': product_data['id'],
                'name': row['product_name'] if not pd.isna(row['product_name']) else product_data['name'],
                'product_uom_qty': float(row['product_uom_qty']),
                'price_unit': float(row['price_unit']),
                'product_uom': product_data['uom_id'][0]
            })]
        }
        
        # Add user_id if found
        if user_data:
            so_vals['user_id'] = user_data['id']
            
        # Add team_id if found
        if team_data:
            so_vals['team_id'] = team_data['id']
        
        # Search for existing SO
        existing_so = models.execute_kw(
            db, uid, password, 'sale.order', 'search',
            [[['name', '=', row['name']]]]
        )
        
        if existing_so:
            # Get existing order lines
            so_data = models.execute_kw(
                db, uid, password, 'sale.order', 'read',
                [existing_so[0]], {'fields': ['order_line']}
            )[0]
            
            # Get all order line details to check for duplicates
            if so_data['order_line']:
                order_lines = models.execute_kw(
                    db, uid, password, 'sale.order.line', 'read',
                    [so_data['order_line']],
                    {'fields': ['id', 'product_id']}
                )
                
                # Find and remove lines with the same product
                lines_to_remove = []
                for line in order_lines:
                    if line['product_id'][0] == product_data['id']:
                        lines_to_remove.append((2, line['id'], 0))  # (2, id, 0) is the command to remove a line
                
                if lines_to_remove:
                    # Remove duplicate lines first
                    models.execute_kw(
                        db, uid, password, 'sale.order', 'write',
                        [existing_so[0], {'order_line': lines_to_remove}]
                    )
            
            # Add new line
            print(f"Updated product {row['product_id']} in order {row['name']}")
            return models.execute_kw(
                db, uid, password, 'sale.order', 'write',
                [existing_so[0], {
                    'order_line': [(0, 0, {
                        'product_id': product_data['id'],
                        'name': row['product_name'] if not pd.isna(row['product_name']) else product_data['name'],
                        'product_uom_qty': float(row['product_uom_qty']),
                        'price_unit': float(row['price_unit']),
                        'product_uom': product_data['uom_id'][0]
                    })]
                }]
            )
        else:
            # Create new SO
            return models.execute_kw(
                db, uid, password, 'sale.order', 'create',
                [so_vals]
            )
            
    except Exception as e:
        log_error(row['name'], row_number, 'Processing Error', str(e), row)
        print(f"Failed to process Sale Order {row['name']}: {e}")
        return None

# --- อ่านไฟล์ Excel ---
try:
    excel_file = 'Data_file/import_SO.xlsx'
    df = pd.read_excel(excel_file)
    print(f"Excel file '{excel_file}' read successfully. Number of rows = {len(df)}")
    print("Excel columns:", list(df.columns))
except FileNotFoundError:
    print(f"Error: Excel file '{excel_file}' not found.")
    sys.exit(1)
except Exception as e:
    print(f"Error reading Excel file: {e}")
    sys.exit(1)

# --- Process each row ---
total_rows = len(df)
processed = 0
success = 0
errors = 0

print("\nStarting import process...")
for index, row in df.iterrows():
    processed += 1
    if processed % 100 == 0:  # Show progress every 100 rows
        print(f"Progress: {processed}/{total_rows} rows ({(processed/total_rows*100):.1f}%)")
    
    try:
        result = create_sale_order(row, index + 2)  # +2 because Excel rows start at 1 and header is row 1
        if result is not None:
            success += 1
        else:
            errors += 1
    except Exception as e:
        errors += 1
        print(f"Unexpected error processing row {index + 2}: {e}")

print(f"\nImport completed:")
print(f"Total rows processed: {processed}")
print(f"Successful imports: {success}")
print(f"Errors: {errors}")

# Export error logs at the end
export_error_logs()