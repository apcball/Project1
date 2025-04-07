import xmlrpc.client
import pandas as pd
import sys
from datetime import datetime
import os

# Initialize list to store error logs
error_logs = []

def log_error(so_name, row_number, error_type, error_message, row_data=None):
    """บันทึก error log"""
    error_logs.append({
        'SO Number': so_name,
        'Row Number': row_number,
        'Error Type': error_type,
        'Error Message': error_message,
        'Row Data': str(row_data) if row_data is not None else ''
    })

def export_error_logs():
    """Export error logs to Excel file"""
    if error_logs:
        try:
            # Create logs directory if it doesn't exist
            log_dir = 'logs'
            if not os.path.exists(log_dir):
                os.makedirs(log_dir)
            
            # Create filename with timestamp
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            log_file = os.path.join(log_dir, f'import_errors_{timestamp}.xlsx')
            
            # Convert logs to DataFrame and export
            df_errors = pd.DataFrame(error_logs)
            df_errors.to_excel(log_file, index=False)
            print(f"\nError log exported to: {log_file}")
            print(f"Total errors logged: {len(error_logs)}")
        except Exception as e:
            print(f"Failed to export error log: {e}")

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
                {'fields': ['id', 'name', 'property_product_pricelist']}
            )[0]
            return partner_data
            
        # If not found, try creating new partner
        partner_id = models.execute_kw(
            db, uid, password, 'res.partner', 'create',
            [{
                'name': partner_name,
                'company_type': 'company',
                'is_company': True
            }]
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
        print(f"Error finding salesperson {user_name}: {e}")
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
        print(f"Error finding sales team {team_name}: {e}")
        return None

def get_pricelist_data(pricelist_name):
    """ค้นหา Pricelist จากชื่อ"""
    if pd.isna(pricelist_name):
        return None
    
    try:
        pricelist_name = str(pricelist_name).strip()
        # Search for pricelist by name
        pricelist_ids = models.execute_kw(
            db, uid, password, 'product.pricelist', 'search',
            [[['name', '=', pricelist_name]]]
        )
        
        if pricelist_ids:
            pricelist_data = models.execute_kw(
                db, uid, password, 'product.pricelist', 'read',
                [pricelist_ids[0]], 
                {'fields': ['id', 'name']}
            )[0]
            return pricelist_data
        return None
    except Exception as e:
        print(f"Error finding pricelist {pricelist_name}: {e}")
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
            [[['name', '=', warehouse_name]]]
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
        print(f"Error finding warehouse {warehouse_name}: {e}")
        return None

def get_existing_so(so_name):
    """ค้นหา SO ที่มีอยู่ในระบบ"""
    so_ids = models.execute_kw(
        db, uid, password, 'sale.order', 'search',
        [[['name', '=', so_name]]]
    )
    if so_ids:
        so_data = models.execute_kw(
            db, uid, password, 'sale.order', 'read',
            [so_ids[0]], 
            {'fields': ['id', 'name', 'state']}
        )[0]
        return so_data
    return None

def get_product_data(product_code):
    """ค้นหาข้อมูลสินค้าจากรหัสสินค้า"""
    if pd.isna(product_code):
        return None
    
    product_code = str(product_code).strip()
    product_ids = models.execute_kw(
        db, uid, password, 'product.product', 'search',
        [[['default_code', '=', product_code]]]
    )
    
    if product_ids:
        product_data = models.execute_kw(
            db, uid, password, 'product.product', 'read',
            [product_ids[0]], 
            {'fields': ['id', 'name', 'uom_id', 'list_price', 'description_sale', 'taxes_id']}
        )[0]
        return product_data
    return None

def create_order_line(row, product_data, index, so_name):
    """สร้าง Order Line จากข้อมูล"""
    try:
        quantity = float(row['product_uom_qty'])
        price_unit = float(row['price_unit']) if pd.notna(row.get('price_unit')) else product_data['list_price']
        
        order_line = {
            'product_id': product_data['id'],
            'product_uom_qty': quantity,
            'price_unit': price_unit,
            'product_uom': product_data['uom_id'][0],
            'name': product_data.get('description_sale') or product_data['name'],
        }

        # Add taxes from product if available
        if product_data.get('taxes_id'):
            order_line['tax_id'] = [(6, 0, product_data['taxes_id'])]
        else:
            order_line['tax_id'] = [(6, 0, [])]

        # Add discount if present
        if pd.notna(row.get('discount')):
            try:
                discount = float(row['discount'])
                order_line['discount'] = discount
            except (ValueError, TypeError):
                print(f"Invalid discount value at row {index + 2}")

        # Override taxes if specified in Excel
        if pd.notna(row.get('tax_id')):
            try:
                tax_name = str(row['tax_id']).strip()
                tax_ids = models.execute_kw(
                    db, uid, password, 'account.tax',
                    'search',
                    [[['name', '=', tax_name], ['type_tax_use', '=', 'sale']]]
                )
                if tax_ids:
                    order_line['tax_id'] = [(6, 0, tax_ids)]
                else:
                    print(f"Tax not found for line in SO {so_name}: {tax_name}")
            except Exception as e:
                print(f"Error processing tax at row {index + 2}: {e}")

        return (0, 0, order_line)
    except (ValueError, TypeError) as e:
        print(f"Error processing quantity or price at row {index + 2}: {e}")
        return None

# --- อ่านข้อมูลจากไฟล์ Excel ---
excel_file = 'Data_file/import_SO.xlsx'
try:
    df = pd.read_excel(excel_file)
    print(f"Excel file '{excel_file}' read successfully. Number of rows = {len(df)}")
    print("Excel columns:", df.columns.tolist())
except Exception as e:
    print("Failed to read Excel file:", e)
    sys.exit(1)

# --- ประมวลผลข้อมูลและสร้าง Sale Order ---
current_so = None
current_order_lines = []
current_order_data = None

# Keep track of processed SOs to avoid duplicates
processed_sos = set()

for index, row in df.iterrows():
    try:
        so_name = str(row['name']).strip()
        
        # ถ้าเป็น SO ใหม่
        if current_so != so_name:
            # บันทึก SO เก่า (ถ้ามี)
            if current_order_data and current_order_lines and current_so not in processed_sos:
                current_order_data['order_line'] = current_order_lines
                
                # ตรวจสอบว่ามี SO อยู่แล้วหรือไม่
                existing_so = get_existing_so(current_so)
                
                try:
                    if existing_so:
                        # ถ้า SO มีอยู่แล้วและอยู่ในสถานะ draft ให้อัพเดท
                        if existing_so['state'] == 'draft':
                            # ลบ order lines เดิม
                            old_lines = models.execute_kw(
                                db, uid, password, 'sale.order.line', 'search',
                                [[['order_id', '=', existing_so['id']]]]
                            )
                            if old_lines:
                                models.execute_kw(
                                    db, uid, password, 'sale.order.line', 'unlink',
                                    [old_lines]
                                )
                            
                            # อัพเดท SO
                            models.execute_kw(
                                db, uid, password, 'sale.order', 'write',
                                [existing_so['id'], current_order_data]
                            )
                            print(f"Updated existing Sale Order {current_so} (ID: {existing_so['id']})")
                        else:
                            print(f"Cannot update Sale Order {current_so} - not in draft state")
                    else:
                        # สร้าง SO ใหม่
                        order_id = models.execute_kw(
                            db, uid, password, 'sale.order', 'create',
                            [current_order_data]
                        )
                        print(f"Created new Sale Order {current_so} with ID: {order_id}")
                    
                    processed_sos.add(current_so)
                except Exception as e:
                    print(f"Failed to process Sale Order {current_so}: {e}")

            # เริ่ม SO ใหม่ (ถ้ายังไม่เคยประมวลผล)
            if so_name not in processed_sos:
                current_so = so_name
                current_order_lines = []
                
                # ค้นหาข้อมูลลูกค้า
                partner_data = get_partner_data(row['partner_id'])
                if not partner_data:
                    log_error(so_name, index + 2, 'Partner Error', 
                            f"Partner not found: {row['partner_id']}", row['partner_id'])
                    print(f"Partner not found for SO {so_name}")
                    current_so = None
                    continue
                
                # สร้างข้อมูล SO
                current_order_data = {
                    'name': so_name,
                    'partner_id': partner_data['id'],
                    'date_order': format_date(row['date_order']) or datetime.now().strftime('%Y-%m-%d'),
                    'validity_date': format_date(row.get('validity_date')),
                    'state': 'draft',
                    'user_id': False  # Set default empty salesperson
                }
                
                # ถ้ามี pricelist_id
                if pd.notna(row.get('pricelist_id')):
                    pricelist_data = get_pricelist_data(row['pricelist_id'])
                    if pricelist_data:
                        current_order_data['pricelist_id'] = pricelist_data['id']
                    else:
                        log_error(so_name, index + 2, 'Pricelist Error',
                                f"Pricelist not found: {row['pricelist_id']}", row['pricelist_id'])
                        print(f"Warning: Pricelist not found for SO {so_name}: {row['pricelist_id']}")
                elif partner_data.get('property_product_pricelist'):
                    current_order_data['pricelist_id'] = partner_data['property_product_pricelist'][0]
                
                # ถ้ามี warehouse_id
                if pd.notna(row.get('warehouse_id')):
                    warehouse_data = get_warehouse_data(row['warehouse_id'])
                    if warehouse_data:
                        current_order_data['warehouse_id'] = warehouse_data['id']
                    else:
                        log_error(so_name, index + 2, 'Warehouse Error',
                                f"Warehouse not found: {row['warehouse_id']}", row['warehouse_id'])
                        print(f"Warning: Warehouse not found for SO {so_name}: {row['warehouse_id']}")
                
                # ถ้ามี partner_shipping_id
                if pd.notna(row.get('partner_shipping_id')):
                    shipping_address = str(row['partner_shipping_id']).strip()
                    shipping_partner = get_shipping_address(shipping_address, partner_data['id'])
                    if shipping_partner:
                        current_order_data['partner_shipping_id'] = shipping_partner['id']
                        # Set delivery address
                        current_order_data['delivery_address'] = shipping_address
                    else:
                        print(f"Warning: Could not process shipping address for SO {so_name}: {shipping_address}")
                else:
                    # If no shipping address specified, use the partner's address
                    current_order_data['partner_shipping_id'] = partner_data['id']
                
                # ถ้ามี user_id (Salesperson) และไม่ใช่ค่าว่าง
                if pd.notna(row.get('user_id')):
                    user_data = get_user_data(row['user_id'])
                    if user_data:
                        current_order_data['user_id'] = user_data['id']
                    else:
                        print(f"Warning: Salesperson not found for SO {so_name}: {row['user_id']}")
                        
                # ถ้ามี team_id (Sales Team)
                if pd.notna(row.get('team_id')):
                    team_data = get_team_data(row['team_id'])
                    if team_data:
                        current_order_data['team_id'] = team_data['id']
                    else:
                        print(f"Warning: Sales Team not found for SO {so_name}: {row['team_id']}")
                        
                # ถ้ามี note
                if pd.notna(row.get('note')):
                    current_order_data['note'] = str(row['note'])
        
        # เพิ่ม order line
        if current_so and current_so not in processed_sos:
            # ดึงข้อมูลสินค้า
            product_data = get_product_data(row['product_id'])
            if not product_data:
                print(f"Product not found: {row['product_id']}")
                continue

            # สร้าง order line
            order_line = create_order_line(row, product_data, index, current_so)
            if order_line:
                current_order_lines.append(order_line)
                print(f"Added product {row['product_id']} to order {current_so}")

    except Exception as e:
        print(f"Error processing row {index + 2}: {e}")
        continue

# บันทึก SO สุดท้าย (ถ้ามี)
if current_order_data and current_order_lines and current_so not in processed_sos:
    current_order_data['order_line'] = current_order_lines
    
    # ตรวจสอบว่ามี SO อยู่แล้วหรือไม่
    existing_so = get_existing_so(current_so)
    
    try:
        if existing_so:
            # ถ้า SO มีอยู่แล้วและอยู่ในสถานะ draft ให้อัพเดท
            if existing_so['state'] == 'draft':
                # ลบ order lines เดิม
                old_lines = models.execute_kw(
                    db, uid, password, 'sale.order.line', 'search',
                    [[['order_id', '=', existing_so['id']]]]
                )
                if old_lines:
                    models.execute_kw(
                        db, uid, password, 'sale.order.line', 'unlink',
                        [old_lines]
                    )
                
                # อัพเดท SO
                models.execute_kw(
                    db, uid, password, 'sale.order', 'write',
                    [existing_so['id'], current_order_data]
                )
                print(f"Updated existing Sale Order {current_so} (ID: {existing_so['id']})")
            else:
                print(f"Cannot update Sale Order {current_so} - not in draft state")
        else:
            # สร้าง SO ใหม่
            order_id = models.execute_kw(
                db, uid, password, 'sale.order', 'create',
                [current_order_data]
            )
            print(f"Created new Sale Order {current_so} with ID: {order_id}")
        
        processed_sos.add(current_so)
    except Exception as e:
        log_error(current_so, 'Final SO', 'Processing Error', f"Failed to process final Sale Order: {str(e)}")
        print(f"Failed to process final Sale Order {current_so}: {e}")

# Export error logs if any
export_error_logs()