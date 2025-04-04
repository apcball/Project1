import xmlrpc.client
import pandas as pd
import sys
from datetime import datetime

# --- ตั้งค่าการเชื่อมต่อ Odoo ---
url = 'http://mogth.work:8069'
db = 'MOG_Training'
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
                ['parent_id', '=', parent_id],
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
            
        # Create new shipping address
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
            {'fields': ['id', 'name', 'uom_id', 'list_price']}
        )[0]
        return product_data
    return None

# --- อ่านข้อมูลจากไฟล์ Excel ---
excel_file = 'Data_file/import_SO1.xlsx'
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
                    print(f"Partner not found for SO {so_name}")
                    current_so = None
                    continue
                
                # สร้างข้อมูล SO
                current_order_data = {
                    'name': so_name,
                    'partner_id': partner_data['id'],
                    'date_order': format_date(row['date_order']) or datetime.now().strftime('%Y-%m-%d'),
                    'validity_date': format_date(row.get('validity_date')),
                    'state': 'draft'
                }
                
                # ถ้ามี partner_shipping_id
                if pd.notna(row.get('partner_shipping_id')):
                    shipping_partner = get_shipping_address(row['partner_shipping_id'], partner_data['id'])
                    if shipping_partner:
                        current_order_data['partner_shipping_id'] = shipping_partner['id']
                    else:
                        print(f"Warning: Could not process shipping address for SO {so_name}")
                
                # ถ้ามี user_id (Salesperson)
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
            try:
                quantity = float(row['product_uom_qty'])
                price_unit = float(row['price_unit']) if pd.notna(row.get('price_unit')) else product_data['list_price']
                
                order_line = {
                    'product_id': product_data['id'],
                    'name': product_data['name'],
                    'product_uom': product_data['uom_id'][0],
                    'product_uom_qty': quantity,
                    'price_unit': price_unit
                }
                current_order_lines.append((0, 0, order_line))
                print(f"Added product {row['product_id']} to order")
                
            except (ValueError, TypeError) as e:
                print(f"Error processing quantity or price at row {index + 2}: {e}")
                continue

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
        print(f"Failed to process Sale Order {current_so}: {e}")

print("Import process completed.")