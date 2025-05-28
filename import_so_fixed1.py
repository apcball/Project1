import xmlrpc.client
import pandas as pd
import sys
from datetime import datetime
import os

def process_discount(discount_value):
    """แปลงค่า discount ให้อยู่ในรูปแบบที่ถูกต้อง"""
    if pd.isna(discount_value):
        return 0.0, 0.0
    
    try:
        # Convert to string and strip whitespace
        discount_str = str(discount_value).strip()
        
        # Check if empty after stripping
        if not discount_str:
            return 0.0, 0.0
        
        # Remove any spaces between number and %
        discount_str = discount_str.replace(' %', '%').replace('% ', '%')
        
        # If ends with %, it's a percentage discount
        if discount_str.endswith('%'):
            # Remove % and convert to float
            percentage = float(discount_str.rstrip('%'))
            return percentage, 0.0
        else:
            # It's a fixed amount discount
            return 0.0, float(discount_str)
            
    except Exception as e:
        print(f"Error processing discount value {discount_value}: {e}")
        return 0.0, 0.0

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

def validate_number(value):
    """Validate and convert numbers to prevent XML-RPC limits"""
    try:
        if pd.isna(value):
            return 0
        
        # Convert to float first to handle both int and float
        num = float(value)
        
        # Check if number exceeds 32-bit integer limits
        if num > 2147483647 or num < -2147483648:
            # For large numbers, return a safe maximum value
            if num > 0:
                return 2147483647
            return -2147483648
        
        return num
    except:
        return 0

def truncate_string(text, max_length=500):
    """Truncate long strings to prevent XML-RPC size issues"""
    if pd.isna(text):
        return ''
    text = str(text)
    if len(text) > max_length:
        return text[:max_length]
    return text

def format_date(date_str):
    """แปลงรูปแบบวันที่ให้ตรงกับ Odoo format"""
    try:
        if pd.isna(date_str):
            return False
        
        if isinstance(date_str, (datetime, pd.Timestamp)):
            return date_str.strftime('%Y-%m-%d %H:%M:%S')
            
        elif isinstance(date_str, str):
            try:
                # Convert string to datetime
                parsed_date = pd.to_datetime(date_str)
                return parsed_date.strftime('%Y-%m-%d %H:%M:%S')
            except:
                print(f"Warning: Could not parse date string: {date_str}")
                return False
            
        return False
        
    except Exception as e:
        print(f"Error formatting date {date_str}: {str(e)}")
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

def get_tags(tag_names):
    """Get or create tags from comma-separated string"""
    if pd.isna(tag_names):
        return []
        
    tag_ids = []
    try:
        # Split tag names and remove whitespace
        tags = [tag.strip() for tag in str(tag_names).split(',') if tag.strip()]
        
        for tag_name in tags:
            # Search for existing tag
            tag_ids_found = models.execute_kw(
                db, uid, password, 'crm.tag', 'search',
                [[['name', '=', tag_name]]]
            )
            
            if tag_ids_found:
                tag_ids.append(tag_ids_found[0])
            else:
                # Create new tag if not found
                tag_id = models.execute_kw(
                    db, uid, password, 'crm.tag', 'create',
                    [{'name': tag_name}]
                )
                if tag_id:
                    tag_ids.append(tag_id)
    
    except Exception as e:
        print(f"Error processing tags {tag_names}: {e}")
    
    return tag_ids

def get_shipping_address(address_name, parent_id):
    """ค้นหาหรือสร้าง Shipping Address โดยใช้ ilike เพื่อค้นหาแบบไม่คำนึงถึงตัวพิมพ์เล็ก/ใหญ่"""
    if pd.isna(address_name):
        return None
    
    try:
        address_name = str(address_name).strip()
        
        # First, try to find address with exact parent
        address_ids = models.execute_kw(
            db, uid, password, 'res.partner', 'search',
            [[
                ['name', 'ilike', address_name],
                ['parent_id', '=', parent_id],
                ['type', '=', 'delivery']
            ]]
        )
        
        # If not found with parent, search without parent constraint
        if not address_ids:
            address_ids = models.execute_kw(
                db, uid, password, 'res.partner', 'search',
                [[
                    ['name', 'ilike', address_name],
                    ['type', '=', 'delivery']
                ]]
            )
        
        if address_ids:
            # Get all matching addresses
            all_addresses = models.execute_kw(
                db, uid, password, 'res.partner', 'read',
                [address_ids],
                {'fields': ['id', 'name', 'parent_id', 'type']}
            )
            
            selected_address = None
            
            # Try to find best match in this order:
            # 1. Exact name match with correct parent
            # 2. Exact name match with any parent
            # 3. Similar name match with correct parent
            # 4. First similar name match
            for address in all_addresses:
                if address['name'].lower() == address_name.lower():
                    if address.get('parent_id') and address['parent_id'][0] == parent_id:
                        selected_address = address
                        break
                    elif not selected_address:
                        selected_address = address
                elif not selected_address and address.get('parent_id') and address['parent_id'][0] == parent_id:
                    selected_address = address
            
            if not selected_address and all_addresses:
                selected_address = all_addresses[0]
            
            if selected_address:
                return {
                    'id': selected_address['id'],
                    'name': selected_address['name']
                }
        
        # If no matching address found, create new one
        print(f"Creating new shipping address: {address_name} for parent {parent_id}")
        address_vals = {
            'name': address_name,
            'parent_id': parent_id,
            'type': 'delivery',
            'company_type': 'person',
            'is_company': False
        }
        
        try:
            address_id = models.execute_kw(
                db, uid, password, 'res.partner', 'create',
                [address_vals]
            )
            
            if address_id:
                return {
                    'id': address_id,
                    'name': address_name
                }
        except Exception as create_error:
            print(f"Failed to create shipping address: {create_error}")
            return None
            
        return None
    except Exception as e:
        print(f"Error processing shipping address {address_name} for parent {parent_id}: {e}")
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
        # Return default team or None if no team specified
        default_team_ids = models.execute_kw(
            db, uid, password, 'crm.team', 'search',
            [[['name', 'ilike', 'sales']]], {'limit': 1}
        )
        if default_team_ids:
            return models.execute_kw(
                db, uid, password, 'crm.team', 'read',
                [default_team_ids[0]], 
                {'fields': ['id', 'name']}
            )[0]
        return None
    
    try:
        team_name = str(team_name).strip()
        print(f"Searching for team: {team_name}")  # Debug log
        
        # Search for team by name with ilike
        team_ids = models.execute_kw(
            db, uid, password, 'crm.team', 'search',
            [[['name', 'ilike', team_name]]]
        )
        
        if team_ids:
            # Get all matching teams
            all_teams = models.execute_kw(
                db, uid, password, 'crm.team', 'read',
                [team_ids],
                {'fields': ['id', 'name']}
            )
            
            print(f"Found {len(all_teams)} matching teams")  # Debug log
            for team in all_teams:
                print(f"Found team: {team['name']}")  # Debug log
            
            # Try exact match first
            for team in all_teams:
                if team['name'].lower().strip() == team_name.lower().strip():
                    print(f"Selected exact match team: {team['name']}")  # Debug log
                    return team
            
            # If no exact match, return first match
            print(f"Selected first available team: {all_teams[0]['name']}")  # Debug log
            return all_teams[0]
            
        print(f"No team found matching: {team_name}, trying default team")
        # Try to get default sales team
        default_team_ids = models.execute_kw(
            db, uid, password, 'crm.team', 'search',
            [[['name', 'ilike', 'sales']]], {'limit': 1}
        )
        if default_team_ids:
            team_data = models.execute_kw(
                db, uid, password, 'crm.team', 'read',
                [default_team_ids[0]], 
                {'fields': ['id', 'name']}
            )[0]
            print(f"Using default team: {team_data['name']}")
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
                {'fields': [
                    'id', 'name', 'default_code', 'list_price', 'uom_id',
                    'taxes_id', 'packaging_ids'
                ]}
            )[0]
            return product_data
        
        # If product not found, log it
        log_missing_product(product_code, product_name if product_name else 'N/A')
        return None
            
    except Exception as e:
        print(f"Error processing product {product_code}: {e}")
        return None

def get_packaging_data(product_id, packaging_code):
    """ค้นหาข้อมูลหน่วยบรรจุจากรหัส"""
    if pd.isna(packaging_code):
        return None
    
    try:
        packaging_code = str(packaging_code).strip()
        # Search for packaging by code
        packaging_ids = models.execute_kw(
            db, uid, password, 'product.packaging', 'search',
            [[
                ['product_id', '=', product_id],
                ['name', '=', packaging_code]
            ]]
        )
        
        if packaging_ids:
            packaging_data = models.execute_kw(
                db, uid, password, 'product.packaging', 'read',
                [packaging_ids[0]], 
                {'fields': ['id', 'name', 'qty']}
            )[0]
            return packaging_data
            
        return None
            
    except Exception as e:
        print(f"Error processing packaging {packaging_code}: {e}")
        return None

def process_discount(discount_value):
    """แปลงค่า discount ให้อยู่ในรูปแบบที่ถูกต้อง"""
    if pd.isna(discount_value):
        return 0.0, 0.0
    
    try:
        # Convert to string and strip whitespace
        discount_str = str(discount_value).strip()
        
        # Check if empty after stripping
        if not discount_str:
            return 0.0, 0.0
        
        # Remove any spaces between number and %
        discount_str = discount_str.replace(' %', '%').replace('% ', '%')
        
        # If ends with %, it's a percentage discount
        if discount_str.endswith('%'):
            # Remove % and convert to float
            percentage = float(discount_str.rstrip('%'))
            return percentage, 0.0
        else:
            # It's a fixed amount discount
            return 0.0, float(discount_str)
            
    except Exception as e:
        print(f"Error processing discount value {discount_value}: {e}")
        return 0.0, 0.0
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

def get_pricelist_data(pricelist_name):
    """ค้นหา Pricelist จากชื่อ โดยใช้ ilike เพื่อค้นหาแบบไม่คำนึงถึงตัวพิมพ์เล็ก/ใหญ่"""
    if pd.isna(pricelist_name):
        # Try to get default THB pricelist
        default_pricelist_ids = models.execute_kw(
            db, uid, password, 'product.pricelist', 'search',
            [[['currency_id.name', '=', 'THB']]], {'limit': 1}
        )
        if default_pricelist_ids:
            return models.execute_kw(
                db, uid, password, 'product.pricelist', 'read',
                [default_pricelist_ids[0]],
                {'fields': ['id', 'name', 'currency_id']}
            )[0]
        return None

    try:
        pricelist_name = str(pricelist_name).strip()
        print(f"Searching for pricelist: {pricelist_name}")  # Debug log
        
        # Extract currency code if present in parentheses
        currency_code = None
        if '(' in pricelist_name and ')' in pricelist_name:
            currency_code = pricelist_name[pricelist_name.rfind('(')+1:pricelist_name.rfind(')')].strip()
            base_name = pricelist_name[:pricelist_name.rfind('(')].strip()
            print(f"Extracted currency code: {currency_code}, base name: {base_name}")  # Debug log
        else:
            base_name = pricelist_name
        
        # Build domain for search
        domain = []
        if currency_code:
            domain = [
                '|',
                ['name', 'ilike', pricelist_name],
                '&',
                ['name', 'ilike', base_name],
                ['currency_id.name', '=', currency_code]
            ]
        else:
            domain = [['name', 'ilike', pricelist_name]]
        
        # Search for pricelist
        pricelist_ids = models.execute_kw(
            db, uid, password, 'product.pricelist', 'search',
            [domain]
        )

        if pricelist_ids:
            # Get all matching pricelists
            all_pricelists = models.execute_kw(
                db, uid, password, 'product.pricelist', 'read',
                [pricelist_ids],
                {'fields': ['id', 'name', 'currency_id']}
            )
            
            print(f"Found {len(all_pricelists)} matching pricelists")  # Debug log
            for pl in all_pricelists:
                print(f"Found pricelist: {pl['name']} (Currency: {pl['currency_id'][1]})")  # Debug log
            
            # Try to find exact match first
            for pricelist in all_pricelists:
                if pricelist['name'].lower().strip() == pricelist_name.lower().strip():
                    print(f"Selected exact match pricelist: {pricelist['name']}")  # Debug log
                    return pricelist
            
            # Try to match by currency if specified
            if currency_code:
                for pricelist in all_pricelists:
                    if pricelist['currency_id'][1] == currency_code:
                        print(f"Selected currency match pricelist: {pricelist['name']}")  # Debug log
                        return pricelist
            
            # If no specific match, return first match
            print(f"Selected first available pricelist: {all_pricelists[0]['name']}")  # Debug log
            return all_pricelists[0]

        print(f"No pricelist found matching: {pricelist_name}, trying default THB pricelist")
        # Try to get default THB pricelist
        default_pricelist_ids = models.execute_kw(
            db, uid, password, 'product.pricelist', 'search',
            [[['currency_id.name', '=', 'THB']]], {'limit': 1}
        )
        if default_pricelist_ids:
            pricelist_data = models.execute_kw(
                db, uid, password, 'product.pricelist', 'read',
                [default_pricelist_ids[0]],
                {'fields': ['id', 'name', 'currency_id']}
            )[0]
            print(f"Using default THB pricelist: {pricelist_data['name']}")
            return pricelist_data

        return None

    except Exception as e:
        print(f"Error processing pricelist {pricelist_name}: {e}")
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
        shipping_data = None
        if not pd.isna(row['partner_shipping_id']):
            shipping_data = get_shipping_address(row['partner_shipping_id'], partner_data['id'])
            if not shipping_data:
                log_error(row['name'], row_number, 'Shipping Address Error', 
                         f"Failed to create/find shipping address: {row['partner_shipping_id']} for partner {partner_data['name']}", row)
                return None
        else:
            # If no shipping address specified, use partner address
            shipping_data = {'id': partner_data['id'], 'name': partner_data['name']}
        
        # Get warehouse data
        warehouse_data = get_warehouse_data(row['warehouse_id'])
        if not warehouse_data:
            log_error(row['name'], row_number, 'Warehouse Error', 
                     f"Warehouse not found: {row['warehouse_id']}", row)
            return None
        
        # Get user data (optional)
        user_data = None
        if not pd.isna(row.get('user_id')):
            user_data = get_user_data(row['user_id'])
        
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

        # Get packaging data if specified
        packaging_data = None
        if not pd.isna(row.get('packaging_id')):
            packaging_data = get_packaging_data(product_data['id'], row['packaging_id'])
            
        # Prepare order line with validated numbers and truncated strings
        discount_percent, discount_fixed = process_discount(row.get('discount'))
        order_line = {
            'product_id': product_data['id'],
            'name': truncate_string(row['product_name'] if not pd.isna(row['product_name']) else product_data['name']),
            'product_uom_qty': validate_number(row['product_uom_qty']),
            'price_unit': validate_number(row['price_unit']),
            'product_uom': product_data['uom_id'][0],
            'sequence': validate_number(row.get('sequence', 10)),
            'discount': discount_percent,
            'discount_fixed': discount_fixed,
            'tax_id': [(6, 0, product_data.get('taxes_id', []))],
        }
        
        
        # Add packaging if found
        if packaging_data:
            order_line.update({
                'product_packaging_id': packaging_data['id'],
                'product_packaging_qty': float(row['packaging_qty']) if not pd.isna(row.get('packaging_qty')) else 1.0
            })
        
        # Get tags data
        tag_ids = get_tags(row.get('tags')) if not pd.isna(row.get('tags')) else []
        
        # Get pricelist data
        pricelist_id = False
        if not pd.isna(row.get('pricelist_id')):
            pricelist_data = get_pricelist_data(row['pricelist_id'])
            if pricelist_data:
                pricelist_id = pricelist_data['id']
                print(f"Using pricelist: {pricelist_data['name']} (ID: {pricelist_id})")  # Debug log
            else:
                print(f"Warning: Could not find pricelist: {row['pricelist_id']}")
        
        # Prepare SO values with truncated strings
        so_vals = {
            'name': truncate_string(row['name']),
            'date_order': format_date(row['date_order']),
            'commitment_date': format_date(row['commitment_date']) if not pd.isna(row.get('commitment_date')) else False,
            'client_order_ref': truncate_string(row['client_order_ref']) if not pd.isna(row.get('client_order_ref')) else False,
            'partner_id': partner_data['id'],
            'pricelist_id': pricelist_id,
            'partner_shipping_id': shipping_data['id'],
            'warehouse_id': warehouse_data['id'],
            'user_id': user_data['id'] if user_data else False,
            'note': truncate_string(row['note'], 1000) if not pd.isna(row['note']) else False,
            'tag_ids': [(6, 0, tag_ids)] if tag_ids else False,
            'order_line': [(0, 0, order_line)]
        }
        
        # Add team_id if found
        if team_data:
            so_vals['team_id'] = team_data['id']
        
        # Search for existing SO
        existing_so = models.execute_kw(
            db, uid, password, 'sale.order', 'search',
            [[['name', '=', row['name']]]]
        )
        
        if existing_so:
            # Get existing order state and other data
            so_data = models.execute_kw(
                db, uid, password, 'sale.order', 'read',
                [existing_so[0]], {'fields': ['state', 'order_line']}
            )[0]

            # Check if order is confirmed (state != 'draft')
            if so_data['state'] != 'draft':
                log_error(
                    row['name'],
                    row_number,
                    'Update Error',
                    f"Cannot update confirmed sale order (State: {so_data['state']})",
                    row
                )
                print(f"Warning: Cannot update confirmed sale order {row['name']}")
                return None
            
            # Get all order line details to check for duplicates
            if so_data['order_line']:
                order_lines = models.execute_kw(
                    db, uid, password, 'sale.order.line', 'read',
                    [so_data['order_line']],
                    {'fields': ['id', 'product_id', 'sequence']}
                )
                
                # Find and remove lines with the same sequence or product
                lines_to_remove = []
                for line in order_lines:
                    if (not pd.isna(row.get('sequence')) and line['sequence'] == int(row['sequence'])) or \
                       line['product_id'][0] == product_data['id']:
                        lines_to_remove.append((2, line['id'], 0))  # (2, id, 0) is the command to remove a line
                
                if lines_to_remove:
                    # Remove duplicate lines first
                    models.execute_kw(
                        db, uid, password, 'sale.order', 'write',
                        [existing_so[0], {'order_line': lines_to_remove}]
                    )
            
                # Add new line and update tags
                print(f"Updated product {row['product_id']} in order {row['name']}")
                
                # Get tags data
                tag_ids = get_tags(row.get('tags')) if not pd.isna(row.get('tags')) else []
                
                update_vals = {
                    'order_line': [(0, 0, {
                        'product_id': product_data['id'],
                        'name': truncate_string(row['product_name'] if not pd.isna(row['product_name']) else product_data['name']),
                        'product_uom_qty': validate_number(row['product_uom_qty']),
                        'price_unit': validate_number(row['price_unit']),
                        'product_uom': product_data['uom_id'][0],
                        'sequence': validate_number(row.get('sequence', 10)),
                        'discount': discount_percent,
                        'discount_fixed': discount_fixed,
                    })]
                }
                
                # Add tags if present
                if tag_ids:
                    update_vals['tag_ids'] = [(6, 0, tag_ids)]
                
                return models.execute_kw(
                    db, uid, password, 'sale.order', 'write',
                    [existing_so[0], update_vals]
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
    excel_file = 'Data_file/import_SO_มีนา.xlsx'

    df = pd.read_excel(excel_file)

    # Convert date columns with dayfirst=True
    if 'date_order' in df.columns:
        df['date_order'] = pd.to_datetime(df['date_order'], dayfirst=True)
        df['date_order'] = df['date_order'].dt.strftime('%Y-%m-%d %H:%M:%S')

    if 'commitment_date' in df.columns and not df['commitment_date'].isna().all():
        df['commitment_date'] = pd.to_datetime(df['commitment_date'], dayfirst=True)
        df['commitment_date'] = df['commitment_date'].dt.strftime('%Y-%m-%d %H:%M:%S')
    
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
    if processed % 10 == 0:  # Show progress every 10 rows
        print(f"Progress: {processed}/{total_rows} rows ({(processed/total_rows*100):.1f}%)")
    
    try:
        result = create_sale_order(row, index + 2)  # +2 because Excel rows start at 1 and header is row 1
        if result is not None:
            success += 1
            print(f"Successfully processed SO {row['name']}")
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