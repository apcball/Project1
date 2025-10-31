import xmlrpc.client
import pandas as pd
import sys
import base64
import os
import requests

# --- ตั้งค่าการเชื่อมต่อ Odoo ---
server_url = 'http://mogth.work:8069'
database = 'MOG_Pretest1'
username = 'apichart@mogen.co.th'
password = '471109538'
 
# --- Authentication ---
try:
    common = xmlrpc.client.ServerProxy(f'{server_url}/xmlrpc/2/common')
    uid = common.authenticate(database, username, password, {})
    if not uid:
        print("Authentication failed: ตรวจสอบ credentials หรือ permission")
        sys.exit(1)
    else:
        print("Authentication successful, uid =", uid)
except Exception as e:
    print("Error during authentication:", e)
    sys.exit(1)

# --- สร้าง models proxy ---
try:
    models = xmlrpc.client.ServerProxy(f'{server_url}/xmlrpc/2/object')
except Exception as e:
    print("Error creating XML-RPC models proxy:", e)
    sys.exit(1)

def search_category(category_path):
    """ค้นหาหรือสร้าง category จาก path"""
    if pd.isna(category_path):
        return False
    
    categories = category_path.split('/')
    parent_id = False
    current_id = False
    
    for category in categories:
        category = category.strip()
        if category:
            domain = [('name', '=', category)]
            if parent_id:
                domain.append(('parent_id', '=', parent_id))
            
            category_ids = models.execute_kw(
                database, uid, password, 'product.category', 'search', [domain]
            )
            
            if category_ids:
                current_id = category_ids[0]
            else:
                # สร้าง category ใหม่
                vals = {
                    'name': category,
                    'parent_id': parent_id
                }
                try:
                    current_id = models.execute_kw(
                        database, uid, password, 'product.category', 'create', [vals]
                    )
                except Exception as e:
                    print(f"Error creating category {category}: {e}")
                    return False
            
            parent_id = current_id
    
    return current_id
def search_uom(uom_name):
    """ค้นหา UoM"""
    if pd.isna(uom_name):
        return False
    
    uom_ids = models.execute_kw(
        database, uid, password, 'uom.uom', 'search', [[('name', '=', uom_name.strip())]]
    )
    return uom_ids[0] if uom_ids else False

def get_customer_tax():
    """ค้นหา Customer Tax (VAT 7%)"""
    tax_ids = models.execute_kw(
        database, uid, password, 'account.tax', 'search',
        [[('type_tax_use', '=', 'sale'), ('amount', '=', 7.0), ('name', 'like', '%7%')]]
    )
    if tax_ids:
        return tax_ids[0]
    print("Warning: Customer VAT 7% tax not found")
    return False

def search_tags(tag_field_value):
    """Resolve a comma-separated list of product tag names or numeric ids to product.tag ids.

    Features:
    - Uses an in-memory cache to reduce RPCs
    - Supports an optional mapping file Data_file/tag_mapping.csv with columns: source,tag_name,tag_id
    - Prefetches existing tags once for fast local matching
    - Auto-creates tags when not found
    """

    if pd.isna(tag_field_value) or tag_field_value is None:
        return []

    raw = str(tag_field_value)
    if not raw.strip():
        return []

    # initialize cache and prefetch structures
    if not hasattr(search_tags, '_cache'):
        search_tags._cache = {}
    if not hasattr(search_tags, '_prefetch'):
        search_tags._prefetch = {}

    # load optional mapping file Data_file/tag_mapping.csv
    mapping = {}
    map_file = os.path.join('Data_file', 'tag_mapping.csv')
    if os.path.exists(map_file):
        try:
            map_df = pd.read_csv(map_file, dtype=str)
            for _, r in map_df.iterrows():
                src = str(r.get('source', '')).strip()
                if not src:
                    continue
                entry = {}
                tgt_name = r.get('tag_name') if 'tag_name' in r.index else None
                tgt_id = r.get('tag_id') if 'tag_id' in r.index else None
                if pd.notna(tgt_name) and str(tgt_name).strip():
                    entry['tag_name'] = str(tgt_name).strip()
                if pd.notna(tgt_id) and str(tgt_id).strip():
                    try:
                        entry['tag_id'] = int(float(str(tgt_id).strip()))
                    except Exception:
                        entry['tag_id'] = None
                mapping[src.lower()] = entry
        except Exception:
            mapping = {}

    # prefetch all tags (id,name) for faster lookups
    if not search_tags._prefetch:
        try:
            all_tags = models.execute_kw(database, uid, password, 'product.tag', 'search_read', [[], ['id', 'name']])
            for t in all_tags:
                name = str(t.get('name', '')).lower()
                if name:
                    search_tags._prefetch[name] = t.get('id')
        except Exception:
            # leave prefetch empty
            pass

    tokens = [t.strip() for t in raw.split(',') if t.strip()]
    resolved_ids = []

    for token in tokens:
        key = token.lower()

        # mapping file override
        if key in mapping:
            entry = mapping[key]
            if 'tag_id' in entry and entry['tag_id']:
                resolved_ids.append(entry['tag_id'])
                search_tags._cache[key] = [entry['tag_id']]
                continue
            if 'tag_name' in entry and entry['tag_name']:
                token = entry['tag_name']
                key = token.lower()

        # cached result
        if key in search_tags._cache:
            cached = search_tags._cache[key]
            if cached:
                resolved_ids.extend(cached)
            continue

        # numeric id token
        if token.replace('.', '', 1).isdigit():
            try:
                tid = int(float(token))
                # verify existence quickly using prefetch or RPC
                if tid in search_tags._prefetch.values():
                    resolved_ids.append(tid)
                    search_tags._cache[key] = [tid]
                    continue
                exists = models.execute_kw(database, uid, password, 'product.tag', 'search', [[('id', '=', tid)]])
                if exists:
                    resolved_ids.append(tid)
                    search_tags._cache[key] = [tid]
                    continue
            except Exception:
                pass

        # exact prefetch match
        pref = search_tags._prefetch.get(token.lower())
        if pref:
            resolved_ids.append(pref)
            search_tags._cache[key] = [pref]
            continue

        # ilike within prefetch keys
        ilike_match = None
        for name_lower, tid in search_tags._prefetch.items():
            if token.lower() in name_lower:
                ilike_match = tid
                break
        if ilike_match:
            resolved_ids.append(ilike_match)
            search_tags._cache[key] = [ilike_match]
            continue

        # RPC fallback exact then ilike
        try:
            found = models.execute_kw(database, uid, password, 'product.tag', 'search', [[('name', '=', token)]])
            if found:
                resolved_ids.append(found[0])
                search_tags._cache[key] = [found[0]]
                search_tags._prefetch[token.lower()] = found[0]
                continue

            found = models.execute_kw(database, uid, password, 'product.tag', 'search', [[('name', 'ilike', token)]])
            if found:
                resolved_ids.append(found[0])
                search_tags._cache[key] = [found[0]]
                search_tags._prefetch[str(found[0]).lower()] = found[0]
                continue

            # auto-create tag if allowed
            sanitized = token.strip()
            if sanitized:
                try:
                    new_id = models.execute_kw(database, uid, password, 'product.tag', 'create', [{'name': sanitized}])
                    if new_id:
                        resolved_ids.append(new_id)
                        search_tags._cache[key] = [new_id]
                        search_tags._prefetch[sanitized.lower()] = new_id
                        print(f"  Auto-created product.tag: '{sanitized}' (id: {new_id})")
                        continue
                except Exception as e:
                    print(f"  ⚠ Failed to create tag '{sanitized}': {e}")
        except Exception:
            # ignore RPC errors and continue
            pass

    # return unique ids preserving order
    unique = list(dict.fromkeys(resolved_ids))
    # cache empty results for tokens not resolved for faster future lookups
    for token in tokens:
        k = token.lower()
        if k not in search_tags._cache:
            search_tags._cache[k] = []
    return unique

def process_image(image_path, product_hint=None):
    """แปลงไฟล์รูปภาพเป็น base64

    If image_path is a directory, attempt to pick a file inside. If product_hint is provided,
    prefer files that include the hint (default_code or name) in the filename.
    """
    if pd.isna(image_path):
        return False, "No image path provided"
    
    image_path = str(image_path).strip()
    if not image_path:
        return False, "Empty image path"
    
    try:
        # ถ้าเป็น URL
        if image_path.startswith(('http://', 'https://')):
            try:
                response = requests.get(image_path, timeout=10)
                if response.status_code == 200:
                    image_data = base64.b64encode(response.content)
                    return image_data.decode('utf-8'), None
                return False, f"Failed to download image. Status code: {response.status_code}"
            except requests.exceptions.RequestException as e:
                return False, f"Error downloading image: {str(e)}"
        else:
            # ถ้าเป็นไฟล์ในเครื่อง
            # กำหนด base path สำหรับรูปภาพ
            base_image_path = r"C:\Users\Ball\Pictures\image"
            
            # ถ้าเป็น relative path ให้ต่อกับ base path
            if not os.path.isabs(image_path):
                image_path = os.path.join(base_image_path, image_path)
            
            print(f"กำลังอ่านรูปภาพจาก: {image_path}")
            
            # If path is a directory, try to find an image file inside
            if os.path.isdir(image_path):
                try:
                    files = [f for f in os.listdir(image_path) if os.path.isfile(os.path.join(image_path, f))]
                    # prefer files that contain product_hint
                    chosen = None
                    if product_hint:
                        low = str(product_hint).lower()
                        for f in files:
                            if low in f.lower():
                                chosen = os.path.join(image_path, f)
                                break
                        # If a product_hint was provided but no files matched, skip the image
                        if not chosen:
                            return False, f"No image filename matches hint '{product_hint}' in folder: {image_path}"
                    else:
                        # No product_hint provided: fall back to previous behavior
                        if len(files) == 1:
                            chosen = os.path.join(image_path, files[0])
                        if not chosen and files:
                            # fallback to first image-like file by extension
                            for f in files:
                                if f.lower().endswith(('.png', '.jpg', '.jpeg', '.bmp', '.gif', '.webp')):
                                    chosen = os.path.join(image_path, f)
                                    break
                        if not chosen:
                            return False, f"ไม่พบไฟล์รูปภาพภายในโฟลเดอร์: {image_path}"
                    image_path = chosen
                except Exception as e:
                    return False, f"Error reading directory: {str(e)}"

            if not os.path.exists(image_path):
                return False, f"ไม่พบไฟล์รูปภาพ: {image_path}"

            try:
                with open(image_path, 'rb') as image_file:
                    image_data = base64.b64encode(image_file.read())
                    if not image_data:
                        return False, "ไม่สามารถอ่านข้อมูลรูปภาพได้"
                    return image_data.decode('utf-8'), None
            except IOError as e:
                return False, f"เกิดข้อผิดพลาดในการอ่านไฟล์: {str(e)}"
    
    except Exception as e:
        return False, f"เกิดข้อผิดพลาดที่ไม่คาดคิด: {str(e)}"

# สร้าง list เก็บข้อมูลสินค้าที่ import ไม่สำเร็จ
failed_imports = []

# --- อ่านข้อมูลจากไฟล์ Excel ---
excel_file = 'Import_Product/import_product_expens.xlsx'
try:
    df = pd.read_excel(excel_file)
    print(f"Excel file '{excel_file}' read successfully. Number of rows = {len(df)}")
    
    # Print column names to check structure
    print("\nAvailable columns in Excel:", df.columns.tolist())
    
    # ข้ามแถวแรกที่เป็นหัวข้อภาษาไทย
    df = df.iloc[2:]
    df = df.reset_index(drop=True)
    
    # Print first row to check data
    print("\nFirst row data:")
    for col in df.columns:
        print(f"{col}: {df.iloc[0][col]}")
    
except Exception as e:
    print("Failed to read Excel file:", e)
    sys.exit(1)

# Get customer tax ID once
customer_tax_id = get_customer_tax()

# --- วนลูปประมวลผลแต่ละแถวใน Excel ---
for index, row in df.iterrows():
    try:
        # ตรวจสอบและแปลง default_code
        if pd.notna(row['default_code']):
            # ถ้าเป็นตัวเลข ให้แปลงเป็นจำนวนเต็ม
            if str(row['default_code']).replace('.', '').isdigit():
                default_code = str(int(float(row['default_code'])))
            else:
                default_code = str(row['default_code']).strip()
        else:
            default_code = ''

        if not default_code:
            print(f"Row {index}: Missing default_code. Skipping.")
            continue

        # Clean up barcode data
        barcode = str(row['barcode']) if pd.notna(row['barcode']) else False
        if barcode and barcode.strip():  # ตรวจสอบว่า barcode มีค่าและไม่ใช่ string ว่าง
            # Remove decimal point and zeros if present
            barcode = str(float(barcode)).rstrip('0').rstrip('.')
        else:
            barcode = False  # ถ้า barcode ว่างหรือเป็น NaN ให้กำหนดเป็น False

        # ตรวจสอบว่ามีสินค้าอยู่แล้วหรือไม่ (ทั้ง default_code และ barcode ที่ไม่ว่าง)
        domain = ['|',
                 ['default_code', '=', default_code],
                 '&',
                 ['barcode', '!=', False],
                 ['barcode', '=', barcode]]
        existing_products = models.execute_kw(
            database, uid, password, 'product.template', 'search',
            [domain]
        )
        
        # ฟังก์ชันสำหรับตรวจสอบค่าบูลีน
        def parse_boolean_field(value, default=True):
            if pd.isna(value):
                return default
            str_value = str(value).strip().lower()
            # ถ้าเป็นตัวเลข
            if str_value.replace('.', '').isdigit():
                return bool(float(str_value))
            # ถ้าเป็นข้อความ
            return str_value in ('yes', 'true', '1', 'y', 't')

        # ฟังก์ชันสำหรับแมปค่าจากคอลัมน์ product_type ใน sheet ไปเป็นค่า detailed_type ของ Odoo 17
        def map_product_type(value):
            """Map various input strings to Odoo's detailed_type values: 'product', 'consu', 'service'.

            Returns None if value is empty or unrecognized.
            """
            if pd.isna(value):
                return None
            s = str(value).strip().lower()
            if not s:
                return None

            # Accept common human-friendly inputs
            if s in ('stockable', 'stockable product', 'storable', 'storable product', 'stock', 'stockable goods', 'stockable_product', 'storable_product'):
                return 'product'
            if s in ('consumable', 'consu', 'consumable product', 'consumable_product'):
                return 'consu'
            if s in ('service', 'services'):
                return 'service'

            # If a user already provided Odoo internal values
            if s in ('product', 'consu', 'service'):
                return s

            # Unknown value -> don't map
            return None

        if existing_products:
            existing_product = models.execute_kw(
                database, uid, password, 'product.template', 'read',
                [existing_products[0]], {'fields': ['name', 'default_code', 'barcode']}
            )[0]
            print(f"\nพบสินค้าในระบบ (Row {index}):")
            print(f"  - Default Code: {existing_product['default_code']}")
            print(f"  - Barcode: {existing_product['barcode']}")
            print(f"  - Name: {existing_product['name']}")
            print("  กำลังอัพเดทข้อมูล...")

            # For updates: only change boolean flags if the Excel row provides them.
            # This prevents unintentionally archiving products when the sheet leaves
            # the 'active' column empty.
            sale_ok_provided = pd.notna(row.get('sale_ok'))
            purchase_ok_provided = pd.notna(row.get('purchase_ok'))
            active_provided = pd.notna(row.get('active'))

            # เตรียมข้อมูลสำหรับอัพเดท (do not include booleans here unless provided)
            update_data = {
                'name': str(row['name']).strip() if pd.notna(row['name']) else existing_product['name'],
                'name_eng': str(row['name_eng']).strip() if pd.notna(row['name_eng']) else '',
                'old_product_code': str(row['old_product_code']).strip() if pd.notna(row['old_product_code']) and str(row['old_product_code']).strip() else False,
                'sku': str(row['sku']).strip() if pd.notna(row['sku']) else '',
                'barcode': barcode if barcode else existing_product.get('barcode', False),
                'categ_id': search_category(row['categ_id']) if pd.notna(row['categ_id']) else False,
                'uom_id': search_uom(row['uom_id']) if pd.notna(row['uom_id']) else False,
                'list_price': float(str(row['list_price']).replace(',', '')) if pd.notna(row['list_price']) else 0.0,
                'standard_price': float(str(row['standard_price']).replace(',', '')) if pd.notna(row['standard_price']) else 0.0,
                'normal_price': float(str(row['normal_price']).replace(',', '')) if pd.notna(row.get('normal_price')) else 0.0,
                # sale_ok, purchase_ok, active: set below only if provided in the sheet
                'can_be_expensed': True if pd.notna(row.get('can_be_expensed')) and str(row.get('can_be_expensed')).strip().lower() in ('yes', 'true', '1', 'y', 't', '1.0') else False,
                'description': str(row['description']).strip() if pd.notna(row['description']) else '',
                'gross_width': float(str(row['gross_width']).replace(',', '')) if pd.notna(row['gross_width']) else 0.0,
                'gross_depth': float(str(row['gross_depth']).replace(',', '')) if pd.notna(row['gross_depth']) else 0.0,
                'gross_height': float(str(row['gross_height']).replace(',', '')) if pd.notna(row['gross_height']) else 0.0,
                'weight': float(str(row['weight']).replace(',', '')) if pd.notna(row['weight']) else 0.0,
                'box_width': float(str(row['box_width']).replace(',', '')) if pd.notna(row['box_width']) else 0.0,
                'box_depth': float(str(row['box_depth']).replace(',', '')) if pd.notna(row['box_depth']) else 0.0,
                'box_height': float(str(row['box_height']).replace(',', '')) if pd.notna(row['box_height']) else 0.0,
                'box_weight': float(str(row['box_weight']).replace(',', '')) if pd.notna(row['box_weight']) else 0.0,
                'taxes_id': [(6, 0, [customer_tax_id])] if customer_tax_id else [(6, 0, [])],
                'supplier_taxes_id': [(6, 0, [])],
            }

            # volume field (numeric) from sheet -> Odoo product 'volume'
            if pd.notna(row.get('volume')):
                try:
                    update_data['volume'] = float(str(row.get('volume')).replace(',', ''))
                except Exception:
                    print(f"  ⚠ ไม่สามารถแปลงค่า volume: {row.get('volume')}")

            # ถ้ามีคอลัมน์ product_type ให้แมปเป็น detailed_type ของ Odoo 17
            if pd.notna(row.get('product_type')):
                mapped = map_product_type(row.get('product_type'))
                if mapped:
                    update_data['detailed_type'] = mapped
                    # also set legacy 'type' to keep behavior consistent
                    update_data['type'] = mapped
                    print(f"  product_type: {row.get('product_type')} -> {mapped}")
                else:
                    print(f"  product_type: {row.get('product_type')} -> Unrecognized, skipping mapping")

                # product_tag_ids on update (comma-separated names or ids)
                if pd.notna(row.get('product_tag_ids')):
                    resolved_tags = search_tags(row.get('product_tag_ids'))
                    if resolved_tags:
                        update_data['product_tag_ids'] = [(6, 0, resolved_tags)]
                        print(f"  product_tag_ids resolved: {resolved_tags}")
                    else:
                        print(f"  product_tag_ids: {row.get('product_tag_ids')} -> no matching tags found")

            # ตรวจสอบและอัพเดทรูปภาพ
            if pd.notna(row.get('image')):
                print(f"  กำลังอัพเดทรูปภาพ: {row['image']}")
                sku_hint = str(row.get('sku')).strip() if pd.notna(row.get('sku')) and str(row.get('sku')).strip() else default_code
                image_data, error_msg = process_image(row['image'], product_hint=sku_hint)
                if image_data:
                    update_data['image_1920'] = image_data
                    print("  ✓ ประมวลผลรูปภาพสำเร็จ")
                else:
                    print(f"  ⚠ ไม่สามารถประมวลผลรูปภาพ: {error_msg}")

            # Now conditionally set boolean flags only when the Excel provides them
            try:
                if sale_ok_provided:
                    update_data['sale_ok'] = parse_boolean_field(row.get('sale_ok'), default=False)
                if purchase_ok_provided:
                    update_data['purchase_ok'] = parse_boolean_field(row.get('purchase_ok'), default=False)
                if active_provided:
                    update_data['active'] = parse_boolean_field(row.get('active'), default=False)
            except Exception:
                # If parsing fails, skip setting the boolean(s) and continue
                pass

            try:
                # อัพเดทข้อมูลสินค้า
                models.execute_kw(
                    database, uid, password, 'product.template', 'write',
                    [existing_products[0], update_data]
                )
                print("  ✓ อัพเดทข้อมูลสำเร็จ")
            except Exception as e:
                error_msg = f"ไม่สามารถอัพเดทข้อมูลได้: {str(e)}"
                print(f"  ✗ {error_msg}")
                failed_imports.append({
                    'Row': index + 3,
                    'Default Code': default_code,
                    'Name': row['name'] if pd.notna(row['name']) else '',
                    'Barcode': barcode,
                    'Error': error_msg
                })
            continue

        # อ่านค่า sale_ok, purchase_ok (default เป็น False ถ้าไม่ระบุค่า)
        # และ active (default เป็น True ถ้าไม่ระบุค่า)
        sale_ok_value = parse_boolean_field(row.get('sale_ok'), default=False)
        purchase_ok_value = parse_boolean_field(row.get('purchase_ok'), default=False)
        active_value = parse_boolean_field(row.get('active'), default=True)

        print(f"\nกำลังตรวจสอบค่าสถานะสินค้า (Row {index}):")
        print(f"  sale_ok: {row.get('sale_ok')} -> {sale_ok_value}")
        print(f"  purchase_ok: {row.get('purchase_ok')} -> {purchase_ok_value}")
        print(f"  active: {row.get('active')} -> {active_value}")

        # เตรียมข้อมูลสินค้า
        # Map product_type for new product (use 'product' as default)
        mapped_product_type = None
        if pd.notna(row.get('product_type')):
            mapped_product_type = map_product_type(row.get('product_type'))
            if mapped_product_type:
                print(f"  product_type: {row.get('product_type')} -> {mapped_product_type}")
            else:
                print(f"  product_type: {row.get('product_type')} -> Unrecognized, will use default")

        product_data = {
            'name': str(row['name']).strip() if pd.notna(row['name']) else '',
            'name_eng': str(row['name_eng']).strip() if pd.notna(row['name_eng']) else '',
            'old_product_code': str(row['old_product_code']).strip() if pd.notna(row['old_product_code']) and str(row['old_product_code']).strip() else False,
            'default_code': default_code,
            'sku': str(row['sku']).strip() if pd.notna(row['sku']) else '',
            'barcode': barcode,
            # set both 'type' (legacy) and 'detailed_type' (Odoo 17) where possible
            'type': mapped_product_type if mapped_product_type else 'product',
            'detailed_type': mapped_product_type if mapped_product_type else 'product',
            'categ_id': search_category(row['categ_id']) if pd.notna(row['categ_id']) else False,
            'uom_id': search_uom(row['uom_id']) if pd.notna(row['uom_id']) else False,
            'list_price': float(str(row['list_price']).replace(',', '')) if pd.notna(row['list_price']) else 0.0,
            'standard_price': float(str(row['standard_price']).replace(',', '')) if pd.notna(row['standard_price']) else 0.0,
            'normal_price': float(str(row['normal_price']).replace(',', '')) if pd.notna(row.get('normal_price')) else 0.0,
            'sale_ok': sale_ok_value,
            'purchase_ok': purchase_ok_value,
            'active': active_value,
            'volume': float(str(row['volume']).replace(',', '')) if pd.notna(row.get('volume')) else 0.0,
            'can_be_expensed': True if pd.notna(row.get('can_be_expensed')) and str(row.get('can_be_expensed')).strip().lower() in ('yes', 'true', '1', 'y', 't', '1.0') else False,
            'description': str(row['description']).strip() if pd.notna(row['description']) else '',
            'gross_width': float(str(row['gross_width']).replace(',', '')) if pd.notna(row['gross_width']) else 0.0,
            'gross_depth': float(str(row['gross_depth']).replace(',', '')) if pd.notna(row['gross_depth']) else 0.0,
            'gross_height': float(str(row['gross_height']).replace(',', '')) if pd.notna(row['gross_height']) else 0.0,
            'weight': float(str(row['weight']).replace(',', '')) if pd.notna(row['weight']) else 0.0,
            'box_width': float(str(row['box_width']).replace(',', '')) if pd.notna(row['box_width']) else 0.0,
            'box_depth': float(str(row['box_depth']).replace(',', '')) if pd.notna(row['box_depth']) else 0.0,
            'box_height': float(str(row['box_height']).replace(',', '')) if pd.notna(row['box_height']) else 0.0,
            'box_weight': float(str(row['box_weight']).replace(',', '')) if pd.notna(row['box_weight']) else 0.0,
            'taxes_id': [(6, 0, [customer_tax_id])] if customer_tax_id else [(6, 0, [])],
            'supplier_taxes_id': [(6, 0, [])],
        }

        # product_tag_ids for new product (comma-separated names or ids)
        if pd.notna(row.get('product_tag_ids')):
            resolved_new_tags = search_tags(row.get('product_tag_ids'))
            if resolved_new_tags:
                product_data['product_tag_ids'] = [(6, 0, resolved_new_tags)]
                print(f"  product_tag_ids resolved for new product: {resolved_new_tags}")
            else:
                print(f"  product_tag_ids: {row.get('product_tag_ids')} -> no matching tags found for new product")

        # แสดงข้อมูลที่จะเพิ่ม
        print(f"\nกำลังเพิ่มสินค้าใหม่ (Row {index}):")
        print(f"  ชื่อสินค้า: {product_data['name']}")
        print(f"  รหัสสินค้า: {product_data['default_code']}")
        print(f"  บาร์โค้ด: {product_data['barcode']}")
        print(f"  ราคาขาย: {product_data['list_price']}")
        print(f"  ราคาทุน: {product_data['standard_price']}")
        # print normal_price and volume if present
        if pd.notna(row.get('normal_price')):
            try:
                print(f"  ราคาปกติ (normal_price): {float(str(row.get('normal_price')).replace(',', ''))}")
            except Exception:
                print(f"  ราคาปกติ (normal_price): {row.get('normal_price')}")
        if pd.notna(row.get('volume')):
            try:
                print(f"  ปริมาตร (volume): {float(str(row.get('volume')).replace(',', ''))}")
            except Exception:
                print(f"  ปริมาตร (volume): {row.get('volume')}")
        
        # ตรวจสอบและแสดงข้อมูลรูปภาพ
        if pd.notna(row.get('image')):
            print(f"  รูปภาพ: {row['image']}")
            sku_hint = str(row.get('sku')).strip() if pd.notna(row.get('sku')) and str(row.get('sku')).strip() else default_code
            image_data, error_msg = process_image(row['image'], product_hint=sku_hint)
            if image_data:
                product_data['image_1920'] = image_data
                print("  ✓ ประมวลผลรูปภาพสำเร็จ")
            else:
                print(f"  ⚠ ไม่สามารถประมวลผลรูปภาพ: {error_msg}")
        else:
            print("  ไม่มีรูปภาพ")

        # สร้างสินค้าใน Odoo
        try:
            # ตรวจสอบ barcode อีกครั้ง
            if product_data['barcode']:
                barcode_exists = models.execute_kw(
                    database, uid, password, 'product.template', 'search_count',
                    [[['barcode', '=', product_data['barcode']]]]
                )
                if barcode_exists:
                    error_msg = f"บาร์โค้ด {product_data['barcode']} มีอยู่แล้วในระบบ"
                    print(f"  ไม่สามารถเพิ่มสินค้าได้: {error_msg}")
                    failed_imports.append({
                        'Row': index + 3,
                        'Default Code': default_code,
                        'Name': product_data['name'],
                        'Barcode': barcode,
                        'Error': error_msg
                    })
                    continue

            new_product_id = models.execute_kw(
                database, uid, password, 'product.template', 'create', [product_data]
            )
            print(f"  ✓ เพิ่มสินค้าสำเร็จ (ID: {new_product_id})")

            # ตรวจสอบการอัพโหลดรูปภาพ
            if 'image_1920' in product_data:
                product_info = models.execute_kw(
                    database, uid, password, 'product.template', 'read',
                    [new_product_id], {'fields': ['image_1920']}
                )
                if product_info and product_info[0].get('image_1920'):
                    print("  ✓ อัพโหลดรูปภาพสำเร็จ")
                else:
                    print("  ⚠ อัพโหลดรูปภาพไม่สำเร็จ ลองอัพเดทอีกครั้ง...")
                    models.execute_kw(
                        database, uid, password, 'product.template', 'write',
                        [[new_product_id], {'image_1920': product_data['image_1920']}]
                    )
                    print("  ✓ อัพเดทรูปภาพสำเร็จ")

        except Exception as e:
            print(f"  ✗ ไม่สามารถเพิ่มสินค้าได้: {str(e)}")
            error_msg = str(e)
            # แสดงข้อมูลเพิ่มเติมเพื่อการแก้ไข
            if "Barcode" in str(e):
                print("    โปรดตรวจสอบ: บาร์โค้ดอาจซ้ำกับสินค้าที่มีอยู่แล้ว")
            
            failed_imports.append({
                'Row': index + 3,
                'Default Code': default_code,
                'Name': product_data['name'],
                'Barcode': barcode,
                'Error': error_msg
            })
            
    except Exception as e:
        error_msg = f"Error processing row: {str(e)}"
        print(f"Row {index}: {error_msg}")
        print("Available columns:", df.columns.tolist())
        
        failed_imports.append({
            'Row': index + 3,
            'Default Code': default_code if 'default_code' in locals() else 'N/A',
            'Name': row['name'] if pd.notna(row['name']) else 'N/A',
            'Barcode': barcode if 'barcode' in locals() else 'N/A',
            'Error': error_msg
        })

# บันทึกข้อมูลสินค้าที่ import ไม่สำเร็จลงไฟล์ Excel
if failed_imports:
    # สร้าง DataFrame จากข้อมูลที่ import ไม่สำเร็จ
    failed_df = pd.DataFrame(failed_imports)
    
    # กำหนดชื่อไฟล์ Excel ที่จะบันทึก
    timestamp = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
    failed_excel_file = f'Project1/Import_Product/failed_imports_{timestamp}.xlsx'
    
    # Ensure directory exists then save Excel
    failed_dir = os.path.dirname(failed_excel_file)
    if failed_dir and not os.path.exists(failed_dir):
        os.makedirs(failed_dir, exist_ok=True)
    failed_df.to_excel(failed_excel_file, index=False, engine='openpyxl')
    print(f"\nบันทึกรายการสินค้าที่ import ไม่สำเร็จไว้ที่: {failed_excel_file}")
    print(f"จำนวนรายการที่ import ไม่สำเร็จ: {len(failed_imports)} รายการ")