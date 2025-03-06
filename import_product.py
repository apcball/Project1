import xmlrpc.client
import pandas as pd
import sys
import base64
import os

# --- ตั้งค่าการเชื่อมต่อ Odoo ---
server_url = 'http://mogth.work:8069'
database = 'Pre_Test'
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

def get_image_data(image_path):
    """อ่านและแปลงไฟล์รูปภาพเป็น base64"""
    if pd.isna(image_path):
        return False
    try:
        full_path = os.path.join('Data_file/images', image_path)
        if os.path.exists(full_path):
            with open(full_path, 'rb') as image_file:
                return base64.b64encode(image_file.read()).decode('utf-8')
    except Exception as e:
        print(f"Error reading image {image_path}: {e}")
    return False

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

def search_tax(tax_name):
    """ค้นหา tax"""
    if pd.isna(tax_name):
        return False

    tax_ids = models.execute_kw(
        database, uid, password, 'account.tax', 'search', [[('name', '=', tax_name.strip())]]
    )
    return tax_ids[0] if tax_ids else False

# --- อ่านข้อมูลจากไฟล์ Excel ---
excel_file = 'Data_file/product_template.xlsx'
try:
    df = pd.read_excel(excel_file)
    print(f"Excel file '{excel_file}' read successfully. Number of rows = {len(df)}")
    
    # ข้ามแถวแรกที่เป็นหัวข้อภาษาไทย
    df = df.iloc[2:]
    df = df.reset_index(drop=True)
    
except Exception as e:
    print("Failed to read Excel file:", e)
    sys.exit(1)

# --- วนลูปประมวลผลแต่ละแถวใน Excel ---
for index, row in df.iterrows():
    try:
        # ตรวจสอบ default_code
        default_code = str(row['default_code']).strip() if pd.notna(row['default_code']) else ''
        if not default_code:
            print(f"Row {index}: Missing default_code. Skipping.")
            continue

        # ตรวจสอบว่ามีสินค้าอยู่แล้วหรือไม่
        existing_products = models.execute_kw(
            database, uid, password, 'product.template', 'search',
            [[['default_code', '=', default_code]]]
        )
        
        if existing_products:
            print(f"Row {index}: Product with default_code '{default_code}' already exists. Skipping.")
            continue

        # เตรียมข้อมูลสินค้า
        product_data = {
            'name': str(row['name']).strip() if pd.notna(row['name']) else '',
            'default_code': default_code,
            'barcode': str(int(row['barcode'])) if pd.notna(row['barcode']) else False,
            'type': 'product',  # กำหนดเป็น storable product
            'categ_id': search_category(row['categ_id']) if pd.notna(row['categ_id']) else False,
            'uom_id': search_uom(row['uom_name']) if pd.notna(row['uom_name']) else False,
            'location': 
            'list_price': float(str(row['list_price']).replace(',', '')) if pd.notna(row['list_price']) else 0.0,
            'standard_price': float(str(row['standard_price']).replace(',', '')) if pd.notna(row['standard_price']) else 0.0,
            'sale_ok': row['sale_ok'] if pd.notna(row['sale_ok']) else True,
            'purchase_ok': row['purchase_ok'] if pd.notna(row['purchase_ok']) else True,
            'active': row['active'] if pd.notna(row['active']) else True,
            'description': str(row['description']).strip() if pd.notna(row['description']) else '',
            'gross_width': float(str(row['gross_width']).replace(',', '')) if pd.notna(row['gross_width']) else 0.0,
            'gross_depth': float(str(row['gross_depth']).replace(',', '')) if pd.notna(row['gross_depth']) else 0.0,
            'gross_height': float(str(row['gross_height']).replace(',', '')) if pd.notna(row['gross_height']) else 0.0,
            'weight': float(str(row['weight']).replace(',', '')) if pd.notna(row['weight']) else 0.0,
            'box_width': float(str(row['box_width']).replace(',', '')) if pd.notna(row['box_width']) else 0.0,
            'box_depth': float(str(row['box_depth']).replace(',', '')) if pd.notna(row['box_depth']) else 0.0,
            'box_height': float(str(row['box_height']).replace(',', '')) if pd.notna(row['box_height']) else 0.0,
            'box_weight': float(str(row['box_weight']).replace(',', '')) if pd.notna(row['box_weight']) else 0.0,
            'cost_method': str(row['cost_method']).strip() if pd.notna(row['cost_method']) else '',
            'qty_available': float(str(row['qty_available']).replace(',', '')) if pd.notna(row['qty_available']) else 0.0,
            'taxes_id': search_tax(row['taxes_id']) if pd.notna(row['taxes_id']) else False,
        }

        # เพิ่มรูปภาพ (ถ้ามี)
        image_data = get_image_data(row['Image']) if pd.notna(row['Image']) else False
        if image_data:
            product_data['image_1920'] = image_data
        # สร้างสินค้าใน Odoo
        try:
            new_product_id = models.execute_kw(
                database, uid, password, 'product.template', 'create', [product_data]
            )
            print(f"Row {index}: Created product '{product_data['name']}' successfully (ID: {new_product_id})")
        except Exception as e:
            print(f"Row {index}: Failed to create product '{product_data['name']}': {e}")
            
    except Exception as e:
        print(f"Row {index}: Error processing row: {e}")