import xmlrpc.client
import pandas as pd
import sys
import base64
import os
import requests

# --- ตั้งค่าการเชื่อมต่อ Odoo ---
server_url = 'http://mogth.work:8069'
database = 'MOG_LIVE'
username = 'apichart@mogen.co.th'
password = '471109538'

# กำหนดขนาดของ batch
BATCH_SIZE = 25

# --- Authentication and Connection Functions ---
def connect_to_odoo():
    try:
        # สร้าง custom transport class with timeout
        class TimeoutTransport(xmlrpc.client.Transport):
            def make_connection(self, host):
                connection = super().make_connection(host)
                connection.timeout = 30  # timeout in seconds
                return connection

        # สร้าง connection with custom transport
        common = xmlrpc.client.ServerProxy(
            f'{server_url}/xmlrpc/2/common',
            transport=TimeoutTransport()
        )
        
        # ทดสอบการเชื่อมต่อก่อน authenticate
        try:
            common.version()
        except Exception as e:
            print(f"Server connection test failed: {e}")
            return None, None
        
        # ทำการ authenticate
        try:
            uid = common.authenticate(database, username, password, {})
            if not uid:
                print("Authentication failed: ตรวจสอบ credentials หรือ permission")
                return None, None
        except Exception as e:
            print(f"Authentication error: {e}")
            return None, None
        
        # สร้าง models proxy with timeout
        models = xmlrpc.client.ServerProxy(
            f'{server_url}/xmlrpc/2/object',
            transport=TimeoutTransport()
        )
        
        print("Connection successful, uid =", uid)
        return uid, models
        
    except ConnectionRefusedError:
        print("Connection refused: เซิร์ฟเวอร์ไม่ตอบสนอง กรุณาตรวจสอบ server_url และการเชื่อมต่อเครือข่าย")
        return None, None
    except xmlrpc.client.ProtocolError as e:
        print(f"Protocol error: {e}")
        return None, None
    except Exception as e:
        print(f"Unexpected connection error: {e}")
        return None, None

def ensure_connection():
    global uid, models
    max_retries = 5  # เพิ่มจำนวนครั้งในการ retry
    initial_retry_delay = 5  # seconds
    max_retry_delay = 60  # maximum delay in seconds
    
    for attempt in range(max_retries):
        if attempt > 0:
            # ใช้ exponential backoff สำหรับ retry delay
            retry_delay = min(initial_retry_delay * (2 ** (attempt - 1)), max_retry_delay)
            print(f"Attempting to reconnect... (Attempt {attempt + 1}/{max_retries}, waiting {retry_delay} seconds)")
            import time
            time.sleep(retry_delay)
        
        try:
            new_uid, new_models = connect_to_odoo()
            if new_uid and new_models:
                uid = new_uid
                models = new_models
                # ทดสอบการเชื่อมต่อด้วยการเรียกใช้คำสั่งง่ายๆ
                try:
                    models.execute_kw(database, uid, password, 'res.users', 'search_count', [[]])
                    return True
                except Exception as e:
                    print(f"Connection test failed: {e}")
                    continue
        except Exception as e:
            print(f"Connection attempt failed: {e}")
            continue
    
    print("Failed to establish a stable connection after multiple attempts")
    return False

# Initial connection
uid, models = connect_to_odoo()
if not uid or not models:
    print("Initial connection failed")
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
    """ค้นหา UoM โดยรองรับหลายรูปแบบของชื่อหน่วย"""
    if pd.isna(uom_name):
        return False
    
    uom_name = str(uom_name).strip().lower()
    
    # สร้าง mapping สำหรับหน่วยที่มีชื่อเรียกต่างกัน
    uom_mapping = {
        'ชิ้น': ['piece', 'pieces', 'ชิ้น', 'pcs', 'pc'],
        'กล่อง': ['box', 'boxes', 'กล่อง'],
        'แพ็ค': ['pack', 'packs', 'แพ็ค', 'แพค'],
        'เซต': ['set', 'sets', 'เซต'],
        'ชุด': ['ชุด', 'set', 'sets'],
        'อัน': ['อัน', 'piece', 'pieces'],
        'คู่': ['pair', 'pairs', 'คู่'],
        'ม้วน': ['roll', 'rolls', 'ม้วน'],
        'แกลลอน': ['gallon', 'gallons', 'แกลลอน'],
        'ถุง': ['bag', 'bags', 'ถุง'],
        'กิโลกรัม': ['kg', 'kgs', 'kilogram', 'kilograms', 'กิโลกรัม'],
        'เมตร': ['meter', 'meters', 'm', 'เมตร'],
        'ลิตร': ['liter', 'liters', 'l', 'ลิตร']
    }
    
    # ค้นหาจากชื่อที่ตรงกันก่อน
    uom_ids = models.execute_kw(
        database, uid, password, 'uom.uom', 'search', [[('name', '=', uom_name)]]
    )
    
    if uom_ids:
        return uom_ids[0]
    
    # ถ้าไม่เจอ ให้ค้นหาจาก mapping
    for standard_name, variants in uom_mapping.items():
        if uom_name in variants:
            # ค้นหาจากชื่อมาตรฐาน
            uom_ids = models.execute_kw(
                database, uid, password, 'uom.uom', 'search',
                [[('name', 'ilike', standard_name)]]
            )
            if uom_ids:
                return uom_ids[0]
            
            # ค้นหาจากชื่อ variants ทั้งหมด
            for variant in variants:
                uom_ids = models.execute_kw(
                    database, uid, password, 'uom.uom', 'search',
                    [[('name', 'ilike', variant)]]
                )
                if uom_ids:
                    return uom_ids[0]
    
    # ถ้ายังไม่เจอ ลองค้นหาแบบ partial match
    uom_ids = models.execute_kw(
        database, uid, password, 'uom.uom', 'search',
        [[('name', 'ilike', uom_name)]]
    )
    
    if not uom_ids:
        print(f"Warning: UoM '{uom_name}' not found. Available UoMs:")
        # แสดงรายการ UoM ที่มีในระบบ
        all_uoms = models.execute_kw(
            database, uid, password, 'uom.uom', 'search_read',
            [[]], {'fields': ['name']}
        )
        for uom in all_uoms:
            print(f"- {uom['name']}")
        return False
        
    return uom_ids[0]

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

def process_image(image_path):
    """แปลงไฟล์รูปภาพเป็น base64"""
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
            base_image_path = r"C:\Users\Ball\Pictures\image"
            
            if not os.path.isabs(image_path):
                image_path = os.path.join(base_image_path, image_path)
            
            print(f"กำลังอ่านรูปภาพจาก: {image_path}")
            
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

def process_product_row(row, index):
    """ประมวลผลข้อมูลสินค้าแต่ละแถว"""
    try:
        # ตรวจสอบและแปลง default_code
        if pd.notna(row['default_code']):
            if str(row['default_code']).replace('.', '').isdigit():
                default_code = str(int(float(row['default_code'])))
            else:
                default_code = str(row['default_code']).strip()
        else:
            default_code = ''

        if not default_code:
            print(f"Row {index}: Missing default_code. Skipping.")
            return None

        # Clean up barcode data
        barcode = str(row['barcode']) if pd.notna(row['barcode']) else False
        if barcode and barcode.strip():
            barcode = str(float(barcode)).rstrip('0').rstrip('.')
        else:
            barcode = False

        # ค้นหาหรือสร้าง category
        categ_id = search_category(row['categ_id'])
        if not categ_id:
            print(f"Row {index}: Failed to find/create category. Skipping.")
            return None

        # ค้นหา UoM
        uom_id = search_uom(row['uom_id'])
        if not uom_id:
            print(f"Row {index}: UoM not found. Skipping.")
            return None

        # ประมวลผลรูปภาพ
        image_data = None
        if pd.notna(row['image']):
            image_data, error = process_image(row['image'])
            if error:
                print(f"Row {index}: Image processing error: {error}")

        # เตรียมข้อมูลสินค้า
        product_vals = {
            'name': str(row['name']).strip() if pd.notna(row['name']) else '',
            'default_code': default_code,
            'barcode': barcode,
            'categ_id': categ_id,
            'type': 'product',
            'uom_id': uom_id,
            'uom_po_id': uom_id,
            'list_price': float(row['list_price']) if pd.notna(row['list_price']) else 0.0,
            'standard_price': float(row['standard_price']) if pd.notna(row['standard_price']) else 0.0,
            'taxes_id': [(6, 0, [customer_tax_id])] if customer_tax_id else False,
            'sale_ok': bool(row['sale_ok']) if pd.notna(row['sale_ok']) else False,
            'purchase_ok': bool(row['purchase_ok']) if pd.notna(row['purchase_ok']) else False,
            'active': True,
            'detailed_type': 'product',
            'invoice_policy': 'order',
        }

        if image_data:
            product_vals['image_1920'] = image_data

        return {
            'index': index,
            'default_code': default_code,
            'barcode': barcode,
            'vals': product_vals
        }

    except Exception as e:
        error_msg = f"Row {index}: Unexpected error processing row: {str(e)}"
        print(error_msg)
        failed_imports.append({
            'row': index,
            'default_code': default_code if 'default_code' in locals() else 'Unknown',
            'error': str(e)
        })
        return None

def process_batch(batch_data):
    """ประมวลผล batch ของสินค้า - สร้างใหม่หรืออัพเดท"""
    # ตรวจสอบการเชื่อมต่อก่อนเริ่มประมวลผล batch
    if not ensure_connection():
        print("Failed to establish connection. Skipping batch.")
        for item in batch_data:
            failed_imports.append({
                'row': item['index'],
                'default_code': item['default_code'],
                'error': "Connection failed"
            })
        return

    # รวบรวม default_codes และ barcodes ที่ไม่ซ้ำกัน
    all_default_codes = [item['default_code'] for item in batch_data if item['default_code']]
    all_barcodes = [item['barcode'] for item in batch_data if item['barcode']]

    # ตรวจสอบสินค้าที่มีอยู่แล้ว
    try:
        domain = ['|',
                ['default_code', 'in', all_default_codes],
                '&',
                ['barcode', '!=', False],
                ['barcode', 'in', all_barcodes]]
        existing_products = models.execute_kw(
            database, uid, password, 'product.template', 'search_read',
            [domain], {'fields': ['id', 'default_code', 'barcode']}
        )
    except Exception as e:
        if not ensure_connection():
            print(f"Connection failed during product search: {str(e)}")
            for item in batch_data:
                failed_imports.append({
                    'row': item['index'],
                    'default_code': item['default_code'],
                    'error': f"Connection error: {str(e)}"
                })
            return
        # Try one more time after reconnection
        try:
            existing_products = models.execute_kw(
                database, uid, password, 'product.template', 'search_read',
                [domain], {'fields': ['id', 'default_code', 'barcode']}
            )
        except Exception as e:
            print(f"Failed to search products even after reconnection: {str(e)}")
            for item in batch_data:
                failed_imports.append({
                    'row': item['index'],
                    'default_code': item['default_code'],
                    'error': f"Search error: {str(e)}"
                })
            return

    # สร้าง dictionary ของสินค้าที่มีอยู่แล้ว
    existing_products_dict = {}
    for product in existing_products:
        if product['default_code']:
            existing_products_dict[product['default_code']] = product['id']
        if product['barcode']:
            existing_products_dict[product['barcode']] = product['id']

    # แยกสินค้าเป็นสองกลุ่ม: สร้างใหม่และอัพเดท
    products_to_create = []
    products_to_update = {}  # {product_id: values}

    for item in batch_data:
        product_id = None
        # ค้นหาสินค้าจาก default_code หรือ barcode
        if item['default_code'] in existing_products_dict:
            product_id = existing_products_dict[item['default_code']]
        elif item['barcode'] and item['barcode'] in existing_products_dict:
            product_id = existing_products_dict[item['barcode']]

        if product_id:
            # สินค้ามีอยู่แล้ว - เตรียมอัพเดท
            products_to_update[product_id] = item['vals']
            print(f"Row {item['index']}: Updating product '{item['default_code']}'")
        else:
            # สินค้าใหม่ - เตรียมสร้าง
            products_to_create.append(item['vals'])
            print(f"Row {item['index']}: Preparing to create new product '{item['default_code']}'")

    # สร้างสินค้าใหม่
    if products_to_create:
        try:
            created_products = models.execute_kw(
                database, uid, password, 'product.template', 'create', [products_to_create]
            )
            print(f"Successfully created {len(created_products)} new products in batch")
        except Exception as e:
            print(f"Error creating new products: {str(e)}")
            for item in batch_data:
                if item['vals'] in products_to_create:
                    failed_imports.append({
                        'row': item['index'],
                        'default_code': item['default_code'],
                        'error': f"Create error: {str(e)}"
                    })

    # อัพเดทสินค้าที่มีอยู่แล้ว
    for product_id, values in products_to_update.items():
        try:
            models.execute_kw(
                database, uid, password, 'product.template', 'write',
                [[product_id], values]
            )
            print(f"Successfully updated product ID {product_id}")
        except Exception as e:
            print(f"Error updating product ID {product_id}: {str(e)}")
            # หา index และ default_code ของสินค้าที่ error
            error_item = next(
                (item for item in batch_data if item['vals'] == values),
                None
            )
            if error_item:
                failed_imports.append({
                    'row': error_item['index'],
                    'default_code': error_item['default_code'],
                    'error': f"Update error: {str(e)}"
                })

# สร้าง list เก็บข้อมูลสินค้าที่ import ไม่สำเร็จ
failed_imports = []

# --- อ่านข้อมูลจากไฟล์ Excel ---
excel_file = 'Data_file/import_product_OB.xlsx'
try:
    df = pd.read_excel(excel_file)
    print(f"Excel file '{excel_file}' read successfully. Number of rows = {len(df)}")
    
    # Print column names to check structure
    print("\nAvailable columns in Excel:", df.columns.tolist())
    
    # ข้ามแถวแรกที่เป็นหัวข้อภาษาไทย
    df = df.iloc[1:]
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

# --- ประมวลผลข้อมูลเป็น batch ---
current_batch = []
total_processed = 0
print(f"\nProcessing {len(df)} products in batches of {BATCH_SIZE}...")

for index, row in df.iterrows():
    processed_data = process_product_row(row, index)
    if processed_data:
        current_batch.append(processed_data)
        
    # เมื่อครบ batch size หรือถึงข้อมูลสุดท้าย ให้ประมวลผล batch
    if len(current_batch) >= BATCH_SIZE or index == len(df) - 1:
        if current_batch:  # ตรวจสอบว่ามีข้อมูลใน batch
            print(f"\nProcessing batch of {len(current_batch)} products...")
            process_batch(current_batch)
            total_processed += len(current_batch)
            print(f"Total products processed so far: {total_processed}")
            current_batch = []  # เคลียร์ batch เพื่อเริ่มใหม่

# --- แสดงสรุปการ import ---
print("\nImport Summary:")
print(f"Total rows processed: {len(df)}")
print(f"Failed imports: {len(failed_imports)}")

if failed_imports:
    print("\nFailed imports details:")
    for fail in failed_imports:
        print(f"Row {fail['row']}: Product {fail['default_code']} - {fail['error']}")