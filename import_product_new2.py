import xmlrpc.client
import pandas as pd
import sys
import base64
import os
import requests
import time

# --- ตั้งค่าการเชื่อมต่อ Odoo ---
server_url = 'http://mogdev.work:8069'
database = 'KYLD_DEV2'
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
    """ค้นหาหรือสร้าง category จาก path โดยตรวจสอบการซ้ำอย่างละเอียด"""
    if pd.isna(category_path):
        return False
    
    # แยก path และทำความสะอาดข้อมูล
    categories = [cat.strip() for cat in category_path.split('/') if cat.strip()]
    parent_id = False
    current_id = False
    
    for category in categories:
        # ลบช่องว่างซ้ำซ้อนและทำความสะอาดข้อมูล
        clean_category = ' '.join(category.split())
        
        try:
            # 1. ค้นหาด้วย exact match ก่อน
            domain = ['|', '|',
                ('name', '=', clean_category),
                ('name', '=', category.strip()),
                ('complete_name', '=', clean_category)
            ]
            if parent_id:
                domain = ['&', ('parent_id', '=', parent_id)] + domain
            else:
                domain = ['&', ('parent_id', '=', False)] + domain
                
            category_ids = models.execute_kw(
                database, uid, password, 'product.category', 'search',
                [domain]
            )
            
            # 2. ถ้าไม่เจอ ลองค้นหาด้วย case-insensitive และ partial match
            if not category_ids:
                domain = ['|', '|', '|',
                    ('name', '=ilike', clean_category),
                    ('name', 'ilike', clean_category),
                    ('complete_name', 'ilike', clean_category),
                    ('complete_name', '=ilike', clean_category)
                ]
                if parent_id:
                    domain = ['&', ('parent_id', '=', parent_id)] + domain
                else:
                    domain = ['&', ('parent_id', '=', False)] + domain
                    
                category_ids = models.execute_kw(
                    database, uid, password, 'product.category', 'search',
                    [domain]
                )
                
                # 3. ถ้ายังไม่เจอ ลองค้นหาโดยไม่สนใจ parent
                if not category_ids:
                    domain = ['|', '|',
                        ('name', '=ilike', clean_category),
                        ('name', 'ilike', clean_category),
                        ('complete_name', 'ilike', clean_category)
                    ]
                    category_ids = models.execute_kw(
                        database, uid, password, 'product.category', 'search',
                        [domain]
                    )
                    
                    if category_ids:
                        # ถ้าเจอหลายรายการ เลือกรายการที่มี parent ตรงกัน
                        if len(category_ids) > 1 and parent_id:
                            for cat_id in category_ids:
                                cat_info = models.execute_kw(
                                    database, uid, password, 'product.category', 'read',
                                    [cat_id], {'fields': ['parent_id']}
                                )
                                if cat_info and cat_info[0]['parent_id'] and cat_info[0]['parent_id'][0] == parent_id:
                                    category_ids = [cat_id]
                                    break
            
            # 4. ถ้ายังไม่เจอ ให้สร้างใหม่
            if not category_ids:
                vals = {
                    'name': clean_category,
                    'parent_id': parent_id
                }
                current_id = models.execute_kw(
                    database, uid, password, 'product.category', 'create',
                    [vals]
                )
                print(f"Created new category: {clean_category}")
            else:
                current_id = category_ids[0]
            
            parent_id = current_id
            
        except Exception as e:
            print(f"Error processing category {clean_category}: {e}")
            if not ensure_connection():
                return False
            return False
            
        try:
            category_ids = models.execute_kw(
                database, uid, password, 'product.category', 'search', [domain]
            )
        except Exception as e:
            print(f"Error searching category {category}: {e}")
            if not ensure_connection():
                return False
            continue
        if category_ids:
            current_id = category_ids[0]
            parent_id = current_id
        else:
            # If not found, stop and return False
            return False
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
    
    try:
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
            print(f"Warning: UoM '{uom_name}' not found.")
            return False
            
        return uom_ids[0]
    except Exception as e:
        print(f"Error searching UoM: {e}")
        if not ensure_connection():
            return False
        return False

def get_customer_tax():
    """ค้นหา Customer Tax (VAT 7%)"""
    try:
        tax_ids = models.execute_kw(
            database, uid, password, 'account.tax', 'search',
            [[('type_tax_use', '=', 'sale'), ('amount', '=', 7.0), ('name', 'like', '%7%')]]
        )
        if tax_ids:
            return tax_ids[0]
        print("Warning: Customer VAT 7% tax not found")
        return False
    except Exception as e:
        print(f"Error getting customer tax: {e}")
        if not ensure_connection():
            return False
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
            if not os.path.exists(image_path):
                return False, f"Image file not found: {image_path}"
            
            with open(image_path, 'rb') as image_file:
                image_data = base64.b64encode(image_file.read())
                return image_data.decode('utf-8'), None
    except Exception as e:
        return False, f"Error processing image: {str(e)}"

def create_or_update_product(row, mode='create'):
    """สร้างหรืออัพเดทสินค้า"""
    # ตรวจสอบและแปลงข้อมูลเบื้องต้น
    if pd.isna(row['default_code']) or pd.isna(row['name']):
        print("Error: Product code and name are required")
        return False
    
    default_code = str(row['default_code']).strip()
    name = str(row['name']).strip()
    
    if not default_code or not name:
        print("Error: Product code and name cannot be empty")
        return False
    
    try:
        # ค้นหาสินค้าที่มีอยู่แล้ว
        existing_product = models.execute_kw(
            database, uid, password, 'product.template', 'search_read',
            [[('default_code', '=', default_code)]],
            {'fields': ['id']}
        )
    except Exception as e:
        print(f"Error searching product {default_code}: {e}")
        if not ensure_connection():
            return False
        return False
    
    # เตรียมข้อมูลสำหรับสร้าง/อัพเดทสินค้า
    # กำหนด detailed_type จากข้อมูล
    detailed_type_val = (
        'service' if ('detailed_type' in row and not pd.isna(row['detailed_type']) and str(row['detailed_type']).strip().lower() == 'service')
        else 'product' if ('detailed_type' in row and not pd.isna(row['detailed_type']) and str(row['detailed_type']).strip().lower() == 'storable product')
        else 'product'
    )
    vals = {
        'default_code': default_code,
        'name': name,
        'type': detailed_type_val,  # ให้ type ตรงกับ detailed_type
        'detailed_type': detailed_type_val,
        'list_price': 0.0,  # ตั้งค่าราคาขายเป็น 0
        'standard_price': 0.0  # ตั้งค่าต้นทุนเป็น 0
    }
    # กำหนด sale_ok (can be sold)
    if 'sale_ok' in row:
        sale_ok_val = str(row['sale_ok']).strip().upper() if not pd.isna(row['sale_ok']) else ''
        vals['sale_ok'] = True if sale_ok_val == 'TRUE' else False
    # กำหนด can_be_expensed
    if 'can_be_expensed' in row:
        expensed_val = str(row['can_be_expensed']).strip().upper() if not pd.isna(row['can_be_expensed']) else ''
        vals['can_be_expensed'] = True if expensed_val == 'TRUE' else False
    
    # ตั้งค่า category
    if 'categ_id' in row and not pd.isna(row['categ_id']):
        categ_id = search_category(str(row['categ_id']))
        if categ_id:
            vals['categ_id'] = categ_id
    
    # ตั้งค่าหน่วยนับ
    if 'uom_id' in row and not pd.isna(row['uom_id']):
        uom_id = search_uom(str(row['uom_id']))
        if uom_id:
            vals['uom_id'] = uom_id
            vals['uom_po_id'] = uom_id  # ใช้หน่วยเดียวกันสำหรับการซื้อ
    
    # ตั้งค่าภาษีขาย
    customer_tax_id = get_customer_tax()
    if customer_tax_id:
        vals['taxes_id'] = [(6, 0, [customer_tax_id])]
    
    # ตั้งค่ารูปภาพ
    if 'image' in row and not pd.isna(row['image']):
        image_data, error = process_image(str(row['image']))
        if image_data:
            vals['image_1920'] = image_data
        else:
            print(f"Warning: {error}")
    
    try:
        if existing_product:
            # อัพเดทสินค้าที่มีอยู่
            product_id = existing_product[0]['id']
            models.execute_kw(
                database, uid, password, 'product.template', 'write',
                [[product_id], vals]
            )
            print(f"Updated product: {default_code}")
            return product_id
        else:
            # สร้างสินค้าใหม่
            product_id = models.execute_kw(
                database, uid, password, 'product.template', 'create',
                [vals]
            )
            print(f"Created product: {default_code}")
            return product_id
    except Exception as e:
        print(f"Error {'updating' if mode == 'update' else 'creating'} product {default_code}: {e}")
        if not ensure_connection():
            return False
        return False

def process_excel_file(file_path, mode='create', resume_from=0):
    """อ่านไฟล์ Excel และประมวลผลข้อมูลสินค้า"""
    try:
        # อ่านไฟล์ Excel
        df = pd.read_excel(file_path)
        
        # ตรวจสอบคอลัมน์ที่จำเป็น
        required_columns = ['default_code', 'name']
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            print(f"Error: Missing required columns: {', '.join(missing_columns)}")
            return
        
        # ประมวลผลข้อมูลเป็น batch
        total_rows = len(df)
        success_count = 0
        failed_products = []
        
        # สร้างไฟล์ progress
        progress_file = 'import_progress.txt'
        if resume_from > 0:
            print(f"Resuming from row {resume_from}")
        
        for start_idx in range(resume_from, total_rows, BATCH_SIZE):
            end_idx = min(start_idx + BATCH_SIZE, total_rows)
            batch = df.iloc[start_idx:end_idx]
            
            print(f"\nProcessing batch {start_idx//BATCH_SIZE + 1} "
                  f"(rows {start_idx + 1} to {end_idx})...")
            
            # บันทึก progress
            with open(progress_file, 'w') as f:
                f.write(str(start_idx))
            
            batch_retry_count = 0
            max_batch_retries = 3
            
            while batch_retry_count < max_batch_retries:
                if batch_retry_count > 0:
                    print(f"Retrying batch (Attempt {batch_retry_count + 1}/{max_batch_retries})")
                    time.sleep(10)  # รอก่อน retry
                
                # ตรวจสอบการเชื่อมต่อก่อนประมวลผลแต่ละ batch
                if not ensure_connection():
                    print("Failed to maintain connection. Retrying...")
                    batch_retry_count += 1
                    continue
                
                batch_success = True
                for idx, row in batch.iterrows():
                    try:
                        if create_or_update_product(row, mode):
                            success_count += 1
                        else:
                            failed_products.append({
                                'row': idx + 1,
                                'code': row['default_code'],
                                'error': 'Failed to create/update'
                            })
                    except Exception as e:
                        print(f"Row {idx + 1}: Product {row['default_code']} - {str(e)}")
                        failed_products.append({
                            'row': idx + 1,
                            'code': row['default_code'],
                            'error': str(e)
                        })
                        if 'Connection refused' in str(e):
                            batch_success = False
                            break
                
                if batch_success:
                    break
                batch_retry_count += 1
            
            if batch_retry_count >= max_batch_retries:
                print("Max retry attempts reached for batch. Saving progress and failed products...")
                break
            
            print(f"Completed {end_idx}/{total_rows} rows "
                  f"({success_count} successful)")
        
        # บันทึกรายการที่ไม่สำเร็จ
        if failed_products:
            with open('failed_products.txt', 'w') as f:
                for product in failed_products:
                    f.write(f"Row {product['row']}: {product['code']} - {product['error']}\n")
            print(f"\nFailed products have been saved to 'failed_products.txt'")
        
        print(f"\nProcess completed. "
              f"Successfully processed {success_count} out of {total_rows} products.")
        
        if os.path.exists(progress_file):
            os.remove(progress_file)
        
    except Exception as e:
        print(f"Error processing Excel file: {e}")
        if 'failed_products' in locals() and failed_products:
            with open('failed_products.txt', 'w') as f:
                for product in failed_products:
                    f.write(f"Row {product['row']}: {product['code']} - {product['error']}\n")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: <excel_file_path> [mode] [resume_from_row]")
        sys.exit(1)
    
    file_path = sys.argv[1]
    mode = sys.argv[2] if len(sys.argv) > 2 else 'create'
    resume_from = int(sys.argv[3]) if len(sys.argv) > 3 else 0
    
    if mode not in ['create', 'update']:
        print("Invalid mode. Use 'create' or 'update'")
        sys.exit(1)
    
    # ตรวจสอบไฟล์ progress ที่มีอยู่
    progress_file = 'import_progress.txt'
    if os.path.exists(progress_file) and resume_from == 0:
        try:
            with open(progress_file, 'r') as f:
                last_position = int(f.read().strip())
                print(f"Found previous progress. Resume from row {last_position}? (y/n)")
                response = input().lower()
                if response == 'y':
                    resume_from = last_position
        except:
            pass
    
    process_excel_file(file_path, mode, resume_from)

        #ตัวอย่างการใช้งาน:
        #สร้างสินค้าใหม่:
            #python import_product_new2.py products.xlsx create
        #อัพเดทสินค้าที่มีอยู่:
            #python import_product_new2.py products.xlsx update
        #เริ่มจากแถวที่ระบุ:
            #python import_product_new2.py products.xlsx create 1445
