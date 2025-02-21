import xmlrpc.client
import pandas as pd
import sys

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

# --- อ่านข้อมูลจากไฟล์ Excel ---
excel_file = 'Data_file/product_category_import.xlsx'
try:
    # ตรวจสอบชื่อ sheet ทั้งหมดในไฟล์ Excel
    with pd.ExcelFile(excel_file) as xls:
        sheet_names = xls.sheet_names
        print(f"Available sheets in the Excel file: {sheet_names}")
        if 'Sheet1' not in sheet_names:
            print("Sheet named 'Sheet1' not found. Please check the sheet name.")
            sys.exit(1)
    
    df = pd.read_excel(excel_file, sheet_name='Sheet1')  # ระบุชื่อ sheet ที่ต้องการอ่าน
    print(f"Excel file '{excel_file}' read successfully. Number of rows = {len(df)}")
    print(f"Columns in the Excel file: {df.columns.tolist()}")
except Exception as e:
    print("Failed to read Excel file:", e)
    sys.exit(1)

def search_category(category_name):
    """
    ค้นหา Product Category โดยใช้ชื่อ (exact match)
    """
    return models.execute_kw(
        database, uid, password, 'product.category', 'search', [[['name', '=', category_name]]]
    )

def search_parent_category(parent_name):
    """
    ค้นหา Parent Category หากระบุไว้ โดยใช้ชื่อ (exact match)
    """
    if pd.isna(parent_name) or parent_name.strip() == "":
        return None
    parent_ids = models.execute_kw(
        database, uid, password, 'product.category', 'search', [[['name', '=', parent_name.strip()]]]
    )
    return parent_ids[0] if parent_ids else None

# --- วนลูปประมวลผลแต่ละแถวใน Excel ---
for index, row in df.iterrows():
    category_name = str(row['product_category_import']).strip() if pd.notna(row['product_category_import']) else ""
    parent_category_name = row.get('Unnamed: 1', '')

    if not category_name:
        print(f"Row {index}: ชื่อ Category ว่างเปล่า. ข้ามแถวนี้ไป")
        continue

    # ตรวจสอบว่ามี Category นี้อยู่ในระบบหรือไม่
    existing_ids = search_category(category_name)
    if existing_ids:
        print(f"Row {index}: Product Category '{category_name}' มีอยู่แล้ว. ข้ามการสร้าง")
        continue

    # ค้นหา Parent Category หากระบุไว้
    parent_id = search_parent_category(parent_category_name)

    # เตรียมข้อมูลสำหรับสร้าง Category
    category_data = {'name': category_name}
    if parent_id:
        category_data['parent_id'] = parent_id

    # สร้าง Product Category ใน Odoo
    try:
        new_category_id = models.execute_kw(
            database, uid, password, 'product.category', 'create', [category_data]
        )
        print(f"Row {index}: สร้าง Product Category '{category_name}' สำเร็จ (ID: {new_category_id})")
    except Exception as e:
        print(f"Row {index}: ไม่สามารถสร้าง Product Category '{category_name}': {e}")