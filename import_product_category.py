import xmlrpc.client
import pandas as pd
import sys

# --- ตั้งค่าการเชื่อมต่อ Odoo ---
server_url = 'http://mogth.work:8069'
database = 'MOG_DEV'
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

def get_cost_method(value):
    """แปลงค่า Costing Method ให้ตรงกับที่ Odoo ต้องการ"""
    if pd.isna(value):
        return 'standard'  # ค่าเริ่มต้น
    
    value = str(value).strip().lower()
    if 'fifo' in value or 'first' in value:
        return 'fifo'
    elif 'average' in value or 'moving' in value or 'เฉลี่ย' in value:
        return 'average'
    else:
        return 'standard'

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
    # ข้ามแถวแรกที่เป็นหัวตาราง
    if index == 0:
        print("ข้ามแถวที่เป็นหัวตาราง")
        continue

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
        
        # เพิ่มการอัพเดท Costing Method, Income Account และ Expense Account
        update_data = {}
        
        # Costing Method - ใช้ค่าจากคอลัมน์ที่ 3 (index 2)
        if pd.notna(row.get('Unnamed: 2')):
            cost_method = str(row['Unnamed: 2']).strip()
            if cost_method.lower() not in ['', 'costingmethod']:  # ข้ามค่าว่างและหัวตาราง
                cost_method_value = get_cost_method(cost_method)
                update_data['property_cost_method'] = cost_method_value
                print(f"Row {index}: กำหนด Costing Method '{cost_method}' -> '{cost_method_value}'")
        
        # Income Account - ใช้ค่าจากคอลัมน์ที่ 4 (index 3)
        if pd.notna(row.get('Unnamed: 3')):
            income_account = str(row['Unnamed: 3']).strip()
            if income_account and income_account.lower() != 'income account':  # ข้ามค่าว่างและหัวตาราง
                income_ids = models.execute_kw(
                    database, uid, password, 'account.account', 'search',
                    [[['code', '=', income_account], ['deprecated', '=', False]]]
                )
                if income_ids:
                    update_data['property_account_income_categ_id'] = income_ids[0]
                    print(f"Row {index}: กำหนดบัญชีรายได้ '{income_account}'")
        
        # Expense Account - ใช้ค่าจากคอลัมน์ที่ 5 (index 4)
        if pd.notna(row.get('Unnamed: 4')):
            expense_account = str(row['Unnamed: 4']).strip()
            if expense_account and expense_account.lower() != 'expense account':  # ข้ามค่าว่างและหัวตาราง
                expense_ids = models.execute_kw(
                    database, uid, password, 'account.account', 'search',
                    [[['code', '=', expense_account], ['deprecated', '=', False]]]
                )
                if expense_ids:
                    update_data['property_account_expense_categ_id'] = expense_ids[0]
                    print(f"Row {index}: กำหนดบัญชีค่าใช้จ่าย '{expense_account}'")
        
        # อัพเดทข้อมูลถ้ามีการเปลี่ยนแปลง
        if update_data:
            models.execute_kw(
                database, uid, password, 'product.category', 'write',
                [[new_category_id], update_data]
            )
            print(f"Row {index}: อัพเดทข้อมูลเพิ่มเติมสำเร็จ")
            
    except Exception as e:
        print(f"Row {index}: ไม่สามารถสร้าง Category '{category_name}': {e}")