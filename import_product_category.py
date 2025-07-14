import xmlrpc.client
import pandas as pd
import sys

# --- ตั้งค่าการเชื่อมต่อ Odoo ---
server_url = 'http://mogdev.work:8069'
database = 'KYLD_DEV'
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

def search_account(account_code):
    """
    ค้นหาบัญชีจากรหัสบัญชี
    """
    if pd.isna(account_code) or str(account_code).strip() == "":
        return None
    account_code = str(account_code).strip()
    account_ids = models.execute_kw(
        database, uid, password, 'account.account', 'search',
        [[['code', '=', account_code], ['deprecated', '=', False]]]
    )
    return account_ids[0] if account_ids else None

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
    parent_name = parent_name.strip()
    parent_ids = models.execute_kw(
        database, uid, password, 'product.category', 'search', [[['name', '=', parent_name]]]
    )
    if parent_ids:
        print(f"พบ Parent Category '{parent_name}' (ID: {parent_ids[0]})")
        return parent_ids[0]
    else:
        print(f"ไม่พบ Parent Category '{parent_name}'")
        return None

def update_category_accounts(category_id, income_account_code, expense_account_code):
    """
    อัพเดทบัญชีรายได้และค่าใช้จ่ายของ Product Category
    """
    update_data = {}
    
    # Income Account
    if income_account_code:
        income_account_id = search_account(income_account_code)
        if income_account_id:
            update_data['property_account_income_categ_id'] = income_account_id
            print(f"กำหนดบัญชีรายได้ '{income_account_code}'")
        else:
            print(f"ไม่พบบัญชีรายได้รหัส '{income_account_code}'")
    
    # Expense Account
    if expense_account_code:
        expense_account_id = search_account(expense_account_code)
        if expense_account_id:
            update_data['property_account_expense_categ_id'] = expense_account_id
            print(f"กำหนดบัญชีค่าใช้จ่าย '{expense_account_code}'")
        else:
            print(f"ไม่พบบัญชีค่าใช้จ่ายรหัส '{expense_account_code}'")
    
    if update_data:
        try:
            models.execute_kw(
                database, uid, password, 'product.category', 'write',
                [[category_id], update_data]
            )
            print("อัพเดทข้อมูลบัญชีสำเร็จ")
            return True
        except Exception as e:
            print(f"ไม่สามารถอัพเดทข้อมูลบัญชี: {e}")
            return False
    return True

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
    
    df = pd.read_excel(excel_file, sheet_name='Sheet1')
    df.columns = df.columns.str.strip()  # ลบช่องว่างที่อาจมีในชื่อคอลัมน์
    print(f"Excel file '{excel_file}' read successfully. Number of rows = {len(df)}")
    print(f"Columns in the Excel file: {df.columns.tolist()}")
    
    # ตรวจสอบและแสดงข้อมูลแต่ละแถวเพื่อการ debug
    print("\nตรวจสอบข้อมูล 5 แถวแรก:")
    print(df.head().to_string())
except Exception as e:
    print("Failed to read Excel file:", e)
    sys.exit(1)

# --- วนลูปประมวลผลแต่ละแถวใน Excel ---
for index, row in df.iterrows():
    # ข้ามแถวแรกที่เป็นหัวตาราง
    if index == 0:
        print("\nข้ามแถวที่เป็นหัวตาราง")
        continue

    # ตรวจสอบชื่อคอลัมน์ที่ถูกต้อง
    name_column = 'Name' if 'Name' in df.columns else 'product_category_import'
    parent_column = 'Parent Category' if 'Parent Category' in df.columns else 'Unnamed: 1'
    costing_column = 'CostingMethod' if 'CostingMethod' in df.columns else 'Unnamed: 2'
    
    category_name = str(row[name_column]).strip() if pd.notna(row[name_column]) else ""
    parent_category_name = str(row[parent_column]).strip() if pd.notna(row[parent_column]) else ""
    income_account = str(row.get('Unnamed: 3', '')).strip() if pd.notna(row.get('Unnamed: 3')) else None
    expense_account = str(row.get('Unnamed: 4', '')).strip() if pd.notna(row.get('Unnamed: 4')) else None

    if not category_name:
        print(f"\nRow {index}: ชื่อ Category ว่างเปล่า. ข้ามแถวนี้ไป")
        continue

    print(f"\nกำลังประมวลผล Row {index}:")
    print(f"Category Name: '{category_name}'")
    print(f"Parent Category: '{parent_category_name}'")

    # ตรวจสอบว่ามี Category นี้อยู่ในระบบหรือไม่
    existing_ids = search_category(category_name)
    
    if existing_ids:
        category_id = existing_ids[0]
        print(f"Product Category '{category_name}' มีอยู่แล้ว (ID: {category_id})")
        
        # อัพเดทข้อมูลเพิ่มเติม
        update_data = {}
        
        # อัพเดท Parent Category ถ้ามี
        if parent_category_name:
            parent_id = search_parent_category(parent_category_name)
            if parent_id:
                update_data['parent_id'] = parent_id
                print(f"อัพเดท Parent Category เป็น '{parent_category_name}'")
        
        # อัพเดท Costing Method ถ้ามีระบุ
        if pd.notna(row.get(costing_column)):
            cost_method = str(row[costing_column]).strip()
            if cost_method.lower() not in ['', 'costingmethod']:
                cost_method_value = get_cost_method(cost_method)
                update_data['property_cost_method'] = cost_method_value
                print(f"กำหนด Costing Method '{cost_method}' -> '{cost_method_value}'")
        
        # อัพเดทข้อมูลถ้ามีการเปลี่ยนแปลง
        if update_data:
            try:
                models.execute_kw(
                    database, uid, password, 'product.category', 'write',
                    [[category_id], update_data]
                )
                print("อัพเดทข้อมูล Category สำเร็จ")
            except Exception as e:
                print(f"ไม่สามารถอัพเดทข้อมูล Category: {e}")
        
        # อัพเดทข้อมูลบัญชี
        if income_account or expense_account:
            update_category_accounts(category_id, income_account, expense_account)
        
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
        print(f"สร้าง Product Category '{category_name}' สำเร็จ (ID: {new_category_id})")
        
        # อัพเดทข้อมูลเพิ่มเติม
        update_data = {}
        
        # Costing Method
        if pd.notna(row.get('Unnamed: 2')):
            cost_method = str(row['Unnamed: 2']).strip()
            if cost_method.lower() not in ['', 'costingmethod']:
                cost_method_value = get_cost_method(cost_method)
                update_data['property_cost_method'] = cost_method_value
                print(f"กำหนด Costing Method '{cost_method}' -> '{cost_method_value}'")
        
        # อัพเดทข้อมูลถ้ามีการเปลี่ยนแปลง
        if update_data:
            models.execute_kw(
                database, uid, password, 'product.category', 'write',
                [[new_category_id], update_data]
            )
            print("อัพเดท Costing Method สำเร็จ")
        
        # อัพเดทข้อมูลบัญชี
        update_category_accounts(new_category_id, income_account, expense_account)
            
    except Exception as e:
        print(f"ไม่สามารถสร้าง Category '{category_name}': {e}")

print("\nเสร็จสิ้นการประมวลผลทั้งหมด")