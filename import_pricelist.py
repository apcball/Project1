import xmlrpc.client
import pandas as pd
import sys

# --- ตั้งค่าการเชื่อมต่อ ---
url = 'http://mogth.work:8069'
db = 'Test_Module'
username = 'apichart@mogen.co.th'
password = '471109538'

# --- ตรวจสอบ Authentication ---
try:
    common = xmlrpc.client.ServerProxy(f'{url}/xmlrpc/2/common')
    uid = common.authenticate(db, username, password, {})
    if not uid:
        print("Authentication failed")
        sys.exit(1)
    else:
        print("Authentication successful, uid =", uid)
except Exception as e:
    print("Authentication error:", e)
    sys.exit(1)

# --- สร้าง XML-RPC proxy สำหรับ models ---
models = xmlrpc.client.ServerProxy(f'{url}/xmlrpc/2/object')

# --- อ่านข้อมูลจาก Excel ---
excel_file = 'pricelist_import.xlsx'
try:
    df = pd.read_excel(excel_file, sheet_name=0)
    print(f"Read {len(df)} rows from file: {excel_file}")
except Exception as e:
    print("Error reading Excel file:", e)
    sys.exit(1)

def search_product_by_default_code(default_code):
    """ ค้นหา Product โดยใช้ Default Code """
    default_code = default_code.strip() if isinstance(default_code, str) else ""
    if not default_code:
        return []
    product_ids = models.execute_kw(
        db, uid, password,
        'product.product', 'search',
        [[['default_code', '=', default_code]]]
    )
    return product_ids

# --- Mapping สำหรับ applied_on (ปรับให้ตรงกับค่า selection ใน Odoo 17) ---
applied_on_mapping = {
    "Products": "1_product",
    "Product": "1_product",
    "Product Variants": "2_product_variant",
    "Variants": "2_product_variant",
    "Product Category": "3_product_category",
    "Product Categories": "3_product_category",
    "All": "0",
}

# --- Process แต่ละแถวใน Excel ---
for index, row in df.iterrows():
    pricelist_name = str(row.get("Pricelist Name", "")).strip()
    if not pricelist_name:
        print(f"แถวที่ {index}: ขาด Pricelist Name, ข้ามแถวนี้")
        continue

    # ค้นหา/สร้าง Pricelist (ตัวอย่างนี้สมมติว่ามี Pricelist แล้ว)
    pricelist_ids = models.execute_kw(
        db, uid, password,
        'product.pricelist', 'search',
        [[['name', '=', pricelist_name]]]
    )
    if pricelist_ids:
        pricelist_id = pricelist_ids[0]
    else:
        pricelist_id = models.execute_kw(
            db, uid, password,
            'product.pricelist', 'create',
            [{'name': pricelist_name}]
        )
        print(f"สร้าง Pricelist ใหม่: {pricelist_name} (ID: {pricelist_id})")

    # ดึงข้อมูลจาก Excel เพื่อสร้าง Pricelist Line
    default_code = str(row.get("Pricelist Items/Product Default Code", "")).strip()
    product_field = str(row.get("Pricelist Items/Product", "")).strip()
    applied_on_input = str(row.get("Pricelist Items/Apply On", "")).strip()
    apply_on = applied_on_mapping.get(applied_on_input, applied_on_input.lower())
    min_qty = row.get("Pricelist Items/Min. Quantity", 0)
    start_date = str(row.get("Pricelist Items/Start Date", "")).strip()
    end_date = str(row.get("Pricelist Items/End Date", "")).strip()
    compute_price = str(row.get("Pricelist Items/Compute Price", "")).strip()
    fixed_price = row.get("Pricelist Items/Fixed Price", 0)
    percentage_price = row.get("Pricelist Items/Percentage Price", 0)
    based_on = str(row.get("Pricelist Items/Based on", "")).strip()

    product_id = False
    if default_code:
        product_ids = search_product_by_default_code(default_code)
        if product_ids:
            product_id = product_ids[0]
        else:
            print(f"แถวที่ {index}: ไม่พบสินค้าสำหรับ Default Code '{default_code}'")
    if not product_id and product_field:
        # fallback: ค้นหาจากชื่อสินค้า (กรณี Default Code ไม่พบ)
        product_ids = models.execute_kw(
            db, uid, password,
            'product.product', 'search',
            [[['name', 'ilike', product_field]]]
        )
        if product_ids:
            product_id = product_ids[0]
        else:
            print(f"แถวที่ {index}: ไม่พบสินค้าสำหรับชื่อสินค้า '{product_field}'")

    if not product_id:
        print(f"แถวที่ {index}: ไม่พบสินค้าเลย จึงข้ามการสร้าง Pricelist Line ครั้งนี้")
        continue

    # เตรียมค่าข้อมูลสำหรับ Pricelist Line
    pricelist_item_data = {
        'pricelist_id': pricelist_id,
        'applied_on': apply_on,
        'product_id': product_id,
        'min_quantity': float(min_qty) if min_qty else 0,
        'date_start': start_date if start_date else False,
        'date_end': end_date if end_date else False,
        'compute_price': compute_price,
        'fixed_price': float(fixed_price) if fixed_price else 0,
        'percent_price': float(percentage_price) if percentage_price else 0,
        'base': based_on,
    }

    try:
        pricelist_item_id = models.execute_kw(
            db, uid, password,
            'product.pricelist.item', 'create',
            [pricelist_item_data]
        )
        print(f"แถวที่ {index}: สร้าง Pricelist Line สำเร็จ (ID: {pricelist_item_id})")
    except Exception as e:
        print(f"แถวที่ {index}: เกิดข้อผิดพลาดในการสร้าง Pricelist Line: {e}")