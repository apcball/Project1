import xmlrpc.client
import pandas as pd
import sys

# --- ตั้งค่าการเชื่อมต่อกับ Odoo ---
url = 'http://mogth.work:8069'
db = 'Test_Module'
username = 'apichart@mogen.co.th'
password = '471109538'

# --- Authentication ---
try:
    common = xmlrpc.client.ServerProxy(f'{url}/xmlrpc/2/common')
    uid = common.authenticate(db, username, password, {})
    if not uid:
        print("Authentication failed. ตรวจสอบ credentials หรือ permissions ใน Odoo.")
        sys.exit(1)
    else:
        print("Authentication successful, uid =", uid)
except Exception as e:
    print("Error during authentication:", e)
    sys.exit(1)

# --- สร้าง models proxy สำหรับเรียกใช้งาน Odoo models ---
try:
    models = xmlrpc.client.ServerProxy(f'{url}/xmlrpc/2/object')
except Exception as e:
    print("Error creating XML-RPC models proxy:", e)
    sys.exit(1)

# --- อ่านข้อมูลจากไฟล์ CSV ---
csv_file = 'bom_import.csv'
try:
    df = pd.read_csv(csv_file, encoding='utf-8')
    print(f"CSV file '{csv_file}' read successfully. Number of rows: {len(df)}")
except Exception as e:
    print("Failed to read CSV file:", e)
    sys.exit(1)

def search_product_template(product_name):
    """
    ค้นหาหมายเลข id ของผลิตภัณฑ์หลักใน product.template โดยใช้ชื่อ (exact match)
    """
    product_ids = models.execute_kw(
        db, uid, password,
        'product.template', 'search',
        [[['name', '=', product_name]]]
    )
    return product_ids[0] if product_ids else None

def search_product(product_name):
    """
    ค้นหาหมายเลข id ของผลิตภัณฑ์ส่วนประกอบใน product.product โดยใช้ชื่อ (exact match)
    """
    product_ids = models.execute_kw(
        db, uid, password,
        'product.product', 'search',
        [[['name', '=', product_name]]]
    )
    return product_ids[0] if product_ids else None

# กำหนดรายการ BOM Type ที่อนุญาต
allowed_bom_types = ["Manufacture this product", "Kit", "Subcontracting"]

# --- สร้าง dictionary เพื่อรวบรวมข้อมูล BOM ตามผลิตภัณฑ์ ---
# โครงสร้างคร่าว ๆ:
# {
#   'ชื่อผลิตภัณฑ์หลัก (Product)': {
#       'reference': <string>,
#       'bom_type': <string>,
#       'bom_qty': <float>,
#       'components': [
#           {'name': <component_name>, 'qty': <component_qty>},
#           ...
#       ]
#   },
#   ...
# }
boms_dict = {}

# --- วนลูปประมวลผลข้อมูลแถว CSV แต่ละแถว แล้วเก็บเข้า boms_dict ---
for index, row in df.iterrows():
    main_product_name = str(row.get('Product', '')).strip()
    if not main_product_name:
        print(f"Row {index}: ไม่มีชื่อผลิตภัณฑ์หลัก, ข้ามแถวนี้")
        continue

    # อ่าน Reference (ถ้าไม่มีหรือเป็น NaN ให้เป็นค่าว่าง)
    reference = str(row.get('Reference', '')).strip() if pd.notna(row.get('Reference', '')) else ''

    # อ่าน BOM Type และตรวจสอบให้เป็นหนึ่งในค่า allowed_bom_types
    raw_bom_type = row.get('BOM Type', 'Manufacture this product')
    bom_type = str(raw_bom_type).strip() if pd.notna(raw_bom_type) else 'Manufacture this product'
    if bom_type not in allowed_bom_types:
        print(f"Row {index}: ค่า BOM Type '{bom_type}' ไม่ถูกต้อง, เปลี่ยนเป็น 'Manufacture this product'")
        bom_type = "Manufacture this product"

    # จำนวน BoM ที่ต้องการสร้าง
    try:
        bom_qty = float(row.get('BoM Quantity', 1))
    except Exception:
        bom_qty = 1.0

    # ถ้า product นี้ยังไม่มีใน boms_dict ให้สร้าง key ใหม่
    if main_product_name not in boms_dict:
        boms_dict[main_product_name] = {
            'reference': reference,
            'bom_type': bom_type,
            'bom_qty': bom_qty,
            'components': []
        }
    else:
        # ถ้าพบว่ามีหลายแถวของ product เดียวกัน
        # สมมติว่าให้ใช้ค่า reference, BOM Type, BOM Qty จาก "แถวล่าสุด"
        boms_dict[main_product_name]['reference'] = reference
        boms_dict[main_product_name]['bom_type'] = bom_type
        boms_dict[main_product_name]['bom_qty'] = bom_qty

    # วนหาคอลัมน์ที่เป็น component
    for col in df.columns:
        if col.startswith('Component:'):
            idx = col.split(':')[1].strip()  # เช่น "Component:1" → "1"
            component_name = row.get(col, '')
            if pd.isna(component_name) or not str(component_name).strip():
                continue
            component_name = str(component_name).strip()

            # ระบุคอลัมน์ของจำนวน component (เช่น "Component Quantity:1")
            qty_col = f"Component Quantity:{idx}"
            try:
                component_qty = float(row.get(qty_col, 1))
            except Exception:
                component_qty = 1.0

            if component_name:
                # เก็บ component เข้า list
                boms_dict[main_product_name]['components'].append({
                    'name': component_name,
                    'qty': component_qty
                })

# --- หลังจากรวบรวมข้อมูลจนครบแล้ว ค่อยวนไปสร้าง BOM ใน Odoo ---
for product_name, bom_info in boms_dict.items():
    # ค้นหาหรือสร้าง product.template ของผลิตภัณฑ์หลัก
    main_product_id = search_product_template(product_name)
    if not main_product_id:
        print(f"ไม่พบผลิตภัณฑ์หลัก '{product_name}', ข้ามการสร้าง BOM")
        continue

    # สร้าง BOM lines
    bom_lines = []
    for comp in bom_info['components']:
        component_name = comp['name']
        component_qty = comp['qty']

        # หา product.product ของ component
        component_id = search_product(component_name)
        if not component_id:
            print(f"ไม่พบส่วนประกอบ '{component_name}' ของ '{product_name}', ข้ามรายการนี้")
            continue

        bom_line = (0, 0, {
            'product_id': component_id,
            'product_qty': component_qty
        })
        bom_lines.append(bom_line)

    if not bom_lines:
        print(f"ไม่มีส่วนประกอบสำหรับผลิตภัณฑ์หลัก '{product_name}', ข้ามการสร้าง BOM")
        continue

    # สร้าง BOM (mrp.bom) ใน Odoo
    bom_data = {
        'product_tmpl_id': main_product_id,
        'product_qty': bom_info['bom_qty'],
        'type': bom_info['bom_type'],
        'bom_line_ids': bom_lines,
        # เพิ่ม Reference ลงไปในฟิลด์ 'code'
        'code': bom_info['reference']
    }

    try:
        new_bom_id = models.execute_kw(db, uid, password, 'mrp.bom', 'create', [bom_data])
        print(f"สร้าง BOM (ID: {new_bom_id}) สำหรับผลิตภัณฑ์ '{product_name}' เรียบร้อย "
              f"(Reference: '{bom_info['reference']}', BOM Type: '{bom_info['bom_type']}').")
    except Exception as e:
        print(f"ไม่สามารถสร้าง BOM สำหรับผลิตภัณฑ์ '{product_name}': {e}")