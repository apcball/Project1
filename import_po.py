import xmlrpc.client
import pandas as pd
import sys
import re

# --- Connection Settings ---
url = 'http://mogth.work:8069'
db = 'Test_Module'
username = 'apichart@mogen.co.th'
password = '471109538'

# --- Authenticate with Odoo ---
try:
    common = xmlrpc.client.ServerProxy(f'{url}/xmlrpc/2/common')
    uid = common.authenticate(db, username, password, {})
    if not uid:
        print("Authentication failed: invalid credentials or insufficient permissions.")
        sys.exit(1)
    else:
        print("Authentication successful, uid =", uid)
except Exception as e:
    print("Error during connection/authentication:", e)
    sys.exit(1)

# --- Create XML-RPC models proxy ---
try:
    models = xmlrpc.client.ServerProxy(f'{url}/xmlrpc/2/object')
except Exception as e:
    print("Error creating XML-RPC models proxy:", e)
    sys.exit(1)

# --- Read CSV File ---
csv_file = 'purchase_order_import1.csv'
try:
    # สำหรับรองรับภาษาไทย ให้ระบุ encoding ตามที่เหมาะสม (ในที่นี้ใช้ 'utf-8')
    df = pd.read_csv(csv_file, encoding='utf-8')
    print(f"CSV file '{csv_file}' read successfully. Number of rows = {len(df)}")
    print("CSV columns:", df.columns.tolist())
except Exception as e:
    print("Failed to read CSV file:", e)
    sys.exit(1)

def search_vendor(vendor_name):
    """
    ค้นหา vendor โดยใช้การแมตช์แบบ exact และ fallback ด้วย 'ilike'
    """
    vendor_name = vendor_name.strip()
    vendor_ids = models.execute_kw(
        db, uid, password, 'res.partner', 'search',
        [[['name', '=', vendor_name]]]
    )
    if not vendor_ids:
        # ลองค้นหาแบบ case-insensitive หากไม่พบ exact match
        vendor_ids = models.execute_kw(
            db, uid, password, 'res.partner', 'search',
            [[['name', 'ilike', vendor_name]]]
        )
    return vendor_ids

def search_product(product_value):
    """
    ค้นหาผลิตภัณฑ์ใน Odoo โดยถ้าใน product_value มีรูปแบบ [code] name
    ให้แยก default_code และ name เพื่อนำไปค้นหาก่อน โดยใช้ fallback กับการค้นหาด้วย name
    """
    product_value = product_value.strip()
    # ตรวจสอบว่าฟิลด์มีรูปแบบ "[default_code] product_name" หรือไม่
    pattern = r"^\[(.*?)\]\s*(.*)"
    match = re.match(pattern, product_value)
    if match:
        product_code = match.group(1).strip()
        product_name = match.group(2).strip()
    else:
        product_code = None
        product_name = product_value

    if product_code:
        # ลองค้นหาด้วย default_code ก่อน
        product_ids = models.execute_kw(
            db, uid, password, 'product.product', 'search',
            [[['default_code', '=', product_code]]]
        )
        if not product_ids:
            # หากไม่พบด้วย default_code ให้ลองค้นหาจาก field 'name'
            product_ids = models.execute_kw(
                db, uid, password, 'product.product', 'search',
                [[['name', '=', product_name]]]
            )
            if not product_ids:
                # fallback ด้วยการค้นหาแบบ ilike
                product_ids = models.execute_kw(
                    db, uid, password, 'product.product', 'search',
                    [[['name', 'ilike', product_name]]]
                )
    else:
        # หากไม่มี default_code ให้ค้นหาจาก field 'name'
        product_ids = models.execute_kw(
            db, uid, password, 'product.product', 'search',
            [[['name', '=', product_name]]]
        )
        if not product_ids:
            product_ids = models.execute_kw(
                db, uid, password, 'product.product', 'search',
                [[['name', 'ilike', product_name]]]
            )

    return product_ids

# --- Process Each Row as a Purchase Order ---
for index, row in df.iterrows():
    vendor_name = row['Vendor']
    order_date = row['Order Date'].strip() if pd.notnull(row['Order Date']) else ''

    # --- Search for Vendor ---
    vendor_ids = search_vendor(vendor_name)
    if vendor_ids:
        vendor_id = vendor_ids[0]
    else:
        print(f"Vendor '{vendor_name}' not found at row {index}. Skipping Purchase Order creation for this row.")
        continue

    order_line_items = []
    # --- Process Order Lines ---
    # วนลูปคอลัมน์ที่ขึ้นต้นด้วย "order_line/Product" (รวมถึงมี suffix ":2", ":3" ฯลฯ)
    for col in df.columns:
        if col.startswith("order_line/Product"):
            # Suffix จะเป็น '' หรือ ':2', ':3',...
            suffix = col.replace("order_line/Product", "")
            product_field = row[col]
            if pd.isna(product_field) or product_field.strip() == "":
                continue  # ข้าม order line ที่ไม่มีข้อมูลผลิตภัณฑ์

            # --- Search for Product ---
            product_ids = search_product(product_field)
            if product_ids:
                product_id = product_ids[0]
            else:
                print(f"Product '{product_field}' not found at row {index} for suffix '{suffix}'. Skipping this order line.")
                continue

            # ดึงข้อมูลปริมาณและราคาต่อหน่วยด้วย suffix เดียวกัน
            quantity_col = "order_line/Quantity" + suffix
            unit_price_col = "order_line/Unit Price" + suffix
            if quantity_col not in df.columns or unit_price_col not in df.columns:
                print(f"Missing quantity or unit price column for suffix '{suffix}' at row {index}. Skipping this order line.")
                continue

            quantity = row[quantity_col]
            unit_price = row[unit_price_col]
            if pd.isna(quantity) or pd.isna(unit_price):
                print(f"Incomplete order line data at row {index} for suffix '{suffix}'. Skipping this order line.")
                continue

            # เพิ่ม order line ลงใน list ในรูปแบบที่ Odoo ต้องการ [(0, 0, {...})]
            order_line_items.append((0, 0, {
                'product_id': product_id,
                'product_qty': quantity,
                'price_unit': unit_price,
            }))

    if not order_line_items:
        print(f"No valid order lines found for row {index}. Skipping Purchase Order creation.")
        continue

    # --- Prepare Purchase Order Data ---
    po_data = {
        'partner_id': vendor_id,
        'date_order': order_date,  # กำหนดฟิลด์วันที่ หาก field ใน Odoo แตกต่างออกไปให้ปรับแก้
        'order_line': order_line_items,
    }

    # --- Create Purchase Order in Odoo ---
    try:
        po_id = models.execute_kw(db, uid, password, 'purchase.order', 'create', [po_data])
        print(f"Purchase Order created for row {index} with ID: {po_id}")
    except Exception as e:
        print(f"Failed to create Purchase Order for row {index}: {e}")