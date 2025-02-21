import xmlrpc.client
import pandas as pd
import sys
import re

# --- ตั้งค่าการเชื่อมต่อ Odoo ---
url = 'http://mogth.work:8069'
db = 'Test_Module'
username = 'apichart@mogen.co.th'
password = '471109538'

# --- Authenticate ---
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

# --- สร้าง models proxy ---
try:
    models = xmlrpc.client.ServerProxy(f'{url}/xmlrpc/2/object')
except Exception as e:
    print("Error creating XML-RPC models proxy:", e)
    sys.exit(1)

# --- อ่านข้อมูลจากไฟล์ CSV ---
csv_file = 'sale_order_import1.csv'
try:
    df = pd.read_csv(csv_file, encoding='utf-8')
    print(f"CSV file '{csv_file}' read successfully. Number of rows = {len(df)}")
    print("CSV columns:", df.columns.tolist())
except Exception as e:
    print("Failed to read CSV file:", e)
    sys.exit(1)

def search_customer(customer_name):
    """
    ค้นหาลูกค้า (Customer) ใน Odoo โดยใช้การเปรียบเทียบแบบ exact match และ fallback ด้วยการค้นหาแบบ 'ilike'
    """
    customer_name = customer_name.strip()
    customer_ids = models.execute_kw(
        db, uid, password, 'res.partner', 'search',
        [[['name', '=', customer_name]]]
    )
    if not customer_ids:
        customer_ids = models.execute_kw(
            db, uid, password, 'res.partner', 'search',
            [[['name', 'ilike', customer_name]]]
        )
    return customer_ids

def search_product(product_value):
    """
    ค้นหาผลิตภัณฑ์ใน Odoo โดยพิจารณารหัสสินค้าจากรูปแบบ [code] product_name หากมี,
    หากไม่พบจะค้นหาด้วยชื่อสินค้า (exact หรือแบบ 'ilike') เป็น fallback
    """
    product_value = product_value.strip()
    pattern = r"^\[(.*?)\]\s*(.*)"
    match = re.match(pattern, product_value)
    if match:
        product_code = match.group(1).strip()
        product_name = match.group(2).strip()
    else:
        product_code = None
        product_name = product_value

    if product_code:
        # ค้นหาด้วย default_code
        product_ids = models.execute_kw(
            db, uid, password, 'product.product', 'search',
            [[['default_code', '=', product_code]]]
        )
        if not product_ids:
            product_ids = models.execute_kw(
                db, uid, password, 'product.product', 'search',
                [[['name', '=', product_name]]]
            )
            if not product_ids:
                product_ids = models.execute_kw(
                    db, uid, password, 'product.product', 'search',
                    [[['name', 'ilike', product_name]]]
                )
    else:
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

# --- ประมวลผลแต่ละแถวใน CSV เพื่อสร้าง Sale Order ---
for index, row in df.iterrows():
    customer_name = row['Customer']
    order_date = row['Order Date'].strip() if pd.notnull(row['Order Date']) else ''

    # ค้นหาลูกค้า
    customer_ids = search_customer(customer_name)
    if customer_ids:
        customer_id = customer_ids[0]
    else:
        print(f"Customer '{customer_name}' not found at row {index}. Skipping Sale Order creation for this row.")
        continue

    order_line_items = []
    # ค้นหา order line โดยวนลูปคอลัมน์ที่ขึ้นต้นด้วย "order_line/Product"
    for col in df.columns:
        if col.startswith("order_line/Product"):
            suffix = col.replace("order_line/Product", "")
            product_field = row[col]
            if pd.isna(product_field) or product_field.strip() == "":
                continue

            product_ids = search_product(product_field)
            if product_ids:
                product_id = product_ids[0]
            else:
                print(f"Product '{product_field}' not found at row {index} for suffix '{suffix}'. Skipping this order line.")
                continue

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

            order_line_items.append((0, 0, {
                'product_id': product_id,
                'product_uom_qty': quantity,  # สำหรับ Sale Order Line
                'price_unit': unit_price,
            }))

    if not order_line_items:
        print(f"No valid order lines found for row {index}. Skipping Sale Order creation.")
        continue

    # --- เตรียมข้อมูลสำหรับ Sale Order ---
    sale_order_data = {
        'partner_id': customer_id,
        'date_order': order_date,
        'order_line': order_line_items,
    }

    # --- สร้าง Sale Order ใน Odoo ---
    try:
        sale_order_id = models.execute_kw(db, uid, password, 'sale.order', 'create', [sale_order_data])
        print(f"Sale Order created for row {index} with ID: {sale_order_id}")
    except Exception as e:
        print(f"Failed to create Sale Order for row {index}: {e}")