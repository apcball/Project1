import pandas as pd
import xmlrpc.client

def clean_code(val):
    if pd.isna(val):
        return ''
    try:
        return str(int(val)).strip()
    except:
        return str(val).strip()

url = 'http://mogth.work:8069'
db = 'MOG_LIVE'
username = 'apichart@mogen.co.th'
password = '471109538'

common = xmlrpc.client.ServerProxy(f'{url}/xmlrpc/2/common')
uid = common.authenticate(db, username, password, {})
models = xmlrpc.client.ServerProxy(f'{url}/xmlrpc/2/object')

df = pd.read_excel(r'Data_file\update_product_name.xlsx')

for index, row in df.iterrows():
    default_code = clean_code(row.get('default_code', ''))
    new_name = str(row.get('Name', '')).strip()

    print(f"Searching for default_code: '{default_code}'")
    product_ids = models.execute_kw(
        db, uid, password,
        'product.template', 'search',
        [[('default_code', '=', default_code)]]
    )
    print(f"Found product.template IDs: {product_ids}")

    if not product_ids and default_code:
        product_ids = models.execute_kw(
            db, uid, password,
            'product.product', 'search',
            [[('default_code', '=', default_code)]]
        )
        print(f"Found product.product IDs: {product_ids}")

    if product_ids and new_name:
        # อัปเดตชื่อใน product.template ถ้าเจอใน template, ถ้าไม่เจอจะอัปเดตใน product.product
        model_name = 'product.template' if len(product_ids) > 0 else 'product.product'
        models.execute_kw(
            db, uid, password,
            model_name, 'write',
            [product_ids, {'name': new_name}]
        )
        print(f"Updated product: '{default_code}' -> '{new_name}'")
    else:
        print(f"Product not found: {default_code}")