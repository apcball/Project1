
# --- Connection Settings ---
url = 'http://160.187.249.148:8069'
db = 'MOG_LIVE'
username = 'apichart@mogen.co.th'
password = '471109538'

# Data file path
data_file = r'C:\Users\Ball\Documents\Git_apcball\Project1\Fix_service\Product Service.xlsx'

import xmlrpc.client as xmlrpc
import pandas as pd

# Read the Excel file
df = pd.read_excel(data_file)

# Assuming the column is named 'Internal Reference'
internal_refs = df['Internal Reference'].dropna().tolist()

# Connect to Odoo
common = xmlrpc.ServerProxy('{}/xmlrpc/2/common'.format(url))
uid = common.authenticate(db, username, password, {})
if not uid:
    raise Exception("Authentication failed")

models = xmlrpc.ServerProxy('{}/xmlrpc/2/object'.format(url))

# Process each internal reference
for ref in internal_refs:
    ref = str(ref).strip()
    if not ref:
        continue
    print(f"Processing: {ref}")
    try:
        # Search for product.product with matching default_code
        product_ids = models.execute_kw(db, uid, password, 'product.product', 'search', [[['default_code', '=', ref]]])
        if product_ids:
            # Get the product template id
            product = models.execute_kw(db, uid, password, 'product.product', 'read', [product_ids[0]], {'fields': ['product_tmpl_id']})
            tmpl_id = product[0]['product_tmpl_id'][0]
            # Update purchase_method to 'purchase' (On ordered quantities)
            models.execute_kw(db, uid, password, 'product.template', 'write', [[tmpl_id], {'purchase_method': 'purchase'}])
            print(f"Updated product {ref}")
        else:
            print(f"Product not found for {ref}")
    except Exception as e:
        print(f"Error processing {ref}: {e}")
        # Optionally, you can add a delay here
        import time
        time.sleep(1)  # Wait 1 second before next attempt

print("Process completed.")