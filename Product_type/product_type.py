# --- Connection Settings ---
url = 'http://160.187.249.148:8069'
db = 'MOG_LIVE'
username = 'apichart@mogen.co.th'
password = '471109538'

# Data file path
data_file = r'C:\Users\Ball\Documents\Git_apcball\Project1\Product_type\product_type.xlsx'

import xmlrpc.client
import pandas as pd
import sys

def map_detailed_type(value):
    """Map the input value to Odoo's detailed_type values"""
    if pd.isna(value):
        return 'product'  # default

    val = str(value).strip().lower()
    if 'service' in val:
        return 'service'
    elif 'consumable' in val or 'consu' in val:
        return 'consu'
    elif 'storable' in val or 'product' in val:
        return 'product'
    else:
        return 'product'  # default

# --- Authentication ---
try:
    common = xmlrpc.client.ServerProxy(f'{url}/xmlrpc/2/common')
    uid = common.authenticate(db, username, password, {})
    if not uid:
        print("Authentication failed: Check credentials or permissions")
        sys.exit(1)
    else:
        print("Authentication successful, uid =", uid)
except Exception as e:
    print("Error during authentication:", e)
    sys.exit(1)

# --- Create models proxy ---
try:
    models = xmlrpc.client.ServerProxy(f'{url}/xmlrpc/2/object')
except Exception as e:
    print("Error creating XML-RPC models proxy:", e)
    sys.exit(1)

# --- Read data file ---
try:
    df = pd.read_excel(data_file)
    print(f"Loaded {len(df)} rows from {data_file}")
    print("Columns:", list(df.columns))
except Exception as e:
    print(f"Error reading data file: {e}")
    sys.exit(1)

# --- Process each row ---
for index, row in df.iterrows():
    default_code = str(row.get('default_code', '')).strip()
    detailed_type_input = row.get('deteiled_type', '')  # Note: using the column name from file

    if not default_code:
        print(f"Row {index}: Skipping - no default_code")
        continue

    detailed_type = map_detailed_type(detailed_type_input)

    print(f"Processing row {index}: default_code='{default_code}', detailed_type='{detailed_type}'")

    # Search for product by default_code
    product_ids = models.execute_kw(
        db, uid, password,
        'product.template', 'search',
        [[('default_code', '=', default_code)]]
    )

    model_name = 'product.template'
    if not product_ids:
        # Try product.product if not found in template
        product_ids = models.execute_kw(
            db, uid, password,
            'product.product', 'search',
            [[('default_code', '=', default_code)]]
        )
        model_name = 'product.product'

    if not product_ids:
        print(f"  Product with default_code '{default_code}' not found")
        continue

    # Update the product
    try:
        update_data = {'detailed_type': detailed_type, 'type': detailed_type}
        result = models.execute_kw(
            db, uid, password,
            model_name, 'write',
            [product_ids, update_data]
        )
        if result:
            print(f"  Successfully updated product {default_code}")
        else:
            print(f"  Failed to update product {default_code}")
    except Exception as e:
        print(f"  Error updating product {default_code}: {e}")

print("Update process completed.")
