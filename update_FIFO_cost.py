import xmlrpc.client

HOST = 'http://160.187.249.148:8069'
DB = 'MOG_LIVE'
USERNAME = 'apichart@mogen.co.th'
PASSWORD = '471109538'

# Connect to Odoo
common = xmlrpc.client.ServerProxy(f'{HOST}/xmlrpc/2/common')
uid = common.authenticate(DB, USERNAME, PASSWORD, {})
if not uid:
    print("Authentication failed")
    exit(1)

models = xmlrpc.client.ServerProxy(f'{HOST}/xmlrpc/2/object')

# Search all product categories
category_ids = models.execute_kw(DB, uid, PASSWORD, 'product.category', 'search', [[]])
print(f"Found {len(category_ids)} categories.")

# Update all categories to FIFO
count = 0
for cid in category_ids:
    try:
        models.execute_kw(DB, uid, PASSWORD, 'product.category', 'write', [[cid], {'property_cost_method': 'fifo'}])
        count += 1
    except Exception as e:
        print(f"Failed to update category {cid}: {e}")

print(f"Updated {count} categories to FIFO.")
