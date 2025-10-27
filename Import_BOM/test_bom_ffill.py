import pandas as pd

# Import the function under test
from import_bom_new import process_dataframe

# Build sample data resembling the user's attachment
rows = [
    { 'default_dode': '201050138', 'product_quantity': 1, 'type': 'Manufacture this product', 'component_code': '403010021', 'component_old_product_code': '', 'product_qty': 0.13 },
    { 'default_dode': '',         'product_quantity': '', 'type': '',                         'component_code': '403030007', 'component_old_product_code': '', 'product_qty': 0.08 },
    { 'default_dode': '201070028','product_quantity': 1, 'type': 'Manufacture this product', 'component_code': '401040014', 'component_old_product_code': '', 'product_qty': 0.03 },
    { 'default_dode': '',         'product_quantity': '', 'type': '',                         'component_code': '401010013', 'component_old_product_code': '', 'product_qty': 0.05 },
    { 'default_dode': '201080097','product_quantity': 1, 'type': 'Manufacture this product', 'component_code': '401050001', 'component_old_product_code': '', 'product_qty': 0.50 },
    { 'default_dode': '',         'product_quantity': '', 'type': '',                         'component_code': '401040006', 'component_old_product_code': '', 'product_qty': 3.20 },
]

df = pd.DataFrame(rows)

print('DataFrame input:')
print(df)

# Run dry-run processing (no Odoo connection required)
process_dataframe(models=None, uid=None, df=df, dry_run=True)
