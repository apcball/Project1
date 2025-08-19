#!/usr/bin/env python3
import xmlrpc.client
import pandas as pd
from datetime import datetime
import logging
import sys

# --- Logging ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename=f'account_expense_import_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'
)
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(message)s')
console_handler.setFormatter(formatter)
logging.getLogger().addHandler(console_handler)

# --- Connection ---
url = 'http://mogth.work:8069'
db = 'MOG_LIVE'
username = 'apichart@mogen.co.th'
password = '471109538'

def connect_to_odoo():
    common = xmlrpc.client.ServerProxy(f'{url}/xmlrpc/2/common')
    uid = common.authenticate(db, username, password, {})
    if not uid:
        raise Exception("Authentication failed")
    models = xmlrpc.client.ServerProxy(f'{url}/xmlrpc/2/object')
    return uid, models

def read_excel_data(file_path):
    df = pd.read_excel(file_path)
    df['default_code'] = df['default_code'].where(df['default_code'].notna(), None).astype(str).str.strip()
    df['account_code'] = df['property_account_expense_id'].where(df['property_account_expense_id'].notna(), None).astype(str).str.strip()
    return df.to_dict('records')

def main():
    uid, models = connect_to_odoo()

    file_path = 'Data_file/import_account_expense_id.xlsx'
    data = read_excel_data(file_path)

    # --- เตรียม set สำหรับค้นหา ---
    default_codes = list({row['default_code'] for row in data if row['default_code']})
    account_codes = list({row['account_code'] for row in data if row['account_code']})

    # --- ดึง product id ตาม default_code ---
    product_records = models.execute_kw(db, uid, password,
        'product.product', 'search_read',
        [[['default_code', 'in', default_codes]]],
        {'fields': ['id', 'default_code', 'product_tmpl_id']}
    )
    product_map = {p['default_code']: p['product_tmpl_id'][0] for p in product_records}

    # --- ดึง account id ตาม code ---
    account_records = models.execute_kw(db, uid, password,
        'account.account', 'search_read',
        [[['code', 'in', account_codes]]],
        {'fields': ['id', 'code']}
    )
    account_map = {a['code']: a['id'] for a in account_records}

    success_count = error_count = skipped_count = 0

    for row in data:
        code = row['default_code']
        acc_code = row['account_code']

        if not code:
            skipped_count += 1
            continue

        if code not in product_map:
            logging.warning(f"Product '{code}' not found")
            error_count += 1
            continue

        tmpl_id = product_map[code]

        if not acc_code:
            # clear expense account
            models.execute_kw(db, uid, password,
                'product.template', 'write',
                [[tmpl_id], {'property_account_expense_id': False}]
            )
            logging.info(f"Cleared expense account for '{code}'")
            success_count += 1
            continue

        if acc_code not in account_map:
            logging.warning(f"Account '{acc_code}' not found")
            error_count += 1
            continue

        models.execute_kw(db, uid, password,
            'product.template', 'write',
            [[tmpl_id], {'property_account_expense_id': account_map[acc_code]}]
        )
        logging.info(f"Updated '{code}' -> account '{acc_code}'")
        success_count += 1

    logging.info(f"""
Import Summary:
--------------
Total: {len(data)}
Success: {success_count}
Errors: {error_count}
Skipped: {skipped_count}
""")

if __name__ == '__main__':
    main()
