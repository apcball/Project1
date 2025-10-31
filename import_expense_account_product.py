#!/usr/bin/env python3
"""
Update product expense account from a CSV/Excel file.

Expected input columns (case-insensitive):
- 'defalut_code' (typo), or 'default_code' -- product default code to match
- 'expense account' -- account code to set as expense account

Behavior:
- Finds product by `default_code` first. If not found, will attempt to find by `old_product_code` (if that field exists in the DB).
- Resolves account.account by its `code` and writes to product.template -> `property_account_expense_id`.
- Supports dry-run mode which won't write to Odoo.

Uses Odoo XML-RPC and configuration from `odoo_config.json` if present.
"""
import os
import sys
import json
import logging
import argparse
from datetime import datetime
import getpass

import pandas as pd
import xmlrpc.client


DEFAULT_CONFIG = {
    "odoo": {
        "url": "http://mogth.work:8069",
        "database": "MOG_SETUP",
        "username": "apichart@mogen.co.th",
        "password": "471109538"
    }
}


def load_config(path='odoo_config.json'):
    if os.path.exists(path):
        with open(path, 'r', encoding='utf-8') as f:
            try:
                cfg = json.load(f)
                odoo = cfg.get('odoo')
                # If file contains only placeholder values, treat it as absent
                if odoo:
                    vals = [str(odoo.get(k, '')).lower() for k in ('url', 'database', 'username', 'password')]
                    placeholders = ['your_database_name', 'your_username', 'your_password', 'localhost', '127.0.0.1']
                    if any(p in v for v in vals for p in placeholders):
                        logging.info('odoo_config.json contains placeholder values; ignoring it')
                        return None
                    return odoo
            except Exception:
                logging.warning('Failed to parse odoo_config.json, using defaults')
    return None


def connect(url, db, username, password):
    # Provide clearer errors for common connection/auth issues
    try:
        common = xmlrpc.client.ServerProxy(f"{url}/xmlrpc/2/common")
        uid = common.authenticate(db, username, password, {})
    except ConnectionRefusedError as e:
        raise SystemExit(f"Connection refused when contacting Odoo at {url}. Is the server running and reachable? ({e})")
    except OSError as e:
        raise SystemExit(f"Network error when contacting Odoo at {url}: {e}\nCheck the URL and that the Odoo server is reachable from this machine.")
    except xmlrpc.client.ProtocolError as e:
        raise SystemExit(f"Protocol error when contacting Odoo at {url}: {e}")
    except Exception as e:
        raise SystemExit(f"Failed to connect/authenticate to Odoo: {e}\nCheck your URL, database name, username and password in odoo_config.json or via --config.")

    if not uid:
        raise SystemExit('Authentication to Odoo failed — check database, username and password')

    models = xmlrpc.client.ServerProxy(f"{url}/xmlrpc/2/object")
    return uid, models


def read_input(path):
    if not os.path.exists(path):
        raise SystemExit(f'Input file not found: {path}')

    ext = os.path.splitext(path)[1].lower()
    if ext in ('.xls', '.xlsx'):
        df = pd.read_excel(path, dtype=str)
    else:
        df = pd.read_csv(path, dtype=str)

    # normalize column names to lower-case
    df.columns = [c.strip() for c in df.columns]
    cols_lower = {c.lower(): c for c in df.columns}

    # Accept 'defalut_code' (typo) or 'default_code'
    code_col = None
    for candidate in ('defalut_code', 'default_code'):
        if candidate in cols_lower:
            code_col = cols_lower[candidate]
            break

    if not code_col:
        raise SystemExit("Input must contain a 'defalut_code' or 'default_code' column")

    # Accept 'expense account' or 'property_account_expense_id' or 'expense_account'
    acct_col = None
    for candidate in ('expense account', 'property_account_expense_id', 'expense_account'):
        if candidate in cols_lower:
            acct_col = cols_lower[candidate]
            break

    if not acct_col:
        raise SystemExit("Input must contain an 'expense account' column (or alternate names)")

    # Trim and normalize values
    df = df[[code_col, acct_col]].copy()
    df.columns = ['default_code', 'account_code']
    df['default_code'] = df['default_code'].astype(str).str.strip().replace({'nan': None})
    df['account_code'] = df['account_code'].astype(str).str.strip().replace({'nan': None})

    # normalize empty strings to None
    df['default_code'] = df['default_code'].where(df['default_code'].notnull() & (df['default_code'] != ''), None)
    df['account_code'] = df['account_code'].where(df['account_code'].notnull() & (df['account_code'] != ''), None)

    return df.to_dict('records')


def get_products_by_default_codes(models, db, uid, password, codes):
    if not codes:
        return {}
    recs = models.execute_kw(db, uid, password,
        'product.product', 'search_read',
        [[['default_code', 'in', codes]]],
        {'fields': ['id', 'default_code', 'product_tmpl_id']}
    )
    return {r['default_code']: r['product_tmpl_id'][0] for r in recs if r.get('default_code')}


def get_products_by_old_codes(models, db, uid, password, codes):
    # Attempt to search by a custom field 'old_product_code'
    try:
        recs = models.execute_kw(db, uid, password,
            'product.product', 'search_read',
            [[['old_product_code', 'in', codes]]],
            {'fields': ['id', 'old_product_code', 'product_tmpl_id']}
        )
        return {r['old_product_code']: r['product_tmpl_id'][0] for r in recs if r.get('old_product_code')}
    except xmlrpc.client.Fault as e:
        logging.warning('Field old_product_code not available or search failed: %s', e)
        return {}


def get_account_map(models, db, uid, password, codes):
    if not codes:
        return {}
    recs = models.execute_kw(db, uid, password,
        'account.account', 'search_read',
        [[['code', 'in', codes]]],
        {'fields': ['id', 'code']}
    )
    return {r['code']: r['id'] for r in recs}


def main():
    parser = argparse.ArgumentParser(description='Import expense account for products')
    parser.add_argument('file', nargs='?', default='Data_file/สินค้าระหว่างทาง1.xlsx', help='CSV/XLSX input file')
    parser.add_argument('--dry-run', action='store_true', help="Don't write to Odoo; just report actions")
    parser.add_argument('--config', default='odoo_config.json', help='Path to odoo_config.json')
    # Allow overriding connection details via CLI for quick runs
    parser.add_argument('--url', help='Odoo URL (overrides config)')
    parser.add_argument('--db', help='Odoo database name (overrides config)')
    parser.add_argument('--username', help='Odoo username (overrides config)')
    parser.add_argument('--password', help='Odoo password (overrides config)')
    parser.add_argument('--no-preflight', action='store_true', help='Skip HTTP preflight check (useful if XML-RPC endpoint differs)')
    parser.add_argument('--yes', action='store_true', help='Auto-confirm and apply changes without interactive prompt')
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    cfg = load_config(args.config)

    # Start with DEFAULT_CONFIG values, then overlay file config (if valid), then CLI overrides
    url = DEFAULT_CONFIG['odoo']['url']
    db = DEFAULT_CONFIG['odoo']['database']
    username = DEFAULT_CONFIG['odoo']['username']
    password = DEFAULT_CONFIG['odoo']['password']

    if cfg:
        # cfg is the inner 'odoo' mapping returned by load_config
        url = cfg.get('url') or url
        db = cfg.get('database') or db
        username = cfg.get('username') or username
        password = cfg.get('password') or password

    # CLI overrides (highest precedence)
    if args.url:
        url = args.url
    if args.db:
        db = args.db
    if args.username:
        username = args.username
    if args.password:
        password = args.password

    # If any essential value still looks like a placeholder or is missing, prompt interactively
    def looks_placeholder(v):
        if not v:
            return True
        lv = str(v).lower()
        return any(p in lv for p in ('your_', 'your', 'localhost', '127.0.0.1', 'example'))

    if looks_placeholder(url):
        url = input(f'Odoo URL [{url}]: ') or url
    if looks_placeholder(db):
        db = input(f'Odoo database [{db}]: ') or db
    if looks_placeholder(username):
        username = input(f'Odoo username [{username}]: ') or username
    if not password or looks_placeholder(password):
        password = getpass.getpass('Odoo password (input hidden): ')

    # Optional preflight HTTP check to give clearer troubleshooting feedback before XML-RPC
    if not args.no_preflight:
        try:
            import requests
            try:
                resp = requests.get(url, timeout=5)
                logging.info('Preflight HTTP GET %s -> %s', url, resp.status_code)
            except requests.RequestException as e:
                logging.warning('HTTP preflight to %s failed: %s', url, e)
                logging.info('You can retry with --no-preflight if XML-RPC is hosted differently.')
        except Exception:
            # requests not available or other import issues; skip preflight
            logging.debug('requests module not available, skipping HTTP preflight')

    logging.info('Connecting to Odoo at %s db=%s user=%s', url, db, username)
    uid, models = connect(url, db, username, password)

    data = read_input(args.file)

    codes = list({r['default_code'] for r in data if r['default_code']})
    account_codes = list({r['account_code'] for r in data if r['account_code']})

    logging.info('Looking up %d product codes and %d account codes', len(codes), len(account_codes))

    product_map = get_products_by_default_codes(models, db, uid, password, codes)

    # Find which codes remain not found and try old_product_code
    not_found = [c for c in codes if c not in product_map]
    if not_found:
        logging.info('Attempting lookup by old_product_code for %d codes', len(not_found))
        old_map = get_products_by_old_codes(models, db, uid, password, not_found)
        # merge
        for k, v in old_map.items():
            if k not in product_map:
                product_map[k] = v

    account_map = get_account_map(models, db, uid, password, account_codes)

    success = error = skipped = 0

    # Pre-apply summary and confirmation
    total_rows = len(data)
    rows_with_code = [r for r in data if r.get('default_code')]
    codes_with = {r['default_code'] for r in rows_with_code}
    found_products_count = sum(1 for c in codes_with if c in product_map)
    missing_products = sorted([c for c in codes_with if c not in product_map])
    # accounts referenced but not found
    account_codes_used = {r['account_code'] for r in data if r.get('account_code')}
    missing_accounts = sorted([c for c in account_codes_used if c not in account_map])

    logging.info('Pre-apply summary: total_rows=%d, rows_with_code=%d, products_found=%d, missing_products=%d, missing_accounts=%d',
                 total_rows, len(rows_with_code), found_products_count, len(missing_products), len(missing_accounts))
    if missing_products:
        logging.info('Missing product codes (sample up to 10): %s', ','.join(missing_products[:10]))
    if missing_accounts:
        logging.info('Missing account codes: %s', ','.join(missing_accounts))

    # If performing real updates, ask for confirmation (unless dry-run or --yes)
    if not args.dry_run and not args.yes:
        proceed = input('Proceed to apply updates to Odoo? type YES to continue: ')
        if proceed.strip().upper() != 'YES':
            logging.info('Aborted by user; no changes made')
            return

    for row in data:
        code = row['default_code']
        acc_code = row['account_code']

        if not code:
            skipped += 1
            continue

        tmpl_id = product_map.get(code)
        if not tmpl_id:
            logging.warning("Product not found for code '%s'", code)
            error += 1
            continue

        if not acc_code:
            # Clear account
            if args.dry_run:
                logging.info("DRY RUN: would clear expense account for %s (template %s)", code, tmpl_id)
            else:
                models.execute_kw(db, uid, password, 'product.template', 'write', [[tmpl_id], {'property_account_expense_id': False}])
                logging.info("Cleared expense account for %s", code)
            success += 1
            continue

        account_id = account_map.get(acc_code)
        if not account_id:
            logging.warning("Account code '%s' not found for product '%s'", acc_code, code)
            error += 1
            continue

        if args.dry_run:
            logging.info("DRY RUN: would set product %s (template %s) -> account %s (id %s)", code, tmpl_id, acc_code, account_id)
        else:
            models.execute_kw(db, uid, password, 'product.template', 'write', [[tmpl_id], {'property_account_expense_id': account_id}])
            logging.info("Updated product %s -> account %s", code, acc_code)
        success += 1

    logging.info('\nSummary: total=%d, success=%d, errors=%d, skipped=%d', len(data), success, error, skipped)


if __name__ == '__main__':
    main()
