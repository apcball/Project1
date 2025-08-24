#!/usr/bin/env python3
import os
import re
import csv
import pandas as pd
import xmlrpc.client

# ==== Odoo Connection ====
URL = 'http://mogth.work:8069'
DB = 'MOG_LIVE'
USERNAME = 'apichart@mogen.co.th'
PASSWORD = '471109538'

# ==== I/O ====
INPUT_PATH = 'Data_file/update_bill_expens_account.xlsx'   # คอลัมน์: Number, expense_account
LOG_DIR = 'Data_file/logs'


def connect():
    common = xmlrpc.client.ServerProxy(f'{URL}/xmlrpc/2/common')
    uid = common.authenticate(DB, USERNAME, PASSWORD, {})
    models = xmlrpc.client.ServerProxy(f'{URL}/xmlrpc/2/object')
    return uid, models


def find_account_by_code(uid, models, account_code):
    """Find account.account id by numeric code prefix"""
    try:
        if not account_code or pd.isna(account_code):
            return None
        code = str(account_code).strip()
        m = re.match(r'^\d+', code)
        if not m:
            return None
        code = m.group(0)
        res = models.execute_kw(DB, uid, PASSWORD,
                                'account.account', 'search_read',
                                [[['code', '=', code]]],
                                {'fields': ['id', 'code', 'name'], 'limit': 1})
        if res:
            return res[0]['id']
        return None
    except Exception as e:
        print(f"Error finding account {account_code}: {e}")
        return None


def update_payable_line(uid, models, move_id, account_id):
    """Update the payable (credit) line account_id for a given move"""
    try:
        # Find credit lines for the move
        lines = models.execute_kw(DB, uid, PASSWORD,
                                  'account.move.line', 'search_read',
                                  [[['move_id', '=', move_id], ['credit', '>', 0]]],
                                  {'fields': ['id', 'account_id', 'credit']})
        if not lines:
            return False, 'no_credit_line_found'
        # Update the first credit line
        line_id = lines[0]['id']
        models.execute_kw(DB, uid, PASSWORD,
                          'account.move.line', 'write',
                          [[line_id], {'account_id': account_id}])
        return True, ''
    except Exception as e:
        return False, str(e)


def ensure_log():
    if not os.path.exists(LOG_DIR):
        os.makedirs(LOG_DIR)
    log_file = os.path.join(LOG_DIR, 'update_vendor_bill_account_log.csv')
    if not os.path.exists(log_file):
        with open(log_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(['Number', 'expense_account', 'status', 'message'])
    return log_file


def log_result(log_file, number, expense_account, status, message=''):
    with open(log_file, 'a', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow([number, expense_account, status, message])


def main():
    print('Starting update_vendor_bill_account')

    if not os.path.exists(INPUT_PATH):
        print(f'Input file not found: {INPUT_PATH}')
        return

    df = pd.read_excel(INPUT_PATH, dtype=str)
    expected_cols = ['Number', 'expense_account']
    for c in expected_cols:
        if c not in df.columns:
            print(f'Missing expected column: {c}')
            return

    uid, models = connect()
    if not uid:
        print('Failed to authenticate to Odoo')
        return

    log_file = ensure_log()

    for idx, row in df.iterrows():
        number = str(row.get('Number')).strip()
        expense_account = str(row.get('expense_account')).strip()

        if not number:
            log_result(log_file, number, expense_account, 'skipped', 'empty_number')
            continue

        try:
            # Find the bill by name and move_type = in_invoice
            bills = models.execute_kw(DB, uid, PASSWORD,
                                      'account.move', 'search_read',
                                      [[['name', '=', number], ['move_type', '=', 'in_invoice']]],
                                      {'fields': ['id', 'state', 'name']})
            if not bills:
                log_result(log_file, number, expense_account, 'not_found', 'no_bill')
                print(f'Bill not found: {number}')
                continue

            bill = bills[0]

            # Find account id by code
            account_id = find_account_by_code(uid, models, expense_account)
            if not account_id:
                log_result(log_file, number, expense_account, 'account_not_found', '')
                print(f'Account not found for code: {expense_account} (Number: {number})')
                continue

            ok, msg = update_payable_line(uid, models, bill['id'], account_id)
            if ok:
                log_result(log_file, number, expense_account, 'updated', '')
                print(f'Updated bill {number} -> account {expense_account}')
            else:
                log_result(log_file, number, expense_account, 'error', msg)
                print(f'Failed to update bill {number}: {msg}')

        except Exception as e:
            log_result(log_file, number, expense_account, 'error', str(e))
            print(f'Exception processing {number}: {e}')

    print('Done')


if __name__ == '__main__':
    main()
