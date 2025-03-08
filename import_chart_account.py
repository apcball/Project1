#!/usr/bin/env python3
import xmlrpc.client
import pandas as pd
from datetime import datetime
import sys
import os
from pathlib import Path

# Odoo connection parameters
url = "http://localhost:8069"
db = "odoo17"
username = "admin"
password = "admin"

# Connect to Odoo
common = xmlrpc.client.ServerProxy('{}/xmlrpc/2/common'.format(url))
uid = common.authenticate(db, username, password, {})
models = xmlrpc.client.ServerProxy('{}/xmlrpc/2/object'.format(url))

def get_account_type(account_type):
    # Map account types to Odoo's internal types
    type_mapping = {
        'receivable': 'asset_receivable',
        'payable': 'liability_payable',
        'bank': 'asset_cash',
        'cash': 'asset_cash',
        'current assets': 'asset_current',
        'non-current assets': 'asset_non_current',
        'prepayments': 'asset_prepayments',
        'fixed assets': 'asset_fixed',
        'current liabilities': 'liability_current',
        'non-current liabilities': 'liability_non_current',
        'equity': 'equity',
        'current year earnings': 'equity_unaffected',
        'income': 'income',
        'other income': 'income_other',
        'expenses': 'expense',
        'other expenses': 'expense_other',
        'cost of revenue': 'expense_direct_cost',
    }
    account_type = str(account_type).lower().strip()
    return type_mapping.get(account_type, 'asset_current')  # default to asset_current if type not found

def import_chart_of_accounts():
    try:
        # Get the current directory
        current_dir = Path(__file__).parent
        
        # Construct the file path
        file_path = current_dir / "For manual import template" / "Chart_Of_Account.xlsx"
        
        print(f"Reading Excel file from: {file_path}")
        
        # Read the Excel file
        df = pd.read_excel(file_path)
        
        # Counter for successful imports
        created_count = 0
        updated_count = 0
        error_count = 0
        
        total_records = len(df)
        print(f"Found {total_records} accounts to process")
        
        # Process each row in the Excel file
        for index, row in df.iterrows():
            try:
                # Convert account code to string and ensure it's not empty
                account_code = str(row['Code']).strip()
                if not account_code:
                    print(f"Skipping row {index + 2}: Empty account code")
                    error_count += 1
                    continue

                # Check if account already exists
                existing_account = models.execute_kw(db, uid, password,
                    'account.account', 'search',
                    [[['code', '=', account_code]]])

                # Prepare account data
                account_data = {
                    'code': account_code,
                    'name': str(row['Name']).strip(),
                    'account_type': get_account_type(row['Account Type']),
                    'reconcile': bool(row.get('Reconcile', False)),
                    'deprecated': bool(row.get('Deprecated', False)),
                }

                if existing_account:
                    # Get existing account data
                    old_data = models.execute_kw(db, uid, password,
                        'account.account', 'read',
                        [existing_account[0]], {'fields': ['name', 'account_type', 'reconcile', 'deprecated']})
                    
                    # Update existing account
                    models.execute_kw(db, uid, password, 'account.account', 'write', [
                        existing_account, account_data])
                    
                    print(f"Updated account: {account_code} - {account_data['name']}")
                    print(f"  Old values: {old_data[0]}")
                    print(f"  New values: {account_data}")
                    updated_count += 1
                else:
                    # Create new account
                    new_id = models.execute_kw(db, uid, password, 'account.account', 'create',
                        [account_data])
                    print(f"Created new account: {account_code} - {account_data['name']}")
                    created_count += 1
                
            except Exception as e:
                print(f"Error processing account {row.get('Code', 'Unknown')}: {str(e)}")
                error_count += 1
                continue
        
        print("\n=== Import Summary ===")
        print(f"Total records processed: {total_records}")
        print(f"New accounts created: {created_count}")
        print(f"Existing accounts updated: {updated_count}")
        print(f"Errors encountered: {error_count}")
        print("===================")
        
    except Exception as e:
        print(f"An error occurred: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    print("Starting Chart of Accounts import...")
    import_chart_of_accounts()