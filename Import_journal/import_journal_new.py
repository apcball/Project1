#!/usr/bin/env python3
import xmlrpc.client
import pandas as pd
from datetime import datetime
import os

# --- Connection Settings ---
url = 'http://119.59.102.189:8069'
db = 'MOG_IMPORT'
username = 'apichart@mogen.co.th'
password = '471109538'

# Function to connect to Odoo
def connect_to_odoo():
    common = xmlrpc.client.ServerProxy(f'{url}/xmlrpc/2/common')
    uid = common.authenticate(db, username, password, {})
    models = xmlrpc.client.ServerProxy(f'{url}/xmlrpc/2/object')
    return uid, models

def read_excel_file():
    # Try to find the Excel file in current directory or Import_journal subdirectory
    file_path = 'import_journal_ค้างจ่าย1.xlsx'
    if not os.path.exists(file_path):
        file_path = 'Import_journal/import_journal_ลูกหนี้อื่น.xlsx'
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Excel file not found at {file_path}")
    
    # Read Excel file first
    df = pd.read_excel(file_path)  # Read all rows
    
    # Print total number of rows for debugging
    print(f"Total rows in Excel: {len(df)}")
    
    # Print column names for debugging
    print("Excel columns:", df.columns.tolist())
    
    # Print first few rows of raw data
    print("\nFirst few rows of raw data:")
    print(df.head().to_string())
    
    # Remove rows where all values are NaN
    df = df.dropna(how='all')
    print(f"Rows after removing empty rows: {len(df)}")
    
    # Rename columns based on their position
    df.columns = [
        'document_number',      # number
        'date',                # accounting Date
        'journal',             # journal
        'reference',           # reference
        'custom_reference',    # custom_referrence
        'account_debit',       # account_debit
        'account_credit',      # account_credit
        'partner_code',        # partner_code
        'old_partner_code',    # old_partner_code
        'partner_name',        # partner_name
        'label',               # label
        'debit',               # debit
        'credit'               # credit
    ]
    
    # Clean up column names by stripping whitespace
    df.columns = df.columns.str.strip()
    
    # Forward fill document_number, date, and journal
    df['document_number'] = df['document_number'].ffill()
    df['date'] = df['date'].ffill()
    df['journal'] = df['journal'].ffill()
    
    # Remove rows where essential columns are NaN
    df = df.dropna(subset=['document_number'], how='any')
    print(f"Rows after removing rows with missing essential data: {len(df)}")
    
    # Convert date column
    df['date'] = pd.to_datetime(df['date'], errors='coerce')
    
    # Clean debit and credit columns
    df['debit'] = pd.to_numeric(df['debit'].fillna(0), errors='coerce').fillna(0)
    df['credit'] = pd.to_numeric(df['credit'].fillna(0), errors='coerce').fillna(0)
    
    # Clean account columns
    df['account_debit'] = df['account_debit'].fillna('').astype(str).str.strip()
    df['account_credit'] = df['account_credit'].fillna('').astype(str).str.strip()
    
    # Clean partner code/name
    df['partner_code'] = df['partner_code'].fillna('').astype(str).str.strip()
    df['old_partner_code'] = df['old_partner_code'].fillna('').astype(str).str.strip()
    df['partner_name'] = df['partner_name'].fillna('').astype(str).str.strip()
    
    # Clean custom_reference
    df['custom_reference'] = df['custom_reference'].fillna('').astype(str).str.strip()
    
    # Clean reference column - handle empty values properly
    df['reference'] = df['reference'].fillna('').astype(str).str.strip()
    
    # Clean document number and ensure it's not empty
    df['document_number'] = df['document_number'].astype(str).str.strip()
    
    # Print sample of document numbers
    print("\nSample of document numbers:")
    print(df['document_number'].head(10))
    
    # Replace 'nan' strings with empty strings
    df = df.replace('nan', '')
    df = df.replace('None', '')
    
    print("\nFirst few rows of processed data:")
    print(df[['document_number', 'date', 'debit', 'credit']].head())
    
    return df

def find_account_by_code(uid, models, account_code):
    if not account_code or pd.isna(account_code) or account_code == 'nan':
        return None
        
    # Clean up account code - extract only the numbers at the start
    account_code = str(account_code).strip()
    import re
    account_code = re.match(r'^\d+', account_code)
    if account_code:
        account_code = account_code.group(0)
    else:
        return None
    
    print(f"Searching for account code: {account_code}")
    account_id = models.execute_kw(db, uid, password,
        'account.account', 'search',
        [[['code', '=', account_code]]])
    
    if account_id:
        account_data = models.execute_kw(db, uid, password,
            'account.account', 'read',
            [account_id[0]], {'fields': ['id', 'name', 'code']})
        print(f"Found account: {account_data[0]}")
        return account_data[0]
    return None

def find_journal_by_code(uid, models, journal_code):
    if not journal_code or pd.isna(journal_code):
        return None
        
    journal_code = str(journal_code).strip()
    print(f"Searching for journal with code: {journal_code}")
    
    # First try exact match
    journal_id = models.execute_kw(db, uid, password,
        'account.journal', 'search',
        [[['code', '=', journal_code]]])
    
    if not journal_id:
        # If not found, try with name
        journal_id = models.execute_kw(db, uid, password,
            'account.journal', 'search',
            [[['name', '=', journal_code]]])
            
    if not journal_id:
        # If still not found, try partial match
        journal_id = models.execute_kw(db, uid, password,
            'account.journal', 'search',
            [[['code', 'ilike', journal_code]]])
            
    if not journal_id:
        # Try partial match with name
        journal_id = models.execute_kw(db, uid, password,
            'account.journal', 'search',
            [[['name', 'ilike', journal_code]]])
    
    if journal_id:
        journal_data = models.execute_kw(db, uid, password,
            'account.journal', 'read',
            [journal_id[0]], {'fields': ['id', 'name', 'code']})
        print(f"Found journal: {journal_data[0]}")
        return journal_data[0]
        
    print(f"No journal found for code: {journal_code}")
    return None

def find_partner_by_code(uid, models, partner_code, old_partner_code=None, partner_name=None):
    # Clean and prepare inputs
    partner_code = str(partner_code).strip() if partner_code and not pd.isna(partner_code) and partner_code != 'nan' and partner_code.strip() else None
    old_partner_code = str(old_partner_code).strip() if old_partner_code and not pd.isna(old_partner_code) and old_partner_code != 'nan' and old_partner_code.strip() else None
    partner_name = str(partner_name).strip() if partner_name and not pd.isna(partner_name) else None
    
    print(f"=== PARTNER SEARCH DEBUG ===")
    print(f"Input values - partner_code: '{partner_code}', old_partner_code: '{old_partner_code}', name: '{partner_name}'")
    
    # First, check if old_code_partner field exists in the database
    try:
        fields_info = models.execute_kw(db, uid, password, 'res.partner', 'fields_get', [], {})
        has_old_code_partner_field = 'old_code_partner' in fields_info
        has_partner_code_field = 'partner_code' in fields_info
        print(f"Database field check - partner_code field exists: {has_partner_code_field}, old_code_partner field exists: {has_old_code_partner_field}")
        if has_old_code_partner_field:
            print(f"old_code_partner field type: {fields_info['old_code_partner'].get('type', 'unknown')}")
    except Exception as e:
        print(f"Error checking database fields: {e}")
        has_old_code_partner_field = False
        has_partner_code_field = True  # Assume it exists based on current code
    
    # First try exact match with partner_code field
    if partner_code and has_partner_code_field:
        print(f"Attempting search: partner_code field = '{partner_code}'")
        partner_id = models.execute_kw(db, uid, password,
            'res.partner', 'search',
            [[['partner_code', '=', partner_code]]])
        
        if partner_id:
            partner_data = models.execute_kw(db, uid, password,
                'res.partner', 'read',
                [partner_id[0]], {'fields': ['id', 'name', 'partner_code', 'old_code_partner']})
            print(f"✓ FOUND partner by partner_code: {partner_data[0]}")
            return partner_data[0]
        else:
            print(f"✗ No match found for partner_code = '{partner_code}'")
    
    # Try exact match with old_partner_code in old_code_partner field (if it exists)
    if old_partner_code and has_old_code_partner_field:
        print(f"Attempting search: old_code_partner field = '{old_partner_code}'")
        partner_id = models.execute_kw(db, uid, password,
            'res.partner', 'search',
            [[['old_code_partner', '=', old_partner_code]]])
        
        if partner_id:
            partner_data = models.execute_kw(db, uid, password,
                'res.partner', 'read',
                [partner_id[0]], {'fields': ['id', 'name', 'partner_code', 'old_code_partner']})
            print(f"✓ FOUND partner by old_code_partner: {partner_data[0]}")
            return partner_data[0]
        else:
            print(f"✗ No match found for old_code_partner = '{old_partner_code}'")
    
    # If not found, try exact match with old_partner_code value in partner_code field
    # (current logic - keeping for comparison)
    if old_partner_code and has_partner_code_field:
        print(f"Attempting search: partner_code field = '{old_partner_code}' (using old_partner_code value)")
        partner_id = models.execute_kw(db, uid, password,
            'res.partner', 'search',
            [[['partner_code', '=', old_partner_code]]])
        
        if partner_id:
            partner_data = models.execute_kw(db, uid, password,
                'res.partner', 'read',
                [partner_id[0]], {'fields': ['id', 'name', 'partner_code', 'old_code_partner']})
            print(f"✓ FOUND partner by old_partner_code (searched in partner_code field): {partner_data[0]}")
            return partner_data[0]
        else:
            print(f"✗ No match found for partner_code = '{old_partner_code}' (using old_partner_code value)")
    
    # If not found by codes, try exact match with name
    if partner_name:
        print(f"Attempting search: name field = '{partner_name}'")
        partner_id = models.execute_kw(db, uid, password,
            'res.partner', 'search',
            [[['name', '=', partner_name]]])
        
        if partner_id:
            partner_data = models.execute_kw(db, uid, password,
                'res.partner', 'read',
                [partner_id[0]], {'fields': ['id', 'name', 'partner_code', 'old_code_partner']})
            print(f"✓ FOUND partner by name: {partner_data[0]}")
            return partner_data[0]
        else:
            print(f"✗ No match found for name = '{partner_name}'")
        
        # Try partial match with name
        print(f"Attempting search: name field ilike '{partner_name}'")
        partner_id = models.execute_kw(db, uid, password,
            'res.partner', 'search',
            [[['name', 'ilike', partner_name]]])
        
        if partner_id:
            partner_data = models.execute_kw(db, uid, password,
                'res.partner', 'read',
                [partner_id[0]], {'fields': ['id', 'name', 'partner_code', 'old_code_partner']})
            print(f"✓ FOUND partner by partial name: {partner_data[0]}")
            return partner_data[0]
        else:
            print(f"✗ No match found for name ilike '{partner_name}'")
    
    print(f"=== PARTNER SEARCH FAILED ===")
    print(f"No partner found with any of the following:")
    print(f"  - partner_code: '{partner_code}'")
    print(f"  - old_partner_code: '{old_partner_code}'")
    print(f"  - name: '{partner_name}'")
    return None

def process_document_group(uid, models, doc_group):
    try:
        if doc_group.empty:
            return False

        first_row = doc_group.iloc[0]
        
        # Find journal
        print(f"\nLooking for journal with code/name: {first_row['journal']}")
        journal = find_journal_by_code(uid, models, first_row['journal'])
        if not journal:
            print(f"Journal not found with code/name: {first_row['journal']}")
            return False

        # Prepare move lines
        move_lines = []
        
        # Process each line in the document group
        for _, row in doc_group.iterrows():
            # Skip rows without both accounts
            if (pd.isna(row['account_debit']) or str(row['account_debit']).strip() == '' or
                pd.isna(row['account_credit']) or str(row['account_credit']).strip() == ''):
                continue

            # Find debit account
            debit_account = find_account_by_code(uid, models, row['account_debit'])
            if not debit_account:
                print(f"Could not find debit account: {row['account_debit']}")
                continue
                
            # Find credit account
            credit_account = find_account_by_code(uid, models, row['account_credit'])
            if not credit_account:
                print(f"Could not find credit account: {row['account_credit']}")
                continue

            # Find partner
            partner = find_partner_by_code(uid, models, row['partner_code'],
                                          row['old_partner_code'], row['partner_name'])

            # Determine amount (use debit if available, otherwise credit)
            amount = float(row['debit']) if not pd.isna(row['debit']) and row['debit'] > 0 else float(row['credit'])
            
            # Create debit line
            debit_line = {
                'account_id': debit_account['id'],
                'name': row['label'] or str(row['document_number']).strip(),
                'debit': amount,
                'credit': 0.0,
            }
            if partner:
                debit_line['partner_id'] = partner['id']
            move_lines.append((0, 0, debit_line))
            
            # Create credit line
            credit_line = {
                'account_id': credit_account['id'],
                'name': row['label'] or str(row['document_number']).strip(),
                'debit': 0.0,
                'credit': amount,
            }
            if partner:
                credit_line['partner_id'] = partner['id']
            move_lines.append((0, 0, credit_line))

        if not move_lines:
            print("No valid lines found for document")
            return False

        # Check if debits and credits balance
        total_debit = sum(line[2]['debit'] for line in move_lines)
        total_credit = sum(line[2]['credit'] for line in move_lines)
        print(f"Total debit: {total_debit}, Total credit: {total_credit}")

        # Prepare move data - leave reference empty if no data
        ref_value = str(first_row['reference']).strip() if first_row['reference'] and str(first_row['reference']).strip() != '' else ''
        
        move_data = {
            'ref': ref_value,  # This will be empty string if no reference data
            'name': str(first_row['document_number']).strip(),
            'date': first_row['date'].strftime('%Y-%m-%d'),
            'journal_id': journal['id'],
            'line_ids': move_lines,
        }
        
        # Add custom_reference if it exists and is not empty
        if first_row['custom_reference'] and str(first_row['custom_reference']).strip() != '':
            move_data['custom_reference'] = str(first_row['custom_reference']).strip()

        print(f"Creating move with ref: {move_data['ref']}")
        
        # Create the move
        move_id = models.execute_kw(db, uid, password,
            'account.move', 'create',
            [move_data])
            
        print(f"Created journal entry with ID: {move_id}")
        return True

    except Exception as e:
        print(f"Error processing document group: {str(e)}")
        return False

def main():
    try:
        # Connect to Odoo
        uid, models = connect_to_odoo()
        print("Successfully connected to Odoo")
        
        # Read Excel file
        df = read_excel_file()
        print("Successfully read Excel file")
        
        # Group by document number
        for doc_number, doc_group in df.groupby('document_number'):
            print(f"\nProcessing document: {doc_number}")
            print(f"Number of lines: {len(doc_group)}")
            
            success = process_document_group(uid, models, doc_group)
            if not success:
                print(f"Failed to create journal entry for document: {doc_number}")
                
    except Exception as e:
        print(f"Error in main function: {str(e)}")

if __name__ == "__main__":
    main()