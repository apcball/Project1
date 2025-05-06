#!/usr/bin/env python3
import xmlrpc.client
import pandas as pd
from datetime import datetime
import os

# --- Connection Settings ---
url = 'http://mogdev.work:8069'
db = 'MOG_Test'
username = 'apichart@mogen.co.th'
password = '471109538'

# Function to connect to Odoo
def connect_to_odoo():
    common = xmlrpc.client.ServerProxy(f'{url}/xmlrpc/2/common')
    uid = common.authenticate(db, username, password, {})
    models = xmlrpc.client.ServerProxy(f'{url}/xmlrpc/2/object')
    return uid, models

def read_excel_file():
    file_path = 'Data_file/import_journal_ค้างจ่าย.xlsx'
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Excel file not found at {file_path}")
    
    # Read Excel file first
    df = pd.read_excel(file_path, skiprows=2)  # Skip first 2 rows to start from row 3
    
    # Print total number of rows for debugging
    print(f"Total rows in Excel: {len(df)}")
    
    # Print column names for debugging
    print("Excel columns:", df.columns.tolist())
    
    # Remove rows where all values are NaN
    df = df.dropna(how='all')
    print(f"Rows after removing empty rows: {len(df)}")
    
    # Rename columns based on their position
    df.columns = [
        'document_number',    # Column 0: CR18030032
        'date',              # Column 1: 3/31/2018
        'journal',           # Column 2: OB-AP
        'ref',              # Column 3: PRPR00008241
        'debit_account',     # Column 4: 911000 OB-AP
        'credit_account',    # Column 5: 214102 ค่าใช้จ จ่ายค้างจ่าย
        'partner_code',      # Column 6: CSC002
        'partner_name',      # Column 7: CSC COMPLEX CENTER SOLE CO.,LTD.
        'label',            # Column 8: -CSC COMPLEX CENTER SOLE CO.,LTD.
        'amount',           # Column 9: 55549.21
        'notes'             # Column 10: Unnamed
    ]
    
    # Remove rows where essential columns are NaN
    df = df.dropna(subset=['document_number', 'date', 'amount'], how='any')
    print(f"Rows after removing rows with missing essential data: {len(df)}")
    
    
    # Convert date column
    df['date'] = pd.to_datetime(df['date'], errors='coerce')
    
    # Clean amount column (remove any commas and spaces, then convert to float)
    df['amount'] = df['amount'].astype(str).str.replace(',', '').str.replace(' ', '')
    df['amount'] = pd.to_numeric(df['amount'], errors='coerce')
    
    # Clean account columns
    df['debit_account'] = df['debit_account'].astype(str).str.strip()
    df['credit_account'] = df['credit_account'].astype(str).str.strip()
    
    # Clean partner code/name
    df['partner_code'] = df['partner_code'].astype(str).str.strip()
    df['partner_name'] = df['partner_name'].astype(str).str.strip()
    
    # Clean document number
    df['document_number'] = df['document_number'].astype(str).str.strip()
    
    # Replace 'nan' strings with empty strings
    df = df.replace('nan', '')
    df = df.replace('None', '')
    
    print("\nFirst few rows of processed data:")
    print(df[['document_number', 'date', 'amount']].head())
    
    return df

def find_account_by_code(uid, models, account_code):
    if not account_code or pd.isna(account_code):
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

def find_partner_by_code(uid, models, partner_code, partner_name=None):
    if not partner_code or pd.isna(partner_code):
        return None

    partner_code = str(partner_code).strip()
    partner_name = str(partner_name).strip() if partner_name and not pd.isna(partner_name) else None
    print(f"Searching for partner - code: {partner_code}, name: {partner_name}")
    
    # First try exact match with partner_code
    partner_id = models.execute_kw(db, uid, password,
        'res.partner', 'search',
        [[['partner_code', '=', partner_code]]])
    
    if not partner_id and partner_name:
        # If not found by code, try exact match with name
        partner_id = models.execute_kw(db, uid, password,
            'res.partner', 'search',
            [[['name', '=', partner_name]]])
        
        if not partner_id:
            # If still not found, try partial match with name
            partner_id = models.execute_kw(db, uid, password,
                'res.partner', 'search',
                [[['name', 'ilike', partner_name]]])
    
    if partner_id:
        partner_data = models.execute_kw(db, uid, password,
            'res.partner', 'read',
            [partner_id[0]], {'fields': ['id', 'name', 'partner_code']})
        print(f"Found partner: {partner_data[0]}")
        return partner_data[0]
    
    print(f"Partner not found with code: {partner_code} or name: {partner_name}")
    return None

def create_journal_entry(uid, models, entry_data):
    try:
        # Find journal
        print(f"\nLooking for journal with code/name: {entry_data['journal_code']}")
        journal = find_journal_by_code(uid, models, entry_data['journal_code'])
        if not journal:
            print(f"Journal not found with code/name: {entry_data['journal_code']}")
            # List available journals for debugging
            all_journals = models.execute_kw(db, uid, password,
                'account.journal', 'search_read',
                [[]], {'fields': ['name', 'code']})
            print("\nAvailable journals:")
            for j in all_journals:
                print(f"Name: {j['name']}, Code: {j['code']}")
            return False

        # Use document number from Excel as the entry name
        next_number = entry_data.get('document_number', '/')

        # Prepare move lines
        move_lines = []
        
        # Process debit line
        debit_account = find_account_by_code(uid, models, entry_data['debit_account'])
        if not debit_account:
            print(f"Debit account not found: {entry_data['debit_account']}")
            return False
            
        debit_partner = None
        if pd.notna(entry_data.get('debit_partner')):
            debit_partner = find_partner_by_code(uid, models, entry_data['debit_partner'], entry_data.get('partner_name'))
            if not debit_partner:
                print(f"Debit partner not found: {entry_data['debit_partner']}")
                return False

        debit_line = {
            'account_id': debit_account['id'],
            'name': str(entry_data['label']),
            'debit': float(entry_data['amount']),
            'credit': 0.0,
            'partner_id': debit_partner['id'] if debit_partner else False,
        }
        move_lines.append((0, 0, debit_line))

        # Process credit line
        credit_account = find_account_by_code(uid, models, entry_data['credit_account'])
        if not credit_account:
            print(f"Credit account not found: {entry_data['credit_account']}")
            return False
            
        credit_partner = None
        if pd.notna(entry_data.get('credit_partner')):
            credit_partner = find_partner_by_code(uid, models, entry_data['credit_partner'], entry_data.get('partner_name'))
            if not credit_partner:
                print(f"Credit partner not found: {entry_data['credit_partner']}")
                return False

        credit_line = {
            'account_id': credit_account['id'],
            'name': str(entry_data['label']),
            'debit': 0.0,
            'credit': float(entry_data['amount']),
            'partner_id': credit_partner['id'] if credit_partner else False,
        }
        move_lines.append((0, 0, credit_line))

        # Create journal entry
        move_vals = {
            'move_type': 'entry',  # Specify that this is a journal entry
            'journal_id': journal['id'],
            'date': entry_data['date'],
            'ref': str(entry_data['ref']) if pd.notna(entry_data.get('ref')) else '',
            'name': next_number,  # Use journal sequence or document number
            'line_ids': move_lines,
            'state': 'draft',  # Create in draft state
        }

        # Create the journal entry
        move_id = models.execute_kw(db, uid, password,
            'account.move', 'create',
            [move_vals])

        if move_id:
            # Verify the creation
            move_data = models.execute_kw(db, uid, password,
                'account.move', 'read',
                [move_id], {'fields': ['name', 'state', 'amount_total']})
            
            if move_data:
                print(f"Successfully created journal entry:")
                print(f"- ID: {move_id}")
                print(f"- Document Number: {move_data[0]['name']}")
                print(f"- State: {move_data[0]['state']}")
                print(f"- Amount: {move_data[0]['amount_total']}")
                return move_id
            else:
                print("Warning: Entry created but could not verify details")
                return move_id
        else:
            print("Failed to create journal entry")
            return False

    except Exception as e:
        print(f"Error creating journal entry: {str(e)}")
        return False

def main():
    try:
        # Connect to Odoo
        uid, models = connect_to_odoo()
        print("Successfully connected to Odoo")
        
        # Read Excel file
        df = read_excel_file()
        print("Successfully read Excel file\n")
        
        # Process rows 3-6
        for index in range(3, 7):
            print(f"\nProcessing row {index}:")
            try:
                row = df.iloc[index - 3]  # Adjust index since we skipped 2 rows
                
                # Convert date
                entry_date = row['date'].strftime('%Y-%m-%d')
                print(f"Date from Excel: {row['date']}")
                print(f"Converted date: {entry_date}")
                
                # Prepare entry data
                entry_data = {
                    'date': entry_date,
                    'journal_code': str(row['journal']).strip(),
                    'ref': str(row['ref']).strip(),
                    'document_number': str(row['document_number']).strip(),
                    'debit_account': str(row['debit_account']).strip(),
                    'credit_account': str(row['credit_account']).strip(),
                    'debit_partner': str(row['partner_code']).strip(),
                    'credit_partner': str(row['partner_code']).strip(),
                    'partner_name': str(row['partner_name']).strip(),
                    'label': str(row['label']).strip(),
                    'amount': abs(float(row['amount'])),
                }
                
                print(f"Entry data prepared: {entry_data}")
                
                # Create journal entry
                if create_journal_entry(uid, models, entry_data):
                    print(f"Successfully processed row {index}")
                else:
                    print(f"Failed to process row {index}")
                    
            except Exception as e:
                print(f"Error processing row {index}: {str(e)}")
                continue
            
    except Exception as e:
        print(f"Error in main: {str(e)}")

if __name__ == "__main__":
    main()