#!/usr/bin/env python3
import xmlrpc.client
import pandas as pd
from datetime import datetime
import os

# --- Connection Settings ---
url = 'http://mogth.work:8069'
db = 'MOG_LIVE'
username = 'apichart@mogen.co.th'
password = '471109538'

# Function to connect to Odoo
def connect_to_odoo():
    common = xmlrpc.client.ServerProxy(f'{url}/xmlrpc/2/common')
    uid = common.authenticate(db, username, password, {})
    models = xmlrpc.client.ServerProxy(f'{url}/xmlrpc/2/object')
    return uid, models

def read_excel_file():
    file_path = 'Data_file/import_invoice_CN.xlsx'
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Excel file not found at {file_path}")
    return pd.read_excel(file_path)

def get_or_create_partner(uid, models, partner_code, partner_name):
    try:
        # Clean partner code and name
        partner_code = str(partner_code).strip()
        partner_name = str(partner_name).strip()
        
        # Search for existing partner using partner_code, old_code_partner, or name
        partner = models.execute_kw(db, uid, password,
            'res.partner', 'search_read',
            [[
                '|', '|',
                ('partner_code', '=', partner_code),
                ('old_code_partner', '=', partner_code),
                ('name', '=', partner_name)
            ]],
            {'fields': ['id', 'name', 'partner_code', 'old_code_partner']})
        
        if partner:
            found_partner = partner[0]
            if found_partner.get('partner_code') == partner_code:
                print(f"Found existing partner by code {partner_code}: {found_partner['name']}")
            else:
                print(f"Found existing partner by name: {found_partner['name']}")
            return found_partner['id']

        # If partner not found, create new partner
        new_partner_vals = {
            'name': partner_name,
            'partner_code': partner_code,
            'customer_rank': 1,  # Mark as customer
            'company_type': 'company',  # Set as company by default
        }
        
        new_partner_id = models.execute_kw(db, uid, password,
            'res.partner', 'create',
            [new_partner_vals])
        
        print(f"Created new partner: {partner_name} (Code: {partner_code}) with ID: {new_partner_id}")
        return new_partner_id

    except Exception as e:
        print(f"Error in partner creation: {str(e)}")
        return False

def find_product_by_code(uid, models, default_code):
    if pd.isna(default_code):
        return None
        
    default_code = str(default_code).strip()
    product_id = models.execute_kw(db, uid, password,
        'product.product', 'search',
        [[['default_code', '=', default_code]]])
    
    if product_id:
        product_data = models.execute_kw(db, uid, password,
            'product.product', 'read',
            [product_id[0]], {'fields': ['id', 'name']})
        return product_data[0]
    return None

def find_existing_credit_note(uid, models, document_number):
    if not document_number:
        return None
    
    # Search for existing credit note with the same name (document number)
    credit_note_ids = models.execute_kw(db, uid, password,
        'account.move', 'search_read',
        [[['name', '=', document_number], ['move_type', '=', 'out_refund']]],
        {'fields': ['id', 'state']})
    
    return credit_note_ids[0] if credit_note_ids else None

def find_journal_by_code(uid, models, journal_code):
    if not journal_code or pd.isna(journal_code):
        return None
        
    journal_code = str(journal_code).strip()
    journal_id = models.execute_kw(db, uid, password,
        'account.journal', 'search',
        [[['code', '=', journal_code]]])
    
    if journal_id:
        journal_data = models.execute_kw(db, uid, password,
            'account.journal', 'read',
            [journal_id[0]], {'fields': ['id', 'name']})
        return journal_data[0]
    return None

def update_or_create_credit_note(uid, models, credit_note_data):
    try:
        # Check if credit note already exists
        existing_credit_note = find_existing_credit_note(uid, models, credit_note_data['document_number'])
        
        # Get or create partner
        partner_id = get_or_create_partner(uid, models, credit_note_data['partner_code'], credit_note_data['partner_name'])
        if not partner_id:
            print("Failed to get or create partner")
            return False

        # Find product by default_code
        product = find_product_by_code(uid, models, credit_note_data['default_code'])
        if not product:
            print(f"Product not found with code: {credit_note_data['default_code']}")
            return False

        # Find journal by code
        journal = find_journal_by_code(uid, models, credit_note_data['journal'])
        if not journal:
            print(f"Journal not found with code: {credit_note_data['journal']}")
            return False

        # Prepare credit note line
        credit_note_line = {
            'product_id': product['id'],
            'name': product['name'],
            'quantity': 1,  # Default quantity to 1 if not specified
            'price_unit': abs(float(credit_note_data['price_unit'])),  # Ensure price is positive
        }

        if existing_credit_note:
            print(f"Found existing credit note with number: {credit_note_data['document_number']}")
            
            # Check if credit note is in draft state
            if existing_credit_note['state'] != 'draft':
                print(f"Cannot update credit note {credit_note_data['document_number']} as it is not in draft state")
                return False

            # Update existing credit note
            # First, delete existing lines
            models.execute_kw(db, uid, password,
                'account.move.line', 'unlink',
                [models.execute_kw(db, uid, password,
                    'account.move.line', 'search',
                    [[['move_id', '=', existing_credit_note['id']], ['product_id', '!=', False]]])])

            # Update credit note fields
            update_vals = {
                'partner_id': partner_id,
                'invoice_date': credit_note_data['invoice_date'],
                'payment_reference': credit_note_data['payment_reference'],
                'narration': credit_note_data['note'],
                'invoice_line_ids': [(0, 0, credit_note_line)],
            }
            
            models.execute_kw(db, uid, password,
                'account.move', 'write',
                [[existing_credit_note['id']], update_vals])
            
            print(f"Successfully updated credit note: {existing_credit_note['id']}")
            return existing_credit_note['id']
        else:
            # Create new credit note
            credit_note_vals = {
                'move_type': 'out_refund',  # This is for customer credit note
                'partner_id': partner_id,
                'invoice_date': credit_note_data['invoice_date'],
                'name': credit_note_data['document_number'],
                'payment_reference': credit_note_data['payment_reference'],
                'narration': credit_note_data['note'],
                'invoice_line_ids': [(0, 0, credit_note_line)],
                'journal_id': journal['id'],  # Set the journal
            }

            credit_note_id = models.execute_kw(db, uid, password,
                'account.move', 'create',
                [credit_note_vals])

            print(f"Successfully created new credit note with ID: {credit_note_id}")
            return credit_note_id

    except Exception as e:
        print(f"Error processing credit note: {str(e)}")
        return False

def main():
    try:
        # Connect to Odoo
        uid, models = connect_to_odoo()
        print("Successfully connected to Odoo")

        # Read Excel file
        df = read_excel_file()
        print("Successfully read Excel file")

        # Process each row in the Excel file
        for index, row in df.iterrows():
            try:
                # Convert invoice_date to string format if it's a datetime
                invoice_date = row['invoice_date']
                if isinstance(invoice_date, pd.Timestamp):
                    invoice_date = invoice_date.strftime('%Y-%m-%d')

                # Clean and prepare data
                credit_note_data = {
                    'invoice_date': invoice_date,
                    'partner_code': str(row['partner_code']).strip(),
                    'partner_name': str(row['partner_id']).strip(),  # Changed from partner_name to partner_id
                    'default_code': str(row['default_code']) if pd.notna(row['default_code']) else '',
                    'journal': str(row['journal']).strip() if pd.notna(row['journal']) else '',
                    'document_number': str(row['name']).strip() if pd.notna(row['name']) else '',
                    'payment_reference': str(row['payment_referance']).strip() if pd.notna(row['payment_referance']) else '',
                    'note': str(row['note']).strip() if pd.notna(row['note']) else '',
                    'price_unit': float(row['price_unit']) if pd.notna(row['price_unit']) else 0.0,
                }

                # Create or update credit note
                result = update_or_create_credit_note(uid, models, credit_note_data)
                if result:
                    print(f"Successfully processed row {index + 2}")
                else:
                    print(f"Failed to process row {index + 2}")

            except Exception as e:
                print(f"Error processing row {index + 2}: {str(e)}")
                continue  # Continue with next row even if current row fails

        print("\nImport process completed")

    except Exception as e:
        print(f"Error in main process: {str(e)}")

if __name__ == "__main__":
    main()