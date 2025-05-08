#!/usr/bin/env python3
import xmlrpc.client
import pandas as pd
from datetime import datetime
import os
import logging
from collections import defaultdict

# Set up logging
logging.basicConfig(
    filename='import_invoice.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Global statistics
import_stats = {
    'success': 0,
    'failed': 0,
    'updated': 0,
    'created': 0
}

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
    file_path = 'Data_file/import_invoice_ARX.xlsx'
    if not os.path.exists(file_path):
        msg = f"Excel file not found at {file_path}"
        logging.error(msg)
        raise FileNotFoundError(msg)
    return pd.read_excel(file_path)

def print_import_summary():
    logging.info("\n=== Import Summary ===")
    logging.info(f"Total Success: {import_stats['success']}")
    logging.info(f"Total Failed: {import_stats['failed']}")
    logging.info(f"New Invoices Created: {import_stats['created']}")
    logging.info(f"Existing Invoices Updated: {import_stats['updated']}")
    logging.info("===================\n")

def update_move_line_account(uid, models, move_id, account_id):
    """Update account in journal items after invoice is posted - for the debit line"""
    try:
        if not account_id:
            return False

        # Get the debit line for this invoice
        move_lines = models.execute_kw(db, uid, password,
            'account.move.line', 'search_read',
            [[['move_id', '=', move_id], 
              ['debit', '>', 0]  # Get debit line
            ]],
            {'fields': ['id', 'account_id', 'name', 'debit']})

        if move_lines:
            # Update the account for the debit line
            models.execute_kw(db, uid, password,
                'account.move.line', 'write',
                [[move_lines[0]['id']], {'account_id': account_id}])
            print(f"Updated account for move line {move_lines[0]['id']}")
            return True
        return False

    except Exception as e:
        print(f"Error updating move line account: {str(e)}")
        return False

def main():
    try:
        logging.info("Starting invoice import process...")
        
        # Connect to Odoo
        uid, models = connect_to_odoo()
        logging.info("Successfully connected to Odoo")

        # Read Excel file
        df = read_excel_file()
        logging.info(f"Read {len(df)} records from Excel file")

        # Group by document number to handle multiple lines
        grouped = df.groupby('document_number')
        
        for doc_num, group in grouped:
            logging.info(f"Processing document number: {doc_num}")
            
            for _, row in group.iterrows():
                invoice_data = {
                    'document_number': doc_num,
                    'partner_code': row['partner_code'],
                    'partner_name': row['partner_name'],
                    'default_code': row['default_code'],
                    'price_unit': row['price_unit'],
                    'quantity': row.get('quantity', 1),
                    'invoice_date': row['invoice_date'],
                    'payment_reference': row.get('payment_reference', ''),
                    'note': row.get('note', ''),
                    'journal': row.get('journal', ''),
                    'account_code': row.get('account_code', '')  # เพิ่ม account_code
                }
                
                result = update_or_create_invoice(uid, models, invoice_data)
                if not result:
                    logging.error(f"Failed to process line for document {doc_num}")

        print_import_summary()
        logging.info("Import process completed")

    except Exception as e:
        logging.error(f"Error in main process: {str(e)}")
        import_stats['failed'] += 1

if __name__ == "__main__":
    main()

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

def find_journal_by_name(uid, models, journal_name):
    if not journal_name or pd.isna(journal_name):
        return None
    
    journal_name = str(journal_name).strip()
    journal_ids = models.execute_kw(db, uid, password,
        'account.journal', 'search_read',
        [[['name', 'ilike', journal_name], ['type', '=', 'sale']]],
        {'fields': ['id', 'name']})
    
    return journal_ids[0]['id'] if journal_ids else None

def find_account_by_code(uid, models, account_code):
    """Find account by code"""
    if not account_code or pd.isna(account_code):
        return None
        
    account_code = str(account_code).strip()
    account_ids = models.execute_kw(db, uid, password,
        'account.account', 'search_read',
        [[['code', '=', account_code]]],
        {'fields': ['id', 'name', 'code']})
    
    if account_ids:
        print(f"Found account: {account_ids[0]['name']} ({account_ids[0]['code']})")
        return account_ids[0]['id']
    print(f"Account not found with code: {account_code}")
    return None

def find_existing_invoice(uid, models, document_number):
    if not document_number:
        return None
    
    # Search for existing invoice with the same name (document number)
    invoice_ids = models.execute_kw(db, uid, password,
        'account.move', 'search_read',
        [[['name', '=', document_number], ['move_type', '=', 'out_invoice']]],
        {'fields': ['id', 'state']})
    
    return invoice_ids[0] if invoice_ids else None

def update_or_create_invoice(uid, models, invoice_data):
    try:
        # Check if invoice already exists
        existing_invoice = find_existing_invoice(uid, models, invoice_data['document_number'])
        
        # Get or create partner
        partner_id = get_or_create_partner(uid, models, invoice_data['partner_code'], invoice_data['partner_name'])
        if not partner_id:
            msg = f"Failed to get or create partner for {invoice_data['document_number']}"
            logging.error(msg)
            import_stats['failed'] += 1
            return False

        # Find product by default_code
        product = find_product_by_code(uid, models, invoice_data['default_code'])
        if not product:
            msg = f"Product not found with code: {invoice_data['default_code']} for invoice {invoice_data['document_number']}"
            logging.error(msg)
            return False

        # Find account by code
        account_id = None
        if 'account_code' in invoice_data and invoice_data['account_code']:
            account_id = find_account_by_code(uid, models, invoice_data['account_code'])
            if not account_id:
                msg = f"Account not found with code: {invoice_data['account_code']} for invoice {invoice_data['document_number']}"
                logging.warning(msg)  # Warning only, will use default account
            import_stats['failed'] += 1
            return False

        # Get quantity from data or default to 1
        quantity = invoice_data.get('quantity', 1)
        
        # Prepare invoice line
        invoice_line = {
            'product_id': product['id'],
            'name': product['name'],
            'quantity': quantity,
            'price_unit': invoice_data['price_unit'],
        }

        # Get journal id
        journal_id = find_journal_by_name(uid, models, invoice_data.get('journal'))
        if not journal_id:
            msg = f"Journal not found: {invoice_data.get('journal')} for invoice {invoice_data['document_number']}"
            logging.error(msg)
            import_stats['failed'] += 1
            return False

        if existing_invoice:
            logging.info(f"Found existing invoice with number: {invoice_data['document_number']}")
            
            # Check if invoice is in draft state
            if existing_invoice['state'] != 'draft':
                msg = f"Cannot update invoice {invoice_data['document_number']} as it is not in draft state"
                logging.error(msg)
                import_stats['failed'] += 1
                return False

            # Add new line to existing invoice
            update_vals = {
                'invoice_line_ids': [(0, 0, invoice_line)],
            }
            
            models.execute_kw(db, uid, password,
                'account.move', 'write',
                [[existing_invoice['id']], update_vals])
            
            msg = f"Successfully added new line to invoice: {existing_invoice['id']}"
            logging.info(msg)
            import_stats['updated'] += 1
            return existing_invoice['id']
        else:
            # Create new invoice
            invoice_vals = {
                'move_type': 'out_invoice',
                'partner_id': partner_id,
                'invoice_date': invoice_data['invoice_date'],
                'name': invoice_data['document_number'],
                'payment_reference': invoice_data['payment_reference'],
                'narration': invoice_data['note'],
                'invoice_line_ids': [(0, 0, invoice_line)],
                'journal_id': journal_id,
            }

            invoice_id = models.execute_kw(db, uid, password,
                'account.move', 'create',
                [invoice_vals])

            msg = f"Successfully created new invoice with ID: {invoice_id}"
            logging.info(msg)
            import_stats['created'] += 1
            import_stats['success'] += 1
            return invoice_id

    except Exception as e:
        msg = f"Error processing invoice {invoice_data['document_number']}: {str(e)}"
        logging.error(msg)
        import_stats['failed'] += 1
        return False
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
                invoice_data = {
                    'invoice_date': invoice_date,
                    'partner_code': str(row['partner_code']).strip(),
                    'partner_name': str(row['partner_id']).strip(),  # Changed from partner_name to partner_id
                    'default_code': str(row['default_code']) if pd.notna(row['default_code']) else '',
                    'document_number': str(row['name']).strip() if pd.notna(row['name']) else '',
                    'payment_reference': str(row['payment_referance']).strip() if pd.notna(row['payment_referance']) else '',
                    'note': str(row['note']).strip() if pd.notna(row['note']) else '',
                    'price_unit': float(row['price_unit']) if pd.notna(row['price_unit']) else 0.0,
                    'journal': str(row['journal']) if pd.notna(row['journal']) else '',  # Add journal field
                }
                
                print(f"\nProcessing invoice for partner: {invoice_data['partner_name']} (Code: {invoice_data['partner_code']})")
                print(f"Document number: {invoice_data['document_number']}")
                print(f"Payment Reference: {invoice_data['payment_reference']}")
                update_or_create_invoice(uid, models, invoice_data)

            except Exception as e:
                print(f"Error processing row {index + 2}: {str(e)}")
                continue

        print("\nImport process completed")

    except Exception as e:
        print(f"Error in main process: {str(e)}")

if __name__ == "__main__":
    main()