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
    file_path = 'Data_file/import_invoice.xlsx'
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Excel file not found at {file_path}")
    return pd.read_excel(file_path)

def get_or_create_partner(uid, models, partner_name):
    try:
        # Clean partner name
        partner_name = partner_name.strip()
        
        # Search for existing partner
        partner = models.execute_kw(db, uid, password,
            'res.partner', 'search_read',
            [[['name', '=', partner_name]]],
            {'fields': ['id', 'name']})
        
        if partner:
            print(f"Found existing partner: {partner_name}")
            return partner[0]['id']

        # If partner not found, create new partner
        new_partner_vals = {
            'name': partner_name,
            'customer_rank': 1,  # Mark as customer
            'company_type': 'company',  # Set as company by default
        }
        
        new_partner_id = models.execute_kw(db, uid, password,
            'res.partner', 'create',
            [new_partner_vals])
        
        print(f"Created new partner: {partner_name} with ID: {new_partner_id}")
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
        partner_id = get_or_create_partner(uid, models, invoice_data['partner_name'])
        if not partner_id:
            print("Failed to get or create partner")
            return False

        # Find product by default_code
        product = find_product_by_code(uid, models, invoice_data['default_code'])
        if not product:
            print(f"Product not found with code: {invoice_data['default_code']}")
            return False

        # Prepare invoice line
        invoice_line = {
            'product_id': product['id'],
            'name': product['name'],
            'quantity': 1,  # Default quantity to 1 if not specified
            'price_unit': invoice_data['price_unit'],
        }

        if existing_invoice:
            print(f"Found existing invoice with number: {invoice_data['document_number']}")
            
            # Check if invoice is in draft state
            if existing_invoice['state'] != 'draft':
                print(f"Cannot update invoice {invoice_data['document_number']} as it is not in draft state")
                return False

            # Update existing invoice
            # First, delete existing lines
            models.execute_kw(db, uid, password,
                'account.move.line', 'unlink',
                [models.execute_kw(db, uid, password,
                    'account.move.line', 'search',
                    [[['move_id', '=', existing_invoice['id']], ['product_id', '!=', False]]])])

            # Update invoice fields
            update_vals = {
                'partner_id': partner_id,
                'invoice_date': invoice_data['invoice_date'],
                'payment_reference': invoice_data['payment_reference'],  # Add payment reference
                'narration': invoice_data['note'],
                'invoice_line_ids': [(0, 0, invoice_line)],
            }
            
            models.execute_kw(db, uid, password,
                'account.move', 'write',
                [[existing_invoice['id']], update_vals])
            
            print(f"Successfully updated invoice: {existing_invoice['id']}")
            return existing_invoice['id']
        else:
            # Create new invoice
            invoice_vals = {
                'move_type': 'out_invoice',
                'partner_id': partner_id,
                'invoice_date': invoice_data['invoice_date'],
                'name': invoice_data['document_number'],  # Set document number as name
                'payment_reference': invoice_data['payment_reference'],  # Add payment reference
                'narration': invoice_data['note'],
                'invoice_line_ids': [(0, 0, invoice_line)],
            }

            invoice_id = models.execute_kw(db, uid, password,
                'account.move', 'create',
                [invoice_vals])

            print(f"Successfully created new invoice with ID: {invoice_id}")
            return invoice_id

    except Exception as e:
        print(f"Error processing invoice: {str(e)}")
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
                    'partner_name': str(row['partner_id']).strip(),
                    'default_code': str(row['default_code']) if pd.notna(row['default_code']) else '',
                    'price_unit': float(row['price_unit']) if pd.notna(row['price_unit']) else 0.0,
                    'document_number': str(row['name']).strip() if pd.notna(row['name']) else '',
                    'payment_reference': str(row['payment_referance']).strip() if pd.notna(row['payment_referance']) else '',  # Added payment_referance
                    'note': str(row['note']).strip() if pd.notna(row['note']) else '',
                }
                
                print(f"\nProcessing invoice for partner: {invoice_data['partner_name']}")
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