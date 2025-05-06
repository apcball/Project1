#!/usr/bin/env python3
import xmlrpc.client
import pandas as pd
from datetime import datetime
import os
import json
import ast

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
    file_path = 'Data_file/import_bill.xlsx'
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Excel file not found at {file_path}")
    
    # Define expected columns
    expected_columns = [
        'partner_id',          # ชื่อผู้ขาย/ผู้จำหน่าย
        'partner_code',        # รหัสผู้ขาย/ผู้จำหน่าย
        'name',               # เลขที่เอกสาร
        'payment_reference',   # เลขที่อ้างอิงการชำระเงิน
        'bill_reference',     # เลขที่อ้างอิงบิล
        'invoice_date',        # วันที่ใบแจ้งหนี้
        'default_code',        # รหัสสินค้า
        'product_name',        # ชื่อสินค้า/บริการ
        'label',              # รายละเอียดสินค้า/บริการ
        'quantity',            # จำนวน
        'uom',                 # หน่วยนับ
        'price_unit',         # ราคาต่อหน่วย
        'tax_id',             # รหัสภาษี
        'analytic_distribution', # รหัสแผนก/โครงการ
        'note',                # หมายเหตุ
        'payment_term',        # เงื่อนไขการชำระเงิน
        'due_date',           # วันครบกำหนดชำระ
        'currency_id',        # สกุลเงิน
    ]
    
    # Read Excel file with all sheets
    df = pd.read_excel(file_path, dtype=str)
    
    # Print all columns to verify structure
    print("\nColumns in Excel file:", df.columns.tolist())
    
    # Verify required columns exist
    required_columns = ['bill_reference', 'payment_reference', 'journal']
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        raise ValueError(f"Required columns not found in Excel file: {', '.join(missing_columns)}")
    
    # Convert numeric columns
    numeric_columns = ['quantity', 'price_unit']
    for col in numeric_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
    
    # Check for missing required columns
    required_columns = ['partner_id', 'name', 'invoice_date', 'default_code', 'price_unit']
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        print("Excel columns found:", df.columns.tolist())
        raise ValueError(f"Missing required columns in Excel file: {', '.join(missing_columns)}")
    if missing_columns:
        raise ValueError(f"Missing required columns in Excel file: {', '.join(missing_columns)}")
    
    return df

def get_or_create_vendor(uid, models, vendor_name, partner_code=None):
    try:
        # Clean vendor name and partner code
        vendor_name = vendor_name.strip()
        
        # First try to find vendor by partner_code if provided
        if partner_code:
            partner_code = str(partner_code).strip()
            # Search by partner_code
            vendor = models.execute_kw(db, uid, password,
                'res.partner', 'search_read',
                [[['partner_code', '=', partner_code]]],
                {'fields': ['id', 'name']})
            
            if vendor:
                print(f"Found existing vendor by partner_code: {partner_code}")
                return vendor[0]['id']
                
            # If not found by partner_code, try old_code_partner
            vendor = models.execute_kw(db, uid, password,
                'res.partner', 'search_read',
                [[['old_code_partner', '=', partner_code]]],
                {'fields': ['id', 'name']})
            
            if vendor:
                print(f"Found existing vendor by old_code_partner: {partner_code}")
                return vendor[0]['id']
        
        # If not found by codes, search by name
        vendor = models.execute_kw(db, uid, password,
            'res.partner', 'search_read',
            [[['name', '=', vendor_name]]],
            {'fields': ['id', 'name']})
        
        if vendor:
            print(f"Found existing vendor by name: {vendor_name}")
            return vendor[0]['id']

        # If vendor not found, create new vendor
        new_vendor_vals = {
            'name': vendor_name,
            'supplier_rank': 1,  # Mark as vendor
            'company_type': 'company',  # Set as company by default
        }
        
        new_vendor_id = models.execute_kw(db, uid, password,
            'res.partner', 'create',
            [new_vendor_vals])
        
        print(f"Created new vendor: {vendor_name} with ID: {new_vendor_id}")
        return new_vendor_id

    except Exception as e:
        print(f"Error in vendor creation: {str(e)}")
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



def get_journal_id(uid, models, journal_code):
    """Find journal ID by code or name"""
    try:
        # Get default purchase journal as fallback
        default_journal = models.execute_kw(db, uid, password,
            'account.journal', 'search_read',
            [[['type', '=', 'purchase']]],
            {'fields': ['id', 'name', 'code'], 'limit': 1})
        
        default_journal_id = default_journal[0]['id'] if default_journal else False
        
        if not journal_code or pd.isna(journal_code):
            print(f"No journal specified, using default purchase journal")
            return default_journal_id
            
        journal_code = str(journal_code).strip()
        
        # Search for journal with various conditions
        domain = ['|', '|', '|',
            ['code', '=', journal_code],
            ['code', 'ilike', journal_code],
            ['name', '=', journal_code],
            ['name', 'ilike', journal_code]
        ]
        
        journals = models.execute_kw(db, uid, password,
            'account.journal', 'search_read',
            [domain],
            {'fields': ['id', 'name', 'code']})
        
        if journals:
            # Print all found journals for debugging
            for j in journals:
                print(f"Found journal: {j['name']} (code: {j['code']})")
            # Use the first match
            print(f"Using journal: {journals[0]['name']} (code: {journals[0]['code']})")
            return journals[0]['id']
        else:
            print(f"Warning: Journal not found for: {journal_code}, using default purchase journal")
            return default_journal_id
            
    except Exception as e:
        print(f"Error finding journal: {str(e)}")
        return default_journal_id

def find_existing_bill(uid, models, document_number):
    """Find existing bill by document number"""
    if not document_number:
        return None
    
    # Search for existing bill with the same name (document number)
    bill_ids = models.execute_kw(db, uid, password,
        'account.move', 'search_read',
        [[['name', '=', document_number], ['move_type', '=', 'in_invoice']]],
        {'fields': ['id', 'state']})
    
    return bill_ids[0] if bill_ids else None

def get_analytic_account_id(uid, models, code):
    """Find analytic account ID by code"""
    try:
        account_ids = models.execute_kw(db, uid, password,
            'account.analytic.account', 'search_read',
            [[['code', '=', code]]],
            {'fields': ['id']})
        
        if account_ids:
            return str(account_ids[0]['id'])
        print(f"Warning: No analytic account found for code: {code}")
        return None
    except Exception as e:
        print(f"Error finding analytic account: {str(e)}")
        return None

def parse_analytic_distribution(uid, models, analytic_str):
    """Parse analytic distribution from code to Odoo format"""
    try:
        if pd.isna(analytic_str) or not analytic_str:
            return {}

        # Clean the input string
        code = str(analytic_str).strip()
        
        # Get the analytic account ID for the code
        account_id = get_analytic_account_id(uid, models, code)
        
        if account_id:
            # Return the distribution with 100% to this account
            return {account_id: 100.0}
        
        return {}

    except Exception as e:
        print(f"Error parsing analytic distribution: {str(e)}")
        print(f"Invalid analytic_distribution format: {analytic_str}")
        return {}

def update_or_create_bill(uid, models, bill_data):
    try:
        # Check if bill already exists
        existing_bill = find_existing_bill(uid, models, bill_data['document_number'])
        
        # Get or create vendor
        vendor_id = get_or_create_vendor(uid, models, bill_data['vendor_name'], bill_data.get('partner_code'))
        if not vendor_id:
            print("Failed to get or create vendor")
            return False

        # Find product by default_code
        if bill_data['default_code']:
            product = find_product_by_code(uid, models, bill_data['default_code'])
            if not product:
                print(f"Product not found with code: {bill_data['default_code']}")
                return False
        else:
            print("No product code provided")
            return False

        # Parse analytic distribution
        analytic_distribution = parse_analytic_distribution(uid, models, bill_data['analytic_distribution'])
        print(f"Using analytic distribution: {analytic_distribution}")

        # Prepare bill line
        bill_line = {
            'product_id': product['id'],
            'name': bill_data['label'] or bill_data['product_name'] or product['name'],
            'quantity': bill_data['quantity'],
            'price_unit': bill_data['price_unit'],
            'analytic_distribution': analytic_distribution,
        }

        if existing_bill:
            print(f"Found existing bill with number: {bill_data['document_number']}")
            
            # Check if bill is in draft state
            if existing_bill['state'] != 'draft':
                print(f"Cannot update bill {bill_data['document_number']} as it is not in draft state")
                return False

            # Update existing bill
            # First, delete existing lines
            existing_lines = models.execute_kw(db, uid, password,
                'account.move.line', 'search',
                [[['move_id', '=', existing_bill['id']], ['display_type', 'in', ['product', False]]]])
            
            if existing_lines:
                models.execute_kw(db, uid, password, 'account.move.line', 'unlink', [existing_lines])

                # Update bill fields
                bill_reference = bill_data.get('bill_reference', '').strip()
                print(f"Setting bill reference to: {bill_reference}")
                
                update_vals = {
                    'partner_id': vendor_id,
                    'invoice_date': bill_data['invoice_date'],
                    'ref': bill_reference,  # This is the bill reference field in Odoo
                    'payment_reference': bill_data['payment_reference'],
                    'narration': bill_data.get('note', ''),
                    'invoice_line_ids': [(0, 0, bill_line)],
                }

            models.execute_kw(db, uid, password,
                'account.move', 'write',
                [[existing_bill['id']], update_vals])
            
            print(f"Successfully updated bill: {existing_bill['id']}")
            return existing_bill['id']
        else:
            # Create new bill
            # Ensure bill reference and payment reference are properly set
            bill_reference = bill_data.get('bill_reference', '').strip()
            payment_reference = bill_data.get('payment_reference', '').strip()
            
            # Get journal_id (will return default journal if specified journal not found)
            journal_id = get_journal_id(uid, models, bill_data.get('journal'))
            
            print(f"Setting bill reference to: {bill_reference}")
            print(f"Setting payment reference to: {payment_reference}")
            print(f"Using journal_id: {journal_id}")
            
            bill_vals = {
                'move_type': 'in_invoice',
                'partner_id': vendor_id,
                'invoice_date': bill_data['invoice_date'],
                'name': bill_data['document_number'],
                'ref': bill_reference,  # Bill reference field
                'payment_reference': payment_reference,  # Payment reference field
                'journal_id': journal_id,  # Journal field
                'narration': bill_data.get('note', ''),
                'invoice_line_ids': [(0, 0, bill_line)],
            }

            try:
                bill_id = models.execute_kw(db, uid, password,
                    'account.move', 'create',
                    [bill_vals])

                if bill_id:
                    print(f"Successfully created new bill with ID: {bill_id}")
                    # Verify the bill was created properly
                    created_bill = models.execute_kw(db, uid, password,
                        'account.move', 'search_read',
                        [[['id', '=', bill_id]]],
                        {'fields': ['id', 'state', 'name']})
                    if created_bill:
                        print(f"Bill {created_bill[0]['name']} created in {created_bill[0]['state']} state")
                        return bill_id
                    else:
                        print("Warning: Bill created but verification failed")
                        return bill_id
                else:
                    print("Failed to create bill - no ID returned")
                    return False

            except xmlrpc.client.Fault as fault:
                print(f"XMLRPC Fault while creating bill: {fault.faultString}")
                if 'access' in fault.faultString.lower():
                    print("Access rights issue detected - please check user permissions")
                return False
            except Exception as e:
                print(f"Unexpected error while creating bill: {str(e)}")
                print(f"Bill values that caused error: {bill_vals}")
                return False

    except Exception as e:
        print(f"Error processing bill: {str(e)}")
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
                if pd.notna(invoice_date):
                    if isinstance(invoice_date, pd.Timestamp):
                        # Convert to YYYY-MM-DD format for Odoo
                        invoice_date = invoice_date.strftime('%Y-%m-%d')
                    else:
                        # If it's already a string, ensure it's in correct format
                        try:
                            # Parse the date assuming m/d/y format
                            date_obj = pd.to_datetime(invoice_date)
                            # Convert to YYYY-MM-DD format for Odoo
                            invoice_date = date_obj.strftime('%Y-%m-%d')
                        except:
                            print(f"Warning: Could not parse date: {invoice_date}")
                            invoice_date = str(invoice_date).strip()

                # Clean and prepare data
                bill_data = {
                    'invoice_date': invoice_date if pd.notna(invoice_date) else None,
                    'vendor_name': str(row['partner_id']).strip() if pd.notna(row['partner_id']) else '',
                    'partner_code': str(row['partner_code']).strip() if pd.notna(row.get('partner_code')) else None,
                    'default_code': str(row['default_code']).strip() if pd.notna(row['default_code']) else '',
                    'price_unit': float(row['price_unit']) if pd.notna(row['price_unit']) else 0.0,
                    'document_number': str(row['name']).strip() if pd.notna(row['name']) else '',
                    'payment_reference': str(row['payment_reference']).strip() if pd.notna(row['payment_reference']) else '',
                    'bill_reference': str(row['bill_reference']).strip() if pd.notna(row['bill_reference']) else '',
                    'journal': str(row['journal']).strip() if pd.notna(row['journal']) else '',
                    'note': str(row['note']).strip() if pd.notna(row.get('note')) else '',
                    'analytic_distribution': row['analytic_distribution'] if pd.notna(row.get('analytic_distribution')) else '',
                    'product_name': str(row['product_name']).strip() if pd.notna(row.get('product_name')) else '',
                    'label': str(row['label']).strip() if pd.notna(row.get('label')) else '',
                    'quantity': float(row['quantity']) if pd.notna(row.get('quantity')) else 1.0,
                }
                
                print(f"\nProcessing bill for vendor: {bill_data['vendor_name']}")
                print(f"Partner Code: {bill_data['partner_code']}")
                print(f"Document number: {bill_data['document_number']}")
                print(f"Bill Reference: {bill_data['bill_reference']}")
                print(f"Payment Reference: {bill_data['payment_reference']}")
                print(f"Journal: {bill_data['journal']}")
                print(f"Invoice date: {bill_data['invoice_date']}")
                print(f"Product code: {bill_data['default_code']}")
                print(f"Product name: {bill_data['product_name']}")
                print(f"Label: {bill_data['label']}")
                print(f"Quantity: {bill_data['quantity']}")
                print(f"Price: {bill_data['price_unit']}")
                print(f"Analytic Distribution: {bill_data['analytic_distribution']}")
                update_or_create_bill(uid, models, bill_data)

            except Exception as e:
                print(f"Error processing row {index + 2}: {str(e)}")
                continue

        print("\nImport process completed")

    except Exception as e:
        print(f"Error in main process: {str(e)}")

if __name__ == "__main__":
    main()