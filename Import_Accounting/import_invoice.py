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

def truncate_string(value, max_length=500):
    """Truncate string to maximum length while preserving words"""
    if not value or len(str(value)) <= max_length:
        return value
    truncated = str(value)[:max_length-3].rsplit(' ', 1)[0]
    return truncated + '...'

def convert_date_format(date_value):
    """Convert date from Excel to proper format"""
    if pd.isna(date_value):
        return False
    
    try:
        # If date_value is already a datetime object (from Excel)
        if isinstance(date_value, datetime):
            return date_value.strftime('%Y-%m-%d')
        
        # If it's a pandas Timestamp
        if isinstance(date_value, pd.Timestamp):
            return date_value.strftime('%Y-%m-%d')
            
        # Try parsing string date in various formats
        date_str = str(date_value).strip()
        
        # Try datetime with time format first
        try:
            # Handle format like "2012-11-22 00:00:00"
            return datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S').strftime('%Y-%m-%d')
        except ValueError:
            pass
        
        # Try common Thai date formats
        formats = [
            '%Y-%m-%d',  # 2023-12-31
            '%d/%m/%Y',  # 31/12/2023
            '%d-%m-%Y',  # 31-12-2023
            '%d/%m/%y',  # 31/12/23
            '%Y/%m/%d',  # 2023/12/31
        ]
        
        for fmt in formats:
            try:
                return datetime.strptime(date_str, fmt).strftime('%Y-%m-%d')
            except ValueError:
                continue
        
        print(f"Warning: Could not parse date: {date_value}, using False")
        return False
        
    except Exception as e:
        print(f"Error converting date {date_value}: {str(e)}")
        return False

def clean_and_validate_data(value, field_name, max_length=500):
    """Clean and validate data fields"""
    if pd.isna(value):
        return ''
    
    cleaned_value = str(value).strip()
    
    # Handle specific field validations
    if field_name in ['quantity', 'price_unit']:
        try:
            return float(cleaned_value) if cleaned_value else 0.0
        except ValueError:
            print(f"Warning: Invalid number in {field_name}: {cleaned_value}, using 0")
            return 0.0
    
    # Handle date fields
    if field_name in ['invoice_date', 'due_date']:
        return convert_date_format(value)
            
    # Truncate long strings
    return truncate_string(cleaned_value, max_length)

def read_excel_file():
    file_path = 'Data_file/import_invoice_ARX1.xlsx'
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Excel file not found at {file_path}")
    
    # Define expected columns with their max lengths
    field_limits = {
        'partner_id': 500,          # ชื่อผู้ขาย/ผู้จำหน่าย
        'partner_code': 64,         # รหัสผู้ขาย/ผู้จำหน่าย
        'name': 255,               # เลขที่เอกสาร
        'payment_reference': 255,   # เลขที่อ้างอิงการชำระเงิน
        'bill_reference': 255,     # เลขที่อ้างอิงบิล
        'invoice_date': 10,        # วันที่ใบแจ้งหนี้
        'default_code': 64,        # รหัสสินค้า
        'product_name': 500,       # ชื่อสินค้า/บริการ
        'label': 1000,            # รายละเอียดสินค้า/บริการ
        'quantity': 0,            # จำนวน (numeric)
        'uom': 64,                # หน่วยนับ
        'price_unit': 0,         # ราคาต่อหน่วย (numeric)
        'tax_id': 64,            # รหัสภาษี
        'analytic_distribution': 64, # รหัสแผนก/โครงการ
        'note': 1000,            # หมายเหตุ
        'payment_term': 255,     # เงื่อนไขการชำระเงิน
        'due_date': 10,         # วันครบกำหนดชำระ
        'currency_id': 64,      # สกุลเงิน
        'journal': 64,          # สมุดรายวัน
        'expense_account': 64,  # รหัสบัญชีค่าใช้จ่าย
    }
    
    # Read Excel file with proper date parsing
    df = pd.read_excel(
        file_path,
        dtype={
            'invoice_date': 'datetime64[ns]'
        },
        parse_dates=['invoice_date']
    )
    print("\nColumns in Excel file:", df.columns.tolist())
    
    # Verify required columns
    required_columns = ['partner_id', 'name', 'invoice_date', 'default_code', 'price_unit', 
                       'bill_reference', 'payment_reference', 'journal']
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        raise ValueError(f"Required columns not found in Excel file: {', '.join(missing_columns)}")
    
    # Clean and validate data
    for column in df.columns:
        if column in field_limits:
            df[column] = df[column].apply(
                lambda x: clean_and_validate_data(x, column, field_limits[column])
            )
    
    # Convert numeric columns
    numeric_columns = ['quantity', 'price_unit']
    for col in numeric_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
    
    return df

def get_or_create_customer(uid, models, customer_name, partner_code=None):
    try:
        # Clean customer name and partner code
        customer_name = customer_name.strip()
        
        # First try to find customer by partner_code if provided
        if partner_code:
            partner_code = str(partner_code).strip()
            # Search by partner_code
            customer = models.execute_kw(db, uid, password,
                'res.partner', 'search_read',
                [[['partner_code', '=', partner_code]]],
                {'fields': ['id', 'name']})
            
            if customer:
                print(f"Found existing customer by partner_code: {partner_code}")
                return customer[0]['id']
                
            # If not found by partner_code, try old_code_partner
            customer = models.execute_kw(db, uid, password,
                'res.partner', 'search_read',
                [[['old_code_partner', '=', partner_code]]],
                {'fields': ['id', 'name']})
            
            if customer:
                print(f"Found existing customer by old_code_partner: {partner_code}")
                return customer[0]['id']
        
        # If not found by codes, search by name
        customer = models.execute_kw(db, uid, password,
            'res.partner', 'search_read',
            [[['name', '=', customer_name]]],
            {'fields': ['id', 'name']})
        
        if customer:
            print(f"Found existing customer by name: {customer_name}")
            return customer[0]['id']

        # If customer not found, create new customer
        new_customer_vals = {
            'name': customer_name,
            'customer_rank': 1,  # Mark as customer
            'company_type': 'company',  # Set as company by default
        }
        
        new_customer_id = models.execute_kw(db, uid, password,
            'res.partner', 'create',
            [new_customer_vals])
        
        print(f"Created new customer: {customer_name} with ID: {new_customer_id}")
        return new_customer_id

    except Exception as e:
        print(f"Error in customer creation: {str(e)}")
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

def update_move_line_account(uid, models, move_id, line_data):
    """Update account in journal items after invoice is posted - only for the debit line"""
    try:
        # Get the revenue account code from line_data
        revenue_account_code = line_data.get('revenue_account')
        if not revenue_account_code or pd.isna(revenue_account_code):
            print("No revenue account code provided, using default 113001")
            revenue_account_code = '113001'  # Default to เงินทดรองจ่าย

        # Find the revenue account
        revenue_account_id = find_account_by_code(uid, models, revenue_account_code)
        if not revenue_account_id:
            print(f"Could not find revenue account with code: {revenue_account_code}, using default 113001")
            # Try to find default account 113001
            revenue_account_id = find_account_by_code(uid, models, '113001')
            if not revenue_account_id:
                # Try alternative accounts in order
                alternative_accounts = ['113001', '113002', '113003']
                for acc_code in alternative_accounts:
                    revenue_account_id = find_account_by_code(uid, models, acc_code)
                    if revenue_account_id:
                        print(f"Using alternative account: {acc_code}")
                        break
                
                if not revenue_account_id:
                    print("Could not find any suitable account")
                    return False

        # Get the debit line (receivable line) for this invoice
        move_lines = models.execute_kw(db, uid, password,
            'account.move.line', 'search_read',
            [[['move_id', '=', move_id], 
              ['debit', '>', 0]  # Get debit line
            ]],
            {'fields': ['id', 'account_id', 'name', 'debit']})

        if not move_lines:
            print("No debit line found")
            return False

        # Get the invoice data to get the due date
        invoice_data = models.execute_kw(db, uid, password,
            'account.move', 'read',
            [move_id],
            {'fields': ['invoice_date', 'invoice_date_due']})

        if not invoice_data:
            print("Could not find invoice data")
            return False

        # Use invoice_date_due if available, otherwise use invoice_date
        due_date = invoice_data[0].get('invoice_date_due') or invoice_data[0].get('invoice_date')
        if not due_date:
            print("No due date or invoice date found")
            return False

        # Update the debit line's account and due date
        try:
            update_vals = {
                'account_id': revenue_account_id,
                'date_maturity': due_date  # Set the due date
            }
            models.execute_kw(db, uid, password,
                'account.move.line', 'write',
                [[move_lines[0]['id']], update_vals])
            print(f"Successfully updated debit account to {revenue_account_code} with due date")
            return True
        except Exception as e:
            print(f"Error updating debit line: {str(e)}")
            return False

    except Exception as e:
        print(f"Error updating move line account: {str(e)}")
        return False

def find_account_by_code(uid, models, account_code):
    """Find account ID by code"""
    try:
        if not account_code or pd.isna(account_code):
            return None
            
        # Clean up account code
        account_code = str(account_code).strip()
        # Remove any spaces and special characters but keep numbers and letters
        account_code = ''.join(c for c in account_code if c.isalnum())
        
        # Search for account with code
        # Try exact match first
        account_ids = models.execute_kw(db, uid, password,
            'account.account', 'search_read',
            [[['code', '=', account_code]]],
            {'fields': ['id', 'name', 'code']})
        
        if not account_ids:
            # If no exact match, try with 'ilike'
            account_ids = models.execute_kw(db, uid, password,
                'account.account', 'search_read',
                [[['code', 'ilike', account_code]]],
                {'fields': ['id', 'name', 'code']})
        
        if account_ids:
            print(f"Found account: {account_ids[0]['name']} (code: {account_ids[0]['code']})")
            return account_ids[0]['id']
        else:
            print(f"Warning: Account not found for code: {account_code}")
            return None
            
    except Exception as e:
        print(f"Error finding account: {str(e)}")
        return None

def get_journal_id(uid, models, journal_code):
    """Find journal ID by code or name"""
    try:
        # Get default sale journal as fallback
        default_journal = models.execute_kw(db, uid, password,
            'account.journal', 'search_read',
            [[['type', '=', 'sale']]],
            {'fields': ['id', 'name', 'code'], 'limit': 1})
        
        default_journal_id = default_journal[0]['id'] if default_journal else False
        
        if not journal_code or pd.isna(journal_code):
            print(f"No journal specified, using default sale journal")
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
            print(f"Warning: Journal not found for: {journal_code}, using default sale journal")
            return default_journal_id
            
    except Exception as e:
        print(f"Error finding journal: {str(e)}")
        return default_journal_id

def find_existing_invoice(uid, models, document_number):
    """Find existing invoice by document number"""
    if not document_number:
        return None
    
    # Search for existing invoice with the same name (document number)
    invoice_ids = models.execute_kw(db, uid, password,
        'account.move', 'search_read',
        [[['name', '=', document_number], ['move_type', '=', 'out_invoice']]],
        {'fields': ['id', 'state']})
    
    return invoice_ids[0] if invoice_ids else None

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

def create_import_log():
    """Create or get import log file"""
    import csv
    from datetime import datetime
    
    log_dir = 'Data_file/logs'
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_file = f'{log_dir}/import_log_{timestamp}.csv'
    
    # Create log file with headers
    with open(log_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow([
            'Timestamp',
            'Document Number',
            'Vendor Name',
            'Bill Reference',
            'Status',
            'Message',
            'Row Number'
        ])
    return log_file

def log_import_result(log_file, data, status, message, row_number):
    """Log import result to CSV file"""
    import csv
    from datetime import datetime
    
    with open(log_file, 'a', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow([
            datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            data.get('document_number', ''),
            data.get('vendor_name', ''),
            data.get('bill_reference', ''),
            status,
            message,
            row_number
        ])

def update_or_create_invoice(uid, models, invoice_data):
    try:
        # Check if invoice already exists
        existing_invoice = find_existing_invoice(uid, models, invoice_data['document_number'])
        
        # Get or create customer
        customer_id = get_or_create_customer(uid, models, invoice_data['customer_name'], invoice_data.get('partner_code'))
        if not customer_id:
            print("Failed to get or create customer")
            return False

        # Find product by default_code
        if invoice_data['default_code']:
            product = find_product_by_code(uid, models, invoice_data['default_code'])
            if not product:
                print(f"Product not found with code: {invoice_data['default_code']}")
                return False
        else:
            print("No product code provided")
            return False

        # Parse analytic distribution
        analytic_distribution = parse_analytic_distribution(uid, models, invoice_data['analytic_distribution'])
        print(f"Using analytic distribution: {analytic_distribution}")

        # Find account by code
        account_id = None
        if invoice_data.get('account_code'):
            account_id = find_account_by_code(uid, models, invoice_data['account_code'])
            if not account_id:
                print(f"Warning: Account not found with code: {invoice_data['account_code']}")

        # Prepare invoice line
        invoice_line = {
            'product_id': product['id'],
            'name': invoice_data['label'] or invoice_data['product_name'] or product['name'],
            'quantity': invoice_data['quantity'],
            'price_unit': invoice_data['price_unit'],
            'analytic_distribution': analytic_distribution,
        }
        
        # Add account if found
        if account_id:
            invoice_line['account_id'] = account_id

        if existing_invoice:
            print(f"Found existing invoice with number: {invoice_data['document_number']}")
            
            # Check if invoice is in draft state
            if existing_invoice['state'] != 'draft':
                print(f"Cannot update invoice {invoice_data['document_number']} as it is not in draft state")
                return False

            # Add new line to existing invoice
            update_vals = {
                'invoice_line_ids': [(0, 0, invoice_line)],
            }

            # Update header fields only if they are different
            invoice_reference = invoice_data.get('invoice_reference', '').strip()
            existing_invoice_data = models.execute_kw(db, uid, password,
                'account.move', 'read',
                [existing_invoice['id']],
                {'fields': ['partner_id', 'invoice_date', 'ref', 'payment_reference', 'narration']})

            if existing_invoice_data:
                current_data = existing_invoice_data[0]
                
                # Only update header fields if they are different or not set
                if current_data['partner_id'] and current_data['partner_id'][0] != customer_id:
                    update_vals['partner_id'] = customer_id
                if not current_data['invoice_date'] or current_data['invoice_date'] != invoice_data['invoice_date']:
                    update_vals['invoice_date'] = invoice_data['invoice_date']
                if not current_data['ref'] or current_data['ref'] != invoice_reference:
                    update_vals['ref'] = invoice_reference
                if not current_data['payment_reference'] or current_data['payment_reference'] != invoice_data['payment_reference']:
                    update_vals['payment_reference'] = invoice_data['payment_reference']
                if invoice_data.get('note') and (not current_data['narration'] or current_data['narration'] != invoice_data['note']):
                    update_vals['narration'] = invoice_data['note']

            models.execute_kw(db, uid, password,
                'account.move', 'write',
                [[existing_invoice['id']], update_vals])
            
            print(f"Successfully added line to existing invoice: {existing_invoice['id']}")
            return existing_invoice['id']
        else:
            # Create new invoice
            # Ensure invoice reference and payment reference are properly set
            invoice_reference = invoice_data.get('invoice_reference', '').strip()
            payment_reference = invoice_data.get('payment_reference', '').strip()
            
            # Get journal_id (will return default journal if specified journal not found)
            journal_id = get_journal_id(uid, models, invoice_data.get('journal'))
            
            print(f"Setting invoice reference to: {invoice_reference}")
            print(f"Setting payment reference to: {payment_reference}")
            print(f"Using journal_id: {journal_id}")
            
            # Set invoice date and due date
            invoice_date = invoice_data['invoice_date']
            due_date = invoice_data.get('due_date', invoice_date)  # Use invoice date as due date if not specified
            
            invoice_vals = {
                'move_type': 'out_invoice',
                'partner_id': customer_id,
                'invoice_date': invoice_date,
                'invoice_date_due': due_date,  # Add due date
                'name': invoice_data['document_number'],
                'ref': invoice_reference,  # Invoice reference field
                'payment_reference': payment_reference,  # Payment reference field
                'journal_id': journal_id,  # Journal field
                'narration': invoice_data.get('note', ''),
                'invoice_line_ids': [(0, 0, invoice_line)],
            }

            try:
                invoice_id = models.execute_kw(db, uid, password,
                    'account.move', 'create',
                    [invoice_vals])

                if invoice_id:
                    print(f"Successfully created new invoice with ID: {invoice_id}")
                    # Verify the invoice was created properly
                    created_invoice = models.execute_kw(db, uid, password,
                        'account.move', 'search_read',
                        [[['id', '=', invoice_id]]],
                        {'fields': ['id', 'state', 'name']})
                    if created_invoice:
                        print(f"Invoice {created_invoice[0]['name']} created in {created_invoice[0]['state']} state")
                        return invoice_id
                    else:
                        print("Warning: Invoice created but verification failed")
                        return invoice_id
                else:
                    print("Failed to create invoice - no ID returned")
                    return False

            except xmlrpc.client.Fault as fault:
                print(f"XMLRPC Fault while creating invoice: {fault.faultString}")
                if 'access' in fault.faultString.lower():
                    print("Access rights issue detected - please check user permissions")
                return False
            except Exception as e:
                print(f"Unexpected error while creating invoice: {str(e)}")
                print(f"Invoice values that caused error: {invoice_vals}")
                return False

    except Exception as e:
        print(f"Error processing invoice: {str(e)}")
        return False

def main():
    try:
        # Connect to Odoo
        uid, models = connect_to_odoo()
        print("Successfully connected to Odoo")

        # Create import log file
        log_file = create_import_log()
        print(f"Created import log file: {log_file}")

        # Read Excel file
        df = read_excel_file()
        print("Successfully read Excel file")
        
        # Initialize counters
        total_rows = len(df)
        success_count = 0
        error_count = 0
        skipped_count = 0

        # Group rows by document number
        grouped_df = df.groupby('name')

        # Process each document
        for doc_number, group in grouped_df:
            try:
                print(f"\nProcessing document number: {doc_number}")
                print(f"Number of lines: {len(group)}")
                
                first_row = group.iloc[0]
                first_row_number = group.index[0] + 2  # Adding 2 because Excel starts at 1 and header row

                # Convert invoice_date to string format if it's a datetime
                invoice_date = first_row['invoice_date']
                if pd.notna(invoice_date):
                    if isinstance(invoice_date, pd.Timestamp):
                        invoice_date = invoice_date.strftime('%Y-%m-%d')
                    else:
                        try:
                            date_obj = pd.to_datetime(invoice_date)
                            invoice_date = date_obj.strftime('%Y-%m-%d')
                        except:
                            print(f"Warning: Could not parse date: {invoice_date}")
                            invoice_date = str(invoice_date).strip()

                # Process each line in the document
                for index, row in group.iterrows():
                    row_number = index + 2  # Adding 2 because Excel starts at 1 and header row
                    try:
                        # Process due date
                        due_date = row.get('due_date')
                        if pd.notna(due_date):
                            if isinstance(due_date, pd.Timestamp):
                                due_date = due_date.strftime('%Y-%m-%d')
                            else:
                                try:
                                    date_obj = pd.to_datetime(due_date)
                                    due_date = date_obj.strftime('%Y-%m-%d')
                                except:
                                    print(f"Warning: Could not parse due date: {due_date}, using invoice date")
                                    due_date = invoice_date

                        # Clean and prepare data with validation
                        invoice_data = {
                            'invoice_date': invoice_date if pd.notna(invoice_date) else None,
                            'due_date': due_date if pd.notna(due_date) else invoice_date,  # Use invoice date if no due date
                            'customer_name': clean_and_validate_data(row['partner_id'], 'partner_id'),
                            'partner_code': clean_and_validate_data(row.get('partner_code'), 'partner_code'),
                            'default_code': clean_and_validate_data(row['default_code'], 'default_code'),
                            'price_unit': clean_and_validate_data(row['price_unit'], 'price_unit'),
                            'document_number': clean_and_validate_data(row['name'], 'name'),
                            'payment_reference': clean_and_validate_data(row['payment_reference'], 'payment_reference'),
                            'invoice_reference': clean_and_validate_data(row['bill_reference'], 'bill_reference'),
                            'journal': clean_and_validate_data(row['journal'], 'journal'),
                            'note': clean_and_validate_data(row.get('note'), 'note'),
                            'analytic_distribution': clean_and_validate_data(row.get('analytic_distribution'), 'analytic_distribution'),
                            'product_name': clean_and_validate_data(row.get('product_name'), 'product_name'),
                            'label': clean_and_validate_data(row.get('label'), 'label'),
                            'quantity': clean_and_validate_data(row.get('quantity', 1.0), 'quantity'),
                            'revenue_account': clean_and_validate_data(row.get('expense_account'), 'expense_account'),
                        }

                        print(f"\nProcessing line {row_number}:")
                        print(f"Product code: {invoice_data['default_code']}")
                        print(f"Product name: {invoice_data['product_name']}")
                        print(f"Quantity: {invoice_data['quantity']}")
                        print(f"Price: {invoice_data['price_unit']}")

                        # Process the invoice
                        result = update_or_create_invoice(uid, models, invoice_data)
                        if result:
                            # After invoice is created, update the debit account from revenue_account
                            update_result = update_move_line_account(uid, models, result, {
                                'revenue_account': row.get('expense_account')
                            })
                            if update_result:
                                message = "Successfully processed and updated debit account"
                            else:
                                message = "Invoice created but debit account update failed"
                            print(f"Line {row_number}: {message}")
                            log_import_result(log_file, invoice_data, 'Success', message, row_number)
                            success_count += 1
                        else:
                            message = "Failed to process line"
                            print(f"Line {row_number}: {message}")
                            log_import_result(log_file, invoice_data, 'Error', message, row_number)
                            error_count += 1

                    except ValueError as ve:
                        message = f"Validation error: {str(ve)}"
                        print(f"Line {row_number}: {message}")
                        log_import_result(log_file, {'document_number': row.get('name', '')}, 'Error', message, row_number)
                        error_count += 1
                        continue

            except Exception as e:
                message = f"Error processing document: {str(e)}"
                print(f"Document {doc_number}: {message}")
                log_import_result(log_file, {'document_number': doc_number}, 'Error', message, first_row_number)
                error_count += len(group)
                continue

        # Print summary
        print("\nImport Summary:")
        print(f"Total rows processed: {total_rows}")
        print(f"Successfully imported: {success_count}")
        print(f"Errors: {error_count}")
        print(f"Skipped: {skipped_count}")
        print(f"\nDetailed log file: {log_file}")

    except Exception as e:
        print(f"Error in main process: {str(e)}")
        if 'log_file' in locals():
            log_import_result(log_file, {}, 'Error', f"Main process error: {str(e)}", 0)

if __name__ == "__main__":
    main()