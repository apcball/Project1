#!/usr/bin/env python3
import xmlrpc.client
import pandas as pd
from datetime import datetime
import os
import json
import ast

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

def truncate_string(value, max_length=500):
    """Truncate string to maximum length while preserving words"""
    if not value or len(str(value)) <= max_length:
        return value
    truncated = str(value)[:max_length-3].rsplit(' ', 1)[0]
    return truncated + '...'

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
            
    # Truncate long strings
    return truncate_string(cleaned_value, max_length)

def read_excel_file():
    file_path = 'Data_file/import_journal_ค้างจ่าย.xlsx'
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
    
    # Read Excel file
    df = pd.read_excel(file_path, dtype=str)
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

def update_move_line_account(uid, models, move_id, line_data):
    """Update account in journal items after bill is posted - only for the credit line"""
    try:
        # Get the expense account code from line_data
        expense_account_code = line_data.get('expense_account')
        if not expense_account_code or pd.isna(expense_account_code):
            print("No expense account code provided, using default 214102")
            expense_account_code = '214102'  # Default to ค่าใช้จ่ายค้างจ่าย

        # Find the expense account
        expense_account_id = find_account_by_code(uid, models, expense_account_code)
        if not expense_account_id:
            print(f"Could not find expense account with code: {expense_account_code}, using default 214102")
            # Try to find default account 214102
            expense_account_id = find_account_by_code(uid, models, '214102')
            if not expense_account_id:
                print("Could not find default account 214102")
                return False

        # Get the credit line (payable line) for this bill
        move_lines = models.execute_kw(db, uid, password,
            'account.move.line', 'search_read',
            [[['move_id', '=', move_id], 
              ['credit', '>', 0]  # Get credit line
            ]],
            {'fields': ['id', 'account_id', 'name', 'credit']})

        if not move_lines:
            print("No credit line found")
            return False

        # Update the credit line's account
        try:
            models.execute_kw(db, uid, password,
                'account.move.line', 'write',
                [[move_lines[0]['id']], {'account_id': expense_account_id}])
            print(f"Successfully updated credit account to {expense_account_code}")
            return True
        except Exception as e:
            print(f"Error updating credit line: {str(e)}")
            return False

    except Exception as e:
        print(f"Error updating move line account: {str(e)}")
        return False

def find_account_by_code(uid, models, account_code):
    """Find account ID by code"""
    try:
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
        
        # Search for account with code
        account_ids = models.execute_kw(db, uid, password,
            'account.account', 'search_read',
            [[['code', '=', account_code]]],
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

        # Find account by code
        account_id = None
        if bill_data.get('account_code'):
            account_id = find_account_by_code(uid, models, bill_data['account_code'])
            if not account_id:
                print(f"Warning: Account not found with code: {bill_data['account_code']}")

        # Prepare bill line
        bill_line = {
            'product_id': product['id'],
            'name': bill_data['label'] or bill_data['product_name'] or product['name'],
            'quantity': bill_data['quantity'],
            'price_unit': bill_data['price_unit'],
            'analytic_distribution': analytic_distribution,
        }
        
        # Add account if found
        if account_id:
            bill_line['account_id'] = account_id

        if existing_bill:
            print(f"Found existing bill with number: {bill_data['document_number']}")
            
            # Check if bill is in draft state
            if existing_bill['state'] != 'draft':
                print(f"Cannot update bill {bill_data['document_number']} as it is not in draft state")
                return False

            # Add new line to existing bill
            update_vals = {
                'invoice_line_ids': [(0, 0, bill_line)],
            }

            # Update header fields only if they are different
            bill_reference = bill_data.get('bill_reference', '').strip()
            existing_bill_data = models.execute_kw(db, uid, password,
                'account.move', 'read',
                [existing_bill['id']],
                {'fields': ['partner_id', 'invoice_date', 'ref', 'payment_reference', 'narration']})

            if existing_bill_data:
                current_data = existing_bill_data[0]
                
                # Only update header fields if they are different or not set
                if current_data['partner_id'] and current_data['partner_id'][0] != vendor_id:
                    update_vals['partner_id'] = vendor_id
                if not current_data['invoice_date'] or current_data['invoice_date'] != bill_data['invoice_date']:
                    update_vals['invoice_date'] = bill_data['invoice_date']
                if not current_data['ref'] or current_data['ref'] != bill_reference:
                    update_vals['ref'] = bill_reference
                if not current_data['payment_reference'] or current_data['payment_reference'] != bill_data['payment_reference']:
                    update_vals['payment_reference'] = bill_data['payment_reference']
                if bill_data.get('note') and (not current_data['narration'] or current_data['narration'] != bill_data['note']):
                    update_vals['narration'] = bill_data['note']

            models.execute_kw(db, uid, password,
                'account.move', 'write',
                [[existing_bill['id']], update_vals])
            
            print(f"Successfully added line to existing bill: {existing_bill['id']}")
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
                        # Clean and prepare data with validation
                        bill_data = {
                            'invoice_date': invoice_date if pd.notna(invoice_date) else None,
                            'vendor_name': clean_and_validate_data(row['partner_id'], 'partner_id'),
                            'partner_code': clean_and_validate_data(row.get('partner_code'), 'partner_code'),
                            'default_code': clean_and_validate_data(row['default_code'], 'default_code'),
                            'price_unit': clean_and_validate_data(row['price_unit'], 'price_unit'),
                            'document_number': clean_and_validate_data(row['name'], 'name'),
                            'payment_reference': clean_and_validate_data(row['payment_reference'], 'payment_reference'),
                            'bill_reference': clean_and_validate_data(row['bill_reference'], 'bill_reference'),
                            'journal': clean_and_validate_data(row['journal'], 'journal'),
                            'note': clean_and_validate_data(row.get('note'), 'note'),
                            'analytic_distribution': clean_and_validate_data(row.get('analytic_distribution'), 'analytic_distribution'),
                            'product_name': clean_and_validate_data(row.get('product_name'), 'product_name'),
                            'label': clean_and_validate_data(row.get('label'), 'label'),
                            'quantity': clean_and_validate_data(row.get('quantity', 1.0), 'quantity'),
                            'expense_account': clean_and_validate_data(row.get('expense_account'), 'expense_account'),
                        }

                        print(f"\nProcessing line {row_number}:")
                        print(f"Product code: {bill_data['default_code']}")
                        print(f"Product name: {bill_data['product_name']}")
                        print(f"Quantity: {bill_data['quantity']}")
                        print(f"Price: {bill_data['price_unit']}")

                        # Process the bill
                        result = update_or_create_bill(uid, models, bill_data)
                        if result:
                            # After bill is created, update the credit account from expense_account
                            update_result = update_move_line_account(uid, models, result, {
                                'expense_account': row.get('expense_account')
                            })
                            if update_result:
                                message = "Successfully processed and updated credit account"
                            else:
                                message = "Bill created but credit account update failed"
                            print(f"Line {row_number}: {message}")
                            log_import_result(log_file, bill_data, 'Success', message, row_number)
                            success_count += 1
                        else:
                            message = "Failed to process line"
                            print(f"Line {row_number}: {message}")
                            log_import_result(log_file, bill_data, 'Error', message, row_number)
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