#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import xmlrpc.client
import pandas as pd
from datetime import datetime
import os
import json
import ast
from openpyxl import load_workbook
import sys
import locale

# Set console encoding for Windows
if sys.platform == 'win32':
    try:
        # Try to set console to UTF-8
        os.system('chcp 65001 > nul')
    except:
        pass

def safe_print(text):
    """Safely print text that might contain non-ASCII characters"""
    try:
        print(text)
    except UnicodeEncodeError:
        # If encoding fails, try to encode with errors='replace'
        try:
            print(str(text).encode('cp850', errors='replace').decode('cp850'))
        except:
            # Last resort: replace problematic characters
            safe_text = str(text).encode('ascii', errors='replace').decode('ascii')
            print(safe_text)

# --- Connection Settings ---
url = 'http://mogth.work:8069'
db = 'Test_import'
username = 'apichart@mogen.co.th'
password = '471109538'

# Data file path
data_file = r'C:\Users\Ball\Documents\Git_apcball\Project1\Import_Accounting\Template_Bill_Refunds.xlsx'

# Function to connect to Odoo
def connect_to_odoo():
    common = xmlrpc.client.ServerProxy(f'{url}/xmlrpc/2/common')
    uid = common.authenticate(db, username, password, {})
    models = xmlrpc.client.ServerProxy(f'{url}/xmlrpc/2/object')
    return uid, models

# Configuration
DRY_RUN = False  # Toggle for dry run mode
LOG_ERRORS = True  # Enable error logging
SHOW_PROGRESS = True  # Show real-time progress

def convert_date_format(date_str):
    """Convert date string from various formats to YYYY-MM-DD format"""
    if not date_str or pd.isna(date_str):
        return None
        
    date_str = str(date_str).strip()
    
    try:
        # Handle both dd/mm/yy and mm/dd/yy formats
        if '/' in date_str:
            parts = date_str.split('/')
            if len(parts) == 3:
                # Try both mm/dd/yy and dd/mm/yy formats
                formats_to_try = [
                    {'month': 0, 'day': 1, 'year': 2},  # mm/dd/yy
                    {'day': 0, 'month': 1, 'year': 2}   # dd/mm/yy
                ]
                
                for date_format in formats_to_try:
                    try:
                        month = parts[date_format['month']]
                        day = parts[date_format['day']]
                        year = parts[date_format['year']]
                        
                        # Convert 2-digit year to 4-digit year
                        if len(year) == 2:
                            year = '20' + year  # Assuming years are in the 2000s
                        
                        # Ensure day and month are 2 digits
                        day = day.zfill(2)
                        month = month.zfill(2)
                        
                        # Validate the date
                        datetime(int(year), int(month), int(day))
                        return f"{year}-{month}-{day}"
                    except ValueError:
                        continue
                
                print(f"Could not parse date in any format: {date_str}")
                return None
                
        # Try to parse with pandas as fallback
        try:
            date_obj = pd.to_datetime(date_str)
            return date_obj.strftime('%Y-%m-%d')
        except:
            print(f"Could not parse date string: {date_str}")
            return None
            
    except Exception as e:
        print(f"Error converting date {date_str}: {str(e)}")
        return None
    
    return None

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
    """Read and validate Excel file"""
    if not os.path.exists(data_file):
        raise FileNotFoundError(f"Excel file not found at {data_file}")
    
    # Define expected columns with their max lengths
    field_limits = {
        'name': 255,               # เลขที่เอกสาร
        'invoice_date': 10,        # วันที่เอกสาร
        'date': 10,              # วันที่ลงบัญชี
        'partner_code': 64,        # รหัสลูกค้า
        'old_partner_code': 64,     # รหัสลูกค้า MGTX
        'journal': 64,            # สมุดรายวัน
        'partner_id': 500,        # ชื่อลูกค้า
        'ref': 255,              # เอกสารอ้างอิง
        'label': 1000,           # รายละเอียดการลง บัญชี
        'account_id': 64,        # account code ลง บัญชี
        'quantity': 0,           # จำนวน (numeric)
        'price_unit': 0,        # ราคาต่อหน่วย (numeric)
        'tax_ids': 64,          # ภาษีมูลค่าเพิ่ม
        'payment_reference': 255, # เอกสารอ้างอิง
        'note': 1000,           # หมายเหตุ
    }
    
    # Read Excel file with explicit date parsing
    df = pd.read_excel(data_file, sheet_name='Bill', dtype={'invoice_date': str, 'date': str})
    
    # Convert date columns to proper date format
    for date_col in ['invoice_date', 'date']:
        if date_col in df.columns:
            df[date_col] = df[date_col].apply(lambda x: convert_date_format(x) if pd.notna(x) else None)
    
    print("\nColumns in Excel file:", df.columns.tolist())
    
    # Verify required columns
    required_columns = ['name', 'partner_id', 'invoice_date', 'account_id', 'quantity', 'price_unit']
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

def get_or_create_vendor(uid, models, vendor_name, partner_code=None, old_partner_code=None):
    """Search for existing vendor by partner_code or old_partner_code, or create new one"""
    try:
        # Clean vendor name and partner codes
        vendor_name = vendor_name.strip()
        
        # First try to find vendor by partner_code if provided
        if partner_code:
            partner_code = str(partner_code).strip()
            if partner_code:  # Only search if not empty
                # Search by partner_code
                vendor = models.execute_kw(db, uid, password,
                    'res.partner', 'search_read',
                    [[['partner_code', '=', partner_code]]],
                    {'fields': ['id', 'name']})
                
                if vendor:
                    print(f"Found existing vendor by partner_code: {partner_code}")
                    return vendor[0]['id']
        
        # Try to find vendor by old_partner_code if provided
        if old_partner_code:
            old_partner_code = str(old_partner_code).strip()
            if old_partner_code:  # Only search if not empty
                # Search by old_partner_code
                vendor = models.execute_kw(db, uid, password,
                    'res.partner', 'search_read',
                    [[['old_code_partner', '=', old_partner_code]]],
                    {'fields': ['id', 'name']})
                
                if vendor:
                    print(f"Found existing vendor by old_partner_code: {old_partner_code}")
                    return vendor[0]['id']
        
        # If not found by codes, search by name
        if vendor_name:
            vendor = models.execute_kw(db, uid, password,
                'res.partner', 'search_read',
                [[['name', '=', vendor_name]]],
                {'fields': ['id', 'name']})
            
            if vendor:
                print(f"Found existing vendor by name: {vendor_name}")
                return vendor[0]['id']

        # If vendor not found and not in dry run mode, create new vendor
        if not DRY_RUN and vendor_name:
            new_vendor_vals = {
                'name': vendor_name,
                'supplier_rank': 1,  # Mark as vendor
                'company_type': 'company',  # Set as company by default
            }
            
            # Add partner codes if provided
            if partner_code:
                new_vendor_vals['partner_code'] = partner_code
            if old_partner_code:
                new_vendor_vals['old_code_partner'] = old_partner_code
            
            new_vendor_id = models.execute_kw(db, uid, password,
                'res.partner', 'create',
                [new_vendor_vals])
            
            print(f"Created new vendor: {vendor_name} with ID: {new_vendor_id}")
            return new_vendor_id
        elif DRY_RUN and vendor_name:
            print(f"[DRY RUN] Would create new vendor: {vendor_name}")
            return f"DRY_RUN_VENDOR_{vendor_name}"

        print("Error: No vendor name provided")
        return False

    except Exception as e:
        print(f"Error in vendor creation: {str(e)}")
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

def get_journal_id(uid, models, journal_name):
    """Find journal ID by name"""
    try:
        # Get default purchase journal as fallback
        default_journal = models.execute_kw(db, uid, password,
            'account.journal', 'search_read',
            [[['type', '=', 'purchase']]],
            {'fields': ['id', 'name', 'code'], 'limit': 1})
        
        default_journal_id = default_journal[0]['id'] if default_journal else False
        
        if not journal_name or pd.isna(journal_name):
            print(f"No journal specified, using default purchase journal")
            return default_journal_id
            
        journal_name = str(journal_name).strip()
        
        # Search for journal with various conditions
        domain = ['|',
            ['name', '=', journal_name],
            ['name', 'ilike', journal_name]
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
            print(f"Warning: Journal not found for: {journal_name}, using default purchase journal")
            return default_journal_id
            
    except Exception as e:
        print(f"Error finding journal: {str(e)}")
        return default_journal_id

def find_existing_bill(uid, models, document_number):
    """Find existing bill by document number"""
    if not document_number:
        return None
    
    # Build search domain
    domain = [
        ['name', '=', document_number],
        ['move_type', '=', 'in_invoice']
    ]
    
    # Search for existing bill
    bill_ids = models.execute_kw(db, uid, password,
        'account.move', 'search_read',
        [domain],
        {'fields': ['id', 'state', 'partner_id', 'invoice_date']})
    
    return bill_ids[0] if bill_ids else None

def create_import_log():
    """Create or get import log file"""
    import csv
    from datetime import datetime
    
    log_dir = 'logs'
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_file = f'{log_dir}/bill_import_log_{timestamp}.csv'
    
    # Create log file with headers
    with open(log_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow([
            'Timestamp',
            'Document Number',
            'Vendor Name',
            'Status',
            'Message',
            'Row Number'
        ])
    return log_file

def log_import_result(log_file, data, status, message, row_number):
    """Log import result to CSV file"""
    import csv
    from datetime import datetime
    
    try:
        with open(log_file, 'a', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow([
                datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                data.get('document_number', ''),
                data.get('vendor_name', ''),
                status,
                message,
                row_number
            ])
    except Exception as e:
        print(f"Warning: Could not write to log file: {str(e)}")

def display_progress(current, total, message=""):
    """Display real-time progress"""
    if SHOW_PROGRESS:
        percentage = (current / total) * 100 if total > 0 else 0
        print(f"\rProgress: {current}/{total} ({percentage:.1f}%) {message}", end="", flush=True)
        if current == total:
            print()  # New line when complete

def update_or_create_bill(uid, models, bill_data):
    """Update existing bill or create new one"""
    try:
        # Skip processing in dry run mode
        if DRY_RUN:
            print(f"[DRY RUN] Would process bill: {bill_data['document_number']}")
            return f"DRY_RUN_BILL_{bill_data['document_number']}"
        
        # Check if bill already exists
        existing_bill = find_existing_bill(uid, models, bill_data['document_number'])
        
        # Get or create vendor
        vendor_id = get_or_create_vendor(uid, models, bill_data['vendor_name'],
                                     bill_data.get('partner_code'), bill_data.get('old_partner_code'))
        if not vendor_id:
            print("Failed to get or create vendor")
            return False

        # Find account by code
        account_id = None
        if bill_data.get('account_id'):
            account_id = find_account_by_code(uid, models, bill_data['account_id'])
            if not account_id:
                print(f"Warning: Account not found with code: {bill_data['account_id']}")

        # Prepare bill line
        bill_line = {
            'name': bill_data['label'] or f"Line for {bill_data['document_number']}",
            'quantity': bill_data['quantity'],
            'price_unit': bill_data['price_unit'],
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
                if not current_data['ref'] or current_data['ref'] != bill_data.get('ref', ''):
                    update_vals['ref'] = bill_data.get('ref', '')
                if not current_data['payment_reference'] or current_data['payment_reference'] != bill_data.get('payment_reference', ''):
                    update_vals['payment_reference'] = bill_data.get('payment_reference', '')
                if bill_data.get('note') and (not current_data['narration'] or current_data['narration'] != bill_data['note']):
                    update_vals['narration'] = bill_data['note']

            if not DRY_RUN:
                models.execute_kw(db, uid, password,
                    'account.move', 'write',
                    [[existing_bill['id']], update_vals])
                print(f"Successfully added line to existing bill: {existing_bill['id']}")
            else:
                print(f"[DRY RUN] Would add line to existing bill: {existing_bill['id']}")
            
            return existing_bill['id']
        else:
            # Create new bill
            # Get journal_id
            journal_id = get_journal_id(uid, models, bill_data.get('journal'))
            
            print(f"Using journal_id: {journal_id}")
            
            bill_vals = {
                'move_type': 'in_invoice',
                'partner_id': vendor_id,
                'invoice_date': bill_data['invoice_date'],
                'date': bill_data.get('date', bill_data['invoice_date']),  # Use accounting date if provided
                'name': bill_data['document_number'],
                'ref': bill_data.get('ref', ''),  # Reference field
                'payment_reference': bill_data.get('payment_reference', ''),  # Payment reference field
                'journal_id': journal_id,  # Journal field
                'narration': bill_data.get('note', ''),
                'invoice_line_ids': [(0, 0, bill_line)],
            }

            if not DRY_RUN:
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
            else:
                print(f"[DRY RUN] Would create new bill with values: {bill_vals}")
                return f"DRY_RUN_BILL_{bill_data['document_number']}"

    except Exception as e:
        print(f"Error processing bill: {str(e)}")
        return False


def main():
    """Main function to orchestrate the bill import process"""
    try:
        safe_print(f"{'='*60}")
        safe_print(f"BILL IMPORT PROCESS")
        safe_print(f"{'='*60}")
        safe_print(f"Database: {db}")
        safe_print(f"Data File: {data_file}")
        safe_print(f"Dry Run Mode: {'ON' if DRY_RUN else 'OFF'}")
        safe_print(f"{'='*60}")
        
        # Connect to Odoo
        if not DRY_RUN:
            uid, models = connect_to_odoo()
            safe_print("Successfully connected to Odoo")
        else:
            safe_print("[DRY RUN] Skipping Odoo connection")
            uid, models = None, None

        # Create import log file
        log_file = create_import_log() if LOG_ERRORS else None
        if log_file:
            safe_print(f"Created import log file: {log_file}")

        # Read Excel file
        df = read_excel_file()
        safe_print("Successfully read Excel file")
        
        # Initialize counters
        total_rows = len(df)
        success_count = 0
        error_count = 0
        processed_count = 0

        # Group rows by document number
        grouped_df = df.groupby('name')
        total_documents = len(grouped_df)
        
        safe_print(f"\nFound {total_documents} unique documents with {total_rows} total lines")
        safe_print(f"Starting import process...")
        safe_print(f"{'='*60}")

        # Process each document
        doc_count = 0
        for doc_number, group in grouped_df:
            doc_count += 1
            try:
                safe_print(f"\nProcessing document {doc_count}/{total_documents}: {doc_number}")
                safe_print(f"Number of lines: {len(group)}")
                
                first_row = group.iloc[0]
                first_row_number = group.index[0] + 2  # Adding 2 because Excel starts at 1 and header row

                # Get invoice date from the first row
                invoice_date = first_row['invoice_date']
                if pd.notna(invoice_date):
                    invoice_date = convert_date_format(invoice_date)
                    if not invoice_date:
                        print(f"Warning: Invalid date format for {doc_number}")
                        invoice_date = None

                # Process each line in the document
                for index, row in group.iterrows():
                    row_number = index + 2  # Adding 2 because Excel starts at 1 and header row
                    processed_count += 1
                    
                    try:
                        # Clean and prepare data with validation
                        bill_data = {
                            'invoice_date': invoice_date if pd.notna(invoice_date) else None,
                            'date': row.get('date') if pd.notna(row.get('date')) else invoice_date,
                            'vendor_name': clean_and_validate_data(row['partner_id'], 'partner_id'),
                            'partner_code': clean_and_validate_data(row.get('partner_code'), 'partner_code'),
                            'old_partner_code': clean_and_validate_data(row.get('old_partner_code'), 'old_partner_code'),
                            'document_number': clean_and_validate_data(row['name'], 'name'),
                            'payment_reference': clean_and_validate_data(row.get('payment_reference'), 'payment_reference'),
                            'ref': clean_and_validate_data(row.get('ref'), 'ref'),
                            'journal': clean_and_validate_data(row.get('journal'), 'journal'),
                            'note': clean_and_validate_data(row.get('note'), 'note'),
                            'label': clean_and_validate_data(row.get('label'), 'label'),
                            'quantity': clean_and_validate_data(row.get('quantity', 1.0), 'quantity'),
                            'price_unit': clean_and_validate_data(row.get('price_unit', 0.0), 'price_unit'),
                            'account_id': clean_and_validate_data(row.get('account_id'), 'account_id'),
                        }

                        safe_print(f"\nProcessing line {row_number}:")
                        safe_print(f"  Vendor: {bill_data['vendor_name']}")
                        safe_print(f"  Account: {bill_data['account_id']}")
                        safe_print(f"  Quantity: {bill_data['quantity']}")
                        safe_print(f"  Price: {bill_data['price_unit']}")
                        safe_print(f"  Description: {bill_data['label']}")

                        # Process the bill
                        result = update_or_create_bill(uid, models, bill_data)
                        if result:
                            message = "Successfully processed"
                            safe_print(f"  ✓ {message}")
                            if log_file:
                                log_import_result(log_file, bill_data, 'Success', message, row_number)
                            success_count += 1
                        else:
                            message = "Failed to process"
                            safe_print(f"  ✗ {message}")
                            if log_file:
                                log_import_result(log_file, bill_data, 'Error', message, row_number)
                            error_count += 1

                        # Update progress
                        display_progress(processed_count, total_rows, f"- Doc: {doc_number}")

                    except ValueError as ve:
                        message = f"Validation error: {str(ve)}"
                        safe_print(f"  ✗ {message}")
                        if log_file:
                            log_import_result(log_file, {'document_number': row.get('name', '')}, 'Error', message, row_number)
                        error_count += 1
                        continue

            except Exception as e:
                message = f"Error processing document: {str(e)}"
                safe_print(f"  ✗ {message}")
                if log_file:
                    log_import_result(log_file, {'document_number': doc_number}, 'Error', message, first_row_number)
                error_count += len(group)
                continue

        # Print summary
        safe_print(f"\n{'='*60}")
        safe_print("IMPORT SUMMARY")
        safe_print(f"{'='*60}")
        safe_print(f"Total rows processed: {total_rows}")
        safe_print(f"Total documents: {total_documents}")
        safe_print(f"Successfully imported: {success_count}")
        safe_print(f"Errors: {error_count}")
        safe_print(f"Success rate: {(success_count/total_rows*100):.1f}%")
        
        if log_file:
            safe_print(f"\nDetailed log file: {log_file}")
        
        safe_print(f"{'='*60}")
        
        if DRY_RUN:
            safe_print("DRY RUN COMPLETED - No actual data was imported")
            safe_print("To perform actual import, set DRY_RUN = False at the top of the script")
        else:
            safe_print("IMPORT COMPLETED")
        
        safe_print(f"{'='*60}")

    except Exception as e:
        safe_print(f"Error in main process: {str(e)}")
        if 'log_file' in locals() and log_file:
            log_import_result(log_file, {}, 'Error', f"Main process error: {str(e)}", 0)

if __name__ == "__main__":
    main()
