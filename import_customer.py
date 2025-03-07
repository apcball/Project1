import xmlrpc.client
import pandas as pd
import sys
import re

# --- Connection Settings ---
url = 'http://mogth.work:8069'
db = 'Pre_Test'
username = 'apichart@mogen.co.th'
password = '471109538'

# --- Authenticate with Odoo ---
try:
    common = xmlrpc.client.ServerProxy(f'{url}/xmlrpc/2/common')
    uid = common.authenticate(db, username, password, {})
    if not uid:
        print("Authentication failed: invalid credentials or insufficient permissions.")
        sys.exit(1)
    else:
        print("Authentication successful, uid =", uid)
except Exception as e:
    print("Error during connection/authentication:", e)
    sys.exit(1)

# --- Create XML-RPC models proxy ---
try:
    models = xmlrpc.client.ServerProxy(f'{url}/xmlrpc/2/object')
except Exception as e:
    print("Error creating XML-RPC models proxy:", e)
    sys.exit(1)

# --- Read the Excel file ---
file_path = r'C:\Users\Ball\Documents\Git_apcball\Project1\Data_file\customer_import.xlsx'
try:
    df = pd.read_excel(file_path)
    print("Column names in the Excel file:", df.columns.tolist())  # Print column names for verification
except Exception as e:
    print("Error reading Excel file:", e)
    sys.exit(1)

# --- Import customers ---
for index, row in df.iterrows():
    customer_data = {
        'old_code_partner ': row.get('Old Code Partner', False),
        'partner_code': row.get('Partner Code', False),
        'name': row.get('Name', False),  # Use get to avoid KeyError
        'company_type': row.get('Company Type', 'person'),
        'is_company': row.get('Is Company', False),
        'parent_id': row.get('Parent ID', False),
        'street': row.get('Street', False),
        'street2': row.get('Street2', False),
        'city': row.get('City', False),
        'state_id': row.get('State ID', False),
        'zip': row.get('Zip', False),
        'country_code': row.get('Country Code', False),
        'vat': row.get('VAT', False),
        'phone': row.get('Phone', False),
        'mobile': row.get('Mobile', False),
        'user_id': row.get('User ID', False),
        'property_payment_term_id': row.get('Payment Term ID', False),
        'lang': row.get('Language', False),
        'customer_rank': 1,  # Set customer rank to 1 to mark as a customer
        'active': row.get('Active', True),
    }

    try:
        # Check if the customer already exists by email
        existing_customer_id = models.execute_kw(db, uid, password, 'res.partner', 'search', [[['email', '=', row.get('Email', False)]]])
        
        if existing_customer_id:
            # Update existing customer
            models.execute_kw(db, uid, password, 'res.partner', 'write', [existing_customer_id, customer_data])
            print(f"Updated customer: {row.get('Name', 'Unknown')}")
        else:
            # Create new customer
            new_customer_id = models.execute_kw(db, uid, password, 'res.partner', 'create', [customer_data])
            print(f"Created new customer: {row.get('Name', 'Unknown')}, ID: {new_customer_id}")
    except Exception as e:
        print(f"Error processing customer {row.get('Name', 'Unknown')}: {e}")

print("Customer import completed.")