cldimport xmlrpc.client
import pandas as pd
import sys

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
    # Get state_id from the state name
    state_name = row.get('state_id', False)
    state_id = False
    if state_name:
        state_id = models.execute_kw(db, uid, password, 'res.country.state', 'search', [[['name', '=', state_name]]])
        state_id = state_id[0] if state_id else False  # Get the first matching state ID

    # Validate and clean data
    zip_code = row.get('zip', False)
    if isinstance(zip_code, (int, float)):
        zip_code = str(int(zip_code))  # Convert to string to avoid integer out of range error
    if isinstance(zip_code, str):
        zip_code = zip_code.strip()  # Remove leading and trailing spaces

    # Additional validation and logging for zip_code
    if not zip_code.isdigit():
        print(f"Invalid zip code for customer {row.get('name', 'Unknown')}: {zip_code}")
        continue

    phone = row.get('phone', '')
    mobile = row.get('mobile', '')

    # Ensure phone and mobile are strings
    if isinstance(phone, (int, float)):
        phone = str(phone)
    if isinstance(mobile, (int, float)):
        mobile = str(mobile)

    # Validate property_payment_term_id
    property_payment_term_id = row.get('property_payment_term_id', False)
    if isinstance(property_payment_term_id, float):
        property_payment_term_id = int(property_payment_term_id)  # Convert to integer if it's a float

    customer_data = {
        'partner_code': row.get('partner_code', False),
        'name': row.get('name', False),
        'company_type': row.get('company_type', 'person'),
        'is_company': bool(row.get('is_company', False)),  # Ensure this is a boolean
        'parent_id': row.get('parent_id/id', False),
        'street': row.get('street', False),
        'street2': row.get('street2', False),
        'city': row.get('city', False),
        'state_id': state_id,  # Use the resolved state ID
        'zip': zip_code,  # Use the validated zip code as a string
        'country_code': row.get('country_code', False),
        'vat': row.get('vat', False),
        'phone': phone,  # Ensure phone is a string
        'mobile': mobile,  # Ensure mobile is a string
        'property_payment_term_id': property_payment_term_id,  # Use the validated payment term ID
        'customer_rank': 1,  # Set customer rank to 1 to mark as a customer
        'active': bool(row.get('active', True)),  # Ensure this is a boolean
    }

    # Debugging: Print customer_data to check values
    print(f"Processing customer data: {customer_data}")

    try:
        # Check if the customer already exists by partner_code
        existing_customer_id = models.execute_kw(db, uid, password, 'res.partner', 'search', [[['partner_code', '=', row.get('partner_code', False)]]])

        if existing_customer_id:
            # Update existing customer
            models.execute_kw(db, uid, password, 'res.partner', 'write', [existing_customer_id, customer_data])
            print(f"Updated customer: {row.get('name', 'Unknown')}")
        else:
            # Create new customer
            new_customer_id = models.execute_kw(db, uid, password, 'res.partner', 'create', [customer_data])
            print(f"Created new customer: {row.get('name', 'Unknown')}, ID: {new_customer_id}")
    except Exception as e:
        print(f"Error processing customer {row.get('name', 'Unknown')}: {e}")

print("Customer import completed.")