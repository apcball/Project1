import xmlrpc.client
import pandas as pd

# --- Connection Settings ---
url = 'http://mogth.work:8069'
db = 'MOG_LIVE'
username = 'apichart@mogen.co.th'
password = '471109538'

# --- Connect to Odoo ---
common = xmlrpc.client.ServerProxy(f'{url}/xmlrpc/2/common')
uid = common.authenticate(db, username, password, {})
models = xmlrpc.client.ServerProxy(f'{url}/xmlrpc/2/object')

def update_customer_by_partner_code(partner_code, update_vals):
    # Search for the partner by code
    partner_ids = models.execute_kw(
        db, uid, password,
        'res.partner', 'search',
        [[['ref', '=', partner_code]]]
    )
    if not partner_ids:
        print(f"No customer found with partner code: {partner_code}")
        return False

    # Update the customer record
    result = models.execute_kw(
        db, uid, password,
        'res.partner', 'write',
        [partner_ids, update_vals]
    )
    if result:
        print(f"Customer with partner code {partner_code} updated successfully.")
    else:
        print(f"Failed to update customer with partner code {partner_code}.")
    return result

def update_invoice_partner_from_excel(excel_path):
    # Read Excel file
    df = pd.read_excel(excel_path)
    print("Excel columns:", list(df.columns))  # Debug: print actual column names
    for idx, row in df.iterrows():
        # Try to get the correct column names, ignoring case and spaces
        columns = {col.strip().lower().replace(' ', '_'): col for col in df.columns}
        display_col = columns.get('display_name') or columns.get('displayname')
        partner_col = columns.get('partner_code') or columns.get('partnercode')
        if not display_col or not partner_col:
            print("Could not find required columns in Excel. Found columns:", list(df.columns))
            return
        display_name = str(row[display_col]).strip()
        partner_code = str(row[partner_col]).strip()
        if not display_name or not partner_code:
            print(f"Row {idx+2}: Missing Display_name or Partner Code, skipping.")
            continue

        # Find the invoice or credit note by name (Display_name)
        invoice_ids = models.execute_kw(
            db, uid, password,
            'account.move', 'search',
            [[['name', '=', display_name], ['move_type', 'in', ['out_invoice', 'out_refund']]]]
        )
        if not invoice_ids:
            print(f"Invoice or credit note '{display_name}' not found.")
            continue

        # Find the partner by Partner Code (use 'partner_code' field instead of 'ref')
        search_code = partner_code.strip()
        print(f"Searching for partner_code: '{search_code}' (raw: '{partner_code}')")
        partner_ids = models.execute_kw(
            db, uid, password,
            'res.partner', 'search',
            [[['partner_code', '=', search_code]]]
        )
        if not partner_ids:
            # Try a 'like' search for similar partner codes, including inactive
            similar_partners = models.execute_kw(
                db, uid, password,
                'res.partner', 'search_read',
                [[['partner_code', 'ilike', search_code]]],
                {'fields': ['id', 'name', 'partner_code', 'active']}
            )
            if similar_partners:
                print(f"Partner with code '{search_code}' not found. Similar codes (including inactive):")
                for p in similar_partners:
                    print(f"  - {p['partner_code']}: {p['name']} (Active: {p['active']})")
            else:
                print(f"Partner with code '{search_code}' not found and no similar codes found (including inactive).")
            continue

        # Update the partner on the invoice
        try:
            result = models.execute_kw(
                db, uid, password,
                'account.move', 'write',
                [invoice_ids, {'partner_id': partner_ids[0]}]
            )
            if result:
                print(f"Updated invoice '{display_name}' with partner '{partner_code}'.")
            else:
                print(f"Failed to update invoice '{display_name}'.")
        except xmlrpc.client.Fault as e:
            print(f"Could not update invoice '{display_name}': {e}")
            continue

# Example usage:
if __name__ == "__main__":
    partner_code = "C0001"
    update_vals = {
        'name': 'Updated Customer Name',
        'email': 'updated_email@example.com'
    }
    update_customer_by_partner_code(partner_code, update_vals)

    excel_path = r"C:\Users\Ball\Documents\Git_apcball\Project1\Data_file\Account OB partner code.xlsx"
    update_invoice_partner_from_excel(excel_path)