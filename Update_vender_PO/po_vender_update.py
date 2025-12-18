# Global configuration
CONFIG = {
    'server_url': 'http://160.187.249.148:8069',
    'database': 'MOG_LIVE',
    'username': 'apichart@mogen.co.th',
    'password': '471109538',
    'log_dir': 'Import_PO/logs',
    'data_file': 'C:/Users/Ball/Documents/Git_apcball/Project1/Update_vender_PO/po_vender_update.xlsx',
    'dry_run': False
}

import xmlrpc.client
import pandas as pd
from datetime import datetime
import logging
import os

# Create log directory if it doesn't exist
if not os.path.exists(CONFIG['log_dir']):
    os.makedirs(CONFIG['log_dir'])

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(CONFIG['log_dir'], 'po_vender_update.log')),
        logging.StreamHandler()
    ]
)

def connect_to_odoo():
    """Establish connection to Odoo server"""
    try:
        common = xmlrpc.client.ServerProxy(f"{CONFIG['server_url']}/xmlrpc/2/common")
        uid = common.authenticate(CONFIG['database'], CONFIG['username'], CONFIG['password'], {})
        models = xmlrpc.client.ServerProxy(f"{CONFIG['server_url']}/xmlrpc/2/object")
        return uid, models
    except Exception as e:
        logging.error(f"Failed to connect to Odoo: {str(e)}")
        return None, None

def update_po_partner_code():
    """
    Read Excel file, find Purchase Orders by reference, and update Partner Code in Odoo.
    """
    uid, models = connect_to_odoo()
    if not uid or not models:
        logging.error("Odoo connection failed.")
        return

    try:
        df = pd.read_excel(CONFIG['data_file'])
    except Exception as e:
        logging.error(f"Failed to read Excel file: {str(e)}")
        return

    if 'reference' not in df.columns or 'partner_code' not in df.columns:
        logging.error("Excel file must contain 'reference' and 'partner_code' columns.")
        return

    for idx, row in df.iterrows():
        po_reference = str(row['reference']).strip()
        partner_code = str(row['partner_code']).strip()
        if not po_reference or not partner_code:
            logging.warning(f"Row {idx}: Missing PO reference or Partner Code.")
            continue

        # Search for Purchase Order by name (reference)
        try:
            po_ids = models.execute_kw(
                CONFIG['database'], uid, CONFIG['password'],
                'purchase.order', 'search',
                [[['name', '=', po_reference]]]
            )
            if not po_ids:
                logging.warning(f"Purchase Order '{po_reference}' not found.")
                continue
            if len(po_ids) > 1:
                logging.warning(f"Multiple Purchase Orders found for '{po_reference}': {po_ids}. Skipping update.")
                continue

            # Find partner by partner_code (assuming custom field or ref)
            partner_ids = models.execute_kw(
                CONFIG['database'], uid, CONFIG['password'],
                'res.partner', 'search',
                [[['ref', '=', partner_code]]]  # Assuming 'ref' is the partner code field
            )
            if not partner_ids:
                # Try alternative search if ref doesn't work
                partner_ids = models.execute_kw(
                    CONFIG['database'], uid, CONFIG['password'],
                    'res.partner', 'search',
                    [[['partner_code', '=', partner_code]]]
                )
            if not partner_ids:
                logging.warning(f"Partner with code '{partner_code}' not found.")
                continue

            # Update Purchase Order's partner_id
            if not CONFIG['dry_run']:
                models.execute_kw(
                    CONFIG['database'], uid, CONFIG['password'],
                    'purchase.order', 'write',
                    [po_ids, {'partner_id': partner_ids[0]}]
                )
                logging.info(f"Updated PO '{po_reference}' with Partner Code '{partner_code}'.")
            else:
                logging.info(f"Dry run: Would update PO '{po_reference}' with Partner Code '{partner_code}'.")
        except Exception as e:
            logging.error(f"Row {idx}: Error updating PO '{po_reference}': {str(e)}")

if __name__ == "__main__":
    update_po_partner_code()

