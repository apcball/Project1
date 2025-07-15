import xmlrpc.client
import pandas as pd
from datetime import datetime
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('update_log.txt'),
        logging.StreamHandler()
    ]
)

# Server configuration
HOST = 'http://mogth.work:8069'
DB = 'MOG_LIVE'
USERNAME = 'apichart@mogen.co.th'
PASSWORD = '471109538'

def connect_to_odoo():
    """Establish connection to Odoo server"""
    try:
        common = xmlrpc.client.ServerProxy('{}/xmlrpc/2/common'.format(HOST))
        uid = common.authenticate(DB, USERNAME, PASSWORD, {})
        models = xmlrpc.client.ServerProxy('{}/xmlrpc/2/object'.format(HOST))
        return uid, models
    except Exception as e:
        logging.error(f"Failed to connect to Odoo: {str(e)}")
        return None, None


def update_so_partner_code(excel_path):
    """
    Read Excel file, find Sale Orders by Name, and update Partner Code in Odoo.
    """
    uid, models = connect_to_odoo()
    if not uid or not models:
        logging.error("Odoo connection failed.")
        return

    try:
        df = pd.read_excel(excel_path)
    except Exception as e:
        logging.error(f"Failed to read Excel file: {str(e)}")
        return

    if 'Name' not in df.columns or 'Partner Code' not in df.columns:
        logging.error("Excel file must contain 'Name' and 'Partner Code' columns.")
        return

    for idx, row in df.iterrows():
        so_name = str(row['Name']).strip()
        partner_code = str(row['Partner Code']).strip()
        if not so_name or not partner_code:
            logging.warning(f"Row {idx}: Missing SO Name or Partner Code.")
            continue

        # Search for Sale Order by name
        try:
            so_ids = models.execute_kw(
                DB, uid, PASSWORD,
                'sale.order', 'search',
                [[['name', '=', so_name]]]
            )
            if not so_ids:
                logging.warning(f"Sale Order '{so_name}' not found.")
                continue
            if len(so_ids) > 1:
                logging.warning(f"Multiple Sale Orders found for '{so_name}': {so_ids}. Skipping update.")
                continue

            # Find partner by partner_code (custom field)
            partner_ids = models.execute_kw(
                DB, uid, PASSWORD,
                'res.partner', 'search',
                [[['partner_code', '=', partner_code]]]
            )
            if not partner_ids:
                # Try to log available fields for debug
                try:
                    sample_ids = models.execute_kw(DB, uid, PASSWORD, 'res.partner', 'search', [[['partner_code', '!=', False]]], {'limit': 5})
                    sample_partners = models.execute_kw(DB, uid, PASSWORD, 'res.partner', 'read', [sample_ids], {'fields': ['id', 'name', 'partner_code']})
                    logging.warning(f"Partner with code '{partner_code}' not found. Sample partners with partner_code: {sample_partners}")
                except Exception as e:
                    logging.warning(f"Partner with code '{partner_code}' not found. Could not fetch sample partners: {str(e)}")
                continue

            # Update Sale Order's partner_id
            models.execute_kw(
                DB, uid, PASSWORD,
                'sale.order', 'write',
                [so_ids, {'partner_id': partner_ids[0]}]
            )
            logging.info(f"Updated SO '{so_name}' with Partner Code '{partner_code}'.")
        except Exception as e:
            logging.error(f"Row {idx}: Error updating SO '{so_name}': {str(e)}")


if __name__ == "__main__":
    import sys
    if len(sys.argv) < 2:
        print("Usage: python update_SO_Partner_Code.py <excel_file_path>")
    else:
        update_so_partner_code(sys.argv[1])

