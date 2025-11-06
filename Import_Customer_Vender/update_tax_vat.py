import xmlrpc.client
import pandas as pd
import sys
import logging
from typing import Dict, Any

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('vendor_import.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Configuration
CONFIG = {
    'url': 'http://mogth.work:8069',
    'db': 'MOG_SETUP',  # Changed to match the actual database name
    'username': 'apichart@mogen.co.th',
    'password': '471109538',
    'excel_path': 'Import_Customer_Vender/Contact MOG.xlsx'
}

# Set to True for dry-run (no changes), False to perform actual updates
DRY_RUN = False

# Simple in-memory caches
field_cache = {}

def connect_to_odoo():
    """Establish connection to Odoo server"""
    try:
        common = xmlrpc.client.ServerProxy(f'{CONFIG["url"]}/xmlrpc/2/common')
        uid = common.authenticate(CONFIG['db'], CONFIG['username'], CONFIG['password'], {})
        
        if not uid:
            logger.error("Authentication failed")
            sys.exit(1)
        
        logger.info(f"Authentication successful, uid = {uid}")
        models = xmlrpc.client.ServerProxy(f'{CONFIG["url"]}/xmlrpc/2/object')
        return uid, models
    
    except Exception as e:
        logger.error(f"Connection error: {e}")
        sys.exit(1)

def has_field(models, db, uid, password, model: str, field: str) -> bool:
    """Check if a field exists on a model."""
    key = f"{model}.{field}"
    if key in field_cache:
        return field_cache[key]
    try:
        res = models.execute_kw(db, uid, password, model, 'fields_get', [field])
        field_cache[key] = bool(res)
        return field_cache[key]
    except Exception:
        field_cache[key] = False
        return False

def find_partner_by_codes(models, db, uid, password, old_code: str = None, partner_code: str = None):
    """Find partner by partner_code (ref, partner_code field, old_code_partner = partner_code, old_code_partner = old_code). Return id or False."""
    try:
        # First, try by partner_code (ref)
        if partner_code:
            logger.info(f"Searching for partner by ref = {partner_code}")
            res = models.execute_kw(db, uid, password, 'res.partner', 'search', [[('ref', '=', partner_code)]], {'limit': 1})
            if res:
                logger.info(f"Found partner {res[0]} by ref = {partner_code}")
                return res[0]
            else:
                logger.info(f"Not found by ref = {partner_code}")
        
        # If not found, try by partner_code field if exists
        if partner_code and has_field(models, db, uid, password, 'res.partner', 'partner_code'):
            logger.info(f"Searching for partner by partner_code = {partner_code}")
            res = models.execute_kw(db, uid, password, 'res.partner', 'search', [[('partner_code', '=', partner_code)]], {'limit': 1})
            if res:
                logger.info(f"Found partner {res[0]} by partner_code = {partner_code}")
                return res[0]
            else:
                logger.info(f"Not found by partner_code = {partner_code}")
        
        # If not found, try by old_code_partner = partner_code if field exists
        if partner_code and has_field(models, db, uid, password, 'res.partner', 'old_code_partner'):
            logger.info(f"Searching for partner by old_code_partner = {partner_code}")
            res = models.execute_kw(db, uid, password, 'res.partner', 'search', [[('old_code_partner', '=', partner_code)]], {'limit': 1})
            if res:
                logger.info(f"Found partner {res[0]} by old_code_partner = {partner_code}")
                return res[0]
            else:
                logger.info(f"Not found by old_code_partner = {partner_code}")
        
        # If still not found, try by old_code_partner = old_code if provided and field exists
        if old_code and has_field(models, db, uid, password, 'res.partner', 'old_code_partner'):
            logger.info(f"Searching for partner by old_code_partner = {old_code}")
            res = models.execute_kw(db, uid, password, 'res.partner', 'search', [[('old_code_partner', '=', old_code)]], {'limit': 1})
            if res:
                logger.info(f"Found partner {res[0]} by old_code_partner = {old_code}")
                return res[0]
            else:
                logger.info(f"Not found by old_code_partner = {old_code}")
        
        logger.info(f"No partner found for partner_code={partner_code}, old_code={old_code}")
        return False
    except Exception as e:
        logger.warning(f"Partner lookup failed: {e}")
    return False

def update_partner_vat(models, db, uid, password, partner_id: int, vat: str, dry_run: bool = False):
    """Update the vat field of a partner, or Thai tax fields if available."""
    try:
        if dry_run:
            logger.info(f"DRY-RUN: Would update partner {partner_id} vat to {vat}")
            return True
        
        # Check if Thai localization fields exist
        if has_field(models, db, uid, password, 'res.partner', 'l10n_th_tax_id') and has_field(models, db, uid, password, 'res.partner', 'l10n_th_branch'):
            # Split VAT into tax_id (first 10) and branch (last 3)
            if len(vat) == 13:
                tax_id = vat[:10]
                branch = vat[10:]
                try:
                    models.execute_kw(db, uid, password, 'res.partner', 'write', [[partner_id], {'l10n_th_tax_id': tax_id, 'l10n_th_branch': branch}])
                    logger.info(f"Updated partner {partner_id} Thai tax_id to {tax_id}, branch to {branch}")
                    return True
                except Exception as e:
                    if 'Invalid field' in str(e):
                        # Fall back to updating vat
                        models.execute_kw(db, uid, password, 'res.partner', 'write', [[partner_id], {'vat': vat}])
                        logger.info(f"Updated partner {partner_id} vat to {vat} (fell back from Thai fields)")
                        return True
                    else:
                        raise
            else:
                # If not 13 digits, update vat
                models.execute_kw(db, uid, password, 'res.partner', 'write', [[partner_id], {'vat': vat}])
                logger.info(f"Updated partner {partner_id} vat to {vat} (not 13 digits)")
                return True
        else:
            # Update vat, and set branch to '000' if field exists to avoid validation
            vals = {'vat': vat}
            if has_field(models, db, uid, password, 'res.partner', 'l10n_th_branch'):
                vals['l10n_th_branch'] = '000'
            models.execute_kw(db, uid, password, 'res.partner', 'write', [[partner_id], vals])
            logger.info(f"Updated partner {partner_id} vat to {vat}")
            return True
    except Exception as e:
        logger.error(f"Failed to update partner {partner_id} vat: {e}")
        return False

def read_excel_file(file_path: str) -> pd.DataFrame:
    """Read Excel file"""
    try:
        df = pd.read_excel(file_path)
        logger.info(f"Read {len(df)} rows from {file_path}")
        return df
    except Exception as e:
        logger.error(f"Failed to read Excel file: {e}")
        sys.exit(1)

def normalize_str(value):
    """Normalize string value"""
    if pd.isna(value):
        return None
    try:
        # Convert to int to remove decimals, then format as 13-digit string with leading zeros
        num = int(float(value))
        return str(num).zfill(13)
    except (ValueError, TypeError):
        return str(value).strip()

def main():
    uid, models = connect_to_odoo()
    password = CONFIG['password']
    db = CONFIG['db']

    df = read_excel_file(CONFIG['excel_path'])
    
    # Use the DRY_RUN setting
    dry_run = DRY_RUN
    
    updated = 0
    failed = 0
    
    for idx, row in df.iterrows():
        partner_code = normalize_str(row.get('Partner Code'))
        old_code_partner = normalize_str(row.get('Old Code Partner'))
        vat = normalize_str(row.get('Tax ID'))
        
        if not vat:
            logger.warning(f"Skipping row {idx}: missing vat")
            failed += 1
            continue
        
        partner_id = find_partner_by_codes(models, db, uid, password, old_code_partner, partner_code)
        if partner_id:
            if update_partner_vat(models, db, uid, password, partner_id, vat, dry_run):
                updated += 1
            else:
                failed += 1
        else:
            logger.warning(f"Partner not found for row {idx}: partner_code={partner_code}, old_code={old_code_partner}")
            failed += 1
    
    logger.info(f"Update finished: updated={updated}, failed={failed}")

if __name__ == '__main__':
    main()