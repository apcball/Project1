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
    'db': 'Test_import',  # Changed to match the actual database name
    'username': 'apichart@mogen.co.th',
    'password': '471109538',
    'excel_path': 'Import_Customer_Vender/Update Vender Payment Term.xlsx'
}

# Set to True for dry-run (no changes), False to perform actual updates
DRY_RUN = False  # Set to True for testing, change to False for production

# Simple in-memory caches
field_cache = {}
payment_term_cache = {}

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

def find_partner_by_old_code(models, db, uid, password, old_code: str):
    """Find partner by old_code_partner field. Return id or False."""
    try:
        if not old_code:
            logger.warning("No old_code_partner provided")
            return False
            
        logger.info(f"Searching for partner by old_code_partner = {old_code}")
        
        # First check if old_code_partner field exists
        if not has_field(models, db, uid, password, 'res.partner', 'old_code_partner'):
            logger.error("old_code_partner field does not exist in res.partner model")
            return False
        
        # Search for partner by old_code_partner
        res = models.execute_kw(db, uid, password, 'res.partner', 'search',
                               [[('old_code_partner', '=', old_code)]],
                               {'limit': 1})
        
        if res:
            logger.info(f"Found partner {res[0]} by old_code_partner = {old_code}")
            return res[0]
        else:
            logger.info(f"No partner found with old_code_partner = {old_code}")
            return False
            
    except Exception as e:
        logger.warning(f"Partner lookup failed: {e}")
        return False

def get_payment_term_id(models, db, uid, password, payment_term_name: str):
    """Get payment term ID by name with flexible matching. Return ID or False."""
    if not payment_term_name:
        return False
        
    # Check cache first
    if payment_term_name in payment_term_cache:
        return payment_term_cache[payment_term_name]
    
    try:
        logger.info(f"Searching for payment term: {payment_term_name}")
        
        # First try exact match
        payment_term_ids = models.execute_kw(db, uid, password, 'account.payment.term', 'search',
                                           [[('name', '=', payment_term_name)]],
                                           {'limit': 1})
        
        # If not found, try to match with numeric patterns (e.g., "0", "30", "60" days)
        if not payment_term_ids and payment_term_name.isdigit():
            days = int(payment_term_name)
            search_patterns = [
                f"{days} Days",  # This is the correct format based on debug output
                f"{days} Day",
                f"Net {days}",
                f"{days}",
                f"Immediate" if days == 0 else None
            ]
            
            for pattern in search_patterns:
                if pattern:
                    payment_term_ids = models.execute_kw(db, uid, password, 'account.payment.term', 'search',
                                                       [[('name', '=', pattern)]],
                                                       {'limit': 1})
                    if payment_term_ids:
                        break
            
            # If still not found, try ilike search
            if not payment_term_ids:
                payment_term_ids = models.execute_kw(db, uid, password, 'account.payment.term', 'search',
                                                   [[('name', 'ilike', payment_term_name)]],
                                                   {'limit': 1})
        
        if payment_term_ids:
            payment_term_id = payment_term_ids[0]
            # Get the actual name for logging
            payment_term_data = models.execute_kw(db, uid, password, 'account.payment.term', 'read',
                                                 [payment_term_id], {'fields': ['name']})
            actual_name = payment_term_data[0]['name'] if payment_term_data else 'Unknown'
            
            payment_term_cache[payment_term_name] = payment_term_id
            logger.info(f"Found payment term '{payment_term_name}' -> '{actual_name}' with ID {payment_term_id}")
            return payment_term_id
        else:
            logger.warning(f"Payment term not found: {payment_term_name}")
            payment_term_cache[payment_term_name] = False
            return False
            
    except Exception as e:
        logger.warning(f"Error searching for payment term {payment_term_name}: {e}")
        payment_term_cache[payment_term_name] = False
        return False

def update_vendor_payment_term(models, db, uid, password, partner_id: int, payment_term_id: int, dry_run: bool = False):
    """Update the property_supplier_payment_term_id field of a partner."""
    try:
        if dry_run:
            logger.info(f"DRY-RUN: Would update partner {partner_id} payment_term_id to {payment_term_id}")
            return True
        
        # Check if property_supplier_payment_term_id field exists
        if not has_field(models, db, uid, password, 'res.partner', 'property_supplier_payment_term_id'):
            logger.error("property_supplier_payment_term_id field does not exist in res.partner model")
            return False
        
        # Update the partner
        models.execute_kw(db, uid, password, 'res.partner', 'write',
                         [[partner_id], {'property_supplier_payment_term_id': payment_term_id}])
        
        logger.info(f"Updated partner {partner_id} property_supplier_payment_term_id to {payment_term_id}")
        return True
        
    except Exception as e:
        logger.error(f"Failed to update partner {partner_id} payment_term_id: {e}")
        return False

def read_excel_file(file_path: str) -> pd.DataFrame:
    """Read Excel file"""
    try:
        df = pd.read_excel(file_path)
        logger.info(f"Read {len(df)} rows from {file_path}")
        logger.info(f"Columns found: {df.columns.tolist()}")
        return df
    except Exception as e:
        logger.error(f"Failed to read Excel file: {e}")
        sys.exit(1)

def normalize_str(value):
    """Normalize string value"""
    if pd.isna(value):
        return None
    return str(value).strip()

def list_all_payment_terms(models, db, uid, password):
    """List all available payment terms in the system for debugging."""
    try:
        payment_terms = models.execute_kw(db, uid, password, 'account.payment.term', 'search_read',
                                        [[]], {'fields': ['id', 'name']})
        logger.info("Available payment terms in the system:")
        for term in payment_terms:
            logger.info(f"  ID: {term['id']}, Name: '{term['name']}'")
        return payment_terms
    except Exception as e:
        logger.error(f"Error listing payment terms: {e}")
        return []

def main():
    uid, models = connect_to_odoo()
    password = CONFIG['password']
    db = CONFIG['db']

    # Optional: List all payment terms for debugging (comment out in production)
    # Uncomment the next line to see all available payment terms
    list_all_payment_terms(models, db, uid, password)  # Enabled for debugging

    df = read_excel_file(CONFIG['excel_path'])
    
    # Use the DRY_RUN setting
    dry_run = DRY_RUN
    
    updated = 0
    failed = 0
    
    for idx, row in df.iterrows():
        old_code_partner = normalize_str(row.get('old_code_partner'))
        payment_term_name = normalize_str(row.get('property_supplier_payment_term_id'))
        
        if not old_code_partner:
            logger.warning(f"Skipping row {idx}: missing old_code_partner")
            failed += 1
            continue
        
        if not payment_term_name:
            logger.warning(f"Skipping row {idx}: missing property_supplier_payment_term_id")
            failed += 1
            continue
        
        # Find partner by old_code_partner
        partner_id = find_partner_by_old_code(models, db, uid, password, old_code_partner)
        
        if partner_id:
            # Get payment term ID
            payment_term_id = get_payment_term_id(models, db, uid, password, payment_term_name)
            
            if payment_term_id:
                # Update partner payment term
                if update_vendor_payment_term(models, db, uid, password, partner_id, payment_term_id, dry_run):
                    updated += 1
                else:
                    failed += 1
            else:
                logger.warning(f"Payment term not found for row {idx}: {payment_term_name}")
                failed += 1
        else:
            logger.warning(f"Partner not found for row {idx}: old_code_partner={old_code_partner}")
            failed += 1
    
    logger.info(f"Update finished: updated={updated}, failed={failed}")

if __name__ == '__main__':
    main()
