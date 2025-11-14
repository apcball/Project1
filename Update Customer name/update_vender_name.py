import xmlrpc.client
import pandas as pd
import sys
import argparse
import os
import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, List, Tuple

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('vendor_update.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Global configuration
CONFIG = {
    'server_url': 'http://mogth.work:8069',
    'database': 'MOG_SETUP',
    'username': 'apichart@mogen.co.th',
    'password': '471109538',
    'data_file': r'C:\Users\Ball\Documents\Git_apcball\Project1\Update Customer name\Contact_name_update.xlsx',
    'dry_run': False
}

# Global variables for logging
success_count = 0
error_count = 0
processed_count = 0
not_found_count = 0

def connect_to_odoo() -> Tuple[int, Any]:
    """Establish connection to Odoo server"""
    try:
        common = xmlrpc.client.ServerProxy(f'{CONFIG["server_url"]}/xmlrpc/2/common')
        uid = common.authenticate(CONFIG['database'], CONFIG['username'], CONFIG['password'], {})
        
        if not uid:
            logger.error("Authentication failed")
            sys.exit(1)
        
        logger.info(f"Authentication successful, uid = {uid}")
        models = xmlrpc.client.ServerProxy(f'{CONFIG["server_url"]}/xmlrpc/2/object')
        return uid, models
    
    except Exception as e:
        logger.error(f"Connection error: {e}")
        sys.exit(1)

def read_excel_file(file_path: str) -> pd.DataFrame:
    """Read Excel file and return DataFrame"""
    try:
        df = pd.read_excel(file_path)
        logger.info(f"Excel file read successfully. Columns: {df.columns.tolist()}")
        logger.info(f"Total rows in Excel file: {len(df)}")
        return df
    except Exception as e:
        logger.error(f"Error reading Excel file: {e}")
        sys.exit(1)

def normalize_partner_code(partner_code: Any) -> str:
    """Normalize partner code to string format"""
    if pd.isna(partner_code):
        return ""
    
    if isinstance(partner_code, (int, float)):
        return str(int(partner_code))
    elif isinstance(partner_code, str):
        return partner_code.strip()
    else:
        return str(partner_code).strip()

def normalize_vendor_name(vendor_name: Any) -> str:
    """Normalize vendor name to string format"""
    if pd.isna(vendor_name):
        return ""
    
    if isinstance(vendor_name, str):
        return vendor_name.strip()
    else:
        return str(vendor_name).strip()

def find_vendor_by_partner_code(models: Any, uid: int, partner_code: str) -> List[Dict]:
    """Search for vendors by partner_code in Odoo"""
    try:
        # Search by partner_code field
        vendor_ids = models.execute_kw(
            CONFIG['database'], uid, CONFIG['password'],
            'res.partner', 'search_read',
            [[
                ['partner_code', '=', partner_code],
                ['supplier_rank', '>', 0]  # Ensure it's a vendor
            ]],
            {'fields': ['id', 'name', 'partner_code', 'ref']}
        )
        
        # If not found by partner_code, try by ref field
        if not vendor_ids:
            vendor_ids = models.execute_kw(
                CONFIG['database'], uid, CONFIG['password'],
                'res.partner', 'search_read',
                [[
                    ['ref', '=', partner_code],
                    ['supplier_rank', '>', 0]  # Ensure it's a vendor
                ]],
                {'fields': ['id', 'name', 'partner_code', 'ref']}
            )
        
        return vendor_ids
    
    except Exception as e:
        logger.error(f"Error searching for vendor with partner_code {partner_code}: {e}")
        return []

def update_vendor_name(models: Any, uid: int, vendor_id: int, new_name: str, partner_code: str) -> bool:
    """Update vendor name in Odoo"""
    try:
        if CONFIG['dry_run']:
            logger.info(f"DRY-RUN: Would update vendor ID {vendor_id} (partner_code: {partner_code}) name to: {new_name}")
            return True
        
        # Update the vendor name
        models.execute_kw(
            CONFIG['database'], uid, CONFIG['password'],
            'res.partner', 'write',
            [[vendor_id], {'name': new_name}]
        )
        
        logger.info(f"Updated vendor ID {vendor_id} (partner_code: {partner_code}) name to: {new_name}")
        return True
    
    except Exception as e:
        logger.error(f"Error updating vendor ID {vendor_id} (partner_code: {partner_code}): {e}")
        return False

def process_vendor_updates(uid: int, models: Any, df: pd.DataFrame) -> Dict[str, int]:
    """Process vendor updates from DataFrame"""
    global success_count, error_count, processed_count, not_found_count
    
    results = {
        'success': 0,
        'error': 0,
        'not_found': 0,
        'total': len(df)
    }
    
    logger.info(f"Starting to process {len(df)} vendor records...")
    
    for index, row in df.iterrows():
        processed_count += 1
        
        # Extract and normalize data
        # Try different possible column names for flexibility
        partner_code_col = None
        vendor_name_col = None
        
        # Find the correct column names
        for col in df.columns:
            col_lower = str(col).lower()
            if 'partner' in col_lower and 'code' in col_lower:
                partner_code_col = col
            elif 'display' in col_lower and 'name' in col_lower:
                vendor_name_col = col
            elif 'vender' in col_lower or 'vendor' in col_lower:
                vendor_name_col = col
        
        if not partner_code_col:
            logger.error(f"Row {index + 1}: Partner Code column not found. Available columns: {df.columns.tolist()}")
            error_count += 1
            results['error'] += 1
            continue
        
        if not vendor_name_col:
            logger.error(f"Row {index + 1}: Vendor Name column not found. Available columns: {df.columns.tolist()}")
            error_count += 1
            results['error'] += 1
            continue
        
        partner_code = normalize_partner_code(row.get(partner_code_col))
        vendor_name = normalize_vendor_name(row.get(vendor_name_col))
        
        if not partner_code:
            logger.warning(f"Row {index + 1}: Empty partner code, skipping")
            error_count += 1
            results['error'] += 1
            continue
        
        if not vendor_name:
            logger.warning(f"Row {index + 1}: Empty vendor name for partner_code {partner_code}, skipping")
            error_count += 1
            results['error'] += 1
            continue
        
        logger.info(f"Processing row {index + 1}: partner_code={partner_code}, new_name={vendor_name}")
        
        # Find vendor by partner code
        vendors = find_vendor_by_partner_code(models, uid, partner_code)
        
        if not vendors:
            logger.warning(f"Row {index + 1}: No vendor found with partner_code {partner_code}")
            not_found_count += 1
            results['not_found'] += 1
            continue
        
        if len(vendors) > 1:
            logger.warning(f"Row {index + 1}: Multiple vendors found with partner_code {partner_code}, updating first one")
        
        vendor = vendors[0]
        vendor_id = vendor['id']
        current_name = vendor['name']
        
        # Check if name needs updating
        if current_name == vendor_name:
            logger.info(f"Row {index + 1}: Vendor name already matches for partner_code {partner_code}")
            success_count += 1
            results['success'] += 1
            continue
        
        # Update vendor name
        if update_vendor_name(models, uid, vendor_id, vendor_name, partner_code):
            success_count += 1
            results['success'] += 1
        else:
            error_count += 1
            results['error'] += 1
    
    return results

def main():
    """Main execution function"""
    global success_count, error_count, processed_count, not_found_count
    
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Update vendor names in Odoo based on partner codes')
    parser.add_argument('--dry-run', action='store_true', help='Run in dry-run mode (no changes made)')
    parser.add_argument('--data-file', help='Path to Excel data file')
    args = parser.parse_args()
    
    # Update config based on arguments
    if args.dry_run:
        CONFIG['dry_run'] = True
        logger.info("Running in DRY-RUN mode - no changes will be made")
    
    if args.data_file:
        CONFIG['data_file'] = args.data_file
    
    # Check if data file exists
    if not os.path.exists(CONFIG['data_file']):
        logger.error(f"Data file not found: {CONFIG['data_file']}")
        sys.exit(1)
    
    logger.info("Starting vendor name update process")
    logger.info(f"Configuration: {json.dumps(CONFIG, indent=2)}")
    
    # Connect to Odoo
    uid, models = connect_to_odoo()
    
    # Read Excel file
    df = read_excel_file(CONFIG['data_file'])
    
    if df.empty:
        logger.error("Excel file is empty or could not be read")
        sys.exit(1)
    
    # Process vendor updates
    results = process_vendor_updates(uid, models, df)
    
    # Print summary
    logger.info("=" * 50)
    logger.info("VENDOR NAME UPDATE SUMMARY")
    logger.info("=" * 50)
    logger.info(f"Total records processed: {results['total']}")
    logger.info(f"Successfully updated: {results['success']}")
    logger.info(f"Not found in system: {results['not_found']}")
    logger.info(f"Errors encountered: {results['error']}")
    logger.info("=" * 50)
    
    if CONFIG['dry_run']:
        logger.info("DRY-RUN COMPLETED - No actual changes were made to Odoo")
    else:
        logger.info("UPDATE PROCESS COMPLETED")

if __name__ == "__main__":
    main()