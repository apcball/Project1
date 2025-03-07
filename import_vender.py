import xmlrpc.client
import pandas as pd
import sys
import logging
from typing import Dict, Any

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuration
CONFIG = {
    'url': 'http://mogth.work:8069',
    'db': 'Pre_Test',
    'username': 'apichart@mogen.co.th',
    'password': '471109538',
    'excel_path': 'Data_file/vender_import.xlsx'
}

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

def read_excel_file(file_path: str) -> pd.DataFrame:
    """Read Excel file"""
    try:
        df = pd.read_excel(file_path)
        logger.info(f"Excel file read successfully. Columns: {df.columns.tolist()}")
        return df
    except Exception as e:
        logger.error(f"Error reading Excel file: {e}")
        sys.exit(1)

def get_bank_id(models, uid, bank_name):
    """Get or create bank ID"""
    if pd.isna(bank_name) or not bank_name:
        return False

    try:
        bank_ids = models.execute_kw(
            CONFIG['db'], uid, CONFIG['password'],
            'res.bank', 'search',
            [[['name', '=', str(bank_name)]]]
        )
        
        if bank_ids:
            return bank_ids[0]
        else:
            new_bank_id = models.execute_kw(
                CONFIG['db'], uid, CONFIG['password'],
                'res.bank', 'create',
                [{'name': str(bank_name)}]
            )
            return new_bank_id
    except Exception as e:
        logger.warning(f"Error handling bank '{bank_name}': {e}")
        return False

def clean_vendor_data(row: pd.Series, models: Any, uid: int) -> Dict[str, Any]:
    """Clean and prepare vendor data"""
    
    # Handle company_type and is_company fields
    is_company_value = row.get('is_company', True)
    
    if pd.isna(is_company_value):
        is_company = True
    elif isinstance(is_company_value, str):
        is_company_value = is_company_value.lower().strip()
        is_company = is_company_value in ['true', '1', 'yes', 'y', 't']
    elif isinstance(is_company_value, (int, float)):
        is_company = bool(int(is_company_value))
    else:
        is_company = True

    company_type = 'company' if is_company else 'person'

    # Get partner group, type, and office
    partner_group = str(row.get('Vendor Group', '')).strip() if not pd.isna(row.get('Vendor Group')) else ''
    partner_type = str(row.get('Vendor Type', '')).strip() if not pd.isna(row.get('Vendor Type')) else ''
    office = str(row.get('office', '')).strip() if not pd.isna(row.get('office')) else ''

    # Get VAT from id tax field
    vat = str(row.get('id tax', '')).strip() if not pd.isna(row.get('id tax')) else False

    # Get partner codes
    partner_code = row.get('partner_code', False)
    old_partner_code = row.get('old_partner_code', False)

    # Clean partner codes
    if not pd.isna(partner_code):
        if isinstance(partner_code, (int, float)):
            partner_code = str(int(partner_code))
        elif isinstance(partner_code, str):
            partner_code = partner_code.strip()
    else:
        partner_code = False

    if not pd.isna(old_partner_code):
        if isinstance(old_partner_code, (int, float)):
            old_partner_code = str(int(old_partner_code))
        elif isinstance(old_partner_code, str):
            old_partner_code = old_partner_code.strip()
    else:
        old_partner_code = False

    # Get currency_id
    currency_id = False
    raw_currency = row.get('currency_id', False)
    
    if not pd.isna(raw_currency):
        try:
            if isinstance(raw_currency, str):
                # Search by currency code/name
                currency_ids = models.execute_kw(
                    CONFIG['db'], uid, CONFIG['password'],
                    'res.currency', 'search',
                    [[['name', '=', raw_currency.strip().upper()]]]
                )
                if not currency_ids:
                    # Try searching by currency name
                    currency_ids = models.execute_kw(
                        CONFIG['db'], uid, CONFIG['password'],
                        'res.currency', 'search',
                        [[['name', 'ilike', raw_currency.strip()]]]
                    )
                if currency_ids:
                    currency_id = currency_ids[0]
            elif isinstance(raw_currency, (int, float)):
                currency_id = int(raw_currency)
            
            if currency_id:
                # Verify currency exists
                currency_data = models.execute_kw(
                    CONFIG['db'], uid, CONFIG['password'],
                    'res.currency', 'read',
                    [currency_id],
                    {'fields': ['name']}
                )
                if currency_data:
                    logger.info(f"Found currency: {currency_data[0]['name']}")
                else:
                    currency_id = False
                    logger.warning(f"Currency ID {raw_currency} not found")
            else:
                logger.warning(f"Could not find currency: {raw_currency}")
        except Exception as e:
            logger.warning(f"Error handling currency '{raw_currency}': {e}")
            currency_id = False

    # Get country_id
    country_id = False
    raw_country_id = row.get('country_id', False)
    
    if not pd.isna(raw_country_id):
        try:
            if isinstance(raw_country_id, str):
                # Search by country code
                country_ids = models.execute_kw(
                    CONFIG['db'], uid, CONFIG['password'],
                    'res.country', 'search',
                    [[['code', '=', raw_country_id.strip().upper()]]]
                )
                if country_ids:
                    country_id = country_ids[0]
                else:
                    # Try searching by name
                    country_ids = models.execute_kw(
                        CONFIG['db'], uid, CONFIG['password'],
                        'res.country', 'search',
                        [[['name', 'ilike', raw_country_id.strip()]]]
                    )
                    if country_ids:
                        country_id = country_ids[0]
            elif isinstance(raw_country_id, (int, float)):
                country_id = int(raw_country_id)
            
            if country_id:
                # Verify country exists
                country_data = models.execute_kw(
                    CONFIG['db'], uid, CONFIG['password'],
                    'res.country', 'read',
                    [country_id],
                    {'fields': ['name']}
                )
                if country_data:
                    logger.info(f"Found country: {country_data[0]['name']}")
                else:
                    country_id = False
                    logger.warning(f"Country ID {raw_country_id} not found")
            else:
                logger.warning(f"Could not find country: {raw_country_id}")
        except Exception as e:
            logger.warning(f"Error handling country '{raw_country_id}': {e}")
            country_id = False

    # Clean zip code
    zip_code = row.get('zip_code', False)
    if pd.isna(zip_code):
        zip_code = False
    elif isinstance(zip_code, (int, float)):
        zip_code = str(int(zip_code))
    elif isinstance(zip_code, str):
        zip_code = zip_code.strip()

    # Clean phone
    phone = row.get('phone', '')
    if pd.isna(phone):
        phone = False
    elif isinstance(phone, (int, float)):
        phone = str(int(phone))
    elif isinstance(phone, str):
        phone = phone.strip()

    # Handle payment terms
    payment_term = row.get('property_supplier_payment_term_id', False)
    property_supplier_payment_term_id = False
    
    if not pd.isna(payment_term) and payment_term:
        try:
            payment_term = str(payment_term).strip()
            payment_terms = models.execute_kw(
                CONFIG['db'], uid, CONFIG['password'],
                'account.payment.term', 'search',
                [[['name', '=', payment_term]]]
            )
            
            if payment_terms:
                property_supplier_payment_term_id = payment_terms[0]
                logger.info(f"Found payment term: {payment_term}")
            else:
                logger.warning(f"Payment term not found: {payment_term}")
        except Exception as e:
            logger.warning(f"Error handling payment term: {e}")

    # Get bank account info
    bank_id = get_bank_id(models, uid, row.get('bank_id'))
    acc_number = row.get('acc_number', False)
    if pd.isna(acc_number):
        acc_number = False
    elif isinstance(acc_number, (int, float)):
        acc_number = str(int(acc_number))
    elif isinstance(acc_number, str):
        acc_number = acc_number.strip()

    # Prepare vendor data
    vendor_data = {
        'name': str(row.get('name', '')).strip() if not pd.isna(row.get('name')) else False,
        'partner_code': partner_code,
        'old_code_partner': old_partner_code,
        'company_type': company_type,
        'is_company': is_company,
        'street': str(row.get('street', '')).strip() if not pd.isna(row.get('street')) else False,
        'street2': str(row.get('street2', '')).strip() if not pd.isna(row.get('street2')) else False,
        'city': str(row.get('city', '')).strip() if not pd.isna(row.get('city')) else False,
        'zip': zip_code,
        'country_id': country_id,
        'phone': phone,
        'email': str(row.get('email', '')).strip() if not pd.isna(row.get('email')) else False,
        'vat': vat,
        'supplier_rank': 1,
        'property_supplier_payment_term_id': property_supplier_payment_term_id,
        'partner_group': partner_group,
        'partner_type': partner_type,
        'office': office,
        'currency_id': currency_id,
        'bank_ids': [(0, 0, {
            'bank_id': bank_id,
            'acc_number': acc_number,
            'currency_id': currency_id
        })] if bank_id and acc_number else False
    }

    return vendor_data

def process_vendor(vendor_data: Dict[str, Any], models: Any, uid: int) -> None:
    """Process vendor data - create or update in Odoo"""
    try:
        # Create new vendor
        new_vendor_id = models.execute_kw(
            CONFIG['db'], uid, CONFIG['password'],
            'res.partner', 'create',
            [vendor_data]
        )
        logger.info(f"Created vendor: {vendor_data['name']}, ID: {new_vendor_id}")
    
    except Exception as e:
        logger.error(f"Error processing vendor {vendor_data['name']}: {e}")

def main():
    """Main execution function"""
    uid, models = connect_to_odoo()
    df = read_excel_file(CONFIG['excel_path'])

    for index, row in df.iterrows():
        vendor_data = clean_vendor_data(row, models, uid)
        process_vendor(vendor_data, models, uid)

    logger.info("Import completed successfully")

if __name__ == "__main__":
    main()