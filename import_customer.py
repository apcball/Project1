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
    'db': 'MOG_DEV',
    'username': 'apichart@mogen.co.th',
    'password': '471109538',
    'excel_path': 'Data_file/customer_import.xlsx'
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

def get_account_id(models, uid, account_identifier, account_type):
    """Get account ID by code or name with proper account type"""
    try:
        # Convert account identifier to string if it's a number
        if isinstance(account_identifier, (int, float)):
            account_identifier = str(int(account_identifier))
        elif isinstance(account_identifier, str):
            account_identifier = account_identifier.strip()
        
        # If no account identifier provided, get default account
        if pd.isna(account_identifier) or not account_identifier:
            # Get default account based on type
            if account_type == 'asset_receivable':
                account_ids = models.execute_kw(
                    CONFIG['db'], uid, CONFIG['password'],
                    'account.account', 'search',
                    [[
                        ['account_type', '=', 'asset_receivable'],
                        ['deprecated', '=', False],
                        ['company_id', '=', 1]
                    ]], {'limit': 1}
                )
            else:  # liability_payable
                account_ids = models.execute_kw(
                    CONFIG['db'], uid, CONFIG['password'],
                    'account.account', 'search',
                    [[
                        ['account_type', '=', 'liability_payable'],
                        ['deprecated', '=', False],
                        ['company_id', '=', 1]
                    ]], {'limit': 1}
                )
            
            if account_ids:
                account_data = models.execute_kw(
                    CONFIG['db'], uid, CONFIG['password'],
                    'account.account', 'read',
                    [account_ids[0]],
                    {'fields': ['code', 'name']}
                )
                if account_data:
                    logger.info(f"Using default {account_type} account: {account_data[0]['name']} ({account_data[0]['code']})")
                    return account_ids[0]
            
            logger.warning(f"No default {account_type} account found")
            return False

        # First try to find by account code
        account_ids = models.execute_kw(
            CONFIG['db'], uid, CONFIG['password'],
            'account.account', 'search',
            [[
                ['code', '=', account_identifier],
                ['account_type', '=', account_type],
                ['deprecated', '=', False]
            ]]
        )
        
        if not account_ids:
            # Try to find by account name
            account_ids = models.execute_kw(
                CONFIG['db'], uid, CONFIG['password'],
                'account.account', 'search',
                [[
                    ['name', '=', account_identifier],
                    ['account_type', '=', account_type],
                    ['deprecated', '=', False]
                ]]
            )
            
        if account_ids:
            # Verify the account
            account_data = models.execute_kw(
                CONFIG['db'], uid, CONFIG['password'],
                'account.account', 'read',
                [account_ids[0]],
                {'fields': ['code', 'name', 'account_type']}
            )
            if account_data:
                logger.info(f"Found {account_type} account: {account_data[0]['name']} ({account_data[0]['code']})")
                return account_ids[0]
            else:
                logger.warning(f"Could not verify account {account_identifier}")
                return False
        else:
            logger.warning(f"Account not found for identifier: {account_identifier}")
            # Try to get default account
            if account_type == 'asset_receivable':
                account_ids = models.execute_kw(
                    CONFIG['db'], uid, CONFIG['password'],
                    'account.account', 'search',
                    [[
                        ['account_type', '=', 'asset_receivable'],
                        ['deprecated', '=', False],
                        ['company_id', '=', 1]
                    ]], {'limit': 1}
                )
            else:  # liability_payable
                account_ids = models.execute_kw(
                    CONFIG['db'], uid, CONFIG['password'],
                    'account.account', 'search',
                    [[
                        ['account_type', '=', 'liability_payable'],
                        ['deprecated', '=', False],
                        ['company_id', '=', 1]
                    ]], {'limit': 1}
                )
            
            if account_ids:
                account_data = models.execute_kw(
                    CONFIG['db'], uid, CONFIG['password'],
                    'account.account', 'read',
                    [account_ids[0]],
                    {'fields': ['code', 'name']}
                )
                if account_data:
                    logger.info(f"Using default {account_type} account: {account_data[0]['name']} ({account_data[0]['code']})")
                    return account_ids[0]
            return False
            
    except Exception as e:
        logger.warning(f"Error getting account for '{account_identifier}': {e}")
        # Try to get default account as fallback
        try:
            if account_type == 'asset_receivable':
                account_ids = models.execute_kw(
                    CONFIG['db'], uid, CONFIG['password'],
                    'account.account', 'search',
                    [[
                        ['account_type', '=', 'asset_receivable'],
                        ['deprecated', '=', False],
                        ['company_id', '=', 1]
                    ]], {'limit': 1}
                )
            else:  # liability_payable
                account_ids = models.execute_kw(
                    CONFIG['db'], uid, CONFIG['password'],
                    'account.account', 'search',
                    [[
                        ['account_type', '=', 'liability_payable'],
                        ['deprecated', '=', False],
                        ['company_id', '=', 1]
                    ]], {'limit': 1}
                )
            
            if account_ids:
                account_data = models.execute_kw(
                    CONFIG['db'], uid, CONFIG['password'],
                    'account.account', 'read',
                    [account_ids[0]],
                    {'fields': ['code', 'name']}
                )
                if account_data:
                    logger.info(f"Using default {account_type} account after error: {account_data[0]['name']} ({account_data[0]['code']})")
                    return account_ids[0]
        except:
            pass
        return False

def get_payment_term_id(models, uid, payment_term):
    """Get payment term ID with better matching and verification"""
    if pd.isna(payment_term) or not payment_term:
        logger.info("Payment term is empty")
        return False

    try:
        # Convert to string and clean
        if isinstance(payment_term, (int, float)):
            payment_term = str(int(payment_term))
        else:
            payment_term = str(payment_term).strip()

        logger.info(f"Processing payment term: {payment_term}")

        # List all available payment terms for debugging
        all_terms = models.execute_kw(
            CONFIG['db'], uid, CONFIG['password'],
            'account.payment.term', 'search_read',
            [[]], {'fields': ['id', 'name', 'code']}
        )
        logger.info(f"Available payment terms: {all_terms}")

        # First try direct ID if it's a number
        if payment_term.isdigit():
            try:
                term_id = int(payment_term)
                term_data = models.execute_kw(
                    CONFIG['db'], uid, CONFIG['password'],
                    'account.payment.term', 'read',
                    [term_id],
                    {'fields': ['name', 'code']}
                )
                if term_data:
                    logger.info(f"Found payment term by ID: {term_data[0]['name']}")
                    return term_id
            except Exception as e:
                logger.warning(f"Could not find payment term by ID {payment_term}: {e}")

        # Try exact match with code
        payment_terms = models.execute_kw(
            CONFIG['db'], uid, CONFIG['password'],
            'account.payment.term', 'search',
            [[['code', '=', payment_term]]]
        )
        logger.info(f"Search by code result: {payment_terms}")
        
        # If not found by code, try exact match with name
        if not payment_terms:
            payment_terms = models.execute_kw(
                CONFIG['db'], uid, CONFIG['password'],
                'account.payment.term', 'search',
                [[['name', '=', payment_term]]]
            )
            logger.info(f"Search by exact name result: {payment_terms}")
        
        # If still not found, try partial match with name
        if not payment_terms:
            payment_terms = models.execute_kw(
                CONFIG['db'], uid, CONFIG['password'],
                'account.payment.term', 'search',
                [[['name', 'ilike', payment_term]]]
            )
            logger.info(f"Search by partial name result: {payment_terms}")

        # If still not found, try to extract number of days and search
        if not payment_terms:
            import re
            days_match = re.search(r'(\d+)\s*(?:day|days|d)', payment_term.lower())
            if days_match:
                days = days_match.group(1)
                payment_terms = models.execute_kw(
                    CONFIG['db'], uid, CONFIG['password'],
                    'account.payment.term', 'search',
                    [[['name', 'ilike', f"{days} day"]]]
                )
                logger.info(f"Search by days result: {payment_terms}")

        # If found any payment term, verify and return
        if payment_terms:
            term_data = models.execute_kw(
                CONFIG['db'], uid, CONFIG['password'],
                'account.payment.term', 'read',
                [payment_terms[0]],
                {'fields': ['name', 'code']}
            )
            if term_data:
                term_info = f"{term_data[0]['name']}"
                if term_data[0].get('code'):
                    term_info += f" (Code: {term_data[0]['code']})"
                logger.info(f"Found payment term: {term_info}")
                return payment_terms[0]
            else:
                logger.warning(f"Could not verify payment term: {payment_term}")
                return False
        else:
            # Try to find default payment term
            default_terms = models.execute_kw(
                CONFIG['db'], uid, CONFIG['password'],
                'account.payment.term', 'search',
                [[['name', 'ilike', '30 days']]], 
                {'limit': 1}
            )
            if default_terms:
                term_data = models.execute_kw(
                    CONFIG['db'], uid, CONFIG['password'],
                    'account.payment.term', 'read',
                    [default_terms[0]],
                    {'fields': ['name', 'code']}
                )
                if term_data:
                    term_info = f"{term_data[0]['name']}"
                    if term_data[0].get('code'):
                        term_info += f" (Code: {term_data[0]['code']})"
                    logger.info(f"Using default payment term: {term_info}")
                    return default_terms[0]
            
            logger.warning(f"Payment term not found: {payment_term}")
            return False
            
    except Exception as e:
        logger.error(f"Error handling payment term '{payment_term}': {e}")
        # Try to get default payment term as fallback
        try:
            default_terms = models.execute_kw(
                CONFIG['db'], uid, CONFIG['password'],
                'account.payment.term', 'search',
                [[['name', 'ilike', '30 days']]], 
                {'limit': 1}
            )
            if default_terms:
                term_data = models.execute_kw(
                    CONFIG['db'], uid, CONFIG['password'],
                    'account.payment.term', 'read',
                    [default_terms[0]],
                    {'fields': ['name', 'code']}
                )
                if term_data:
                    term_info = f"{term_data[0]['name']}"
                    if term_data[0].get('code'):
                        term_info += f" (Code: {term_data[0]['code']})"
                    logger.info(f"Using default payment term after error: {term_info}")
                    return default_terms[0]
        except Exception as e2:
            logger.error(f"Error getting default payment term: {e2}")
        return False

def clean_customer_data(row: pd.Series, models: Any, uid: int) -> Dict[str, Any]:
    """Clean and prepare customer data"""
    # Debug payment term data from Excel
    raw_payment_term = row.get('property_supplier_payment_term_id')
    if not pd.isna(raw_payment_term):
        logger.info(f"Raw payment term from Excel: {raw_payment_term}")
    
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
    partner_group = str(row.get('Customer Group', '')).strip() if not pd.isna(row.get('Customer Group')) else ''
    partner_type = str(row.get('Customer Type', '')).strip() if not pd.isna(row.get('Customer Type')) else ''
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

    # Get currency_id - Default to THB if not specified
    currency_id = False
    raw_currency = row.get('currency_id', 'THB')  # Default to THB
    
    if pd.isna(raw_currency):
        # If currency is NA, use THB
        raw_currency = 'THB'
    
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
            if not currency_ids:
                # If still not found, try to get THB
                currency_ids = models.execute_kw(
                    CONFIG['db'], uid, CONFIG['password'],
                    'res.currency', 'search',
                    [[['name', '=', 'THB']]]
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
                logger.info(f"Using currency: {currency_data[0]['name']}")
            else:
                # If verification fails, try to get THB
                currency_ids = models.execute_kw(
                    CONFIG['db'], uid, CONFIG['password'],
                    'res.currency', 'search',
                    [[['name', '=', 'THB']]]
                )
                if currency_ids:
                    currency_id = currency_ids[0]
                    logger.info("Using default currency: THB")
                else:
                    currency_id = False
                    logger.warning("Could not find default THB currency")
        else:
            # If no currency found, try to get THB
            currency_ids = models.execute_kw(
                CONFIG['db'], uid, CONFIG['password'],
                'res.currency', 'search',
                [[['name', '=', 'THB']]]
            )
            if currency_ids:
                currency_id = currency_ids[0]
                logger.info("Using default currency: THB")
            else:
                logger.warning("Could not find default THB currency")
    except Exception as e:
        logger.warning(f"Error handling currency '{raw_currency}': {e}")
        # Try to get THB as a last resort
        try:
            currency_ids = models.execute_kw(
                CONFIG['db'], uid, CONFIG['password'],
                'res.currency', 'search',
                [[['name', '=', 'THB']]]
            )
            if currency_ids:
                currency_id = currency_ids[0]
                logger.info("Using default currency: THB after error")
        except:
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

    # Get payment term ID
    property_payment_term_id = False
    raw_payment_term = row.get('property_payment_term_id')
    
    if not pd.isna(raw_payment_term):
        try:
            # Convert to string and clean
            if isinstance(raw_payment_term, (int, float)):
                payment_term = str(int(raw_payment_term))
            else:
                payment_term = str(raw_payment_term).strip()

            logger.info(f"Looking for payment term: {payment_term}")

            # First try direct ID lookup if it's a number
            if payment_term.isdigit():
                try:
                    term_id = int(payment_term)
                    term_data = models.execute_kw(
                        CONFIG['db'], uid, CONFIG['password'],
                        'account.payment.term', 'read',
                        [term_id],
                        {'fields': ['name']}
                    )
                    if term_data:
                        logger.info(f"Found payment term by ID: {term_data[0]['name']}")
                        property_payment_term_id = term_id
                except Exception as e:
                    logger.debug(f"Could not find payment term by ID {payment_term}: {e}")

            # If not found by ID, try name search
            if not property_payment_term_id:
                # Try exact match first
                payment_terms = models.execute_kw(
                    CONFIG['db'], uid, CONFIG['password'],
                    'account.payment.term', 'search',
                    [[['name', '=', payment_term]]]
                )

                # If not found, try case-insensitive partial match
                if not payment_terms:
                    payment_terms = models.execute_kw(
                        CONFIG['db'], uid, CONFIG['password'],
                        'account.payment.term', 'search',
                        [[['name', 'ilike', payment_term]]]
                    )

                # If still not found, try to extract and match number of days
                if not payment_terms:
                    import re
                    days_match = re.search(r'(\d+)\s*(?:day|days|d)', payment_term.lower())
                    if days_match:
                        days = days_match.group(1)
                        payment_terms = models.execute_kw(
                            CONFIG['db'], uid, CONFIG['password'],
                            'account.payment.term', 'search',
                            [[['name', 'ilike', f"{days} day"]]]
                        )

                if payment_terms:
                    property_payment_term_id = payment_terms[0]
                    term_data = models.execute_kw(
                        CONFIG['db'], uid, CONFIG['password'],
                        'account.payment.term', 'read',
                        [property_payment_term_id],
                        {'fields': ['name']}
                    )
                    if term_data:
                        logger.info(f"Found payment term: {term_data[0]['name']}")
                    else:
                        logger.warning(f"Could not verify payment term: {payment_term}")
                        property_payment_term_id = False
                else:
                    logger.warning(f"No payment term found for: {payment_term}")

            # If still not found, try to get default payment term
            if not property_payment_term_id:
                default_terms = models.execute_kw(
                    CONFIG['db'], uid, CONFIG['password'],
                    'account.payment.term', 'search',
                    [[['name', 'ilike', '30 days']]], 
                    {'limit': 1}
                )
                if default_terms:
                    property_payment_term_id = default_terms[0]
                    term_data = models.execute_kw(
                        CONFIG['db'], uid, CONFIG['password'],
                        'account.payment.term', 'read',
                        [property_payment_term_id],
                        {'fields': ['name']}
                    )
                    if term_data:
                        logger.info(f"Using default payment term: {term_data[0]['name']}")
                    else:
                        property_payment_term_id = False
                        logger.warning("Could not verify default payment term")

        except Exception as e:
            logger.warning(f"Error handling payment term: {e}")
            property_payment_term_id = False

    # Get bank account info
    bank_id = get_bank_id(models, uid, row.get('bank_id'))
    acc_number = row.get('acc_number', False)
    if pd.isna(acc_number):
        acc_number = False
    elif isinstance(acc_number, (int, float)):
        acc_number = str(int(acc_number))
    elif isinstance(acc_number, str):
        acc_number = acc_number.strip()

    # Get receivable and payable accounts
    property_account_receivable_id = get_account_id(
        models, uid,
        row.get('property_account_receivable_id'),
        'asset_receivable'
    )
    
    property_account_payable_id = get_account_id(
        models, uid,
        row.get('property_account_payable_id'),
        'liability_payable'
    )

    # Prepare customer data
    customer_data = {
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
        'customer_rank': 1,
        'property_payment_term_id': property_payment_term_id,
        'partner_group': partner_group,
        'partner_type': partner_type,
        'office': office,
        'currency_id': currency_id,
        'bank_ids': [(0, 0, {
            'bank_id': bank_id,
            'acc_number': acc_number,
            'currency_id': currency_id
        })] if bank_id and acc_number else False,
        'property_account_receivable_id': property_account_receivable_id,
        'property_account_payable_id': property_account_payable_id
    }

    return customer_data

def process_customer(customer_data: Dict[str, Any], models: Any, uid: int) -> None:
    """
    Process customer data - create or update in Odoo based on partner_code
    Args:
        customer_data: Dictionary containing customer data
        models: Odoo models proxy
        uid: User ID
    """
    # Ensure customer name is not truncated
    if customer_data.get('name'):
        customer_data['name'] = str(customer_data['name']).strip()
    try:
        partner_code = customer_data.get('partner_code')
        if not partner_code:
            logger.warning(f"Skipping customer {customer_data['name']} - No partner_code provided")
            return

        # Search for existing customer by partner_code
        existing_customer = models.execute_kw(
            CONFIG['db'], uid, CONFIG['password'],
            'res.partner', 'search_read',
            [[['partner_code', '=', partner_code]]],
            {'fields': ['id', 'name', 'partner_code']}
        )

        if existing_customer:
            # Customer exists - Update the record
            existing_id = existing_customer[0]['id']
            try:
                # Handle bank account update
                if customer_data.get('bank_ids'):
                    # Get existing bank accounts
                    existing_banks = models.execute_kw(
                        CONFIG['db'], uid, CONFIG['password'],
                        'res.partner.bank', 'search',
                        [[['partner_id', '=', existing_id]]]
                    )
                    # Remove existing bank accounts if any
                    if existing_banks:
                        models.execute_kw(
                            CONFIG['db'], uid, CONFIG['password'],
                            'res.partner.bank', 'unlink',
                            [existing_banks]
                        )

                # Update customer data
                models.execute_kw(
                    CONFIG['db'], uid, CONFIG['password'],
                    'res.partner', 'write',
                    [existing_id, customer_data]
                )
                logger.info(f"Updated existing customer - Partner Code: {partner_code}, Name: {customer_data['name']}")

                # Verify the update
                updated_data = models.execute_kw(
                    CONFIG['db'], uid, CONFIG['password'],
                    'res.partner', 'read',
                    [existing_id],
                    {'fields': ['name', 'partner_code', 'property_payment_term_id', 'property_account_receivable_id', 'property_account_payable_id']}
                )
                
                # Verify payment term if it was set
                if customer_data.get('property_payment_term_id'):
                    if updated_data[0]['property_payment_term_id'] == customer_data['property_payment_term_id']:
                        logger.info(f"Payment term verified for customer {customer_data['name']}")
                    else:
                        logger.warning(f"Payment term may not have been set correctly for {customer_data['name']}")

                # Verify accounts
                if customer_data.get('property_account_receivable_id'):
                    if updated_data[0]['property_account_receivable_id'] == customer_data['property_account_receivable_id']:
                        logger.info(f"Receivable account verified for customer {customer_data['name']}")
                    else:
                        logger.warning(f"Receivable account may not have been set correctly for {customer_data['name']}")

                if customer_data.get('property_account_payable_id'):
                    if updated_data[0]['property_account_payable_id'] == customer_data['property_account_payable_id']:
                        logger.info(f"Payable account verified for customer {customer_data['name']}")
                    else:
                        logger.warning(f"Payable account may not have been set correctly for {customer_data['name']}")

            except Exception as update_error:
                logger.error(f"Error updating customer {partner_code}: {update_error}")
                
        else:
            # No existing customer with this partner_code - Create new
            try:
                new_customer_id = models.execute_kw(
                    CONFIG['db'], uid, CONFIG['password'],
                    'res.partner', 'create',
                    [customer_data]
                )
                logger.info(f"Created new customer - Partner Code: {partner_code}, Name: {customer_data['name']}, ID: {new_customer_id}")

                # Verify the creation
                new_data = models.execute_kw(
                    CONFIG['db'], uid, CONFIG['password'],
                    'res.partner', 'read',
                    [new_customer_id],
                    {'fields': ['name', 'partner_code', 'property_payment_term_id', 'property_account_receivable_id', 'property_account_payable_id']}
                )
                
                # Verify payment term if it was set
                if customer_data.get('property_payment_term_id'):
                    if new_data[0]['property_payment_term_id'] == customer_data['property_payment_term_id']:
                        logger.info(f"Payment term verified for new customer {customer_data['name']}")
                    else:
                        logger.warning(f"Payment term may not have been set correctly for {customer_data['name']}")

                # Verify accounts
                if customer_data.get('property_account_receivable_id'):
                    if new_data[0]['property_account_receivable_id'] == customer_data['property_account_receivable_id']:
                        logger.info(f"Receivable account verified for new customer {customer_data['name']}")
                    else:
                        logger.warning(f"Receivable account may not have been set correctly for {customer_data['name']}")

                if customer_data.get('property_account_payable_id'):
                    if new_data[0]['property_account_payable_id'] == customer_data['property_account_payable_id']:
                        logger.info(f"Payable account verified for new customer {customer_data['name']}")
                    else:
                        logger.warning(f"Payable account may not have been set correctly for {customer_data['name']}")

            except Exception as create_error:
                logger.error(f"Error creating new customer {partner_code}: {create_error}")

    except Exception as e:
        logger.error(f"Error processing customer {customer_data.get('name', 'Unknown')}: {e}")

def main():
    """Main execution function"""
    uid, models = connect_to_odoo()
    df = read_excel_file(CONFIG['excel_path'])

    for index, row in df.iterrows():
        customer_data = clean_customer_data(row, models, uid)
        process_customer(customer_data, models, uid)

    logger.info("Import completed successfully")

if __name__ == "__main__":
    main()