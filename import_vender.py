import xmlrpc.client
import pandas as pd
import sys
import logging
from typing import Dict, Any, Optional

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

def connect_to_odoo() -> tuple[Any, Any]:
    """
    Establish connection to Odoo server and authenticate
    Returns:
        tuple: (uid, models) - User ID and models proxy object
    """
    try:
        common = xmlrpc.client.ServerProxy(f'{CONFIG["url"]}/xmlrpc/2/common')
        uid = common.authenticate(CONFIG['db'], CONFIG['username'], CONFIG['password'], {})
        
        if not uid:
            logger.error("Authentication failed: invalid credentials or insufficient permissions.")
            sys.exit(1)
        
        logger.info(f"Authentication successful, uid = {uid}")
        
        # Create XML-RPC models proxy
        models = xmlrpc.client.ServerProxy(f'{CONFIG["url"]}/xmlrpc/2/object')
        return uid, models
    
    except Exception as e:
        logger.error(f"Error during connection/authentication: {e}")
        sys.exit(1)

def read_excel_file(file_path: str) -> pd.DataFrame:
    """
    Read and validate the Excel file
    Args:
        file_path: Path to the Excel file
    Returns:
        pd.DataFrame: DataFrame containing vendor data
    """
    try:
        df = pd.read_excel(file_path)
        logger.info(f"Excel file read successfully. Columns: {df.columns.tolist()}")
        return df
    except Exception as e:
        logger.error(f"Error reading Excel file: {e}")
        sys.exit(1)

def get_partner_group(models: Any, uid: int, group_name: str) -> int:
    """
    Get or create partner group ID
    Args:
        models: Odoo models proxy
        uid: User ID
        group_name: Name of the partner group
    Returns:
        int: Partner group ID
    """
    if pd.isna(group_name) or not group_name:
        return False

    try:
        # Search for existing group
        group_ids = models.execute_kw(
            CONFIG['db'], uid, CONFIG['password'],
            'res.partner.group', 'search',
            [[['name', '=', str(group_name)]]]
        )
        
        if group_ids:
            return group_ids[0]
        else:
            # Create new group if not found
            new_group_id = models.execute_kw(
                CONFIG['db'], uid, CONFIG['password'],
                'res.partner.group', 'create',
                [{'name': str(group_name)}]
            )
            return new_group_id
    except Exception as e:
        logger.warning(f"Error handling partner group '{group_name}': {e}")
        return False

def get_partner_type(models: Any, uid: int, type_name: str) -> int:
    """
    Get or create partner type ID
    Args:
        models: Odoo models proxy
        uid: User ID
        type_name: Name of the partner type
    Returns:
        int: Partner type ID
    """
    if pd.isna(type_name) or not type_name:
        return False

    try:
        # Search for existing type
        type_ids = models.execute_kw(
            CONFIG['db'], uid, CONFIG['password'],
            'res.partner.type', 'search',
            [[['name', '=', str(type_name)]]]
        )
        
        if type_ids:
            return type_ids[0]
        else:
            # Create new type if not found
            new_type_id = models.execute_kw(
                CONFIG['db'], uid, CONFIG['password'],
                'res.partner.type', 'create',
                [{'name': str(type_name)}]
            )
            return new_type_id
    except Exception as e:
        logger.warning(f"Error handling partner type '{type_name}': {e}")
        return False

def get_office(models: Any, uid: int, office_name: str) -> int:
    """
    Get or create office ID
    Args:
        models: Odoo models proxy
        uid: User ID
        office_name: Name of the office
    Returns:
        int: Office ID
    """
    if pd.isna(office_name) or not office_name:
        return False

    try:
        # Search for existing office
        office_ids = models.execute_kw(
            CONFIG['db'], uid, CONFIG['password'],
            'res.partner.office', 'search',
            [[['name', '=', str(office_name)]]]
        )
        
        if office_ids:
            return office_ids[0]
        else:
            # Create new office if not found
            new_office_id = models.execute_kw(
                CONFIG['db'], uid, CONFIG['password'],
                'res.partner.office', 'create',
                [{'name': str(office_name)}]
            )
            return new_office_id
    except Exception as e:
        logger.warning(f"Error handling office '{office_name}': {e}")
        return False

def get_bank_id(models: Any, uid: int, bank_name: str) -> int:
    """
    Get or create bank ID
    Args:
        models: Odoo models proxy
        uid: User ID
        bank_name: Name of the bank
    Returns:
        int: Bank ID
    """
    if pd.isna(bank_name) or not bank_name:
        return False

    try:
        # Search for existing bank
        bank_ids = models.execute_kw(
            CONFIG['db'], uid, CONFIG['password'],
            'res.bank', 'search',
            [[['name', '=', str(bank_name)]]]
        )
        
        if bank_ids:
            return bank_ids[0]
        else:
            # Create new bank if not found
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
    """
    Clean and validate vendor data from Excel row
    Args:
        row: pandas Series containing vendor data
        models: Odoo models proxy
        uid: User ID
    Returns:
        dict: Cleaned vendor data
    """
    # Get state_id from the state name
    state_name = row.get('state_id', False)
    state_id = False
    if not pd.isna(state_name) and state_name:
        state_id = models.execute_kw(CONFIG['db'], uid, CONFIG['password'], 
                                   'res.country.state', 'search', 
                                   [[['name', '=', state_name]]])
        state_id = state_id[0] if state_id else False

    # Get country_id from either country_id or country_code
    country_code = row.get('country_code', False)
    raw_country_id = row.get('country_id', False)
    country_id = False

    # Try to get country from either field
    if not pd.isna(raw_country_id):
        if isinstance(raw_country_id, str):
            try:
                country_ids = models.execute_kw(
                    CONFIG['db'], uid, CONFIG['password'],
                    'res.country', 'search',
                    [[['code', '=', raw_country_id]]]
                )
                if country_ids:
                    country_id = country_ids[0]
                else:
                    logger.warning(f"Could not find country with code: {raw_country_id}")
            except Exception as e:
                logger.warning(f"Error looking up country code '{raw_country_id}': {e}")
        elif isinstance(raw_country_id, (int, float)):
            country_id = int(raw_country_id)
    
    if not country_id and not pd.isna(country_code) and country_code:
        try:
            country_ids = models.execute_kw(
                CONFIG['db'], uid, CONFIG['password'],
                'res.country', 'search',
                [[['code', '=', country_code]]]
            )
            if country_ids:
                country_id = country_ids[0]
            else:
                logger.warning(f"Could not find country with code: {country_code}")
        except Exception as e:
            logger.warning(f"Error looking up country code '{country_code}': {e}")

    # Clean zip code
    zip_code = row.get('zip', False)
    if pd.isna(zip_code):
        zip_code = False
    elif isinstance(zip_code, (int, float)):
        zip_code = str(int(zip_code))
    elif isinstance(zip_code, str):
        zip_code = zip_code.strip()

    # Clean phone numbers
    phone = row.get('phone', '')
    mobile = row.get('mobile', '')
    
    if pd.isna(phone):
        phone = False
    elif isinstance(phone, (int, float)):
        phone = str(int(phone))
    elif isinstance(phone, str):
        phone = phone.strip()

    if pd.isna(mobile):
        mobile = False
    elif isinstance(mobile, (int, float)):
        mobile = str(int(mobile))
    elif isinstance(mobile, str):
        mobile = mobile.strip()

    # Handle vendor payment terms
    payment_term = row.get('property_supplier_payment_term_id', False)
    property_supplier_payment_term_id = False
    
    if not pd.isna(payment_term) and payment_term:
        try:
            # Convert to string and clean up
            payment_term = str(payment_term).strip()
            
            # First try exact match
            payment_terms = models.execute_kw(
                CONFIG['db'], uid, CONFIG['password'],
                'account.payment.term', 'search',
                [[['name', '=', payment_term]]]
            )
            
            if payment_terms:
                property_supplier_payment_term_id = payment_terms[0]
                logger.info(f"Found payment term with exact match: {payment_term} (ID: {property_supplier_payment_term_id})")
            else:
                # Try case-insensitive partial match
                payment_terms = models.execute_kw(
                    CONFIG['db'], uid, CONFIG['password'],
                    'account.payment.term', 'search',
                    [[['name', 'ilike', payment_term]]]
                )
                
                if payment_terms:
                    property_supplier_payment_term_id = payment_terms[0]
                    # Get the actual name for logging
                    term_name = models.execute_kw(
                        CONFIG['db'], uid, CONFIG['password'],
                        'account.payment.term', 'read',
                        [property_supplier_payment_term_id],
                        {'fields': ['name']}
                    )[0]['name']
                    logger.info(f"Found payment term with partial match: {term_name} (ID: {property_supplier_payment_term_id})")
                else:
                    # Create new payment term
                    try:
                        new_term_id = models.execute_kw(
                            CONFIG['db'], uid, CONFIG['password'],
                            'account.payment.term', 'create',
                            [{'name': payment_term}]
                        )
                        property_supplier_payment_term_id = new_term_id
                        logger.info(f"Created new payment term: {payment_term} (ID: {new_term_id})")
                    except Exception as e:
                        logger.error(f"Failed to create payment term '{payment_term}': {e}")
        except Exception as e:
            logger.error(f"Error handling payment term '{payment_term}': {e}")
            property_supplier_payment_term_id = False
            
    # Debug log for payment term
    logger.info(f"Final payment term ID: {property_supplier_payment_term_id}")
    if property_supplier_payment_term_id:
        try:
            # Verify payment term exists
            term_check = models.execute_kw(
                CONFIG['db'], uid, CONFIG['password'],
                'account.payment.term', 'read',
                [property_supplier_payment_term_id],
                {'fields': ['name']}
            )
            logger.info(f"Verified payment term: {term_check[0]['name']}")
        except Exception as e:
            logger.error(f"Failed to verify payment term ID {property_supplier_payment_term_id}: {e}")
            property_supplier_payment_term_id = False

    # Handle company_type and is_company fields
    is_company = row.get('is_company', True)  # Default to True for vendors
    
    # Clean is_company - only True values will set company_type to 'company'
    if pd.isna(is_company):
        is_company = True  # Default to True for vendors
    elif isinstance(is_company, str):
        is_company = is_company.lower().strip()
        is_company = is_company in ['true', '1', 'yes', 'y', 't']
    elif isinstance(is_company, (int, float)):
        is_company = bool(is_company)
    else:
        is_company = True  # Default to True if value is unexpected

    # Set company_type based on is_company
    company_type = 'company' if is_company else 'person'

    # Get partner group, type, and office as text values
    partner_group = str(row.get('Vendor Group', '')).strip() if not pd.isna(row.get('Vendor Group')) else ''
    partner_type = str(row.get('Vendor Type', '')).strip() if not pd.isna(row.get('Vendor Type')) else ''
    office = str(row.get('office', '')).strip() if not pd.isna(row.get('office')) else ''

    # Get VAT/Tax ID from id tax field
    vat = str(row.get('id tax', '')).strip() if not pd.isna(row.get('id tax')) else False

    # Get currency_id from currency name or code
    currency = row.get('currency_id', False)
    currency_id = False
    if not pd.isna(currency) and currency:
        try:
            # Try to find currency by code first
            currency_ids = models.execute_kw(
                CONFIG['db'], uid, CONFIG['password'],
                'res.currency', 'search',
                [[['name', '=', str(currency)]]]
            )
            
            if not currency_ids:
                # Try to find by name if code not found
                currency_ids = models.execute_kw(
                    CONFIG['db'], uid, CONFIG['password'],
                    'res.currency', 'search',
                    [[['name', 'ilike', str(currency)]]]
                )
            
            if currency_ids:
                currency_id = currency_ids[0]
                logger.info(f"Found currency: {currency} (ID: {currency_id})")
            else:
                logger.warning(f"Currency not found: {currency}")
        except Exception as e:
            logger.warning(f"Error handling currency '{currency}': {e}")

    # Clean zip code from zip_code field
    zip_code = row.get('zip_code', False)
    if pd.isna(zip_code):
        zip_code = False
    elif isinstance(zip_code, (int, float)):
        zip_code = str(int(zip_code))
    elif isinstance(zip_code, str):
        zip_code = zip_code.strip()

    # Get bank
    bank_id = get_bank_id(models, uid, row.get('bank_id'))

    # Clean account number
    acc_number = row.get('acc_number', False)
    if pd.isna(acc_number):
        acc_number = False
    elif isinstance(acc_number, (int, float)):
        acc_number = str(int(acc_number))
    elif isinstance(acc_number, str):i
        acc_number = acc_number.strip()

    # Clean and validate data
    vendor_data = {
        'old_code_partner': str(row.get('old_partner_code', '')) if not pd.isna(row.get('old_partner_code')) else False,
        'partner_code': str(row.get('partner_code', '')) if not pd.isna(row.get('partner_code')) else False,
        'name': str(row.get('name', '')) if not pd.isna(row.get('name')) else False,
        'company_type': company_type,
        'is_company': is_company,
        'street': str(row.get('street', '')) if not pd.isna(row.get('street')) else False,
        'street2': str(row.get('street2', '')) if not pd.isna(row.get('street2')) else False,
        'city': str(row.get('city', '')) if not pd.isna(row.get('city')) else False,
        'state_id': state_id,
        'country_id': country_id,
        'zip': zip_code if zip_code else False,
        'country_code': str(row.get('country_code', '')) if not pd.isna(row.get('country_code')) else False,
        'vat': vat,  # Use id tax field for VAT
        'phone': phone if phone else False,
        'email': str(row.get('email', '')) if not pd.isna(row.get('email')) else False,
        'mobile': mobile if mobile else False,
        'supplier_rank': 1,  # Set supplier rank instead of customer rank
        'active': bool(row.get('active', True)) if not pd.isna(row.get('active')) else True,
        'property_supplier_payment_term_id': property_supplier_payment_term_id,  # Use supplier payment term
        'partner_group': partner_group if partner_group else False,  # Store partner group
        'partner_type': partner_type if partner_type else False,  # Store partner type
        'office': office if office else False,  # Store office
        'currency_id': currency_id,  # Add currency
        'bank_ids': [(0, 0, {
            'bank_id': bank_id,
            'acc_number': acc_number,
            'currency_id': currency_id
        })] if bank_id and acc_number else False  # Add bank account information
    }

    return vendor_data

def check_duplicate_partner(models: Any, uid: int, name: str) -> bool:
    """
    Check if partner with the same name already exists
    Args:
        models: Odoo models proxy
        uid: User ID
        name: Partner name to check
    Returns:
        bool: True if partner exists, False otherwise
    """
    try:
        existing_partners = models.execute_kw(
            CONFIG['db'], uid, CONFIG['password'],
            'res.partner', 'search_count',
            [[['name', '=', name]]]
        )
        return existing_partners > 0
    except Exception as e:
        logger.error(f"Error checking duplicate partner {name}: {e}")
        return False

def process_vendor(vendor_data: Dict[str, Any], models: Any, uid: int) -> None:
    """
    Process vendor data - create or update in Odoo
    Args:
        vendor_data: Dictionary containing vendor data
        models: Odoo models proxy
        uid: User ID
    """
    try:
        # Check if vendor exists by partner_code
        existing_vendor_id = models.execute_kw(
            CONFIG['db'], uid, CONFIG['password'],
            'res.partner', 'search',
            [[['partner_code', '=', vendor_data['partner_code']]]]
        )

        # Check for duplicate name
        is_duplicate = check_duplicate_partner(models, uid, vendor_data['name'])

        if existing_vendor_id:
            # Update existing vendor
            models.execute_kw(
                CONFIG['db'], uid, CONFIG['password'],
                'res.partner', 'write',
                [existing_vendor_id, vendor_data]
            )
            logger.info(f"Updated vendor: {vendor_data['name']}")
            
            # Verify payment term was set
            if vendor_data.get('property_supplier_payment_term_id'):
                partner_data = models.execute_kw(
                    CONFIG['db'], uid, CONFIG['password'],
                    'res.partner', 'read',
                    [existing_vendor_id[0]],
                    {'fields': ['property_supplier_payment_term_id']}
                )
                if partner_data and partner_data[0]['property_supplier_payment_term_id'] == vendor_data['property_supplier_payment_term_id']:
                    logger.info(f"Payment term verified for vendor {vendor_data['name']}")
                else:
                    logger.warning(f"Payment term may not have been set correctly for {vendor_data['name']}")
        elif is_duplicate:
            # Skip creating new vendor if name already exists
            logger.warning(f"Skipping creation of duplicate vendor: {vendor_data['name']}")
        else:
            # Create new vendor
            new_vendor_id = models.execute_kw(
                CONFIG['db'], uid, CONFIG['password'],
                'res.partner', 'create',
                [vendor_data]
            )
            logger.info(f"Created new vendor: {vendor_data['name']}, ID: {new_vendor_id}")
            
            # Verify payment term was set
            if vendor_data.get('property_supplier_payment_term_id'):
                partner_data = models.execute_kw(
                    CONFIG['db'], uid, CONFIG['password'],
                    'res.partner', 'read',
                    [new_vendor_id],
                    {'fields': ['property_supplier_payment_term_id']}
                )
                if partner_data and partner_data[0]['property_supplier_payment_term_id'] == vendor_data['property_supplier_payment_term_id']:
                    logger.info(f"Payment term verified for new vendor {vendor_data['name']}")
                else:
                    logger.warning(f"Payment term may not have been set correctly for {vendor_data['name']}")
    
    except Exception as e:
        logger.error(f"Error processing vendor {vendor_data['name']}: {e}")

def main():
    """Main execution function"""
    # Connect to Odoo
    uid, models = connect_to_odoo()

    # Read Excel file
    df = read_excel_file(CONFIG['excel_path'])

    # Process each vendor
    for index, row in df.iterrows():
        vendor_data = clean_vendor_data(row, models, uid)
        logger.debug(f"Processing vendor data: {vendor_data}")
        process_vendor(vendor_data, models, uid)

    logger.info("Vendor import completed successfully")

if __name__ == "__main__":
    main()