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
    'db': 'MOG_DEV',
    'username': 'apichart@mogen.co.th',
    'password': '471109538',
    'excel_path': 'Data_file/customer_import.xlsx'
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
        pd.DataFrame: DataFrame containing customer data
    """
    try:
        df = pd.read_excel(file_path)
        logger.info(f"Excel file read successfully. Columns: {df.columns.tolist()}")
        return df
    except Exception as e:
        logger.error(f"Error reading Excel file: {e}")
        sys.exit(1)

def clean_customer_data(row: pd.Series, models: Any, uid: int) -> Dict[str, Any]:
    """
    Clean and validate customer data from Excel row
    Args:
        row: pandas Series containing customer data
        models: Odoo models proxy
        uid: User ID
    Returns:
        dict: Cleaned customer data
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
        # If country_id is actually a country code (like 'TH')
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
        # If it's a numeric ID
        elif isinstance(raw_country_id, (int, float)):
            country_id = int(raw_country_id)
    
    # If no country_id found yet, try country_code
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

    # Handle payment terms for property_payment_term_id (Customer Payment Terms)
    payment_term = row.get('property_supplier_payment_term_id', False)  # Changed to match Excel column name
    property_payment_term_id = False
    
    if not pd.isna(payment_term) and payment_term:
        try:
            # Convert to string and clean up
            payment_term = str(payment_term).strip()
            
            # Try to find payment term by name
            payment_terms = models.execute_kw(
                CONFIG['db'], uid, CONFIG['password'],
                'account.payment.term', 'search_read',
                [[['name', 'ilike', payment_term]]],
                {'fields': ['id', 'name']}
            )
            
            if payment_terms:
                property_payment_term_id = payment_terms[0]['id']
                logger.info(f"Found payment term: {payment_terms[0]['name']} (ID: {property_payment_term_id})")
            else:
                logger.warning(f"Payment term not found: {payment_term}")
        except Exception as e:
            logger.warning(f"Error handling payment term '{payment_term}': {e}")
            property_payment_term_id = False
            
    # Debug log
    if property_payment_term_id:
        logger.info(f"Setting payment term ID: {property_payment_term_id}")

    # Handle account receivable and payable
    property_account_receivable_id = False
    property_account_payable_id = False
    
    # Get receivable account
    raw_receivable = row.get('property_account_receivable_id', False)
    if not pd.isna(raw_receivable):
        try:
            if isinstance(raw_receivable, (int, float)):
                property_account_receivable_id = int(raw_receivable)
            elif isinstance(raw_receivable, str):
                # Search by account code or name
                receivable_ids = models.execute_kw(
                    CONFIG['db'], uid, CONFIG['password'],
                    'account.account', 'search',
                    [[['code', '=', raw_receivable.strip()], '|', ['name', '=', raw_receivable.strip()]]]
                )
                if receivable_ids:
                    property_account_receivable_id = receivable_ids[0]
                    logger.info(f"Found receivable account: {raw_receivable}")
                else:
                    logger.warning(f"Receivable account not found: {raw_receivable}")
        except Exception as e:
            logger.warning(f"Error handling receivable account '{raw_receivable}': {e}")

    # Get payable account
    raw_payable = row.get('property_account_payable_id', False)
    if not pd.isna(raw_payable):
        try:
            if isinstance(raw_payable, (int, float)):
                property_account_payable_id = int(raw_payable)
            elif isinstance(raw_payable, str):
                # Search by account code or name
                payable_ids = models.execute_kw(
                    CONFIG['db'], uid, CONFIG['password'],
                    'account.account', 'search',
                    [[['code', '=', raw_payable.strip()], '|', ['name', '=', raw_payable.strip()]]]
                )
                if payable_ids:
                    property_account_payable_id = payable_ids[0]
                    logger.info(f"Found payable account: {raw_payable}")
                else:
                    logger.warning(f"Payable account not found: {raw_payable}")
        except Exception as e:
            logger.warning(f"Error handling payable account '{raw_payable}': {e}")

    # Clean and validate data
    # Handle company_type field - must be either 'person' or 'company'
    company_type = row.get('company_type', 'person')
    if pd.isna(company_type):
        company_type = 'person'
    else:
        company_type = str(company_type).lower().strip()
        if company_type not in ['person', 'company']:
            company_type = 'company' if row.get('is_company', False) else 'person'

    # Clean and validate data
    customer_data = {
        'old_code_partner': str(row.get('old_code_partner', '')) if not pd.isna(row.get('old_code_partner')) else False,
        'partner_code': str(row.get('partner_code', '')) if not pd.isna(row.get('partner_code')) else False,
        'name': str(row.get('name', '')) if not pd.isna(row.get('name')) else False,
        'company_type': company_type,
        'is_company': bool(row.get('is_company', False)) if not pd.isna(row.get('is_company')) else False,
        'street': str(row.get('street', '')) if not pd.isna(row.get('street')) else False,
        'street2': str(row.get('street2', '')) if not pd.isna(row.get('street2')) else False,
        'city': str(row.get('city', '')) if not pd.isna(row.get('city')) else False,
        'state_id': state_id,
        'country_id': country_id,
        'zip': zip_code if zip_code else False,
        'country_code': str(row.get('country_code', '')) if not pd.isna(row.get('country_code')) else False,
        'vat': str(row.get('vat', '')) if not pd.isna(row.get('vat')) else False,
        'phone': phone if phone else False,
        'mobile': mobile if mobile else False,
        'customer_rank': 1,
        'active': bool(row.get('active', True)) if not pd.isna(row.get('active')) else True,
        'property_payment_term_id': property_payment_term_id,
        'property_account_receivable_id': property_account_receivable_id,
        'property_account_payable_id': property_account_payable_id
    }

    return customer_data

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

def process_customer(customer_data: Dict[str, Any], models: Any, uid: int) -> None:
    """
    Process customer data - create or update in Odoo
    Args:
        customer_data: Dictionary containing customer data
        models: Odoo models proxy
        uid: User ID
    """
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
                    {'fields': ['name', 'partner_code', 'property_payment_term_id']}
                )
                
                # Verify payment term if it was set
                if customer_data.get('property_payment_term_id'):
                    if updated_data[0]['property_payment_term_id'] == customer_data['property_payment_term_id']:
                        logger.info(f"Payment term verified for customer {customer_data['name']}")
                    else:
                        logger.warning(f"Payment term may not have been set correctly for {customer_data['name']}")

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

                # Verify the creation and payment term
                if customer_data.get('property_payment_term_id'):
                    new_data = models.execute_kw(
                        CONFIG['db'], uid, CONFIG['password'],
                        'res.partner', 'read',
                        [new_customer_id],
                        {'fields': ['property_payment_term_id']}
                    )
                    if new_data and new_data[0]['property_payment_term_id'] == customer_data['property_payment_term_id']:
                        logger.info(f"Payment term verified for new customer {customer_data['name']}")
                    else:
                        logger.warning(f"Payment term may not have been set correctly for {customer_data['name']}")

            except Exception as create_error:
                logger.error(f"Error creating new customer {partner_code}: {create_error}")

    except Exception as e:
        logger.error(f"Error processing customer {customer_data.get('name', 'Unknown')}: {e}")

def main():
    """Main execution function"""
    # Connect to Odoo
    uid, models = connect_to_odoo()

    # Read Excel file
    df = read_excel_file(CONFIG['excel_path'])

    # Process each customer
    for index, row in df.iterrows():
        customer_data = clean_customer_data(row, models, uid)
        logger.debug(f"Processing customer data: {customer_data}")
        process_customer(customer_data, models, uid)

    logger.info("Customer import completed successfully")

if __name__ == "__main__":
    main()