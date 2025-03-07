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

def process_customer(customer_data: Dict[str, Any], contact_name: str, models: Any, uid: int) -> Optional[int]:
    """
    Process customer data - create or update in Odoo
    Args:
        customer_data: Dictionary containing customer data
        contact_name: Contact name for creating child contact
        models: Odoo models proxy
        uid: User ID
    Returns:
        int: ID of created/updated customer or None if failed
    """
    try:
        # Check if customer exists
        existing_customer_id = models.execute_kw(
            CONFIG['db'], uid, CONFIG['password'],
            'res.partner', 'search',
            [[['partner_code', '=', customer_data['partner_code']]]]
        )

        customer_id = None
        if existing_customer_id:
            # Update existing customer
            models.execute_kw(
                CONFIG['db'], uid, CONFIG['password'],
                'res.partner', 'write',
                [existing_customer_id, customer_data]
            )
            customer_id = existing_customer_id[0]
            logger.info(f"Updated customer: {customer_data['name']}")
        else:
            # Create new customer
            customer_id = models.execute_kw(
                CONFIG['db'], uid, CONFIG['password'],
                'res.partner', 'create',
                [customer_data]
            )
            logger.info(f"Created new customer: {customer_data['name']}, ID: {customer_id}")

        # Create contact if provided
        if contact_name and customer_id:
            contact_data = {
                'name': contact_name,
                'parent_id': customer_id,
                'type': 'contact',
                'company_type': 'person',
                'is_company': False,
            }
            
            # Check if contact already exists
            existing_contact = models.execute_kw(
                CONFIG['db'], uid, CONFIG['password'],
                'res.partner', 'search',
                [[['name', '=', contact_name], ['parent_id', '=', customer_id]]]
            )
            
            if not existing_contact:
                contact_id = models.execute_kw(
                    CONFIG['db'], uid, CONFIG['password'],
                    'res.partner', 'create',
                    [contact_data]
                )
                logger.info(f"Created contact: {contact_name} for customer {customer_data['name']}")

        # Create "Other Address" entry
        if customer_id:
            other_address_data = {
                'name': f"{customer_data['name']} (Other Address)",
                'parent_id': customer_id,
                'type': 'other',
                'company_type': 'person',
                'is_company': False,
                'street': customer_data.get('street', False),
                'street2': customer_data.get('street2', False),
                'city': customer_data.get('city', False),
                'state_id': customer_data.get('state_id', False),
                'zip': customer_data.get('zip', False),
                'country_id': customer_data.get('country_id', False),
                'phone': customer_data.get('phone', False),
                'mobile': customer_data.get('mobile', False),
            }
            
            # Check if other address already exists
            existing_other_address = models.execute_kw(
                CONFIG['db'], uid, CONFIG['password'],
                'res.partner', 'search',
                [[['name', '=', other_address_data['name']], ['parent_id', '=', customer_id], ['type', '=', 'other']]]
            )
            
            if not existing_other_address:
                other_address_id = models.execute_kw(
                    CONFIG['db'], uid, CONFIG['password'],
                    'res.partner', 'create',
                    [other_address_data]
                )
                logger.info(f"Created other address for customer {customer_data['name']}")

        return customer_id
    
    except Exception as e:
        logger.error(f"Error processing customer {customer_data['name']}: {e}")
        return None

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

    # Verify if numeric country_id exists in database
    if country_id and isinstance(country_id, (int, float)):
        try:
            country_exists = models.execute_kw(
                CONFIG['db'], uid, CONFIG['password'],
                'res.country', 'search',
                [[['id', '=', int(country_id)]]]
            )
            if not country_exists:
                logger.warning(f"Country ID {country_id} not found in database")
                country_id = False
        except Exception as e:
            logger.warning(f"Error verifying country_id {country_id}: {e}")
            country_id = False

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

    # Handle customer payment term ID
    payment_term = row.get('property_payment_term_id', False)
    property_payment_term_id = False
    if not pd.isna(payment_term):
        try:
            if isinstance(payment_term, (int, float)):
                property_payment_term_id = int(payment_term)
            elif isinstance(payment_term, str):
                # Try to find payment term by name
                payment_terms = models.execute_kw(
                    CONFIG['db'], uid, CONFIG['password'],
                    'account.payment.term', 'search',
                    [[['name', '=', payment_term]]]
                )
                if payment_terms:
                    property_payment_term_id = payment_terms[0]
                else:
                    logger.warning(f"Could not find payment term: {payment_term}")
        except Exception as e:
            logger.warning(f"Error handling payment term '{payment_term}': {e}")

    # Handle supplier payment term ID
    supplier_payment_term = row.get('property_supplier_payment_term_id', False)
    property_supplier_payment_term_id = False
    if not pd.isna(supplier_payment_term):
        try:
            if isinstance(supplier_payment_term, (int, float)):
                property_supplier_payment_term_id = int(supplier_payment_term)
            elif isinstance(supplier_payment_term, str):
                # Try to find supplier payment term by name
                supplier_payment_terms = models.execute_kw(
                    CONFIG['db'], uid, CONFIG['password'],
                    'account.payment.term', 'search',
                    [[['name', '=', supplier_payment_term]]]
                )
                if supplier_payment_terms:
                    property_supplier_payment_term_id = supplier_payment_terms[0]
                else:
                    logger.warning(f"Could not find supplier payment term: {supplier_payment_term}")
        except Exception as e:
            logger.warning(f"Error handling supplier payment term '{supplier_payment_term}': {e}")

    # Clean and validate data
    customer_data = {
        'old_code_partner': str(row.get('old_code_partner', '')) if row.get('old_code_partner') else False,
        'partner_code': str(row.get('partner_code', '')) if row.get('partner_code') else False,
        'name': str(row.get('name', '')) if row.get('name') else False,
        'company_type': row.get('company_type', 'person'),
        'is_company': bool(row.get('is_company', False)),
        'street': str(row.get('street', '')) if row.get('street') else False,
        'street2': str(row.get('street2', '')) if row.get('street2') else False,
        'city': str(row.get('city', '')) if row.get('city') else False,
        'state_id': state_id,
        'country_id': country_id,
        'zip': zip_code if zip_code else False,
        'country_code': str(row.get('country_code', '')) if row.get('country_code') else False,
        'vat': str(row.get('vat', '')) if row.get('vat') else False,
        'phone': phone if phone else False,
        'mobile': mobile if mobile else False,
        'customer_rank': 1,
        'active': bool(row.get('active', True))
    }

    # Only add payment terms if they were found
    if property_payment_term_id:
        customer_data['property_payment_term_id'] = property_payment_term_id
    if property_supplier_payment_term_id:
        customer_data['property_supplier_payment_term_id'] = property_supplier_payment_term_id

    return customer_data

def main():
    """Main execution function"""
    # Connect to Odoo
    uid, models = connect_to_odoo()

    # Read Excel file
    df = read_excel_file(CONFIG['excel_path'])

    # Process each customer
    for index, row in df.iterrows():
        customer_data = clean_customer_data(row, models, uid)
        contact_name = row.get('Contact Name', False)
        logger.debug(f"Processing customer data: {customer_data}")
        process_customer(customer_data, contact_name, models, uid)

    logger.info("Customer import completed successfully")

def process_customer(customer_data: Dict[str, Any], models: Any, uid: int) -> None:
    """
    Process customer data - create or update in Odoo
    Args:
        customer_data: Dictionary containing customer data
        models: Odoo models proxy
        uid: User ID
    """
    try:
        # Check if customer exists
        existing_customer_id = models.execute_kw(
            CONFIG['db'], uid, CONFIG['password'],
            'res.partner', 'search',
            [[['partner_code', '=', customer_data['partner_code']]]]
        )

        if existing_customer_id:
            # Update existing customer
            models.execute_kw(
                CONFIG['db'], uid, CONFIG['password'],
                'res.partner', 'write',
                [existing_customer_id, customer_data]
            )
            logger.info(f"Updated customer: {customer_data['name']}")
        else:
            # Create new customer
            new_customer_id = models.execute_kw(
                CONFIG['db'], uid, CONFIG['password'],
                'res.partner', 'create',
                [customer_data]
            )
            logger.info(f"Created new customer: {customer_data['name']}, ID: {new_customer_id}")
    
    except Exception as e:
        logger.error(f"Error processing customer {customer_data['name']}: {e}")

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