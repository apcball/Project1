#!/usr/bin/env python3
import xmlrpc.client
import pandas as pd
from datetime import datetime
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('credit_limit_import.log'),
        logging.StreamHandler()
    ]
)

# --- Connection Settings ---
url = 'http://mogth.work:8069'
db = 'MOG_LIVE'
username = 'apichart@mogen.co.th'
password = '471109538'

def connect_to_odoo():
    """Establish connection to Odoo server"""
    try:
        common = xmlrpc.client.ServerProxy(f'{url}/xmlrpc/2/common')
        uid = common.authenticate(db, username, password, {})
        if not uid:
            raise Exception("Authentication failed")
        models = xmlrpc.client.ServerProxy(f'{url}/xmlrpc/2/object')
        return uid, models
    except Exception as e:
        logging.error(f"Failed to connect to Odoo: {str(e)}")
        raise

def read_excel_data(file_path):
    """Read data from Excel file"""
    try:
        df = pd.read_excel(file_path)
        
        # Convert all column names to string type and lowercase for case-insensitive mapping
        df.columns = df.columns.astype(str).str.lower().str.strip()
        
        # Define column mappings (add all possible variations of column names)
        column_mapping = {
            # Credit limit columns
            'credit_limit': 'credit_limit',
            'use_partner_credit_limit': 'credit_limit',
            'วงเงินเครดิต': 'credit_limit',
            'วงเงิน': 'credit_limit',
            
            # Partner code columns
            'partner_code': 'partner_code',
            'ref': 'partner_code',
            'รหัสลูกค้า': 'partner_code',
            'code': 'partner_code',
            
            # Partner name columns
            'partner_name': 'partner_name',
            'name': 'partner_name',
            'ชื่อลูกค้า': 'partner_name',
            'customer name': 'partner_name',
            
            # Old code columns
            'old_code_partner': 'old_code_partner',
            'old code': 'old_code_partner',
            'รหัสเก่า': 'old_code_partner',
            
            # Payment term columns
            'property_payment_term_id': 'property_payment_term_id',
            'payment_term': 'property_payment_term_id',
            'payment term': 'property_payment_term_id',
            'เงื่อนไขการชำระเงิน': 'property_payment_term_id'
        }
        
        # Create a mapping dictionary for existing columns only
        actual_mapping = {old: new for old, new in column_mapping.items() if old in df.columns}
        
        if not actual_mapping:
            logging.error("No matching columns found in Excel file. Available columns: " + ", ".join(df.columns))
            raise ValueError("No matching columns found in Excel file")
        
        # Rename the columns that exist in the file
        df = df.rename(columns=actual_mapping)
        
        # Log the column mapping for debugging
        logging.info("Column mapping:")
        for old_col, new_col in actual_mapping.items():
            logging.info(f"  {old_col} -> {new_col}")
        
        # Ensure required columns exist
        required_columns = ['partner_code', 'credit_limit']
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            raise ValueError(f"Missing required columns: {', '.join(missing_columns)}")
        
        # Convert credit limit to float, replacing any non-numeric values with 0.0
        if 'credit_limit' in df.columns:
            df['credit_limit'] = pd.to_numeric(df['credit_limit'], errors='coerce').fillna(0.0)
        
        # Clean up string columns
        string_columns = ['partner_code', 'partner_name', 'old_code_partner', 'property_payment_term_id']
        for col in string_columns:
            if col in df.columns:
                df[col] = df[col].astype(str).apply(lambda x: x.strip() if not pd.isna(x) else '')
        
        # Show summary of data
        logging.info("\nData Summary:")
        logging.info(f"Total records: {len(df)}")
        logging.info("Columns found: " + ", ".join(df.columns.tolist()))
        if len(df) > 0:
            logging.info("\nFirst row example:")
            logging.info(df.iloc[0].to_dict())
        
        return df.to_dict('records')
        
    except Exception as e:
        logging.error(f"Failed to read Excel file: {str(e)}")
        raise

def get_payment_term_id(uid, models, payment_term_name):
    """Get payment term ID by name"""
    if not payment_term_name or pd.isna(payment_term_name):
        return False
        
    try:
        payment_term_ids = models.execute_kw(
            db, uid, password,
            'account.payment.term',
            'search',
            [[('name', '=', str(payment_term_name).strip())]]
        )
        return payment_term_ids[0] if payment_term_ids else False
    except Exception as e:
        logging.error(f"Error searching payment term '{payment_term_name}': {str(e)}")
        return False

def search_partner(uid, models, partner_code, partner_name, old_code_partner=None):
    """Search for partner using various methods"""
    try:
        # Clean and prepare search values
        partner_code = str(partner_code).strip() if partner_code and not pd.isna(partner_code) else None
        partner_name = str(partner_name).strip() if partner_name and not pd.isna(partner_name) else None
        old_code_partner = str(old_code_partner).strip() if old_code_partner and not pd.isna(old_code_partner) else None

        # Try searching by each criteria separately
        if partner_code:
            # Search by ref (partner_code)
            partner_ids = models.execute_kw(
                db, uid, password,
                'res.partner',
                'search',
                [[('ref', '=', partner_code)]],
                {'limit': 1}
            )
            if partner_ids:
                return partner_ids[0]

        if old_code_partner:
            # Search by old_code_partner
            partner_ids = models.execute_kw(
                db, uid, password,
                'res.partner',
                'search',
                [[('old_code_partner', '=', old_code_partner)]],
                {'limit': 1}
            )
            if partner_ids:
                return partner_ids[0]

        if partner_name:
            # Search by exact name match
            partner_ids = models.execute_kw(
                db, uid, password,
                'res.partner',
                'search',
                [[('name', '=', partner_name)]],
                {'limit': 1}
            )
            if partner_ids:
                return partner_ids[0]

            # If no exact match, try case-insensitive search
            partner_ids = models.execute_kw(
                db, uid, password,
                'res.partner',
                'search',
                [[('name', 'ilike', partner_name)]],
                {'limit': 1}
            )
            if partner_ids:
                return partner_ids[0]

        if not any([partner_code, partner_name, old_code_partner]):
            logging.warning("No search criteria provided")
            return False

        # Log search attempt
        search_msg = f"No partner found - "
        if partner_code:
            search_msg += f"Code: {partner_code}, "
        if old_code_partner:
            search_msg += f"Old Code: {old_code_partner}, "
        if partner_name:
            search_msg += f"Name: {partner_name}"
        logging.warning(search_msg.rstrip(", "))
        return False

    except Exception as e:
        logging.error(f"Error searching partner - Code: {partner_code}, Name: {partner_name}, Old Code: {old_code_partner}: {str(e)}")
        return False

def update_partner_credit_limits(uid, models, partners_data):
    """Update credit limits and payment terms for partners in Odoo"""
    success_count = 0
    error_count = 0
    not_found_partners = []
    
    for partner in partners_data:
        try:
            # Get partner details
            partner_code = str(partner.get('partner_code', '')).strip()
            partner_name = str(partner.get('partner_name', '')).strip()
            old_code_partner = str(partner.get('old_code_partner', '')).strip()
            
            # Handle credit limit settings
            try:
                # Read credit limit amount
                credit_limit = float(partner.get('credit_limit', 0.0))
                if pd.isna(credit_limit):
                    credit_limit = 0.0
                
                # Ensure credit limit is not negative
                credit_limit = max(0.0, credit_limit)
                
            except (ValueError, TypeError) as e:
                credit_limit = 0.0
                logging.warning(f"Invalid credit limit value for partner {partner_code}, setting to 0: {str(e)}")
            
            # Read payment term
            payment_term_name = partner.get('property_payment_term_id', '')
            if pd.isna(payment_term_name):
                payment_term_name = ''
            else:
                payment_term_name = str(payment_term_name).strip()
            
            if not partner_code and not partner_name and not old_code_partner:
                logging.warning("Skipping record: No partner code, old code, or name provided")
                error_count += 1
                continue
            
            # Search for partner
            partner_id = search_partner(uid, models, partner_code, partner_name, old_code_partner)
            
            if not partner_id:
                error_msg = f"Partner not found - Code: {partner_code}, Old Code: {old_code_partner}, Name: {partner_name}"
                logging.warning(error_msg)
                not_found_partners.append(error_msg)
                error_count += 1
                continue

            # First update: Enable credit limit
            if credit_limit > 0:
                enable_values = {
                    'use_partner_credit_limit': True
                }
                models.execute_kw(
                    db, uid, password,
                    'res.partner',
                    'write',
                    [[partner_id], enable_values]
                )
                
                # Second update: Set credit limit and payment term
                update_values = {
                    'credit_limit': credit_limit
                }
                
                # Add payment term if provided
                if payment_term_name:
                    payment_term_id = get_payment_term_id(uid, models, payment_term_name)
                    if payment_term_id:
                        update_values['property_payment_term_id'] = payment_term_id
                        logging.info(f"Found payment term ID {payment_term_id} for term {payment_term_name}")
                    else:
                        logging.warning(f"Payment term not found: {payment_term_name} for partner: {partner_code}")
                
                # Update partner
                models.execute_kw(
                    db, uid, password,
                    'res.partner',
                    'write',
                    [[partner_id], update_values]
                )
            
            success_count += 1
            update_msg = f"Successfully updated partner: {partner_code}"
            if credit_limit > 0:
                update_msg += f" - Credit Limit Enabled with amount: {credit_limit}"
            if payment_term_name:
                update_msg += f", Payment Term: {payment_term_name}"
            logging.info(update_msg)
            
        except Exception as e:
            error_count += 1
            error_msg = f"Error updating partner {partner_code}: {str(e)}"
            logging.error(error_msg)
            continue
            
    return success_count, error_count, not_found_partners

def main():
    try:
        # Connect to Odoo
        logging.info("Connecting to Odoo server...")
        uid, models = connect_to_odoo()
        logging.info("Successfully connected to Odoo")
        
        # Read Excel data
        logging.info("Reading Excel file...")
        excel_file = "Data_file/credit_limit_import.xlsx"
        partners_data = read_excel_data(excel_file)
        logging.info(f"Found {len(partners_data)} records in Excel file")
        
        # Update credit limits
        logging.info("Starting credit limit update process...")
        success_count, error_count, not_found_partners = update_partner_credit_limits(uid, models, partners_data)
        
        # Log summary
        logging.info("\nImport process completed")
        logging.info(f"Successfully updated: {success_count} partners")
        logging.info(f"Errors encountered: {error_count} partners")
        
        if not_found_partners:
            logging.info("\nPartners not found in system:")
            for partner in not_found_partners:
                logging.info(partner)
        
    except Exception as e:
        logging.error(f"Main process failed: {str(e)}")

if __name__ == "__main__":
    main()