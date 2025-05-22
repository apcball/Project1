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

def convert_date_format(date_str):
    """Convert date from MM/DD/YYYY to YYYY-MM-DD format"""
    try:
        # Parse the date string as MM/DD/YYYY
        if isinstance(date_str, str):
            date_obj = datetime.strptime(date_str, '%m/%d/%Y')
        elif isinstance(date_str, datetime):
            date_obj = date_str
        else:
            # If it's already a pandas timestamp or other format
            date_obj = pd.to_datetime(date_str)
        
        # Convert to Odoo format (YYYY-MM-DD)
        return date_obj.strftime('%Y-%m-%d')
    except Exception as e:
        logging.error(f"Error converting date {date_str}: {str(e)}")
        return None

def read_excel_data(file_path):
    """Read bill data from Excel file"""
    try:
        # Read Excel file with date parsing
        df = pd.read_excel(file_path, parse_dates=['bill_date'])
        
        # Verify required columns exist
        required_columns = ['name', 'bill_date']
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            logging.error(f"Missing required columns in Excel: {missing_columns}")
            return None
            
        return df
    except Exception as e:
        logging.error(f"Failed to read Excel file: {str(e)}")
        return None

def update_bill_dates(uid, models, data):
    """Update bill dates in Odoo"""
    if not uid or not models or data is None:
        return

    success_count = 0
    error_count = 0

    for index, row in data.iterrows():
        try:
            # Search for the bill using bill number
            bill_number = str(row['name'])
            
            # Convert the date to the correct format
            bill_date = convert_date_format(row['bill_date'])
            if not bill_date:
                logging.error(f"Invalid date format for bill {bill_number}")
                error_count += 1
                continue

            # Search for the bill in Odoo
            bill_ids = models.execute_kw(DB, uid, PASSWORD,
                'account.move',
                'search',
                [[['name', '=', bill_number]]]
            )

            if bill_ids:
                # Update the bill date
                models.execute_kw(DB, uid, PASSWORD,
                    'account.move',
                    'write',
                    [bill_ids, {
                        'date': bill_date,
                        'invoice_date': bill_date
                    }]
                )
                success_count += 1
                logging.info(f"Successfully updated bill {bill_number} with date {bill_date}")
            else:
                logging.warning(f"Bill not found: {bill_number}")
                error_count += 1

        except Exception as e:
            logging.error(f"Error updating bill {bill_number}: {str(e)}")
            error_count += 1

    return success_count, error_count

def main():
    """Main execution function"""
    logging.info("Starting bill date update process")
    
    # Connect to Odoo
    uid, models = connect_to_odoo()
    if not uid or not models:
        logging.error("Failed to connect to Odoo server")
        return

    # Read Excel data
    data = read_excel_data('Data_file/date_bill.xlsx')
    if data is None:
        logging.error("Failed to read Excel data")
        return

    # Update bill dates
    success_count, error_count = update_bill_dates(uid, models, data)
    
    # Log summary
    logging.info(f"Update process completed:")
    logging.info(f"Successfully updated: {success_count} bills")
    logging.info(f"Failed updates: {error_count} bills")

if __name__ == "__main__":
    main()