import xmlrpc.client
import pandas as pd
from datetime import datetime
import logging
import sys
from typing import Tuple, Any

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Connection settings
HOST = 'http://mogth.work:8069'
DB = 'MOG_LIVE'
USERNAME = 'apichart@mogen.co.th'
PASSWORD = '471109538'

def connect_to_odoo() -> Tuple[int, Any]:
    """
    Establish connection to Odoo server using XML-RPC.
    
    Returns:
        Tuple containing user ID (uid) and models proxy object
    
    Raises:
        Exception: If connection or authentication fails
    """
    try:
        # Connect to Odoo common interface
        common = xmlrpc.client.ServerProxy(f'{HOST}/xmlrpc/2/common')
        
        # Verify server version
        version_info = common.version()
        logger.info(f"Connected to Odoo server version {version_info.get('server_version', 'unknown')}")
        
        # Authenticate
        uid = common.authenticate(DB, USERNAME, PASSWORD, {})
        if not uid:
            raise Exception("Authentication failed")
        
        # Connect to object endpoint
        models = xmlrpc.client.ServerProxy(f'{HOST}/xmlrpc/2/object')
        
        logger.info(f"Authentication successful, uid = {uid}")
        return uid, models
        
    except Exception as e:
        logger.error(f"Failed to connect to Odoo: {str(e)}")
        raise

def convert_date(date_str: str) -> str:
    """
    Convert date string to Odoo compatible format (YYYY-MM-DD).
    Prioritizes Thai date format (DD/MM/YYYY) over US format (MM/DD/YYYY).

    Args:
    date_str: Date string from Excel file

    Returns:
    Formatted date string
    """
    try:
        original_date = str(date_str)
        logger.info(f"Original date value: {original_date}")

        # Remove any time component if present
        if ' ' in original_date:
            original_date = original_date.split(' ')[0]

        # If it's already in YYYY-MM-DD format
        if isinstance(original_date, str) and len(original_date.split('-')) == 3:
            logger.info(f"Date already in YYYY-MM-DD format: {original_date}")
            return original_date

        date_parts = original_date.strip().split('/')
        if len(date_parts) != 3:
            raise ValueError(f"Unable to parse date: {original_date}")

        # Try Thai format first (DD/MM/YYYY)
        try:
            day = int(date_parts[0])
            month = int(date_parts[1])
            year = int(date_parts[2])
            if year < 100:
                year += 2000
            dt = datetime(year, month, day)
            logger.info(f"Successfully parsed as Thai format (DD/MM/YYYY): {dt}")
            return dt.strftime('%Y-%m-%d')
        except ValueError:
            logger.info(f"Failed to parse as Thai format, trying US format")

            # Fall back to US format (MM/DD/YYYY)
            try:
                month = int(date_parts[0])
                day = int(date_parts[1])
                year = int(date_parts[2])
                if year < 100:
                    year += 2000
                dt = datetime(year, month, day)
                logger.info(f"Successfully parsed as US format (MM/DD/YYYY): {dt}")
                return dt.strftime('%Y-%m-%d')
            except ValueError as e:
                logger.error(f"Invalid date components: year={year}, month={month}, day={day}")
                raise ValueError(f"Invalid date in both Thai and US formats: {original_date}") from e

    except Exception as e:
        logger.error(f"Date conversion error for {date_str}: {str(e)}")
        raise

def update_so_dates(uid: int, models: Any) -> None:
    """
    Update sale order dates from Excel file.
    
    Args:
        uid: Odoo user ID
        models: Odoo models proxy object
    """
    try:
        # Read Excel file with explicit date parsing
        df = pd.read_excel('Data_file/import_date_SO_04.xlsx')
        logger.info(f"Found {len(df)} records in Excel file")
        
        # Remove duplicates based on 'name' column to avoid multiple updates
        df = df.drop_duplicates(subset=['name'])
        logger.info(f"Processing {len(df)} unique sale orders after removing duplicates")
        
        success_count = 0
        error_count = 0
        
        for index, row in df.iterrows():
            try:
                so_name = str(row['name']).strip()
                logger.info(f"Processing sale order {so_name} with original date: {row['date_order']}")
                date_order = convert_date(row['date_order'])
                logger.info(f"Converted date for {so_name}: {date_order}")
                
                # Search for the sale order
                so_ids = models.execute_kw(DB, uid, PASSWORD,
                    'sale.order', 'search',
                    [[['name', '=', so_name]]]
                )
                
                if not so_ids:
                    logger.warning(f"Sale order {so_name} not found")
                    error_count += 1
                    continue
                
                # Update the sale order date (date only, without time)
                models.execute_kw(DB, uid, PASSWORD, 'sale.order', 'write', [
                    so_ids,
                    {
                        'date_order': f"{date_order} 00:00:00",
                    }
                ])
                
                success_count += 1
                logger.info(f"Updated date for sale order {so_name} to {date_order}")
                
            except Exception as e:
                logger.error(f"Error processing row {index + 2}: {str(e)}")
                error_count += 1
                
        logger.info(f"Process completed. Success: {success_count}, Errors: {error_count}")
        
    except Exception as e:
        logger.error(f"Failed to process Excel file: {str(e)}")
        raise

def main():
    """Main execution function"""
    try:
        logger.info("Starting sale order date update process")
        uid, models = connect_to_odoo()
        update_so_dates(uid, models)
        logger.info("Process completed successfully")
        
    except Exception as e:
        logger.error(f"Process failed: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()


