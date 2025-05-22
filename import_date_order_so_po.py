import xmlrpc.client
import pandas as pd
from datetime import datetime

# Connection settings
HOST = 'http://mogth.work:8069'
DB = 'MOG_LIVE'
USERNAME = 'apichart@mogen.co.th'
PASSWORD = '471109538'

def connect_to_odoo():
    """Establish connection to Odoo server"""
    common = xmlrpc.client.ServerProxy(f'{HOST}/xmlrpc/2/common')
    uid = common.authenticate(DB, USERNAME, PASSWORD, {})
    models = xmlrpc.client.ServerProxy(f'{HOST}/xmlrpc/2/object')

    print(f"Authentication successful, uid = {uid}")
    return uid, models






def convert_date(date_str):
    """Convert date string to Odoo format (YYYY-MM-DD) with proper date conversion"""
    if pd.isna(date_str):
        return False
    
    try:
        if isinstance(date_str, str) and '/' in date_str:
            # For M/D/Y format from Excel (e.g., 3/31/2025)
            month, day, year = map(int, date_str.split('/'))
            # Create date with day and month in correct position
            date_obj = datetime(year, month, day)
        else:
            # For YYYY-MM-DD HH:MM:SS format
            date_obj = datetime.strptime(str(date_str), '%Y-%m-%d %H:%M:%S')
            # Swap month and day
            year = date_obj.year
            month = date_obj.month
            day = date_obj.day
            # Create new date with swapped month and day
            date_obj = datetime(year, day, month)
        
        # Validate date
        if date_obj.month > 12 or date_obj.day > 31:
            raise ValueError("Invalid month or day")
        
        # Return in YYYY-MM-DD format
        return date_obj.strftime('%Y-%m-%d')
        
    except ValueError as e:
        print(f"Error converting date {date_str}: {str(e)}")
        return False
    except Exception as e:
        print(f"Error converting date {date_str}: {str(e)}")
        return False
def update_so_dates(uid, models):
    """Update sale order dates from Excel file"""
    try:
        # Read Excel file
        df = pd.read_excel('Data_file/import_date_SO1.xlsx')
        print(f"\nExcel file read successfully. Number of rows = {len(df)}")

        # Print column names to verify structure
        print("\nAvailable columns in Excel:")
        print(df.columns.tolist())

        # Process each row
        for index, row in df.iterrows():
            try:
                so_name = str(row['name']) if 'name' in row else None
                new_date = convert_date(row['date_order']) if 'date_order' in row else None

                if not so_name or not new_date:
                    print(f"Skipping row {index + 1}: Missing required data")
                    print(f"SO Name: {so_name}, Original Date: {row.get('date_order', 'Not found')}")
                    continue

                # Search for the sale order
                so_ids = models.execute_kw(DB, uid, PASSWORD,
                    'sale.order', 'search',
                    [[['name', '=', so_name]]]
                )

                if not so_ids:
                    print(f"Sale Order {so_name} not found")
                    continue

                # Update the date with current time
                current_time = datetime.now().strftime('%H:%M:%S')
                date_order = f"{new_date} {current_time}"

                # Debug log
                print(f"Processing SO {so_name}: Original={row['date_order']}, Converted={new_date}")

                # Update the record
                result = models.execute_kw(DB, uid, PASSWORD, 'sale.order', 'write', [
                    so_ids[0],
                    {
                        'date_order': date_order,
                        'effective_date': new_date
                    }
                ])

                if result:
                    # Force commit the changes
                    models.execute_kw(DB, uid, PASSWORD, 'sale.order', 'flush', [['date_order', 'effective_date']])
                    print(f"Successfully updated and committed date for SO {so_name} to {date_order}")
                else:
                    print(f"Failed to update SO {so_name}")

            except Exception as e:
                print(f"Error processing row {index + 1}: {str(e)}")
                continue

    except Exception as e:
        print(f"Error processing Excel file: {str(e)}")
    
def update_so_dates(uid, models):
    """Update sale order dates from Excel file"""
    try:
        # Read Excel file
        df = pd.read_excel('Data_file/import_date_SO1.xlsx')
        print(f"\nExcel file read successfully. Number of rows = {len(df)}")

        # Print column names to verify structure
        print("\nAvailable columns in Excel:")
        print(df.columns.tolist())

        # Process each row
        for index, row in df.iterrows():
            try:
                so_name = str(row['name']) if 'name' in row else None
                new_date = convert_date(row['date_order']) if 'date_order' in row else None

                if not so_name or not new_date:
                    print(f"Skipping row {index + 1}: Missing required data")
                    print(f"SO Name: {so_name}, Original Date: {row.get('date_order', 'Not found')}")
                    continue

                # Search for the sale order
                so_ids = models.execute_kw(DB, uid, PASSWORD,
                    'sale.order', 'search',
                    [[['name', '=', so_name]]]
                )

                if not so_ids:
                    print(f"Sale Order {so_name} not found")
                    continue

                # Update the date with current time
                current_time = datetime.now().strftime('%H:%M:%S')
                date_order = f"{new_date} {current_time}"

                # Debug log
                print(f"Processing SO {so_name}: Original={row['date_order']}, Converted={new_date}")

                models.execute_kw(DB, uid, PASSWORD, 'sale.order', 'write', [
                    so_ids[0],
                    {
                        'date_order': date_order,
                        'effective_date': new_date
                    }
                ])
                print(f"Successfully updated date for SO {so_name} to {date_order}")

            except Exception as e:
                print(f"Error processing row {index + 1}: {str(e)}")
                continue

    except Exception as e:
        print(f"Error processing Excel file: {str(e)}")

def main():
    """Main function"""
    uid, models = connect_to_odoo()
    update_so_dates(uid, models)

if __name__ == "__main__":
    main()