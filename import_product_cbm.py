import xmlrpc.client
import sys
from pathlib import Path

# Check and install required packages
def install_required_packages():
    try:
        import pandas as pd
    except ImportError:
        print("Installing pandas...")
        import subprocess
        subprocess.check_call([sys.executable, "-m", "pip", "install", "pandas"])
        import pandas as pd
    
    try:
        import xlrd
    except ImportError:
        print("Installing xlrd...")
        import subprocess
        subprocess.check_call([sys.executable, "-m", "pip", "install", "xlrd==2.0.1"])
        import xlrd

install_required_packages()
import pandas as pd

# Odoo connection settings
SERVER_URL = 'http://mogth.work:8069'
DATABASE = 'MOG_Training'
USERNAME = 'apichart@mogen.co.th'
PASSWORD = '471109538'

def connect_to_odoo():
    """Establish connection to Odoo server"""
    common = xmlrpc.client.ServerProxy(f'{SERVER_URL}/xmlrpc/2/common')
    uid = common.authenticate(DATABASE, USERNAME, PASSWORD, {})
    models = xmlrpc.client.ServerProxy(f'{SERVER_URL}/xmlrpc/2/object')
    return uid, models

def read_excel_file(file_path):
    """Read the Excel file and return DataFrame"""
    try:
        df = pd.read_excel(file_path)
        print("\nData read from Excel file:")
        print(df.head())
        print(f"\nTotal records in Excel: {len(df)}")
        print("\nColumn names in Excel:", df.columns.tolist())
        return df
    except Exception as e:
        print(f"Error reading Excel file: {e}")
        return None

def update_product_volume(uid, models, default_code, volume):
    """Update volume for a product"""
    try:
        # Search for product by default_code
        product_ids = models.execute_kw(
            DATABASE, uid, PASSWORD,
            'product.template', 'search',
            [[['default_code', '=', default_code]]]
        )
        
        if product_ids:
            # Get current value before update
            product_data = models.execute_kw(
                DATABASE, uid, PASSWORD,
                'product.template', 'read',
                [product_ids[0]], {'fields': ['name', 'default_code', 'volume']}
            )[0]
            
            print(f"\nProduct found: {default_code}")
            print(f"Current name: {product_data.get('name')}")
            print(f"Current volume: {product_data.get('volume')}")
            print(f"New volume to set: {volume}")
            
            # Update the volume field
            models.execute_kw(
                DATABASE, uid, PASSWORD,
                'product.template', 'write',
                [product_ids, {'volume': volume}]
            )
            
            # Verify the update
            updated_data = models.execute_kw(
                DATABASE, uid, PASSWORD,
                'product.template', 'read',
                [product_ids[0]], {'fields': ['volume']}
            )[0]
            
            print(f"Updated volume value: {updated_data.get('volume')}")
            return True
        else:
            print(f"\nProduct not found: {default_code}")
            return False
    except Exception as e:
        print(f"\nError updating product {default_code}: {e}")
        return False

def main():
    # Connect to Odoo
    print("\nConnecting to Odoo...")
    uid, models = connect_to_odoo()
    if not uid:
        print("Failed to connect to Odoo")
        return
    print("Successfully connected to Odoo")

    # Read Excel file
    print("\nReading Excel file...")
    file_path = Path('Data_file/Prd_CBM.xlsx')
    df = read_excel_file(file_path)
    if df is None:
        return

    # Process each row in the Excel file
    success_count = 0
    error_count = 0

    print("\nProcessing products...")
    for index, row in df.iterrows():
        try:
            # Print the current row data
            print(f"\nProcessing row {index + 1}:")
            print(row)
            
            # Get default_code and volume from the correct column names
            default_code = str(row['Default Code']).strip() if 'Default Code' in row else str(row['default_code']).strip()
            volume = float(row['Cubic Meter']) if 'Cubic Meter' in row else float(row['volume'])
            
            print(f"Processing: Default Code = {default_code}, Volume = {volume}")
            
            if update_product_volume(uid, models, default_code, volume):
                success_count += 1
            else:
                error_count += 1
        except Exception as e:
            print(f"Error processing row {index + 1}: {e}")
            error_count += 1

    print(f"\nImport Summary:")
    print(f"Successfully updated: {success_count}")
    print(f"Errors: {error_count}")

if __name__ == "__main__":
    main()