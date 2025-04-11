import xmlrpc.client
import sys
from pathlib import Path

def check_required_packages():
    missing_packages = []
    try:
        import pandas as pd
    except ImportError:
        missing_packages.append("pandas")
    
    try:
        import xlrd
    except ImportError:
        missing_packages.append("xlrd==2.0.1")
    
    if missing_packages:
        print("\nRequired packages are missing:", ", ".join(missing_packages))
        print("\nPlease set up a virtual environment and install the required packages using:")
        print("\npython3 -m venv myenv")
        print("source myenv/bin/activate  # On Unix/macOS")
        print("# OR")
        print("myenv\\Scripts\\activate  # On Windows")
        print("\npip install " + " ".join(missing_packages))
        sys.exit(1)

check_required_packages()
import pandas as pd

# Odoo connection settings
SERVER_URL = 'http://mogth.work:8069'
DATABASE = 'MOG_LIVE'
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

def update_product_volumes_batch(uid, models, product_data_batch):
    """Update volumes for a batch of products"""
    success_count = 0
    error_products = []
    
    try:
        # Create a list of default codes for batch search
        default_codes = [str(item['default_code']).strip() for item in product_data_batch]
        
        # Search for all products in batch
        product_records = models.execute_kw(
            DATABASE, uid, PASSWORD,
            'product.template', 'search_read',
            [[['default_code', 'in', default_codes]]],
            {'fields': ['id', 'name', 'default_code', 'volume']}
        )
        
        # Create a mapping of default_code to product_id
        product_map = {str(record['default_code']): record for record in product_records}
        
        # Prepare batch update data
        updates = []
        for item in product_data_batch:
            default_code = str(item['default_code']).strip()
            volume = float(item['volume'])
            
            if default_code in product_map:
                product = product_map[default_code]
                updates.append({
                    'id': product['id'],
                    'default_code': default_code,
                    'old_volume': product['volume'],
                    'new_volume': volume
                })
            else:
                error_products.append({
                    'default_code': default_code,
                    'error': 'Product not found'
                })
        
        # Perform batch update
        if updates:
            for update in updates:
                try:
                    models.execute_kw(
                        DATABASE, uid, PASSWORD,
                        'product.template', 'write',
                        [[update['id']], {'volume': update['new_volume']}]
                    )
                    print(f"Updated {update['default_code']}: {update['old_volume']} → {update['new_volume']}")
                    success_count += 1
                except Exception as e:
                    error_products.append({
                        'default_code': update['default_code'],
                        'error': str(e)
                    })
        
        # Print errors if any
        for error in error_products:
            print(f"\nError updating product {error['default_code']}: {error['error']}")
        
        return success_count, len(error_products)
    
    except Exception as e:
        print(f"\nBatch update error: {e}")
        return 0, len(product_data_batch)

def main():
    # Configuration
    BATCH_SIZE = 50  # Number of records to process in each batch
    
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

    # Initialize counters
    total_success = 0
    total_errors = 0
    total_records = len(df)
    
    print("\nProcessing products in batches...")
    
    # Process data in batches
    for start_idx in range(0, len(df), BATCH_SIZE):
        batch_df = df.iloc[start_idx:start_idx + BATCH_SIZE]
        batch_data = []
        
        # Prepare batch data
        for _, row in batch_df.iterrows():
            try:
                default_code = str(row['Default Code']).strip() if 'Default Code' in row else str(row['default_code']).strip()
                volume = float(row['Cubic Meter']) if 'Cubic Meter' in row else float(row['volume'])
                
                batch_data.append({
                    'default_code': default_code,
                    'volume': volume
                })
            except Exception as e:
                print(f"Error preparing data: {e}")
                total_errors += 1
        
        # Process the batch
        if batch_data:
            print(f"\nProcessing batch {start_idx//BATCH_SIZE + 1} of {(total_records + BATCH_SIZE - 1)//BATCH_SIZE}")
            success, errors = update_product_volumes_batch(uid, models, batch_data)
            total_success += success
            total_errors += errors
        
        # Show progress
        progress = min(100, (start_idx + BATCH_SIZE) * 100 // total_records)
        print(f"Progress: {progress}% complete")

    print(f"\nImport Summary:")
    print(f"Successfully updated: {total_success}")
    print(f"Errors: {total_errors}")
    print(f"Total records processed: {total_records}")

if __name__ == "__main__":
    main()