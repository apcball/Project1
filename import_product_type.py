#!/usr/bin/env python3
import xmlrpc.client
import pandas as pd
from datetime import datetime
import logging
import sys
import csv

# Configure logging
current_time = datetime.now().strftime("%Y%m%d_%H%M%S")
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename=f'product_update_log_{current_time}.log'
)

# Create CSV file for failed updates
failed_updates_file = f'failed_updates_{current_time}.csv'
failed_updates = []

# Odoo connection parameters
url = 'http://mogdev.work:8069'
<<<<<<< HEAD
db = 'KYLD_DEV2'
=======
db = 'KYLD_DEV'
>>>>>>> 3da2cedc13e21a4d2e95ee0cb79555b06f1bfb77
username = 'apichart@mogen.co.th'
password = '471109538'

# Connect to Odoo
print("Connecting to Odoo server...")
try:
    common = xmlrpc.client.ServerProxy('{}/xmlrpc/2/common'.format(url))
    uid = common.authenticate(db, username, password, {})
    if not uid:
        print("Authentication failed! Please check your credentials.")
        sys.exit(1)
    print(f"Successfully connected to Odoo. User ID: {uid}")
    models = xmlrpc.client.ServerProxy('{}/xmlrpc/2/object'.format(url))
except Exception as e:
    print(f"Connection error: {str(e)}")
    sys.exit(1)

def read_excel_file():
    """Read the Excel file and return a DataFrame"""
    try:
        print("Reading Excel file...")
        df = pd.read_excel('Data_file/Product_service_KYLD.xlsx')
        print(f"Successfully read {len(df)} rows from Excel file")
        
        # Print column names for debugging
        print("\nAvailable columns in Excel file:")
        for col in df.columns:
            print(f"- {col}")
        
        return df
    except Exception as e:
        print(f"Error reading Excel file: {str(e)}")
        logging.error(f"Error reading Excel file: {str(e)}")
        raise

def save_failed_updates():
    """Save failed updates to CSV file"""
    if failed_updates:
        try:
            with open(failed_updates_file, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=['Default Code', 'Product Type', 'Error Message', 'Timestamp'])
                writer.writeheader()
                writer.writerows(failed_updates)
            print(f"\nFailed updates have been saved to: {failed_updates_file}")
        except Exception as e:
            print(f"Error saving failed updates: {str(e)}")

def log_failed_update(internal_ref, product_type, error_message):
    """Log a failed update to both the log file and the failed updates list"""
    failed_updates.append({
        'Default Code': internal_ref,
        'Product Type': product_type,
        'Error Message': error_message,
        'Timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    })
    logging.error(f"Failed update - Default Code: {internal_ref}, Type: {product_type}, Error: {error_message}")

def update_product(default_code, product_type):
    """Update product type for a specific product using default_code"""
    try:
        # Search for the product using default_code
        product_ids = models.execute_kw(db, uid, password,
            'product.template', 'search',
            [[['default_code', '=', default_code]]]
        )
        
        if not product_ids:
            error_msg = "Product not found in system"
            log_failed_update(default_code, product_type, error_msg)
            print(f"Product with default code {default_code} not found")
            return False
        
        # Update product type only
        update_vals = {
            'type': product_type,  # 'product', 'consu', or 'service'
        }
        
        result = models.execute_kw(db, uid, password,
            'product.template', 'write',
            [product_ids[0], update_vals]
        )
        
        if result:
            print(f"Successfully updated product {default_code} - Type: {product_type}")
            logging.info(f"Successfully updated product {default_code} - Type: {product_type}")
            return True
        else:
            error_msg = "Update operation failed"
            log_failed_update(default_code, product_type, error_msg)
            print(f"Failed to update product {default_code}")
            return False
            
    except Exception as e:
        error_msg = str(e)
        log_failed_update(default_code, product_type, error_msg)
        print(f"Error updating product {default_code}: {error_msg}")
        return False

def main():
    try:
        # Read Excel file
        df = read_excel_file()
        
        # Initialize counters
        total_products = len(df)
        successful_updates = 0
        failed_updates_count = 0
        
        # Process each row
        for index, row in df.iterrows():
            try:
                # Try different possible column names for default code
                default_code = None
                if 'Default Code' in df.columns:
                    default_code = str(row['Default Code']).strip()
                elif 'default_code' in df.columns:
                    default_code = str(row['default_code']).strip()
                elif 'Internal Reference' in df.columns:
                    default_code = str(row['Internal Reference']).strip()
                elif 'Reference' in df.columns:
                    default_code = str(row['Reference']).strip()
                elif 'รหัสสินค้า' in df.columns:
                    default_code = str(row['รหัสสินค้า']).strip()
                else:
                    print("Could not find default code column. Available columns:")
                    for col in df.columns:
                        print(f"- {col}")
                    return

                # Try different possible column names for product type
                product_type = None
                if 'Product Type' in df.columns:
                    product_type_str = str(row['Product Type']).strip().lower()
                    # Map display product types to Odoo internal types
                    if product_type_str == 'storable product':
                        product_type = 'product'
                    elif product_type_str == 'consumable':
                        product_type = 'consu'
                    elif product_type_str == 'service':
                        product_type = 'service'
                    else:
                        product_type = product_type_str
                elif 'Type' in df.columns:
                    product_type_str = str(row['Type']).strip().lower()
                    # Map display product types to Odoo internal types
                    if product_type_str == 'storable product':
                        product_type = 'product'
                    elif product_type_str == 'consumable':
                        product_type = 'consu'
                    elif product_type_str == 'service':
                        product_type = 'service'
                    else:
                        product_type = product_type_str
                elif 'product_type' in df.columns:
                    product_type_str = str(row['product_type']).strip().lower()
                    # Map display product types to Odoo internal types
                    if product_type_str == 'storable product':
                        product_type = 'product'
                    elif product_type_str == 'consumable':
                        product_type = 'consu'
                    elif product_type_str == 'service':
                        product_type = 'service'
                    else:
                        product_type = product_type_str
                elif 'type' in df.columns:
                    product_type_str = str(row['type']).strip().lower()
                    # Map display product types to Odoo internal types
                    if product_type_str == 'storable product':
                        product_type = 'product'
                    elif product_type_str == 'consumable':
                        product_type = 'consu'
                    elif product_type_str == 'service':
                        product_type = 'service'
                    else:
                        product_type = product_type_str
                elif 'ประเภทสินค้า' in df.columns:
                    # Map Thai product type to Odoo product type values
                    thai_type = str(row['ประเภทสินค้า']).strip()
                    if 'สินค้าคงคลัง' in thai_type or 'คงคลัง' in thai_type:
                        product_type = 'product'
                    elif 'วัตถุดิบ' in thai_type or 'วัสดุสิ้นเปลือง' in thai_type:
                        product_type = 'consu'
                    elif 'บริการ' in thai_type or 'service' in thai_type.lower():
                        product_type = 'service'
                    else:
                        # Default to product if not recognized
                        product_type = 'product'
                        print(f"Warning: Unrecognized product type '{thai_type}', defaulting to 'product'")
                else:
                    print("Could not find product type column. Available columns:")
                    for col in df.columns:
                        print(f"- {col}")
                    return

                # Check for missing data
                if pd.isna(default_code) or pd.isna(product_type) or default_code == 'nan' or product_type == 'nan':
                    error_msg = "Missing required data"
                    log_failed_update(default_code if not pd.isna(default_code) and default_code != 'nan' else "N/A",
                                    product_type if not pd.isna(product_type) and product_type != 'nan' else "N/A",
                                    error_msg)
                    failed_updates_count += 1
                    continue
                    
                # Validate product type
                valid_types = ['product', 'consu', 'service']
                if product_type not in valid_types:
                    error_msg = f"Invalid product type: {product_type}. Must be 'product', 'consu', or 'service'"
                    log_failed_update(default_code, product_type, error_msg)
                    failed_updates_count += 1
                    continue
                    
                if update_product(default_code, product_type):
                    successful_updates += 1
                else:
                    failed_updates_count += 1
                
                # Show progress
                if (index + 1) % 10 == 0:
                    print(f"\nProgress: {index + 1}/{total_products} products processed")
                    print(f"Successful: {successful_updates}, Failed: {failed_updates_count}")
                
            except Exception as e:
                error_msg = f"Error processing row {index + 2}: {str(e)}"
                print(error_msg)
                logging.error(error_msg)
                failed_updates_count += 1
                continue
            
        # Print final summary
        print("\nUpdate Process Completed!")
        print(f"Total products processed: {total_products}")
        print(f"Successful updates: {successful_updates}")
        print(f"Failed updates: {failed_updates_count}")
        
        # Save failed updates to CSV
        save_failed_updates()
            
    except Exception as e:
        print(f"Main execution error: {str(e)}")
        logging.error(f"Main execution error: {str(e)}")
        # Save any failed updates that occurred before the error
        save_failed_updates()

if __name__ == "__main__":
    try:
        main()
        print("\nScript execution completed.")
    except KeyboardInterrupt:
        print("\nScript execution interrupted by user.")
        save_failed_updates()
    except Exception as e:
        print(f"\nUnexpected error: {str(e)}")
        logging.error(f"Unexpected error: {str(e)}")
        save_failed_updates()