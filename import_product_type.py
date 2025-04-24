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
url = 'http://119.59.102.189:8069'
db = 'MOG_LIVE'
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
        df = pd.read_excel('Data_file/Product_service.xlsx')
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
                writer = csv.DictWriter(f, fieldnames=['Internal Reference', 'Product Type', 'UOM', 'Error Message', 'Timestamp'])
                writer.writeheader()
                writer.writerows(failed_updates)
            print(f"\nFailed updates have been saved to: {failed_updates_file}")
        except Exception as e:
            print(f"Error saving failed updates: {str(e)}")

def log_failed_update(internal_ref, product_type, uom, error_message):
    """Log a failed update to both the log file and the failed updates list"""
    failed_updates.append({
        'Internal Reference': internal_ref,
        'Product Type': product_type,
        'UOM': uom,
        'Error Message': error_message,
        'Timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    })
    logging.error(f"Failed update - Internal Reference: {internal_ref}, Type: {product_type}, UOM: {uom}, Error: {error_message}")

def update_product(internal_ref, product_type, uom):
    """Update product type and unit of measure for a specific product"""
    try:
        # Search for the product using default_code
        product_ids = models.execute_kw(db, uid, password,
            'product.template', 'search',
            [[['default_code', '=', internal_ref]]]
        )
        
        if not product_ids:
            error_msg = "Product not found in system"
            log_failed_update(internal_ref, product_type, uom, error_msg)
            print(f"Product with internal reference {internal_ref} not found")
            return False
        
        # Get UOM ID
        uom_ids = models.execute_kw(db, uid, password,
            'uom.uom', 'search',
            [[['name', '=', uom]]]
        )
        
        if not uom_ids:
            error_msg = f"UOM '{uom}' not found in system"
            log_failed_update(internal_ref, product_type, uom, error_msg)
            print(f"UOM {uom} not found")
            return False
        
        # Update product
        update_vals = {
            'type': product_type,  # 'product', 'consu', or 'service'
            'uom_id': uom_ids[0],
            'uom_po_id': uom_ids[0],  # Setting the same UOM for purchase
        }
        
        result = models.execute_kw(db, uid, password,
            'product.template', 'write',
            [product_ids[0], update_vals]
        )
        
        if result:
            print(f"Successfully updated product {internal_ref}")
            logging.info(f"Successfully updated product {internal_ref}")
            return True
        else:
            error_msg = "Update operation failed"
            log_failed_update(internal_ref, product_type, uom, error_msg)
            print(f"Failed to update product {internal_ref}")
            return False
            
    except Exception as e:
        error_msg = str(e)
        log_failed_update(internal_ref, product_type, uom, error_msg)
        print(f"Error updating product {internal_ref}: {error_msg}")
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
                # Try different possible column names for internal reference
                if 'Internal Reference' in df.columns:
                    internal_ref = str(row['Internal Reference']).strip()
                elif 'Default Code' in df.columns:
                    internal_ref = str(row['Default Code']).strip()
                elif 'Reference' in df.columns:
                    internal_ref = str(row['Reference']).strip()
                else:
                    print("Could not find internal reference column. Please check the Excel file.")
                    return

                # Try different possible column names for product type
                if 'Type' in df.columns:
                    product_type = str(row['Type']).strip().lower()
                elif 'Product Type' in df.columns:
                    product_type = str(row['Product Type']).strip().lower()
                else:
                    print("Could not find product type column. Please check the Excel file.")
                    return

                # Try different possible column names for UOM
                if 'UOM' in df.columns:
                    uom = str(row['UOM']).strip()
                elif 'Unit of Measure' in df.columns:
                    uom = str(row['Unit of Measure']).strip()
                else:
                    print("Could not find UOM column. Please check the Excel file.")
                    return

                if pd.isna(internal_ref) or pd.isna(product_type) or pd.isna(uom):
                    error_msg = "Missing required data"
                    log_failed_update(internal_ref if not pd.isna(internal_ref) else "N/A",
                                    product_type if not pd.isna(product_type) else "N/A",
                                    uom if not pd.isna(uom) else "N/A",
                                    error_msg)
                    failed_updates_count += 1
                    continue
                    
                # Validate product type
                if product_type not in ['product', 'consu', 'service']:
                    error_msg = f"Invalid product type: {product_type}"
                    log_failed_update(internal_ref, product_type, uom, error_msg)
                    failed_updates_count += 1
                    continue
                    
                if update_product(internal_ref, product_type, uom):
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

if __name__ == "__main__":
    main()