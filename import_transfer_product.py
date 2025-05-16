import xmlrpc.client
import pandas as pd
from datetime import datetime
import logging
import sys
import json

# Configure logging
log_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

# Main logger for process information
main_logger = logging.getLogger('main')
main_logger.setLevel(logging.INFO)

# Add file handler for main log
main_file_handler = logging.FileHandler('import_transfer.log')
main_file_handler.setFormatter(log_formatter)
main_logger.addHandler(main_file_handler)

# Add console handler for main log
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setFormatter(log_formatter)
main_logger.addHandler(console_handler)

# Import records logger
import_logger = logging.getLogger('import_records')
import_logger.setLevel(logging.INFO)

# Add file handler for import records
import_file_handler = logging.FileHandler('import_records.log')
import_file_handler.setFormatter(log_formatter)
import_logger.addHandler(import_file_handler)

# Odoo connection parameters
ODOO_CONFIG = {
    'url': 'http://mogdev.work:8069',
    'db': 'MOG_Test',
    'username': 'apichart@mogen.co.th',
    'password': '471109538'
}

def connect_to_odoo():
    """Establish connection to Odoo server"""
    try:
        common = xmlrpc.client.ServerProxy(f"{ODOO_CONFIG['url']}/xmlrpc/2/common")
        uid = common.authenticate(
            ODOO_CONFIG['db'],
            ODOO_CONFIG['username'],
            ODOO_CONFIG['password'],
            {}
        )
        models = xmlrpc.client.ServerProxy(f"{ODOO_CONFIG['url']}/xmlrpc/2/object")
        return uid, models
    except Exception as e:
        main_logger.error(f"Failed to connect to Odoo: {str(e)}")
        raise

def get_location_id(models, uid, location_name):
    """Get location ID by name with enhanced search"""
    try:
        # Log the location name we're searching for
        main_logger.info(f"Searching for location: {location_name}")
        
        # First try exact match
        location_ids = models.execute_kw(
            ODOO_CONFIG['db'], uid, ODOO_CONFIG['password'],
            'stock.location', 'search',
            [[['name', '=', location_name]]]
        )
        
        if location_ids:
            # Get location details for logging
            location_data = models.execute_kw(
                ODOO_CONFIG['db'], uid, ODOO_CONFIG['password'],
                'stock.location', 'read',
                [location_ids[0]],
                {'fields': ['name', 'complete_name']}
            )
            main_logger.info(f"Found location: {location_data[0]['complete_name']}")
            return location_ids[0]
        
        # If no exact match, try case-insensitive contains search
        location_ids = models.execute_kw(
            ODOO_CONFIG['db'], uid, ODOO_CONFIG['password'],
            'stock.location', 'search',
            [[['complete_name', 'ilike', location_name]]]
        )
        
        if location_ids:
            # Get all matching locations for logging
            locations = models.execute_kw(
                ODOO_CONFIG['db'], uid, ODOO_CONFIG['password'],
                'stock.location', 'read',
                [location_ids],
                {'fields': ['name', 'complete_name']}
            )
            main_logger.info(f"Found similar locations: {[loc['complete_name'] for loc in locations]}")
            return location_ids[0]
        
        # If still not found, log available locations
        all_locations = models.execute_kw(
            ODOO_CONFIG['db'], uid, ODOO_CONFIG['password'],
            'stock.location', 'search_read',
            [[['usage', 'in', ['internal', 'transit']]]],
            {'fields': ['name', 'complete_name']}
        )
        main_logger.error(f"Location '{location_name}' not found. Available locations: {[loc['complete_name'] for loc in all_locations]}")
        return False
        
    except Exception as e:
        main_logger.error(f"Error searching for location '{location_name}': {str(e)}")
        return False

def get_product_id(models, uid, product_code):
    """Get product ID by searching both default_code and old_product_code"""
    # First try to find by default_code
    product_ids = models.execute_kw(
        ODOO_CONFIG['db'], uid, ODOO_CONFIG['password'],
        'product.product', 'search',
        [[['default_code', '=', product_code]]]
    )
    
    if not product_ids:
        # If not found, try to find by old_product_code
        product_ids = models.execute_kw(
            ODOO_CONFIG['db'], uid, ODOO_CONFIG['password'],
            'product.product', 'search',
            [[['old_product_code', '=', product_code]]]
        )
    
    if product_ids:
        # Get product details for logging
        product_data = models.execute_kw(
            ODOO_CONFIG['db'], uid, ODOO_CONFIG['password'],
            'product.product', 'read',
            [product_ids[0]],
            {'fields': ['name', 'default_code', 'old_product_code']}
        )
        product = product_data[0]
        main_logger.info(f"Found product: {product['name']} (Default Code: {product['default_code']}, Old Code: {product.get('old_product_code', 'N/A')})")
        return product_ids[0]
    
    main_logger.error(f"Product not found with code {product_code} in either default_code or old_product_code")
    return False

def get_uom_id(models, uid, product_id):
    """Get UoM ID for the product"""
    product_data = models.execute_kw(
        ODOO_CONFIG['db'], uid, ODOO_CONFIG['password'],
        'product.product', 'read',
        [product_id],
        {'fields': ['uom_id']}
    )
    return product_data[0]['uom_id'][0] if product_data else False

def read_excel_file(file_path):
    """Read the Excel file and return a DataFrame"""
    try:
        df = pd.read_excel(file_path)
        main_logger.info(f"Excel file columns: {list(df.columns)}")
        
        # Convert date columns to datetime if they exist
        date_columns = df.select_dtypes(include=['datetime64[ns]']).columns
        for col in date_columns:
            df[col] = pd.to_datetime(df[col]).dt.strftime('%Y-%m-%d %H:%M:%S')
        return df
    except Exception as e:
        main_logger.error(f"Failed to read Excel file: {str(e)}")
        raise

def log_transfer_details(doc_number, doc_date, products, status, error=None):
    """Log transfer details to import_records.log"""
    log_entry = {
        'document_number': doc_number,
        'date': doc_date,
        'products': products,
        'status': status
    }
    if error:
        log_entry['error'] = str(error)
    
    import_logger.info(json.dumps(log_entry, ensure_ascii=False))

def main():
    try:
        # Connect to Odoo
        uid, models = connect_to_odoo()
        main_logger.info("Successfully connected to Odoo")

        # Read Excel file
        excel_file = "Data_file/import_internal_tranfer_ตัดจ่าย.xlsx"
        df = read_excel_file(excel_file)
        main_logger.info(f"Successfully read Excel file: {excel_file}")

        # Map the actual Excel columns to required columns
        df = df.rename(columns={
            'default_code': 'Product Code',
            'product_name': 'Product Description',
            'demand': 'Quantity'
        })

        # Convert numeric columns to Python native types
        df['Quantity'] = df['Quantity'].astype(float)
        if 'scheduled date' in df.columns:
            df['scheduled date'] = pd.to_datetime(df['scheduled date']).dt.strftime('%Y-%m-%d %H:%M:%S')

        # Validate required columns
        required_columns = ['Product Code', 'Product Description', 'Quantity', 
                          'Source Location', 'Destination Location']
        
        # Check which columns are present
        present_columns = list(df.columns)
        main_logger.info(f"Present columns after mapping: {present_columns}")
        
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            raise ValueError(f"Missing required columns: {missing_columns}")

        # Get internal transfer picking type
        picking_type_ids = models.execute_kw(
            ODOO_CONFIG['db'], uid, ODOO_CONFIG['password'],
            'stock.picking.type', 'search',
            [[['code', '=', 'internal']]]
        )
        if not picking_type_ids:
            raise ValueError("Internal transfer picking type not found")
        
        # Group by document number (name) and date
        successful_transfers = 0
        failed_transfers = 0

        # Group the dataframe by document number and date
        grouped = df.groupby(['name', 'scheduled date'])

        for (doc_number, doc_date), group_df in grouped:
            try:
                if pd.isna(doc_number):
                    continue

                main_logger.info(f"Processing document: {doc_number} dated {doc_date}")

                # Take the first row for header information
                first_row = group_df.iloc[0].to_dict()  # Convert to dict to avoid numpy types

                # Get location IDs
                source_location_id = get_location_id(models, uid, first_row['Source Location'])
                dest_location_id = get_location_id(models, uid, first_row['Destination Location'])
                
                if not (source_location_id and dest_location_id):
                    raise ValueError(f"Source or destination location not found for document {doc_number}")
                
                # Create the transfer header
                picking_vals = {
                    'picking_type_id': picking_type_ids[0],
                    'location_id': source_location_id,
                    'location_dest_id': dest_location_id,
                    'scheduled_date': str(doc_date) if doc_date else datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    'name': str(doc_number),
                    'company_id': 1,
                    'note': f"Imported from Excel - Document: {doc_number}"
                }

                # Add origin (source document) only if it exists and is not empty
                if pd.notna(first_row.get('requisition order')) and str(first_row.get('requisition order')).strip():
                    picking_vals['origin'] = str(first_row['requisition order']).strip()

                # Add partner_id (contact) if provided
                if pd.notna(first_row.get('contect')):
                    partner_ids = models.execute_kw(
                        ODOO_CONFIG['db'], uid, ODOO_CONFIG['password'],
                        'res.partner', 'search',
                        [[['name', '=', str(first_row['contect'])]]]
                    )
                    if partner_ids:
                        picking_vals['partner_id'] = partner_ids[0]
                        main_logger.info(f"Found contact: {first_row['contect']}")
                    else:
                        main_logger.warning(f"Contact not found: {first_row['contect']}")

                # Create the transfer
                picking_id = models.execute_kw(
                    ODOO_CONFIG['db'], uid, ODOO_CONFIG['password'],
                    'stock.picking', 'create',
                    [picking_vals]
                )

                # Track products for logging
                processed_products = []

                # Process each product line in the group
                for _, row in group_df.iterrows():
                    try:
                        if pd.isna(row['Product Code']):
                            continue

                        row_dict = row.to_dict()  # Convert to dict to avoid numpy types
                        product_code = str(row_dict['Product Code'])
                        product_id = get_product_id(models, uid, product_code)
                        
                        if not product_id:
                            raise ValueError(f"Product not found: {product_code}")

                        # Get UoM
                        uom_id = get_uom_id(models, uid, product_id)
                        if not uom_id:
                            raise ValueError(f"UoM not found for product {product_code}")

                        # Create move line for this product
                        move_vals = {
                            'name': str(row_dict['Product Description']),
                            'product_id': product_id,
                            'product_uom_qty': float(row_dict['Quantity']),
                            'product_uom': uom_id,
                            'picking_id': picking_id,
                            'location_id': source_location_id,
                            'location_dest_id': dest_location_id,
                        }

                        move_id = models.execute_kw(
                            ODOO_CONFIG['db'], uid, ODOO_CONFIG['password'],
                            'stock.move', 'create',
                            [move_vals]
                        )

                        # Add to processed products list
                        processed_products.append({
                            'code': product_code,
                            'description': str(row_dict['Product Description']),
                            'quantity': float(row_dict['Quantity'])
                        })

                        main_logger.info(f"Added product {product_code} to transfer {doc_number}")

                    except Exception as e:
                        main_logger.error(f"Error adding product {row_dict['Product Code']} to transfer {doc_number}: {str(e)}")
                        continue

                # Confirm the picking after all products are added
                models.execute_kw(
                    ODOO_CONFIG['db'], uid, ODOO_CONFIG['password'],
                    'stock.picking', 'action_confirm',
                    [[picking_id]]
                )

                main_logger.info(f"Created and confirmed transfer {doc_number} with {len(group_df)} products")
                successful_transfers += 1

                # Log successful transfer details
                log_transfer_details(
                    doc_number=doc_number,
                    doc_date=str(doc_date),
                    products=processed_products,
                    status='success'
                )

            except Exception as e:
                main_logger.error(f"Error processing document {doc_number}: {str(e)}")
                failed_transfers += 1
                
                # Log failed transfer details
                log_transfer_details(
                    doc_number=doc_number,
                    doc_date=str(doc_date),
                    products=[],
                    status='failed',
                    error=str(e)
                )
                continue

        main_logger.info(f"Import process completed. Successful transfers: {successful_transfers}, Failed transfers: {failed_transfers}")

    except Exception as e:
        main_logger.error(f"Main process failed: {str(e)}")
        raise

if __name__ == "__main__":
    main()