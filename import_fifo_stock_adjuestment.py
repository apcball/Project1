#!/usr/bin/env python3
import xmlrpc.client
import pandas as pd
import logging
from datetime import datetime
import os
import re

# Odoo connection parameters
HOST = 'http://mogth.work:8069'
DB = 'MOG_LIVE'
USERNAME = 'apichart@mogen.co.th'
PASSWORD = '471109538'

# Excel file path
EXCEL_FILE = 'Data_file/FG10 Adjuestment-02-1.xlsx'
# Default picking type (Delivery Orders for delivery operations)
DEFAULT_PICKING_TYPE = 'Delivery Orders'
# Default source location - None means we'll use the column value or the picking type's default
DEFAULT_SOURCE_LOCATION = None

# Configure logging
log_filename = f'fifo_stock_import_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_filename, encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def connect_to_odoo():
    try:
        common = xmlrpc.client.ServerProxy(f'{HOST}/xmlrpc/2/common')
        uid = common.authenticate(DB, USERNAME, PASSWORD, {})
        if not uid:
            raise Exception("Authentication failed")
        models = xmlrpc.client.ServerProxy(f'{HOST}/xmlrpc/2/object')
        logger.info(f"Successfully connected to Odoo as user ID: {uid}")
        
        # Check Odoo version and available fields for stock.picking
        try:
            # Get Odoo version
            version_info = models.execute_kw(DB, uid, PASSWORD, 'ir.module.module', 'search_read',
                [[['name', '=', 'base']]],
                {'fields': ['latest_version']}
            )
            if version_info:
                logger.info(f"Odoo version: {version_info[0].get('latest_version', 'Unknown')}")
            
            # Get fields for stock.picking
            picking_fields = models.execute_kw(DB, uid, PASSWORD, 'stock.picking', 'fields_get', [])
            date_fields = [field for field in picking_fields.keys() if 'date' in field.lower()]
            logger.info(f"Available date fields in stock.picking: {date_fields}")
        except Exception as e:
            logger.warning(f"Could not determine Odoo version or fields: {str(e)}")
        
        return uid, models
    except Exception as e:
        logger.error(f"Failed to connect to Odoo: {str(e)}")
        raise

def read_excel_file():
    try:
        if not os.path.exists(EXCEL_FILE):
            raise FileNotFoundError(f"Excel file not found: {EXCEL_FILE}")
        
        # Read Excel file without parsing dates - we'll handle date parsing manually
        df = pd.read_excel(EXCEL_FILE, parse_dates=False)
        logger.info(f"Successfully read Excel file with {len(df)} rows")
        logger.info(f"Excel columns: {list(df.columns)}")

        # Check for "Delivery Orders" column and map it to picking_type_id (source location)
        if 'Delivery Orders' in df.columns:
            logger.info("Found 'Delivery Orders' column, using it as source location")
            # If both picking_type_id and "Delivery Orders" exist, use "Delivery Orders"
            if 'picking_type_id' in df.columns:
                logger.info("Both 'picking_type_id' and 'Delivery Orders' columns exist, prioritizing 'Delivery Orders'")
                df['picking_type_id'] = df['Delivery Orders']
            else:
                # Create picking_type_id column from "Delivery Orders"
                df['picking_type_id'] = df['Delivery Orders']
                logger.info("Added 'picking_type_id' column from 'Delivery Orders' values")

        # Handle sequence column if present
        if 'sequence' in df.columns:
            df.loc[:, 'sequence'] = pd.to_numeric(df['sequence'], errors='coerce')
            df.loc[df['sequence'].isna(), 'sequence'] = 0
            logger.info("Processed sequence column")
          # Convert date columns to datetime explicitly
        if 'scheduled_date' in df.columns:
            # Check for all NaN values in scheduled_date
            if df['scheduled_date'].isna().all():
                logger.warning("All scheduled_date values are NaN, using current date")
                current_datetime = datetime.now()
                df['scheduled_date'] = current_datetime.strftime('%Y-%m-%d %H:%M:%S')
                df['scheduled_date_orig'] = current_datetime
                logger.info(f"Added current datetime to all rows: {df['scheduled_date'].iloc[0]}")
            else:
                # Convert to datetime and handle errors
                df['scheduled_date'] = pd.to_datetime(df['scheduled_date'], errors='coerce')
                logger.info(f"scheduled_date dtype: {df['scheduled_date'].dtype}")
                logger.info(f"scheduled_date sample: {df['scheduled_date'].iloc[0] if len(df) > 0 else None}")
                
                # For rows with NaN dates, use current datetime
                missing_date_rows = df['scheduled_date'].isna()
                if missing_date_rows.any():
                    current_datetime = datetime.now()
                    df.loc[missing_date_rows, 'scheduled_date'] = current_datetime
                    logger.warning(f"Replaced {missing_date_rows.sum()} NaN scheduled_date values with current datetime")
                
                # Keep the original datetime objects for later use
                df['scheduled_date_orig'] = df['scheduled_date'].copy()
                
                # Convert to string in the format expected by Odoo
                df.loc[:, 'scheduled_date'] = df['scheduled_date'].dt.strftime('%Y-%m-%d %H:%M:%S')
                logger.info(f"Converted scheduled_date to string format. Sample: {df['scheduled_date'].iloc[0] if len(df) > 0 else None}")
        else:
            # If no scheduled_date column, add it with current datetime
            current_datetime = datetime.now()
            df['scheduled_date'] = current_datetime.strftime('%Y-%m-%d %H:%M:%S')
            df['scheduled_date_orig'] = current_datetime
            logger.info(f"Added missing scheduled_date column with current time: {df['scheduled_date'].iloc[0]}")
        
        # Fix the picking type name in the DataFrame        # Note: 'picking_type_id' column is now used for source location
        if 'picking_type_id' in df.columns:
            # Clean up the source location values if needed
            df['picking_type_id'] = df['picking_type_id'].replace('My Company: OB FIFO', 'OB FIFO')
            logger.info("Using 'picking_type_id' column as source location")
            logger.info("Fixed picking type name from 'My Company: OB FIFO' to 'OB FIFO'")
        
        # Check for required columns
        required_fields = ['product_id', 'product_uom_qty']
        missing_columns = [field for field in required_fields if field not in df.columns]
        if missing_columns:
            logger.error(f"Required columns missing from Excel file: {missing_columns}")
            raise ValueError(f"Required columns missing: {missing_columns}")
        
        # Check for missing values in product_id and product_uom_qty (these must be present in every row)
        for field in ['product_id', 'product_uom_qty']:
            if df[field].isna().any():
                missing_rows = df[df[field].isna()].index.tolist()
                logger.error(f"Column '{field}' has missing values in rows: {missing_rows}")
                logger.error("Each product line must have a product code and quantity.")
                raise ValueError(f"Missing required product data in rows: {missing_rows}")
          # Make sure location_dest_id and source location (picking_type_id column) exist
        if 'location_dest_id' not in df.columns:
            # ไม่ต้องเติมค่า default ใดๆ ถ้าไม่มี column นี้
            logger.info("No 'location_dest_id' column found. Will skip this field if missing.")
              # Note: 'picking_type_id' column is now used as source location
        if 'picking_type_id' not in df.columns:
            # We'll keep the column name for backwards compatibility but use it for source location
            df['picking_type_id'] = DEFAULT_SOURCE_LOCATION  # This will be None, handled later
            logger.info(f"Added missing 'picking_type_id' column (source location) with default value None")
        
        # Try to identify groups by origin if available
        if 'origin' in df.columns and not df['origin'].isna().all():
            logger.info("Using 'origin' column to identify transfer groups")
            # Use ffill() instead of fillna(method='ffill') to avoid deprecation warning
            df['transfer_group'] = df['origin'].ffill()
        else:
            # If no origin, create groups based on consecutive rows with the same partner_id
            logger.info("No valid 'origin' column, creating transfer groups based on consecutive rows")
            # Create a new group whenever partner_id changes or is first defined
            if 'partner_id' in df.columns and not df['partner_id'].isna().all():
                df['group_change'] = df['partner_id'].ne(df['partner_id'].shift()).cumsum()
            else:
                # If no partner_id, just create sequential groups of 5 rows (arbitrary)
                df['group_change'] = (df.index // 5) + 1
            
            df['transfer_group'] = df['group_change']
        
        # Now fill missing values within each group
        for group_id, group_df in df.groupby('transfer_group'):            # Find first non-null values for location_dest_id and source location (picking_type_id column) in this group
            first_location = group_df['location_dest_id'].dropna().iloc[0] if not group_df['location_dest_id'].isna().all() else 'Customers'
            first_source_location = group_df['picking_type_id'].dropna().iloc[0] if not group_df['picking_type_id'].isna().all() else DEFAULT_SOURCE_LOCATION
            
            # Fill missing location_dest_id values in this group
            group_indices = group_df.index[group_df['location_dest_id'].isna()]
            if len(group_indices) > 0:
                logger.info(f"Filling missing location_dest_id with '{first_location}' for {len(group_indices)} rows in group {group_id}")
                df.loc[group_indices, 'location_dest_id'] = first_location
            
            # Fill missing source location (picking_type_id column) values in this group
            group_indices = group_df.index[group_df['picking_type_id'].isna()]
            if len(group_indices) > 0:
                logger.info(f"Filling missing source location with '{first_source_location}' for {len(group_indices)} rows in group {group_id}")
                df.loc[group_indices, 'picking_type_id'] = first_source_location
        
        # Remove the temporary columns
        if 'transfer_group' in df.columns:
            df = df.drop(columns=['transfer_group'])
        if 'group_change' in df.columns:
            df = df.drop(columns=['group_change'])
        
        # Group by scheduled_date and product_id and sum quantities
        logger.info("Starting to group duplicate product entries...")
        original_row_count = len(df)
          # Define key columns to group by - products with the same values in these columns will be merged
        group_keys = ['product_id']
        
        # Use 'date_done' as scheduled_date if scheduled_date is all NaN
        if 'scheduled_date' in df.columns and df['scheduled_date'].isna().all() and 'date_done' in df.columns:
            logger.info("Using date_done column as scheduled_date")
            df['scheduled_date'] = df['date_done']
            
        # Make sure we have enough valid columns to group by
        valid_scheduled_date = 'scheduled_date' in df.columns and not df['scheduled_date'].isna().all()
        if valid_scheduled_date:
            group_keys.append('scheduled_date')
              # Add other grouping columns if they exist and aren't all null
        # Note: 'picking_type_id' column is now used as source location
        for col in ['location_dest_id', 'picking_type_id', 'partner_id', 'origin']:
            if col in df.columns and not df[col].isna().all():
                group_keys.append(col)
                
        # Make sure we have at least product_id for grouping
        logger.info(f"Grouping by columns: {group_keys}")
        
        # Identify columns to aggregate
        sum_columns = ['product_uom_qty']
        if 'price_unit' in df.columns:
            # For price_unit, we'll take the weighted average
            df['total_value'] = df['product_uom_qty'] * df['price_unit'].fillna(0)        # Check if DataFrame is empty
        if len(df) == 0:
            logger.error("DataFrame is empty after preprocessing")
            return df
            
        # Check if required columns exist
        missing_req_cols = [col for col in ['product_id', 'product_uom_qty'] if col not in df.columns]
        if missing_req_cols:
            logger.error(f"Required columns missing: {missing_req_cols}")
            return df
            
        try:
            # Group the DataFrame
            logger.info(f"Grouping DataFrame with {len(df)} rows")
            
            # Make sure product_uom_qty is numeric
            df['product_uom_qty'] = pd.to_numeric(df['product_uom_qty'], errors='coerce')
            
            # Define aggregations
            aggs = {'product_uom_qty': 'sum'}
            
            if 'sequence' in df.columns:
                aggs['sequence'] = 'first'  # Keep the first sequence number
                
            grouped_df = df.groupby(group_keys, as_index=False).agg(aggs)
            logger.info(f"After grouping: {len(grouped_df)} rows")
        except Exception as e:
            logger.error(f"Error during grouping: {str(e)}")
            # Return original DataFrame if grouping fails
            return df
          # Handling price_unit if available
        try:
            if 'price_unit' in df.columns and 'total_value' in df.columns:
                # Calculate total values per group
                total_values = df.groupby(group_keys)['total_value'].sum().reset_index()
                      # Merge the totals back
            if len(grouped_df) > 0 and len(total_values) > 0:  # Make sure both DataFrames are not empty
                grouped_df = grouped_df.merge(total_values, on=group_keys, how='left')
                
                # Calculate weighted average price
                grouped_df['price_unit'] = grouped_df['total_value'] / grouped_df['product_uom_qty']
                grouped_df = grouped_df.drop(columns=['total_value'])
                logger.info("Successfully calculated weighted average prices")
        except Exception as e:
            logger.error(f"Error handling price calculation: {str(e)}")
            # If price calculation fails, try to keep the original prices
            if 'price_unit' in df.columns:
                # Just take the first price for each group as a fallback
                price_agg = df.groupby(group_keys)['price_unit'].first().reset_index()
                grouped_df = grouped_df.merge(price_agg, on=group_keys, how='left')
                logger.warning("Using first price value for each group due to calculation error")
              # Log the results
        grouped_row_count = len(grouped_df) if 'grouped_df' in locals() else 0
        duplicate_entries = original_row_count - grouped_row_count
        logger.info(f"Grouped duplicate products: {original_row_count} rows reduced to {grouped_row_count} rows ({duplicate_entries} duplicates merged)")
        
        # If grouping resulted in zero rows, use the original DataFrame
        if grouped_row_count == 0 and original_row_count > 0:
            logger.warning("Grouping resulted in 0 rows. Using original DataFrame instead.")
            return df
        
        logger.info("Excel file validation and data filling completed successfully")
        return grouped_df
    except Exception as e:
        logger.error(f"Failed to read Excel file: {str(e)}")
        raise

def get_or_create_partner(models, uid, partner_name):
    try:
        # First try exact match
        partner_ids = models.execute_kw(DB, uid, PASSWORD,
            'res.partner', 'search',
            [[['name', '=', partner_name]]]
        )
        
        # If not found, try with ilike
        if not partner_ids:
            partner_ids = models.execute_kw(DB, uid, PASSWORD,
                'res.partner', 'search',
                [[['name', 'ilike', partner_name]]]
            )
        
        if partner_ids:
            logger.info(f"Found partner ID {partner_ids[0]} for '{partner_name}'")
            return partner_ids[0]
        
        # If still not found, create a new partner
        logger.warning(f"Partner '{partner_name}' not found. Creating new partner.")
        new_partner_id = models.execute_kw(DB, uid, PASSWORD,
            'res.partner', 'create',
            [{'name': partner_name}]
        )
        logger.info(f"Created new partner '{partner_name}' with ID {new_partner_id}")
        return new_partner_id
    except Exception as e:
        logger.error(f"Error finding/creating partner '{partner_name}': {str(e)}")
        return False

def get_location_id(models, uid, location_name):
    try:
        location_name = str(location_name).strip()
        logger.info(f"Searching for location: '{location_name}'")

        # ถ้าเป็น Customers หรือ Partners/Customers ให้หา usage = 'customer'
        if location_name.lower() in ['customers', 'partners/customers', 'partner/customers']:
            location_ids = models.execute_kw(DB, uid, PASSWORD,
                'stock.location', 'search',
                [[['usage', '=', 'customer']]],
                {'limit': 1}
            )
            if location_ids:
                logger.info(f"Found customer location for '{location_name}'")
                return location_ids[0]
            else:
                logger.error(f"Customer location not found for '{location_name}'")
                return False

        # ถ้าเป็น Vendors หรือ Partners/Vendors ให้หา usage = 'supplier'
        if location_name.lower() in ['vendors', 'partners/vendors', 'partner/vendors']:
            location_ids = models.execute_kw(DB, uid, PASSWORD,
                'stock.location', 'search',
                [[['usage', '=', 'supplier']]],
                {'limit': 1}
            )
            if location_ids:
                logger.info(f"Found supplier location for '{location_name}'")
                return location_ids[0]
            else:
                logger.error(f"Supplier location not found for '{location_name}'")
                return False

        # First try exact match
        location_ids = models.execute_kw(DB, uid, PASSWORD,
            'stock.location', 'search',
            [[['name', '=', location_name], ['usage', '=', 'internal']]]
        )
        
        if location_ids:
            logger.info(f"Found location with exact match: '{location_name}'")
            return location_ids[0]
        
        # Try with complete_name for hierarchical locations
        location_ids = models.execute_kw(DB, uid, PASSWORD,
            'stock.location', 'search',
            [[['complete_name', 'ilike', location_name], ['usage', '=', 'internal']]]
        )
        
        if location_ids:
            location_data = models.execute_kw(DB, uid, PASSWORD,
                'stock.location', 'read',
                [location_ids[0]],
                {'fields': ['name', 'complete_name']}
            )
            logger.info(f"Found location with complete_name match: '{location_data[0].get('complete_name', location_data[0]['name'])}' for '{location_name}'")
            return location_ids[0]
        
        # Try with ilike search if exact match fails
        location_ids = models.execute_kw(DB, uid, PASSWORD,
            'stock.location', 'search',
            [[['name', 'ilike', location_name], ['usage', '=', 'internal']]]
        )
        
        if location_ids:
            location_data = models.execute_kw(DB, uid, PASSWORD,
                'stock.location', 'read',
                [location_ids[0]],
                {'fields': ['name']}
            )
            logger.info(f"Found location '{location_data[0]['name']}' using ilike search for '{location_name}'")
            return location_ids[0]
        
        # Special case for 'FG50/Stock' - try to find 'Stock' or similar
        if 'FG13' in location_name or 'Stock' in location_name:
            logger.info(f"Trying to find a stock location as fallback for '{location_name}'")
            # Try to find any stock location
            stock_location_ids = models.execute_kw(DB, uid, PASSWORD,
                'stock.location', 'search',
                [[['name', '=', 'Stock'], ['usage', '=', 'internal']]]
            )
            
            if not stock_location_ids:
                # Try with ilike if exact match fails
                stock_location_ids = models.execute_kw(DB, uid, PASSWORD,
                    'stock.location', 'search',
                    [[['name', 'ilike', 'Stock'], ['usage', '=', 'internal']]],
                    {'limit': 1}
                )
            
            if stock_location_ids:
                # Get the names of found locations for logging
                stock_locations = models.execute_kw(DB, uid, PASSWORD,
                    'stock.location', 'read',
                    [stock_location_ids[0]],
                    {'fields': ['name']}
                )
                logger.warning(f"Using '{stock_locations[0]['name']}' as fallback for '{location_name}'. Please verify this is correct.")
                return stock_location_ids[0]
        
        # If no location found, list available locations to help user
        logger.error(f"Location not found: '{location_name}'")
        available_locations = models.execute_kw(DB, uid, PASSWORD,
            'stock.location', 'search_read',
            [[['usage', '=', 'internal']]],
            {'fields': ['name'], 'limit': 10}
        )
        location_names = [loc['name'] for loc in available_locations]
        logger.info(f"Available locations in Odoo (first 10): {location_names}")
        
        # Use the first available stock location as a last resort
        if available_locations:
            logger.warning(f"Using '{available_locations[0]['name']}' as a last resort for '{location_name}'")
            return available_locations[0]['id']
        
        return False
    except Exception as e:
        logger.error(f"Error finding location '{location_name}': {str(e)}")
        return False
def clean_product_name(product_name):
    if not product_name:
        return ""
        
    # Convert to string if not already
    product_name = str(product_name).strip()
    
    # Fix some common issues with Thai product names
    import re
    
    # Fix multiple spaces
    product_name = re.sub(r'\s+', ' ', product_name)
    
    # Fix incorrect parentheses
    product_name = product_name.replace('( ', '(').replace(' )', ')')
    
    # Fix some common character encoding issues in Thai text
    # Map of problematic characters to their correct versions
    char_map = {
        '�': '',  # Remove replacement character
        '\u00a0': ' ',  # Replace non-breaking space with regular space
        '\t': ' '  # Replace tabs with spaces
    }
    
    for bad_char, good_char in char_map.items():
        product_name = product_name.replace(bad_char, good_char)
    
    # Clean up square brackets format
    product_name = re.sub(r'\[\s+', '[', product_name)  # Remove space after opening bracket
    product_name = re.sub(r'\s+\]', ']', product_name)  # Remove space before closing bracket
    
    # Remove double spaces that might have been introduced
    product_name = re.sub(r'\s+', ' ', product_name)
    
    return product_name.strip()


def get_product_id(models, uid, default_code, old_product_code=None):
    try:
        # Ensure default_code is a string and strip whitespace
        default_code = clean_product_name(default_code)
        logger.info(f"Searching for product with code: '{default_code}'")

        # 1. Exact match on default_code
        product_ids = models.execute_kw(DB, uid, PASSWORD,
                                        'product.product', 'search',
                                        [[['default_code', '=', default_code]]]
                                        )
        if product_ids:
            logger.info(f"Found product with exact match on default_code: '{default_code}'")
            return product_ids[0]

        # 2. ilike match on default_code
        product_ids = models.execute_kw(DB, uid, PASSWORD,
                                        'product.product', 'search',
                                        [[['default_code', 'ilike', default_code]]]
                                        )
        if product_ids:
            product_data = models.execute_kw(DB, uid, PASSWORD,
                                             'product.product', 'read',
                                             [product_ids[0]],
                                             {'fields': ['default_code', 'name']}
                                             )
            logger.info(f"Found product '{product_data[0]['name']}' with code '{product_data[0]['default_code']}' using ilike search for '{default_code}'")
            return product_ids[0]

        # 3. Exact match on barcode
        product_ids = models.execute_kw(DB, uid, PASSWORD,
                                        'product.product', 'search',
                                        [[['barcode', '=', default_code]]]
                                        )
        if product_ids:
            logger.info(f"Found product with exact match on barcode: '{default_code}'")
            return product_ids[0]

        # 4. ilike match on name
        product_ids = models.execute_kw(DB, uid, PASSWORD,
                                        'product.product', 'search',
                                        [[['name', 'ilike', default_code]]]
                                        )
        if product_ids:
            product_data = models.execute_kw(DB, uid, PASSWORD,
                                             'product.product', 'read',
                                             [product_ids[0]],
                                             {'fields': ['default_code', 'name']}
                                             )
            logger.info(f"Found product '{product_data[0]['name']}' using ilike search for name '{default_code}'")
            return product_ids[0]

        # 5. Fallback: direct SQL search (if available)
        try:
            product_id = search_product_by_direct_sql(models, uid, default_code)
            if product_id:
                logger.info(f"Found product via direct SQL search: '{default_code}'")
                return product_id
        except Exception as e:
            logger.warning(f"Direct SQL search failed: {str(e)}")

        logger.error(f"Product not found with code, barcode, or name: '{default_code}'")
        return False
    except Exception as e:
        logger.error(f"Error finding product '{default_code}': {str(e)}")
        return False

def get_uom_id(models, uid, product_id):
    try:
        product_data = models.execute_kw(DB, uid, PASSWORD,
            'product.product', 'read',
            [product_id],
            {'fields': ['uom_id']}
        )
        return product_data[0]['uom_id'][0] if product_data else False
    except Exception as e:
        logger.error(f"Error getting UoM for product ID {product_id}: {str(e)}")
        return False

def create_internal_transfers(uid, models, df):
    successful_transfers = 0
    failed_transfers = 0

    try:
        # เรียงลำดับตาม scheduled_date และ sequence
        df = df.sort_values(by=['scheduled_date', 'sequence']).reset_index(drop=True)

        # Group ตามวันที่
        for scheduled_date, group_df in df.groupby('scheduled_date'):
            group_df = group_df.reset_index(drop=True)
            first_row = group_df.iloc[0]
            sequence = first_row.get('sequence', '')
            
            # Get source location from picking_type_id column (which now contains source location)
            source_location_name = None
            
            # First check if we have "Delivery Orders" column with valid data
            if 'Delivery Orders' in first_row and pd.notna(first_row['Delivery Orders']):
                source_location_name = str(first_row.get('Delivery Orders', '')).strip()
                logger.info(f"Using source location from 'Delivery Orders' column: '{source_location_name}'")
            # Then check picking_type_id (which may also contain data from "Delivery Orders")
            elif 'picking_type_id' in first_row and pd.notna(first_row['picking_type_id']):
                source_location_name = str(first_row.get('picking_type_id', '')).strip()
                logger.info(f"Using source location from picking_type_id column: '{source_location_name}'")
            
            # Get source location ID using the location name from the column
            source_location_id = False
            if source_location_name and source_location_name != "":
                source_location_id = get_location_id(models, uid, source_location_name)
                logger.info(f"Using source location from Excel: '{source_location_name}'")
            
            # If no valid source location was found, use default picking type's source location
            if not source_location_id:
                # Use default picking type for the operation
                picking_type_name = DEFAULT_PICKING_TYPE
                logger.info(f"No valid source location found in Excel, using default picking type: {picking_type_name}")
                
                picking_type_ids = models.execute_kw(DB, uid, PASSWORD,
                    'stock.picking.type', 'search',
                    [[['name', '=', picking_type_name]]]
                )
                if not picking_type_ids:
                    logger.error(f"Default picking type not found: {picking_type_name}")
                    continue
                
                picking_type_id = picking_type_ids[0]
                picking_type_data = models.execute_kw(DB, uid, PASSWORD,
                    'stock.picking.type', 'read',
                    [picking_type_id], {'fields': ['default_location_src_id']}
                )
                
                source_location_id = picking_type_data[0]['default_location_src_id'][0] if picking_type_data[0]['default_location_src_id'] else False
                if not source_location_id:
                    logger.error(f"Source location not found for default picking type: {picking_type_name}")
                    continue
            else:
                # We have a valid source location, now find a suitable picking type
                # For now, use the default picking type
                picking_type_name = DEFAULT_PICKING_TYPE
                picking_type_ids = models.execute_kw(DB, uid, PASSWORD,
                    'stock.picking.type', 'search',
                    [[['name', '=', picking_type_name]]]
                )
                if not picking_type_ids:
                    logger.error(f"Default picking type not found: {picking_type_name}")
                    continue
                
                picking_type_id = picking_type_ids[0]
                
            # Get destination location
            dest_location = first_row.get('location_dest_id', None)
            if pd.notna(dest_location) and str(dest_location).strip() != "":
                dest_location_id = get_location_id(models, uid, str(dest_location).strip())
            else:
                dest_location_id = False
                
            partner_id = False
            shipping_address = ""
            if 'partner_id' in first_row and pd.notna(first_row['partner_id']):
                partner_name = str(first_row['partner_id']).strip()
                partner_id = get_or_create_partner(models, uid, partner_name)
            
            # For delivery operations, get customer information and location
            customer_location_id = False
            if picking_type_name == DEFAULT_PICKING_TYPE and partner_id:
                customer_location_id, shipping_address, partner_id = get_customer_shipping_info(models, uid, partner_id)
                if customer_location_id:
                    # Override destination location with customer's location for delivery
                    dest_location_id = customer_location_id
                    logger.info(f"Using customer location as destination for delivery to {partner_name}")

            date_str = str(scheduled_date)
            
            # Build note with shipping address for deliveries
            note = f"Imported from Excel file: {os.path.basename(EXCEL_FILE)} - Date: {date_str} - Sequence: {sequence}"
            if shipping_address:
                note += f"\nShipping Address: {shipping_address}"

            picking_vals = {
                'picking_type_id': picking_type_id,
                'location_id': source_location_id,
                # ใส่ location_dest_id เฉพาะถ้ามีค่า
                **({'location_dest_id': dest_location_id} if dest_location_id else {}),
                'origin': str(first_row.get('origin', f"Import {date_str.split(' ')[0]}")).strip(),
                'note': note,
                'scheduled_date': date_str,
                'date': date_str,
                'date_deadline': date_str,
            }
            if partner_id:
                picking_vals['partner_id'] = partner_id

            context = {
                'force_date': date_str,
                'planned_date': date_str,
                'default_scheduled_date': date_str,
                'default_date': date_str,
                'tracking_disable': True,
                'mail_notrack': True,
                'mail_create_nolog': True,
                'no_recompute': True
            }

            picking_id = models.execute_kw(DB, uid, PASSWORD,
                'stock.picking', 'create',
                [picking_vals],
                {'context': context}
            )
            logger.info(f"Created transfer for date {date_str} sequence {sequence} with picking ID {picking_id}")            # วนลูปแต่ละบรรทัดใน group (รายการสินค้าซ้ำถูกรวมปริมาณแล้วในขั้นตอน read_excel_file)
            for idx, row in group_df.iterrows():
                try:
                    logger.info(f"Processing Excel row {idx+1}: {row.to_dict()}")
                    product_code = str(row['product_id']).strip() if pd.notna(row['product_id']) else None
                    if not product_code:
                        logger.warning("Skipping row with empty product_id")
                        continue

                    product_id = get_product_id(models, uid, product_code, product_code)
                    if not product_id:
                        logger.warning(f"Product not found in Odoo: {product_code}")
                        continue

                    try:
                        quantity = float(row['product_uom_qty'])
                    except (ValueError, TypeError):
                        logger.warning(f"Skipping product {product_code} with invalid quantity: {row['product_uom_qty']}")
                        continue

                    if quantity <= 0:
                        logger.warning(f"Skipping product {product_code} with invalid quantity: {quantity}")
                        continue

                    uom_id = get_uom_id(models, uid, product_id)
                    if not uom_id:
                        logger.warning(f"UoM not found for product {product_code}")
                        continue
                        
                    move_vals = {
                        'name': f"Move {product_code} | Seq:{row['sequence']} | Line:{idx+1}",
                        'product_id': product_id,
                        'product_uom_qty': quantity,
                        'product_uom': uom_id,
                        'picking_id': picking_id,
                        'location_id': source_location_id,  # Using the source_location_id we determined from the column
                        'sequence': int(row['sequence']) if 'sequence' in row and pd.notna(row['sequence']) else 10,
                        'description_picking': f"Excel Row {idx+1} | Seq:{row['sequence']} | Product:{product_code}",
                    }
                    if dest_location_id:
                        move_vals['location_dest_id'] = dest_location_id

                    if 'price_unit' in row and pd.notna(row['price_unit']):
                        try:
                            move_vals['price_unit'] = float(row['price_unit'])
                        except (ValueError, TypeError):
                            logger.warning(f"Invalid price_unit value for product {product_code}: {row['price_unit']}")

                    move_id = models.execute_kw(DB, uid, PASSWORD,
                        'stock.move', 'create',
                        [move_vals]
                    )
                    logger.info(f"Added product {product_code} with quantity {quantity} to picking {picking_id}")

                except Exception as e:
                    logger.error(f"Error processing row {idx+1}: {str(e)}")
                    failed_transfers += 1

            # Keep picking in draft state - only update dates
            try:
                models.execute_kw(DB, uid, PASSWORD, 'stock.picking', 'write',
                    [[picking_id], {
                        'scheduled_date': date_str,
                        'date': date_str,
                        'date_deadline': date_str
                    }]
                )
                logger.info(f"Updated dates for picking {picking_id} (kept in draft state): {date_str}")
            except Exception as e:
                logger.warning(f"Could not update dates for picking: {str(e)}")

            # Note: Picking is kept in draft state (not confirmed)
            logger.info(f"Created transfer {picking_id} in draft state - ready for manual review")

            successful_transfers += 1

    except Exception as e:
        logger.error(f"Error creating internal transfers: {str(e)}")
        raise

    logger.info(f"Import completed: {successful_transfers} success, {failed_transfers} failed.")

def get_customer_shipping_info(models, uid, partner_id):
    try:
        if not partner_id:
            return False, False, False
            
        partner_data = models.execute_kw(DB, uid, PASSWORD,
            'res.partner', 'read',
            [partner_id],
            {'fields': ['property_stock_customer', 'name', 'street', 'street2', 'city', 'state_id', 'zip', 'country_id']}
        )
        
        if not partner_data:
            logger.warning(f"No data found for partner ID {partner_id}")
            return False, False, partner_id
            
        # Get customer stock location (for delivery)
        customer_location_id = partner_data[0].get('property_stock_customer', False)
        if customer_location_id:
            customer_location_id = customer_location_id[0]
        else:
            # If not found on partner, get default customer location
            location_ids = models.execute_kw(DB, uid, PASSWORD,
                'stock.location', 'search',
                [[['usage', '=', 'customer']]],
                {'limit': 1}
            )
            if location_ids:
                customer_location_id = location_ids[0]
                logger.info(f"Using default customer location for partner {partner_data[0]['name']}")
            else:
                logger.warning(f"No customer location found for partner {partner_data[0]['name']}")
        
        # Format shipping address for notes
        address_parts = []
        for field in ['street', 'street2', 'city']:
            if partner_data[0].get(field):
                address_parts.append(partner_data[0][field])
        
        if partner_data[0].get('state_id'):
            state_data = models.execute_kw(DB, uid, PASSWORD,
                'res.country.state', 'read',
                [partner_data[0]['state_id'][0]],
                {'fields': ['name']}
            )
            if state_data:
                address_parts.append(state_data[0]['name'])
                
        if partner_data[0].get('zip'):
            address_parts.append(partner_data[0]['zip'])
            
        if partner_data[0].get('country_id'):
            country_data = models.execute_kw(DB, uid, PASSWORD,
                'res.country', 'read',
                [partner_data[0]['country_id'][0]],
                {'fields': ['name']}
            )
            if country_data:
                address_parts.append(country_data[0]['name'])
                
        shipping_address = ", ".join(address_parts)
        
        logger.info(f"Retrieved shipping information for partner: {partner_data[0]['name']}")
        return customer_location_id, shipping_address, partner_id
        
    except Exception as e:
        logger.error(f"Error getting customer shipping info for partner ID {partner_id}: {str(e)}")
        return False, False, partner_id
    

if __name__ == "__main__":
    try:        # You can change these parameters based on your delivery import requirements
        EXCEL_FILE = 'Data_file/FG10 Adjuestment-02-1.xlsx'
        DEFAULT_PICKING_TYPE = 'Delivery Orders'  # Use 'Delivery Orders' for delivery operations
        # Note: The 'picking_type_id' column in the Excel file is now used as the Source Location
        
        uid, models = connect_to_odoo()
        
        # Read and process the Excel file
        df = read_excel_file()
        
        # Show basic info about the DataFrame
        print("Original DataFrame rows:", len(df))
        print("DataFrame columns:", df.columns.tolist())
        print("Sample data (first 3 rows):")
        if len(df) > 0:
            print(df.head(3))
        
        # Additional check for "Delivery Orders" column
        if 'Delivery Orders' in df.columns:
            print("Found 'Delivery Orders' column. Will use it as source location for transfers.")
            # Make sure it's used for picking_type_id (source location)
            df['picking_type_id'] = df['Delivery Orders']
        
        # Check for required columns and clean data
        if 'product_id' not in df.columns or 'product_uom_qty' not in df.columns:
            logger.error("Required columns 'product_id' or 'product_uom_qty' missing from Excel file")
            exit(1)
            
        # Remove rows with missing product_id or product_uom_qty
        original_len = len(df)
        df = df.dropna(subset=['product_id', 'product_uom_qty'])
        if len(df) < original_len:
            logger.warning(f"Dropped {original_len - len(df)} rows with missing product_id or product_uom_qty")
        
        # Check if we have any data left
        if len(df) == 0:
            logger.error("No valid data found in Excel file after cleaning")
            exit(1)
            
        print("DataFrame rows after cleaning:", len(df))
        
        # Process transfers
        create_internal_transfers(uid, models, df)
    except Exception as e:
        logger.error(f"Fatal error: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())

def search_product_by_direct_sql(models, uid, search_term):
    try:
        logger.info(f"Attempting direct SQL search for product: '{search_term}'")
        # Use a direct SQL query via the Odoo execute_kw interface
        # This provides better matching for Thai characters in some cases
        sql_query = """
            SELECT id, name, default_code 
            FROM product_product pp
            JOIN product_template pt ON pp.product_tmpl_id = pt.id
            WHERE pp.active = true 
            AND (pp.default_code ILIKE %s 
                OR pt.name ILIKE %s
                OR pp.barcode = %s)
            LIMIT 5
        """
        
        # Extract any product code patterns
        import re
        code_pattern = re.search(r'([A-Z0-9]{2,}[0-9]{2,}[A-Z0-9]*)', search_term)
        code_param = f"%{code_pattern.group(1)}%" if code_pattern else f"%{search_term}%"
        
        # Execute the SQL query
        results = models.execute_kw(DB, uid, PASSWORD, 
                                   'product.product', 'execute_sql', 
                                   [sql_query, [code_param, f"%{search_term}%", search_term]])
        
        if results and len(results) > 0:
            product_id = results[0][0]
            product_name = results[0][1]
            product_code = results[0][2] or "N/A"
            logger.info(f"Found product via direct SQL: ID={product_id}, '{product_name}' with code '{product_code}'")
            return product_id
            
        return False
    except Exception as e:
        logger.warning(f"Direct SQL search failed: {str(e)}")
        return False
