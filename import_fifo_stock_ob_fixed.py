#!/usr/bin/env python3
import xmlrpc.client
import pandas as pd
import logging
from datetime import datetime
import os

# Odoo connection parameters
HOST = 'http://mogdev.work:8069'
DB = 'MOG_LIVE3'
USERNAME = 'apichart@mogen.co.th'
PASSWORD = '471109538'

# Excel file path
<<<<<<< HEAD
EXCEL_FILE = 'Data_file/SCG.xlsx'
=======
EXCEL_FILE = 'Data_file/บริการเทคนิค1.xlsx'
>>>>>>> fb906cb (ok)

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
    """Establish connection to Odoo server"""
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
    """Read the Excel file and return a DataFrame with validation and data filling"""
    try:
        if not os.path.exists(EXCEL_FILE):
            raise FileNotFoundError(f"Excel file not found: {EXCEL_FILE}")
        
        # Read Excel file without parsing dates - we'll handle date parsing manually
        df = pd.read_excel(EXCEL_FILE, parse_dates=False)
        logger.info(f"Successfully read Excel file with {len(df)} rows")
        logger.info(f"Excel columns: {list(df.columns)}")

        # Handle sequence column if present
        if 'sequence' in df.columns:
            df.loc[:, 'sequence'] = pd.to_numeric(df['sequence'], errors='coerce')
            df.loc[df['sequence'].isna(), 'sequence'] = 0
            logger.info("Processed sequence column")
        
        # Convert date columns to datetime explicitly
        if 'scheduled_date' in df.columns:
            # Convert to datetime and handle errors
            df['scheduled_date'] = pd.to_datetime(df['scheduled_date'], errors='coerce')
            logger.info(f"scheduled_date dtype: {df['scheduled_date'].dtype}")
            logger.info(f"scheduled_date sample: {df['scheduled_date'].iloc[0] if len(df) > 0 else None}")
            
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
        
        # Fix the picking type name in the DataFrame
        if 'picking_type_id' in df.columns:
            df['picking_type_id'] = df['picking_type_id'].replace('My Company: OB FIFO', 'OB FIFO')
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
        
        # Make sure location_dest_id and picking_type_id columns exist
        if 'location_dest_id' not in df.columns:
            df['location_dest_id'] = 'Stock'  # Default value
            logger.info("Added missing 'location_dest_id' column with default value 'Stock'")
        
        if 'picking_type_id' not in df.columns:
            df['picking_type_id'] = 'OB FIFO'  # Default value
            logger.info("Added missing 'picking_type_id' column with default value 'OB FIFO'")
        
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
        for group_id, group_df in df.groupby('transfer_group'):
            # Find first non-null values for location_dest_id and picking_type_id in this group
            first_location = group_df['location_dest_id'].dropna().iloc[0] if not group_df['location_dest_id'].isna().all() else 'Stock'
            first_picking_type = group_df['picking_type_id'].dropna().iloc[0] if not group_df['picking_type_id'].isna().all() else 'OB FIFO'
            
            # Fill missing location_dest_id values in this group
            group_indices = group_df.index[group_df['location_dest_id'].isna()]
            if len(group_indices) > 0:
                logger.info(f"Filling missing location_dest_id with '{first_location}' for {len(group_indices)} rows in group {group_id}")
                df.loc[group_indices, 'location_dest_id'] = first_location
            
            # Fill missing picking_type_id values in this group
            group_indices = group_df.index[group_df['picking_type_id'].isna()]
            if len(group_indices) > 0:
                logger.info(f"Filling missing picking_type_id with '{first_picking_type}' for {len(group_indices)} rows in group {group_id}")
                df.loc[group_indices, 'picking_type_id'] = first_picking_type
        
        # Remove the temporary columns
        if 'transfer_group' in df.columns:
            df = df.drop(columns=['transfer_group'])
        if 'group_change' in df.columns:
            df = df.drop(columns=['group_change'])
        
        logger.info("Excel file validation and data filling completed successfully")
        return df
    except Exception as e:
        logger.error(f"Failed to read Excel file: {str(e)}")
        raise

def get_or_create_partner(models, uid, partner_name):
    """Get partner ID by name or create it if not found"""
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
    """Get location ID by name - improved version"""
    try:
        # Ensure location_name is a string and strip whitespace
        location_name = str(location_name).strip()
        logger.info(f"Searching for location: '{location_name}'")
        
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
def get_product_id(models, uid, default_code, old_product_code=None):
    """Get product ID by default_code or old_product_code using ilike search"""
    try:
        # Ensure default_code and old_product_code are strings and strip whitespace
        default_code = str(default_code).strip()
        if old_product_code:
            old_product_code = str(old_product_code).strip()
        logger.info(f"Searching for product with code: '{default_code}' and old code: '{old_product_code}'")

        # First try exact match on default_code
        product_ids = models.execute_kw(DB, uid, PASSWORD,
                                        'product.product', 'search',
                                        [[['default_code', '=', default_code]]]
                                        )
        if product_ids:
            logger.info(f"Found product with exact match on default_code: '{default_code}'")
            return product_ids[0]

        # Try with ilike search if exact match fails
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

        # If still not found, try searching by old_product_code (exact and ilike)
        if old_product_code:
            # Exact match
            product_ids = models.execute_kw(DB, uid, PASSWORD,
                                            'product.product', 'search',
                                            [[['old_product_code', '=', old_product_code]]]
                                            )
            if product_ids:
                logger.info(f"Found product with exact match on old_product_code: '{old_product_code}'")
                return product_ids[0]

            # ilike match
            product_ids = models.execute_kw(DB, uid, PASSWORD,
                                            'product.product', 'search',
                                            [[['old_product_code', 'ilike', old_product_code]]]
                                            )
            if product_ids:
                product_data = models.execute_kw(DB, uid, PASSWORD,
                                                 'product.product', 'read',
                                                 [product_ids[0]],
                                                 {'fields': ['default_code', 'name']}
                                                 )
                logger.info(f"Found product '{product_data[0]['name']}' with code '{product_data[0]['default_code']}' using ilike search for old_product_code '{old_product_code}'")
                return product_ids[0]

        # If still not found, try searching by name
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
            logger.info(f"Found product by name: '{product_data[0]['name']}' with code '{product_data[0]['default_code']}' for search term '{default_code}'")
            return product_ids[0]

        logger.error(f"Product not found with code or name: '{default_code}' or old code: '{old_product_code}'")
        return False
    except Exception as e:
        logger.error(f"Error finding product '{default_code}' or old code '{old_product_code}': {str(e)}")
        return False

def get_uom_id(models, uid, product_id):
    """Get UoM ID for a product"""
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

            # เตรียมข้อมูล picking ตาม first_row
            picking_type_name = str(first_row.get('picking_type_id', 'OB FIFO')).strip()
            picking_type_ids = models.execute_kw(DB, uid, PASSWORD,
                'stock.picking.type', 'search',
                [[['name', '=', picking_type_name]]]
            )
            if not picking_type_ids:
                logger.error(f"Picking type not found: {picking_type_name}")
                continue
            picking_type_id = picking_type_ids[0]

            picking_type_data = models.execute_kw(DB, uid, PASSWORD,
                'stock.picking.type', 'read',
                [picking_type_id],
                {'fields': ['default_location_src_id']}
            )
            source_location_id = picking_type_data[0]['default_location_src_id'][0] if picking_type_data[0]['default_location_src_id'] else False
            if not source_location_id:
                logger.error(f"Source location not found for picking type: {picking_type_name}")
                continue

            dest_location = str(first_row.get('location_dest_id', 'Stock')).strip()
            dest_location_id = get_location_id(models, uid, dest_location)
            if not dest_location_id:
                logger.error(f"Destination location not found: {dest_location}")
                continue

            partner_id = False
            if 'partner_id' in first_row and pd.notna(first_row['partner_id']):
                partner_name = str(first_row['partner_id']).strip()
                partner_id = get_or_create_partner(models, uid, partner_name)

            date_str = str(scheduled_date)

            picking_vals = {
                'picking_type_id': picking_type_id,
                'location_id': source_location_id,
                'location_dest_id': dest_location_id,
                'origin': str(first_row.get('origin', f"Import {date_str.split(' ')[0]}")).strip(),
                'note': f"Imported from Excel file: {os.path.basename(EXCEL_FILE)} - Date: {date_str} - Sequence: {sequence}",
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
            logger.info(f"Created transfer for date {date_str} sequence {sequence} with picking ID {picking_id}")

            # วนลูปแต่ละบรรทัดใน group (แม้รหัสซ้ำก็สร้าง move ใหม่ทุกบรรทัด)
            for idx, row in group_df.iterrows():
                try:
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
                        'location_id': source_location_id,
                        'location_dest_id': dest_location_id,
                        'sequence': int(row['sequence']) if 'sequence' in row and pd.notna(row['sequence']) else 10,
                        'description_picking': f"Excel Row {idx+1} | Seq:{row['sequence']} | Product:{product_code}",
                        # 'description': f"Excel Row {idx+1} | Seq:{row['sequence']} | Product:{product_code}",
                    }
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

            # Confirm picking หลังจากเพิ่ม move ครบ
            try:
                models.execute_kw(DB, uid, PASSWORD, 'stock.picking', 'write',
                    [[picking_id], {
                        'scheduled_date': date_str,
                        'date': date_str,
                        'date_deadline': date_str
                    }]
                )
                logger.info(f"Re-updated dates before confirming picking {picking_id}: {date_str}")
            except Exception as e:
                logger.warning(f"Could not update dates before confirming picking: {str(e)}")

            try:
                models.execute_kw(DB, uid, PASSWORD, 'stock.picking', 'action_confirm', [[picking_id]], {
                    'context': {
                        'force_date': date_str,
                        'planned_date': date_str,
                        'default_scheduled_date': date_str,
                        'default_date': date_str,
                    }
<<<<<<< HEAD
                })
                logger.info(f"Confirmed transfer {picking_id} with forced date context")
            except Exception as e:
                logger.error(f"Failed to confirm transfer {picking_id}: {str(e)}")

            successful_transfers += 1
=======
                    
                    # Add date_done if available
                    if 'date_done_orig' in first_row and pd.notna(first_row['date_done_orig']):
                        # Use the original datetime object
                        date_done_obj = first_row['date_done_orig']
                        if isinstance(date_done_obj, pd.Timestamp):
                            picking_vals['date_done'] = date_done_obj.strftime('%Y-%m-%d %H:%M:%S')
                        else:
                            picking_vals['date_done'] = str(first_row['date_done'])
                        logger.info(f"Setting date_done to {picking_vals['date_done']} from Excel (original datetime)")
                    elif 'date_done' in first_row and pd.notna(first_row['date_done']):
                        picking_vals['date_done'] = str(first_row['date_done'])
                        logger.info(f"Setting date_done to {picking_vals['date_done']} from Excel (string version)")
                    
                    # Add partner_id if available
                    if partner_id:
                        picking_vals['partner_id'] = partner_id
                    
                    # Create the picking with context to force the date
                    picking_id = models.execute_kw(DB, uid, PASSWORD,
                        'stock.picking', 'create',
                        [picking_vals],
                        {'context': context}
                    )
                    logger.info(f"Created transfer document for date {date_group} to location {dest_location} with ID {picking_id}")
                    
                    # Check the actual date that was saved
                    picking_data_before = models.execute_kw(DB, uid, PASSWORD,
                        'stock.picking', 'read',
                        [picking_id],
                        {'fields': ['scheduled_date', 'date', 'date_deadline']}
                    )
                    logger.info(f"Initial dates after creation: {picking_data_before[0]}")
                    
                    # Try a direct database update approach for Odoo 17
                    try:
                        # Try to update the picking directly using the ORM with a special context
                        bypass_context = {
                            'bypass_date_validation': True, 
                            'tracking_disable': True,
                            'mail_notrack': True,
                            'mail_create_nolog': True,
                            'no_recompute': True,
                            'force_period_date': date_str.split(' ')[0]  # Force period date (Odoo specific)
                        }
                        
                        # Try to update using write method with bypass context
                        models.execute_kw(DB, uid, PASSWORD,
                            'stock.picking', 'write',
                            [[picking_id], {
                                'scheduled_date': date_str,
                                'date': date_str,
                                'date_deadline': date_str
                            }],
                            {'context': bypass_context}
                        )
                        logger.info(f"Updated dates using write method with bypass context: {date_str}")
                    except Exception as e:
                        logger.warning(f"Could not update dates using ORM with bypass context: {str(e)}")
                    
                    # Try standard write method as fallback
                    try:
                        models.execute_kw(DB, uid, PASSWORD,
                            'stock.picking', 'write',
                            [[picking_id], {
                                'scheduled_date': date_str,
                                'date': date_str,
                                'date_deadline': date_str
                            }]
                        )
                        logger.info(f"Updated dates using standard write method: {date_str}")
                    except Exception as e2:
                        logger.error(f"Failed to update dates using all methods: {str(e2)}")
                    
                    # Try one more approach - update the picking before adding moves
                    try:
                        # Try to update the picking state to force date recalculation
                        models.execute_kw(DB, uid, PASSWORD,
                            'stock.picking', 'write',
                            [[picking_id], {'state': 'draft'}],
                            {'context': {'force_period_date': date_str.split(' ')[0]}}
                        )
                        logger.info("Set picking state to draft to force date recalculation")
                    except Exception as state_error:
                        logger.warning(f"Could not update picking state: {str(state_error)}")
                    
                    # Verify the update was successful
                    picking_data_after = models.execute_kw(DB, uid, PASSWORD,
                        'stock.picking', 'read',
                        [picking_id],
                        {'fields': ['scheduled_date', 'date', 'date_deadline', 'state']}
                    )
                    logger.info(f"Final dates after updates: {picking_data_after[0]}")
                    
                    # Process each product in this group
                    move_ids = []
                    for _, row in location_df.iterrows():
                        try:
                            # Get product information
                            product_code = str(row['product_id']).strip()
                            if not product_code:
                                logger.warning(f"Skipping row with empty product code")
                                continue
                            
                            try:
                                quantity = float(row['product_uom_qty'])
                            except (ValueError, TypeError):
                                logger.warning(f"Skipping product {product_code} with invalid quantity: {row['product_uom_qty']}")
                                continue
                            
                            # Skip if quantity is zero or negative
                            if quantity <= 0:
                                logger.warning(f"Skipping product {product_code} with invalid quantity: {quantity}")
                                continue
                            
                            # Get product ID
                            product_id = get_product_id(models, uid, product_code)
                            if not product_id:
                                logger.warning(f"Product not found: {product_code}")
                                continue
                            
                            # Get UoM ID
                            uom_id = get_uom_id(models, uid, product_id)
                            if not uom_id:
                                logger.warning(f"UoM not found for product {product_code}")
                                continue
                            
                            # Create stock move
                            move_vals = {
                                'name': f"Move {product_code}",
                                'product_id': product_id,
                                'product_uom_qty': quantity,
                                'product_uom': uom_id,
                                'picking_id': picking_id,
                                'location_id': source_location_id,
                                'location_dest_id': dest_location_id,
                            }

                            # Add sequence if available
                            if 'sequence' in row and pd.notna(row['sequence']):
                                move_vals['sequence'] = int(row['sequence'])
                            
                            # Add price_unit if available
                            if 'price_unit' in row and pd.notna(row['price_unit']):
                                try:
                                    move_vals['price_unit'] = float(row['price_unit'])
                                except (ValueError, TypeError):
                                    logger.warning(f"Invalid price_unit value for product {product_code}: {row['price_unit']}")
                            
                            move_id = models.execute_kw(DB, uid, PASSWORD,
                                'stock.move', 'create',
                                [move_vals]
                            )
                            move_ids.append(move_id)
                            logger.info(f"Added product {product_code} with quantity {quantity} to transfer {picking_id}")
                            successful_transfers += 1
                        except Exception as e:
                            logger.error(f"Error processing product {row.get('product_id', 'unknown')}: {str(e)}")
                            failed_transfers += 1
                    
                    # Confirm the transfer if we have at least one move
                    if move_ids:
                        # Before confirming, try to set the date one more time
                        try:
                            models.execute_kw(DB, uid, PASSWORD,
                                'stock.picking', 'write',
                                [[picking_id], {
                                    'scheduled_date': date_str,
                                    'date': date_str,
                                    'date_deadline': date_str
                                }],
                                {'context': {'force_period_date': date_str.split(' ')[0]}}
                            )
                            logger.info(f"Final date update before confirmation: {date_str}")
                        except Exception as final_update_error:
                            logger.warning(f"Final date update failed: {str(final_update_error)}")
                        
                        # Now confirm the transfer
                        models.execute_kw(DB, uid, PASSWORD,
                            'stock.picking', 'action_confirm',
                            [[picking_id]]
                        )
                        logger.info(f"Confirmed transfer {picking_id} with {len(move_ids)} products")
                        
                        # Check the dates after confirmation
                        picking_data_confirmed = models.execute_kw(DB, uid, PASSWORD,
                            'stock.picking', 'read',
                            [picking_id],
                            {'fields': ['scheduled_date', 'date', 'date_deadline', 'state']}
                        )
                        logger.info(f"Dates after confirmation: {picking_data_confirmed[0]}")
                    else:
                        # If no moves were created, delete the empty picking
                        models.execute_kw(DB, uid, PASSWORD,
                            'stock.picking', 'unlink',
                            [[picking_id]]
                        )
                        logger.warning(f"Deleted empty transfer {picking_id} as no valid products were found")
                
                except Exception as e:
                    logger.error(f"Error processing location group {dest_location} for date {date_group}: {str(e)}")
                    failed_transfers += len(location_df)
        
        return successful_transfers, failed_transfers
    
    except Exception as e:
        logger.error(f"Error in create_internal_transfers: {str(e)}")
        return successful_transfers, failed_transfers
>>>>>>> fb906cb (ok)

    except Exception as e:
        logger.error(f"Error creating internal transfers: {str(e)}")
        raise

    logger.info(f"Import completed: {successful_transfers} success, {failed_transfers} failed.")

if __name__ == "__main__":
    try:
        EXCEL_FILE = 'Data_file/SCG.xlsx'
        uid, models = connect_to_odoo()
        df = read_excel_file()
        create_internal_transfers(uid, models, df)
    except Exception as e:
        logger.error(f"Fatal error: {str(e)}")
