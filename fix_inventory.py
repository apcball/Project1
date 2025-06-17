#!/usr/bin/env python3
import xmlrpc.client
import pandas as pd
import logging
from datetime import datetime
import os
import argparse
import sys
import socket

# Set up command line arguments
parser = argparse.ArgumentParser(description='Fix inventory quantities and prices in Odoo')
parser.add_argument('--dry-run', action='store_true', help='Run in dry-run mode (no changes applied)')
parser.add_argument('--location', help='Specific location name or ID to use')
parser.add_argument('--timeout', type=int, default=120, help='XML-RPC timeout in seconds')
parser.add_argument('--excel', help='Path to Excel file')
args = parser.parse_args()

# Odoo connection parameters
HOST = 'http://mogdev.work:8069'
DB = 'MOG_LIVE3'
USERNAME = 'apichart@mogen.co.th'
PASSWORD = '471109538'

# Excel file path
EXCEL_FILE = args.excel if args.excel else 'Data_file/สิ้นเปลืองโรงงาน.xlsx'

# Configure logging
log_filename = f'fix_inventory_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'
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
    """Establish connection to Odoo server with timeout handling"""
    try:
        # Set socket timeout
        socket.setdefaulttimeout(args.timeout)
        
        # Connect to common endpoint
        common = xmlrpc.client.ServerProxy(f'{HOST}/xmlrpc/2/common')
        uid = common.authenticate(DB, USERNAME, PASSWORD, {})
        if not uid:
            raise Exception("Authentication failed")
        
        # Connect to object endpoint
        models = xmlrpc.client.ServerProxy(f'{HOST}/xmlrpc/2/object')
        
        # Test connection with a simple call
        version_info = common.version()
        odoo_version = version_info.get('server_version', 'Unknown')
        logger.info(f"Successfully connected to Odoo version {odoo_version} as user ID: {uid}")
        
        return uid, models, odoo_version
    except xmlrpc.client.Fault as e:
        logger.error(f"Odoo server error: {str(e)}")
        raise
    except socket.timeout:
        logger.error(f"Connection timeout. Consider increasing timeout (current: {args.timeout}s)")
        raise
    except Exception as e:
        logger.error(f"Failed to connect to Odoo: {str(e)}")
        raise

def read_excel_file():
    """Read the Excel file and return a DataFrame"""
    try:
        if not os.path.exists(EXCEL_FILE):
            raise FileNotFoundError(f"Excel file not found: {EXCEL_FILE}")
        
        # Read Excel file
        df = pd.read_excel(EXCEL_FILE)
        logger.info(f"Successfully read Excel file with {len(df)} rows")
        logger.info(f"Excel columns: {list(df.columns)}")
        
        return df
    except Exception as e:
        logger.error(f"Failed to read Excel file: {str(e)}")
        raise

def get_product_id(models, uid, default_code):
    """Get product ID by default_code or old_product_code with enhanced search capability"""
    try:
        default_code = str(default_code).strip()
        logger.info(f"Searching for product with code: '{default_code}'")

        # 1. Try exact match by default_code first
        product_ids = models.execute_kw(DB, uid, PASSWORD, 'product.product', 'search',
            [[('default_code', '=', default_code)]],
            {'limit': 1}
        )
        if product_ids:
            product_data = models.execute_kw(DB, uid, PASSWORD, 'product.product', 'read',
                [product_ids[0]], {'fields': ['name']}
            )
            logger.info(f"Found product with exact match: {product_data[0]['name']}")
            return product_ids[0]

        # 2. Try case-insensitive match on default_code
        product_ids = models.execute_kw(DB, uid, PASSWORD, 'product.product', 'search',
            [[('default_code', 'ilike', default_code)]],
            {'limit': 5}
        )
        if product_ids:
            product_data = models.execute_kw(DB, uid, PASSWORD, 'product.product', 'read',
                [product_ids[0]], {'fields': ['name', 'default_code']}
            )
            logger.info(f"Found product with similar code: {product_data[0]['name']} (code: {product_data[0].get('default_code', 'N/A')})")
            return product_ids[0]
        
        # 3. Try exact match on old_product_code
        logger.info(f"Searching for product with old_product_code: '{default_code}'")
        try:
            # Check if old_product_code field exists
            fields = models.execute_kw(DB, uid, PASSWORD, 'product.product', 'fields_get', [])
            if 'old_product_code' in fields:
                product_ids = models.execute_kw(DB, uid, PASSWORD, 'product.product', 'search',
                    [[('old_product_code', '=', default_code)]],
                    {'limit': 1}
                )
                if product_ids:
                    product_data = models.execute_kw(DB, uid, PASSWORD, 'product.product', 'read',
                        [product_ids[0]], {'fields': ['name', 'default_code', 'old_product_code']}
                    )
                    logger.info(f"Found product using old_product_code: {product_data[0]['name']} " + 
                                f"(code: {product_data[0].get('default_code', 'N/A')}, old code: {product_data[0].get('old_product_code', 'N/A')})")
                    return product_ids[0]
                
                # 4. Try case-insensitive match on old_product_code
                product_ids = models.execute_kw(DB, uid, PASSWORD, 'product.product', 'search',
                    [[('old_product_code', 'ilike', default_code)]],
                    {'limit': 1}
                )
                if product_ids:
                    product_data = models.execute_kw(DB, uid, PASSWORD, 'product.product', 'read',
                        [product_ids[0]], {'fields': ['name', 'default_code', 'old_product_code']}
                    )
                    logger.info(f"Found product with similar old_product_code: {product_data[0]['name']} " + 
                                f"(code: {product_data[0].get('default_code', 'N/A')}, old code: {product_data[0].get('old_product_code', 'N/A')})")
                    return product_ids[0]
            else:
                logger.debug("old_product_code field not available in product.product model")
        except Exception as e_old:
            logger.warning(f"Error while searching by old_product_code: {str(e_old)}")
        
        # 5. If code starts with INSTALL_, try without the prefix
        if default_code.startswith('INSTALL_'):
            alt_code = default_code.replace('INSTALL_', '')
            product_ids = models.execute_kw(DB, uid, PASSWORD, 'product.product', 'search',
                [[('default_code', 'ilike', alt_code)]],
                {'limit': 1}
            )
            if product_ids:
                product_data = models.execute_kw(DB, uid, PASSWORD, 'product.product', 'read',
                    [product_ids[0]], {'fields': ['name']}
                )
                logger.info(f"Found product without INSTALL_ prefix: {product_data[0]['name']}")
                return product_ids[0]

        # 6. Try name search as a last resort
        product_ids = models.execute_kw(DB, uid, PASSWORD, 'product.product', 'search',
            [[('name', 'ilike', default_code)]],
            {'limit': 1}
        )
        if product_ids:
            product_data = models.execute_kw(DB, uid, PASSWORD, 'product.product', 'read',
                [product_ids[0]], {'fields': ['name']}
            )
            logger.info(f"Found product by name: {product_data[0]['name']}")
            return product_ids[0]

        logger.warning(f"Product not found: '{default_code}'")
        return False
        
    except Exception as e:
        logger.error(f"Error finding product '{default_code}': {str(e)}")
        return False

def get_stock_location(models, uid):
    """Find an appropriate stock location based on command line args or defaults"""
    try:
        logger.info("Searching for stock location")
        
        # If location is specified by ID (numeric)
        if args.location and args.location.isdigit():
            location_id = int(args.location)
            location_data = models.execute_kw(DB, uid, PASSWORD, 'stock.location', 'read',
                [location_id], {'fields': ['name', 'usage']}
            )
            if location_data and location_data[0]['usage'] == 'internal':
                logger.info(f"Using specified location: {location_data[0]['name']} (ID: {location_id})")
                return location_id
            else:
                logger.warning(f"Specified location ID {location_id} is not valid or not internal")
        
        # If location is specified by name
        elif args.location:
            location_ids = models.execute_kw(DB, uid, PASSWORD, 'stock.location', 'search',
                [[('name', '=', args.location), ('usage', '=', 'internal')]],
                {'limit': 1}
            )
            if location_ids:
                location_data = models.execute_kw(DB, uid, PASSWORD, 'stock.location', 'read',
                    [location_ids[0]], {'fields': ['name']}
                )
                logger.info(f"Using specified location by name: {location_data[0]['name']} (ID: {location_ids[0]})")
                return location_ids[0]
            else:
                logger.warning(f"Specified location name '{args.location}' not found or not internal")
        
        # Find default location - first try Stock location
        location_ids = models.execute_kw(DB, uid, PASSWORD, 'stock.location', 'search',
            [[('name', '=', 'Stock'), ('usage', '=', 'internal')]],
            {'limit': 1}
        )
        
        # If not found, try WH/Stock
        if not location_ids:
            location_ids = models.execute_kw(DB, uid, PASSWORD, 'stock.location', 'search',
                [[('name', '=', 'WH/Stock'), ('usage', '=', 'internal')]],
                {'limit': 1}
            )
        
        # If still not found, get any internal location
        if not location_ids:
            location_ids = models.execute_kw(DB, uid, PASSWORD, 'stock.location', 'search',
                [[('usage', '=', 'internal')]],
                {'limit': 1}
            )
        
        if location_ids:
            location_data = models.execute_kw(DB, uid, PASSWORD, 'stock.location', 'read',
                [location_ids[0]], {'fields': ['name']}
            )
            logger.info(f"Using default location: {location_data[0]['name']} (ID: {location_ids[0]})")
            return location_ids[0]
        
        logger.error("No internal stock location found")
        return False
        
    except Exception as e:
        logger.error(f"Error finding stock location: {str(e)}")
        return False

def check_product_in_location(models, uid, product_id, location_id):
    """Check if product exists in inventory at the given location"""
    try:
        quant_ids = models.execute_kw(DB, uid, PASSWORD, 'stock.quant', 'search',
            [[('product_id', '=', product_id), ('location_id', '=', location_id)]],
            {'limit': 1}
        )
        
        if quant_ids:
            quant_data = models.execute_kw(DB, uid, PASSWORD, 'stock.quant', 'read',
                [quant_ids[0]], {'fields': ['quantity']}
            )
            current_qty = quant_data[0]['quantity']
            return True, quant_ids[0], current_qty
        
        return False, False, 0
        
    except Exception as e:
        logger.error(f"Error checking product in location: {str(e)}")
        return False, False, 0

def fix_inventory_prices():
    """Fix inventory valuation by updating product standard prices"""
    try:
        uid, models = connect_to_odoo()
        df = read_excel_file()
        
        # Calculate total valuation from Excel
        if 'price_unit' in df.columns and 'product_uom_qty' in df.columns:
            df['total_value'] = df['price_unit'] * df['product_uom_qty']
            total_excel_value = df['total_value'].sum()
            logger.info(f"Total Excel valuation: {total_excel_value}")
        else:
            logger.error("Excel file missing required columns")
            return
        
        # Group by product_id to consolidate quantities and get average price
        grouped_df = df.groupby('product_id').agg({
            'product_uom_qty': 'sum',
            'price_unit': 'mean',
            'total_value': 'sum'
        }).reset_index()
        
        logger.info(f"Found {len(grouped_df)} unique products to update")
        
        # Check Odoo version
        ir_model_ids = models.execute_kw(DB, uid, PASSWORD, 'ir.model', 'search',
            [[('model', '=', 'stock.change.product.qty')]],
            {'limit': 1}
        )
        has_change_qty_wizard = len(ir_model_ids) > 0
        
        if has_change_qty_wizard:
            logger.info("Using stock.change.product.qty wizard for inventory updates")
        else:
            logger.warning("stock.change.product.qty wizard not found - will try alternative methods")
        
        # Process each product
        success_count = 0
        failed_count = 0
        
        for idx, row in grouped_df.iterrows():
            try:
                product_code = str(row['product_id']).strip()
                quantity = float(row['product_uom_qty'])
                price_unit = float(row['price_unit']) if pd.notna(row['price_unit']) else 0.0
                
                # Find product in Odoo
                product_id = get_product_id(models, uid, product_code)
                if not product_id:
                    logger.error(f"Cannot update product - not found: {product_code}")
                    failed_count += 1
                    continue
                
                # 1. Update standard_price
                if price_unit > 0:
                    logger.info(f"Updating standard_price for {product_code} to {price_unit}")
                    models.execute_kw(DB, uid, PASSWORD, 'product.product', 'write',
                        [product_id, {'standard_price': price_unit}]
                    )
                
                # 2. Update quantity using wizard
                if has_change_qty_wizard:
                    try:
                        # Get product template ID
                        product_data = models.execute_kw(DB, uid, PASSWORD, 'product.product', 'read',
                            [product_id], {'fields': ['product_tmpl_id']}
                        )
                        product_tmpl_id = product_data[0]['product_tmpl_id'][0]
                        
                        # Find a stock location
                        location_id = get_stock_location(models, uid)
                        if not location_id:
                            logger.warning(f"No valid stock location found for {product_code}")
                            failed_count += 1
                            continue
                        
                        # Create wizard
                        wizard_vals = {
                            'product_id': product_id,
                            'product_tmpl_id': product_tmpl_id,
                            'new_quantity': quantity
                        }
                        
                        # Check if location_id is needed
                        wizard_fields = models.execute_kw(DB, uid, PASSWORD, 'stock.change.product.qty', 'fields_get', [])
                        if 'location_id' in wizard_fields:
                            wizard_vals['location_id'] = location_id
                        
                        wizard_id = models.execute_kw(DB, uid, PASSWORD, 
                            'stock.change.product.qty', 'create', [wizard_vals]
                        )
                        
                        # Execute wizard
                        result = models.execute_kw(DB, uid, PASSWORD,
                            'stock.change.product.qty', 'change_product_qty', [[wizard_id]]
                        )
                        
                        logger.info(f"Updated quantity for {product_code} to {quantity}")
                        success_count += 1
                        
                    except Exception as e:
                        logger.error(f"Failed to update quantity for {product_code}: {str(e)}")
                        failed_count += 1
                
                else:
                    # Alternative: Try using stock.quant directly
                    try:
                        # Find an internal location
                        location_ids = models.execute_kw(DB, uid, PASSWORD, 'stock.location', 'search',
                            [[('usage', '=', 'internal')]],
                            {'limit': 1}
                        )
                        location_id = location_ids[0] if location_ids else False
                        
                        if not location_id:
                            logger.warning(f"No internal location found for {product_code}")
                            continue
                            
                        # Find existing quants
                        quant_ids = models.execute_kw(DB, uid, PASSWORD, 'stock.quant', 'search',
                            [[('product_id', '=', product_id), ('location_id', '=', location_id)]]
                        )
                        
                        if quant_ids:
                            # Update existing quant
                            quant_data = models.execute_kw(DB, uid, PASSWORD, 'stock.quant', 'read',
                                [quant_ids[0]], {'fields': ['quantity']}
                            )
                            current_qty = quant_data[0]['quantity']
                            
                            # Use inventory_quantity and inventory_diff_quantity if available
                            quant_fields = models.execute_kw(DB, uid, PASSWORD, 'stock.quant', 'fields_get', [])
                            
                            update_vals = {}
                            if 'inventory_quantity' in quant_fields:
                                update_vals['inventory_quantity'] = quantity
                            if 'inventory_diff_quantity' in quant_fields:
                                update_vals['inventory_diff_quantity'] = quantity - current_qty
                                
                            if update_vals:
                                models.execute_kw(DB, uid, PASSWORD, 'stock.quant', 'write',
                                    [quant_ids[0], update_vals]
                                )
                                logger.info(f"Updated quant for {product_code}: {update_vals}")
                            else:
                                # Direct quantity update (less recommended)
                                models.execute_kw(DB, uid, PASSWORD, 'stock.quant', 'write',
                                    [quant_ids[0], {'quantity': quantity}]
                                )
                                logger.info(f"Updated quant quantity directly for {product_code} to {quantity}")
                                
                            success_count += 1
                            
                        else:
                            # Create new quant
                            quant_vals = {
                                'product_id': product_id,
                                'location_id': location_id,
                                'quantity': quantity
                            }
                            
                            # Add inventory fields if available
                            quant_fields = models.execute_kw(DB, uid, PASSWORD, 'stock.quant', 'fields_get', [])
                            if 'inventory_quantity' in quant_fields:
                                quant_vals['inventory_quantity'] = quantity
                            if 'inventory_diff_quantity' in quant_fields:
                                quant_vals['inventory_diff_quantity'] = quantity
                            
                            new_quant_id = models.execute_kw(DB, uid, PASSWORD, 'stock.quant', 'create',
                                [quant_vals]
                            )
                            logger.info(f"Created new quant for {product_code} with quantity {quantity}")
                            success_count += 1
                    
                    except Exception as e:
                        logger.error(f"Failed to update quant for {product_code}: {str(e)}")
                        failed_count += 1
                
            except Exception as e:
                logger.error(f"Error processing product {row['product_id']}: {str(e)}")
                failed_count += 1
        
        # Summary
        logger.info("=== Summary ===")
        logger.info(f"Total products processed: {len(grouped_df)}")
        logger.info(f"Successful updates: {success_count}")
        logger.info(f"Failed updates: {failed_count}")
        logger.info(f"Total Excel valuation: {total_excel_value}")
        
        # Check final valuation in Odoo
        try:
            # Try to get valuation from stock.valuation.layer
            valuation_data = models.execute_kw(DB, uid, PASSWORD, 'stock.valuation.layer', 'search_read',
                [[]], {'fields': ['value']}
            )
            
            if valuation_data:
                total_odoo_value = sum(layer['value'] for layer in valuation_data)
                logger.info(f"Total Odoo valuation from layers: {total_odoo_value}")
                logger.info(f"Difference: {total_odoo_value - total_excel_value}")
        except Exception as e:
            logger.warning(f"Could not get final valuation: {str(e)}")
            
            # Alternative: calculate from products
            try:
                products = models.execute_kw(DB, uid, PASSWORD, 'product.product', 'search_read',
                    [[('type', '=', 'product')]], 
                    {'fields': ['qty_available', 'standard_price']}
                )
                
                if products:
                    estimated_value = sum(p['qty_available'] * p['standard_price'] for p in products)
                    logger.info(f"Estimated Odoo valuation from standard_price: {estimated_value}")
                    logger.info(f"Difference: {estimated_value - total_excel_value}")
            except Exception as e2:
                logger.error(f"Could not calculate estimated valuation: {str(e2)}")
                
    except Exception as e:
        logger.error(f"Fatal error: {str(e)}")
        
if __name__ == "__main__":
    fix_inventory_prices()
