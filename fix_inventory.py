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
EXCEL_FILE = 'Data_file/สิ้นเปลืองโรงงาน.xlsx'

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
    """Establish connection to Odoo server"""
    try:
        common = xmlrpc.client.ServerProxy(f'{HOST}/xmlrpc/2/common')
        uid = common.authenticate(DB, USERNAME, PASSWORD, {})
        if not uid:
            raise Exception("Authentication failed")
        models = xmlrpc.client.ServerProxy(f'{HOST}/xmlrpc/2/object')
        logger.info(f"Successfully connected to Odoo as user ID: {uid}")
        return uid, models
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
    """Get product ID by default_code with enhanced search capability"""
    try:
        default_code = str(default_code).strip()
        logger.info(f"Searching for product with code: '{default_code}'")

        # 1. Try exact match first
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

        # 2. Try case-insensitive match
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
        
        # 3. If code starts with INSTALL_, try without the prefix
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

        # 4. Try name search as a last resort
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
                        location_ids = models.execute_kw(DB, uid, PASSWORD, 'stock.location', 'search',
                            [[('usage', '=', 'internal')]],
                            {'limit': 1}
                        )
                        location_id = location_ids[0] if location_ids else False
                        
                        if not location_id:
                            logger.warning(f"No internal location found for {product_code}")
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
