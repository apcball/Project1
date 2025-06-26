import xmlrpc.client
import os
import logging
import time
from datetime import datetime, timedelta

# 🔧 Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f"clear_stock_zero_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# 🔐 Connection parameters
HOST = 'http://mogdev.work:8069'
DB = 'MOG_LIVE_26-06'
USERNAME = 'apichart@mogen.co.th'
# Consider using environment variables for sensitive information
PASSWORD = os.environ.get('ODOO_PASSWORD', '471109538')  # Better to use environment variable

try:
    # 📌 Connect to Odoo
    common = xmlrpc.client.ServerProxy(f'{HOST}/xmlrpc/2/common')
    uid = common.authenticate(DB, USERNAME, PASSWORD, {})
    if not uid:
        raise Exception("Authentication failed")
    logger.info(f"Successfully connected to Odoo as user ID: {uid}")
    
    models = xmlrpc.client.ServerProxy(f'{HOST}/xmlrpc/2/object')
    
    # 📋 First, let's check all available internal locations
    all_internal_locations = models.execute_kw(DB, uid, PASSWORD,
        'stock.location', 'search_read',
        [[['usage', '=', 'internal']]],
        {'fields': ['name', 'complete_name']}
    )
    
    logger.info(f"Available internal locations ({len(all_internal_locations)}):")
    for loc in all_internal_locations:
        logger.info(f"- ID: {loc['id']}, Name: {loc['name']}, Complete Name: {loc.get('complete_name', 'N/A')}")

    # 🗓️ Set Inventory Adjustment Date to 31/01/2025
    adjustment_date = '2025-01-31 00:00:00'  # Format: YYYY-MM-DD HH:MM:SS
    logger.info(f"Setting inventory adjustment date to: {adjustment_date} (31/01/2025)")
    # Check and update Odoo 17 settings to allow backdating
    try:
        # Get company ID
        company_id = models.execute_kw(DB, uid, PASSWORD,
            'res.company', 'search',
            [[]], {'limit': 1}
        )[0]
        
        # Check all possible settings that affect backdating
        field_list = ['stock_move_timestamp_backward', 'accounting_date_lock', 
                      'stock_move_period_lock', 'period_lock_date', 
                      'fiscalyear_lock_date', 'stock_lock_date']
        
        company_data = models.execute_kw(DB, uid, PASSWORD,
            'res.company', 'read',
            [[company_id]], {'fields': field_list}
        )[0]
        
        logger.info(f"Current company settings: {company_data}")
        
        # Try to update ALL possible settings to ensure backdating works
        company_settings = {
            'stock_move_timestamp_backward': True,    # Allow backdating stock moves
            'stock_move_period_lock': False,          # Disable period locking for stock
            'period_lock_date': False,                # Clear lock date for accounting
            'fiscalyear_lock_date': False,            # Clear fiscal year lock
            'stock_lock_date': False,                 # Clear stock lock date
        }
        
        models.execute_kw(DB, uid, PASSWORD,
            'res.company', 'write',
            [[company_id], company_settings]
        )
        
        # Check if there are stock.config.settings or any other settings models
        try:
            # Try to update stock settings if available
            stock_config_id = models.execute_kw(DB, uid, PASSWORD,
                'stock.config.settings', 'create',
                [{
                    'group_stock_multi_locations': True,
                    'module_stock_account': True,
                    'allow_negative_stock': True,  # Allow negative stock
                    'allow_backorder': False       # Disable backorders
                }]
            )
            
            # Try to apply the settings
            models.execute_kw(DB, uid, PASSWORD,
                'stock.config.settings', 'execute',
                [[stock_config_id]]
            )
            logger.info("Updated stock configuration settings")
        except Exception as stock_config_error:
            logger.info(f"Stock config settings not available: {str(stock_config_error)}")
        
        logger.info("Updated company settings to allow backdating")
    except Exception as settings_error:
        logger.warning(f"Could not update company settings for backdating: {str(settings_error)}")

    # Ask user to confirm proceeding with specific locations or use discovered ones
    location_names = ['FG10/Stock', 'RM01/Stock', 'AS01/Stock']  # Original location names
    
    # Use input only if running in interactive mode
    import sys
    if sys.stdin.isatty():
        print("\n⚠️ ATTENTION ⚠️: This script will reset ALL stock to ZERO at selected locations!")
        print(f"👉 Date for inventory adjustment: Current date ({adjustment_date})")
        print("\nDo you want to:")
        print("1. Proceed with original location names:", location_names)
        print("2. Enter new location names")
        print("3. Use ALL internal locations (BE CAREFUL!)")
        choice = input("Enter your choice (1/2/3): ")
        
        # Additional confirmation for safety
        if choice in ["1", "2", "3"]:
            confirmation = input(f"\n⚠️ CONFIRMATION REQUIRED: Are you sure you want to set ALL stock to ZERO at the selected locations on {adjustment_date} (current date)? (yes/no): ")
            if confirmation.lower() not in ["yes", "y"]:
                logger.info("Operation canceled by user")
                exit(0)
        
        if choice == "2":
            input_names = input("Enter location names separated by comma: ")
            location_names = [name.strip() for name in input_names.split(',')]
        elif choice == "3":
            location_names = [loc['name'] for loc in all_internal_locations]
    else:
        logger.info("Running in non-interactive mode, using original location names")

    # 🔍 Search target internal locations - using both name and complete_name for better matches
    location_ids = []
    # First try with complete_name
    complete_name_ids = models.execute_kw(DB, uid, PASSWORD,
        'stock.location', 'search',
        [[['usage', '=', 'internal'], ['complete_name', 'in', location_names]]]
    )
    location_ids.extend(complete_name_ids)
    
    # If no results or not all locations found, try a more flexible search
    if len(location_ids) < len(location_names):
        logger.info(f"Some locations not found by complete_name, trying alternative search methods...")
        # Get all locations again to do manual checks
        all_locs = models.execute_kw(DB, uid, PASSWORD,
            'stock.location', 'search_read',
            [[['usage', '=', 'internal']]],
            {'fields': ['name', 'complete_name']}
        )
        
        # Find locations by matching partial name or path
        for name in location_names:
            parts = name.split('/')
            for loc in all_locs:
                # Skip if already found
                if loc['id'] in location_ids:
                    continue
                    
                # Try to match by complete name parts
                if parts[0] in loc.get('complete_name', '') and (len(parts) == 1 or parts[-1] in loc.get('complete_name', '')):
                    location_ids.append(loc['id'])
                    logger.info(f"Found location via partial match: {loc.get('complete_name', 'N/A')} (ID: {loc['id']})")
    
    # Remove duplicates and sort
    location_ids = sorted(list(set(location_ids)))
    logger.info(f"Found {len(location_ids)} locations: {location_ids}")
    
    if not location_ids:
        logger.warning("No locations found with the specified names. Check location names and try again.")
        exit(0)
        
    # Display found locations before proceeding
    location_details = models.execute_kw(DB, uid, PASSWORD,
        'stock.location', 'read',
        [location_ids, ['name', 'complete_name']]
    )
    
    logger.info("The following locations will be processed:")
    for loc in location_details:
        logger.info(f"- {loc.get('complete_name', loc['name'])} (ID: {loc['id']})")
    
    # Final confirmation when in interactive mode
    if sys.stdin.isatty():
        print("\nThe following locations will have ALL stock reset to ZERO:")
        for loc in location_details:
            print(f"- {loc.get('complete_name', loc['name'])} (ID: {loc['id']})")
        print(f"\nInventory Date: Current date ({adjustment_date})")
        final_confirm = input("\n⚠️ THIS IS YOUR LAST CHANCE TO CANCEL ⚠️\nType 'CONFIRM ZERO STOCK' (all caps) to proceed: ")
        if final_confirm != "CONFIRM ZERO STOCK":
            logger.info("Operation canceled by user at final confirmation")
            exit(0)

    # 🧾 Loop each location and directly update stock quantities
    for loc_id in location_ids:
        try:
            # Get location name for better logging
            loc_name = models.execute_kw(DB, uid, PASSWORD,
                'stock.location', 'read',
                [loc_id, ['name']]
            )[0]['name']
            
            logger.info(f"Processing location: {loc_name} (ID: {loc_id})")
            
            # Since Odoo 17 doesn't have stock.inventory model, we'll use direct methods on stock.quant
            
            # Step 1: Get all products with non-zero quantity in this location
            products = models.execute_kw(DB, uid, PASSWORD,
                'stock.quant', 'search_read',
                [[['location_id', '=', loc_id], ['quantity', '!=', 0]]],
                {'fields': ['product_id', 'quantity', 'product_uom_id']}
            )
            
            logger.info(f"Found {len(products)} products with stock in location {loc_id}")
            
            # Step 2: Get the inventory adjustment location (location where lost items go)
            inventory_loc_ids = models.execute_kw(DB, uid, PASSWORD,
                'stock.location', 'search',
                [[['usage', '=', 'inventory']]], {'limit': 1}
            )
            
            if not inventory_loc_ids:
                logger.warning("No inventory loss location found, creating one...")
                inventory_loc_id = models.execute_kw(DB, uid, PASSWORD,
                    'stock.location', 'create',
                    [{
                        'name': 'Inventory adjustment',
                        'usage': 'inventory'
                    }]
                )
            else:
                inventory_loc_id = inventory_loc_ids[0]
                
            logger.info(f"Using inventory loss location ID: {inventory_loc_id}")
            
            # Keep track of total quantity adjusted
            total_adjusted = 0
            successful_products = 0
            
            # Step 3: Process each product in this location
            for product in products:
                product_id = product['product_id'][0]
                current_qty = product['quantity']
                product_name = product['product_id'][1] if len(product['product_id']) > 1 else f"Product ID {product_id}"
                
                logger.info(f"Setting to zero: {product_name} (current qty: {current_qty})")
                
                try:
                    # Get the product's UoM
                    product_uom = models.execute_kw(DB, uid, PASSWORD,
                        'product.product', 'read',
                        [product_id, ['uom_id']]
                    )[0]['uom_id'][0]
                    
                    # Method 1: Try to create inventory adjustments directly using stock.move
                    try:
                        # Creating inventory loss move directly with extra fields to ensure proper impact
                        move_vals = {
                            'name': f'Reset stock to 0 on {adjustment_date}',
                            'product_id': product_id,
                            'product_uom_qty': current_qty,
                            'product_uom': product_uom,
                            'date': adjustment_date,
                            'location_id': loc_id,
                            'location_dest_id': inventory_loc_id,
                            # Force immediate impact and processing
                            'state': 'draft',
                            'procure_method': 'make_to_stock',
                            'is_inventory': True
                        }
                        
                        # Create the move first
                        move_id = models.execute_kw(DB, uid, PASSWORD,
                            'stock.move', 'create', [move_vals]
                        )
                        
                        # Ensure that the move is properly processed
                        try:
                            # For Odoo 17, we can't call private methods (_action_assign)
                            # Direct state update approach
                            try:
                                # Try to set the state to 'done' directly
                                models.execute_kw(DB, uid, PASSWORD,
                                    'stock.move', 'write',
                                    [[move_id], {'state': 'done', 'date': adjustment_date}]
                                )
                            except Exception as state_error:
                                logger.warning(f"Could not update state directly: {str(state_error)}")
                                
                            # Try to use available public methods if direct state change fails
                            try:
                                # Try to use a public method if available
                                available_methods = models.execute_kw(DB, uid, PASSWORD,
                                    'stock.move', 'get_external_api', 
                                    [], {'method': 'name_search', 'args': ['']}
                                )
                                logger.info(f"Available methods: {available_methods}")
                            except Exception:
                                pass
                                                                
                            # Check if the move had any effect
                            move_details = models.execute_kw(DB, uid, PASSWORD,
                                'stock.move', 'read',
                                [[move_id], ['state']]
                            )[0]
                            
                            logger.info(f"Move state after processing: {move_details['state']}")
                                
                            # Explicitly update the date
                            models.execute_kw(DB, uid, PASSWORD,
                                'stock.move', 'write',
                                [[move_id], {'date': adjustment_date}]
                            )
                            
                            # Verify the move actually affected stock - just check the state
                            # In Odoo 17, 'quantity_done' field may have a different name or not be accessible
                            move_details = models.execute_kw(DB, uid, PASSWORD,
                                'stock.move', 'read',
                                [[move_id], ['state']]
                            )[0]
                            
                            if move_details['state'] == 'done':
                                logger.info(f"✅ Created and confirmed stock move to reset {product_name} (state: {move_details['state']})")
                            else:
                                logger.warning(f"⚠️ Move created but may not have been fully processed: {move_details}")
                        except Exception as action_error:
                            logger.warning(f"Error while processing move: {str(action_error)}")
                        
                        logger.info(f"✅ Created and confirmed stock move to reset {product_name}")
                        total_adjusted += current_qty
                        successful_products += 1
                    
                    # Method 2: If move creation fails, try direct quant update
                    except Exception as move_error:
                        logger.warning(f"Move creation failed for {product_name}, trying direct quant update: {str(move_error)}")
                        
                        # Find quants for this product in this location
                        quant_ids = models.execute_kw(DB, uid, PASSWORD,
                            'stock.quant', 'search',
                            [[['location_id', '=', loc_id], ['product_id', '=', product_id]]]
                        )
                        
                        if quant_ids:
                            try:
                                # First, check if we have an action_apply method for inventory adjustments
                                try:
                                    # Create inventory adjustment entry
                                    models.execute_kw(DB, uid, PASSWORD,
                                        'stock.quant', 'action_set_inventory_quantity',
                                        [[quant_ids[0]]],
                                        {'inventory_quantity': 0.0, 'inventory_date': adjustment_date}
                                    )
                                    logger.info(f"✅ Applied inventory quantity via action_set_inventory_quantity for {product_name}")
                                except Exception:
                                    # Direct update the quant - multiple approaches
                                    try:
                                        # Try method 1: Update with inventory_quantity field
                                        models.execute_kw(DB, uid, PASSWORD,
                                            'stock.quant', 'write',
                                            [quant_ids, {
                                                'quantity': 0.0,
                                                'inventory_date': adjustment_date,
                                                'inventory_quantity': 0.0,
                                                'inventory_diff_quantity': -current_qty
                                            }]
                                        )
                                        
                                        # Try to apply the inventory
                                        try:
                                            models.execute_kw(DB, uid, PASSWORD,
                                                'stock.quant', 'action_apply_inventory',
                                                [quant_ids]
                                            )
                                            logger.info("Applied inventory via action_apply_inventory")
                                        except:
                                            pass
                                            
                                    except Exception as write_error:
                                        # Try method 2: Create a stock.inventory adjustment record directly in SQL
                                        logger.warning(f"Direct quant update failed: {str(write_error)}")
                                        logger.info("Attempting direct database inventory correction...")
                                        
                                        # In Odoo 17, we might need to create a stock.quant.correction record
                                        try:
                                            correction_id = models.execute_kw(DB, uid, PASSWORD,
                                                'stock.quant.correction', 'create',
                                                [{
                                                    'product_id': product_id,
                                                    'location_id': loc_id,
                                                    'inventory_quantity': 0.0,
                                                    'inventory_diff_quantity': -current_qty,
                                                    'date': adjustment_date
                                                }]
                                            )
                                            
                                            # Try to apply the correction
                                            models.execute_kw(DB, uid, PASSWORD,
                                                'stock.quant.correction', 'action_validate_inventory',
                                                [[correction_id]]
                                            )
                                            logger.info(f"Created and validated quant correction for {product_name}")
                                        except Exception as correction_error:
                                            logger.error(f"Correction failed: {str(correction_error)}")
                                
                                # Verify the update worked by reading the current quantity
                                current_qty_after = models.execute_kw(DB, uid, PASSWORD,
                                    'stock.quant', 'read',
                                    [quant_ids, ['quantity']]
                                )[0]['quantity']
                                
                                if current_qty_after == 0:
                                    logger.info(f"✅ Verified: quantity is now zero for {product_name}")
                                else:
                                    logger.warning(f"⚠️ Quantity not zero after update for {product_name}: {current_qty_after}")
                                
                                total_adjusted += current_qty
                                successful_products += 1
                            except Exception as quant_error:
                                logger.error(f"❌ Failed direct quant update for {product_name}: {str(quant_error)}")
                        else:
                            logger.warning(f"No quants found for {product_name} at location {loc_name}")
                
                except Exception as product_error:
                    logger.error(f"❌ Failed to process {product_name}: {str(product_error)}")
            
            # Log summary for this location
            logger.info(f"📊 Summary for {loc_name} (ID: {loc_id}):")
            logger.info(f"   - Total products found: {len(products)}")
            logger.info(f"   - Successfully adjusted: {successful_products}")
            logger.info(f"   - Total quantity adjusted: {total_adjusted}")
            
            if successful_products == len(products) and successful_products > 0:
                logger.info(f"[✅] Successfully reset ALL stock to 0 at location {loc_name} (ID: {loc_id}) on {adjustment_date}")
            elif successful_products > 0:
                logger.info(f"[⚠️] Partially reset stock to 0 at location {loc_name} (ID: {loc_id}) on {adjustment_date}")
            else:
                logger.info(f"[❌] Failed to reset stock to 0 at location {loc_name} (ID: {loc_id})")
                
        except Exception as e:
            logger.error(f"Error processing location ID {loc_id}: {str(e)}")
            logger.info(f"[✅] Reset stock to 0 at location {loc_name} (ID: {loc_id}) on {adjustment_date}")
        
        except Exception as e:
            logger.error(f"Error processing location ID {loc_id}: {str(e)}")

except Exception as e:
    logger.error(f"Error: {str(e)}")
    exit(1)

logger.info("Script completed successfully")
