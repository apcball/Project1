import xmlrpc.client
import pandas as pd
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('cancel_orders.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)

# Odoo connection settings
url = 'http://mogdev.work:8069'
db = 'MOG_LIVE1'
username = 'apichart@mogen.co.th'
password = '471109538'

def connect_to_odoo():
    """Establish connection to Odoo server"""
    try:
        # Create server proxy with allow_none=True
        common = xmlrpc.client.ServerProxy(f'{url}/xmlrpc/2/common', allow_none=True)
        uid = common.authenticate(db, username, password, {})
        models = xmlrpc.client.ServerProxy(f'{url}/xmlrpc/2/object', allow_none=True)
        return uid, models
    except Exception as e:
        logging.error(f"Failed to connect to Odoo: {str(e)}")
        return None, None

def get_order_details(models, uid, order_number, order_type):
    """Get order details from Odoo"""
    try:
        model = 'sale.order' if order_type == 'SO' else 'purchase.order'
        order_ids = models.execute_kw(db, uid, password,
            model, 'search',
            [[['name', '=', order_number]]]
        )
        
        if order_ids:
            fields = ['name', 'state', 'id']
            if order_type == 'PO':
                fields.extend(['picking_ids', 'invoice_ids'])
            elif order_type == 'SO':
                fields.extend(['picking_ids', 'invoice_ids'])
                
            order_data = models.execute_kw(db, uid, password,
                model, 'read',
                [order_ids[0]],  # Get first order only
                {'fields': fields}
            )
            if order_data:
                return order_data[0]  # Return first record
        return None
    except Exception as e:
        logging.error(f"Error getting {order_type} details for {order_number}: {str(e)}")
        return None

def create_return_picking(models, uid, picking_id):
    """Create a return picking for a completed receipt"""
    try:
        # Get picking details with moves
        picking = models.execute_kw(db, uid, password,
            'stock.picking', 'read',
            [picking_id],
            {'fields': ['state', 'move_ids', 'location_id', 'location_dest_id']}
        )[0]
        
        if picking['state'] != 'done':
            return False
            
        # Get move details
        if picking.get('move_ids'):
            moves = models.execute_kw(db, uid, password,
                'stock.move', 'read',
                [picking['move_ids']],
                {'fields': ['product_id', 'product_uom_qty']}
            )
        
        # Create return picking wizard
        wizard_id = models.execute_kw(db, uid, password,
            'stock.return.picking', 'create',
            [{
                'picking_id': picking_id,
            }]
        )
        
        # Get product return moves
        wizard = models.execute_kw(db, uid, password,
            'stock.return.picking', 'read',
            [wizard_id],
            {'fields': ['product_return_moves']}
        )[0]
        
        # Update return quantities based on original moves
        if wizard.get('product_return_moves'):
            for move in wizard['product_return_moves']:
                # Find matching move
                matching_moves = [m for m in moves if m['product_id'][0] == move[2]['product_id']]
                if matching_moves:
                    total_qty = sum(m['product_uom_qty'] for m in matching_moves)
                    models.execute_kw(db, uid, password,
                        'stock.return.picking.line', 'write',
                        [[move[1]], {'quantity': total_qty}]
                    )
        
        # Create the return picking
        result = models.execute_kw(db, uid, password,
            'stock.return.picking', 'create_returns',
            [wizard_id]
        )
        
        if result and isinstance(result, dict):
            return_picking_id = result.get('res_id')
            if return_picking_id:
                # Validate the return picking
                models.execute_kw(db, uid, password,
                    'stock.picking', 'action_confirm',
                    [[return_picking_id]]
                )
                
                models.execute_kw(db, uid, password,
                    'stock.picking', 'action_assign',
                    [[return_picking_id]]
                )
                
                # Get return moves
                return_moves = models.execute_kw(db, uid, password,
                    'stock.move', 'search_read',
                    [[['picking_id', '=', return_picking_id]]],
                    {'fields': ['product_uom_qty']}
                )
                
                # Set done quantities on moves
                for move in return_moves:
                    models.execute_kw(db, uid, password,
                        'stock.move', 'write',
                        [[move['id']], {'product_uom_qty': move['product_uom_qty']}]
                    )
                
                # Validate return picking
                try:
                    models.execute_kw(db, uid, password,
                        'stock.picking', 'button_validate',
                        [[return_picking_id]]
                    )
                except Exception as e:
                    logging.warning(f"Could not validate return picking: {str(e)}")
                    # Try to force the state
                    try:
                        models.execute_kw(db, uid, password,
                            'stock.picking', 'write',
                            [[return_picking_id], {'state': 'done'}]
                        )
                    except Exception as e2:
                        logging.error(f"Could not force return picking state: {str(e2)}")
                        return False
                
                return True
        
        return False
            
    except Exception as e:
        logging.error(f"Error creating return for picking {picking_id}: {str(e)}")
        return False

def cancel_purchase_order(models, uid, order_number):
    """Cancel a purchase order (RFQ) in Odoo 17"""
    try:
        # Get order details with additional fields
        order_data = get_order_details(models, uid, order_number, 'PO')
        if not order_data:
            logging.warning(f"Purchase order {order_number} not found")
            return False
            
        # Check order state
        current_state = order_data['state']
        if current_state == 'cancel':
            logging.info(f"Purchase order {order_number} is already cancelled")
            return True
            
        # Handle completed receipts first
        if 'picking_ids' in order_data and order_data['picking_ids']:
            pickings = models.execute_kw(db, uid, password,
                'stock.picking', 'read',
                [order_data['picking_ids']],
                {'fields': ['state']}
            )
            
            # Create returns for done pickings
            for pick in pickings:
                if pick['state'] == 'done':
                    logging.info(f"Creating return for receipt {pick['id']} of PO {order_number}")
                    if not create_return_picking(models, uid, pick['id']):
                        logging.error(f"Failed to create return for receipt {pick['id']}")
                elif pick['state'] not in ['cancel', 'done']:
                    # Cancel any pending receipts
                    try:
                        models.execute_kw(db, uid, password,
                            'stock.picking', 'action_cancel',
                            [[pick['id']]]
                        )
                    except Exception as e:
                        logging.warning(f"Could not cancel receipt {pick['id']}: {str(e)}")
        
        # Cancel any draft invoices
        if 'invoice_ids' in order_data and order_data['invoice_ids']:
            invoice_ids = models.execute_kw(db, uid, password,
                'account.move', 'search',
                [[['id', 'in', order_data['invoice_ids']], ['state', '=', 'draft']]]
            )
            if invoice_ids:
                try:
                    models.execute_kw(db, uid, password,
                        'account.move', 'button_cancel',
                        [invoice_ids]
                    )
                except Exception as e:
                    logging.warning(f"Could not cancel draft invoices: {str(e)}")
        
        # Try to cancel the PO
        order_ids = models.execute_kw(db, uid, password,
            'purchase.order', 'search',
            [[['name', '=', order_number]]]
        )
        
        # Force cancel by directly writing state
        try:
            models.execute_kw(db, uid, password,
                'purchase.order', 'write',
                [order_ids, {
                    'state': 'cancel',
                    'invoice_status': 'no'
                }]
            )
            logging.info(f"Successfully cancelled purchase order: {order_number}")
            return True
            
        except Exception as e:
            logging.error(f"Force cancel failed for PO {order_number}: {str(e)}")
            return False
            
    except Exception as e:
        logging.error(f"Error cancelling purchase order {order_number}: {str(e)}")
        return False

def process_cancellation_file(file_path):
    """Process the Excel file containing purchase orders to cancel"""
    try:
        # Read Excel file
        df = pd.read_excel(file_path)
        
        # Clean and prepare data
        df['name'] = df['name'].astype(str).str.strip()
        
        # Remove empty names and duplicates
        df = df[df['name'].str.len() > 0]  # Remove empty names
        df = df.drop_duplicates(subset=['name'])  # Remove duplicates
        
        # Connect to Odoo
        uid, models = connect_to_odoo()
        if not uid or not models:
            logging.error("Failed to connect to Odoo. Exiting.")
            return
        
        # Initialize results tracking
        results = {
            'successful': [],
            'failed': [],
            'skipped': []
        }
        
        # Process each row
        total_pos = len(df)
        for index, row in df.iterrows():
            order_number = row['name']
            logging.info(f"Processing PO {order_number} ({index + 1}/{total_pos})")
            
            # Get order details first
            order_data = get_order_details(models, uid, order_number, 'PO')
            if not order_data:
                results['skipped'].append((order_number, 'Not found'))
                continue
                
            # Check if order is already cancelled
            if order_data['state'] == 'cancel':
                results['skipped'].append((order_number, 'Already cancelled'))
                continue
                
            # Try to cancel the PO
            if cancel_purchase_order(models, uid, order_number):
                results['successful'].append(order_number)
            else:
                results['failed'].append((order_number, order_data['state']))
        
        # Log detailed summary
        logging.info("\nPurchase Order Cancellation Summary:")
        
        logging.info(f"\nSuccessful Cancellations ({len(results['successful'])}/{total_pos}):")
        for order in results['successful']:
            logging.info(f"  - {order}")
            
        logging.info(f"\nFailed Cancellations ({len(results['failed'])}/{total_pos}):")
        for order, state in results['failed']:
            logging.info(f"  - {order} (State: {state})")
            
        logging.info(f"\nSkipped Orders ({len(results['skipped'])}/{total_pos}):")
        for order, reason in results['skipped']:
            logging.info(f"  - {order} ({reason})")
        
    except Exception as e:
        logging.error(f"Error processing Excel file: {str(e)}")
        logging.error("Stack trace:", exc_info=True)

if __name__ == "__main__":
    excel_file = "Data_file/Cancel_SO.xlsx"
    logging.info("Starting order cancellation process...")
    process_cancellation_file(excel_file)
    logging.info("Order cancellation process completed.")