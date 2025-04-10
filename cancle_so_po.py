import xmlrpc.client
import pandas as pd
from datetime import datetime
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('cancel_orders.log'),
        logging.StreamHandler()
    ]
)

# Odoo connection settings
url = 'http://mogth.work:8069'
db = 'MOG_LIVE'
username = 'apichart@mogen.co.th'
password = '471109538'

def connect_to_odoo():
    """Establish connection to Odoo server"""
    try:
        common = xmlrpc.client.ServerProxy(f'{url}/xmlrpc/2/common')
        uid = common.authenticate(db, username, password, {})
        models = xmlrpc.client.ServerProxy(f'{url}/xmlrpc/2/object')
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
            order_data = models.execute_kw(db, uid, password,
                model, 'read',
                [order_ids],
                {'fields': ['name', 'state']}
            )
            return order_data[0]
        return None
    except Exception as e:
        logging.error(f"Error getting {order_type} details for {order_number}: {str(e)}")
        return None

def cancel_sale_order(models, uid, order_number):
    """Cancel a sale order (quotation) in Odoo"""
    try:
        # Get order details first
        order_data = get_order_details(models, uid, order_number, 'SO')
        if not order_data:
            logging.warning(f"Sale order {order_number} not found")
            return False
            
        # Check if order can be cancelled
        if order_data['state'] in ['cancel', 'done']:
            logging.warning(f"Sale order {order_number} is already {order_data['state']}")
            return False
            
        # Search for the sale order
        order_ids = models.execute_kw(db, uid, password,
            'sale.order', 'search',
            [[['name', '=', order_number]]]
        )
        
        # Cancel the sale order
        result = models.execute_kw(db, uid, password,
            'sale.order', 'action_cancel',
            [order_ids]
        )
        
        # Verify the cancellation
        updated_order = get_order_details(models, uid, order_number, 'SO')
        if updated_order and updated_order['state'] == 'cancel':
            logging.info(f"Successfully cancelled sale order: {order_number}")
            return True
        else:
            logging.error(f"Failed to cancel sale order {order_number}")
            return False
            
    except Exception as e:
        logging.error(f"Error cancelling sale order {order_number}: {str(e)}")
        return False

def cancel_purchase_order(models, uid, order_number):
    """Cancel a purchase order (RFQ) in Odoo"""
    try:
        # Get order details first
        order_data = get_order_details(models, uid, order_number, 'PO')
        if not order_data:
            logging.warning(f"Purchase order {order_number} not found")
            return False
            
        # Check if order can be cancelled
        if order_data['state'] in ['cancel', 'done', 'purchase']:
            logging.warning(f"Purchase order {order_number} is already {order_data['state']}")
            return False
            
        # Search for the purchase order
        order_ids = models.execute_kw(db, uid, password,
            'purchase.order', 'search',
            [[['name', '=', order_number]]]
        )
        
        # Cancel the purchase order
        result = models.execute_kw(db, uid, password,
            'purchase.order', 'button_cancel',
            [order_ids]
        )
        
        # Verify the cancellation
        updated_order = get_order_details(models, uid, order_number, 'PO')
        if updated_order and updated_order['state'] == 'cancel':
            logging.info(f"Successfully cancelled purchase order: {order_number}")
            return True
        else:
            logging.error(f"Failed to cancel purchase order {order_number}")
            return False
            
    except Exception as e:
        logging.error(f"Error cancelling purchase order {order_number}: {str(e)}")
        return False

def process_cancellation_file(file_path):
    """Process the Excel file containing orders to cancel"""
    try:
        # Read Excel file
        df = pd.read_excel(file_path)
        
        # Verify required columns exist
        required_columns = ['name', 'type']
        if not all(col.lower() in [c.lower() for c in df.columns] for col in required_columns):
            logging.error(f"Excel file must contain columns: {required_columns}")
            return
        
        # Connect to Odoo
        uid, models = connect_to_odoo()
        if not uid or not models:
            logging.error("Failed to connect to Odoo. Exiting.")
            return
        
        # Initialize counters
        successful_so = 0
        failed_so = 0
        successful_po = 0
        failed_po = 0
        
        # Process each row
        for index, row in df.iterrows():
            order_number = str(row['name']).strip()
            order_type = str(row['type']).strip().upper()
            
            logging.info(f"Processing {order_type} {order_number}")
            
            if order_type == 'SO':
                if cancel_sale_order(models, uid, order_number):
                    successful_so += 1
                else:
                    failed_so += 1
            elif order_type == 'PO':
                if cancel_purchase_order(models, uid, order_number):
                    successful_po += 1
                else:
                    failed_po += 1
            else:
                logging.warning(f"Unknown order type '{order_type}' for order {order_number}")
        
        # Log summary
        logging.info(f"""
        Cancellation Summary:
        Sales Orders: {successful_so} successful, {failed_so} failed
        Purchase Orders: {successful_po} successful, {failed_po} failed
        """)
        
    except Exception as e:
        logging.error(f"Error processing Excel file: {str(e)}")

if __name__ == "__main__":
    excel_file = "Data_file/Cancel_SO.xlsx"
    logging.info("Starting order cancellation process...")
    process_cancellation_file(excel_file)
    logging.info("Order cancellation process completed.")