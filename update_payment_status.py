import xmlrpc.client
import logging
import time

# Create log file
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('update_payment_status.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)

# Odoo connection settings (Matched with api_payment_config.py)
url = 'http://160.187.249.148:8069'
db = 'MOG_LIVE'
username = 'apichart@mogen.co.th'
password = '471109538'

def connect_to_odoo():
    """Establish connection to Odoo server"""
    try:
        common = xmlrpc.client.ServerProxy(f'{url}/xmlrpc/2/common', allow_none=True)
        uid = common.authenticate(db, username, password, {})
        models = xmlrpc.client.ServerProxy(f'{url}/xmlrpc/2/object', allow_none=True)
        logging.info(f"Connected to Odoo: {url} DB: {db}")
        return uid, models
    except Exception as e:
        logging.error(f"Failed to connect to Odoo: {str(e)}")
        return None, None

def get_cancelled_payments(models, uid):
    """Search for Vendor and Customer payments with status 'cancel'"""
    try:
        # Domain to find payments:
        # 1. state is 'cancel'
        # 2. payment_type is either 'inbound' (Customer) or 'outbound' (Vendor)
        domain = [
            ['state', '=', 'cancel'],
            ['payment_type', 'in', ['inbound', 'outbound']]
        ]
        
        payment_ids = models.execute_kw(db, uid, password,
            'account.payment', 'search',
            [domain]
        )
        return payment_ids
    except Exception as e:
        logging.error(f"Error searching for payments: {str(e)}")
        return []

def update_payment_status(models, uid, payment_id):
    """Update payment status from Cancel -> Draft -> Posted"""
    try:
        # Read payment details for logging
        payment = models.execute_kw(db, uid, password,
            'account.payment', 'read',
            [payment_id],
            {'fields': ['name', 'payment_type', 'partner_type', 'state']}
        )[0]
        
        name = payment.get('name', 'Unknown')
        payment_type = payment.get('payment_type', 'Unknown')
        current_state = payment.get('state')

        logging.info(f"Processing Payment: {name} | Type: {payment_type} | Current State: {current_state}")

        if current_state != 'cancel':
            logging.warning(f"Payment {name} is not in 'cancel' state. Skipping.")
            return False

        # Step 1: Set to Draft
        logging.info(f"  - Setting {name} to Draft...")
        try:
            models.execute_kw(db, uid, password,
                'account.payment', 'action_draft',
                [[payment_id]]
            )
        except xmlrpc.client.Fault as e:
            if "cannot marshal None" in str(e):
                logging.warning("  - RPC returned None (known server issue), verifying state change...")
            else:
                logging.error(f"  - Fault in action_draft: {e}")
                # Don't return yet, check state
        except Exception as e:
            logging.error(f"  - Error in action_draft: {e}")

        # Verify Draft State
        payment = models.execute_kw(db, uid, password,
            'account.payment', 'read',
            [payment_id],
            {'fields': ['state']}
        )[0]
        
        if payment['state'] == 'draft':
            logging.info(f"  - State validated: Draft")
        else:
            logging.error(f"  - Failed to set to Draft. Current state: {payment['state']}")
            return False

        # Step 2: Post
        logging.info(f"  - Posting {name}...")
        try:
            models.execute_kw(db, uid, password,
                'account.payment', 'action_post',
                [[payment_id]]
            )
        except xmlrpc.client.Fault as e:
            if "cannot marshal None" in str(e):
                logging.warning("  - RPC returned None (known server issue), verifying state change...")
            else:
                logging.error(f"  - Fault in action_post: {e}")
        except Exception as e:
            logging.error(f"  - Error in action_post: {e}")
        
        # Verify Posted Status
        final_payment = models.execute_kw(db, uid, password,
            'account.payment', 'read',
            [payment_id],
            {'fields': ['state']}
        )[0]
        
        if final_payment['state'] == 'posted':
            logging.info(f"  - Success: {name} is now Posted.")
            return True
        else:
            logging.error(f"  - Failed: {name} is in state '{final_payment['state']}' after action.")
            return False

    except Exception as e:
        logging.error(f"Error processing payment ID {payment_id}: {str(e)}")
        return False

def main():
    logging.info("Starting Payment Status Update Script...")
    
    uid, models = connect_to_odoo()
    if not uid:
        return

    # Find payments
    logging.info("Searching for cancelled Vendor and Customer payments...")
    payment_ids = get_cancelled_payments(models, uid)
    
    if not payment_ids:
        logging.info("No cancelled payments found.")
        return

    logging.info(f"Found {len(payment_ids)} cancelled payments. Starting update process...")
    
    success_count = 0
    fail_count = 0
    
    # Process each payment
    for index, payment_id in enumerate(payment_ids):
        # Progress log every 10 items
        if index % 10 == 0:
            logging.info(f"Progress: {index}/{len(payment_ids)}")
            
        if update_payment_status(models, uid, payment_id):
            success_count += 1
        else:
            fail_count += 1
            
        # Optional: update this to sleep lightly if system load is a concern
        # time.sleep(0.1)

    logging.info("="*50)
    logging.info("Process Completed")
    logging.info(f"Total Found: {len(payment_ids)}")
    logging.info(f"Successfully Posted: {success_count}")
    logging.info(f"Failed: {fail_count}")
    logging.info("="*50)

if __name__ == "__main__":
    main()
