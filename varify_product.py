import xmlrpc.client
import pandas as pd
from datetime import datetime
import time
import json
import os
from typing import Tuple, List, Dict
import logging

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('product_verify.log'),
        logging.StreamHandler()
    ]
)

# --- Connection Settings ---
url = 'http://mogth.work:8069'
db = 'MOG_LIVE'
username = 'apichart@mogen.co.th'
password = '471109538'

# Constants
BATCH_SIZE = 1000  # Number of records to process in each batch
MAX_RETRIES = 3    # Maximum number of connection retry attempts
RETRY_DELAY = 5    # Delay in seconds between retries
CHECKPOINT_FILE = 'verification_checkpoint.json'
TEMP_RESULTS_FILE = 'temp_not_found_products.xlsx'

class OdooConnection:
    def __init__(self):
        self.uid = None
        self.models = None
        self.common = None
        self.last_connection_attempt = 0
        self.connection_cooldown = 30  # Cooldown period in seconds
        self.max_backoff = 300  # Maximum backoff time in seconds

    def calculate_backoff(self, attempt: int) -> int:
        """Calculate exponential backoff time with jitter"""
        import random
        base_delay = min(RETRY_DELAY * (2 ** attempt), self.max_backoff)
        jitter = random.uniform(0, 0.1 * base_delay)  # 10% jitter
        return base_delay + jitter

    def should_attempt_reconnect(self) -> bool:
        """Check if enough time has passed since last connection attempt"""
        current_time = time.time()
        if current_time - self.last_connection_attempt < self.connection_cooldown:
            return False
        return True

    def connect(self) -> bool:
        if not self.should_attempt_reconnect():
            logging.warning("Connection attempt too soon. Waiting for cooldown...")
            return False

        self.last_connection_attempt = time.time()

        for attempt in range(MAX_RETRIES):
            try:
                # Test internet connectivity first
                import socket
                socket.create_connection(("8.8.8.8", 53), timeout=3)
                
                # Initialize connection objects
                self.common = xmlrpc.client.ServerProxy(f'{url}/xmlrpc/2/common', allow_none=True)
                
                # Test server availability with a timeout
                self.common.version()
                
                # Authenticate
                self.uid = self.common.authenticate(db, username, password, {})
                if not self.uid:
                    raise Exception("Authentication failed")
                
                # Initialize models
                self.models = xmlrpc.client.ServerProxy(f'{url}/xmlrpc/2/object', allow_none=True)
                
                logging.info("Successfully connected to Odoo")
                return True

            except socket.error as e:
                logging.error(f"Network error on attempt {attempt + 1}: {str(e)}")
            except xmlrpc.client.Fault as e:
                logging.error(f"Odoo server error on attempt {attempt + 1}: {str(e)}")
            except Exception as e:
                logging.error(f"Connection attempt {attempt + 1} failed: {str(e)}")

            if attempt < MAX_RETRIES - 1:
                backoff_time = self.calculate_backoff(attempt)
                logging.info(f"Waiting {backoff_time:.1f} seconds before next attempt...")
                time.sleep(backoff_time)
            else:
                logging.error("Max retry attempts reached. Connection failed.")
                return False

        return False

    def ensure_connected(self) -> bool:
        """Ensure connection is active and reconnect if necessary"""
        try:
            if self.uid and self.models:
                # Test connection with a light-weight call
                self.models.execute_kw(db, self.uid, password, 'res.partner', 'check_access_rights', ['read'], {'raise_exception': False})
                return True
        except Exception:
            logging.warning("Connection test failed, attempting to reconnect...")
            return self.connect()
        return False

    def search_product(self, code: str) -> Tuple[bool, str, dict]:
        max_search_retries = 3
        for attempt in range(max_search_retries):
            try:
                # Ensure connection is active before searching
                if not self.ensure_connected():
                    raise Exception("Failed to establish connection")

                # Search by default_code
                product = self.models.execute_kw(db, self.uid, password,
                    'product.template', 'search_read',
                    [[['default_code', '=', code]]],
                    {'fields': ['default_code', 'old_product_code', 'name']})
                
                if product:
                    return True, "default_code", product[0]

                # Search by old_product_code
                product = self.models.execute_kw(db, self.uid, password,
                    'product.template', 'search_read',
                    [[['old_product_code', '=', code]]],
                    {'fields': ['default_code', 'old_product_code', 'name']})
                
                if product:
                    return True, "old_product_code", product[0]

                return False, "", {}

            except xmlrpc.client.Fault as e:
                logging.error(f"Odoo API error for product {code} (attempt {attempt + 1}): {str(e)}")
                if "session expired" in str(e).lower():
                    self.connect()
                    continue
                if attempt < max_search_retries - 1:
                    time.sleep(self.calculate_backoff(attempt))
                    continue
                raise

            except Exception as e:
                logging.error(f"Error searching product {code} (attempt {attempt + 1}): {str(e)}")
                if attempt < max_search_retries - 1:
                    time.sleep(self.calculate_backoff(attempt))
                    continue
                raise

class ProductVerifier:
    def __init__(self):
        self.odoo = OdooConnection()
        self.checkpoint_data = self.load_checkpoint()
        self.not_found_products = self.load_temp_results()

    def load_checkpoint(self) -> dict:
        if os.path.exists(CHECKPOINT_FILE):
            try:
                with open(CHECKPOINT_FILE, 'r') as f:
                    return json.load(f)
            except Exception as e:
                logging.error(f"Error loading checkpoint: {str(e)}")
        return {'last_processed_index': 0}

    def save_checkpoint(self, index: int):
        try:
            with open(CHECKPOINT_FILE, 'w') as f:
                json.dump({'last_processed_index': index}, f)
        except Exception as e:
            logging.error(f"Error saving checkpoint: {str(e)}")

    def load_temp_results(self) -> List[Dict]:
        if os.path.exists(TEMP_RESULTS_FILE):
            try:
                df = pd.read_excel(TEMP_RESULTS_FILE)
                return df.to_dict('records')
            except Exception as e:
                logging.error(f"Error loading temporary results: {str(e)}")
        return []

    def save_temp_results(self):
        try:
            df = pd.DataFrame(self.not_found_products)
            df.to_excel(TEMP_RESULTS_FILE, index=False)
        except Exception as e:
            logging.error(f"Error saving temporary results: {str(e)}")

    def process_batch(self, batch_df: pd.DataFrame, start_idx: int):
        consecutive_errors = 0
        max_consecutive_errors = 5
        
        for index, row in batch_df.iterrows():
            absolute_index = start_idx + index
            
            try:
                old_product_code = str(row['old_product_code']).strip()
                description = str(row['description']).strip()

                # Check for too many consecutive errors
                if consecutive_errors >= max_consecutive_errors:
                    logging.error(f"Too many consecutive errors ({consecutive_errors}). Taking a longer break...")
                    time.sleep(60)  # Take a longer break
                    consecutive_errors = 0

                found, match_type, product_data = self.odoo.search_product(old_product_code)
                
                if not found:
                    self.not_found_products.append({
                        'old_product_code': old_product_code,
                        'description': description
                    })
                    logging.info(f"Product not found: {old_product_code}")
                else:
                    logging.info(f"Product found: {old_product_code} (matched by {match_type})")

                # Reset consecutive errors on successful processing
                consecutive_errors = 0

            except (xmlrpc.client.Fault, xmlrpc.client.ProtocolError, ConnectionError) as e:
                consecutive_errors += 1
                logging.error(f"Connection error for product {old_product_code}: {str(e)}")
                
                # Save progress before retry
                self.save_checkpoint(absolute_index)
                self.save_temp_results()
                
                # Wait before retrying
                time.sleep(self.odoo.calculate_backoff(consecutive_errors))
                continue

            except Exception as e:
                consecutive_errors += 1
                logging.error(f"Error processing product {old_product_code}: {str(e)}")
                
                # Save progress
                self.save_checkpoint(absolute_index)
                self.save_temp_results()
                
                if consecutive_errors >= max_consecutive_errors:
                    logging.error("Maximum consecutive errors reached. Stopping process.")
                    raise

            # Save progress periodically
            if (index + 1) % 50 == 0:  # Increased frequency of checkpoints
                self.save_checkpoint(absolute_index)
                self.save_temp_results()
                logging.info(f"Progress saved at record {absolute_index + 1}")

    def verify_products(self):
        try:
            # Read the Excel file
            input_file = 'Data_file/import_OB4.xlsx'
            df = pd.read_excel(input_file)
            
            total_records = len(df)
            start_index = self.checkpoint_data['last_processed_index']
            
            logging.info(f"Starting verification from index {start_index}")
            logging.info(f"Total records to process: {total_records}")

            # Process in batches
            for batch_start in range(start_index, total_records, BATCH_SIZE):
                batch_end = min(batch_start + BATCH_SIZE, total_records)
                batch_df = df.iloc[batch_start:batch_end]
                
                logging.info(f"Processing batch {batch_start}-{batch_end} of {total_records}")
                
                self.process_batch(batch_df, batch_start)
                
                logging.info(f"Completed batch. Progress: {batch_end}/{total_records}")

            # Save final results
            if self.not_found_products:
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                output_file = f'not_found_products_{timestamp}.xlsx'
                df_result = pd.DataFrame(self.not_found_products)
                df_result.to_excel(output_file, index=False)
                logging.info(f"Products not found have been saved to: {output_file}")
                logging.info(f"Total products not found: {len(self.not_found_products)}")
            else:
                logging.info("All products were found in the system!")

            # Clean up temporary files
            if os.path.exists(CHECKPOINT_FILE):
                os.remove(CHECKPOINT_FILE)
            if os.path.exists(TEMP_RESULTS_FILE):
                os.remove(TEMP_RESULTS_FILE)

        except Exception as e:
            logging.error(f"An error occurred during verification: {str(e)}")
            raise

def main():
    verifier = ProductVerifier()
    if not verifier.odoo.connect():
        logging.error("Failed to connect to Odoo")
        return

    try:
        verifier.verify_products()
    except Exception as e:
        logging.error(f"Main process error: {str(e)}")

if __name__ == "__main__":
    main()