#!/usr/bin/env python3
import xmlrpc.client
import pandas as pd
import sys
import argparse
import os
import json
import logging
import time
import socket
import random
from datetime import datetime
from pathlib import Path
from typing import Tuple, List, Dict, Optional

# Global configuration
CONFIG = {
    'server_url': 'http://mogth.work:8069',
    'database': 'MOG_SETUP',
    'username': 'apichart@mogen.co.th',
    'password': '471109538',
    'log_dir': 'Varify_partner/logs',
    'data_file': 'Varify_partner/Partner_Find.xlsx',
    'output_dir': 'Varify_partner/output',
    'dry_run': False,
    'batch_size': 100,
    'max_retries': 3,
    'retry_delay': 5,
    'checkpoint_file': 'Varify_partner/checkpoint.json'
}

class OdooConnection:
    """Handles connection to Odoo with retry logic and error handling"""
    
    def __init__(self, config: dict):
        self.config = config
        self.uid = None
        self.models = None
        self.common = None
        self.last_connection_attempt = 0
        self.connection_cooldown = 30
        self.max_backoff = 300
        
    def calculate_backoff(self, attempt: int) -> float:
        """Calculate exponential backoff time with jitter"""
        base_delay = min(self.config['retry_delay'] * (2 ** attempt), self.max_backoff)
        jitter = random.uniform(0, 0.1 * base_delay)  # 10% jitter
        return base_delay + jitter
    
    def should_attempt_reconnect(self) -> bool:
        """Check if enough time has passed since last connection attempt"""
        current_time = time.time()
        if current_time - self.last_connection_attempt < self.connection_cooldown:
            return False
        return True
    
    def connect(self) -> bool:
        """Establish connection to Odoo with retry logic"""
        if not self.should_attempt_reconnect():
            logging.warning("Connection attempt too soon. Waiting for cooldown...")
            return False
        
        self.last_connection_attempt = time.time()
        
        for attempt in range(self.config['max_retries']):
            try:
                # Test internet connectivity first
                socket.create_connection(("8.8.8.8", 53), timeout=3)
                
                # Initialize connection objects
                self.common = xmlrpc.client.ServerProxy(
                    f'{self.config["server_url"]}/xmlrpc/2/common', 
                    allow_none=True
                )
                
                # Test server availability
                self.common.version()
                
                # Authenticate
                self.uid = self.common.authenticate(
                    self.config['database'], 
                    self.config['username'], 
                    self.config['password'], 
                    {}
                )
                if not self.uid:
                    raise Exception("Authentication failed")
                
                # Initialize models
                self.models = xmlrpc.client.ServerProxy(
                    f'{self.config["server_url"]}/xmlrpc/2/object', 
                    allow_none=True
                )
                
                logging.info(f"Successfully connected to Odoo as user ID: {self.uid}")
                return True
                
            except socket.error as e:
                logging.error(f"Network error on attempt {attempt + 1}: {str(e)}")
            except xmlrpc.client.Fault as e:
                logging.error(f"Odoo server error on attempt {attempt + 1}: {str(e)}")
            except Exception as e:
                logging.error(f"Connection attempt {attempt + 1} failed: {str(e)}")
            
            if attempt < self.config['max_retries'] - 1:
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
                self.models.execute_kw(
                    self.config['database'], 
                    self.uid, 
                    self.config['password'], 
                    'res.partner', 
                    'check_access_rights', 
                    ['read'], 
                    {'raise_exception': False}
                )
                return True
        except Exception:
            logging.warning("Connection test failed, attempting to reconnect...")
            return self.connect()
        return False
    
    def search_partner_by_old_code(self, partner_code: str) -> Tuple[bool, dict]:
        """Search for partner by old_code_partner field first, then fallback to partner_code field"""
        max_search_retries = 3
        
        for attempt in range(max_search_retries):
            try:
                # Ensure connection is active before searching
                if not self.ensure_connected():
                    raise Exception("Failed to establish connection")
                
                # First, search by old_code_partner field
                partner = self.models.execute_kw(
                    self.config['database'],
                    self.uid,
                    self.config['password'],
                    'res.partner', 'search_read',
                    [[['old_code_partner', '=', partner_code]]],
                    {'fields': ['id', 'name', 'old_code_partner', 'partner_code']}
                )
                
                if partner:
                    logging.info(f"Partner found by old_code_partner: {partner_code} - ID: {partner[0]['id']}, Name: {partner[0]['name']}")
                    return True, partner[0]
                
                # If not found in old_code_partner, search in partner_code field
                logging.info(f"Partner not found in old_code_partner, searching in partner_code field: {partner_code}")
                partner = self.models.execute_kw(
                    self.config['database'],
                    self.uid,
                    self.config['password'],
                    'res.partner', 'search_read',
                    [[['partner_code', '=', partner_code]]],
                    {'fields': ['id', 'name', 'old_code_partner', 'partner_code']}
                )
                
                if partner:
                    logging.info(f"Partner found by partner_code: {partner_code} - ID: {partner[0]['id']}, Name: {partner[0]['name']}")
                    return True, partner[0]
                
                return False, {}
                
            except xmlrpc.client.Fault as e:
                logging.error(f"Odoo API error for partner {partner_code} (attempt {attempt + 1}): {str(e)}")
                if "session expired" in str(e).lower():
                    self.connect()
                    continue
                if attempt < max_search_retries - 1:
                    time.sleep(self.calculate_backoff(attempt))
                    continue
                raise
                
            except Exception as e:
                logging.error(f"Error searching partner {partner_code} (attempt {attempt + 1}): {str(e)}")
                if attempt < max_search_retries - 1:
                    time.sleep(self.calculate_backoff(attempt))
                    continue
                raise
        
        return False, {}

class PartnerVerifier:
    """Main class for verifying partner codes against Odoo"""
    
    def __init__(self, config: dict):
        self.config = config
        self.odoo = OdooConnection(config)
        self.checkpoint_data = self.load_checkpoint()
        self.not_found_partners = []
        
        # Setup logging
        self.setup_logging()
        
    def setup_logging(self):
        """Configure logging to both file and console"""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        log_file = os.path.join(
            self.config['log_dir'], 
            f'partner_verify_{timestamp}.log'
        )
        
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file),
                logging.StreamHandler()
            ]
        )
        
        logging.info(f"Log file created: {log_file}")
    
    def load_checkpoint(self) -> dict:
        """Load checkpoint data if exists"""
        if os.path.exists(self.config['checkpoint_file']):
            try:
                with open(self.config['checkpoint_file'], 'r') as f:
                    return json.load(f)
            except Exception as e:
                logging.error(f"Error loading checkpoint: {str(e)}")
        
        return {'last_processed_index': 0}
    
    def save_checkpoint(self, index: int):
        """Save current progress to checkpoint file"""
        try:
            with open(self.config['checkpoint_file'], 'w') as f:
                json.dump({'last_processed_index': index}, f)
        except Exception as e:
            logging.error(f"Error saving checkpoint: {str(e)}")
    
    def save_not_found_partner(self, partner_code: str):
        """Add partner to not found list"""
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        self.not_found_partners.append({
            'old_partner_code': partner_code,
            'timestamp': timestamp,
            'status': 'NOT_FOUND'
        })
    
    def export_not_found_partners(self):
        """Export not found partners to Excel file"""
        if not self.not_found_partners:
            logging.info("All partners were found in the system!")
            return
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        output_file = os.path.join(
            self.config['output_dir'],
            f'partners_not_found_{timestamp}.xlsx'
        )
        
        try:
            df = pd.DataFrame(self.not_found_partners)
            df.to_excel(output_file, index=False)
            logging.info(f"Exported {len(self.not_found_partners)} not found partners to: {output_file}")
            
            # Also create a summary
            summary_file = os.path.join(
                self.config['output_dir'],
                f'verification_summary_{timestamp}.txt'
            )
            
            with open(summary_file, 'w') as f:
                f.write(f"Partner Verification Summary\n")
                f.write(f"===========================\n")
                f.write(f"Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                f.write(f"Total partners processed: {self.checkpoint_data['last_processed_index']}\n")
                f.write(f"Partners not found: {len(self.not_found_partners)}\n")
                f.write(f"Success rate: {((self.checkpoint_data['last_processed_index'] - len(self.not_found_partners)) / self.checkpoint_data['last_processed_index'] * 100):.2f}%\n")
                f.write(f"\nNot found partners saved to: {output_file}\n")
            
            logging.info(f"Summary report saved to: {summary_file}")
            
        except Exception as e:
            logging.error(f"Error exporting not found partners: {str(e)}")
    
    def process_batch(self, batch_df: pd.DataFrame, start_idx: int):
        """Process a batch of partner codes"""
        consecutive_errors = 0
        max_consecutive_errors = 5
        
        for index, row in batch_df.iterrows():
            absolute_index = start_idx + index
            
            try:
                # Get partner code from the first column (old_partner_code)
                partner_code = str(row.iloc[0]).strip()
                
                # Skip empty or invalid codes
                if not partner_code or partner_code.lower() in ['nan', '', 'none']:
                    logging.debug(f"Skipping empty partner code at index {absolute_index}")
                    continue
                
                # Check for too many consecutive errors
                if consecutive_errors >= max_consecutive_errors:
                    logging.error(f"Too many consecutive errors ({consecutive_errors}). Taking a longer break...")
                    time.sleep(60)  # Take a longer break
                    consecutive_errors = 0
                
                # Search for partner in Odoo (first in old_code_partner, then partner_code)
                found, partner_data = self.odoo.search_partner_by_old_code(partner_code)
                
                if found:
                    logging.info(f"Partner found: {partner_code} - ID: {partner_data['id']}, Name: {partner_data['name']}")
                else:
                    self.save_not_found_partner(partner_code)
                    logging.warning(f"Partner NOT found in both old_code_partner and partner_code fields: {partner_code}")
                
                # Reset consecutive errors on successful processing
                consecutive_errors = 0
                
            except Exception as e:
                consecutive_errors += 1
                logging.error(f"Error processing partner at index {absolute_index}: {str(e)}")
                
                # Save progress before retry
                self.save_checkpoint(absolute_index)
                
                if consecutive_errors >= max_consecutive_errors:
                    logging.error("Maximum consecutive errors reached. Stopping process.")
                    raise
            
            # Save progress periodically
            if (index + 1) % 50 == 0:
                self.save_checkpoint(absolute_index)
    
    def verify_partners(self):
        """Main verification process"""
        try:
            # Read the Excel file
            if not os.path.exists(self.config['data_file']):
                logging.error(f"Data file not found: {self.config['data_file']}")
                return False
            
            df = pd.read_excel(self.config['data_file'])
            
            if df.empty:
                logging.error("Input file is empty")
                return False
            
            total_records = len(df)
            start_index = self.checkpoint_data['last_processed_index']
            
            logging.info(f"Starting verification from index {start_index}")
            logging.info(f"Total records to process: {total_records}")
            
            # Process in batches
            for batch_start in range(start_index, total_records, self.config['batch_size']):
                batch_end = min(batch_start + self.config['batch_size'], total_records)
                batch_df = df.iloc[batch_start:batch_end]
                
                logging.info(f"Processing batch {batch_start}-{batch_end} of {total_records}")
                
                if not self.config['dry_run']:
                    self.process_batch(batch_df, batch_start)
                else:
                    logging.info(f"DRY RUN: Would process batch {batch_start}-{batch_end}")
                
                logging.info(f"Completed batch. Progress: {batch_end}/{total_records}")
            
            # Export results
            if not self.config['dry_run']:
                self.export_not_found_partners()
            
            # Clean up checkpoint file
            if os.path.exists(self.config['checkpoint_file']):
                os.remove(self.config['checkpoint_file'])
            
            logging.info("Partner verification completed successfully!")
            return True
            
        except Exception as e:
            logging.error(f"An error occurred during verification: {str(e)}")
            return False

def parse_arguments():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description='Verify partner codes against Odoo database')
    
    parser.add_argument(
        '--data-file', 
        default=CONFIG['data_file'],
        help='Path to Excel file containing partner codes'
    )
    
    parser.add_argument(
        '--output-dir',
        default=CONFIG['output_dir'],
        help='Directory to save output files'
    )
    
    parser.add_argument(
        '--log-dir',
        default=CONFIG['log_dir'],
        help='Directory to save log files'
    )
    
    parser.add_argument(
        '--batch-size',
        type=int,
        default=CONFIG['batch_size'],
        help='Number of records to process in each batch'
    )
    
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Run without making actual API calls'
    )
    
    parser.add_argument(
        '--resume',
        action='store_true',
        help='Resume from last checkpoint'
    )
    
    return parser.parse_args()

def main():
    """Main execution function"""
    # Parse command line arguments
    args = parse_arguments()
    
    # Update config with command line arguments
    CONFIG.update({
        'data_file': args.data_file,
        'output_dir': args.output_dir,
        'log_dir': args.log_dir,
        'batch_size': args.batch_size,
        'dry_run': args.dry_run
    })
    
    # Create directories if they don't exist
    os.makedirs(CONFIG['log_dir'], exist_ok=True)
    os.makedirs(CONFIG['output_dir'], exist_ok=True)
    
    # Initialize verifier
    verifier = PartnerVerifier(CONFIG)
    
    # Connect to Odoo (skip in dry run)
    if not CONFIG['dry_run']:
        if not verifier.odoo.connect():
            logging.error("Failed to connect to Odoo")
            sys.exit(1)
    else:
        logging.info("DRY RUN MODE: Skipping Odoo connection")
    
    try:
        # Start verification
        success = verifier.verify_partners()
        sys.exit(0 if success else 1)
        
    except KeyboardInterrupt:
        logging.info("Process interrupted by user")
        sys.exit(1)
    except Exception as e:
        logging.error(f"Unexpected error: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()
