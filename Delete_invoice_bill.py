#!/usr/bin/env python3
import xmlrpc.client
import pandas as pd
from datetime import datetime
import logging
import sys
import ssl

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('delete_invoice_log.txt'),
        logging.StreamHandler(sys.stdout)
    ]
)

# Odoo connection parameters
ODOO_CONFIG = {
    'url': 'http://mogth.work:8069',  # Replace with your Odoo URL
    'db': 'MOG_LIVE',           # Replace with your database name
    'username': 'apichart@mogen.co.th',     # Replace with your username
    'password': '471109538'      # Replace with your password
}

def connect_to_odoo():
    """Establish connection to Odoo server"""
    try:
        # Handle SSL context for secure connections
        ssl_context = ssl.create_default_context()
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE

        common = xmlrpc.client.ServerProxy(f'{ODOO_CONFIG["url"]}/xmlrpc/2/common')
        uid = common.authenticate(
            ODOO_CONFIG['db'],
            ODOO_CONFIG['username'],
            ODOO_CONFIG['password'],
            {}
        )
        models = xmlrpc.client.ServerProxy(f'{ODOO_CONFIG["url"]}/xmlrpc/2/object')
        
        return uid, models
    except Exception as e:
        logging.error(f"Failed to connect to Odoo: {str(e)}")
        raise

def read_excel_data(file_path):
    """Read document numbers and types from Excel file"""
    try:
        # Read Excel file
        df = pd.read_excel(file_path)
        
        # Log available columns
        logging.info(f"Available columns in Excel: {', '.join(df.columns)}")
        
        if 'name' not in df.columns:
            raise ValueError("Column 'name' not found in Excel file")
        if 'type' not in df.columns:
            raise ValueError("Column 'type' not found in Excel file")
        
        logging.info(f"Using columns: Document Number='name', Type='type'")
        
        # Create a list of tuples containing (document_number, type)
        documents = []
        for _, row in df.iterrows():
            doc_num = str(row['name']).strip()
            doc_type = str(row['type']).strip().lower()
            
            # Skip empty document numbers
            if pd.isna(doc_num) or doc_num == '':
                continue
                
            documents.append((doc_num, doc_type))
        
        # Separate documents by type
        invoices = [(doc, typ) for doc, typ in documents if typ == 'invoice']
        bills = [(doc, typ) for doc, typ in documents if typ == 'bill']
        
        # Log document counts
        logging.info(f"Found {len(invoices)} invoices and {len(bills)} bills to process")
        
        if not invoices and not bills:
            logging.warning("No valid documents found to process. Please check if the type column contains 'invoice' or 'bill'")
        
        return invoices, bills
    except Exception as e:
        logging.error(f"Failed to read Excel file: {str(e)}")
        raise

def delete_documents(uid, models, documents, move_type):
    """Delete documents based on document numbers and type"""
    deleted_count = 0
    failed_count = 0
    
    for doc_number, _ in documents:
        try:
            # Search for documents in account.move model with specific move_type
            domain = [
                ['name', '=', doc_number],
                ['move_type', 'in', move_type]
            ]
            
            doc_ids = models.execute_kw(
                ODOO_CONFIG['db'],
                uid,
                ODOO_CONFIG['password'],
                'account.move',
                'search',
                [domain]
            )

            if doc_ids:
                # First try to cancel the document if it's not already cancelled
                try:
                    models.execute_kw(
                        ODOO_CONFIG['db'],
                        uid,
                        ODOO_CONFIG['password'],
                        'account.move',
                        'button_draft',
                        [doc_ids]
                    )
                except Exception as e:
                    logging.warning(f"Could not set to draft state for {doc_number}: {str(e)}")

                # Then delete the document
                models.execute_kw(
                    ODOO_CONFIG['db'],
                    uid,
                    ODOO_CONFIG['password'],
                    'account.move',
                    'unlink',
                    [doc_ids]
                )
                
                deleted_count += 1
                logging.info(f"Successfully deleted {move_type} document: {doc_number}")
            else:
                failed_count += 1
                logging.warning(f"{move_type} document not found: {doc_number}")

        except Exception as e:
            failed_count += 1
            logging.error(f"Failed to delete {move_type} document {doc_number}: {str(e)}")

    return deleted_count, failed_count

def main():
    """Main execution function"""
    try:
        # Connect to Odoo
        logging.info("Connecting to Odoo...")
        uid, models = connect_to_odoo()
        
        # Read document numbers from Excel
        logging.info("Reading Excel file...")
        excel_file = "Data_file/Delete_invoice_bill.xlsx"
        invoices, bills = read_excel_data(excel_file)
        
        # Process invoices
        if invoices:
            logging.info(f"Starting deletion process for {len(invoices)} invoices...")
            invoice_deleted, invoice_failed = delete_documents(
                uid, 
                models, 
                invoices, 
                ['out_invoice', 'out_refund']
            )
            logging.info(f"""
            Invoice deletion completed:
            - Total invoices processed: {len(invoices)}
            - Successfully deleted: {invoice_deleted}
            - Failed to delete: {invoice_failed}
            """)
        
        # Process bills
        if bills:
            logging.info(f"Starting deletion process for {len(bills)} bills...")
            bills_deleted, bills_failed = delete_documents(
                uid, 
                models, 
                bills, 
                ['in_invoice', 'in_refund']
            )
            logging.info(f"""
            Bills deletion completed:
            - Total bills processed: {len(bills)}
            - Successfully deleted: {bills_deleted}
            - Failed to delete: {bills_failed}
            """)
        
        # Log final summary
        total_processed = len(invoices) + len(bills)
        total_deleted = (invoice_deleted if invoices else 0) + (bills_deleted if bills else 0)
        total_failed = (invoice_failed if invoices else 0) + (bills_failed if bills else 0)
        
        logging.info(f"""
        Final Summary:
        - Total documents processed: {total_processed}
        - Total successfully deleted: {total_deleted}
        - Total failed to delete: {total_failed}
        """)

    except Exception as e:
        logging.error(f"Process failed: {str(e)}")
        raise

if __name__ == "__main__":
    main()