#!/usr/bin/env python3
"""
Debug script to list all available payment terms in Odoo
"""

import xmlrpc.client
import sys
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuration (same as update_payment_term_vender.py)
CONFIG = {
    'url': 'http://mogth.work:8069',
    'db': 'Test_import',
    'username': 'apichart@mogen.co.th',
    'password': '471109538'
}

def connect_to_odoo():
    """Establish connection to Odoo server"""
    try:
        common = xmlrpc.client.ServerProxy(f'{CONFIG["url"]}/xmlrpc/2/common')
        uid = common.authenticate(CONFIG['db'], CONFIG['username'], CONFIG['password'], {})
        
        if not uid:
            logger.error("Authentication failed")
            sys.exit(1)
        
        logger.info(f"Authentication successful, uid = {uid}")
        models = xmlrpc.client.ServerProxy(f'{CONFIG["url"]}/xmlrpc/2/object')
        return uid, models
    
    except Exception as e:
        logger.error(f"Connection error: {e}")
        sys.exit(1)

def list_all_payment_terms(models, db, uid, password):
    """List all available payment terms in the system."""
    try:
        payment_terms = models.execute_kw(db, uid, password, 'account.payment.term', 'search_read',
                                        [[]], {'fields': ['id', 'name', 'note']})
        logger.info(f"Found {len(payment_terms)} payment terms in the system:")
        print("\n" + "="*80)
        print("AVAILABLE PAYMENT TERMS IN ODOO:")
        print("="*80)
        
        for term in payment_terms:
            print(f"ID: {term['id']:3d} | Name: '{term['name']:<30}' | Note: {term.get('note', 'N/A')}")
        
        print("="*80)
        return payment_terms
    except Exception as e:
        logger.error(f"Error listing payment terms: {e}")
        return []

def test_specific_terms(models, db, uid, password):
    """Test specific payment terms that are failing."""
    test_terms = ['0', '30', '60', 'Immediate', '0 Days', '30 Days', '60 Days']
    
    print("\n" + "="*80)
    print("TESTING SPECIFIC PAYMENT TERM SEARCHES:")
    print("="*80)
    
    for term_name in test_terms:
        try:
            payment_term_ids = models.execute_kw(db, uid, password, 'account.payment.term', 'search',
                                               [[('name', '=', term_name)]],
                                               {'limit': 1})
            if payment_term_ids:
                print(f"✓ Found '{term_name}' -> ID: {payment_term_ids[0]}")
            else:
                print(f"✗ Not found: '{term_name}'")
        except Exception as e:
            print(f"✗ Error searching for '{term_name}': {e}")

def main():
    uid, models = connect_to_odoo()
    password = CONFIG['password']
    db = CONFIG['db']

    # List all payment terms
    list_all_payment_terms(models, db, uid, password)
    
    # Test specific terms
    test_specific_terms(models, db, uid, password)

if __name__ == '__main__':
    main()