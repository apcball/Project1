#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Sale Order Import API for Odoo 17
Imports sale orders from Excel file with support for:
- Multiple partner/product lookup methods
- SO grouping by ref_name
- Dry run mode
- Comprehensive logging
- Command-line file selection
"""

import xmlrpc.client
import pandas as pd
import sys
import argparse
import os
import json
from datetime import datetime
from pathlib import Path

# Global configuration
CONFIG = {
    'server_url': 'http://mogth.work:8069',
    'database': 'Test_import',
    'username': 'apichart@mogen.co.th',
    'password': '471109538',
    'log_dir': 'Import_SO/logs',
    'data_file': 'Import_SO/Template_SO.xlsx',
    'dry_run': False
}

# Global variables for logging
error_logs = []
missing_products = []
missing_partners = []
success_count = 0
error_count = 0
processed_count = 0

# Odoo connection variables
common = None
models = None
uid = None


def log_error(so_name, row_number, error_type, error_message, row_data=None):
    """บันทึก error log"""
    error_logs.append({
        'Timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'SO Number': so_name,
        'Row Number': row_number,
        'Error Type': error_type,
        'Error Message': error_message,
        'Row Data': str(row_data) if row_data is not None else ''
    })


def log_missing_product(product_id, product_name):
    """บันทึกรายการสินค้าที่ไม่พบในระบบ"""
    if not any(p['Product ID'] == product_id for p in missing_products):
        missing_products.append({
            'Timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'Product ID': product_id,
            'Product Name': product_name
        })


def log_missing_partner(partner_code, partner_name):
    """บันทึกรายการลูกค้าที่ไม่พบในระบบ"""
    if not any(p['Partner Code'] == partner_code for p in missing_partners):
        missing_partners.append({
            'Timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'Partner Code': partner_code,
            'Partner Name': partner_name
        })


def export_logs():
    """Export error logs and missing data to Excel files"""
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    try:
        # Create logs directory if it doesn't exist
        log_dir = Path(CONFIG['log_dir'])
        log_dir.mkdir(parents=True, exist_ok=True)
        
        # Export error logs
        if error_logs:
            error_log_file = log_dir / f'import_errors_{timestamp}.xlsx'
            df_errors = pd.DataFrame(error_logs)
            df_errors.to_excel(error_log_file, index=False)
            print(f"\nError log exported to: {error_log_file}")
            print(f"Total errors logged: {len(error_logs)}")
        
        # Export missing products
        if missing_products:
            missing_products_file = log_dir / f'missing_products_{timestamp}.xlsx'
            df_missing = pd.DataFrame(missing_products)
            df_missing.to_excel(missing_products_file, index=False)
            print(f"\nMissing products exported to: {missing_products_file}")
            print(f"Total missing products: {len(missing_products)}")
        
        # Export missing partners
        if missing_partners:
            missing_partners_file = log_dir / f'missing_partners_{timestamp}.xlsx'
            df_missing = pd.DataFrame(missing_partners)
            df_missing.to_excel(missing_partners_file, index=False)
            print(f"\nMissing partners exported to: {missing_partners_file}")
            print(f"Total missing partners: {len(missing_partners)}")
        
        # Export summary
        summary_file = log_dir / f'import_summary_{timestamp}.txt'
        with open(summary_file, 'w', encoding='utf-8') as f:
            f.write(f"Sale Order Import Summary\n")
            f.write(f"========================\n")
            f.write(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"Database: {CONFIG['database']}\n")
            f.write(f"Dry Run: {CONFIG['dry_run']}\n")
            f.write(f"Total Rows Processed: {processed_count}\n")
            f.write(f"Successful: {success_count}\n")
            f.write(f"Errors: {error_count}\n")
            f.write(f"Success Rate: {(success_count/processed_count*100):.1f}%\n")
        
        print(f"\nImport summary exported to: {summary_file}")
        
    except Exception as e:
        print(f"Failed to export logs: {e}")


def parse_arguments():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description='Import Sale Orders to Odoo 17')
    parser.add_argument('--file', '-f', required=False, default=CONFIG['data_file'], help='Excel file to import')
    parser.add_argument('--dry-run', '-d', action='store_true', help='Simulate import without making changes')
    parser.add_argument('--db', help='Database name (default: Test_import)')
    parser.add_argument('--url', help='Odoo server URL (default: http://mogth.work:8069)')
    
    return parser.parse_args()


def setup_configuration(args):
    """Setup configuration from command line arguments"""
    global CONFIG
    
    if args.dry_run:
        CONFIG['dry_run'] = True
        print("DRY RUN MODE: No changes will be made to the database")
    
    if args.db:
        CONFIG['database'] = args.db
    
    if args.url:
        CONFIG['server_url'] = args.url
    
    print(f"Configuration:")
    print(f"  Server URL: {CONFIG['server_url']}")
    print(f"  Database: {CONFIG['database']}")
    print(f"  Dry Run: {CONFIG['dry_run']}")
    print(f"  Log Directory: {CONFIG['log_dir']}")


def authenticate_odoo():
    """Authenticate with Odoo server"""
    global common, models, uid
    
    try:
        print(f"\nConnecting to {CONFIG['server_url']}...")
        common = xmlrpc.client.ServerProxy(f'{CONFIG["server_url"]}/xmlrpc/2/common')
        
        print(f"Authenticating user {CONFIG['username']} on database {CONFIG['database']}...")
        uid = common.authenticate(CONFIG['database'], CONFIG['username'], CONFIG['password'], {})
        
        if not uid:
            print("Authentication failed: invalid credentials or insufficient permissions.")
            return False
        
        # Get server version to verify connection
        server_version = common.version()
        print(f"Connected to Odoo server version {server_version.get('server_version', 'unknown')}")
        print(f"Authentication successful, uid = {uid}")
        
        # Create models proxy
        models = xmlrpc.client.ServerProxy(f'{CONFIG["server_url"]}/xmlrpc/2/object')
        
        return True
        
    except ConnectionRefusedError:
        print(f"Error: Could not connect to server at {CONFIG['server_url']}. Please verify server is running and accessible.")
        return False
    except xmlrpc.client.Fault as e:
        if "database" in str(e).lower():
            print(f"Database error: The database '{CONFIG['database']}' might not exist or is not accessible.")
        else:
            print(f"XMLRPC Error: {str(e)}")
        return False
    except Exception as e:
        print("Error during connection/authentication:", str(e))
        return False


def validate_excel_file(file_path):
    """Validate Excel file exists and has required columns"""
    if not os.path.exists(file_path):
        print(f"Error: Excel file '{file_path}' not found.")
        return False
    
    try:
        df = pd.read_excel(file_path)
        print(f"DEBUG: Excel file columns found: {df.columns.tolist()}")
        
        # Check for both possible column sets
        required_columns_set1 = ['ref_name', 'date_order', 'partner_code', 'product_id', 'product_uom_qty', 'price_unit']
        required_columns_set2 = ['name', 'date_order', 'partner_id', 'product_id', 'product_uom_qty', 'price_unit']
        
        # Check which column set is being used
        if all(col in df.columns for col in required_columns_set1):
            print("DEBUG: Using template format with ref_name and partner_code columns")
            return True
        elif all(col in df.columns for col in required_columns_set2):
            print("DEBUG: Using update format with name and partner_id columns")
            return True
        else:
            missing_columns_set1 = [col for col in required_columns_set1 if col not in df.columns]
            missing_columns_set2 = [col for col in required_columns_set2 if col not in df.columns]
            print(f"Error: Missing required columns.")
            print(f"Template format missing: {', '.join(missing_columns_set1)}")
            print(f"Update format missing: {', '.join(missing_columns_set2)}")
            return False
        
        print(f"Excel file '{file_path}' validated successfully.")
        print(f"Number of rows: {len(df)}")
        print(f"Columns found: {', '.join(df.columns)}")
        
        return True
        
    except Exception as e:
        print(f"Error reading Excel file: {e}")
        return False


def format_date(date_str):
    """แปลงรูปแบบวันที่ให้ตรงกับ Odoo format"""
    if pd.isna(date_str):
        return False
    
    try:
        if isinstance(date_str, (datetime, pd.Timestamp)):
            return date_str.strftime('%Y-%m-%d')
        elif isinstance(date_str, str):
            try:
                return datetime.strptime(date_str, '%Y-%m-%d').strftime('%Y-%m-%d')
            except ValueError:
                return False
        return False
    except Exception:
        return False


def validate_number(value):
    """Validate and convert numbers to prevent XML-RPC limits"""
    try:
        if pd.isna(value):
            return 0
        
        # Convert to float first to handle both int and float
        num = float(value)
        
        # Check if number exceeds 32-bit integer limits
        if num > 2147483647 or num < -2147483648:
            # For large numbers, return a safe maximum value
            if num > 0:
                return 2147483647
            return -2147483648
        
        return num
    except:
        return 0


def truncate_string(text, max_length=500):
    """Truncate long strings to prevent XML-RPC size issues"""
    if pd.isna(text):
        return ''
    text = str(text)
    if len(text) > max_length:
        return text[:max_length]
    return text


def get_partner_by_codes(partner_code, old_code_partner, partner_name):
    """ค้นหาข้อมูลลูกค้าจาก partner_code และ old_code_partner"""
    print(f"DEBUG: get_partner_by_codes called with partner_code={partner_code}, old_code_partner={old_code_partner}, partner_name={partner_name}")
    
    if pd.isna(partner_code) and pd.isna(old_code_partner):
        print("DEBUG: Both partner_code and old_code_partner are NaN")
        return None
    
    try:
        # Priority 1: Try partner_code exact match
        if not pd.isna(partner_code):
            partner_code = str(partner_code).strip()
            print(f"DEBUG: Searching for partner with code: '{partner_code}'")
            partner_ids = models.execute_kw(
                CONFIG['database'], uid, CONFIG['password'], 'res.partner', 'search',
                [[['ref', '=', partner_code]]]
            )
            print(f"DEBUG: Found partner IDs: {partner_ids}")
            
            if partner_ids:
                partner_data = models.execute_kw(
                    CONFIG['database'], uid, CONFIG['password'], 'res.partner', 'read',
                    [partner_ids[0]],
                    {'fields': ['id', 'name', 'ref']}
                )[0]
                print(f"Found partner by code: {partner_code} -> {partner_data['name']}")
                return partner_data
            else:
                print(f"DEBUG: No partner found with code: '{partner_code}'")
        
        # Priority 2: Try old_code_partner exact match
        if not pd.isna(old_code_partner):
            old_code = str(old_code_partner).strip()
            print(f"DEBUG: Searching for partner with old code: '{old_code}'")
            # Search in ref field (partner code)
            partner_ids = models.execute_kw(
                CONFIG['database'], uid, CONFIG['password'], 'res.partner', 'search',
                [[['ref', '=', old_code]]]
            )
            print(f"DEBUG: Found partner IDs: {partner_ids}")
            
            if partner_ids:
                partner_data = models.execute_kw(
                    CONFIG['database'], uid, CONFIG['password'], 'res.partner', 'read',
                    [partner_ids[0]],
                    {'fields': ['id', 'name', 'ref']}
                )[0]
                print(f"Found partner by old code: {old_code} -> {partner_data['name']}")
                return partner_data
            else:
                print(f"DEBUG: No partner found with old code: '{old_code}'")
        
        # Priority 3: Try partner_name exact match
        if not pd.isna(partner_name):
            name = str(partner_name).strip()
            print(f"DEBUG: Searching for partner with name: '{name}'")
            partner_ids = models.execute_kw(
                CONFIG['database'], uid, CONFIG['password'], 'res.partner', 'search',
                [[['name', '=', name]]]
            )
            print(f"DEBUG: Found partner IDs by name: {partner_ids}")
            
            if partner_ids:
                partner_data = models.execute_kw(
                    CONFIG['database'], uid, CONFIG['password'], 'res.partner', 'read',
                    [partner_ids[0]],
                    {'fields': ['id', 'name', 'ref']}
                )[0]
                print(f"Found partner by name: {name} -> {partner_data['name']}")
                return partner_data
        
        # If not found, log missing partner
        print(f"DEBUG: Partner not found, logging missing partner")
        log_missing_partner(
            partner_code if not pd.isna(partner_code) else old_code_partner,
            partner_name if not pd.isna(partner_name) else 'N/A'
        )
        
        return None
        
    except Exception as e:
        print(f"Error processing partner {partner_code}/{old_code_partner}: {e}")
        return None


def get_product_by_codes(product_id, old_product_code, product_name):
    """ค้นหาข้อมูลสินค้าจาก product_id และ old_product_code"""
    if pd.isna(product_id) and pd.isna(old_product_code):
        return None
    
    try:
        # Priority 1: Try product_id (default_code) exact match
        if not pd.isna(product_id):
            product_code = str(product_id).strip()
            product_ids = models.execute_kw(
                CONFIG['database'], uid, CONFIG['password'], 'product.product', 'search',
                [[['default_code', '=', product_code]]]
            )
            
            if product_ids:
                product_data = models.execute_kw(
                    CONFIG['database'], uid, CONFIG['password'], 'product.product', 'read',
                    [product_ids[0]], 
                    {'fields': [
                        'id', 'name', 'default_code', 'list_price', 'uom_id',
                        'taxes_id', 'description_sale'
                    ]}
                )[0]
                print(f"Found product by code: {product_code} -> {product_data['name']}")
                return product_data
        
        # Priority 2: Try old_product_code in default_code field
        if not pd.isna(old_product_code):
            old_code = str(old_product_code).strip()
            # Search in default_code field
            product_ids = models.execute_kw(
                CONFIG['database'], uid, CONFIG['password'], 'product.product', 'search',
                [[['default_code', '=', old_code]]]
            )
            
            if product_ids:
                product_data = models.execute_kw(
                    CONFIG['database'], uid, CONFIG['password'], 'product.product', 'read',
                    [product_ids[0]],
                    {'fields': [
                        'id', 'name', 'default_code', 'list_price', 'uom_id',
                        'taxes_id', 'description_sale'
                    ]}
                )[0]
                print(f"Found product by old code: {old_code} -> {product_data['name']}")
                return product_data
        
        # Priority 3: Try product_name exact match
        if not pd.isna(product_name):
            name = str(product_name).strip()
            product_ids = models.execute_kw(
                CONFIG['database'], uid, CONFIG['password'], 'product.product', 'search',
                [[['name', '=', name]]]
            )
            
            if product_ids:
                product_data = models.execute_kw(
                    CONFIG['database'], uid, CONFIG['password'], 'product.product', 'read',
                    [product_ids[0]], 
                    {'fields': [
                        'id', 'name', 'default_code', 'list_price', 'uom_id',
                        'taxes_id', 'description_sale'
                    ]}
                )[0]
                print(f"Found product by name: {name} -> {product_data['name']}")
                return product_data
        
        # If product not found, log it
        log_missing_product(
            product_id if not pd.isna(product_id) else old_product_code,
            product_name if not pd.isna(product_name) else 'N/A'
        )
        
        return None
        
    except Exception as e:
        print(f"Error processing product {product_id}/{old_product_code}: {e}")
        return None


def get_warehouse_data(warehouse_name):
    """ค้นหา Warehouse จากชื่อ"""
    if pd.isna(warehouse_name):
        return None
    
    try:
        warehouse_name = str(warehouse_name).strip()
        print(f"DEBUG: Searching for warehouse with name: '{warehouse_name}'")
        print(f"DEBUG: Warehouse name length: {len(warehouse_name)}")
        print(f"DEBUG: Warehouse name repr: {repr(warehouse_name)}")
        
        # First try exact match
        print("DEBUG: Trying exact match search...")
        warehouse_ids = models.execute_kw(
            CONFIG['database'], uid, CONFIG['password'], 'stock.warehouse', 'search',
            [[['name', '=', warehouse_name]]]
        )
        print(f"DEBUG: Exact match found warehouse IDs: {warehouse_ids}")
        
        # If exact match fails, try ilike
        if not warehouse_ids:
            print("DEBUG: Exact match failed, trying ilike search...")
            warehouse_ids = models.execute_kw(
                CONFIG['database'], uid, CONFIG['password'], 'stock.warehouse', 'search',
                [[['name', 'ilike', warehouse_name]]]
            )
            print(f"DEBUG: ILIKE match found warehouse IDs: {warehouse_ids}")
        
        if warehouse_ids:
            # Get all matching warehouses to check for multiple matches
            all_warehouses = models.execute_kw(
                CONFIG['database'], uid, CONFIG['password'], 'stock.warehouse', 'read',
                warehouse_ids,
                {'fields': ['id', 'name']}
            )
            print(f"DEBUG: All matching warehouses: {all_warehouses}")
            
            # Try to find exact match among results
            for warehouse in all_warehouses:
                if warehouse['name'] == warehouse_name:
                    print(f"DEBUG: Found exact match: {warehouse}")
                    return warehouse
            
            # If no exact match, return first result
            print(f"DEBUG: No exact match found, returning first result: {all_warehouses[0]}")
            return all_warehouses[0]
        
        # If no warehouse found, try to get default warehouse
        print("DEBUG: No warehouses found, trying to get default warehouse...")
        default_warehouse_ids = models.execute_kw(
            CONFIG['database'], uid, CONFIG['password'], 'stock.warehouse', 'search',
            [[['company_id', '=', 1]], {'limit': 1}]
        )
        
        if default_warehouse_ids:
            default_warehouse = models.execute_kw(
                CONFIG['database'], uid, CONFIG['password'], 'stock.warehouse', 'read',
                [default_warehouse_ids[0]],
                {'fields': ['id', 'name']}
            )[0]
            print(f"DEBUG: Using default warehouse: {default_warehouse}")
            return default_warehouse
        
        print("DEBUG: No warehouses found at all")
        return None
    except Exception as e:
        print(f"Error processing warehouse {warehouse_name}: {e}")
        return None


def get_pricelist_data(pricelist_name):
    """ค้นหา Pricelist จากชื่อ"""
    if pd.isna(pricelist_name):
        # Try to get default THB pricelist
        default_pricelist_ids = models.execute_kw(
            CONFIG['database'], uid, CONFIG['password'], 'product.pricelist', 'search',
            [[['currency_id.name', '=', 'THB']]], {'limit': 1}
        )
        if default_pricelist_ids:
            return models.execute_kw(
                CONFIG['database'], uid, CONFIG['password'], 'product.pricelist', 'read',
                [default_pricelist_ids[0]],
                {'fields': ['id', 'name', 'currency_id']}
            )[0]
        return None
    
    try:
        pricelist_name = str(pricelist_name).strip()
        
        # Extract currency code if present in parentheses
        currency_code = None
        if '(' in pricelist_name and ')' in pricelist_name:
            currency_code = pricelist_name[pricelist_name.rfind('(')+1:pricelist_name.rfind(')')].strip()
            base_name = pricelist_name[:pricelist_name.rfind('(')].strip()
        else:
            base_name = pricelist_name
        
        # Build domain for search
        domain = []
        if currency_code:
            domain = [
                '|',
                ['name', 'ilike', pricelist_name],
                '&',
                ['name', 'ilike', base_name],
                ['currency_id.name', '=', currency_code]
            ]
        else:
            domain = [['name', 'ilike', pricelist_name]]
        
        pricelist_ids = models.execute_kw(
            CONFIG['database'], uid, CONFIG['password'], 'product.pricelist', 'search',
            [domain]
        )
        
        if pricelist_ids:
            all_pricelists = models.execute_kw(
                CONFIG['database'], uid, CONFIG['password'], 'product.pricelist', 'read',
                [pricelist_ids],
                {'fields': ['id', 'name', 'currency_id']}
            )
            
            # Try exact match first
            for pricelist in all_pricelists:
                if pricelist['name'].lower().strip() == pricelist_name.lower().strip():
                    return pricelist
            
            # Try to match by currency if specified
            if currency_code:
                for pricelist in all_pricelists:
                    if pricelist['currency_id'][1] == currency_code:
                        return pricelist
            
            # Return first match
            return all_pricelists[0]
        
        return None
    except Exception as e:
        print(f"Error processing pricelist {pricelist_name}: {e}")
        return None


def get_user_data(user_name):
    """ค้นหา Salesperson จากชื่อ"""
    if pd.isna(user_name):
        return None
    
    try:
        user_name = str(user_name).strip()
        user_ids = models.execute_kw(
            CONFIG['database'], uid, CONFIG['password'], 'res.users', 'search',
            [[['name', 'ilike', user_name]]]
        )
        
        if user_ids:
            user_data = models.execute_kw(
                CONFIG['database'], uid, CONFIG['password'], 'res.users', 'read',
                [user_ids[0]], 
                {'fields': ['id', 'name']}
            )[0]
            return user_data
        
        return None
    except Exception as e:
        print(f"Error processing user {user_name}: {e}")
        return None


def get_team_data(team_name):
    """ค้นหา Sales Team จากชื่อ"""
    if pd.isna(team_name):
        # Return default team or None if no team specified
        default_team_ids = models.execute_kw(
            CONFIG['database'], uid, CONFIG['password'], 'crm.team', 'search',
            [[['name', 'ilike', 'sales']], {'limit': 1}]
        )
        if default_team_ids:
            return models.execute_kw(
                CONFIG['database'], uid, CONFIG['password'], 'crm.team', 'read',
                [default_team_ids[0]], 
                {'fields': ['id', 'name']}
            )[0]
        return None
    
    try:
        team_name = str(team_name).strip()
        
        team_ids = models.execute_kw(
            CONFIG['database'], uid, CONFIG['password'], 'crm.team', 'search',
            [[['name', 'ilike', team_name]]]
        )
        
        if team_ids:
            all_teams = models.execute_kw(
                CONFIG['database'], uid, CONFIG['password'], 'crm.team', 'read',
                [team_ids],
                {'fields': ['id', 'name']}
            )
            
            # Try exact match first
            for team in all_teams:
                if team['name'].lower().strip() == team_name.lower().strip():
                    return team
            
            # Return first match
            return all_teams[0]
        
        return None
    except Exception as e:
        print(f"Error processing team {team_name}: {e}")
        return None


def get_tax_data(tax_name):
    """ค้นหา Tax จากชื่อ"""
    if pd.isna(tax_name):
        return []
    
    try:
        tax_name = str(tax_name).strip()
        tax_ids = models.execute_kw(
            CONFIG['database'], uid, CONFIG['password'], 'account.tax', 'search',
            [[['name', 'ilike', tax_name]]]
        )
        
        if tax_ids:
            return [(6, 0, tax_ids)]
        
        return []
    except Exception as e:
        print(f"Error processing tax {tax_name}: {e}")
        return []


def get_shipping_address(address_name, parent_id):
    """ค้นหาหรือสร้าง Shipping Address"""
    if pd.isna(address_name):
        return None
    
    try:
        address_name = str(address_name).strip()
        
        # First, try to find address with exact parent
        address_ids = models.execute_kw(
            CONFIG['database'], uid, CONFIG['password'], 'res.partner', 'search',
            [[
                ['name', 'ilike', address_name],
                ['parent_id', '=', parent_id],
                ['type', '=', 'delivery']
            ]]
        )
        
        # If not found with parent, search without parent constraint
        if not address_ids:
            address_ids = models.execute_kw(
                CONFIG['database'], uid, CONFIG['password'], 'res.partner', 'search',
                [[
                    ['name', 'ilike', address_name],
                    ['type', '=', 'delivery']
                ]]
            )
        
        if address_ids:
            all_addresses = models.execute_kw(
                CONFIG['database'], uid, CONFIG['password'], 'res.partner', 'read',
                [address_ids],
                {'fields': ['id', 'name', 'parent_id', 'type']}
            )
            
            selected_address = None
            
            # Try to find best match
            for address in all_addresses:
                if address['name'].lower() == address_name.lower():
                    if address.get('parent_id') and address['parent_id'][0] == parent_id:
                        selected_address = address
                        break
                    elif not selected_address:
                        selected_address = address
                elif not selected_address and address.get('parent_id') and address['parent_id'][0] == parent_id:
                    selected_address = address
            
            if not selected_address and all_addresses:
                selected_address = all_addresses[0]
            
            if selected_address:
                return {
                    'id': selected_address['id'],
                    'name': selected_address['name']
                }
        
        # If no matching address found, create new one
        if not CONFIG['dry_run']:
            print(f"Creating new shipping address: {address_name} for parent {parent_id}")
            address_vals = {
                'name': address_name,
                'parent_id': parent_id,
                'type': 'delivery',
                'company_type': 'person',
                'is_company': False
            }
            
            try:
                address_id = models.execute_kw(
                    CONFIG['database'], uid, CONFIG['password'], 'res.partner', 'create',
                    [address_vals]
                )
                
                if address_id:
                    return {
                        'id': address_id,
                        'name': address_name
                    }
            except Exception as create_error:
                print(f"Failed to create shipping address: {create_error}")
                return None
        else:
            # In dry run mode, return a mock address
            return {
                'id': f'dry_run_address_{parent_id}',
                'name': address_name
            }
        
        return None
    except Exception as e:
        print(f"Error processing shipping address {address_name} for parent {parent_id}: {e}")
        return None


def get_tags(tag_names):
    """Get or create tags from comma-separated string"""
    if pd.isna(tag_names):
        return []
        
    tag_ids = []
    try:
        tags = [tag.strip() for tag in str(tag_names).split(',') if tag.strip()]
        
        for tag_name in tags:
            # Search for existing tag
            tag_ids_found = models.execute_kw(
                CONFIG['database'], uid, CONFIG['password'], 'crm.tag', 'search',
                [[['name', '=', tag_name]]]
            )
            
            if tag_ids_found:
                tag_ids.append(tag_ids_found[0])
            else:
                # Create new tag if not found and not in dry run
                if not CONFIG['dry_run']:
                    tag_id = models.execute_kw(
                        CONFIG['database'], uid, CONFIG['password'], 'crm.tag', 'create',
                        [{'name': tag_name}]
                    )
                    if tag_id:
                        tag_ids.append(tag_id)
                else:
                    # In dry run mode, use mock ID
                    tag_ids.append(f'dry_run_tag_{tag_name}')
    
    except Exception as e:
        print(f"Error processing tags {tag_names}: {e}")
    
    return tag_ids


def group_rows_by_ref_name(df):
    """Group rows by ref_name for SO processing"""
    grouped = {}
    
    # Check which column name to use for grouping
    group_column = 'ref_name' if 'ref_name' in df.columns else 'name'
    print(f"DEBUG: Using column '{group_column}' for grouping SOs")
    
    for index, row in df.iterrows():
        ref_name = str(row[group_column]) if not pd.isna(row[group_column]) else f'UNNAMED_{index}'
        
        if ref_name not in grouped:
            grouped[ref_name] = []
        
        grouped[ref_name].append({
            'index': index,
            'row': row
        })
    
    return grouped


def create_sale_order(ref_name, rows):
    """Create or update Sale Order from grouped rows"""
    global success_count, error_count
    
    try:
        # Get first row for SO header information
        first_row = rows[0]['row']
        
        print(f"DEBUG: Processing SO {ref_name}")
        print(f"DEBUG: Row columns available: {first_row.index.tolist()}")
        
        # Determine which format we're using
        is_template_format = 'partner_code' in first_row.index and 'old_code_partner' in first_row.index
        is_update_format = 'partner_id' in first_row.index
        
        print(f"DEBUG: Template format: {is_template_format}, Update format: {is_update_format}")
        
        if is_template_format:
            print(f"DEBUG: partner_code value: {first_row.get('partner_code')}")
            print(f"DEBUG: old_code_partner value: {first_row.get('old_code_partner')}")
            print(f"DEBUG: partner_id value: {first_row.get('partner_id')}")
            
            # Get partner data using template format
            partner_data = get_partner_by_codes(
                first_row.get('partner_code'),
                first_row.get('old_code_partner'),
                first_row.get('partner_id')
            )
        elif is_update_format:
            print(f"DEBUG: partner_id value: {first_row.get('partner_id')}")
            
            # For update format, partner_id contains the partner name
            # We need to search by name directly
            partner_name = first_row.get('partner_id')
            if not pd.isna(partner_name):
                partner_data = get_partner_by_codes(None, None, partner_name)
            else:
                partner_data = None
        else:
            print("DEBUG: Unknown Excel format - cannot determine partner lookup method")
            partner_data = None
        
        if not partner_data:
            log_error(ref_name, rows[0]['index'] + 2, 'Partner Error', 
                     f"Partner not found: {first_row.get('partner_code')}/{first_row.get('old_code_partner')}", first_row)
            error_count += 1
            return False
        
        # Get warehouse data
        warehouse_id_value = first_row.get('warehouse_id')
        print(f"DEBUG: SO {ref_name} - warehouse_id from Excel: {repr(warehouse_id_value)}")
        warehouse_data = get_warehouse_data(warehouse_id_value)
        if not warehouse_data:
            log_error(ref_name, rows[0]['index'] + 2, 'Warehouse Error',
                     f"Warehouse not found: {warehouse_id_value}", first_row)
            error_count += 1
            return False
        else:
            print(f"DEBUG: SO {ref_name} - Selected warehouse: ID={warehouse_data['id']}, Name={repr(warehouse_data['name'])}")
        
        # Get pricelist data
        pricelist_data = get_pricelist_data(first_row.get('pricelist_id'))
        
        # Get user data (optional)
        user_data = None
        if not pd.isna(first_row.get('user_id')):
            user_data = get_user_data(first_row['user_id'])
        
        # Get team data (optional)
        team_data = get_team_data(first_row.get('team_id'))
        
        # Get shipping address data
        shipping_data = None
        if not pd.isna(first_row.get('partner_shipping_id')):
            shipping_data = get_shipping_address(first_row['partner_shipping_id'], partner_data['id'])
        
        # If no shipping data found, use partner address as shipping
        if not shipping_data:
            shipping_data = {'id': partner_data['id'], 'name': partner_data['name']}
        
        # Prepare order lines
        order_lines = []
        for row_info in rows:
            row = row_info['row']
            row_index = row_info['index']
            
            # Determine which format we're using for product lookup
            is_template_format = 'old_product_code' in row.index
            
            if is_template_format:
                # Get product data using template format
                product_data = get_product_by_codes(
                    row.get('product_id'),
                    row.get('old_product_code'),
                    row.get('product_name')
                )
            else:
                # For update format, product_id contains product code
                product_data = get_product_by_codes(
                    row.get('product_id'),
                    None,
                    row.get('product_name')
                )
            
            if not product_data:
                log_error(ref_name, row_index + 2, 'Product Error', 
                         f"Product not found: {row.get('product_id')}/{row.get('old_product_code')}", row)
                continue
            
            # Prepare order line
            order_line = {
                'product_id': product_data['id'],
                'name': truncate_string(row.get('product_name') if not pd.isna(row.get('product_name')) else product_data['name']),
                'product_uom_qty': validate_number(row.get('product_uom_qty')),
                'price_unit': validate_number(row.get('price_unit')),
                'product_uom': product_data['uom_id'][0] if product_data.get('uom_id') else 1,
                'sequence': len(order_lines) + 1,
                'discount': validate_number(row.get('discount')) if not pd.isna(row.get('discount')) else 0.0,
                'discount_fixed': validate_number(row.get('discount_fixed')) if not pd.isna(row.get('discount_fixed')) else 0.0,
                'tax_id': get_tax_data(row.get('tax_id')) if not pd.isna(row.get('tax_id')) else [],
            }
            
            order_lines.append((0, 0, order_line))
        
        if not order_lines:
            log_error(ref_name, rows[0]['index'] + 2, 'Line Error', "No valid order lines found", first_row)
            error_count += 1
            return False
        
        # Get tags data
        tag_ids = get_tags(first_row.get('tags')) if not pd.isna(first_row.get('tags')) else []
        
        # Prepare SO values
        so_vals = {
            'name': truncate_string(ref_name),
            'date_order': format_date(first_row.get('date_order')),
            'commitment_date': format_date(first_row.get('commitment_date')) if not pd.isna(first_row.get('commitment_date')) else False,
            'client_order_ref': truncate_string(first_row.get('client_order_ref')) if not pd.isna(first_row.get('client_order_ref')) else False,
            'partner_id': partner_data['id'],
            'partner_shipping_id': shipping_data['id'] if shipping_data else partner_data['id'],
            'warehouse_id': warehouse_data['id'],
            'pricelist_id': pricelist_data['id'] if pricelist_data else False,
            'user_id': user_data['id'] if user_data else False,
            'team_id': team_data['id'] if team_data else False,
            'note': truncate_string(first_row.get('note'), 1000) if not pd.isna(first_row.get('note')) else False,
            'tag_ids': [(6, 0, tag_ids)] if tag_ids else False,
            'order_line': order_lines
        }
        
        # Search for existing SO
        existing_so = models.execute_kw(
            CONFIG['database'], uid, CONFIG['password'], 'sale.order', 'search',
            [[['name', '=', ref_name]]]
        )
        
        if existing_so:
            if CONFIG['dry_run']:
                print(f"DRY RUN: Would update existing SO: {ref_name}")
                success_count += 1
                return True
            
            # Get existing order state
            so_data = models.execute_kw(
                CONFIG['database'], uid, CONFIG['password'], 'sale.order', 'read',
                [existing_so[0]], {'fields': ['state']}
            )[0]
            
            # Check if order is confirmed
            if so_data['state'] != 'draft':
                log_error(ref_name, rows[0]['index'] + 2, 'Update Error', 
                         f"Cannot update confirmed sale order (State: {so_data['state']})", first_row)
                error_count += 1
                return False
            
            # Update existing SO
            result = models.execute_kw(
                CONFIG['database'], uid, CONFIG['password'], 'sale.order', 'write',
                [existing_so[0], so_vals]
            )
            
            if result:
                print(f"Updated existing SO: {ref_name}")
                success_count += 1
                return True
            else:
                log_error(ref_name, rows[0]['index'] + 2, 'Update Error', "Failed to update existing SO", first_row)
                error_count += 1
                return False
        else:
            if CONFIG['dry_run']:
                print(f"DRY RUN: Would create new SO: {ref_name}")
                success_count += 1
                return True
            
            # Create new SO
            result = models.execute_kw(
                CONFIG['database'], uid, CONFIG['password'], 'sale.order', 'create',
                [so_vals]
            )
            
            if result:
                print(f"Created new SO: {ref_name} (ID: {result})")
                success_count += 1
                return True
            else:
                log_error(ref_name, rows[0]['index'] + 2, 'Creation Error', "Failed to create new SO", first_row)
                error_count += 1
                return False
        
    except Exception as e:
        log_error(ref_name, rows[0]['index'] + 2, 'Processing Error', str(e), rows[0]['row'])
        print(f"Failed to process Sale Order {ref_name}: {e}")
        error_count += 1
        return False


def main():
    """Main function"""
    global processed_count
    
    # Parse command line arguments
    args = parse_arguments()
    
    # Setup configuration
    setup_configuration(args)
    
    # Validate Excel file
    if not validate_excel_file(args.file):
        sys.exit(1)
    
    # Authenticate with Odoo
    if not authenticate_odoo():
        sys.exit(1)
    
    # Read Excel file
    try:
        df = pd.read_excel(args.file)
        print(f"\nProcessing Excel file: {args.file}")
        print(f"Total rows: {len(df)}")
    except Exception as e:
        print(f"Error reading Excel file: {e}")
        sys.exit(1)
    
    # Group rows by ref_name
    grouped_rows = group_rows_by_ref_name(df)
    print(f"\nFound {len(grouped_rows)} unique sale orders to process")
    
    # Process each group
    total_groups = len(grouped_rows)
    processed_groups = 0
    
    print("\nStarting import process...")
    for ref_name, rows in grouped_rows.items():
        processed_groups += 1
        processed_count += len(rows)
        
        # Show progress
        if processed_groups % 10 == 0 or processed_groups == total_groups:
            print(f"Progress: {processed_groups}/{total_groups} SOs ({(processed_groups/total_groups*100):.1f}%)")
        
        # Create/update SO
        create_sale_order(ref_name, rows)
    
    # Print summary
    print(f"\nImport completed:")
    print(f"Total SOs processed: {total_groups}")
    print(f"Total rows processed: {processed_count}")
    print(f"Successful: {success_count}")
    print(f"Errors: {error_count}")
    print(f"Success rate: {(success_count/total_groups*100):.1f}%")
    
    # Export logs
    export_logs()


if __name__ == "__main__":
    main()