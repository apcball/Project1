import xmlrpc.client
import pandas as pd
import sys
import argparse
import os
import json
from datetime import datetime
from pathlib import Path
import logging

# Global configuration
CONFIG = {
    'server_url': 'http://mogth.work:8069',
    'database': 'Test_import',
    'username': 'apichart@mogen.co.th',
    'password': '471109538',
    'log_dir': 'Import_PO/logs',
    'data_file': 'Import_PO/Template_PO_new.xlsx',
    'dry_run': False
}

class POImporter:
    def __init__(self, config):
        self.config = config
        self.url = config['server_url']
        self.db = config['database']
        self.username = config['username']
        self.password = config['password']
        self.dry_run = config.get('dry_run', True)
        
        # Setup logging
        self.setup_logging()
        
        # Initialize Odoo connection
        self.models = None
        self.uid = None
        
    def setup_logging(self):
        """Setup logging configuration"""
        log_dir = Path(self.config['log_dir'])
        log_dir.mkdir(exist_ok=True)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_file = log_dir / f"po_import_{timestamp}.log"
        
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file),
                logging.StreamHandler(sys.stdout)
            ]
        )
        self.logger = logging.getLogger(__name__)
        
        # Create error log file for failed imports
        self.error_log_file = log_dir / f"po_errors_{timestamp}.xlsx"
        
    def connect_odoo(self):
        """Connect to Odoo server"""
        try:
            common = xmlrpc.client.ServerProxy(f'{self.url}/xmlrpc/2/common')
            self.uid = common.authenticate(self.db, self.username, self.password, {})
            
            if not self.uid:
                raise Exception("Authentication failed")
                
            self.models = xmlrpc.client.ServerProxy(f'{self.url}/xmlrpc/2/object')
            self.logger.info(f"Successfully connected to Odoo as {self.username}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to connect to Odoo: {str(e)}")
            return False
            
    def search_partner(self, old_code_partner=None, partner_code=None):
        """Search for partner by old_code_partner or partner_code
        
        Priority:
        1. Search old_code_partner value in Odoo's old_code_partner field
        2. If not found, search partner_code value in Odoo's partner_code field
        """
        # Clean input values
        old_code_partner = str(old_code_partner).strip() if pd.notna(old_code_partner) and old_code_partner else None
        partner_code = str(partner_code).strip() if pd.notna(partner_code) and partner_code else None
        
        if not old_code_partner and not partner_code:
            return None
            
        try:
            # Priority 1: Try to find using old_code_partner field
            if old_code_partner:
                domain = [('old_code_partner', '=', old_code_partner)]
                partner_ids = self.models.execute_kw(
                    self.db, self.uid, self.password,
                    'res.partner', 'search', [domain]
                )
                
                if partner_ids and len(partner_ids) > 0:
                    partner_data = self.models.execute_kw(
                        self.db, self.uid, self.password,
                        'res.partner', 'read', [partner_ids[0]],
                        {'fields': ['id', 'name', 'old_code_partner', 'partner_code']}
                    )
                    self.logger.debug(f"Found partner using old_code_partner: {old_code_partner}")
                    return partner_data[0]
            
            # Priority 2: Try to find using partner_code field
            if partner_code:
                domain = [('partner_code', '=', partner_code)]
                partner_ids = self.models.execute_kw(
                    self.db, self.uid, self.password,
                    'res.partner', 'search', [domain]
                )
                
                if partner_ids and len(partner_ids) > 0:
                    partner_data = self.models.execute_kw(
                        self.db, self.uid, self.password,
                        'res.partner', 'read', [partner_ids[0]],
                        {'fields': ['id', 'name', 'old_code_partner', 'partner_code']}
                    )
                    self.logger.debug(f"Found partner using partner_code: {partner_code}")
                    return partner_data[0]
                
        except Exception as e:
            self.logger.error(f"Error searching partner: {str(e)}")
            
        return None
        
    def search_product(self, old_product_code=None, default_code=None):
        """Search for product by old_product_code or default_code"""
        # Clean input values
        old_product_code = str(old_product_code).strip() if pd.notna(old_product_code) and old_product_code else None
        default_code = str(default_code).strip() if pd.notna(default_code) and default_code else None
        
        if not old_product_code and not default_code:
            return None
            
        try:
            # Try default_code first
            if default_code:
                domain = [('default_code', '=', default_code)]
                product_ids = self.models.execute_kw(
                    self.db, self.uid, self.password,
                    'product.product', 'search', [domain]
                )
                
                if product_ids and len(product_ids) > 0:
                    product_data = self.models.execute_kw(
                        self.db, self.uid, self.password,
                        'product.product', 'read', [product_ids[0]],
                        {'fields': ['id', 'name', 'default_code']}
                    )
                    return product_data[0]
            
            # Try old_product_code (assuming it's also stored in 'default_code' field)
            if old_product_code:
                domain = [('default_code', '=', old_product_code)]
                product_ids = self.models.execute_kw(
                    self.db, self.uid, self.password,
                    'product.product', 'search', [domain]
                )
                
                if product_ids and len(product_ids) > 0:
                    product_data = self.models.execute_kw(
                        self.db, self.uid, self.password,
                        'product.product', 'read', [product_ids[0]],
                        {'fields': ['id', 'name', 'default_code']}
                    )
                    return product_data[0]
                
        except Exception as e:
            self.logger.error(f"Error searching product: {str(e)}")
            
        return None
        
    def search_purchase_order(self, ref_name):
        """Search for existing purchase order by ref_name"""
        # Clean input value
        ref_name = str(ref_name).strip() if pd.notna(ref_name) and ref_name else None
        
        if not ref_name:
            return None
            
        try:
            # Use a more explicit domain format
            domain = [('name', '=', ref_name)]
            po_ids = self.models.execute_kw(
                self.db, self.uid, self.password,
                'purchase.order', 'search', [domain]
            )
            
            if po_ids and len(po_ids) > 0:
                return po_ids[0]
                
        except Exception as e:
            self.logger.error(f"Error searching purchase order: {str(e)}")
            
        return None
        
    def create_purchase_order_line(self, product_id, name, price_unit, fixed_discount, product_qty, date_planned):
        """Create purchase order line data"""
        line_data = {
            'product_id': product_id,
            'name': name,
            'price_unit': self.to_float(price_unit, 0.0),
            'product_qty': self.to_float(product_qty, 1.0),
            'date_planned': self.parse_date(date_planned),
            'product_uom': 1,  # Default UOM
        }

        # Discount should be float (percent)
        disc = self.to_float(fixed_discount, 0.0)
        if disc and disc != 0.0:
            line_data['discount'] = disc
            
        return line_data
        
    def process_po_group(self, group):
        """Process a group of rows for one purchase order"""
        # Get first row for header data
        first_row = group.iloc[0]
        ref_name = str(first_row.get('ref_name')).strip() if pd.notna(first_row.get('ref_name')) else None
        if not ref_name:
            return False, "Missing ref_name"
            
        # Search for existing PO
        existing_po_id = self.search_purchase_order(ref_name)
        if existing_po_id:
            self.logger.debug(f"  Found existing PO ID: {existing_po_id}")
        
        # Search for partner
        old_code = first_row.get('old_code_partner')
        partner_code = first_row.get('partner_code')
        self.logger.debug(f"  Searching for partner: old_code={old_code}, partner_code={partner_code}")
        partner = self.search_partner(
            first_row.get('old_code_partner'), 
            first_row.get('partner_code')
        )
        
        if not partner:
            return False, f"Partner not found for old_code_partner: {first_row.get('old_code_partner')}, partner_code: {first_row.get('partner_code')}"
            
        # Prepare PO data for create/update
        po_create_data = {
            'name': ref_name,
            'partner_id': partner['id'],
            'date_order': self.parse_date(first_row.get('date_order')),
            'date_planned': self.parse_date(first_row.get('date_planned')),
            'company_id': 1,  # Default company
            'currency_id': self.resolve_currency(first_row.get('currency_id')),
            'notes': str(first_row.get('notes', '')).strip() if pd.notna(first_row.get('notes')) else '',
        }
        
        # Handle picking_type_id if provided (resolve name -> id)
        # Handle picking_type_id if provided (resolve name -> id)
        picking_type_val = first_row.get('picking_type_id')
        if picking_type_val and pd.notna(picking_type_val):
            picking_type_val = str(picking_type_val).strip()
            # Use dedicated resolver for picking type
            pt_id = self.resolve_picking_type(picking_type_val)
            if pt_id:
                po_create_data['picking_type_id'] = pt_id
                self.logger.info(f"Using picking_type_id: {picking_type_val} (ID: {pt_id})")
            else:
                self.logger.debug(f"Picking type '{picking_type_val}' not found in Odoo - will use Odoo default")
            
        # Collect lines
        lines = []
        for _, row in group.iterrows():
            # Search for product
            product = self.search_product(
                row.get('old_product_code'), 
                row.get('default_code')
            )
            
            if not product:
                return False, f"Product not found for old_product_code: {row.get('old_product_code')}, default_code: {row.get('default_code')}"
                
            # Create order line
            line_data = self.create_purchase_order_line(
                product['id'],
                row.get('name', product['name']),
                row.get('price_unit'),
                row.get('fixed_discount'),
                row.get('product_qty'),
                row.get('date_planned')
            )
            if row.get('texs_id'):
                # resolve taxes (accept id or name)
                tax_val = row.get('texs_id')
                if isinstance(tax_val, (list, tuple)):
                    tax_ids = [self.resolve_m2o('account.tax', t, ['name', 'description']) for t in tax_val]
                    tax_ids = [t for t in tax_ids if t]
                else:
                    resolved = self.resolve_m2o('account.tax', tax_val, ['name', 'description'])
                    tax_ids = [resolved] if resolved else []
                if tax_ids:
                    line_data['taxes_id'] = [(6, 0, tax_ids)]
            lines.append(line_data)
        
        try:
            if self.dry_run:
                action = 'update' if existing_po_id else 'create'
                self.logger.info(f"DRY RUN: Would {action} PO {ref_name} with {len(lines)} lines")
                return True, f"DRY RUN: Would {action} PO {ref_name}"
                
            if existing_po_id:
                # PO already exists - don't update it, just skip
                # (Odoo locks most fields once PO is confirmed)
                self.logger.info(f"PO {ref_name} already exists (ID: {existing_po_id}), skipping update")
                return True, f"PO {ref_name} already exists, skipped"
            else:
                # Create new PO with all data
                po_create_data['order_line'] = [(0, 0, line) for line in lines]
                po_id = self.models.execute_kw(
                    self.db, self.uid, self.password,
                    'purchase.order', 'create', [po_create_data]
                )
                self.logger.info(f"Created PO {ref_name} with ID {po_id}")
                return True, f"Created PO {ref_name} with ID {po_id}"
                
        except Exception as e:
            error_msg = f"Error {'updating' if existing_po_id else 'creating'} PO {ref_name}: {str(e)}"
            self.logger.error(error_msg)
            return False, error_msg
        

    def get_default_currency(self):
        """Get default currency ID"""
        try:
            currency_ids = self.models.execute_kw(
                self.db, self.uid, self.password,
                'res.currency', 'search', [[('name', '=', 'THB')]]
            )
            return currency_ids[0] if currency_ids else 1
        except:
            return 1

    def parse_date(self, value):
        """Parse input value to Odoo datetime string 'YYYY-MM-DD HH:MM:SS'.

        Supports datetime, pandas Timestamp, or strings like '15/8/2024' (dayfirst).
        Returns None if value is empty or cannot be parsed.
        """
        if value is None:
            return None

        try:
            # pandas handles many formats and Excel datetimes
            dt = None
            if isinstance(value, str):
                # Try with dayfirst True to handle '15/8/2024'
                dt = pd.to_datetime(value, dayfirst=True, errors='coerce')
            else:
                dt = pd.to_datetime(value, errors='coerce')

            if pd.isna(dt):
                return None

            # Ensure python datetime
            if hasattr(dt, 'to_pydatetime'):
                py_dt = dt.to_pydatetime()
            else:
                py_dt = dt

            return py_dt.strftime('%Y-%m-%d %H:%M:%S')
        except Exception:
            return None

    def to_float(self, value, default=0.0):
        """Coerce value to float, handling pandas NaN and empty strings."""
        try:
            if pd.isna(value):
                return float(default)
        except Exception:
            pass
        try:
            return float(value)
        except Exception:
            return float(default)

    def resolve_m2o(self, model, value, search_fields=None):
        """Resolve a many2one value: accepts int id, numeric string, or search by name/code.

        Returns integer id or None.
        """
        if not value and value != 0:
            return None

        # If already an int-like, return int
        try:
            return int(value)
        except Exception:
            pass

        # Try to search by provided search_fields or common fields
        try:
            val = str(value).strip()
            fields = search_fields or ['name', 'code', 'ref']
            for f in fields:
                domain = [(f, '=', val)]
                ids = self.models.execute_kw(
                    self.db, self.uid, self.password,
                    model, 'search', [domain]
                )
                if ids:
                    return ids[0]
        except Exception as e:
            self.logger.debug(f"resolve_m2o failed for {model} value={value}: {e}")

        return None

    def resolve_currency(self, currency_value):
        """Return currency id for given input which may be int or code/name string."""
        # If already falsy, return default
        if not currency_value:
            return self.get_default_currency()

        # If it's already an int (or stringified int), return int
        try:
            return int(currency_value)
        except Exception:
            pass

        # Otherwise try to search by name/code
        try:
            val = str(currency_value).strip()
            domain = [('name', '=', val)]
            currency_ids = self.models.execute_kw(
                self.db, self.uid, self.password,
                'res.currency', 'search', [domain]
            )
            if currency_ids:
                return currency_ids[0]

            # Try by symbol/code
            domain = [('symbol', '=', val)]
            currency_ids = self.models.execute_kw(
                self.db, self.uid, self.password,
                'res.currency', 'search', [domain]
            )
            if currency_ids:
                return currency_ids[0]

        except Exception as e:
            self.logger.debug(f"resolve_currency lookup failed for {currency_value}: {e}")

        return self.get_default_currency()

    def resolve_picking_type(self, picking_type_value):
        """Resolve picking type by warehouse name.
        
        The picking_type_value is a warehouse name in Thai, often with ': Receipts' appended.
        We search in stock.warehouse by name, then get the incoming picking type for that warehouse.
        Returns the picking type ID or None if not found.
        """
        if not picking_type_value:
            return None

        # If already an int, return it directly
        try:
            return int(picking_type_value)
        except Exception:
            pass

        try:
            val = str(picking_type_value).strip()
            self.logger.debug(f"Resolving picking_type: '{val}'")
            
            # Remove ': Receipts' suffix if present to get the warehouse name
            warehouse_name = val
            if ': Receipts' in val:
                warehouse_name = val.replace(': Receipts', '').strip()
                self.logger.debug(f"  Extracted warehouse name: '{warehouse_name}'")
            
            # First, try to find the warehouse by name (exact match)
            domain = [('name', '=', warehouse_name)]
            warehouse_ids = self.models.execute_kw(
                self.db, self.uid, self.password,
                'stock.warehouse', 'search', [domain]
            )
            
            if warehouse_ids:
                warehouse_id = warehouse_ids[0]
                self.logger.debug(f"✓ Found warehouse: '{warehouse_name}' (ID: {warehouse_id})")
                
                # Get the incoming (receipt) picking type for this warehouse
                # code='incoming' and warehouse_id matches
                domain = [
                    ('code', '=', 'incoming'),
                    ('warehouse_id', '=', warehouse_id)
                ]
                pt_ids = self.models.execute_kw(
                    self.db, self.uid, self.password,
                    'stock.picking.type', 'search', [domain]
                )
                
                if pt_ids:
                    self.logger.debug(f"✓ Found picking_type ID {pt_ids[0]} for warehouse: '{warehouse_name}'")
                    return pt_ids[0]
                else:
                    self.logger.debug(f"✗ No incoming picking_type found for warehouse '{warehouse_name}'")
            else:
                self.logger.debug(f"✗ Warehouse '{warehouse_name}' not found")
            
            # Fallback: Try direct picking type search by the full value
            self.logger.debug(f"Trying fallback searches for: '{val}'")
            
            # Try by name (exact match)
            domain = [('name', '=', val)]
            pt_ids = self.models.execute_kw(
                self.db, self.uid, self.password,
                'stock.picking.type', 'search', [domain]
            )
            if pt_ids:
                self.logger.debug(f"✓ Found by picking_type name: '{val}' -> ID {pt_ids[0]}")
                return pt_ids[0]

            # Try by code (exact match)
            domain = [('code', '=', val)]
            pt_ids = self.models.execute_kw(
                self.db, self.uid, self.password,
                'stock.picking.type', 'search', [domain]
            )
            if pt_ids:
                self.logger.debug(f"✓ Found by code: '{val}' -> ID {pt_ids[0]}")
                return pt_ids[0]
            
            self.logger.debug(f"✗ Picking type not found: '{val}' (will use Odoo default)")

        except Exception as e:
            self.logger.debug(f"Error resolving picking_type '{picking_type_value}': {str(e)[:100]}")

        return None
            
    def process_excel_file(self, file_path):
        """Process Excel file and import purchase orders"""
        try:
            # Read Excel file
            self.logger.info(f"\n{'='*80}")
            self.logger.info(f"Starting import from: {file_path}")
            self.logger.info(f"Dry-run mode: {self.dry_run}")
            self.logger.info(f"{'='*80}")
            
            df = pd.read_excel(file_path)
            self.logger.info(f"✓ Excel file loaded: {len(df)} total rows")
            
            # Required columns
            required_columns = [
                'ref_name', 'date_order', 'old_code_partner', 'partner_code',
                'date_planned', 'old_product_code', 'default_code',
                'price_unit', 'fixed_discount', 'product_qty'
            ]
            
            # Check if all required columns exist
            missing_columns = [col for col in required_columns if col not in df.columns]
            if missing_columns:
                self.logger.error(f"✗ Missing required columns: {missing_columns}")
                return False
            
            self.logger.info(f"✓ All required columns found")
                
            # Group by ref_name to handle multiple lines per PO
            grouped = df.groupby('ref_name')
            total_pos = len(grouped)
            
            self.logger.info(f"✓ Found {total_pos} purchase orders to process")
            self.logger.info(f"{'-'*80}")
            
            success_count = 0
            error_count = 0
            error_data = []
            po_number = 0
            
            for ref_name, group in grouped:
                po_number += 1
                num_lines = len(group)
                self.logger.info(f"[{po_number}/{total_pos}] Processing PO: {ref_name} ({num_lines} lines)")
                
                success, message = self.process_po_group(group)
                
                if success:
                    success_count += 1
                    self.logger.info(f"  ✓ {message}")
                else:
                    error_count += 1
                    self.logger.error(f"  ✗ {message}")
                    error_data.append({
                        'ref_name': ref_name,
                        'error': message,
                        'row_data': group.to_dict('records')
                    })
            
            # Print summary
            self.logger.info(f"{'-'*80}")
            self.logger.info(f"\n{'='*80}")
            self.logger.info(f"IMPORT SUMMARY")
            self.logger.info(f"{'='*80}")
            self.logger.info(f"Total POs processed: {total_pos}")
            self.logger.info(f"✓ Successful:        {success_count}")
            self.logger.info(f"✗ Failed:            {error_count}")
            if total_pos > 0:
                self.logger.info(f"Success rate:        {(success_count/total_pos*100):.1f}%")
                    
            # Save error data to Excel if any
            if error_data:
                error_df = pd.DataFrame(error_data)
                error_df.to_excel(self.error_log_file, index=False)
                self.logger.info(f"\n! Error details saved to: {self.error_log_file}")
                
            self.logger.info(f"{'='*80}\n")
            return True
            
        except Exception as e:
            self.logger.error(f"\n✗ Error processing Excel file: {str(e)}")
            self.logger.error(f"{'='*80}\n")
            return False
            
    def run(self, file_path=None):
        """Main execution method"""
        if not file_path:
            file_path = self.config['data_file']
            
        # Connect to Odoo
        if not self.connect_odoo():
            return False
            
        # Process the Excel file
        return self.process_excel_file(file_path)

def main():
    parser = argparse.ArgumentParser(description='Import Purchase Orders to Odoo')
    parser.add_argument('--file', '-f', help='Excel file path to import')
    parser.add_argument('--dry-run', action='store_true', help='Run in dry run mode (no actual changes)')
    parser.add_argument('--no-dry-run', action='store_true', help='Disable dry run mode')
    parser.add_argument('--server', help='Odoo server URL')
    parser.add_argument('--database', help='Odoo database name')
    parser.add_argument('--username', help='Odoo username')
    parser.add_argument('--password', help='Odoo password')
    
    args = parser.parse_args()
    
    # Update config with command line arguments
    config = CONFIG.copy()
    
    if args.file:
        config['data_file'] = args.file
    if args.server:
        config['server_url'] = args.server
    if args.database:
        config['database'] = args.database
    if args.username:
        config['username'] = args.username
    if args.password:
        config['password'] = args.password
        
    # Handle dry run mode
    if args.dry_run:
        config['dry_run'] = True
    elif args.no_dry_run:
        config['dry_run'] = False
        
    # Create and run importer
    importer = POImporter(config)
    success = importer.run()
    
    if success:
        print("Import completed successfully")
        sys.exit(0)
    else:
        print("Import failed")
        sys.exit(1)

if __name__ == "__main__":
    main()