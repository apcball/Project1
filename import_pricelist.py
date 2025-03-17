#!/usr/bin/env python3
import xmlrpc.client
import ssl
import pandas as pd
import logging
from datetime import datetime
import sys
import os
from pathlib import Path

# Configure logging with Thai language support
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('pricelist_import.log', encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)

class PricelistImporter:
    def __init__(self, host, db, username, password):
        """Initialize the PricelistImporter with Odoo connection details."""
        self.host = host
        self.db = db
        self.username = username
        self.password = password
        self.common = xmlrpc.client.ServerProxy(f'{host}/xmlrpc/2/common')
        self.models = xmlrpc.client.ServerProxy(f'{host}/xmlrpc/2/object')
        self.uid = None

    def connect(self):
        """Establish connection to Odoo server."""
        try:
            self.uid = self.common.authenticate(self.db, self.username, self.password, {})
            if not self.uid:
                raise Exception("การเชื่อมต่อล้มเหลว: ไม่สามารถยืนยันตัวตนได้")
            logging.info("เชื่อมต่อกับ Odoo server สำเร็จ")
            return True
        except Exception as e:
            logging.error(f"การเชื่อมต่อล้มเหลว: {str(e)}")
            return False

    def execute_kw(self, model, method, *args, **kwargs):
        """Execute Odoo API calls with error handling."""
        try:
            return self.models.execute_kw(self.db, self.uid, self.password, model, method, *args, **kwargs)
        except Exception as e:
            logging.error(f"API call ล้มเหลว: {str(e)}")
            raise

    def get_product_id(self, default_code):
        """Get product ID from default_code (internal reference) or product_tmpl_id."""
        if pd.isna(default_code):
            return None
            
        # Try to convert to integer for product_tmpl_id
        try:
            product_id = int(default_code)
            # Check if this ID exists
            product = self.execute_kw('product.template', 'search', 
                [[('id', '=', product_id)]])
            if product:
                return product[0]
        except (ValueError, TypeError):
            # If not an integer, search by default_code
            product_ids = self.execute_kw('product.template', 'search', 
                [[('default_code', '=', str(default_code).strip())]])
            if product_ids:
                return product_ids[0]
            
            # If not found, try partial match
            product_ids = self.execute_kw('product.template', 'search', 
                [[('default_code', 'ilike', str(default_code).strip())]])
            if product_ids:
                return product_ids[0]
            
        return None

    def get_currency_id(self, currency_name):
        """Get currency ID from currency name or code."""
        if pd.isna(currency_name):
            return 165  # Default to THB
        currency_ids = self.execute_kw('res.currency', 'search', 
            [[('name', '=', str(currency_name))]])
        if currency_ids:
            return currency_ids[0]
        return 165  # Default to THB if not found

    def create_pricelist(self, name, currency_id=None):
        """Create a new pricelist."""
        try:
            # Check if pricelist already exists
            existing_ids = self.execute_kw('product.pricelist', 'search', 
                [[('name', '=', name)]])
            if existing_ids:
                logging.info(f"Pricelist {name} มีอยู่แล้ว ใช้ ID ที่มีอยู่")
                return existing_ids[0]
            
            # If currency_id is not provided or invalid, get THB currency
            if not currency_id:
                thb_currency = self.execute_kw('res.currency', 'search', 
                    [[('name', '=', 'THB')]])
                if thb_currency:
                    currency_id = thb_currency[0]
                else:
                    # If THB not found, get company currency
                    company = self.execute_kw('res.company', 'search_read', 
                        [[('id', '=', 1)]], {'fields': ['currency_id']})
                    if company:
                        currency_id = company[0]['currency_id'][0]
                    else:
                        currency_id = 165  # Default to THB ID

            vals = {
                'name': name,
                'currency_id': currency_id,
                'company_id': 1,  # Default company
            }
            
            pricelist_id = self.execute_kw('product.pricelist', 'create', [vals])
            logging.info(f"สร้าง pricelist ใหม่: {name}")
            return pricelist_id
        except Exception as e:
            logging.error(f"ไม่สามารถสร้าง pricelist {name}: {str(e)}")
            return None

    def map_applied_on(self, value):
        """Map applied_on values from Excel to Odoo values."""
        if pd.isna(value):
            return '3_global'  # Default to global
            
        mapping = {
            'Products': '1_product',
            'Product Category': '2_product_category',
            'Product Variant': '0_product_variant',
            'Global': '3_global',
            'All Products': '3_global',
            # Add the direct values as well
            '1_product': '1_product',
            '2_product_category': '2_product_category',
            '0_product_variant': '0_product_variant',
            '3_global': '3_global'
        }
        return mapping.get(str(value).strip(), '3_global')  # Default to global if not found

    def map_compute_price(self, value):
        """Map compute_price values from Excel to Odoo values."""
        if pd.isna(value):
            return 'fixed'
            
        mapping = {
            'Fixed Price': 'fixed',
            'Percentage': 'percentage',
            'Formula': 'formula',
            # Add the direct values as well
            'fixed': 'fixed',
            'percentage': 'percentage',
            'formula': 'formula'
        }
        return mapping.get(str(value).strip(), 'fixed')  # Default to fixed if not found

    def map_base(self, value):
        """Map base values from Excel to Odoo values."""
        if pd.isna(value):
            return 'list_price'
            
        mapping = {
            'Public Price': 'list_price',
            'Cost': 'standard_price',
            'Other Pricelist': 'pricelist',
            # Add the direct values as well
            'list_price': 'list_price',
            'standard_price': 'standard_price',
            'pricelist': 'pricelist'
        }
        return mapping.get(str(value).strip(), 'list_price')  # Default to list_price if not found

    def update_pricelist_item(self, pricelist_id, product_id, fixed_price, min_quantity=1, date_start=False, date_end=False, applied_on='3_global', compute_price='fixed', percentage_price=0, base='list_price'):
        """Update or create pricelist item."""
        try:
            # Map the values
            applied_on = self.map_applied_on(applied_on)
            compute_price = self.map_compute_price(compute_price)
            base = self.map_base(base)

            # Prepare search domain based on applied_on
            domain = [('pricelist_id', '=', pricelist_id)]
            if applied_on == '3_global':
                domain.append(('applied_on', '=', '3_global'))
            elif applied_on == '2_product_category':
                domain.extend([
                    ('applied_on', '=', '2_product_category'),
                    ('categ_id', '=', product_id)
                ])
            elif applied_on == '1_product':
                domain.extend([
                    ('applied_on', '=', '1_product'),
                    ('product_tmpl_id', '=', product_id)
                ])
            elif applied_on == '0_product_variant':
                domain.extend([
                    ('applied_on', '=', '0_product_variant'),
                    ('product_id', '=', product_id)
                ])

            # Add min_quantity to domain
            domain.append(('min_quantity', '=', min_quantity))

            # Search for existing item
            existing_items = self.execute_kw('product.pricelist.item', 'search', [domain])

            # Prepare values for create/write
            vals = {
                'pricelist_id': pricelist_id,
                'min_quantity': min_quantity,
                'compute_price': compute_price,
                'applied_on': applied_on,
                'base': base
            }

            # Add fields based on applied_on
            if applied_on == '1_product':
                vals['product_tmpl_id'] = product_id
            elif applied_on == '2_product_category':
                vals['categ_id'] = product_id
            elif applied_on == '0_product_variant':
                vals['product_id'] = product_id

            # Add price based on compute_price
            if compute_price == 'fixed':
                vals['fixed_price'] = fixed_price
            elif compute_price == 'percentage':
                vals['percent_price'] = percentage_price

            # Add dates if provided
            if date_start:
                vals['date_start'] = date_start
            if date_end:
                vals['date_end'] = date_end

            if existing_items:
                # Update existing item
                self.execute_kw('product.pricelist.item', 'write', [existing_items[0], vals])
                logging.info(f"อัพเดท pricelist item สำหรับสินค้า ID {product_id}")
                return existing_items[0]
            else:
                # Create new item
                item_id = self.execute_kw('product.pricelist.item', 'create', [vals])
                logging.info(f"สร้าง pricelist item สำหรับสินค้า ID {product_id}")
                return item_id

        except Exception as e:
            logging.error(f"ไม่สามารถสร้างหรืออัพเดท pricelist item: {str(e)}")
            return None

    def import_pricelist(self, excel_file):
        """Import pricelist from Excel file."""
        try:
            # Read Excel file
            df = pd.read_excel(excel_file)
            
            # Debug: Print actual columns in Excel file
            logging.info("พบคอลัมน์ในไฟล์ Excel ดังนี้:")
            for col in df.columns:
                logging.info(f"- {col}")

            # Process each pricelist
            current_pricelist = None
            current_pricelist_id = None
            
            success_count = 0
            error_count = 0

            for idx, row in df.iterrows():
                try:
                    # Skip rows with empty product codes
                    if pd.isna(row['default_code']):
                        logging.info(f"ข้ามแถวที่ {idx + 2} เนื่องจากไม่มีรหัสสินค้า")
                        continue

                    # Check if we need to create a new pricelist
                    if pd.notna(row['name']) and (current_pricelist != row['name']):
                        current_pricelist = row['name']
                        # Try to get currency_id, default to None if not found
                        try:
                            currency_id = int(row['currency_id']) if pd.notna(row.get('currency_id')) else None
                        except (ValueError, TypeError):
                            currency_id = None
                            
                        current_pricelist_id = self.create_pricelist(
                            row['name'],
                            currency_id
                        )

                    if not current_pricelist_id:
                        logging.error(f"ไม่สามารถประมวลผล pricelist {current_pricelist}")
                        error_count += 1
                        continue

                    # Get product or category ID based on applied_on
                    applied_on = row.get('applied_on', '3_global')
                    product_or_categ_id = None
                    
                    if self.map_applied_on(applied_on) == '2_product_category':
                        # Get category ID
                        if pd.notna(row.get('product_tmpl_id')):
                            try:
                                categ_ids = self.execute_kw('product.category', 'search',
                                    [[('id', '=', int(row['product_tmpl_id']))]])
                                if categ_ids:
                                    product_or_categ_id = categ_ids[0]
                                else:
                                    logging.error(f"ไม่พบหมวดหมู่สินค้า ID: {row['product_tmpl_id']}")
                                    error_count += 1
                                    continue
                            except ValueError:
                                logging.error(f"รหัสหมวดหมู่สินค้าไม่ถูกต้อง: {row['product_tmpl_id']}")
                                error_count += 1
                                continue
                    elif self.map_applied_on(applied_on) == '1_product':
                        # Try to get product by ID first
                        if pd.notna(row.get('product_tmpl_id')):
                            try:
                                product_id = int(row['product_tmpl_id'])
                                product_exists = self.execute_kw('product.template', 'search',
                                    [[('id', '=', product_id)]])
                                if product_exists:
                                    product_or_categ_id = product_id
                            except ValueError:
                                # If product_tmpl_id is not an integer, ignore and continue to next method
                                pass

                        # If product not found by ID, try default_code
                        if not product_or_categ_id and pd.notna(row.get('default_code')):
                            product_ids = self.execute_kw('product.template', 'search',
                                [[('default_code', '=', str(row['default_code']))]])
                            if product_ids:
                                product_or_categ_id = product_ids[0]
                        
                        if not product_or_categ_id:
                            logging.error(f"ไม่พบสินค้า: {row.get('default_code', row.get('product_tmpl_id', 'N/A'))}")
                            error_count += 1
                            continue
                    else:
                        # For global rules, we don't need a product ID
                        product_or_categ_id = False

                    # Handle dates
                    date_start = row.get('date_start', False)
                    date_end = row.get('end_date', False)
                    
                    # Convert dates to string format if they exist
                    if pd.notna(date_start):
                        if isinstance(date_start, str):
                            date_start = pd.to_datetime(date_start).strftime('%Y-%m-%d')
                        else:
                            date_start = date_start.strftime('%Y-%m-%d')
                    if pd.notna(date_end):
                        if isinstance(date_end, str):
                            date_end = pd.to_datetime(date_end).strftime('%Y-%m-%d')
                        else:
                            date_end = date_end.strftime('%Y-%m-%d')
                    
                    # Get additional fields with proper type conversion and null handling
                    compute_price = row.get('compute_price', 'fixed')
                    percentage_price = float(row['percentage_price']) if pd.notna(row.get('percentage_price')) else 0
                    base = row.get('base', 'list_price')
                    min_quantity = int(row['min_quantity']) if pd.notna(row.get('min_quantity')) else 1
                    fixed_price = float(row['fixed price']) if pd.notna(row.get('fixed price')) else 0.0
                    
                    if self.update_pricelist_item(
                        current_pricelist_id,
                        product_or_categ_id,
                        fixed_price,
                        min_quantity,
                        date_start,
                        date_end,
                        applied_on,
                        compute_price,
                        percentage_price,
                        base
                    ):
                        success_count += 1
                    else:
                        error_count += 1

                except Exception as e:
                    logging.error(f"ข้อผิดพลาดในการประมวลผลแถวที่ {idx + 2}: {str(e)}")
                    error_count += 1
                    continue

            logging.info(f"การนำเข้า Pricelist เสร็จสิ้น: สำเร็จ {success_count} รายการ, ผิดพลาด {error_count} รายการ")
            return True

        except Exception as e:
            logging.error(f"การนำเข้า Pricelist ล้มเหลว: {str(e)}")
            return False

def main():
    # --- ตั้งค่าการเชื่อมต่อ Odoo ---
    HOST = 'http://mogth.work:8069'
    DB = 'MOG_DEV'
    USERNAME = 'apichart@mogen.co.th'
    PASSWORD = '471109538'

    # Initialize importer
    importer = PricelistImporter(HOST, DB, USERNAME, PASSWORD)
    
    # Connect to Odoo
    if not importer.connect():
        sys.exit(1)

    # Excel file path
    excel_file = 'Data_file/import_pricelist.xlsx'
    
    # Import pricelist
    if importer.import_pricelist(excel_file):
        logging.info("กระบวนการนำเข้า Pricelist เสร็จสมบูรณ์")
    else:
        logging.error("กระบวนการนำเข้า Pricelist ล้มเหลว")

if __name__ == "__main__":
    main()