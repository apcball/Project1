import xmlrpc.client
import pandas as pd
import numpy as np
from datetime import datetime
import logging

# Odoo connection parameters
HOST = 'http://mogth.work:8069'
DB = 'MOG_SETUP'
USERNAME = 'apichart@mogen.co.th'
PASSWORD = '471109538'

# simple in-memory cache for product lookups to reduce RPC calls
_PRODUCT_CACHE = {}

# Logging setup: write import actions to a log file under Import_BOM
LOG_FILE = 'Import_BOM/import_bom_new.log'
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
)
logger = logging.getLogger('import_bom')

def read_excel_template():
    """Read and validate the Excel template"""
    try:
        # Read the Excel file
        df = pd.read_excel('Import_BOM/import_bom_BU1.xlsx')
        
        # Clean up the data
        df = df.fillna('')  # Replace NaN with empty string
        
        # Clean up column names
        df.columns = [str(col).strip() for col in df.columns]
        
        print("\nColumns found in Excel:")
        print(df.columns.tolist())
        
        return df
    except Exception as e:
        print(f"Error reading Excel file: {str(e)}")
        return None

def connect_odoo():
    """Establish connection to Odoo"""
    try:
        # Common endpoint for authentication
        common = xmlrpc.client.ServerProxy(f'{HOST}/xmlrpc/2/common')
        uid = common.authenticate(DB, USERNAME, PASSWORD, {})
        
        # Object endpoint for model operations
        models = xmlrpc.client.ServerProxy(f'{HOST}/xmlrpc/2/object')
        
        return uid, models
    except Exception as e:
        print(f"Error connecting to Odoo: {str(e)}")
        return None, None


def _read_config_from_file(path='odoo_config.json'):
    """Try to read Odoo connection from local json file (optional)."""
    try:
        import json, os
        if not os.path.exists(path):
            return
        with open(path, 'r', encoding='utf-8') as f:
            cfg = json.load(f)
        global HOST, DB, USERNAME, PASSWORD
        HOST = cfg.get('host', HOST)
        DB = cfg.get('db', DB)
        USERNAME = cfg.get('username', USERNAME)
        PASSWORD = cfg.get('password', PASSWORD)
        print(f"Loaded Odoo config from {path}")
        logger.info(f"Loaded Odoo config from {path}")
    except Exception as e:
        print(f"Could not read config file {path}: {e}")
        logger.error(f"Could not read config file {path}: {e}")


def _map_bom_type(txt):
    """Map incoming type values to Odoo mrp.bom.type ('normal'|'phantom').
    Common mapping: 'Kit' -> 'phantom' (kit components not manufactured separately).
    """
    if not isinstance(txt, str):
        return 'normal'
    t = txt.strip().lower()
    if t in ('kit', 'kits'):
        return 'phantom'
    if t in ('phantom',):
        return 'phantom'
    return 'normal'


def find_product_by_default_code(models, uid, code):
    """Return product.product record (id and fields) matching default_code, or None."""
    if not code:
        return None
    # simple in-memory cache to reduce RPC calls
    try:
        if _PRODUCT_CACHE.get(code):
            return _PRODUCT_CACHE.get(code)
    except NameError:
        pass
    try:
        domain = [['default_code', '=', str(code).strip()]]
        ids = models.execute_kw(DB, uid, PASSWORD, 'product.product', 'search', [domain], {'limit': 1})
        if ids:
            fields = ['id', 'name', 'default_code', 'product_tmpl_id', 'uom_id']
            recs = models.execute_kw(DB, uid, PASSWORD, 'product.product', 'read', [ids, fields])
            # store in cache
            try:
                _PRODUCT_CACHE[str(code).strip()] = recs[0]
            except NameError:
                pass
            logger.info(f"Found product by default_code {code} -> id={recs[0].get('id')}")
            return recs[0]
    except Exception as e:
        print(f"Error finding product {code}: {e}")
        logger.error(f"Error finding product {code}: {e}")
    return None


def find_product_by_old_code(models, uid, code):
    """Try to find a product by legacy field 'old_product_code' or 'x_old_product_code'.
    Search both product.product and product.template as a fallback.
    """
    if not code:
        return None
    key = f"old:{str(code).strip()}"
    try:
        if _PRODUCT_CACHE.get(key):
            return _PRODUCT_CACHE.get(key)
    except NameError:
        pass
    try:
        search_code = str(code).strip()
        # Try product.product first with field old_product_code
        domain = [['old_product_code', '=', search_code]]
        ids = models.execute_kw(DB, uid, PASSWORD, 'product.product', 'search', [domain], {'limit': 1})
        if ids:
            fields = ['id', 'name', 'default_code', 'product_tmpl_id', 'uom_id']
            recs = models.execute_kw(DB, uid, PASSWORD, 'product.product', 'read', [ids, fields])
            try:
                _PRODUCT_CACHE[key] = recs[0]
            except NameError:
                pass
            logger.info(f"Found product by old_product_code {code} -> id={recs[0].get('id')}")
            return recs[0]
        # If not found on product.product, try product.template using same field
        ids = models.execute_kw(DB, uid, PASSWORD, 'product.template', 'search', [domain], {'limit': 1})
        if ids:
            # get a product.product for that template
            domain2 = [['product_tmpl_id', '=', ids[0]]]
            prod_ids = models.execute_kw(DB, uid, PASSWORD, 'product.product', 'search', [domain2], {'limit': 1})
            if prod_ids:
                fields = ['id', 'name', 'default_code', 'product_tmpl_id', 'uom_id']
                recs = models.execute_kw(DB, uid, PASSWORD, 'product.product', 'read', [prod_ids, fields])
                try:
                    _PRODUCT_CACHE[key] = recs[0]
                except NameError:
                    pass
                logger.info(f"Found product via template old_product_code {code} -> id={recs[0].get('id')}")
                return recs[0]
    except Exception as e:
        print(f"Error finding product by old code {code}: {e}")
        logger.error(f"Error finding product by old code {code}: {e}")
    return None


def create_product_placeholder(models, uid, code):
    """Create a minimal product with the given default_code and return its id/record.
    This will create a product.product (and underlying template) with a simple name.
    """
    try:
        vals = {
            'name': str(code),
            'default_code': str(code),
            'type': 'product',
            'sale_ok': False,
            'purchase_ok': False,
        }
        prod_id = models.execute_kw(DB, uid, PASSWORD, 'product.product', 'create', [vals])
        rec = models.execute_kw(DB, uid, PASSWORD, 'product.product', 'read', [[prod_id], ['id', 'name', 'default_code', 'product_tmpl_id', 'uom_id']])
        print(f"Created placeholder product for {code} (id={prod_id})")
        logger.info(f"Created placeholder product for {code} (id={prod_id})")
        return rec[0]
    except Exception as e:
        print(f"Failed to create placeholder product for {code}: {e}")
        logger.error(f"Failed to create placeholder product for {code}: {e}")
        return None


def get_or_create_product(models, uid, code):
    # use cache first
    key = str(code).strip()
    try:
        if _PRODUCT_CACHE.get(key):
            return _PRODUCT_CACHE.get(key)
    except NameError:
        pass
    prod = find_product_by_default_code(models, uid, code)
    if prod:
        return prod
    # Fallback: try legacy/old product code field
    prod = find_product_by_old_code(models, uid, code)
    if prod:
        return prod
    created = create_product_placeholder(models, uid, code)
    try:
        _PRODUCT_CACHE[key] = created
    except NameError:
        pass
    return created


def get_uom_id_for_product(models, uid, product_rec):
    """Return a sensible UOM id. Prefer product.uom_id if available, else try to find 'Unit(s)'."""
    try:
        if product_rec and product_rec.get('uom_id'):
            return product_rec['uom_id'][0]
        # try common name
        ids = models.execute_kw(DB, uid, PASSWORD, 'uom.uom', 'search', [[['name', 'ilike', 'unit']]], {'limit': 1})
        if ids:
            return ids[0]
    except Exception as e:
        logger.error(f"Error finding UoM for product {product_rec and product_rec.get('default_code')}: {e}")
        pass
    return False


def find_or_create_bom(models, uid, product_rec, bom_type='normal'):
    """Find existing BOM for the product template or create a new one.
    Returns bom id.
    """
    try:
        tmpl_id = None
        if product_rec.get('product_tmpl_id'):
            tmpl_id = product_rec['product_tmpl_id'][0]
        # Prefer bom linked to product_tmpl
        domain = []
        if tmpl_id:
            domain.append(['product_tmpl_id', '=', tmpl_id])
        else:
            # fallback: try product_id link
            domain.append(['product_id', '=', product_rec['id']])
        domain.append(['type', '=', bom_type])
        ids = models.execute_kw(DB, uid, PASSWORD, 'mrp.bom', 'search', [domain], {'limit': 1})
        if ids:
            print(f"Found existing BOM (id={ids[0]}) for product {product_rec.get('default_code')}")
            logger.info(f"Found existing BOM (id={ids[0]}) for product {product_rec.get('default_code')}")
            return ids[0]

        vals = {
            'product_tmpl_id': tmpl_id,
            'product_id': False,
            'type': bom_type,
            'product_qty': 1.0,
            'code': product_rec.get('default_code') or product_rec.get('name')
        }
        bom_id = models.execute_kw(DB, uid, PASSWORD, 'mrp.bom', 'create', [vals])
        print(f"Created BOM id={bom_id} for product {product_rec.get('default_code')}")
        logger.info(f"Created BOM id={bom_id} for product {product_rec.get('default_code')}")
        return bom_id
    except Exception as e:
        print(f"Error find/create BOM for {product_rec.get('default_code')}: {e}")
        logger.error(f"Error find/create BOM for {product_rec.get('default_code')}: {e}")
        return None


def ensure_bom_line(models, uid, bom_id, component_prod, qty, uom_id=None):
    """Create or update a mrp.bom.line for given bom and component product.
    If a line for the same product exists, update its quantity.
    """
    try:
        # search existing line
        domain = [['bom_id', '=', bom_id], ['product_id', '=', component_prod['id']]]
        ids = models.execute_kw(DB, uid, PASSWORD, 'mrp.bom.line', 'search', [domain], {'limit': 1})
        vals = {
            'bom_id': bom_id,
            'product_id': component_prod['id'],
            'product_qty': float(qty) if qty not in (None, '') else 0.0,
        }
        if uom_id:
            vals['product_uom_id'] = uom_id

        if ids:
            models.execute_kw(DB, uid, PASSWORD, 'mrp.bom.line', 'write', [ids, vals])
            print(f"Updated BOM line for product {component_prod.get('default_code')} qty={vals['product_qty']}")
            logger.info(f"Updated BOM line for product {component_prod.get('default_code')} qty={vals['product_qty']}")
            return ids[0]
        else:
            line_id = models.execute_kw(DB, uid, PASSWORD, 'mrp.bom.line', 'create', [vals])
            print(f"Created BOM line id={line_id} product {component_prod.get('default_code')} qty={vals['product_qty']}")
            logger.info(f"Created BOM line id={line_id} product {component_prod.get('default_code')} qty={vals['product_qty']}")
            return line_id
    except Exception as e:
        print(f"Error creating/updating BOM line for {component_prod.get('default_code')}: {e}")
        logger.error(f"Error creating/updating BOM line for {component_prod.get('default_code')}: {e}")
        return None


def process_dataframe(models, uid, df, dry_run=True):
    """Process the DataFrame and create/update BOMs in Odoo.
    Expected columns (case-insensitive): default_code (or default_dode), component_code, type, product_qty
    """
    # Normalize column names
    cols = {c.strip().lower(): c for c in df.columns}
    def has(*names):
        for n in names:
            if n in cols:
                return True
        return False

    parent_col = None
    for candidate in ('default_code', 'default_dode', 'old_product_code'):
        if candidate in cols:
            parent_col = cols[candidate]
            break
    if not parent_col:
        raise ValueError('Could not find parent product column (default_code) in DataFrame')

    comp_col = cols.get('component_code') or cols.get('component_old_product_code')
    qty_col = cols.get('product_qty') or cols.get('product_quantity') or cols.get('product_qty')
    type_col = cols.get('type')

    # Group by parent product
    grouped = df.groupby(parent_col)
    for parent_code, group in grouped:
        parent_code = str(parent_code).strip()
        if parent_code == '' or pd.isna(parent_code):
            continue
        print(f"\nProcessing BOM for parent product: {parent_code}")
        # If dry-run, don't contact Odoo at all — just simulate
        if dry_run:
            # Determine BOM type from first non-empty type cell
            bom_type_txt = None
            if type_col:
                vals = group[type_col].dropna().astype(str).tolist()
                if vals:
                    bom_type_txt = vals[0]
            bom_type = _map_bom_type(bom_type_txt)
            print(f"Dry-run: would create/find BOM for {parent_code} type={bom_type}")
            for _, row in group.iterrows():
                comp_code = None
                if comp_col:
                    comp_code = str(row[comp_col]).strip()
                if not comp_code or comp_code == 'nan':
                    continue
                qty = 1.0
                if qty_col:
                    try:
                        qty = float(row[qty_col])
                    except Exception:
                        qty = 1.0
                print(f"  - component {comp_code} qty={qty}")
            continue

        prod = get_or_create_product(models, uid, parent_code)
        if not prod:
            print(f"Skipping parent {parent_code}: product not available")
            continue
        # Determine BOM type from first non-empty type cell
        bom_type_txt = None
        if type_col:
            vals = group[type_col].dropna().astype(str).tolist()
            if vals:
                bom_type_txt = vals[0]
        bom_type = _map_bom_type(bom_type_txt)

        if dry_run:
            print(f"Dry-run: would create/find BOM for {parent_code} type={bom_type}")
            # only simulate lines
            for _, row in group.iterrows():
                comp_code = None
                if comp_col:
                    comp_code = str(row[comp_col]).strip()
                if not comp_code or comp_code == 'nan':
                    continue
                qty = 1.0
                if qty_col:
                    try:
                        qty = float(row[qty_col])
                    except Exception:
                        qty = 1.0
                print(f"  - component {comp_code} qty={qty}")
            continue

        bom_id = find_or_create_bom(models, uid, prod, bom_type=bom_type)
        if not bom_id:
            print(f"Cannot create/find BOM for {parent_code}")
            continue

        for _, row in group.iterrows():
            comp_code = None
            if comp_col:
                comp_code = str(row[comp_col]).strip()
            if not comp_code or comp_code == 'nan':
                continue
            comp = get_or_create_product(models, uid, comp_code)
            if not comp:
                print(f"Skipping component {comp_code}: product not available")
                continue
            qty = 1.0
            if qty_col:
                try:
                    qty = float(row[qty_col])
                except Exception:
                    qty = 1.0
            uom_id = get_uom_id_for_product(models, uid, comp)
            ensure_bom_line(models, uid, bom_id, comp, qty, uom_id=uom_id)


def main(excel_path='Import_BOM/import_bom_BU1.xlsx', dry_run=True):
    _read_config_from_file()
    df = None
    try:
        df = pd.read_excel(excel_path)
    except Exception as e:
        print(f"Failed to read {excel_path}: {e}")
        return

    uid, models = connect_odoo()
    if not uid or not models:
        print("Unable to connect to Odoo. Aborting.")
        return

    process_dataframe(models, uid, df, dry_run=dry_run)


if __name__ == '__main__':
    # quick manual run: dry_run=True to inspect changes without modifying Odoo
    main(dry_run=True)

