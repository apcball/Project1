import xmlrpc.client
import pandas as pd
import sys
import logging
from typing import Dict, Any

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuration
CONFIG = {
    'url': 'http://mogth.work:8069',
    'db': 'MOG_SETUP',
    'username': 'apichart@mogen.co.th',
    'password': '471109538',
    'excel_path': 'Import_Customer_Vender/vender_import_rev1.xlsx'
}

# Simple in-memory caches to reduce XML-RPC calls during large imports
payment_term_cache = {}
account_cache = {}
field_cache = {}
country_cache = {}
state_cache = {}

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

def read_excel_file(file_path: str) -> pd.DataFrame:
    """Read Excel file"""
    try:
        df = pd.read_excel(file_path)
        logger.info(f"Excel file read successfully. Columns: {df.columns.tolist()}")
        return df
    except Exception as e:
        logger.error(f"Error reading Excel file: {e}")
        sys.exit(1)


def normalize_value(val: Any) -> Any:
    if pd.isna(val):
        return None
    if isinstance(val, str):
        return val.strip()
    return val


def normalize_str(val: Any) -> Any:
    """Return a trimmed string for the value or None for NaN/None."""
    if pd.isna(val) or val is None:
        return None
    s = str(val).strip()
    return s if s != '' else None


def normalize_zip(val: Any) -> Any:
    """Ensure zip/postcode is a string without decimal part (e.g., 10240.0 -> '10240')."""
    if val is None:
        return None
    # if float like 10240.0, convert to int then str
    try:
        if isinstance(val, float):
            if val.is_integer():
                return str(int(val))
            return str(val)
        # if numeric string like '10240.0'
        s = str(val).strip()
        if '.' in s and s.replace('.', '', 1).isdigit():
            # remove trailing .0
            parts = s.split('.')
            if parts[1] == '0':
                return parts[0]
        return s
    except Exception:
        return str(val)


def find_country_id(models, db, uid, password, country_name_or_code: str):
    """Find a country by code or name and return its id, or False if not found."""
    if not country_name_or_code:
        return False
    country_name_or_code = str(country_name_or_code).strip()
    key = country_name_or_code.lower()
    if key in country_cache:
        return country_cache[key]
    # try by code first
    try:
        res = models.execute_kw(db, uid, password, 'res.country', 'search', [[('code', '=', country_name_or_code)]], {'limit': 1})
        if res:
            country_cache[key] = res[0]
            return res[0]
        # try by name
        res = models.execute_kw(db, uid, password, 'res.country', 'search', [[('name', 'ilike', country_name_or_code)]], {'limit': 1})
        if res:
            country_cache[key] = res[0]
            return res[0]
    except Exception as e:
        logger.warning(f"Country lookup failed for '{country_name_or_code}': {e}")
    country_cache[key] = False
    return False


def find_state_id(models, db, uid, password, state_name: str, country_id: int = None):
    if not state_name:
        return False
    state_name = str(state_name).strip()
    key = state_name.lower()
    if country_id:
        key = f"{key}:{country_id}"
    if key in state_cache:
        return state_cache[key]
    try:
        domain = [('name', 'ilike', state_name)]
        if country_id:
            domain.append(('country_id', '=', country_id))
        res = models.execute_kw(db, uid, password, 'res.country.state', 'search', [domain], {'limit': 1})
        if res:
            state_cache[key] = res[0]
            return res[0]
    except Exception as e:
        logger.warning(f"State lookup failed for '{state_name}': {e}")
    state_cache[key] = False
    return False


def find_payment_term(models, db, uid, password, term_name: str):
    if term_name is None:
        return False
    term_name = str(term_name).strip()
    if not term_name:
        return False
    key = term_name.lower()
    if key in payment_term_cache:
        return payment_term_cache[key]
    try:
        res = models.execute_kw(db, uid, password, 'account.payment.term', 'search', [[('name', 'ilike', term_name)]], {'limit': 1})
        if res:
            payment_term_cache[key] = res[0]
            return res[0]
    except Exception as e:
        logger.warning(f"Payment term lookup failed for '{term_name}': {e}")
    payment_term_cache[key] = False
    return False


def find_account_by_code(models, db, uid, password, code: str):
    if not code:
        return False
    code = str(code).strip()
    key = code
    if key in account_cache:
        return account_cache[key]
    try:
        res = models.execute_kw(db, uid, password, 'account.account', 'search', [[('code', '=', code)]], {'limit': 1})
        if res:
            account_cache[key] = res[0]
            return res[0]
    except Exception as e:
        logger.warning(f"Account lookup failed for code '{code}': {e}")
    account_cache[key] = False
    return False


def has_field(models, db, uid, password, model: str, field_name: str) -> bool:
    """Return True if model has field_name defined in ir.model.fields"""
    key = f"{model}:{field_name}"
    if key in field_cache:
        return field_cache[key]
    try:
        res = models.execute_kw(db, uid, password, 'ir.model.fields', 'search', [[('model', '=', model), ('name', '=', field_name)]], {'limit': 1})
        field_cache[key] = bool(res)
        return field_cache[key]
    except Exception:
        field_cache[key] = False
        return False


def find_partner_by_codes(models, db, uid, password, old_code: str = None, partner_code: str = None):
    """Find partner by old_code_partner and partner_code (ref). Return id or False.
    If old_code field doesn't exist, fall back to searching by partner_code only.
    """
    try:
        domain = []
        # require partner_code (ref) to be present for matching: treat missing partner_code as new branch
        if old_code and partner_code:
            # prefer to use old_code_partner field if exists
            if has_field(models, db, uid, password, 'res.partner', 'old_code_partner'):
                domain = [[('old_code_partner', '=', old_code), ('ref', '=', partner_code)]]
            else:
                # no old_code field, match only by ref
                domain = [[('ref', '=', partner_code)]]
        else:
            # partner_code missing -> do not match, create new partner per row
            return False

        if not domain:
            return False

        res = models.execute_kw(db, uid, password, 'res.partner', 'search', domain, {'limit': 1})
        if res:
            return res[0]
    except Exception as e:
        logger.warning(f"Partner by codes lookup failed: {e}")
    return False


def find_partner(models, db, uid, password, vat: str = None, ref: str = None, name: str = None):
    """Try to find an existing partner by VAT, reference or name (in that order). Returns id or False."""
    domain = []
    try:
        if vat:
            domain = [[('vat', '=', vat)]]
            res = models.execute_kw(db, uid, password, 'res.partner', 'search', domain, {'limit': 1})
            if res:
                return res[0]
        if ref:
            domain = [[('ref', '=', ref)]]
            res = models.execute_kw(db, uid, password, 'res.partner', 'search', domain, {'limit': 1})
            if res:
                return res[0]
        if name:
            domain = [[('name', 'ilike', name)]]
            res = models.execute_kw(db, uid, password, 'res.partner', 'search', domain, {'limit': 1})
            if res:
                return res[0]
    except Exception as e:
        logger.warning(f"Partner lookup failed: {e}")
    return False


def upsert_partner(models, db, uid, password, vals: Dict[str, Any], dry_run: bool = False):
    """Create or update partner based on vat/ref/name. Returns (id_or_None, created_bool).
    When dry_run is True, do not call create/write; only search/read and log what would happen.
    """
    vat = vals.get('vat')
    ref = vals.get('ref')
    name = vals.get('name')
    old_code = vals.get('old_code_partner')

    def norm_code(c):
        if c is None:
            return ''
        return str(c).replace(' ', '').upper()

    try:
        # 1) If old_code provided and field exists, search by old_code_partner
        if old_code and has_field(models, db, uid, password, 'res.partner', 'old_code_partner'):
            res = models.execute_kw(db, uid, password, 'res.partner', 'search', [[('old_code_partner', '=', old_code)]], {'limit': 1})
            if res:
                partner_id = res[0]
                # read existing ref to compare
                existing = models.execute_kw(db, uid, password, 'res.partner', 'read', [[partner_id], ['ref']])
                existing_ref = existing[0].get('ref') if existing else None
                if norm_code(existing_ref) == norm_code(ref):
                    if dry_run:
                        logger.info(f"DRY-RUN: Would update partner {name} (id={partner_id}) by old_code_partner match")
                        return None, False
                    models.execute_kw(db, uid, password, 'res.partner', 'write', [[partner_id], vals])
                    logger.info(f"Updated partner {name} (id={partner_id}) by old_code_partner match")
                    return partner_id, False
                else:
                    # codes differ -> create new partner (separate branch)
                    if dry_run:
                        logger.info(f"DRY-RUN: Would create partner {name} because codes differed (existing ref={existing_ref} vs incoming ref={ref})")
                        return None, True
                    new_id = models.execute_kw(db, uid, password, 'res.partner', 'create', [vals])
                    logger.info(f"Created partner {name} (id={new_id}) because codes differed (existing ref={existing_ref} vs incoming ref={ref})")
                    return new_id, True

        # 2) If partner_code(ref) present, search by ref and update
        if ref:
            res = models.execute_kw(db, uid, password, 'res.partner', 'search', [[('ref', '=', ref)]], {'limit': 1})
            if res:
                partner_id = res[0]
                if dry_run:
                    logger.info(f"DRY-RUN: Would update partner {name} (id={partner_id}) by ref match")
                    return None, False
                models.execute_kw(db, uid, password, 'res.partner', 'write', [[partner_id], vals])
                logger.info(f"Updated partner {name} (id={partner_id}) by ref match")
                return partner_id, False

        # 3) No matching codes -> create new partner
        if dry_run:
            logger.info(f"DRY-RUN: Would create partner {name} by no-match")
            return None, True
        new_id = models.execute_kw(db, uid, password, 'res.partner', 'create', [vals])
        logger.info(f"Created partner {name} (id={new_id}) by no-match")
        return new_id, True

    except Exception as e:
        logger.error(f"Failed to create/update partner {name}: {e}")
        return False, False


def process_dataframe(uid, models, db, password, df: pd.DataFrame, dry_run: bool = False, limit: int = None):
    """Process rows of dataframe and import into Odoo as partners."""
    created = 0
    updated = 0
    failed = 0
    processed = 0
    for idx, row in df.iterrows():
        # Map expected columns using the exact headers you provided
        # headers: old_code_partner, partner_code, name, branch, is_company, street, street2, city, state_id, zip, name, vat, phone, mobile, email, property_payment_term_id, customer_rank, property_account_receivable_id, property_account_payable_id
        # prefer commercial_company_name if present
        name = normalize_value(row.get('commercial_company_name') or row.get('name') or row.get('Name') or row.get('customer_name'))
        if not name:
            logger.warning(f"Skipping row {idx}: missing name")
            failed += 1
            continue

        old_code_partner = normalize_str(row.get('old_code_partner'))
        partner_code = normalize_str(row.get('partner_code'))
        branch = normalize_str(row.get('branch'))
        is_company_val = normalize_value(row.get('is_company'))
        street = normalize_value(row.get('street'))
        street2 = normalize_value(row.get('street2'))
        city = normalize_value(row.get('city'))
        state_raw = normalize_value(row.get('state_id') or row.get('state'))
        zip_code = normalize_zip(row.get('zip'))
        vat = normalize_str(row.get('vat'))
        phone = normalize_str(row.get('phone'))
        mobile = normalize_str(row.get('mobile'))
        email = normalize_value(row.get('email'))
        payment_term_raw = normalize_value(row.get('property_payment_term_id'))
        supplier_payment_term_raw = normalize_value(row.get('property_supplier_payment_term_id'))
        customer_rank = normalize_value(row.get('customer_rank'))
        partner_type = normalize_value(row.get('partner_type'))
        partner_group = normalize_value(row.get('partner_group'))
        receivable_code = normalize_str(row.get('property_account_receivable_id'))
        payable_code = normalize_str(row.get('property_account_payable_id'))

        # interpret is_company values (TRUE/FALSE) robustly
        def _to_bool(x):
            """Robust conversion to boolean for is_company values.
            Treats these as True: True, 'true', 't', 'yes', 'y', '1', 1, 1.0,
            Thai yes variants like 'ใช่', 'y', 'Y', and 'บริษัท' (company word).
            Treats these as False: False, 'false', 'f', 'no', 'n', '0', 0, 0.0,
            Thai no variants like 'ไม่ใช่'.
            Default: False when None or unrecognized.
            """
            if x is None:
                return False
            if isinstance(x, bool):
                return x
            # numeric values
            try:
                if isinstance(x, (int, float)):
                    return float(x) == 1.0
            except Exception:
                pass
            s = str(x).strip().lower()
            if not s:
                return False
            true_values = {'true', 't', 'yes', 'y', '1', 'ใช่', 'company', 'บริษัท'}
            false_values = {'false', 'f', 'no', 'n', '0', 'ไม่ใช่'}
            # accept strings like '1.0' as true
            if s.replace('.', '', 1).isdigit():
                try:
                    return float(s) == 1.0
                except Exception:
                    pass
            if s in true_values:
                return True
            if s in false_values:
                return False
            # fallback: consider single character 'y'/'n' covered; otherwise False
            return False

        is_company_bool = _to_bool(is_company_val)

        vals: Dict[str, Any] = {
            'name': name,
            'street': street,
            'street2': street2,
            'city': city,
            'zip': zip_code,
            'phone': phone,
            'mobile': mobile,
            'email': email,
            'vat': vat,
            # use partner_code in ref
            'ref': partner_code,
            'is_company': is_company_bool,
            'company_type': 'company' if is_company_bool else 'person',
        }

        # include commercial_company_name if present (Odoo community may have this custom field)
        commercial_company_name = normalize_value(row.get('commercial_company_name'))
        if commercial_company_name:
            vals['commercial_company_name'] = commercial_company_name

        # map partner_type field if provided
        if partner_type:
            vals['partner_type'] = partner_type

        # map partner_group field if provided
        if partner_group:
            vals['partner_group'] = partner_group

        # Ensure partner is recognized as a customer in Odoo 14+ by setting customer_rank
        # Map customer_rank column: 1 => Customer, 2 => Vendor
        try:
            cr_val = int(customer_rank) if customer_rank is not None else 1
        except Exception:
            cr_val = 1

        if cr_val == 1:
            vals['customer_rank'] = 1
            vals['supplier_rank'] = 0
        elif cr_val == 2:
            vals['customer_rank'] = 0
            vals['supplier_rank'] = 1
        else:
            # default to customer
            vals['customer_rank'] = 1
            vals['supplier_rank'] = 0

        # build a comment to include old_code and branch when present
        comments = []
        if old_code_partner:
            comments.append(f"old_code:{old_code_partner}")
            # map directly to partner field 'old_code_partner' if present on model
            vals['old_code_partner'] = old_code_partner
        if branch:
            comments.append(f"branch:{branch}")
        if comments:
            vals['comment'] = ' '.join(comments)

        # Country and state
        # Map branch column directly to partner field 'branch' if present
        if branch:
            vals['branch'] = branch

        country_raw = normalize_value(row.get('country') or row.get('Country') or row.get('name'))
        country_id = find_country_id(models, db, uid, password, country_raw)
        if country_id:
            vals['country_id'] = country_id
            state_id = find_state_id(models, db, uid, password, state_raw, country_id)
        else:
            state_id = find_state_id(models, db, uid, password, state_raw)
        if state_id:
            vals['state_id'] = state_id

        # Payment term (customer)
        payment_term_id = find_payment_term(models, db, uid, password, payment_term_raw)
        if payment_term_id:
            vals['property_payment_term_id'] = payment_term_id

        # Supplier payment term (vendor)
        supplier_payment_term_id = find_payment_term(models, db, uid, password, supplier_payment_term_raw)
        if supplier_payment_term_id:
            vals['property_supplier_payment_term_id'] = supplier_payment_term_id

        # Accounts (lookup by code)
        receivable_id = find_account_by_code(models, db, uid, password, receivable_code)
        if receivable_id:
            vals['property_account_receivable_id'] = receivable_id
        payable_id = find_account_by_code(models, db, uid, password, payable_code)
        if payable_id:
            vals['property_account_payable_id'] = payable_id

        # Country lookup
        country_raw = normalize_value(row.get('country') or row.get('Country') or row.get('country_code'))
        country_id = find_country_id(models, db, uid, password, country_raw)
        if country_id:
            vals['country_id'] = country_id

        # Remove None values to avoid sending them to Odoo
        vals = {k: v for k, v in vals.items() if v is not None}

        partner_id, created_flag = upsert_partner(models, db, uid, password, vals, dry_run=dry_run)
        # In dry_run mode partner_id will be None; we still count created/updated based on created_flag
        if created_flag:
            created += 1
        else:
            # if not created_flag, treat as update
            updated += 1

        processed += 1
        if limit and processed >= limit:
            logger.info(f"Reached processing limit: {limit}")
            break

    logger.info(f"Import finished: created={created}, updated={updated}, failed={failed}")
    return {'created': created, 'updated': updated, 'failed': failed}


def main():
    # allow overriding excel path via CONFIG or keep current
    excel_path = CONFIG.get('excel_path') or r'C:\Users\Ball\Documents\Git_apcball\Project1\Import_Customer_Vender\customer_import_rev1.xlsx'
    # prefer absolute path provided by user
    if not excel_path or excel_path == 'Data_file/customer_import_rev1.xlsx':
        excel_path = r'C:\Users\Ball\Documents\Git_apcball\Project1\Import_Customer_Vender\customer_import_rev1.xlsx'

    uid, models = connect_to_odoo()
    password = CONFIG['password']
    db = CONFIG['db']

    df = read_excel_file(excel_path)
    # basic normalization: lower-case columns
    df.columns = [c.strip() if isinstance(c, str) else c for c in df.columns]
    # detect --dry-run flag
    dry_run = '--dry-run' in sys.argv or '-n' in sys.argv
    # parse --limit N if provided
    limit = None
    if '--limit' in sys.argv:
        try:
            idx = sys.argv.index('--limit')
            limit = int(sys.argv[idx + 1])
        except Exception:
            limit = None

    if dry_run:
        logger.info("Running in DRY-RUN mode: no changes will be written to Odoo")
    process_dataframe(uid, models, db, password, df, dry_run=dry_run, limit=limit)


if __name__ == '__main__':
    main()

