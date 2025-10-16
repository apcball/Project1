# -*- coding: utf-8 -*-
"""
Import/Upsert Product Categories from Excel (Path style) → Odoo 17 (XML-RPC)

Excel columns (5):
Display Name | Costing Method | Inventory Valuation | Income Account | Expense Account
- Display Name: "All / 0-FG SET / ... / Leaf"
- Costing Method: e.g. "First In First Out (FIFO)" → mapped to 'fifo'
- Inventory Valuation: "Manual" → 'manual_periodic', "Automatic" → 'real_time'
- Income/Expense Account: "411000 รายได้จาก..." → extract "411000"

Author: มะนาว 🍋
"""
import os
import re
import xmlrpc.client
import openpyxl
from urllib.parse import urlparse

# ------------ Load config (ไม่พึ่ง dotenv) ------------
URL = os.getenv('ODOO_URL', 'http://mogth.work:8069')
DB = os.getenv('ODOO_DB', 'MOG_SETUP')
USERNAME = os.getenv('ODOO_USER', 'apichart@mogen.co.th')
PASSWORD = os.getenv('ODOO_PASSWORD', '471109538')

def normalize_base_url(u: str) -> str:
    # ตัดช่องว่าง/อัญประกาศ/เครื่องหมายพิเศษที่เผลอติดมา
    u = (u or '').strip().strip('"').strip("'")
    if not u:
        raise ValueError("ODOO_URL is empty")

    if not u.startswith(('http://', 'https://')):
        u = 'http://' + u  # default เป็น http

    p = urlparse(u)
    if not p.hostname:
        raise ValueError(f"Invalid ODOO_URL: {u}")

    # ถ้าไม่มีพอร์ต ให้ติดตาม scheme: http→80, https→443
    netloc = p.hostname
    if p.port:
        netloc = f"{p.hostname}:{p.port}"
    elif p.scheme == 'http':
        netloc = f"{p.hostname}:80"
    elif p.scheme == 'https':
        netloc = f"{p.hostname}:443"

    base = f"{p.scheme}://{netloc}"
    return base

BASE_URL = normalize_base_url(URL)

print(f"[DEBUG] Using Odoo base URL: {BASE_URL}")
print(f"[DEBUG] DB: {DB}, USER: {USERNAME}")

# ------------ Connect ------------
common = xmlrpc.client.ServerProxy(f'{BASE_URL}/xmlrpc/2/common', allow_none=True)
uid = common.authenticate(DB, USERNAME, PASSWORD, {})
if not uid:
    raise RuntimeError("Authentication failed. ตรวจ URL/DB/USER/PASS และสิทธิ์ผู้ใช้")

models = xmlrpc.client.ServerProxy(f'{BASE_URL}/xmlrpc/2/object', allow_none=True)

# ------------------------------------------------------------
# Mapping helpers
# ------------------------------------------------------------
def map_costing_method(text):
    """Map Excel 'Costing Method' to Odoo values."""
    if not text:
        return 'fifo'
    t = str(text).strip().lower()
    if 'fifo' in t:
        return 'fifo'
    if 'standard' in t:
        return 'standard'
    if 'average' in t or 'avco' in t or 'avg' in t:
        return 'average'
    return 'fifo'


def map_inventory_valuation(text):
    """Map Excel 'Inventory Valuation' to Odoo values."""
    if not text:
        return 'manual_periodic'
    t = str(text).strip().lower()
    if t in ('manual', 'แมนวล', 'periodic', 'manual periodic', 'manual (periodic)'):
        return 'manual_periodic'
    if 'autom' in t or 'perpetual' in t:  # automatic/automated/automation/perpetual
        return 'real_time'
    return 'manual_periodic'


def extract_account_code(cell):
    """
    รับค่าอย่าง '411000 รายได้...' หรือ '411000' แล้วดึงเลขบัญชีด้านหน้า
    คืนค่าเป็นสตริงเลขล้วน หรือ False ถ้าดึงไม่ได้
    """
    if not cell:
        return False
    s = str(cell).replace(',', ' ').strip()
    m = re.match(r'\s*(\d{3,})', s)  # เลขตั้งแต่ 3 หลักขึ้นไป
    return m.group(1) if m else False


# ------------------------------------------------------------
# XML-RPC helpers
# ------------------------------------------------------------
def get_account_id(account_code, company_id=None):
    """Find account by exact code (optional filter by company)."""
    if not account_code:
        return False
    domain = [['code', '=', str(account_code)]]
    if company_id:
        domain.append(['company_id', '=', company_id])
    res = models.execute_kw(
        DB, uid, PASSWORD,
        'account.account', 'search_read',
        [domain],
        {'fields': ['id'], 'limit': 1}
    )
    return res[0]['id'] if res else False


def ensure_category_path(path_str):
    """
    Ensure PARENT chain exists. Return (parent_id, complete_name_of_parent)
    path_str: 'All / 0-FG SET / ...' (ไม่มี leaf)
    """
    if not path_str:
        return (False, None)

    parts = [p.strip() for p in path_str.replace(' / ', '/').split('/') if p.strip()]
    parent_id = False
    complete = []
    for p in parts:
        complete.append(p)
        comp_name = ' / '.join(complete)
        # หาโดย complete_name ก่อน
        existing = models.execute_kw(
            DB, uid, PASSWORD,
            'product.category', 'search_read',
            [[['complete_name', '=', comp_name]]],
            {'fields': ['id'], 'limit': 1}
        )
        if existing:
            parent_id = existing[0]['id']
            continue

        # ถ้าไม่พบ ให้สร้าง level นี้ใต้ parent ปัจจุบัน
        new_id = models.execute_kw(
            DB, uid, PASSWORD,
            'product.category', 'create',
            [{
                'name': p,
                'parent_id': parent_id or False,
                'property_cost_method': 'fifo',
                'property_valuation': 'manual_periodic',
                # ตัด properties สต๊อกให้ว่าง (ไม่ลงบัญชีอัตโนมัติ)
                'property_stock_account_input_categ_id': False,
                'property_stock_account_output_categ_id': False,
                'property_stock_valuation_account_id': False,
                'property_stock_journal': False,
                'property_account_income_categ_id': False,
                'property_account_expense_categ_id': False,
            }]
        )
        print(f"Created parent: {comp_name}")
        parent_id = new_id

    return (parent_id, ' / '.join(parts))


def upsert_category(leaf_name, parent_id, vals_extra, key_cache):
    """
    Create/Update LEAF under parent; compare then write-if-diff; cache by complete_name.
    """
    if parent_id:
        parent_rec = models.execute_kw(
            DB, uid, PASSWORD,
            'product.category', 'read',
            [[parent_id]],
            {'fields': ['complete_name']}
        )[0]
        complete_path = f"{parent_rec['complete_name']} / {leaf_name}"
    else:
        complete_path = leaf_name

    if complete_path in key_cache:
        return key_cache[complete_path]

    # หา leaf โดย complete_name
    existing = models.execute_kw(
        DB, uid, PASSWORD,
        'product.category', 'search_read',
        [[['complete_name', '=', complete_path]]],
        {'fields': ['id', 'name', 'parent_id'], 'limit': 1}
    )

    base_vals = {
        'name': leaf_name,
        'parent_id': parent_id or False,
        'property_cost_method': vals_extra.get('property_cost_method', 'fifo'),
        'property_valuation': vals_extra.get('property_valuation', 'manual_periodic'),
        'property_stock_account_input_categ_id': False,
        'property_stock_account_output_categ_id': False,
        'property_stock_valuation_account_id': False,
        'property_stock_journal': False,
    }

    if vals_extra.get('income_account_id'):
        base_vals['property_account_income_categ_id'] = vals_extra['income_account_id']
    if vals_extra.get('expense_account_id'):
        base_vals['property_account_expense_categ_id'] = vals_extra['expense_account_id']

    if existing:
        cat_id = existing[0]['id']
        current = models.execute_kw(
            DB, uid, PASSWORD,
            'product.category', 'read',
            [[cat_id]],
            {'fields': list(base_vals.keys())}
        )[0]
        needs_write = any(current.get(k) != v for k, v in base_vals.items())
        if needs_write:
            models.execute_kw(DB, uid, PASSWORD, 'product.category', 'write', [[cat_id], base_vals])
            print(f"Updated: {complete_path}")
        else:
            print(f"No change: {complete_path}")
    else:
        cat_id = models.execute_kw(DB, uid, PASSWORD, 'product.category', 'create', [base_vals])
        print(f"Created: {complete_path}")

    key_cache[complete_path] = cat_id
    return cat_id


# ------------------------------------------------------------
# Excel Reader
# ------------------------------------------------------------
def read_excel_file(file_path):
    """Read Excel → list of dict rows ready for import."""
    try:
        print(f"Opening file: {file_path}")
        wb = openpyxl.load_workbook(file_path, data_only=True)
        sheet = wb.active
        data = []

        headers = [cell.value for cell in sheet[1]]
        print(f"Found headers: {headers}")

        def idx(name, default=None):
            try:
                return headers.index(name)
            except ValueError:
                return default

        i_path = idx('Display Name', 0)
        i_cost = idx('Costing Method', 1)
        i_valu = idx('Inventory Valuation', 2)
        i_income = idx('Income Account', 3)
        i_expense = idx('Expense Account', 4)

        for row in sheet.iter_rows(min_row=2, values_only=True):
            if not row or not any(row):
                continue

            raw_path = (str(row[i_path]).strip() if (i_path is not None and row[i_path]) else None)
            if not raw_path:
                continue

            parts = [p.strip() for p in str(raw_path).replace(' / ', '/').split('/') if p and p.strip()]
            if not parts:
                continue

            parent_path = ' / '.join(parts[:-1]) if len(parts) > 1 else None
            leaf = parts[-1]

            costing = map_costing_method(row[i_cost] if i_cost is not None else None)
            valuation = map_inventory_valuation(row[i_valu] if i_valu is not None else None)

            income_code = extract_account_code(row[i_income] if i_income is not None else None)
            expense_code = extract_account_code(row[i_expense] if i_expense is not None else None)

            data.append({
                'leaf_name': leaf,
                'parent_path': parent_path,          # e.g. "All / 0-FG SET / ชุดบันไดสำเร็จรูป / ..."
                'property_cost_method': costing,     # 'fifo' | 'standard' | 'average'
                'property_valuation': valuation,     # 'manual_periodic' | 'real_time'
                'income_code': income_code,          # e.g. '411000'
                'expense_code': expense_code,        # e.g. '512000'
            })

        return data
    except Exception as e:
        print(f"Error reading Excel file: {e}")
        return None


# ------------------------------------------------------------
# Main
# ------------------------------------------------------------
def main():
    # ปรับ path ได้ผ่าน ENV: EXCEL_PATH
    file_path = os.getenv('EXCEL_PATH', os.path.join('Import_Product_Category', 'ProductCategoryupdate.xlsx'))
    abs_path = os.path.abspath(file_path)
    print(f"Looking for file at: {abs_path}")
    if not os.path.exists(file_path):
        print(f"Error: File not found at {abs_path}")
        return

    rows = read_excel_file(file_path)
    if not rows:
        print("Failed to read Excel file or no valid data found.")
        return

    print(f"Loaded {len(rows)} rows from Excel.")

    # อ่าน company ของ user ปัจจุบัน (เพื่อช่วยแม็พบัญชีให้ถูกบริษัท)
    company_id = False
    try:
        users = models.execute_kw(DB, uid, PASSWORD, 'res.users', 'read', [[uid]], {'fields': ['company_id']})
        if users and users[0].get('company_id'):
            company_id = users[0]['company_id'][0]
    except Exception:
        pass

    cache = {}
    for c in rows:
        # 1) สร้าง PARENT chain ให้ครบ
        parent_id = False
        if c['parent_path']:
            parent_id, _ = ensure_category_path(c['parent_path'])

        # 2) Map account code → account_id
        income_id = get_account_id(c['income_code'], company_id) if c['income_code'] else False
        expense_id = get_account_id(c['expense_code'], company_id) if c['expense_code'] else False

        vals_extra = {
            'property_cost_method': c['property_cost_method'],
            'property_valuation': c['property_valuation'],
            'income_account_id': income_id or False,
            'expense_account_id': expense_id or False,
        }

        # 3) Upsert leaf
        upsert_category(c['leaf_name'], parent_id, vals_extra, cache)

    print("Import completed successfully!")


if __name__ == "__main__":
    main()