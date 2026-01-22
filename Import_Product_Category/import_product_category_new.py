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
URL = os.getenv('ODOO_URL', 'http://119.59.103.142:8069')
DB = os.getenv('ODOO_DB', 'KYLD_LIVE')
USERNAME = os.getenv('ODOO_USER', 'apichart@mogen.co.th')
PASSWORD = os.getenv('ODOO_PASSWORD', '471109538')

# ========== MULTI-COMPANY SETTING ==========
# ตั้งค่า company ID ที่ต้องการ import ถ้า None = ใช้ user's default company
ODOO_COMPANY_ID = 2  # เปลี่ยนเป็นเลขที่ต้องการ เช่น 1, 2, 3 ฯลฯ

# ========== FORCE UPDATE SETTING ==========
# ถ้า True จะอัพเดต Costing Method แม้ category มี products
FORCE_UPDATE_COSTING = True  # เปลี่ยนเป็น False ถ้าต้องการหลีกเลี่ยงการเปลี่ยน costing method

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
def get_available_companies():
    """Get list of all available companies."""
    try:
        companies = models.execute_kw(
            DB, uid, PASSWORD,
            'res.company', 'search_read',
            [[]],
            {'fields': ['id', 'name']}
        )
        return companies
    except Exception as e:
        print(f"Error fetching companies: {e}")
        return []


def get_user_company_id():
    """Get the default company of the logged-in user."""
    try:
        users = models.execute_kw(
            DB, uid, PASSWORD,
            'res.users', 'read',
            [[uid]],
            {'fields': ['company_id', 'company_ids']}
        )
        if users and users[0].get('company_id'):
            return users[0]['company_id'][0]
    except Exception as e:
        print(f"Error fetching user company: {e}")
    return False


def get_account_id(account_code, company_id=None, debug=False):
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
        {'fields': ['id', 'name'], 'limit': 1}
    )
    if res:
        if debug:
            print(f"  ✓ Found account {account_code}: {res[0]['name']} (ID: {res[0]['id']})")
        return res[0]['id']
    else:
        if debug:
            print(f"  ✗ Account {account_code} NOT found (company_id={company_id})")
        return False


def ensure_category_path(path_str, company_id=None):
    """
    Ensure PARENT chain exists. Return (parent_id, complete_name_of_parent)
    path_str: 'All / 0-FG SET / ...' (ไม่มี leaf)
    """
    if not path_str:
        return (False, None)

    parts = [p.strip() for p in path_str.replace(' / ', '/').split('/') if p.strip()]
    parent_id = False
    complete = []
    
    # สร้าง context สำหรับ company
    context = {
        'allowed_company_ids': [company_id],
        'force_company': company_id
    } if company_id else {}
    
    for p in parts:
        complete.append(p)
        comp_name = ' / '.join(complete)
        # หาโดย complete_name ก่อน (ไม่ filter company_id เพราะไม่มี field)
        domain = [['complete_name', '=', comp_name]]
        existing = models.execute_kw(
            DB, uid, PASSWORD,
            'product.category', 'search_read',
            [domain],
            {'fields': ['id'], 'limit': 1}
        )
        if existing:
            parent_id = existing[0]['id']
            continue

        # ถ้าไม่พบ ให้สร้าง level นี้ใต้ parent ปัจจุบัน (ด้วย context)
        create_vals = {
            'name': p,
            'parent_id': parent_id or False,
            'property_cost_method': 'fifo',
            'property_valuation': 'manual_periodic',
            # ตัด properties สต๊อกให้ว่าง (ไม่ลงบัญชีอัตโนมัติ)
            'property_account_income_categ_id': False,
            'property_account_expense_categ_id': False,
        }
        
        new_id = models.execute_kw(
            DB, uid, PASSWORD,
            'product.category', 'create',
            [create_vals],
            {'context': context}
        )
        print(f"Created parent: {comp_name} (company {company_id})")
        parent_id = new_id

    return (parent_id, ' / '.join(parts))


def upsert_category(leaf_name, parent_id, vals_extra, key_cache, company_id=None):
    """
    Create/Update LEAF under parent; compare then write-if-diff; cache by complete_name.
    Returns status string for tracking.
    """
    # สร้าง context สำหรับ company - ใช้ allowed_company_ids เพื่อให้ properties ถูกเซ็ตถูก company
    context = {
        'allowed_company_ids': [company_id],
        'force_company': company_id
    } if company_id else {}
    
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
        return "Cached"

    # หา leaf โดย complete_name (ไม่ filter company_id เพราะไม่มี field)
    domain = [['complete_name', '=', complete_path]]
    existing = models.execute_kw(
        DB, uid, PASSWORD,
        'product.category', 'search_read',
        [domain],
        {'fields': ['id', 'name', 'parent_id'], 'limit': 1}
    )

    # แยก base fields และ property fields
    base_vals = {
        'name': leaf_name,
        'parent_id': parent_id or False,
    }
    
    # Property fields ที่ต้องเซ็ตด้วย context (company-dependent)
    property_vals = {
        'property_cost_method': vals_extra.get('property_cost_method', 'fifo'),
        'property_valuation': vals_extra.get('property_valuation', 'manual_periodic'),
    }
    
    if vals_extra.get('income_account_id'):
        property_vals['property_account_income_categ_id'] = vals_extra['income_account_id']
    if vals_extra.get('expense_account_id'):
        property_vals['property_account_expense_categ_id'] = vals_extra['expense_account_id']

    if existing:
        cat_id = existing[0]['id']
        # Check if category has products
        has_products = models.execute_kw(
            DB, uid, PASSWORD,
            'product.product', 'search_count',
            [[['categ_id', '=', cat_id]]]
        ) > 0
        
        # อัพเดต base fields (ไม่ต้องใช้ context)
        needs_base_write = False
        if base_vals:
            current_base = models.execute_kw(
                DB, uid, PASSWORD,
                'product.category', 'read',
                [[cat_id]],
                {'fields': list(base_vals.keys())}
            )[0]
            needs_base_write = any(current_base.get(k) != v for k, v in base_vals.items())
            if needs_base_write:
                models.execute_kw(
                    DB, uid, PASSWORD,
                    'product.category', 'write',
                    [[cat_id], base_vals]
                )
        
        # อัพเดต property fields ด้วย context (company-dependent)
        # ถ้า FORCE_UPDATE_COSTING = False และมี products จะไม่อัพเดต costing method
        if has_products and not FORCE_UPDATE_COSTING:
            # ลบ costing method และ valuation ออก
            property_vals_filtered = {
                k: v for k, v in property_vals.items() 
                if k not in ['property_cost_method', 'property_valuation']
            }
        else:
            property_vals_filtered = property_vals
            
        if property_vals_filtered:
            models.execute_kw(
                DB, uid, PASSWORD,
                'product.category', 'write',
                [[cat_id], property_vals_filtered],
                {'context': context}
            )
        
        status = "Updated" if (needs_base_write or property_vals_filtered) else "No change"
        print(f"  ✓ {'Updated' if needs_base_write or property_vals_filtered else 'No change'}: {complete_path} (company {company_id})")
    else:
        # สร้างใหม่ - รวม base_vals + property_vals แล้ว create ด้วย context
        all_vals = {**base_vals, **property_vals}
        cat_id = models.execute_kw(
            DB, uid, PASSWORD,
            'product.category', 'create',
            [all_vals],
            {'context': context}
        )
        status = "Created"
        print(f"  ✓ Created: {complete_path} (company {company_id})")

    key_cache[complete_path] = cat_id
    return status


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

    # ====== MULTI-COMPANY SUPPORT ======
    # รับ company_id ผ่าน ENV: ODOO_COMPANY_ID (เช่น ODOO_COMPANY_ID=2)
    # ถ้าไม่ระบุ จะใช้ default company ของ user
    if ODOO_COMPANY_ID:
        try:
            company_id = int(ODOO_COMPANY_ID)
            print(f"[INFO] Using specified company ID: {company_id}")
        except ValueError:
            print(f"[WARNING] Invalid ODOO_COMPANY_ID '{ODOO_COMPANY_ID}', will use user's default company")
            company_id = get_user_company_id()
    else:
        company_id = get_user_company_id()
        if company_id:
            print(f"[INFO] Using user's default company ID: {company_id}")
        else:
            print(f"[WARNING] Could not determine company, will use company_id=False")
    
    # Display available companies
    companies = get_available_companies()
    if companies:
        company_list = ', '.join([f"{c['name']} (ID: {c['id']})" for c in companies])
        print(f"[INFO] Available companies: {company_list}")

    cache = {}
    created_count = 0
    updated_count = 0
    failed_count = 0
    
    for idx, c in enumerate(rows, 1):
        print(f"\n[{idx}/{len(rows)}] Processing: {c['leaf_name']}")
        
        # 1) สร้าง PARENT chain ให้ครบ (ส่ง company_id)
        parent_id = False
        if c['parent_path']:
            parent_id, _ = ensure_category_path(c['parent_path'], company_id)

        # 2) Map account code → account_id (with debug)
        income_id = get_account_id(c['income_code'], company_id, debug=True) if c['income_code'] else False
        expense_id = get_account_id(c['expense_code'], company_id, debug=True) if c['expense_code'] else False

        vals_extra = {
            'property_cost_method': c['property_cost_method'],
            'property_valuation': c['property_valuation'],
            'income_account_id': income_id or False,
            'expense_account_id': expense_id or False,
        }

        # 3) Upsert leaf (ส่ง company_id)
        try:
            result = upsert_category(c['leaf_name'], parent_id, vals_extra, cache, company_id)
            if "Created" in result or "Updated" in result:
                if "Created" in result:
                    created_count += 1
                else:
                    updated_count += 1
        except Exception as e:
            print(f"  ✗ Error processing '{c['leaf_name']}': {e}")
            failed_count += 1

    print(f"\n{'='*60}")
    print(f"Import Summary:")
    print(f"  Created: {created_count}")
    print(f"  Updated: {updated_count}")
    print(f"  Failed: {failed_count}")
    print(f"  Total: {len(rows)}")
    print(f"{'='*60}")
    print("Import completed!")


if __name__ == "__main__":
    main()