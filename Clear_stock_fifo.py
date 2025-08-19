#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Clear to zero AS-OF a specific date for ONE root location (no child_of).
- Odoo 17 XML-RPC
- Safe vs. domain-guard (ใช้ search_read)
- Skip KIT (phantom BOM)
- Preflight บัญชีสต็อกหมวดสินค้า: ข้ามอัตโนมัติถ้า category ยังตั้งบัญชีไม่ครบ
- Robust timeout: backoff + per-quant fallback
"""

import xmlrpc.client
import socket, time
from math import pow

# ── Connection ────────────────────────────────────────────────────────────────
HOST = 'http://119.59.124.100:8069'
DB = 'MOG_LIVE_15_08'
USERNAME = 'apichart@mogen.co.th'
PASSWORD = '471109538'

# ── Settings ──────────────────────────────────────────────────────────────────
ROOT_LOCATION_ID   = 253                    # FG10/Stock
CLEAR_DATE         = '2025-01-31 23:00:00'  # วันที่ต้องการให้สต็อก = 0
DRY_RUN            = False                  # False = ปรับจริง, True = พรีวิว
BATCH_SIZE_READ    = 200
BATCH_SIZE_APPLY   = 10
APPLY_LIMIT        = 50                     # ตั้ง None = เอาทั้งหมด
COMPANY_ID         = None
TZ                 = 'Asia/Bangkok'

# ── RPC / retries ────────────────────────────────────────────────────────────
TIMEOUT        = 300
MAX_RETRIES    = 7
BACKOFF_BASE_S = 3

class Rpc:
    def __init__(self):
        self.common = None
        self.models = None
        self.uid = None
        self.ctx = {}

    def connect(self):
        print("🔌 Connecting to Odoo ...")
        socket.setdefaulttimeout(TIMEOUT)
        tr = xmlrpc.client.Transport()
        self.common = xmlrpc.client.ServerProxy(f'{HOST}/xmlrpc/2/common', transport=tr, allow_none=True)
        self.models = xmlrpc.client.ServerProxy(f'{HOST}/xmlrpc/2/object', transport=tr, allow_none=True)
        self.uid = self.common.authenticate(DB, USERNAME, PASSWORD, {})
        if not self.uid:
            raise RuntimeError("Authentication failed")
        print(f"✅ Connected as uid={self.uid}")

    def set_context(self, company_id=None, tz=TZ, lang="en_US"):
        self.ctx = {'tz': tz, 'lang': lang}
        if company_id:
            self.ctx.update({'company_id': company_id, 'force_company': company_id})

    def call(self, model, method, *args, **kw):
        kw = kw or {}
        ctx_extra = kw.pop('context', {}) or {}
        ctx = dict(self.ctx); ctx.update(ctx_extra)
        kw['context'] = ctx
        args = list(args)
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                return self.models.execute_kw(DB, self.uid, PASSWORD, model, method, args, kw)
            except xmlrpc.client.Fault:
                raise
            except (socket.timeout, socket.error, xmlrpc.client.ProtocolError) as e:
                if attempt >= MAX_RETRIES:
                    raise
                delay = BACKOFF_BASE_S * pow(2, attempt-1)
                print(f"⚠️ RPC transport error: {e}. Retry {attempt}/{MAX_RETRIES} after {int(delay)}s...")
                time.sleep(delay)

rpc = Rpc()
rpc.connect()
rpc.set_context(company_id=COMPANY_ID, tz=TZ)
print("🏢 Using user's default company (no explicit company ctx)" if not COMPANY_ID
      else f"🏢 Using explicit company_id={COMPANY_ID}")

# ── Helpers ───────────────────────────────────────────────────────────────────
def chunked(seq, size):
    for i in range(0, len(seq), size):
        yield seq[i:i+size]

def safe_search_ids(model, domain, order='id asc', page=2000):
    ids, offset = [], 0
    while True:
        rows = rpc.call(model, 'search_read', domain, fields=['id'], offset=offset, limit=page, order=order)
        if not rows:
            break
        ids.extend(r['id'] for r in rows)
        offset += len(rows)
        if len(rows) < page:
            break
    return ids

def get_location_name(loc_id):
    try:
        rows = rpc.call('stock.location', 'read', [loc_id], ['complete_name'])
        return rows[0].get('complete_name', f'ID {loc_id}') if rows else f'ID {loc_id}'
    except Exception:
        return f'ID {loc_id}'

def quant_ids_in_root(root_loc_id):
    dom = [('location_id', '=', root_loc_id), ('product_id.type', '=', 'product')]
    return safe_search_ids('stock.quant', dom, order='id asc', page=2000)

def read_quants_at_date(quant_ids, at_date):
    out = []
    for part in chunked(quant_ids, BATCH_SIZE_READ):
        rows = rpc.call('stock.quant', 'read', part,
                        ['product_id', 'location_id', 'lot_id', 'quantity'],
                        context={'to_date': at_date, 'force_date': at_date})
        out.extend(rows)
    return out

def prod_info_map(prod_ids):
    """คืน map: product_id -> {'tmpl_id', 'categ_id', 'code', 'name'}"""
    info = {}
    if not prod_ids:
        return info
    for part in chunked(prod_ids, BATCH_SIZE_READ):
        rows = rpc.call('product.product', 'read', part,
                        ['product_tmpl_id','categ_id','default_code','display_name'])
        for r in rows:
            pt = r.get('product_tmpl_id') or False
            cg = r.get('categ_id') or False
            info[r['id']] = {
                'tmpl_id': pt[0] if isinstance(pt, (list,tuple)) and pt else None,
                'categ_id': cg[0] if isinstance(cg, (list,tuple)) and cg else None,
                'code': r.get('default_code'),
                'name': r.get('display_name'),
            }
    return info

def phantom_bom_templates(tmpl_ids):
    if not tmpl_ids:
        return set()
    res = set()
    for part in chunked(list(tmpl_ids), 500):
        dom = [('product_tmpl_id', 'in', part), ('type', '=', 'phantom')]
        rows = rpc.call('mrp.bom', 'search_read', dom, fields=['id','product_tmpl_id'])
        for r in rows:
            pt = r.get('product_tmpl_id') or False
            if isinstance(pt, (list, tuple)) and pt:
                res.add(pt[0])
    return res

def categ_account_gaps(categ_ids):
    """คืน (ok_set, bad_map) โดย bad_map[categ_id] = {'name':..,'missing':[...]}"""
    if not categ_ids:
        return set(), {}
    ok, bad = set(), {}
    F = ['name','property_valuation',
         'property_stock_valuation_account_id',
         'property_stock_account_input_categ_id',
         'property_stock_account_output_categ_id',
         'property_stock_journal']
    for part in chunked(list(categ_ids), 200):
        rows = rpc.call('product.category', 'read', part, F)
        for r in rows:
            cid = r['id']; missing = []
            # Odoo จะต้องมีบัญชีพวกนี้เมื่อ valuation = real_time
            if (r.get('property_valuation') or 'real_time') == 'real_time':
                if not r.get('property_stock_valuation_account_id'):
                    missing.append('Stock Valuation Account')
                if not r.get('property_stock_account_input_categ_id'):
                    missing.append('Stock Input Account')
                if not r.get('property_stock_account_output_categ_id'):
                    missing.append('Stock Output Account')
                if not r.get('property_stock_journal'):
                    missing.append('Stock Journal')
            if missing:
                bad[cid] = {'name': r.get('name', f'Category {cid}'), 'missing': missing}
            else:
                ok.add(cid)
    return ok, bad

def apply_zero(quant_ids, at_date):
    applied, failed = 0, []
    for batch_no, part in enumerate(chunked(quant_ids, BATCH_SIZE_APPLY), start=1):
        print(f"🧺 Applying batch {batch_no}: {len(part)} quants ...")
        try:
            rpc.call('stock.quant', 'write',
                     part, {'inventory_quantity': 0.0, 'inventory_date': at_date},
                     context={'to_date': at_date, 'force_date': at_date})
            rpc.call('stock.quant', 'action_apply_inventory',
                     part, context={'to_date': at_date, 'force_date': at_date})
            applied += len(part)
        except Exception as e:
            print(f"   ⚠️ Batch failed, fallback per-quant: {e}")
            for qid in part:
                try:
                    rpc.call('stock.quant', 'write',
                             [qid], {'inventory_quantity': 0.0, 'inventory_date': at_date},
                             context={'to_date': at_date, 'force_date': at_date})
                    rpc.call('stock.quant', 'action_apply_inventory',
                             [qid], context={'to_date': at_date, 'force_date': at_date})
                    applied += 1
                    print(f"   ✔ qid={qid} applied")
                except Exception as ee:
                    failed.append((qid, repr(ee)))
                    print(f"   ✗ qid={qid} failed: {ee}")
    return applied, failed

# ── Main ──────────────────────────────────────────────────────────────────────
root_name = get_location_name(ROOT_LOCATION_ID)
print(f"🏷️  Root: {root_name} (id={ROOT_LOCATION_ID})")
print("📍 Locations to process: 1 (root only)")

all_quant_ids = quant_ids_in_root(ROOT_LOCATION_ID)
print(f"🔎 Scanned quants: {len(all_quant_ids)}, eligible at {CLEAR_DATE}: (computing...)")

quants = read_quants_at_date(all_quant_ids, CLEAR_DATE)

# เลือกที่ qty ณ วันที่กำหนด ≠ 0
eligible = [q for q in quants if abs(q.get('quantity') or 0.0) > 1e-9]
prod_ids = { (q.get('product_id') or [None])[0] for q in eligible if q.get('product_id') }

# ข้าม KIT
prod_map = prod_info_map([pid for pid in prod_ids if pid])
tmpl_ids = {info['tmpl_id'] for info in prod_map.values() if info['tmpl_id']}
phantoms = phantom_bom_templates(tmpl_ids)

skip_kit, final = [], []
for q in eligible:
    pid = (q.get('product_id') or [None])[0]
    info = prod_map.get(pid)
    if info and info['tmpl_id'] in phantoms:
        skip_kit.append(q['id'])
    else:
        final.append(q)

# เช็คบัญชีหมวดสินค้า → ข้ามที่ตั้งบัญชีไม่ครบ
categ_ids = {info['categ_id'] for info in prod_map.values() if info['categ_id']}
ok_categ, bad_categ = categ_account_gaps(categ_ids)

skip_badacc, final_ids = [], []
for q in final:
    pid = (q.get('product_id') or [None])[0]
    info = prod_map.get(pid)
    if not info:
        continue
    cg = info['categ_id']
    if cg in ok_categ:
        final_ids.append(q['id'])
    else:
        skip_badacc.append((q['id'], cg, info['code'], info['name']))

# จำกัดจำนวน
if APPLY_LIMIT is not None and len(final_ids) > APPLY_LIMIT:
    final_ids = final_ids[:APPLY_LIMIT]
    print(f"✂️  APPLY_LIMIT active → will apply only first {len(final_ids)} quants")

print("\n📊 SUMMARY (AS-OF)")
print(f" Location   : {root_name} (ID {ROOT_LOCATION_ID})")
print(f" Clear date : {CLEAR_DATE}")
print(f" DRY_RUN    : {DRY_RUN}")
print(f" Candidates : {len(final_ids)} quants to set 0 @ {CLEAR_DATE}")
print(f"ℹ️  Skipped KIT quants          : {len(skip_kit)}")
print(f"ℹ️  Skipped (missing accounts)  : {len(skip_badacc)}")
if skip_badacc and len(skip_badacc) <= 20:
    print("   → ตัวอย่าง (qid, category, code, name):")
    for qid, cg, code, name in skip_badacc[:20]:
        cname = bad_categ.get(cg, {}).get('name', f'Cat {cg}')
        print(f"     - qid={qid}, {cname}, [{code}] {name}")
if bad_categ:
    print("   → หมวดที่ต้องตั้งบัญชีเพิ่ม:")
    for cid, meta in bad_categ.items():
        need = ", ".join(meta['missing'])
        print(f"     - {meta['name']} (ID {cid}) : missing {need}")

if not final_ids:
    print("✅ Nothing to do (ทุกตัวถูกข้ามเพราะ KIT/บัญชีไม่ครบ).")
else:
    if DRY_RUN:
        sample = [q for q in quants if q['id'] in final_ids][:10]
        print("🔎 Preview first items:")
        for r in sample:
            prod = r['product_id'][1] if r.get('product_id') else 'n/a'
            loc  = r['location_id'][1] if r.get('location_id') else f'ID {ROOT_LOCATION_ID}'
            lot  = (r.get('lot_id') or [None, '-'])[1]
            print(f"   • [[{prod}]] @ {loc} qty_at_{CLEAR_DATE}={r.get('quantity')} lot:{lot} → set 0")
        print("👉 DRY_RUN=True → ยังไม่ปรับจริง (สลับเป็น False เพื่อ apply)")
    else:
        print(f"🧺 Applying in batches of {BATCH_SIZE_APPLY} ...")
        applied, failed = apply_zero(final_ids, CLEAR_DATE)
        print("\n📊 DONE")
        print(f" Applied    : {applied} quants → 0 at {CLEAR_DATE}")
        print(f" Skipped KIT: {len(skip_kit)}")
        print(f" Skipped ACC: {len(skip_badacc)} (ต้องตั้งบัญชี)")
        if failed:
            print(f" ⚠️ Failed  : {len(failed)} quants (ดูข้อความด้านบน)")

        print("\n👉 ตรวจผลที่ Inventory → Reporting → Stock Valuation (as of date) + GL")
        if bad_categ:
            print("👉 ไปตั้งค่าบัญชีที่: Inventory → Configuration → Product Categories → Accounting")
            print("   ใส่: Stock Valuation, Stock Input, Stock Output, Stock Journal (หรือกำหนดบน Location แทนได้)")
