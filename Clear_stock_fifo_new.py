import xmlrpc.client
from datetime import datetime

# 🔐 Connection parameters
HOST = 'http://mogdev.work:8069'
DB = 'MOG_LIVE_26-06'
USERNAME = 'apichart@mogen.co.th'
PASSWORD = '471109538'

# 🌐 XML-RPC endpoints
common = xmlrpc.client.ServerProxy(f'{HOST}/xmlrpc/2/common')
uid = common.authenticate(DB, USERNAME, PASSWORD, {})
models = xmlrpc.client.ServerProxy(f'{HOST}/xmlrpc/2/object')

# 📅 วันที่ที่ต้องการเคลียร์ (วันก่อนหน้า)
clear_date = '2025-01-31 23:59:59'

# 📌 ดึง internal locations ทั้งหมด
location_ids = models.execute_kw(DB, uid, PASSWORD,
    'stock.location', 'search',
    [[('usage', '=', 'internal')]])

print(f"พบ {len(location_ids)} internal locations")

# 📦 ดึง product ทั้งหมดที่มีอยู่
product_ids = models.execute_kw(DB, uid, PASSWORD,
    'product.product', 'search',
    [[('type', '=', 'product')]])

print(f"พบสินค้าทั้งหมด {len(product_ids)} รายการ")

# 📍 Scrap Location (ของระบบมาตรฐาน)
scrap_location_id = models.execute_kw(DB, uid, PASSWORD,
    'stock.location', 'search',
    [[('scrap_location', '=', True)]], {'limit': 1})[0]

# 🔁 Clear FIFO Cost Layers and Valuations
print(f"🔍 Searching for stock valuation entries to clear up to date {clear_date}...")

# ค้นหา valuation entries ที่มีวันที่ไม่เกิน clear_date
try:
    # เพิ่ม limit และรันหลายรอบ
    total_rounds = 0
    grand_total_value = 0
    grand_total_count = 0
    
    while True:
        total_rounds += 1
        print(f"\n🔄 Round {total_rounds}: Searching for valuation entries up to {clear_date}...")
        
        valuation_entries = models.execute_kw(DB, uid, PASSWORD,
            'stock.valuation.layer', 'search_read',
            [[
                ('value', '!=', 0), 
                ('create_date', '<=', clear_date)  # เพิ่มเงื่อนไขวันที่
            ]],
            {'fields': ['id', 'value', 'quantity', 'create_date'], 'limit': 500})  # เพิ่ม limit
        
        if not valuation_entries:
            print(f"✅ No more valuation entries found. Finished after {total_rounds} rounds.")
            break
            
        print(f"📊 Found {len(valuation_entries)} valuation entries to clear (up to {clear_date})")
        
        if valuation_entries:
            print("🧹 Clearing valuation entries...")
            
            total_cleared_value = 0
            cleared_count = 0
            
            for entry in valuation_entries:
                try:
                    entry_id = entry['id']
                    value = entry['value']
                    entry_date = entry.get('create_date', 'unknown date')
                    
                    if cleared_count % 50 == 0:  # แสดงทุก 50 รายการ
                        print(f"  Processing entry {cleared_count + 1}/{len(valuation_entries)}... (date: {entry_date})")
                    
                    # ล้างค่าโดยตรง
                    models.execute_kw(DB, uid, PASSWORD,
                        'stock.valuation.layer', 'write',
                        [[entry_id], {'value': 0}])
                    
                    total_cleared_value += value
                    cleared_count += 1
                    
                except Exception as e:
                    print(f"  ❌ Error clearing entry {entry_id}: {str(e)}")
                    continue
            
            grand_total_value += total_cleared_value
            grand_total_count += cleared_count
            
            print(f"\n📊 Round {total_rounds} Summary:")
            print(f"   Entries cleared: {cleared_count}")
            print(f"   Total value cleared: {total_cleared_value:,.2f}")
            
            # ตรวจสอบว่ายังมี entries ที่เหลืออยู่หรือไม่
            remaining_count = models.execute_kw(DB, uid, PASSWORD,
                'stock.valuation.layer', 'search_count', 
                [[('value', '!=', 0), ('create_date', '<=', clear_date)]])
                
            if remaining_count == 0:
                print(f"✅ All valuation entries up to {clear_date} have been cleared!")
                break
            else:
                print(f"ℹ️ {remaining_count} valuation entries still need clearing. Continuing...")
            
        # หยุดถ้ารันเกิน 30 รอบ เพื่อป้องกัน infinite loop (เพิ่มจากเดิม)
        if total_rounds >= 30:
            print("⚠️ Reached maximum rounds (30). Stopping.")
            break
    
    print(f"\n🎯 FINAL SUMMARY:")
    print(f"   Total rounds: {total_rounds}")
    print(f"   Total entries cleared: {grand_total_count}")
    print(f"   Total value cleared: {grand_total_value:,.2f}")
    
except Exception as e:
    print(f"❌ Error accessing stock.valuation.layer: {str(e)}")
    print("📋 Trying alternative approach...")

# วิธีการทางเลือก: ล้างผ่าน account moves
print(f"\n🔍 Checking for account moves with stock valuation up to {clear_date}...")

try:
    # หา account moves ที่เกี่ยวข้องกับ stock valuation และมีวันที่ไม่เกิน clear_date
    account_moves = models.execute_kw(DB, uid, PASSWORD,
        'account.move.line', 'search_read',
        [[
            ('date', '<=', clear_date),  # ใช้ตัวแปร clear_date แทนค่าคงที่
            ('account_id.code', 'like', '1301%'),  # สินค้าคงคลัง
            ('balance', '!=', 0)
        ]],
        {'fields': ['id', 'account_id', 'balance', 'date'], 'limit': 200})
    
    print(f"📊 Found {len(account_moves)} account move lines to check")
    
    if account_moves:
        print("🧹 Clearing stock account balances...")
        
        for move_line in account_moves:
            try:
                line_id = move_line['id']
                balance = move_line['balance']
                
                if abs(balance) > 0.01:  # มีค่าที่ต้องล้าง
                    print(f"  Found balance: {balance} in line {line_id}")
                    
                    # สร้าง journal entry เพื่อล้างยอด
                    journal_entry = {
                        'date': clear_date,
                        'ref': f'Clear Stock Valuation - {clear_date}',
                        'journal_id': 1,  # General Journal
                        'line_ids': [
                            (0, 0, {
                                'account_id': move_line['account_id'][0],
                                'debit': balance if balance < 0 else 0,
                                'credit': balance if balance > 0 else 0,
                                'name': f'Clear stock valuation {clear_date}',
                            }),
                            (0, 0, {
                                'account_id': models.execute_kw(DB, uid, PASSWORD,
                                    'account.account', 'search',
                                    [[('code', '=', '5101')]], {'limit': 1})[0],  # Cost of Goods Sold
                                'debit': balance if balance > 0 else 0,
                                'credit': balance if balance < 0 else 0,
                                'name': f'Clear stock valuation offset {clear_date}',
                            }),
                        ]
                    }
                    
                    # สร้าง journal entry
                    move_id = models.execute_kw(DB, uid, PASSWORD,
                        'account.move', 'create', [journal_entry])
                    
                    # ทำให้ entry เป็น posted
                    models.execute_kw(DB, uid, PASSWORD,
                        'account.move', 'action_post', [[move_id]])
                    
                    print(f"  ✅ Created clearing entry for balance: {balance}")
                    
            except Exception as e:
                print(f"  ❌ Error processing account move line {line_id}: {str(e)}")
                continue

except Exception as e:
    print(f"❌ Error accessing account moves: {str(e)}")

# 📝 ข้ามการล้าง Physical Stock Quantities
print("\n📋 Skipping clearing of physical stock quantities...")
print("📋 This script will only clear financial values (stock valuation layers)")
print("📋 Physical quantities in stock.quant, stock.move, and related models will be preserved")

# 📝 ข้ามการเคลียร์ quantities ด้วยวิธีการแบบ Direct
print("\n📋 Skipping final direct approaches for clearing quantities...")
print("📋 Physical quantities in stock.move and related models will be preserved")

# 📝 ข้ามการใช้วิธี aggressive approach สำหรับล้าง stock moves
print("\n📋 Skipping aggressive stock move quantity clearing approaches...")

# ตรวจสอบยืนยันว่าล้าง valuation entries จนถึงวันที่สำเร็จหรือไม่
print("\n🔍 Final verification of valuation entries...")

valuation_success = False
try:
    # ตรวจสอบว่ายังมี valuation entries ที่ยังมีค่าไม่เป็น 0 หรือไม่
    remaining_entries = models.execute_kw(DB, uid, PASSWORD,
        'stock.valuation.layer', 'search_count',
        [[('value', '!=', 0), ('create_date', '<=', clear_date)]])
    
    print(f"📊 Valuation entries with value != 0 up to {clear_date}: {remaining_entries}")
    
    if remaining_entries == 0:
        valuation_success = True
        print(f"✅ SUCCESS: All valuation entries up to {clear_date} have been cleared!")
    else:
        print(f"⚠️ WARNING: {remaining_entries} valuation entries still have value != 0")
        print(f"   Consider running the script again to clear remaining entries")
except Exception as e:
    print(f"❌ Error during final verification: {str(e)}")

print(f"\n🎯 SUMMARY:")
print(f"{'✅' if valuation_success else '⚠️'} FIFO Cost Valuation: {'COMPLETELY' if valuation_success else 'PARTIALLY'} CLEARED up to {clear_date}")
print(f"📋 Physical Stock Quantities: PRESERVED (Not cleared)")
print(f"📋 The financial impact (cost valuation) has been {'successfully eliminated' if valuation_success else 'partially cleared'}!")
print(f"📋 Physical stock quantities have been preserved as requested")
