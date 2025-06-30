import xmlrpc.client
from datetime import datetime
import time
import socket
import json
import os

# 🔐 Connection parameters
HOST = 'http://mogdev.work:8069'
DB = 'MOG_LIVE_26-06'
USERNAME = 'apichart@mogen.co.th'
PASSWORD = '471109538'

# ⚙️ การตั้งค่าการเชื่อมต่อ
MAX_RETRIES = 5      # จำนวนครั้งที่จะลองเชื่อมต่อซ้ำ
RETRY_DELAY = 3      # ระยะเวลารอระหว่างการลองใหม่ (วินาที)
TIMEOUT = 60         # timeout สำหรับการเชื่อมต่อ (วินาที)

# 📅 วันที่ที่ต้องการเคลียร์ (วันก่อนหน้า)
clear_date = '2025-01-31 23:59:59'

# กำหนดตัวแปรสำหรับการเชื่อมต่อ
global_vars = {'common': None, 'uid': None, 'models': None}

# 🔌 ฟังก์ชั่นสำหรับสร้างการเชื่อมต่อใหม่
def create_connection():
    print("🔄 Creating connection to Odoo server...")
    common_proxy = xmlrpc.client.ServerProxy(f'{HOST}/xmlrpc/2/common', 
                                           transport=xmlrpc.client.Transport(timeout=TIMEOUT))
    user_id = common_proxy.authenticate(DB, USERNAME, PASSWORD, {})
    models_proxy = xmlrpc.client.ServerProxy(f'{HOST}/xmlrpc/2/object', 
                                           transport=xmlrpc.client.Transport(timeout=TIMEOUT))
    print("✅ Connection established successfully")
    return common_proxy, user_id, models_proxy

# 🔄 ฟังก์ชั่นสำหรับเรียกใช้ method กับการลองเชื่อมต่อใหม่อัตโนมัติ
def execute_with_retry(model, method, args, kwargs=None):
    if kwargs is None:
        kwargs = {}
    
    retry_count = 0
    while retry_count < MAX_RETRIES:
        try:
            return global_vars['models'].execute_kw(DB, global_vars['uid'], PASSWORD, model, method, args, kwargs)
        except (xmlrpc.client.ProtocolError, socket.error, ConnectionRefusedError, xmlrpc.client.Fault) as e:
            retry_count += 1
            if retry_count < MAX_RETRIES:
                print(f"⚠️ Connection error: {str(e)}")
                print(f"🔄 Retrying connection ({retry_count}/{MAX_RETRIES})... waiting {RETRY_DELAY} seconds")
                time.sleep(RETRY_DELAY)
                
                # สร้างการเชื่อมต่อใหม่
                global_vars['common'], global_vars['uid'], global_vars['models'] = create_connection()
            else:
                print(f"❌ Failed after {MAX_RETRIES} attempts: {str(e)}")
                raise

# 💾 ฟังก์ชั่นจัดการการบันทึกความคืบหน้า
def save_progress(total_rounds, grand_total_value, grand_total_count, processed_entries):
    try:
        progress_data = {
            "date": clear_date,
            "total_rounds": total_rounds,
            "grand_total_value": grand_total_value,
            "grand_total_count": grand_total_count,
            "processed_entries": processed_entries,
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
        
        with open('clear_fifo_progress.json', 'w') as f:
            json.dump(progress_data, f)
        print(f"💾 Progress saved. Total entries processed: {len(processed_entries)}")
    except Exception as e:
        print(f"⚠️ Could not save progress: {str(e)}")

def load_progress():
    if os.path.exists('clear_fifo_progress.json'):
        try:
            with open('clear_fifo_progress.json', 'r') as f:
                progress_data = json.load(f)
                
            saved_date = progress_data.get("date")
            if saved_date == clear_date:
                print(f"📂 Found saved progress for {saved_date}")
                return (
                    progress_data.get("total_rounds", 0),
                    progress_data.get("grand_total_value", 0),
                    progress_data.get("grand_total_count", 0),
                    set(progress_data.get("processed_entries", []))
                )
            else:
                print(f"📂 Saved progress is for a different date ({saved_date}), starting fresh")
        except Exception as e:
            print(f"⚠️ Error loading progress: {str(e)}")
    
    return 0, 0, 0, set()

# 🌐 สร้างการเชื่อมต่อครั้งแรก
global_vars['common'], global_vars['uid'], global_vars['models'] = create_connection()

# 📌 ดึง internal locations ทั้งหมด
location_ids = execute_with_retry('stock.location', 'search', 
    [[('usage', '=', 'internal')]])

print(f"พบ {len(location_ids)} internal locations")

# 📦 ดึง product ทั้งหมดที่มีอยู่
product_ids = execute_with_retry('product.product', 'search',
    [[('type', '=', 'product')]])

print(f"พบสินค้าทั้งหมด {len(product_ids)} รายการ")

# 📍 Scrap Location (ของระบบมาตรฐาน)
scrap_location_id = execute_with_retry('stock.location', 'search',
    [[('scrap_location', '=', True)]], {'limit': 1})[0]

# 🔁 Clear FIFO Cost Layers and Valuations
print(f"🔍 Searching for stock valuation entries to clear up to date {clear_date}...")

# ค้นหา valuation entries ที่มีวันที่ไม่เกิน clear_date
try:
    # โหลดความคืบหน้าที่บันทึกไว้ (ถ้ามี)
    total_rounds, grand_total_value, grand_total_count, processed_entries = load_progress()
    
    # ถ้ามีความคืบหน้าที่บันทึกไว้ เริ่มจากจุดที่ค้างไว้
    if total_rounds > 0:
        print(f"🔄 Resuming from previous run. Rounds completed: {total_rounds}")
        print(f"🔄 Entries processed so far: {grand_total_count}")
        print(f"🔄 Total value cleared so far: {grand_total_value:,.2f}")
    else:
        # เริ่มใหม่
        processed_entries = set()
    
    # สถานะการทำงานล่าสุด - บันทึกทุก 20 วินาที
    last_save_time = time.time()
    save_interval = 20  # seconds
    
    while True:
        total_rounds += 1
        print(f"\n🔄 Round {total_rounds}: Searching for valuation entries up to {clear_date}...")
        
        valuation_entries = execute_with_retry(
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
                    
                    # ข้ามถ้าเคยประมวลผลแล้ว
                    if entry_id in processed_entries:
                        continue
                        
                    value = entry['value']
                    entry_date = entry.get('create_date', 'unknown date')
                    
                    if cleared_count % 50 == 0:  # แสดงทุก 50 รายการ
                        print(f"  Processing entry {cleared_count + 1}/{len(valuation_entries)}... (date: {entry_date})")
                    
                    # ล้างค่าโดยตรง - ใช้ฟังก์ชั่นที่มีการ retry อัตโนมัติ
                    execute_with_retry(
                        'stock.valuation.layer', 'write',
                        [[entry_id], {'value': 0}])
                    
                    # บันทึกว่าได้ประมวลผลแล้ว
                    processed_entries.add(entry_id)
                    
                    total_cleared_value += value
                    cleared_count += 1
                    
                    # บันทึกความคืบหน้าเป็นระยะ
                    if time.time() - last_save_time > save_interval:
                        save_progress(total_rounds, grand_total_value + total_cleared_value, 
                                      grand_total_count + cleared_count, list(processed_entries))
                        last_save_time = time.time()
                    
                except Exception as e:
                    print(f"  ❌ Error clearing entry {entry_id}: {str(e)}")
                    continue
            
            grand_total_value += total_cleared_value
            grand_total_count += cleared_count
            
            print(f"\n📊 Round {total_rounds} Summary:")
            print(f"   Entries cleared: {cleared_count}")
            print(f"   Total value cleared: {total_cleared_value:,.2f}")
            
            # บันทึกความคืบหน้าหลังจบแต่ละรอบ
            save_progress(total_rounds, grand_total_value, grand_total_count, list(processed_entries))
            
            # ตรวจสอบว่ายังมี entries ที่เหลืออยู่หรือไม่ - ใช้ฟังก์ชั่นที่มีการ retry อัตโนมัติ
            remaining_count = execute_with_retry(
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
    # หา account moves ที่เกี่ยวข้องกับ stock valuation และมีวันที่ไม่เกิน clear_date - ใช้ฟังก์ชั่นที่มีการ retry อัตโนมัติ
    account_moves = execute_with_retry(
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
                                'account_id': execute_with_retry(
                                    'account.account', 'search',
                                    [[('code', '=', '5101')]], 
                                    {'limit': 1})[0],  # Cost of Goods Sold
                                'debit': balance if balance > 0 else 0,
                                'credit': balance if balance < 0 else 0,
                                'name': f'Clear stock valuation offset {clear_date}',
                            }),
                        ]
                    }
                    
                    # สร้าง journal entry - ใช้ฟังก์ชั่นที่มีการ retry อัตโนมัติ
                    move_id = execute_with_retry(
                        'account.move', 'create', [journal_entry])
                    
                    # ทำให้ entry เป็น posted - ใช้ฟังก์ชั่นที่มีการ retry อัตโนมัติ
                    execute_with_retry(
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
    # ตรวจสอบว่ายังมี valuation entries ที่ยังมีค่าไม่เป็น 0 หรือไม่ - ใช้ฟังก์ชั่นที่มีการ retry อัตโนมัติ
    remaining_entries = execute_with_retry(
        'stock.valuation.layer', 'search_count',
        [[('value', '!=', 0), ('create_date', '<=', clear_date)]])
    
    print(f"📊 Valuation entries with value != 0 up to {clear_date}: {remaining_entries}")
    
    if remaining_entries == 0:
        valuation_success = True
        print(f"✅ SUCCESS: All valuation entries up to {clear_date} have been cleared!")
        
        # ลบไฟล์บันทึกความคืบหน้าเมื่อทำงานสำเร็จ
        if os.path.exists('clear_fifo_progress.json'):
            os.remove('clear_fifo_progress.json')
            print("🧹 Cleared progress file as task completed successfully")
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
