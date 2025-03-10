import xmlrpc.client
import pandas as pd
import sys
import base64
import os
import requests

# --- ตั้งค่าการเชื่อมต่อ Odoo ---
server_url = 'http://mogth.work:8069'
database = 'Test_Import'
username = 'apichart@mogen.co.th'
password = '471109538'

# --- Authentication ---
try:
    common = xmlrpc.client.ServerProxy(f'{server_url}/xmlrpc/2/common')
    uid = common.authenticate(database, username, password, {})
    if not uid:
        print("Authentication failed: ตรวจสอบ credentials หรือ permission")
        sys.exit(1)
    else:
        print("Authentication successful, uid =", uid)
except Exception as e:
    print("Error during authentication:", e)
    sys.exit(1)

# --- สร้าง models proxy ---
try:
    models = xmlrpc.client.ServerProxy(f'{server_url}/xmlrpc/2/object')
except Exception as e:
    print("Error creating XML-RPC models proxy:", e)
    sys.exit(1)


def search_category(category_path):
    """ค้นหาหรือสร้าง category จาก path"""
    if pd.isna(category_path):
        return False
    
    categories = category_path.split('/')
    parent_id = False
    current_id = False
    
    for category in categories:
        category = category.strip()
        if category:
            domain = [('name', '=', category)]
            if parent_id:
                domain.append(('parent_id', '=', parent_id))
            
            category_ids = models.execute_kw(
                database, uid, password, 'product.category', 'search', [domain]
            )
            
            if category_ids:
                current_id = category_ids[0]
            else:
                # สร้าง category ใหม่
                vals = {
                    'name': category,
                    'parent_id': parent_id
                }
                try:
                    current_id = models.execute_kw(
                        database, uid, password, 'product.category', 'create', [vals]
                    )
                except Exception as e:
                    print(f"Error creating category {category}: {e}")
                    return False
            
            parent_id = current_id
    
    return current_id

def search_uom(uom_name):
    """ค้นหา UoM"""
    if pd.isna(uom_name):
        return False
    
    uom_ids = models.execute_kw(
        database, uid, password, 'uom.uom', 'search', [[('name', '=', uom_name.strip())]]
    )
    return uom_ids[0] if uom_ids else False

def get_customer_tax():
    """ค้นหา Customer Tax (VAT 7%)"""
    tax_ids = models.execute_kw(
        database, uid, password, 'account.tax', 'search',
        [[('type_tax_use', '=', 'sale'), ('amount', '=', 7.0), ('name', 'like', '%7%')]]
    )
    if tax_ids:
        return tax_ids[0]
    print("Warning: Customer VAT 7% tax not found")
    return False

# สร้าง list เก็บข้อมูลสินค้าที่ import ไม่สำเร็จ
failed_imports = []

# --- อ่านข้อมูลจากไฟล์ Excel ---
excel_file = 'Project1/Data_file/import_product.xlsx'
try:
    df = pd.read_excel(excel_file)
    print(f"Excel file '{excel_file}' read successfully. Number of rows = {len(df)}")
    
    # ข้ามแถวแรกที่เป็นหัวข้อภาษาไทย
    df = df.iloc[2:]
    df = df.reset_index(drop=True)
    
except Exception as e:
    print("Failed to read Excel file:", e)
    sys.exit(1)

# Get customer tax ID once
customer_tax_id = get_customer_tax()

# --- วนลูปประมวลผลแต่ละแถวใน Excel ---
for index, row in df.iterrows():
    try:
        # ตรวจสอบ default_code
        default_code = str(row['default_code']).strip() if pd.notna(row['default_code']) else ''
        if not default_code:
            print(f"Row {index}: Missing default_code. Skipping.")
            continue

        # Clean up barcode data
        barcode = str(row['barcode']) if pd.notna(row['barcode']) else False
        if barcode and barcode.strip():  # ตรวจสอบว่า barcode มีค่าและไม่ใช่ string ว่าง
            # Remove decimal point and zeros if present
            barcode = str(float(barcode)).rstrip('0').rstrip('.')
        else:
            barcode = False  # ถ้า barcode ว่างหรือเป็น NaN ให้กำหนดเป็น False

        # ตรวจสอบว่ามีสินค้าอยู่แล้วหรือไม่ (ทั้ง default_code และ barcode ที่ไม่ว่าง)
        domain = ['|',
                 ['default_code', '=', default_code],
                 '&',
                 ['barcode', '!=', False],
                 ['barcode', '=', barcode]]
        existing_products = models.execute_kw(
            database, uid, password, 'product.template', 'search',
            [domain]
        )
        
        if existing_products:
            existing_product = models.execute_kw(
                database, uid, password, 'product.template', 'read',
                [existing_products[0]], {'fields': ['name', 'default_code', 'barcode']}
            )[0]
            error_msg = f"สินค้ามีอยู่แล้วในระบบ - Default Code: {existing_product['default_code']}, Barcode: {existing_product['barcode']}, Name: {existing_product['name']}"
            print(f"Row {index}: Product already exists with:")
            print(f"  - Default Code: {existing_product['default_code']}")
            print(f"  - Barcode: {existing_product['barcode']}")
            print(f"  - Name: {existing_product['name']}")
            print("  Skipping.")
            
            # เก็บข้อมูลสินค้าที่ import ไม่สำเร็จ
            failed_imports.append({
                'Row': index + 3,  # +3 เพราะมีการข้ามแถวแรก 2 แถว และ index เริ่มที่ 0
                'Default Code': default_code,
                'Name': row['name'] if pd.notna(row['name']) else '',
                'Barcode': barcode,
                'Error': error_msg
            })
            continue

        # เตรียมข้อมูลสินค้า
        product_data = {
            'name': str(row['name']).strip() if pd.notna(row['name']) else '',
            'name_eng': str(row['name_eng']).strip() if pd.notna(row['name_eng']) else '',
            'default_code': default_code,
            'barcode': barcode,
            'type': 'product',  # กำหนดเป็น storable product
            'categ_id': search_category(row['categ_id']) if pd.notna(row['categ_id']) else False,
            'uom_id': search_uom(row['uom_id']) if pd.notna(row['uom_id']) else False,
            'list_price': float(str(row['list_price']).replace(',', '')) if pd.notna(row['list_price']) else 0.0,
            'standard_price': float(str(row['standard_price']).replace(',', '')) if pd.notna(row['standard_price']) else 0.0,
            'sale_ok': True if pd.notna(row['sale_ok']) and str(row['sale_ok']).strip().lower() == 'yes' else False,
            'purchase_ok': True if pd.notna(row['purchase_ok']) and str(row['purchase_ok']).strip().lower() == 'yes' else False,
            'active': True,  # Set active status to True by default
            'description': str(row['description']).strip() if pd.notna(row['description']) else '',
            'gross_width': float(str(row['gross_width']).replace(',', '')) if pd.notna(row['gross_width']) else 0.0,
            'gross_depth': float(str(row['gross_depth']).replace(',', '')) if pd.notna(row['gross_depth']) else 0.0,
            'gross_height': float(str(row['gross_height']).replace(',', '')) if pd.notna(row['gross_height']) else 0.0,
            'weight': float(str(row['weight']).replace(',', '')) if pd.notna(row['weight']) else 0.0,
            'box_width': float(str(row['box_width']).replace(',', '')) if pd.notna(row['box_width']) else 0.0,
            'box_depth': float(str(row['box_depth']).replace(',', '')) if pd.notna(row['box_depth']) else 0.0,
            'box_height': float(str(row['box_height']).replace(',', '')) if pd.notna(row['box_height']) else 0.0,
            'box_weight': float(str(row['box_weight']).replace(',', '')) if pd.notna(row['box_weight']) else 0.0,
            'taxes_id': [(6, 0, [customer_tax_id])] if customer_tax_id else [(6, 0, [])],
        }


        # แสดงข้อมูลที่จะเพิ่ม
        print(f"\nกำลังเพิ่มสินค้าใหม่ (Row {index}):")
        print(f"  ชื่อสินค้า: {product_data['name']}")
        print(f"  รหัสสินค้า: {product_data['default_code']}")
        print(f"  บาร์โค้ด: {product_data['barcode']}")
        print(f"  ราคาขาย: {product_data['list_price']}")
        print(f"  ราคาทุน: {product_data['standard_price']}")
        
        # สร้างสินค้าใน Odoo
        try:
            # ตรวจสอบ barcode อีกครั้ง
            if product_data['barcode']:
                barcode_exists = models.execute_kw(
                    database, uid, password, 'product.template', 'search_count',
                    [[['barcode', '=', product_data['barcode']]]]
                )
                if barcode_exists:
                    error_msg = f"บาร์โค้ด {product_data['barcode']} มีอยู่แล้วในระบบ"
                    print(f"  ไม่สามารถเพิ่มสินค้าได้: {error_msg}")
                    failed_imports.append({
                        'Row': index + 3,
                        'Default Code': default_code,
                        'Name': product_data['name'],
                        'Barcode': barcode,
                        'Error': error_msg
                    })
                    continue

            new_product_id = models.execute_kw(
                database, uid, password, 'product.template', 'create', [product_data]
            )
            print(f"  ✓ เพิ่มสินค้าสำเร็จ (ID: {new_product_id})")
        except Exception as e:
            print(f"  ✗ ไม่สามารถเพิ่มสินค้าได้: {str(e)}")
            error_msg = str(e)
            # แสดงข้อมูลเพิ่มเติมเพื่อการแก้ไข
            if "Barcode" in str(e):
                print("    โปรดตรวจสอบ: บาร์โค้ดอาจซ้ำกับสินค้าที่มีอยู่แล้ว")
            
            # เก็บข้อมูลสินค้าที่ import ไม่สำเร็จ
            failed_imports.append({
                'Row': index + 3,
                'Default Code': default_code,
                'Name': product_data['name'],
                'Barcode': barcode,
                'Error': error_msg
            })
            
    except Exception as e:
        error_msg = f"Error processing row: {str(e)}"
        print(f"Row {index}: {error_msg}")
        # แสดงชื่อคอลัมน์ทั้งหมดเมื่อเกิดข้อผิดพลาด
        print("Available columns:", df.columns.tolist())
        
        # เก็บข้อมูลสินค้าที่ import ไม่สำเร็จ
        failed_imports.append({
            'Row': index + 3,
            'Default Code': default_code if 'default_code' in locals() else 'N/A',
            'Name': row['name'] if pd.notna(row['name']) else 'N/A',
            'Barcode': barcode if 'barcode' in locals() else 'N/A',
            'Error': error_msg
        })

# บันทึกข้อมูลสินค้าที่ import ไม่สำเร็จลงไฟล์ Excel
if failed_imports:
    # สร้าง DataFrame จากข้อมูลที่ import ไม่สำเร็จ
    failed_df = pd.DataFrame(failed_imports)
    
    # กำหนดชื่อไฟล์ Excel ที่จะบันทึก
    timestamp = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
    failed_excel_file = f'Project1/Data_file/failed_imports_{timestamp}.xlsx'
    
    # บันทึกไฟล์ Excel
    failed_df.to_excel(failed_excel_file, index=False, engine='openpyxl')
    print(f"\nบันทึกรายการสินค้าที่ import ไม่สำเร็จไว้ที่: {failed_excel_file}")
    print(f"จำนวนรายการที่ import ไม่สำเร็จ: {len(failed_imports)} รายการ")