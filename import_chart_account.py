import xmlrpc.client
import pandas as pd
import sys
import logging
import json
from pathlib import Path

# ตั้งค่า logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('chart_accounts_import.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Odoo connection parameters (can be overridden by config file)
url = 'http://kyld.site:8069'
db = 'KYLD_LIVE'
username = 'apichart@mogen.co.th'
password = '471109538'
company_id = None  # Will be set during connection or from config

def load_config():
    """โหลดการตั้งค่าจากไฟล์ config"""
    global url, db, username, password, company_id
    try:
        config_path = Path('odoo_config.json')
        if config_path.exists():
            with open(config_path, 'r', encoding='utf-8') as f:
                config = json.load(f)
                if 'odoo' in config:
                    url = config['odoo'].get('url', url)
                    db = config['odoo'].get('database', db)
                    username = config['odoo'].get('username', username)
                    password = config['odoo'].get('password', password)
                    company_id = config['odoo'].get('company_id', company_id)
                    logger.info(f"โหลด config จาก {config_path}")
    except Exception as e:
        logger.warning(f"ไม่สามารถโหลด config: {str(e)}, ใช้ค่าเริ่มต้น")

def connect_to_odoo():
    """เชื่อมต่อกับ Odoo"""
    try:
        common = xmlrpc.client.ServerProxy(f'{url}/xmlrpc/2/common')
        uid = common.authenticate(db, username, password, {})
        if not uid:
            logger.error("การเชื่อมต่อล้มเหลว: ไม่สามารถยืนยันตัวตนได้")
            return None, None
        
        models = xmlrpc.client.ServerProxy(f'{url}/xmlrpc/2/object')
        return uid, models
    except Exception as e:
        logger.error(f"การเชื่อมต่อล้มเหลว: {str(e)}")
        return None, None

def get_company_id(models, uid, company_name_or_id=None):
    """ดึง company_id จากชื่อหรือ ID ของบริษัท"""
    global company_id
    
    # ถ้ามีการระบุ company มาใน parameter
    if company_name_or_id:
        try:
            # ลองแปลงเป็น ID ก่อน
            cid = int(company_name_or_id)
            company_exists = models.execute_kw(db, uid, password,
                'res.company', 'search_count',
                [[['id', '=', cid]]]
            )
            if company_exists:
                company_id = cid
                logger.info(f"ใช้ company_id: {company_id}")
                return company_id
        except (ValueError, TypeError):
            # ค้นหาจากชื่อบริษัท
            company_ids = models.execute_kw(db, uid, password,
                'res.company', 'search',
                [[['name', 'ilike', company_name_or_id]]]
            )
            if company_ids:
                company_id = company_ids[0]
                logger.info(f"พบบริษัท '{company_name_or_id}' ด้วย ID: {company_id}")
                return company_id
    
    # ถ้ายังไม่มี company_id ให้ใช้ company เริ่มต้นของ user
    if not company_id:
        try:
            user_info = models.execute_kw(db, uid, password,
                'res.users', 'read',
                [[uid]], {'fields': ['company_id']}
            )
            if user_info and user_info[0].get('company_id'):
                company_id = user_info[0]['company_id'][0]
                company_name = user_info[0]['company_id'][1]
                logger.info(f"ใช้ company เริ่มต้น: {company_name} (ID: {company_id})")
        except Exception as e:
            logger.warning(f"ไม่สามารถดึง company เริ่มต้นได้: {str(e)}")
    
    return company_id

def get_account_type(account_type):
    """แปลงประเภทบัญชีให้ตรงกับ Odoo 17"""
    type_mapping = {
        'receivable': 'asset_receivable',
        'payable': 'liability_payable',
        'bank': 'asset_cash',
        'cash': 'asset_cash',
        'current assets': 'asset_current',
        'non-current assets': 'asset_non_current',
        'prepayments': 'asset_prepayments',
        'fixed assets': 'asset_fixed',
        'current liabilities': 'liability_current',
        'non-current liabilities': 'liability_non_current',
        'equity': 'equity',
        'current year earnings': 'equity_unaffected',
        'income': 'income',
        'other income': 'income_other',
        'expenses': 'expense',
        'other expenses': 'expense_depreciation',
        'cost of revenue': 'expense_direct_cost',
    }
    if not account_type or pd.isna(account_type):
        return 'asset_current'
    return type_mapping.get(str(account_type).lower().strip(), 'asset_current')

def get_currency_id(currency_value, models, uid):
    """ค้นหา currency_id จากค่าที่ระบุ (สามารถเป็นได้ทั้ง ID หรือรหัสสกุลเงิน)"""
    try:
        # พยายามแปลงเป็นตัวเลขก่อน (กรณีระบุเป็น ID)
        currency_id = float(currency_value)
        if currency_id.is_integer():
            currency_id = int(currency_id)
            # ตรวจสอบว่า ID มีอยู่ในระบบ
            currency_exists = models.execute_kw(db, uid, password,
                'res.currency', 'search_count',
                [[['id', '=', currency_id]]]
            )
            if currency_exists:
                return currency_id
    except (ValueError, TypeError):
        # กรณีไม่สามารถแปลงเป็นตัวเลขได้ ให้ค้นหาจากรหัสสกุลเงิน
        currency_code = str(currency_value).strip().upper()
        currency_ids = models.execute_kw(db, uid, password,
            'res.currency', 'search',
            [[['name', '=', currency_code]]]
        )
        if currency_ids:
            return currency_ids[0]
    return None

def clean_account_code(code):
    """แปลงรหัสบัญชีให้เป็น string และตัด .0 ถ้าเป็นตัวเลขเต็ม"""
    try:
        if pd.notna(code) and float(code).is_integer():
            return str(int(float(code)))
    except Exception:
        pass
    return str(code).strip()

def prepare_account_data(row, models, uid):
    """เตรียมข้อมูลบัญชีสำหรับสร้างหรืออัพเดท"""
    account_type = get_account_type(row['account_type'])
    
    account_data = {
        'code': clean_account_code(row['code']),
        'name': str(row['name']).strip() if pd.notna(row['name']) else '',
        'account_type': account_type,
    }
    
    # เพิ่ม company_id ถ้ามีการระบุ
    if company_id:
        account_data['company_id'] = company_id

    # Set reconcile=True for receivable and payable accounts
    if account_type in ['asset_receivable', 'liability_payable']:
        account_data['reconcile'] = True
    elif 'reconcile' in row and pd.notna(row['reconcile']):
        account_data['reconcile'] = bool(row['reconcile'])

    # เพิ่มข้อมูล currency_id ถ้ามี
    if 'currency_id' in row and pd.notna(row['currency_id']):
        currency_id = get_currency_id(row['currency_id'], models, uid)
        code_cleaned = clean_account_code(row['code'])
        if currency_id:
            account_data['currency_id'] = currency_id
            logger.info(f"กำหนด currency_id {currency_id} สำหรับบัญชี {code_cleaned}")
        else:
            logger.warning(f"ไม่พบสกุลเงิน '{row['currency_id']}' ในระบบสำหรับบัญชี {code_cleaned}")

    return account_data

def import_or_update_accounts(company_name_or_id=None):
    """นำเข้าหรืออัพเดทข้อมูลบัญชี
    
    Args:
        company_name_or_id: ชื่อหรือ ID ของบริษัทที่ต้องการนำเข้า (ถ้าไม่ระบุจะใช้ค่าจาก config หรือ company เริ่มต้น)
    """
    try:
        # โหลด config file
        load_config()
        
        # เชื่อมต่อกับ Odoo
        uid, models = connect_to_odoo()
        if not uid or not models:
            return
        
        # ดึง company_id
        get_company_id(models, uid, company_name_or_id)

        # อ่านไฟล์ Excel
        file_path = Path(r"Data_file/Chart_Of_Account_kyld2.xlsx")
        logger.info(f"กำลังอ่านไฟล์ Excel: {file_path}")
        
        df = pd.read_excel(file_path)
        logger.info(f"พบคอลัมน์ในไฟล์: {', '.join(df.columns.tolist())}")

        # สถิติการนำเข้า
        stats = {'total': 0, 'created': 0, 'updated': 0, 'errors': 0}

        # ดึงข้อมูลบัญชีทั้งหมดที่มีในระบบ (กรองตาม company ถ้ามี)
        domain = []
        if company_id:
            domain.append(['company_id', '=', company_id])
            logger.info(f"กรองบัญชีสำหรับ company_id: {company_id}")
        
        existing_accounts = models.execute_kw(db, uid, password,
            'account.account', 'search_read',
            [domain], {'fields': ['code', 'id', 'company_id']}
        )
        
        # สร้าง dictionary ของบัญชีที่มีอยู่ (รวม company_id ในการเปรียบเทียบ)
        if company_id:
            existing_account_dict = {
                acc['code']: acc['id'] for acc in existing_accounts 
                if acc.get('company_id') and acc['company_id'][0] == company_id
            }
        else:
            existing_account_dict = {acc['code']: acc['id'] for acc in existing_accounts}

        # ประมวลผลแต่ละรายการในไฟล์ Excel
        for index, row in df.iterrows():
            try:
                stats['total'] += 1
                
                # ข้ามแถวที่ว่าง
                if row.isna().all():
                    continue

                # อ่านรหัสบัญชี
                if not pd.notna(row['code']):
                    logger.warning(f"ข้ามแถวที่ {index + 2}: ไม่มีรหัสบัญชี")
                    stats['errors'] += 1
                    continue

                account_code = clean_account_code(row['code'])
                account_data = prepare_account_data(row, models, uid)

                try:
                    # ตรวจสอบว่ามีบัญชีนี้ในระบบหรือไม่
                    if account_code in existing_account_dict:
                        # อัพเดทบัญชีที่มีอยู่
                        models.execute_kw(db, uid, password, 'account.account', 'write', [
                            [existing_account_dict[account_code]], account_data
                        ])
                        logger.info(f"อัพเดทบัญชี: {account_code} - {account_data['name']}")
                        stats['updated'] += 1
                    else:
                        # สร้างบัญชีใหม่
                        new_account_id = models.execute_kw(db, uid, password, 'account.account', 'create', [account_data])
                        logger.info(f"สร้างบัญชีใหม่: {account_code} - {account_data['name']}")
                        stats['created'] += 1

                except Exception as e:
                    logger.error(f"เกิดข้อผิดพลาดในการบันทึกบัญชี {account_code}: {str(e)}")
                    stats['errors'] += 1

            except Exception as e:
                logger.error(f"เกิดข้อผิดพลาดในการประมวลผลแถวที่ {index + 2}: {str(e)}")
                stats['errors'] += 1

        # แสดงสรุปผล
        logger.info("\n=== สรุปผลการนำเข้า ===")
        logger.info(f"จำนวนรายการทั้งหมด: {stats['total']}")
        logger.info(f"สร้างบัญชีใหม่: {stats['created']}")
        logger.info(f"อัพเดทบัญชี: {stats['updated']}")
        logger.info(f"เกิดข้อผิดพลาด: {stats['errors']}")
        logger.info("=====================")

    except Exception as e:
        logger.error(f"เกิดข้อผิดพลาดในการนำเข้า: {str(e)}")
        raise

if __name__ == "__main__":
    try:
        # รับ company_id จาก command line argument (ถ้ามี)
        company_arg = sys.argv[1] if len(sys.argv) > 1 else None
        
        if company_arg:
            logger.info(f"เริ่มต้นการนำเข้าและอัพเดทผังบัญชีสำหรับ company: {company_arg}")
        else:
            logger.info("เริ่มต้นการนำเข้าและอัพเดทผังบัญชี...")
        
        import_or_update_accounts(company_arg)
        logger.info("การนำเข้าและอัพเดทเสร็จสมบูรณ์")
    except Exception as e:
        logger.error(f"เกิดข้อผิดพลาด: {str(e)}")
        sys.exit(1)