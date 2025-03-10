import xmlrpc.client
import pandas as pd
import sys
import logging
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

# Odoo connection parameters
url = 'http://mogth.work:8069'
db = 'MOG_Traning'
username = 'apichart@mogen.co.th'
password = '471109538'

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

def get_account_type(account_type):
    """แปลงประเภทบัญชีให้ตรงกับ Odoo"""
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
        'other expenses': 'expense_other',
        'cost of revenue': 'expense_direct_cost',
    }
    if not account_type or pd.isna(account_type):
        return 'asset_current'
    return type_mapping.get(str(account_type).lower().strip(), 'asset_current')

def prepare_account_data(row):
    """เตรียมข้อมูลบัญชีสำหรับสร้างหรืออัพเดท"""
    account_data = {
        'code': str(row['code']).strip(),
        'name': str(row['name']).strip() if pd.notna(row['name']) else '',
        'account_type': get_account_type(row['account_type']),
    }

    # เพิ่มข้อมูล reconcile ถ้ามี
    if 'reconcile' in row and pd.notna(row['reconcile']):
        account_data['reconcile'] = bool(row['reconcile'])

    # เพิ่มข้อมูล currency_id ถ้ามี
    if 'currency_id' in row and pd.notna(row['currency_id']):
        currency_id = int(row['currency_id']) if str(row['currency_id']).isdigit() else None
        if currency_id:
            account_data['currency_id'] = currency_id

    return account_data

def import_or_update_accounts():
    """นำเข้าหรืออัพเดทข้อมูลบัญชี"""
    try:
        # เชื่อมต่อกับ Odoo
        uid, models = connect_to_odoo()
        if not uid or not models:
            return

        # อ่านไฟล์ Excel
        file_path = Path(r"C:\Users\Ball\Documents\Git_apcball\Project1\Data_file\Chart_Of_Account.xlsx")
        logger.info(f"กำลังอ่านไฟล์ Excel: {file_path}")
        
        df = pd.read_excel(file_path)
        logger.info(f"พบคอลัมน์ในไฟล์: {', '.join(df.columns.tolist())}")

        # สถิติการนำเข้า
        stats = {'total': 0, 'created': 0, 'updated': 0, 'errors': 0}

        # ดึงข้อมูลบัญชีทั้งหมดที่มีในระบบ
        existing_accounts = models.execute_kw(db, uid, password,
            'account.account', 'search_read',
            [[]], {'fields': ['code', 'id']}
        )
        
        # สร้าง dictionary ของบัญชีที่มีอยู่
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

                account_code = str(row['code']).strip()
                account_data = prepare_account_data(row)

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
        logger.info("เริ่มต้นการนำเข้าและอัพเดทผังบัญชี...")
        import_or_update_accounts()
        logger.info("การนำเข้าและอัพเดทเสร็จสมบูรณ์")
    except Exception as e:
        logger.error(f"เกิดข้อผิดพลาด: {str(e)}")
        sys.exit(1)