# Chart of Account Import - Multi-Company Support

โปรแกรมนำเข้าผังบัญชีที่รองรับ multi-company สำหรับ Odoo

## ฟีเจอร์ใหม่

- ✅ รองรับการนำเข้าข้อมูลสำหรับหลาย company
- ✅ กรองและจัดการบัญชีตาม company_id
- ✅ รองรับการกำหนด company ผ่าน config file หรือ command line
- ✅ ใช้ default company ของ user ถ้าไม่ระบุ

## วิธีใช้งาน

### 1. ผ่าน Config File (แนะนำ)

สร้างไฟล์ `odoo_config.json`:

```json
{
    "odoo": {
        "url": "http://mogdev.work:8069",
        "database": "KYLD_DEV2",
        "username": "apichart@mogen.co.th",
        "password": "471109538",
        "company_id": 1
    }
}
```

จากนั้นรันคำสั่ง:
```bash
python import_chart_account.py
```

### 2. ผ่าน Command Line

สามารถระบุ company_id หรือชื่อ company ผ่าน command line:

```bash
# ใช้ company_id
python import_chart_account.py 1

# ใช้ชื่อ company
python import_chart_account.py "KYLD Company"
```

### 3. ใช้ Default Company

ถ้าไม่ระบุ company_id จะใช้ company เริ่มต้นของ user ที่เข้าสู่ระบบ:

```bash
python import_chart_account.py
```

## การตั้งค่า Company

### ลำดับความสำคัญ:
1. Command line argument (สูงสุด)
2. Config file (`odoo_config.json`)
3. Default company ของ user (ต่ำสุด)

## ตัวอย่าง Config File

```json
{
    "odoo": {
        "url": "http://mogdev.work:8069",
        "database": "KYLD_DEV2",
        "username": "apichart@mogen.co.th",
        "password": "471109538",
        "company_id": null
    },
    "logging": {
        "level": "INFO",
        "file": "chart_accounts_import.log"
    }
}
```

**หมายเหตุ:** ถ้าตั้ง `company_id: null` จะใช้ company เริ่มต้นของ user

## ข้อมูลที่จัดการ

โปรแกรมจะ:
- ✅ สร้างบัญชีใหม่พร้อม company_id
- ✅ อัพเดทบัญชีที่มีอยู่ในระบบ
- ✅ กรองบัญชีตาม company เพื่อป้องกัน conflict
- ✅ ตรวจสอบและแสดง company ที่กำลังทำงาน

## Log Files

ผลลัพธ์จะถูกบันทึกใน:
- `chart_accounts_import.log` - สำหรับการ import ทั่วไป
- หรือตามที่กำหนดใน config file

## ตัวอย่างการใช้งาน Multi-Company

### สร้างบัญชีสำหรับ Company หลัก (ID: 1)
```bash
python import_chart_account.py 1
```

### สร้างบัญชีสำหรับ Company สาขา (ID: 2)
```bash
python import_chart_account.py 2
```

### ใช้ชื่อ Company
```bash
python import_chart_account.py "KYLD Main Office"
```

## ข้อควรระวัง

1. **Unique Code per Company**: รหัสบัญชีต้องไม่ซ้ำภายใน company เดียวกัน
2. **Currency**: ตรวจสอบให้แน่ใจว่า currency_id ที่ระบุมีในระบบ
3. **Permissions**: user ต้องมีสิทธิ์เข้าถึง company ที่ระบุ
4. **Account Type**: ต้องระบุ account_type ที่ถูกต้องตาม Odoo 17

## Supported Account Types

- `asset_receivable` - ลูกหนี้การค้า
- `liability_payable` - เจ้าหนี้การค้า
- `asset_cash` - เงินสด/ธนาคาร
- `asset_current` - สินทรัพย์หมุนเวียน
- `asset_non_current` - สินทรัพย์ไม่หมุนเวียน
- `asset_fixed` - สินทรัพย์ถาวร
- `liability_current` - หนี้สินหมุนเวียน
- `liability_non_current` - หนี้สินไม่หมุนเวียน
- `equity` - ส่วนของเจ้าของ
- `income` - รายได้
- `expense` - ค่าใช้จ่าย

## การแก้ไขปัญหา

### ไม่พบ company
```
ไม่พบบริษัท 'XXX' ในระบบ
```
**แก้ไข**: ตรวจสอบชื่อ company หรือใช้ company_id แทน

### ไม่มีสิทธิ์เข้าถึง company
```
Access Denied
```
**แก้ไข**: ตรวจสอบว่า user มีสิทธิ์เข้าถึง company ที่ระบุ

### รหัสบัญชีซ้ำ
```
Account code already exists
```
**แก้ไข**: ตรวจสอบว่ารหัสบัญชีไม่ซ้ำภายใน company
