implement api code import_bill.py for import bill in odoo17.

data file = C:\Users\Ball\Documents\Git_apcball\Project1\Import_Accounting\Template_Bill_Refunds.xlsx

core 

api จะเข้าไป อ่านข้อมูลใน data file และ import ข้อมูลใน module accounting > bill, ค้นหา ชื่อ เอกสารในระบบ field = name , data file colume name จะเจอให้ Update ข้อมูล ถ้าไม่เจอให้สร้างใหม่ group ตามชื่อเอกสารม, ค้นหา vender ใช้ field partner_code และ old_partner_code ค้นหา data file colume partner_code และ old_partner_code ถ้าเจออย่างใดอย่างหนึ่ง ให้เลือกใช้, update ข้อมูลตาม field ต่าง ตาม colume

| name | invoice_date | date | partner_code | old_partner_code | journal | partner_id | ref | label | account_id | quantity | price_unit | tax_ids | payment_reference | note |
| ---- | ------------ | ---- | ------------ | ---------------- | ------- | ---------- | --- | ----- | ---------- | -------- | ---------- | ------- | ----------------- | ---- |

** field ใน colume จะตรงกับ field ใน odoo แล้ว สามารถ map ตามชื่อได้เลย

feature

มี dry run mode เปิดปิดได้ง่าย

มี log เก็บ Error เพิ่อใช้ในการ re import ใหม่

แสดง ข้อมูลการ import แบบ real time
