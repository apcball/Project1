implement api code import_so_fixed.py  to import sale order in odoo17.

Data file is Import_SO\Template_SO.xlsx

api จะ ค้นหา เลขที่เอกสาร ในระบบถ้าเจอ ให้ update ข้อมูล ถ้าไม่เจอให้ สร้างเอกสารใหม่ โดยใช้ข้อมูลใน data file. api จะค้นหา partner_code และ old_code_partner ในระบบ colume partner_code, old_code_partner ถ้าเจออย่างใดอย่างหนึ่งให้เลือกมาใช้ ค้นหา default_code และ old_product_code ในระบบ  colume old_product_code, product_id ถ้าเจอให้เลือกมาใช้ 

- การ import ข้อมูล อ้างอิง เลขที่เอกสาร ถ้าเลขที่เอกสารเดียวกัน colume ref_name แสดงว่าเป็นเอกสารใบเดียวกัน จะเพิ่มใน order line ไม่สร้างเอกสารใหม่
- มี mode dry run เปิด ปิด ได้ง่าย
- เลือก data file ได้ง่าย
- เพิ่ม log ในการ import ที่ folder C:\Users\Ball\Documents\Git_apcball\Project1\Import_SO
- แสดงข้อมูลในการ import ข้อมูล

example data is

| ref_name | date_order | commitment_date | client_order_ref | old_code_partner | partner_code | partner_id | partner_shipping_id | pricelist_id | warehouse_id | old_product_code | product_id | product_name | product_uom_qty | price_unit | tax_id | user_id | team_id | discount_fixed | discount | note | tags |
| -------- | ---------- | --------------- | ---------------- | ---------------- | ------------ | ---------- | ------------------- | ------------ | ------------ | ---------------- | ---------- | ------------ | --------------- | ---------- | ------ | ------- | ------- | -------------- | -------- | ---- | ---- |

map data

ref_name = field name (เลขที่เอกสาร), ค้นหาถ้าเจอให้ update ไม่เจอให้สร้างใหม่

date_order = field date_order, 

commitment_date = field commitment_date,

client_order_ref = field client_order_ref

old_code_partner = field old_code_partner (ค้นหาในระบบและเลือกใช้)

partner_code = field partner_code (ค้นหาในระบบและเลือกใช้)

partner_shipping_id = field partner_shipping_id

pricelist_id = field pricelist_id (ค้นหาในระบบและเลือกใช้)

warehouse_id = field warehouse_id  (ค้นหาในระบบและเลือกใช้)

old_product_code = field old_product_code (ค้นหาในระบบและเลือกใช้)

product_id = field default_code (ค้นหาในระบบและเลือกใช้)

product_uom_qty = field product_uom_qty 

price_unit = field price_unit

tax_id = field tax_id (ค้นหาในระบบและเลือกใช้)

user_id = field user_id  (ค้นหาในระบบและเลือกใช้)

team_id = field team_id (ค้นหาในระบบและเลือกใช้) 

discount_fixed = field discount_fixed 

discount = field discount

note = field note

tags = field tags
