import openpyxl
import os

def analyze_and_generate_template(input_file="Data_file/product_pricelist.xlsx", output_file="Data_file/import_pricelist_template.xlsx"):
    # ตรวจสอบว่าไฟล์ input มีอยู่หรือไม่
    if not os.path.exists(input_file):
        print(f"ไม่พบไฟล์: {input_file}")
        return

    # โหลด Workbook จากไฟล์ Excel
    try:
        wb_in = openpyxl.load_workbook(input_file)
    except Exception as e:
        print("เกิดข้อผิดพลาดขณะโหลดไฟล์:", e)
        return

    # เลือก sheet "Template" ถ้ามี ถ้าไม่มีก็เลือก sheet แรก
    if "Template" in wb_in.sheetnames:
        ws_in = wb_in["Template"]
    else:
        ws_in = wb_in.active

    # อ่านข้อมูลทั้งหมดจาก sheet (แถวแรกถือเป็น header)
    data_rows = list(ws_in.iter_rows(values_only=True))
    if not data_rows:
        print("ไม่พบข้อมูลในไฟล์")
        return

    input_header = data_rows[0]
    print("สรุปข้อมูลจากไฟล์:", input_file)
    print("ชื่อคอลัมน์ (Header):", input_header)
    print("จำนวนแถวทั้งหมด (รวม header):", len(data_rows))

    # สร้าง mapping สำหรับ header จากไฟล์ input (ชื่อคอลัมน์ -> ดัชนี)
    header_map = {name: idx for idx, name in enumerate(input_header) if name is not None}

    # กำหนด header target สำหรับ Import Pricelist
    target_headers = [
        "Pricelist Name",
        "Pricelist Items/Apply On",
        "Pricelist Items/Product",
        "Pricelist Items/Min. Quantity",
        "Pricelist Items/Start Date",
        "Pricelist Items/End Date",
        "Pricelist Items/Compute Price",
        "Pricelist Items/Fixed Price",
        "Pricelist Items/Percentage Price",
        "Pricelist Items/Based on"
    ]

    # สร้าง Workbook ใหม่สำหรับ template
    wb_out = openpyxl.Workbook()
    ws_out = wb_out.active
    ws_out.title = "Import Pricelist Template"

    # เขียน header ของไฟล์ template
    ws_out.append(target_headers)

    # นำข้อมูลที่ได้ (ข้าม header จาก input) มา map ตาม target header
    for row in data_rows[1:]:
        new_row = []
        for target in target_headers:
            # หาก header target มีในไฟล์ input ให้ดึงค่าออกมา มิฉะนั้นใส่ค่า None
            if target in header_map:
                new_row.append(row[header_map[target]])
            else:
                new_row.append(None)
        ws_out.append(new_row)

    # บันทึกไฟล์ template ที่สร้างขึ้น
    try:
        wb_out.save(output_file)
        print(f"สร้างไฟล์ template สำเร็จแล้ว: {output_file}")
    except Exception as e:
        print("เกิดข้อผิดพลาดในการบันทึกไฟล์:", e)

if __name__ == "__main__":
    analyze_and_generate_template()