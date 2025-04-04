import xmlrpc.client
import pandas as pd
import sys
import os
import logging
from datetime import datetime
from dateutil.parser import parse

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Odoo connection parameters
server_url = 'http://mogth.work:8069'
database = 'MOG_LIVE'
username = 'apichart@mogen.co.th'
password = '471109538'

# Authentication
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

# Create models proxy
try:
    models = xmlrpc.client.ServerProxy(f'{server_url}/xmlrpc/2/object')
except Exception as e:
    print("Error creating XML-RPC models proxy:", e)
    sys.exit(1)

def clean_email(email):
    """ทำความสะอาดและปรับรูปแบบ email"""
    if not email or pd.isna(email):
        return False
    try:
        # ลบช่องว่างและแปลงเป็นตัวพิมพ์เล็ก
        email = str(email).lower().strip()
        
        # ลบช่องว่างระหว่างตัวอักษร
        email = "".join(email.split())
        
        # ถ้าไม่มี @ ให้เติม @mogen.co.th
        if '@' not in email:
            email = f"{email}@mogen.co.th"
            
        return email
    except Exception:
        return False

def find_user_by_id(user_id):
    """ค้นหา User จาก ID โดยตรง"""
    if not user_id or pd.isna(user_id):
        return False
    
    try:
        user_id = int(float(user_id))  # แปลงค่าจาก Excel ที่อาจเป็น float ให้เป็น int
        # ค้นหา user จาก ID โดยตรง
        user = models.execute_kw(database, uid, password,
            'res.users', 'search_read',
            [[('id', '=', user_id)]],
            {'fields': ['id', 'name', 'login', 'email']})
        
        if user:
            logger.info(f"Found user by ID: {user[0]['name']} (ID: {user[0]['id']}, login: {user[0]['login']})")
            return user[0]['id']
        
        logger.info(f"No user found with ID: {user_id}")
        return False
    except Exception as e:
        logger.error(f"Error searching for user with ID {user_id}: {str(e)}")
        return False

def find_user(user_id=None, email=None):
    """ค้นหา User จาก ID หรือ email"""
    try:
        # ค้นหาจาก ID ก่อน
        if user_id:
            user_id_result = find_user_by_id(user_id)
            if user_id_result:
                return user_id_result
        
        # ถ้าไม่เจอจาก ID และมี email ให้ค้นหาจาก email
        if email:
            # ทำความสะอาด email
            email = clean_email(email)
            if not email:
                return False
                
            # ค้นหาจาก login หรือ email แบบไม่สนใจตัวพิมพ์เล็ก/ใหญ่
            user = models.execute_kw(database, uid, password,
                'res.users', 'search_read',
                [['|', ('login', 'ilike', email), ('email', 'ilike', email)]],
                {'fields': ['id', 'name', 'login', 'email']})
                
            if user:
                # ตรวจสอบการตรงกันแบบไม่สนใจตัวพิมพ์เล็ก/ใหญ่
                exact_match = next(
                    (u for u in user if u['login'].lower() == email.lower() or 
                     (u.get('email') and u['email'].lower() == email.lower())),
                    None
                )
                if exact_match:
                    logger.info(f"Found exact user match by email: {exact_match['name']} (login: {exact_match['login']}, email: {exact_match.get('email', 'no email')})")
                    return exact_match['id']
                
                # ถ้าไม่มีที่ตรงกันพอดี ใช้อันแรกที่คล้ายที่สุด
                logger.info(f"Found user with similar email: {user[0]['name']} (login: {user[0]['login']}, email: {user[0].get('email', 'no email')})")
                return user[0]['id']
        
        return False
    except Exception as e:
        logger.error(f"Error searching for user (ID: {user_id}, email: {email}): {str(e)}")
        return False

def link_employee_to_user(employee_id, user_id=None, email=None):
    """เชื่อมโยงพนักงานกับ User โดยใช้ ID หรือ email"""
    try:
        # ค้นหา User
        found_user_id = find_user(user_id, email)
        if not found_user_id:
            logger.info(f"No user found (ID: {user_id}, email: {email})")
            return False
        
        # อัพเดทข้อมูลพนักงานให้เชื่อมกับ User
        models.execute_kw(database, uid, password,
            'hr.employee', 'write',
            [[employee_id], {'user_id': found_user_id}])
        
        # ตรวจสอบว่าการเชื่อมโยงสำเร็จหรือไม่
        employee = models.execute_kw(database, uid, password,
            'hr.employee', 'search_read',
            [[['id', '=', employee_id]]],
            {'fields': ['id', 'name', 'user_id']})
            
        if employee and employee[0].get('user_id') == found_user_id:
            logger.info(f"Successfully linked employee {employee[0]['name']} to user ID: {found_user_id}")
            return True
        else:
            logger.error(f"Failed to verify link between employee ID {employee_id} and user ID {found_user_id}")
            return False
            
    except Exception as e:
        logger.error(f"Error linking employee to user: {str(e)}")
        return False

def get_or_create_department(department_full_name):
    """ค้นหาหรือสร้างแผนก พร้อมความสัมพันธ์แม่-ลูก"""
    if pd.isna(department_full_name):
        return False
    
    try:
        department_full_name = str(department_full_name).strip()
        
        # แยกชื่อฝ่ายและแผนก
        parts = department_full_name.split('/')
        parent_name = parts[0].strip() if len(parts) > 1 else None
        child_name = parts[1].strip() if len(parts) > 1 else department_full_name.strip()
        
        # ค้นหาหรือสร้างแผนกแม่ (ฝ่าย)
        parent_id = False
        if parent_name:
            parent_dept = models.execute_kw(database, uid, password,
                'hr.department', 'search_read',
                [[['name', '=', parent_name]]],
                {'fields': ['id']})
            
            if parent_dept:
                parent_id = parent_dept[0]['id']
            else:
                # สร้างแผนกแม่
                parent_id = models.execute_kw(database, uid, password,
                    'hr.department', 'create',
                    [{'name': parent_name}])
                logger.info(f"Created new parent department: {parent_name}")
        
        # ค้นหาแผนกลูก (แผนก)
        domain = [('name', '=', child_name)]
        if parent_id:
            domain = ['&', ('name', '=', child_name), ('parent_id', '=', parent_id)]
        
        child_dept = models.execute_kw(database, uid, password,
            'hr.department', 'search_read',
            [domain],
            {'fields': ['id']})
        
        if child_dept:
            # ถ้าเจอแผนกลูกที่มีอยู่แล้ว
            return child_dept[0]['id']
        else:
            # สร้างแผนกลูกใหม่
            dept_data = {
                'name': child_name,
                'parent_id': parent_id
            }
            dept_id = models.execute_kw(database, uid, password,
                'hr.department', 'create',
                [dept_data])
            logger.info(f"Created new department: {child_name} under {parent_name if parent_id else 'root'}")
            return dept_id
            
    except Exception as e:
        logger.error(f"Error processing department {department_full_name}: {str(e)}")
        return False

def get_or_create_job(job_title):
    """ค้นหาหรือสร้างตำแหน่งงาน"""
    if pd.isna(job_title):
        return False
    
    try:
        job_title = str(job_title).strip()
        # Search for existing job
        job = models.execute_kw(database, uid, password,
            'hr.job', 'search_read',
            [[['name', '=', job_title]]],
            {'fields': ['id']})
        
        if job:
            return job[0]['id']
        else:
            # Create new job
            job_id = models.execute_kw(database, uid, password,
                'hr.job', 'create',
                [{'name': job_title, 'state': 'recruit'}])
            logger.info(f"Created new job position: {job_title}")
            return job_id
    except Exception as e:
        logger.error(f"Error processing job {job_title}: {str(e)}")
        return False

def get_or_create_work_location(location_name):
    """ค้นหาหรือสร้างสถานที่ทำงาน"""
    if pd.isna(location_name):
        return False
    
    try:
        location_name = str(location_name).strip()
        # Search for existing work location
        location = models.execute_kw(database, uid, password,
            'hr.work.location', 'search_read',
            [[['name', '=', location_name]]],
            {'fields': ['id']})
        
        if location:
            return location[0]['id']
        else:
            # Create new work location
            location_id = models.execute_kw(database, uid, password,
                'hr.work.location', 'create',
                [{'name': location_name, 'active': True}])
            logger.info(f"Created new work location: {location_name}")
            return location_id
    except Exception as e:
        logger.error(f"Error processing work location {location_name}: {str(e)}")
        return False

def parse_date(date_str):
    """แปลงวันที่จาก string เป็น format ที่ถูกต้อง"""
    if pd.isna(date_str):
        return False
    try:
        if isinstance(date_str, datetime):
            return date_str.strftime('%Y-%m-%d')
        return parse(str(date_str)).strftime('%Y-%m-%d')
    except Exception as e:
        logger.error(f"Error parsing date {date_str}: {str(e)}")
        return False

def get_or_create_contract_type(contract_type):
    """ค้นหาหรือสร้างประเภทสัญญาจ้าง"""
    if pd.isna(contract_type):
        return False
    
    try:
        contract_type = str(contract_type).strip()
        # Search for existing contract type
        type_record = models.execute_kw(database, uid, password,
            'hr.contract.type', 'search_read',
            [[['name', '=', contract_type]]],
            {'fields': ['id']})
        
        if type_record:
            return type_record[0]['id']
        else:
            # Create new contract type
            type_id = models.execute_kw(database, uid, password,
                'hr.contract.type', 'create',
                [{'name': contract_type}])
            logger.info(f"Created new contract type: {contract_type}")
            return type_id
    except Exception as e:
        logger.error(f"Error processing contract type {contract_type}: {str(e)}")
        return False

def find_existing_employee(name, email=None):
    """ค้นหาพนักงานที่มีอยู่แล้วโดยใช้ชื่อหรืออีเมล์"""
    try:
        domain = []
        if email and not pd.isna(email):
            domain.append('|')
            domain.append(['work_email', '=', str(email).strip()])
        if name and not pd.isna(name):
            if domain:  # ถ้ามีเงื่อนไขอีเมล์แล้ว
                domain.append(['name', '=', str(name).strip()])
            else:  # ถ้ายังไม่มีเงื่อนไขใดๆ
                domain = [['name', '=', str(name).strip()]]
        
        if not domain:  # ถ้าไม่มีเงื่อนไขใดๆ
            return False
        
        employee = models.execute_kw(database, uid, password,
            'hr.employee', 'search_read',
            [domain],
            {'fields': ['id', 'name', 'work_email']})
        
        if employee:
            logger.info(f"Found existing employee: {employee[0]['name']} ({employee[0].get('work_email', 'no email')})")
            return employee[0]['id']
        return False
    except Exception as e:
        logger.error(f"Error searching for employee: {str(e)}")
        return False

# List to store failed imports
failed_imports = []

# Read Excel file
excel_file = 'Data_file/employee_import.xlsx'
try:
    df = pd.read_excel(excel_file)
    print(f"\nExcel file '{excel_file}' read successfully. Number of rows = {len(df)}")
    print("\nColumns found in Excel:", df.columns.tolist())
    print("\nFirst few rows:")
    print(df.head().to_string())
    print("\n")
except Exception as e:
    print("Failed to read Excel file:", e)
    sys.exit(1)

# Process each row
for index, row in df.iterrows():
    try:
        # Check required fields
        if pd.isna(row['Employee Name']):
            error_msg = "Missing required field (Employee Name)"
            print(f"Row {index + 2}: {error_msg}")
            failed_imports.append({
                'Row': index + 2,
                'Name': row['Employee Name'] if pd.notna(row['Employee Name']) else '',
                'Error': error_msg
            })
            continue

        # Clean email
        work_email = clean_email(row['Work Email']) if pd.notna(row['Work Email']) else False

        # Prepare employee data
        employee_data = {
            'name': str(row['Employee Name']).strip(),
            'work_email': work_email,
            'department_id': False,
            'job_id': False,
            'parent_id': False,
            'active': True,
            'user_id': False,  # จะถูกอัพเดทจาก User_id ในไฟล์ Excel
            
            # Additional fields
            'work_phone': str(row['Work Phone']).strip() if pd.notna(row.get('Work Phone')) else False,
            'mobile_phone': str(row['Mobile Phone']).strip() if pd.notna(row.get('Mobile Phone')) else False,
            'gender': str(row['Gender']).lower() if pd.notna(row.get('Gender')) else False,
            'birthday': parse_date(row['Birthday']) if pd.notna(row.get('Birthday')) else False,
            'identification_id': str(row['Identification ID']).strip() if pd.notna(row.get('Identification ID')) else False,
            'passport_id': str(row['Passport ID']).strip() if pd.notna(row.get('Passport ID')) else False,
            'pin': str(row['PIN']).strip() if pd.notna(row.get('PIN')) else False,
            'work_location_id': False,
            'employee_type': str(row['Employee Type']).strip() if pd.notna(row.get('Employee Type')) else 'employee',
        }

        # ค้นหาและกำหนด user_id จากคอลัมน์ User_id
        if pd.notna(row.get('User_id')):
            user_id = find_user_by_id(row['User_id'])
            if user_id:
                employee_data['user_id'] = user_id
                print(f"  Found matching user ID: {user_id}")
            else:
                print(f"  Warning: User ID {row['User_id']} not found")
        
        # Process work location
        if pd.notna(row.get('Work Location')):
            location_id = get_or_create_work_location(row['Work Location'])
            if location_id:
                employee_data['work_location_id'] = location_id

        # Process department
        if pd.notna(row['Department']):
            dept_id = get_or_create_department(row['Department'])
            if dept_id:
                employee_data['department_id'] = dept_id

        # Process job position
        if pd.notna(row['Job Position']):
            job_id = get_or_create_job(row['Job Position'])
            if job_id:
                employee_data['job_id'] = job_id

        # Process manager (parent)
        if pd.notna(row['Manager']):
            parent_employee = models.execute_kw(database, uid, password,
                'hr.employee', 'search_read',
                [[['name', '=', str(row['Manager']).strip()]]],
                {'fields': ['id']})
            if parent_employee:
                employee_data['parent_id'] = parent_employee[0]['id']

        # Process contract type if provided
        if pd.notna(row.get('Contract Type')):
            contract_type_id = get_or_create_contract_type(row['Contract Type'])
            if contract_type_id:
                employee_data['contract_type_id'] = contract_type_id

        # Find existing employee
        existing_employee_id = find_existing_employee(
            employee_data['name'],
            employee_data['work_email']
        )

        if existing_employee_id:
            print(f"\nUpdating existing employee (Row {index + 2}):")
            print(f"  Name: {employee_data['name']}")
            print(f"  Email: {employee_data['work_email']}")
            try:
                models.execute_kw(database, uid, password,
                    'hr.employee', 'write',
                    [[existing_employee_id], employee_data])
                print("  ✓ Updated successfully")
                if employee_data.get('user_id'):
                    print(f"  ✓ Linked to user: {employee_data['user_id']}")
                else:
                    print("  ℹ No matching user account found")
                
            except Exception as e:
                error_msg = f"Failed to update: {str(e)}"
                print(f"  ✗ {error_msg}")
                failed_imports.append({
                    'Row': index + 2,
                    'Name': employee_data['name'],
                    'Error': error_msg
                })
        else:
            print(f"\nCreating new employee (Row {index + 2}):")
            print(f"  Name: {employee_data['name']}")
            print(f"  Email: {employee_data['work_email']}")
            try:
                # สร้าง employee ใหม่
                new_employee_id = models.execute_kw(database, uid, password,
                    'hr.employee', 'create',
                    [employee_data])
                print("  ✓ Created successfully")
                
                # พยายามเชื่อมโยงกับ user
                if link_employee_to_user(new_employee_id, employee_data['name'], employee_data['work_email']):
                    print("  ✓ Successfully linked with user account")
                else:
                    print("  ℹ No matching user account found or failed to link")
                
            except Exception as e:
                error_msg = f"Failed to create: {str(e)}"
                print(f"  ✗ {error_msg}")
                failed_imports.append({
                    'Row': index + 2,
                    'Name': employee_data['name'],
                    'Error': error_msg
                })

    except Exception as e:
        error_msg = f"Error processing row: {str(e)}"
        print(f"Row {index + 2}: {error_msg}")
        failed_imports.append({
            'Row': index + 2,
            'Name': row['Employee Name'] if pd.notna(row['Employee Name']) else 'N/A',
            'Error': error_msg
        })

# Save failed imports to Excel file
if failed_imports:
    failed_df = pd.DataFrame(failed_imports)
    timestamp = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
    failed_excel_file = f'Data_file/failed_employee_imports_{timestamp}.xlsx'
    
    # Create detailed error report
    error_report = []
    for failed in failed_imports:
        error_report.append({
            'Row': failed['Row'],
            'Name': failed['Name'],
            'Error': failed['Error'],
            'Timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            'Status': 'Failed'
        })
    
    error_df = pd.DataFrame(error_report)
    error_df.to_excel(failed_excel_file, index=False, engine='openpyxl')
    
    print(f"\nImport Summary:")
    print(f"Total records processed: {len(df)}")
    print(f"Successfully imported: {len(df) - len(failed_imports)}")
    print(f"Failed imports: {len(failed_imports)}")
    print(f"\nDetailed error report saved to: {failed_excel_file}")
    
    # Log the summary
    logger.info(f"Import completed - Success: {len(df) - len(failed_imports)}, Failed: {len(failed_imports)}")
else:
    print("\nImport Summary:")
    print(f"Total records processed: {len(df)}")
    print("All employees imported successfully!")
    logger.info(f"Import completed successfully - {len(df)} records processed")