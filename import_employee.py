import xmlrpc.client
import pandas as pd
import sys
import os
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Odoo connection parameters
server_url = 'http://mogth.work:8069'
database = 'Test_Import'
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

def get_or_create_department(department_name, parent_department_name=None):
    """ค้นหาหรือสร้างแผนก โดยรองรับการมีแผนกแม่"""
    if pd.isna(department_name):
        return False
    
    try:
        department_name = str(department_name).strip()
        
        # ถ้ามีแผนกแม่ ให้สร้างหรือค้นหาแผนกแม่ก่อน
        parent_id = False
        if parent_department_name and not pd.isna(parent_department_name):
            parent_department_name = str(parent_department_name).strip()
            
            # ค้นหาแผนกแม่
            parent_dept = models.execute_kw(database, uid, password,
                'hr.department', 'search_read',
                [[['name', '=', parent_department_name]]],
                {'fields': ['id']})
            
            if parent_dept:
                parent_id = parent_dept[0]['id']
            else:
                # สร้างแผนกแม่
                parent_id = models.execute_kw(database, uid, password,
                    'hr.department', 'create',
                    [{'name': parent_department_name}])
                logger.info(f"Created new parent department: {parent_department_name}")
        
        # ค้นหาแผนก โดยใช้ทั้งชื่อและแผนกแม่
        domain = [('name', '=', department_name)]
        if parent_id:
            domain.append(('parent_id', '=', parent_id))
        
        department = models.execute_kw(database, uid, password,
            'hr.department', 'search_read',
            [domain],
            {'fields': ['id']})
        
        if department:
            return department[0]['id']
        else:
            # สร้างแผนกใหม่
            dept_data = {
                'name': department_name,
                'parent_id': parent_id
            }
            dept_id = models.execute_kw(database, uid, password,
                'hr.department', 'create',
                [dept_data])
            logger.info(f"Created new department: {department_name} under {parent_department_name if parent_id else 'root'}")
            return dept_id
            
    except Exception as e:
        logger.error(f"Error processing department {department_name}: {str(e)}")
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
                [{'name': job_title}])
            logger.info(f"Created new job position: {job_title}")
            return job_id
    except Exception as e:
        logger.error(f"Error processing job {job_title}: {str(e)}")
        return False

def find_existing_employee(name):
    """ค้นหาพนักงานที่มีอยู่แล้วโดยใช้ชื่อ"""
    try:
        if name and not pd.isna(name):
            employee = models.execute_kw(database, uid, password,
                'hr.employee', 'search_read',
                [[['name', '=', str(name).strip()]]],
                {'fields': ['id']})
            if employee:
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
        if pd.isna(row['name']):
            error_msg = "Missing required field (name)"
            print(f"Row {index + 2}: {error_msg}")
            failed_imports.append({
                'Row': index + 2,
                'Name': row['name'] if pd.notna(row['name']) else '',
                'Error': error_msg
            })
            continue

        # Prepare employee data
        employee_data = {
            'name': str(row['name']).strip(),
            'work_email': str(row['work_email']).strip() if pd.notna(row['work_email']) else False,
            'department_id': False,
            'job_id': False,
            'parent_id': False,
            'active': True,
        }

        # Process department with parent department
        if pd.notna(row['department_id']):
            parent_dept_name = row['parent_department_id'] if pd.notna(row['parent_department_id']) else None
            dept_id = get_or_create_department(row['department_id'], parent_dept_name)
            if dept_id:
                employee_data['department_id'] = dept_id

        # Process job position
        if pd.notna(row['job_id']):
            job_id = get_or_create_job(row['job_id'])
            if job_id:
                employee_data['job_id'] = job_id

        # Process manager (parent)
        if pd.notna(row['parent_id']):
            parent_employee = models.execute_kw(database, uid, password,
                'hr.employee', 'search_read',
                [[['name', '=', str(row['parent_id']).strip()]]],
                {'fields': ['id']})
            if parent_employee:
                employee_data['parent_id'] = parent_employee[0]['id']

        # Find existing employee
        existing_employee_id = find_existing_employee(employee_data['name'])

        if existing_employee_id:
            print(f"\nUpdating existing employee (Row {index + 2}):")
            print(f"  Name: {employee_data['name']}")
            print(f"  Email: {employee_data['work_email']}")
            try:
                models.execute_kw(database, uid, password,
                    'hr.employee', 'write',
                    [[existing_employee_id], employee_data])
                print("  ✓ Updated successfully")
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
                models.execute_kw(database, uid, password,
                    'hr.employee', 'create',
                    [employee_data])
                print("  ✓ Created successfully")
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
            'Name': row['name'] if pd.notna(row['name']) else 'N/A',
            'Error': error_msg
        })

# Save failed imports to Excel file
if failed_imports:
    failed_df = pd.DataFrame(failed_imports)
    timestamp = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
    failed_excel_file = f'Data_file/failed_employee_imports_{timestamp}.xlsx'
    failed_df.to_excel(failed_excel_file, index=False, engine='openpyxl')
    print(f"\nFailed imports saved to: {failed_excel_file}")
    print(f"Number of failed imports: {len(failed_imports)}")
else:
    print("\nAll employees imported successfully!")