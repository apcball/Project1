import xmlrpc.client
import ssl
from datetime import datetime
import openpyxl
import os
from dotenv import load_dotenv

# Odoo connection parameters - define these first
url = 'http://mogdev.work:8069'
db = 'KYLD_DEV2'
username = 'apichart@mogen.co.th'
password = '471109538'

# Load environment variables (in case we need to override settings)
load_dotenv()

# Disable SSL verification
ssl._create_default_https_context = ssl._create_unverified_context

# Connect to Odoo
common = xmlrpc.client.ServerProxy(f'{url}/xmlrpc/2/common')
uid = common.authenticate(db, username, password, {})
models = xmlrpc.client.ServerProxy(f'{url}/xmlrpc/2/object')

def read_excel_file(file_path):
    """Read the Excel file and return the data."""
    try:
        print(f"Opening file: {file_path}")
        wb = openpyxl.load_workbook(file_path, data_only=True)
        sheet = wb.active
        data = []
        
        # Get headers first to verify column structure
        headers = [cell.value for cell in sheet[1]]
        print(f"Found headers: {headers}")
        
        # Skip header row
        for row in sheet.iter_rows(min_row=2, values_only=True):
            if not row or not any(row):  # Skip empty rows
                continue
            
            # Process the category name to extract parent category
            category_name = str(row[0]) if row[0] else None
            if not category_name:
                continue
                
            # Parse the hierarchical category structure
            parts = category_name.split('/')
            parts = [p.strip() for p in parts]
            
            # Get the direct parent (if any) and the actual category name
            parent_category = ' / '.join(parts[:-1]) if len(parts) > 1 else None
            actual_name = parts[-1].strip()
            
            # Set costing method to FIFO by default
            costing_method = 'fifo'
                
            # Clean and format account numbers
            income_account = str(row[2]).strip() if row[2] else False
            expense_account = str(row[3]).strip() if row[3] else False
            
            # Remove commas from account numbers and ensure they are valid
            if income_account and income_account not in ['', 'False', 'None']:
                income_account = income_account.replace(',', '')
                # Verify it's a valid number
                try:
                    int(income_account)
                except:
                    income_account = False
            else:
                income_account = False
                
            if expense_account and expense_account not in ['', 'False', 'None']:
                expense_account = expense_account.replace(',', '')
                # Verify it's a valid number
                try:
                    int(expense_account)
                except:
                    expense_account = False
            else:
                expense_account = False
                
            category_data = {
                'name': actual_name,  # Category Name
                'parent_category': parent_category,  # Parent Category
                'property_cost_method': costing_method.lower(),  # Costing Method
                'property_valuation': 'manual_periodic',  # Disable automatic inventory valuation
                'property_account_income_categ_id': income_account if income_account else False,  # Income Account
                'property_account_expense_categ_id': expense_account if expense_account else False  # Expense Account
            }
            
            # Only append if we have at least a category name
            if category_data['name']:
                data.append(category_data)
        
        return data
    except Exception as e:
        print(f"Error reading Excel file: {str(e)}")
        return None

def get_account_id(account_code):
    """Get account ID from account code."""
    if not account_code:
        return False
        
    account = models.execute_kw(db, uid, password,
        'account.account', 'search_read',
        [[['code', '=', str(account_code)]]],
        {'fields': ['id']}
    )
    return account[0]['id'] if account else False

def get_journal_id(journal_name):
    """Get journal ID from journal name."""
    if not journal_name:
        return False
        
    journal = models.execute_kw(db, uid, password,
        'account.journal', 'search_read',
        [[['name', '=', journal_name]]],
        {'fields': ['id']}
    )
    return journal[0]['id'] if journal else False

def check_existing_category(category_name, parent_id=None):
    """Check if a category exists with the given name and parent."""
    # First, check for exact name match
    domain = [('name', '=', category_name)]
    if parent_id:
        domain.append(('parent_id', '=', parent_id))
    
    existing_category = models.execute_kw(db, uid, password,
        'product.category', 'search_read',
        [domain],
        {'fields': ['id', 'name', 'parent_id', 'complete_name']}
    )
    
    if existing_category:
        return existing_category[0]
        
    # If no exact match found, check by complete name (full path)
    if parent_id:
        # Get parent's complete name
        parent = models.execute_kw(db, uid, password,
            'product.category', 'read',
            [parent_id],
            {'fields': ['complete_name']}
        )
        if parent:
            complete_name = f"{parent[0]['complete_name']} / {category_name}"
            existing_by_path = models.execute_kw(db, uid, password,
                'product.category', 'search_read',
                [[('complete_name', '=', complete_name)]],
                {'fields': ['id', 'name', 'parent_id', 'complete_name']}
            )
            if existing_by_path:
                print(f"Found existing category with complete path: {complete_name}")
                return existing_by_path[0]
    
    return None

def verify_category_path(category_name, parent_category=None):
    """Verify if a category path already exists and return the existing category if found."""
    if parent_category:
        complete_name = f"{parent_category} / {category_name}"
    else:
        complete_name = category_name
        
    existing = models.execute_kw(db, uid, password,
        'product.category', 'search_read',
        [[('complete_name', '=', complete_name)]],
        {'fields': ['id', 'name', 'parent_id', 'complete_name']}
    )
    
    if existing:
        print(f"Found existing category with path: {complete_name}")
        return existing[0]
    return None

def create_or_update_category(category_data, processed_categories):
    """Create or update product category with parent hierarchy."""
    try:
        # First verify if the complete category path exists
        existing_path = verify_category_path(category_data['name'], category_data['parent_category'])
        if existing_path:
            processed_categories[category_data['name']] = existing_path['id']
            print(f"Using existing category: {existing_path['complete_name']}")
            return existing_path['id']

        # Process parent category first if needed
        parent_id = None
        if category_data['parent_category']:
            if category_data['parent_category'] not in processed_categories:
                # Create parent category with basic settings and disable all stock properties
                parent_values = {
                    'name': category_data['parent_category'],
                    'property_cost_method': 'fifo',
                    'property_valuation': 'manual_periodic',  # Manual inventory valuation
                    # Explicitly disable all stock account properties
                    'property_stock_account_input_categ_id': False,
                    'property_stock_account_output_categ_id': False,
                    'property_stock_valuation_account_id': False,
                    'property_stock_journal': False,
                    'property_account_income_categ_id': False,
                    'property_account_expense_categ_id': False
                }
                
                # Check if parent already exists in Odoo
                existing_parent = models.execute_kw(db, uid, password,
                    'product.category', 'search_read',
                    [[['name', '=', category_data['parent_category']]]],
                    {'fields': ['id']}
                )
                
                if existing_parent:
                    parent_id = existing_parent[0]['id']
                    # Update existing parent to ensure stock properties are disabled
                    models.execute_kw(db, uid, password,
                        'product.category', 'write',
                        [[parent_id], parent_values]
                    )
                    processed_categories[category_data['parent_category']] = parent_id
                else:
                    parent_id = models.execute_kw(db, uid, password,
                        'product.category', 'create',
                        [parent_values]
                    )
                    processed_categories[category_data['parent_category']] = parent_id
                    print(f"Created parent category: {category_data['parent_category']}")
            else:
                parent_id = processed_categories[category_data['parent_category']]

        # Check if category already exists
        category_name = category_data['name']
        if category_name in processed_categories:
            return processed_categories[category_name]

        # Prepare category values
        category_values = {
            'name': category_name,
            'property_cost_method': category_data['property_cost_method'],
            'property_valuation': 'manual_periodic',  # Manual inventory valuation
            # Explicitly disable all stock account properties
            'property_stock_account_input_categ_id': False,
            'property_stock_account_output_categ_id': False,
            'property_stock_valuation_account_id': False,
            'property_stock_journal': False,
            'property_stock_account_output_categ_id': False,
            'property_stock_account_input_categ_id': False,
        }

        # Set parent category
        if parent_id:
            category_values['parent_id'] = parent_id

        # Set account properties
        if category_data['property_account_income_categ_id']:
            income_account_id = get_account_id(category_data['property_account_income_categ_id'])
            if income_account_id:
                category_values['property_account_income_categ_id'] = income_account_id
                
        if category_data['property_account_expense_categ_id']:
            expense_account_id = get_account_id(category_data['property_account_expense_categ_id'])
            if expense_account_id:
                category_values['property_account_expense_categ_id'] = expense_account_id

        # Check for existing category with parent
        existing_category = check_existing_category(category_name, parent_id)

        if existing_category:
            # Category exists with same parent - update if needed
            category_id = existing_category['id']
            # Compare existing values with new values before updating
            current_category = models.execute_kw(db, uid, password,
                'product.category', 'read',
                [category_id],
                {'fields': list(category_values.keys())}
            )[0]

            # Check if any values are different
            needs_update = False
            for key, value in category_values.items():
                if current_category.get(key) != value:
                    needs_update = True
                    break

            if needs_update:
                models.execute_kw(db, uid, password,
                    'product.category', 'write',
                    [[category_id], category_values]
                )
                print(f"Updated existing category: {category_name}")
            else:
                print(f"Category already exists with same values: {category_name}")
        else:
            # Create new category
            category_id = models.execute_kw(db, uid, password,
                'product.category', 'create',
                [category_values]
            )
            print(f"Created new category: {category_name}")

        processed_categories[category_name] = category_id
        return category_id

    except Exception as e:
        print(f"Error processing category {category_data['name']}: {str(e)}")
        return None

def main():
    try:
        # Excel file path
        file_path = os.path.join('Data_file', 'KYLD Product Category update R4.xlsx')
        print(f"Looking for file at: {os.path.abspath(file_path)}")
        
        if not os.path.exists(file_path):
            print(f"Error: File not found at {os.path.abspath(file_path)}")
            return
        
        # Read Excel data
        print("Reading Excel file...")
        categories_data = read_excel_file(file_path)
        
        if not categories_data:
            print("Failed to read Excel file or no valid data found.")
            return
            
        print(f"Successfully read {len(categories_data)} categories from Excel file.")
        
        # Process categories
        processed_categories = {}
        print("Processing categories...")
        
        # First pass: create/update all categories
        for category_data in categories_data:
            create_or_update_category(category_data, processed_categories)
            
        print("Import completed successfully!")
        
    except Exception as e:
        print(f"An error occurred: {str(e)}")

    # Process categories
    processed_categories = {}
    print("Processing categories...")
    
    # First pass: create/update all categories
    for category_data in categories_data:
        create_or_update_category(category_data, processed_categories)

    print("Import completed successfully!")

if __name__ == "__main__":
    main()
