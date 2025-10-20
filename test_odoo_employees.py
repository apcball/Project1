#!/usr/bin/env python3
"""
Test script for Odoo 17 Employee Update API
This script demonstrates how to use the update_odoo_employees.py module
"""

import sys
import os
import json

# Add the parent directory to the path so we can import the module
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from update_odoo_employees import OdooEmployeeUpdater

def load_config():
    """Load Odoo configuration from JSON file"""
    config_path = os.path.join(os.path.dirname(__file__), 'odoo_config.json')
    try:
        with open(config_path, 'r') as f:
            config = json.load(f)
        return config['odoo']
    except FileNotFoundError:
        print(f"Configuration file not found at {config_path}")
        print("Please create odoo_config.json with your Odoo connection details")
        return None
    except json.JSONDecodeError as e:
        print(f"Error parsing configuration file: {e}")
        return None
    except KeyError as e:
        print(f"Missing key in configuration file: {e}")
        return None

def test_single_employee_update():
    """Test updating a single employee"""
    print("=== Testing Single Employee Update ===")
    
    # Load configuration
    config = load_config()
    if not config:
        return False
    
    # Configuration
    ODOO_URL = config['url']
    ODOO_DB = config['database']
    ODOO_USERNAME = config['username']
    ODOO_PASSWORD = config['password']
    
    # Create updater instance
    updater = OdooEmployeeUpdater(ODOO_URL, ODOO_DB, ODOO_USERNAME, ODOO_PASSWORD)
    
    # Authenticate
    if not updater.authenticate():
        print("Failed to authenticate with Odoo")
        return False
    
    # Check access rights
    if not updater.check_access_rights():
        print("User doesn't have write access to hr.employee model")
        return False
    
    # Example: Update a single employee by ID
    employee_id = 1  # Replace with actual employee ID
    update_data = {
        'work_email': 'new.email@company.com',
        'work_phone': '+1234567890',
        'mobile_phone': '+0987654321'
    }
    
    success = updater.update_employee(employee_id, update_data)
    if success:
        print(f"Successfully updated employee ID {employee_id}")
    else:
        print(f"Failed to update employee ID {employee_id}")
    
    return success

def test_batch_employee_update():
    """Test updating multiple employees"""
    print("\n=== Testing Batch Employee Update ===")
    
    # Load configuration
    config = load_config()
    if not config:
        return False
    
    # Configuration
    ODOO_URL = config['url']
    ODOO_DB = config['database']
    ODOO_USERNAME = config['username']
    ODOO_PASSWORD = config['password']
    
    # Create updater instance
    updater = OdooEmployeeUpdater(ODOO_URL, ODOO_DB, ODOO_USERNAME, ODOO_PASSWORD)
    
    # Authenticate
    if not updater.authenticate():
        print("Failed to authenticate with Odoo")
        return False
    
    # Check access rights
    if not updater.check_access_rights():
        print("User doesn't have write access to hr.employee model")
        return False
    
    # Example: Update multiple employees
    employee_updates = [
        {
            'id': 1,  # Replace with actual employee ID
            'data': {
                'work_email': 'john.doe@company.com',
                'work_phone': '+1234567890'
            }
        },
        {
            'id': 2,  # Replace with actual employee ID
            'data': {
                'work_email': 'jane.smith@company.com',
                'mobile_phone': '+0987654321'
            }
        }
    ]
    
    results = updater.update_employees_batch(employee_updates)
    print(f"Batch update results:")
    print(f"  Successful: {len(results['successful'])}")
    print(f"  Failed: {len(results['failed'])}")
    print(f"  Errors: {len(results['errors'])}")
    
    return len(results['errors']) == 0

def test_employee_search_and_update():
    """Test searching for employees and then updating them"""
    print("\n=== Testing Employee Search and Update ===")
    
    # Load configuration
    config = load_config()
    if not config:
        return False
    
    # Configuration
    ODOO_URL = config['url']
    ODOO_DB = config['database']
    ODOO_USERNAME = config['username']
    ODOO_PASSWORD = config['password']
    
    # Create updater instance
    updater = OdooEmployeeUpdater(ODOO_URL, ODOO_DB, ODOO_USERNAME, ODOO_PASSWORD)
    
    # Authenticate
    if not updater.authenticate():
        print("Failed to authenticate with Odoo")
        return False
    
    # Check access rights
    if not updater.check_access_rights():
        print("User doesn't have write access to hr.employee model")
        return False
    
    # Search for employees
    search_domain = [['name', 'ilike', 'john']]  # Search for employees with 'john' in their name
    employee_ids = updater.search_employee(search_domain)
    
    if not employee_ids:
        print("No employees found matching search criteria")
        return True
    
    print(f"Found {len(employee_ids)} employee(s) matching search criteria")
    
    # Read employee data
    employees_data = updater.read_employees(employee_ids, ['name', 'work_email'])
    print("Employee data before update:")
    for emp in employees_data:
        print(f"  ID: {emp['id']}, Name: {emp['name']}, Email: {emp.get('work_email', 'N/A')}")
    
    # Update the found employees
    employee_updates = []
    for emp_id in employee_ids:
        update_data = {
            'work_email': f"updated_{emp_id}@company.com",
            'work_phone': f"+123456789{emp_id % 100:02d}"
        }
        employee_updates.append({
            'id': emp_id,
            'data': update_data
        })
    
    results = updater.update_employees_batch(employee_updates)
    print(f"Batch update results:")
    print(f"  Successful: {len(results['successful'])}")
    print(f"  Failed: {len(results['failed'])}")
    print(f"  Errors: {len(results['errors'])}")
    
    return len(results['errors']) == 0

def test_create_employee():
    """Test creating a new employee"""
    print("\n=== Testing Employee Creation ===")
    
    # Load configuration
    config = load_config()
    if not config:
        return False
    
    # Configuration
    ODOO_URL = config['url']
    ODOO_DB = config['database']
    ODOO_USERNAME = config['username']
    ODOO_PASSWORD = config['password']
    
    # Create updater instance
    updater = OdooEmployeeUpdater(ODOO_URL, ODOO_DB, ODOO_USERNAME, ODOO_PASSWORD)
    
    # Authenticate
    if not updater.authenticate():
        print("Failed to authenticate with Odoo")
        return False
    
    # Check access rights for create operation
    if not updater.check_access_rights(operation='create'):
        print("User doesn't have create access to hr.employee model")
        return False
    
    # Example: Create a new employee
    employee_data = {
        'name': 'John Doe',
        'work_email': 'john.doe@company.com',
        'work_phone': '+1234567890',
        'mobile_phone': '+0987654321'
    }
    
    employee_id = updater.create_employee(employee_data)
    if employee_id:
        print(f"Successfully created employee with ID {employee_id}")
        return True
    else:
        print("Failed to create employee")
        return False

def main():
    """Run all tests"""
    print("Odoo Employee API Test Suite")
    print("=" * 40)
    
    # Check if config file exists
    config_path = os.path.join(os.path.dirname(__file__), 'odoo_config.json')
    if not os.path.exists(config_path):
        print("Configuration file not found!")
        print("Please create 'odoo_config.json' with your Odoo connection details.")
        print("You can use 'odoo_config.json.example' as a template.")
        return False
    
    # Run tests
    tests = [
        test_single_employee_update,
        test_batch_employee_update,
        test_employee_search_and_update,
        test_create_employee
    ]
    
    results = []
    for test in tests:
        try:
            result = test()
            results.append(result)
        except Exception as e:
            print(f"Test {test.__name__} failed with exception: {str(e)}")
            results.append(False)
    
    # Summary
    print("\n" + "=" * 40)
    print("Test Summary:")
    passed = sum(results)
    total = len(results)
    print(f"Passed: {passed}/{total}")
    
    if passed == total:
        print("All tests passed!")
    else:
        print(f"{total - passed} test(s) failed.")
    
    return passed == total

if __name__ == "__main__":
    main()