# Odoo 17 Employee Update API

This project provides a Python API to update employee records in Odoo 17 using the XML-RPC interface.

## Features

- Authentication with Odoo server
- Update single employee records
- Batch update multiple employee records
- Search employees by criteria
- Read employee data
- Create new employee records
- Comprehensive error handling and logging

## Installation

1. Clone this repository
2. Install required dependencies:
   ```
   pip install xmlrpc.client
   ```

## Configuration

1. Copy the example configuration file:
   ```
   cp odoo_config.json.example odoo_config.json
   ```

2. Edit `odoo_config.json` with your Odoo server details:
   ```json
   {
     "odoo": {
       "url": "http://localhost:8069",
       "database": "your_database_name",
       "username": "your_username",
       "password": "your_password"
     }
   }
   ```

## Usage

### As a Module

```python
from update_odoo_employees import OdooEmployeeUpdater

# Initialize the updater
updater = OdooEmployeeUpdater(
    url="http://localhost:8069",
    db="your_database_name",
    username="your_username",
    password="your_password"
)

# Authenticate
if updater.authenticate():
    # Update an employee
    updater.update_employee(1, {
        'work_email': 'new.email@company.com',
        'work_phone': '+1234567890'
    })
```

### Running Tests

To run the test suite:
```
python test_odoo_employees.py
```

## API Methods

### `OdooEmployeeUpdater(url, db, username, password)`
Initialize the updater with Odoo connection details.

### `authenticate()`
Authenticate with the Odoo server. Returns `True` if successful.

### `check_access_rights(model='hr.employee', operation='write')`
Check if the authenticated user has rights to perform an operation on a model.

### `search_employee(search_domain)`
Search for employees based on a domain filter. Returns a list of employee IDs.

### `read_employees(employee_ids, fields=[])`
Read employee data for given IDs. Returns a list of employee data.

### `update_employee(employee_id, employee_data)`
Update a single employee record. Returns `True` if successful.

### `update_employees_batch(employee_updates)`
Update multiple employee records in batch.

### `create_employee(employee_data)`
Create a new employee record. Returns the ID of the newly created employee.

### `get_employee_fields()`
Get list of available fields for the hr.employee model.