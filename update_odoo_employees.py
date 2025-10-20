#!/usr/bin/env python3
"""
Odoo 17 Employee Update API
Script to update employee records in Odoo 17 using XML-RPC API
"""

import xmlrpc.client
import logging
import json
import os
from typing import Dict, List, Any, Optional

# Configure logging
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("odoo_employee_updates.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class OdooEmployeeUpdater:
    """Class to handle employee updates in Odoo 17"""
    
    def __init__(self, url: str, db: str, username: str, password: str):
        """
        Initialize the Odoo connection
        
        Args:
            url: Odoo server URL (e.g., http://localhost:8069)
            db: Database name
            username: Username for authentication
            password: Password or API key for authentication
        """
        self.url = url.rstrip('/')  # Remove trailing slash if present
        self.db = db
        self.username = username
        self.password = password
        self.uid = None
        self.models = None
        self.common = None
        
    def authenticate(self) -> bool:
        """
        Authenticate with the Odoo server
        
        Returns:
            bool: True if authentication successful, False otherwise
        """
        try:
            # Create RPC connections
            common_url = f"{self.url}/xmlrpc/2/common"
            object_url = f"{self.url}/xmlrpc/2/object"
            
            logger.debug(f"Connecting to Odoo server: {self.url}")
            self.common = xmlrpc.client.ServerProxy(common_url)
            self.models = xmlrpc.client.ServerProxy(object_url)
            
            # Test connection first
            try:
                version = self.common.version()
                logger.info(f"Connected to Odoo server (version: {version.get('server_version', 'unknown')})")
            except Exception as conn_error:
                logger.error(f"Failed to connect to Odoo server: {str(conn_error)}")
                return False
            
            # Authenticate
            self.uid = self.common.authenticate(self.db, self.username, self.password, {})
            
            if self.uid:
                logger.info(f"Successfully authenticated as {self.username} (uid: {self.uid})")
                return True
            else:
                logger.error("Authentication failed - Check your credentials")
                return False
                
        except xmlrpc.client.Fault as fault:
            logger.error(f"Odoo authentication error {fault.faultCode}: {fault.faultString}")
            return False
        except xmlrpc.client.ProtocolError as err:
            logger.error(f"Protocol error during authentication: {err.errcode} - {err.errmsg}")
            return False
        except ConnectionRefusedError:
            logger.error("Connection refused - Check if Odoo server is running and accessible")
            return False
        except Exception as e:
            logger.error(f"Unexpected authentication error: {str(e)}")
            return False
    
    def check_access_rights(self, model: str = 'hr.employee', operation: str = 'write') -> bool:
        """
        Check if the authenticated user has rights to perform an operation on a model
        
        Args:
            model: The Odoo model name (default: hr.employee)
            operation: The operation to check (default: write)
            
        Returns:
            bool: True if user has access rights, False otherwise
        """
        if not self.uid:
            logger.error("Not authenticated - Please authenticate first")
            return False
            
        try:
            has_access = self.models.execute_kw(
                self.db, self.uid, self.password, 
                model, 'check_access_rights', 
                [operation], {'raise_exception': False}
            )
            
            if has_access:
                logger.debug(f"User has {operation} access to {model} model")
            else:
                logger.warning(f"User does not have {operation} access to {model} model")
                
            return has_access
        except xmlrpc.client.Fault as fault:
            logger.error(f"Odoo error checking access rights {fault.faultCode}: {fault.faultString}")
            return False
        except Exception as e:
            logger.error(f"Error checking access rights: {str(e)}")
            return False
    
    def search_employee(self, search_domain: List[List]) -> List[int]:
        """
        Search for employees based on a domain filter
        
        Args:
            search_domain: Odoo domain filter (e.g., [['name', '=', 'John Doe']])
            
        Returns:
            List[int]: List of employee IDs matching the search criteria
        """
        if not self.uid:
            logger.error("Not authenticated - Please authenticate first")
            return []
            
        try:
            logger.debug(f"Searching employees with domain: {search_domain}")
            employee_ids = self.models.execute_kw(
                self.db, self.uid, self.password,
                'hr.employee', 'search',
                [search_domain]
            )
            
            logger.info(f"Found {len(employee_ids)} employee(s) matching search criteria")
            return employee_ids
        except xmlrpc.client.Fault as fault:
            logger.error(f"Odoo error searching employees {fault.faultCode}: {fault.faultString}")
            return []
        except Exception as e:
            logger.error(f"Error searching employees: {str(e)}")
            return []
    
    def read_employees(self, employee_ids: List[int], fields: List[str] = []) -> List[Dict]:
        """
        Read employee data for given IDs
        
        Args:
            employee_ids: List of employee IDs to read
            fields: List of fields to retrieve (empty list means all fields)
            
        Returns:
            List[Dict]: List of employee data
        """
        if not self.uid:
            logger.error("Not authenticated - Please authenticate first")
            return []
            
        if not employee_ids:
            logger.warning("No employee IDs provided for reading")
            return []
            
        try:
            logger.debug(f"Reading {len(employee_ids)} employee(s) with fields: {fields}")
            employees = self.models.execute_kw(
                self.db, self.uid, self.password,
                'hr.employee', 'read',
                [employee_ids], {'fields': fields}
            )
            
            logger.info(f"Successfully read {len(employees)} employee record(s)")
            return employees
        except xmlrpc.client.Fault as fault:
            logger.error(f"Odoo error reading employees {fault.faultCode}: {fault.faultString}")
            return []
        except Exception as e:
            logger.error(f"Error reading employees: {str(e)}")
            return []
    
    def update_employee(self, employee_id: int, employee_data: Dict[str, Any]) -> bool:
        """
        Update a single employee record
        
        Args:
            employee_id: ID of the employee to update
            employee_data: Dictionary containing employee fields to update
            
        Returns:
            bool: True if update successful, False otherwise
        """
        if not self.uid:
            logger.error("Not authenticated - Please authenticate first")
            return False
            
        if not employee_id:
            logger.error("Employee ID is required for update")
            return False
            
        if not employee_data:
            logger.warning("No update data provided")
            return False
            
        try:
            logger.debug(f"Updating employee ID {employee_id} with data: {employee_data}")
            # Update employee record
            result = self.models.execute_kw(
                self.db, self.uid, self.password,
                'hr.employee', 'write',
                [[employee_id], employee_data]
            )
            
            if result:
                logger.info(f"Successfully updated employee ID {employee_id}")
                return True
            else:
                logger.warning(f"Update may not have been applied for employee ID {employee_id}")
                return False
                
        except xmlrpc.client.Fault as fault:
            logger.error(f"Odoo error updating employee ID {employee_id} {fault.faultCode}: {fault.faultString}")
            return False
        except Exception as e:
            logger.error(f"Error updating employee ID {employee_id}: {str(e)}")
            return False
    
    def update_employees_batch(self, employee_updates: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Update multiple employee records in batch
        
        Args:
            employee_updates: List of dictionaries, each containing:
                            - 'id': Employee ID
                            - 'data': Dictionary with fields to update
                            
        Returns:
            Dict: Summary of update results
        """
        results = {
            'successful': [],
            'failed': [],
            'errors': []
        }
        
        for update in employee_updates:
            employee_id = update.get('id')
            employee_data = update.get('data', {})
            
            if not employee_id:
                error_msg = f"Missing employee ID in update data: {update}"
                logger.error(error_msg)
                results['errors'].append(error_msg)
                results['failed'].append(update)
                continue
                
            if not employee_data:
                error_msg = f"No update data provided for employee ID {employee_id}"
                logger.warning(error_msg)
                results['errors'].append(error_msg)
                results['failed'].append(update)
                continue
                
            success = self.update_employee(employee_id, employee_data)
            if success:
                results['successful'].append(update)
            else:
                results['failed'].append(update)
                
        logger.info(f"Batch update completed: {len(results['successful'])} successful, {len(results['failed'])} failed")
        return results

    def create_employee(self, employee_data: Dict[str, Any]) -> Optional[int]:
        """
        Create a new employee record
        
        Args:
            employee_data: Dictionary containing employee fields
            
        Returns:
            int: ID of the newly created employee, or None if failed
        """
        if not self.uid:
            logger.error("Not authenticated - Please authenticate first")
            return None
            
        if not employee_data:
            logger.error("Employee data is required for creation")
            return None
            
        try:
            logger.debug(f"Creating employee with data: {employee_data}")
            # Create employee record
            employee_id = self.models.execute_kw(
                self.db, self.uid, self.password,
                'hr.employee', 'create',
                [employee_data]
            )
            
            if employee_id:
                logger.info(f"Successfully created employee with ID {employee_id}")
                return employee_id
            else:
                logger.error("Failed to create employee")
                return None
                
        except xmlrpc.client.Fault as fault:
            logger.error(f"Odoo error creating employee {fault.faultCode}: {fault.faultString}")
            return None
        except Exception as e:
            logger.error(f"Error creating employee: {str(e)}")
            return None

    def get_employee_fields(self) -> List[str]:
        """
        Get list of available fields for hr.employee model
        
        Returns:
            List[str]: List of field names
        """
        if not self.uid:
            logger.error("Not authenticated - Please authenticate first")
            return []
            
        try:
            fields = self.models.execute_kw(
                self.db, self.uid, self.password,
                'hr.employee', 'fields_get',
                [], {'attributes': ['string', 'help', 'type']}
            )
            
            field_names = list(fields.keys())
            logger.info(f"Retrieved {len(field_names)} fields for hr.employee model")
            return field_names
        except xmlrpc.client.Fault as fault:
            logger.error(f"Odoo error getting employee fields {fault.faultCode}: {fault.faultString}")
            return []
        except Exception as e:
            logger.error(f"Error getting employee fields: {str(e)}")
            return []

def main():
    """Example usage of the OdooEmployeeUpdater class"""
    
    # Configuration - Replace with your Odoo instance details
    ODOO_URL = "http://localhost:8069"
    ODOO_DB = "your_database_name"
    ODOO_USERNAME = "your_username"
    ODOO_PASSWORD = "your_password"  # Can be password or API key
    
    # Create updater instance
    updater = OdooEmployeeUpdater(ODOO_URL, ODOO_DB, ODOO_USERNAME, ODOO_PASSWORD)
    
    # Authenticate
    if not updater.authenticate():
        logger.error("Failed to authenticate with Odoo")
        return
    
    # Check access rights
    if not updater.check_access_rights():
        logger.error("User doesn't have write access to hr.employee model")
        return
    
    # Example 1: Update a single employee by ID
    # employee_id = 1
    # update_data = {
    #     'work_email': 'new.email@company.com',
    #     'work_phone': '+1234567890'
    # }
    # updater.update_employee(employee_id, update_data)
    
    # Example 2: Update multiple employees
    # employee_updates = [
    #     {
    #         'id': 1,
    #         'data': {
    #             'work_email': 'john.doe@company.com',
    #             'work_phone': '+1234567890'
    #         }
    #     },
    #     {
    #         'id': 2,
    #         'data': {
    #             'work_email': 'jane.smith@company.com',
    #             'mobile_phone': '+0987654321'
    #         }
    #     }
    # ]
    # results = updater.update_employees_batch(employee_updates)
    # print(json.dumps(results, indent=2))
    
    # Example 3: Find and update employees by search criteria
    # search_domain = [['name', 'ilike', 'john']]
    # employee_ids = updater.search_employee(search_domain)
    # if employee_ids:
    #     employees_data = updater.read_employees(employee_ids, ['name', 'work_email'])
    #     print("Found employees:", json.dumps(employees_data, indent=2))
    
    logger.info("Odoo Employee Updater initialized successfully. See examples in main() for usage.")

if __name__ == "__main__":
    main()