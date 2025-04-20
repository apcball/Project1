import xmlrpc.client
import pandas as pd
import sys
import re
from datetime import datetime
import csv
import os
import time
import logging
from typing import List, Dict, Any, Tuple
from concurrent.futures import ThreadPoolExecutor
import threading
from functools import lru_cache
import gc

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Constants
BATCH_SIZE = 30   # Optimized for faster processing with smaller batches
MAX_WORKERS = 6   # Balanced number of workers
CACHE_SIZE = 5000 # Optimized cache size
MAX_RETRIES = 3   # Maximum number of retry attempts
RETRY_DELAY = 1   # Delay between retries in seconds
CONNECTION_TIMEOUT = 30  # Reduced connection timeout for faster response
CONNECTION_POOL_SIZE = 8  # Optimized pool size
KEEPALIVE_INTERVAL = 20  # Reduced interval for more responsive connections
MAX_IDLE_TIME = 180  # Reduced idle time (3 minutes)

# Memory optimization settings
CHUNK_SIZE = 1000  # Size for reading Excel chunks
GC_THRESHOLD = 5000  # Records processed before forcing garbage collection

# Create log directory if it doesn't exist
if not os.path.exists('logs'):
    os.makedirs('logs')

# Initialize lists to store successful and failed imports with thread safety
failed_imports_lock = threading.Lock()
failed_imports = []
error_messages = []

# Performance monitoring
class PerformanceMonitor:
    def __init__(self):
        self.start_time = None
        self.records_processed = 0
        self.lock = threading.Lock()
        self.last_gc_count = 0
        self.last_check_time = time.time()
        self.processing_times = []  # Track processing time per batch
        self.memory_usage = []      # Track memory usage
        
    def start(self):
        self.start_time = time.time()
        self.last_check_time = time.time()
        
    def increment(self, count=1):
        with self.lock:
            current_time = time.time()
            self.records_processed += count
            
            # Track processing time for this batch
            batch_time = current_time - self.last_check_time
            self.processing_times.append(batch_time)
            
            # Track memory usage
            memory_used = psutil.Process().memory_info().rss / 1024 / 1024  # MB
            self.memory_usage.append(memory_used)
            
            # Perform memory cleanup if needed
            if self.records_processed % 1000 == 0:
                self._cleanup_memory()
            
            self.last_check_time = current_time
            
    def _cleanup_memory(self):
        # Force garbage collection if memory usage is high
        if len(self.memory_usage) > 2 and self.memory_usage[-1] > self.memory_usage[-2] * 1.5:
            gc.collect()
            self.last_gc_count = gc.get_count()[0]
            
    def get_stats(self):
        if self.start_time is None:
            return "Processing not started"
            
        current_time = time.time()
        elapsed_time = current_time - self.start_time
        records_per_second = self.records_processed / elapsed_time if elapsed_time > 0 else 0
        
        # Calculate performance metrics
        recent_speed = 0
        if len(self.processing_times) > 10:
            recent_times = self.processing_times[-10:]
            recent_speed = len(recent_times) / sum(recent_times)
        
        # Memory usage stats
        current_memory = psutil.Process().memory_info().rss / 1024 / 1024  # MB
        peak_memory = max(self.memory_usage) if self.memory_usage else 0
        
        stats = (
            f"Processed {self.records_processed} records in {elapsed_time:.2f} seconds\n"
            f"Overall speed: {records_per_second:.2f} records/sec\n"
            f"Recent speed: {recent_speed:.2f} records/sec\n"
            f"Current memory: {current_memory:.1f} MB\n"
            f"Peak memory: {peak_memory:.1f} MB"
        )
        return stats

performance_monitor = PerformanceMonitor()

def log_error(po_name, line_number, product_code, error_message, row_index=None):
    """Log error details for failed imports with thread safety"""
    with failed_imports_lock:
        error_entry = {
            'PO Number': po_name,
            'Line Number': line_number,
            'Product Code': product_code,
            'Error Message': error_message,
            'Date Time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'Excel Row': f'Row {row_index}' if row_index is not None else 'N/A'
        }
        failed_imports.append(error_entry)
        error_messages.append(f"Error in PO {po_name}, Line {line_number}, Row {row_index}: {error_message}")
        logger.error(f"Import error - PO: {po_name}, Line: {line_number}, Row: {row_index}, Error: {error_message}")

def optimize_dataframe(df):
    """Optimize DataFrame memory usage"""
    for col in df.columns:
        if df[col].dtype == 'object':
            df[col] = pd.Categorical(df[col])
        elif df[col].dtype == 'float64':
            df[col] = pd.to_numeric(df[col], downcast='float')
        elif df[col].dtype == 'int64':
            df[col] = pd.to_numeric(df[col], downcast='integer')
    return df

def save_error_log():
    """Save error log to Excel file with enhanced memory optimization"""
    if failed_imports:
        try:
            # Create DataFrame in optimized chunks
            chunk_size = CHUNK_SIZE
            chunks = [failed_imports[i:i + chunk_size] for i in range(0, len(failed_imports), chunk_size)]
            
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            log_file = f'logs/import_errors_{timestamp}.xlsx'
            
            # Process first chunk with optimization
            df = optimize_dataframe(pd.DataFrame(chunks[0]))
            df.to_excel(log_file, index=False, engine='openpyxl')
            del df  # Free memory explicitly
            
            # Process remaining chunks
            if len(chunks) > 1:
                with pd.ExcelWriter(log_file, mode='a', engine='openpyxl') as writer:
                    for chunk in chunks[1:]:
                        df = optimize_dataframe(pd.DataFrame(chunk))
                        df.to_excel(writer, index=False, header=False)
                        del df  # Free memory explicitly
                        gc.collect()  # Force garbage collection
            
            logger.info(f"Error log saved to: {log_file}")
            
            # Print error summary efficiently
            logger.info("\nError Summary:")
            for msg in error_messages[-100:]:  # Show only last 100 errors
                logger.info(msg)
                
        except Exception as e:
            logger.error(f"Error saving log file: {str(e)}")
            
        finally:
            # Aggressive memory cleanup
            gc.collect()
            gc.collect()

# --- Connection Settings ---
url = 'http://mogth.work:8069/'
db = 'MOG_LIVE'
username = 'apichart@mogen.co.th'
password = '471109538'

# --- Data File Settings ---
excel_file = 'Data_file/import_OB4.xlsx'

class OdooConnection:
    _instances = {}  # Connection pool
    _pool_lock = threading.Lock()
    _last_keepalive = {}  # Track last keepalive for each connection
    _request_counts = {}  # Track request counts per connection
    _connection_semaphore = threading.Semaphore(CONNECTION_POOL_SIZE)
    _connection_stats = {}  # Track connection performance

    @classmethod
    def get_instance(cls):
        """Get or create a connection from the pool with enhanced monitoring"""
        thread_id = threading.get_ident()
        current_time = time.time()
        
        with cls._pool_lock:
            # Check if we need to clean up old connections
            cls._cleanup_old_connections()
            
            if thread_id in cls._instances:
                instance = cls._instances[thread_id]
                # Check connection health
                if not cls._is_connection_healthy(instance, thread_id):
                    instance = cls._create_new_instance(thread_id)
                return instance
            else:
                return cls._create_new_instance(thread_id)

    @classmethod
    def _is_connection_healthy(cls, instance, thread_id):
        """Check if connection is healthy and performing well"""
        current_time = time.time()
        
        # Check basic connection
        if not instance.check_connection():
            return False
            
        # Check if connection is too old
        connection_age = current_time - instance._session_start
        if connection_age > MAX_IDLE_TIME:
            return False
            
        # Check request count
        request_count = cls._request_counts.get(thread_id, 0)
        if request_count > instance.max_requests:
            return False
            
        # Check response times
        stats = cls._connection_stats.get(thread_id, {})
        recent_response_times = stats.get('response_times', [])
        if recent_response_times and sum(recent_response_times[-5:]) / 5 > 2.0:  # If avg > 2 sec
            return False
            
        return True

    @classmethod
    def _cleanup_old_connections(cls):
        """Remove old or underperforming connections"""
        current_time = time.time()
        to_remove = []
        
        for thread_id, instance in cls._instances.items():
            if (current_time - instance._session_start > MAX_IDLE_TIME or
                cls._request_counts.get(thread_id, 0) > instance.max_requests):
                to_remove.append(thread_id)
                
        for thread_id in to_remove:
            del cls._instances[thread_id]
            cls._request_counts.pop(thread_id, None)
            cls._last_keepalive.pop(thread_id, None)
            cls._connection_stats.pop(thread_id, None)

    def __init__(self):
        super().__init__()
        self._session_start = time.time()
        self._request_count = 0
        self.max_requests = 500  # Reduced from 1000 to prevent degradation
        self._response_times = []

    @classmethod
    def _create_new_instance(cls, thread_id):
        """Create a new connection instance"""
        if len(cls._instances) >= CONNECTION_POOL_SIZE:
            # Remove oldest connection if pool is full
            oldest_thread = min(cls._instances.items(), key=lambda x: x[1]._last_activity)[0]
            del cls._instances[oldest_thread]
            del cls._last_keepalive[oldest_thread]
        
        instance = cls()
        cls._instances[thread_id] = instance
        cls._last_keepalive[thread_id] = time.time()
        return instance

    def check_connection(self):
        """Check if connection is still valid"""
        try:
            if not self.uid or not self.models:
                return False
            # Try a lightweight operation to test connection
            self.models.execute_kw(self.db, self.uid, self.password, 'res.users', 'search_count', [[['id', '=', self.uid]]])
            return True
        except:
            return False

    def __init__(self):
        self.url = url
        self.db = db
        self.username = username
        self.password = password
        self.uid = None
        self.models = None
        self._connection_lock = threading.Lock()
        self._last_activity = time.time()
        self.timeout = CONNECTION_TIMEOUT
        self._session_start = time.time()
        self._request_count = 0
        self.max_requests = 1000  # Reset connection after this many requests
        
    def _create_transport(self):
        """Create custom transport class with timeout"""
        class TimeoutTransport(xmlrpc.client.Transport):
            def __init__(self):
                super().__init__()
                self.timeout = 30

            def make_connection(self, host):
                connection = super().make_connection(host)
                if hasattr(connection, '_conn'):
                    connection._conn.timeout = self.timeout
                else:
                    connection.timeout = self.timeout
                return connection
        return TimeoutTransport()

    def connect(self):
        """Create a new connection to Odoo with timeout handling"""
        try:
            # Create connection with custom transport
            common = xmlrpc.client.ServerProxy(
                f'{self.url}/xmlrpc/2/common',
                transport=self._create_transport()
            )
            
            # Test connection
            common.version()
            
            # Authenticate
            self.uid = common.authenticate(self.db, self.username, self.password, {})
            if not self.uid:
                print("Authentication failed")
                return False
            
            # Create models proxy
            self.models = xmlrpc.client.ServerProxy(
                f'{self.url}/xmlrpc/2/object',
                transport=self._create_transport()
            )
            
            self._last_activity = time.time()
            print(f"Connection successful, uid = {self.uid}")
            return True
            
        except Exception as e:
            print(f"Connection error: {str(e)}")
            return False

    def ensure_connected(self):
        """Ensure connection is active and fresh"""
        with self._connection_lock:
            current_time = time.time()
            
            # Check if connection is too old or has been idle too long
            if (current_time - self._session_start > 3600 or  # 1 hour session limit
                current_time - self._last_activity > MAX_IDLE_TIME):
                print("Connection expired, reconnecting...")
                return self.connect()
                
            # Check if too many requests have been made
            if self._request_count > self.max_requests:
                print("Request limit reached, reconnecting...")
                return self.connect()
                
            # Verify connection is still valid
            if not self.check_connection():
                print("Connection check failed, reconnecting...")
                return self.connect()
                
            return True

    def execute(self, model, method, *args, **kwargs):
        """Execute Odoo method with automatic reconnection and improved retry logic"""
        self._request_count += 1
        
        # Reset connection if too many requests or too old
        if (self._request_count > self.max_requests or 
            time.time() - self._session_start > 3600):  # 1 hour
            self.connect()
            self._request_count = 0
            self._session_start = time.time()

        for attempt in range(MAX_RETRIES):
            try:
                if not self.ensure_connected():
                    raise Exception("Connection failed")
                    
                result = self.models.execute_kw(
                    self.db, self.uid, self.password,
                    model, method, args, kwargs
                )
                self._last_activity = time.time()
                return result
                
            except xmlrpc.client.Fault as e:
                logger.error(f"XMLRPC Fault: {str(e)}")
                if attempt == MAX_RETRIES - 1:
                    raise
                    
            except (ConnectionError, TimeoutError) as e:
                logger.warning(f"Connection error (attempt {attempt + 1}/{MAX_RETRIES}): {str(e)}")
                if attempt == MAX_RETRIES - 1:
                    raise
                    
            except Exception as e:
                logger.error(f"General error (attempt {attempt + 1}/{MAX_RETRIES}): {str(e)}")
                if attempt == MAX_RETRIES - 1:
                    raise
                
                # Exponential backoff
                wait_time = RETRY_DELAY * (2 ** attempt)
                time.sleep(wait_time)
                
                # Force reconnection on next attempt
                self.connect()

def safe_float_convert(value):
    """Safely convert value to float, handling special cases"""
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        # Handle special cases
        value = value.strip()
        if value in ['-', '', 'N/A', 'NA', 'None', 'null']:
            return 0.0
        try:
            # Remove any thousand separators and convert
            value = value.replace(',', '')
            return float(value)
        except ValueError:
            logger.warning(f"Could not convert '{value}' to float, using 0.0")
            return 0.0
    return 0.0

def connect_to_odoo():
    """Create a new connection to Odoo with timeout handling - Legacy support"""
    global uid, models
    if odoo_connection.connect():
        uid = odoo_connection.uid
        models = odoo_connection.models
        return uid, models
    return None, None

def ensure_connection():
    """Ensure connection is active - Legacy support"""
    global uid, models
    if odoo_connection.ensure_connected():
        uid = odoo_connection.uid
        models = odoo_connection.models
        return True
    return False

# Initialize global connection
odoo_connection = OdooConnection.get_instance()

# Initial connection
uid, models = connect_to_odoo()
if not uid or not models:
    logger.error("Initial connection failed")
    sys.exit(1)

@lru_cache(maxsize=1000)
def search_vendor(partner_name=None, partner_code=None, partner_id=None):
    """Search for vendor in Odoo. If not found, create a new one."""
    try:
        if not partner_id or pd.isna(partner_id):
            logger.warning("No vendor information provided")
            return False

        vendor_name = str(partner_id).strip()
        
        # Search for existing vendor
        try:
            vendor_ids = models.execute_kw(
                db, uid, password, 'res.partner', 'search',
                [[['name', '=', vendor_name]]]
            )
        except Exception as e:
            logger.error(f"Error searching vendor: {e}")
            if not ensure_connection():
                return False
            return False
        
        if vendor_ids:
            logger.info(f"Found existing vendor: {vendor_name}")
            return vendor_ids[0]
        
        # If vendor not found, create a new one
        logger.info(f"Vendor not found: {vendor_name}. Creating new vendor...")
        vendor_data = {
            'name': vendor_name,
            'company_type': 'company',
            'supplier_rank': 1,
            'customer_rank': 0,
            'is_company': True,
        }
        
        try:
            new_vendor_id = models.execute_kw(
                db, uid, password, 'res.partner', 'create', [vendor_data]
            )
            logger.info(f"Successfully created new vendor: {vendor_name} (ID: {new_vendor_id})")
            return new_vendor_id
        except Exception as create_error:
            logger.error(f"Failed to create vendor: {vendor_name}")
            logger.error(f"Creation error: {str(create_error)}")
            if not ensure_connection():
                return False
            return False
        
    except Exception as e:
        error_msg = str(e)
        logger.error(f"Error in search_vendor: {error_msg}")
        log_error('N/A', 'N/A', 'N/A', f"Vendor Search Error: {error_msg}")
        return False

def search_product(product_value):
    """Search for product in Odoo using multiple search strategies"""
    if not isinstance(product_value, str):
        product_value = str(product_value)
    
    product_value = product_value.strip()
    
    try:
        # Function to safely execute search
        def safe_search(domain):
            try:
                return models.execute_kw(
                    db, uid, password, 'product.product', 'search',
                    [domain]
                )
            except Exception as e:
                print(f"Error in product search: {e}")
                if not ensure_connection():
                    return []
                return []

        # 1. Try exact match on default_code
        product_ids = safe_search([['default_code', '=', product_value]])
        if product_ids:
            print(f"Found product with default_code: {product_value}")
            return product_ids

        # 2. Try exact match on old_product_code
        product_ids = safe_search([['old_product_code', '=', product_value]])
        if product_ids:
            print(f"Found product with old_product_code: {product_value}")
            return product_ids

        # 3. Try case-insensitive match on default_code
        product_ids = safe_search([['default_code', 'ilike', product_value]])
        if product_ids:
            print(f"Found product with similar default_code: {product_value}")
            return product_ids

        # 4. Try case-insensitive match on old_product_code
        product_ids = safe_search([['old_product_code', 'ilike', product_value]])
        if product_ids:
            print(f"Found product with similar old_product_code: {product_value}")
            return product_ids

        # 5. For BG- codes, try searching without the prefix
        if product_value.upper().startswith('BG-'):
            code_without_prefix = product_value[3:]
            product_ids = safe_search([
                '|',
                ['default_code', 'ilike', code_without_prefix],
                ['old_product_code', 'ilike', code_without_prefix]
            ])
            if product_ids:
                print(f"Found product matching code without BG- prefix: {product_value}")
                return product_ids

        # 6. For MAC codes, try flexible matching
        if 'MAC' in product_value.upper():
            # Remove any spaces and try matching
            clean_code = product_value.upper().replace(' ', '')
            product_ids = safe_search([
                '|',
                ['default_code', 'ilike', clean_code],
                ['old_product_code', 'ilike', clean_code]
            ])
            if product_ids:
                print(f"Found product with cleaned MAC code: {product_value}")
                return product_ids
        
            # 7. Try searching with wildcards for partial matches
            product_ids = safe_search([
                '|',
                ['default_code', 'ilike', f"%{product_value}%"],
                ['old_product_code', 'ilike', f"%{product_value}%"]
            ])
            if product_ids:
                print(f"Found product with partial code match: {product_value}")
                return product_ids
        
        print(f"Product not found: {product_value}")
        log_error('N/A', 'N/A', product_value, f"Product not found in system: {product_value}")
        return []
        
    except Exception as e:
        error_msg = str(e)
        print(f"Error searching product: {error_msg}")
        log_error('N/A', 'N/A', product_value, f"Product Search Error: {error_msg}")
        if not ensure_connection():
            return []
        return []

def convert_date(pd_timestamp):
    """Convert pandas timestamp to string"""
    if pd.notnull(pd_timestamp):
        if isinstance(pd_timestamp, str):
            try:
                # Try to parse string date
                parsed_date = pd.to_datetime(pd_timestamp, format='%d/%m/%Y', dayfirst=True)
                return parsed_date.strftime('%Y-%m-%d %H:%M:%S')
            except:
                return datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        return pd_timestamp.strftime('%Y-%m-%d %H:%M:%S')
    return datetime.now().strftime('%Y-%m-%d %H:%M:%S')  # Return current date if no date provided

def get_tax_id(tax_value):
    """Get tax ID from value"""
    if not tax_value or pd.isna(tax_value):
        return False

    try:
        all_taxes = models.execute_kw(
            db, uid, password, 'account.tax', 'search_read',
            [[['type_tax_use', 'in', ['purchase', 'all']], ['active', '=', True]]],
            {'fields': ['id', 'name', 'amount', 'type_tax_use']}
        )

        if isinstance(tax_value, str):
            tax_value = tax_value.strip()
            if tax_value.endswith('%'):
                tax_percentage = float(tax_value.rstrip('%'))
            else:
                tax_percentage = float(tax_value) * 100
        else:
            tax_percentage = float(tax_value) * 100

        matching_taxes = [tax for tax in all_taxes if abs(tax['amount'] - tax_percentage) < 0.01]
        if matching_taxes:
            tax_id = matching_taxes[0]['id']
            print(f"Found purchase tax {tax_percentage}% with ID: {tax_id}")
            return tax_id

        print(f"Tax not found: {tax_value}")
        return False
    except Exception as e:
        print(f"Error getting tax ID: {e}")
        if not ensure_connection():
            return False
        return False

def search_picking_type(picking_type_value):
    """Search for picking type in Odoo"""
    def get_default_picking_type():
        try:
            picking_type_ids = models.execute_kw(
                db, uid, password, 'stock.picking.type', 'search',
                [[['code', '=', 'incoming'], ['warehouse_id', '!=', False]]],
                {'limit': 1}
            )
            if picking_type_ids:
                print("Using default Purchase picking type")
                return picking_type_ids[0]
            return False
        except Exception as e:
            print(f"Error getting default picking type: {e}")
            if not ensure_connection():
                return False
            return False

    if not picking_type_value or pd.isna(picking_type_value):
        return get_default_picking_type()

    picking_type_value = str(picking_type_value).strip()
    
    try:
        # Get all picking types and their warehouses
        try:
            all_picking_types = models.execute_kw(
                db, uid, password, 'stock.picking.type', 'search_read',
                [[['code', '=', 'incoming']]],
                {'fields': ['name', 'warehouse_id']}
            )

            # Get warehouse details
            warehouse_ids = list(set([pt['warehouse_id'][0] for pt in all_picking_types if pt['warehouse_id']]))
            warehouses = models.execute_kw(
                db, uid, password, 'stock.warehouse', 'search_read',
                [[['id', 'in', warehouse_ids]]],
                {'fields': ['id', 'name']}
            )
            warehouse_dict = {w['id']: w['name'] for w in warehouses}
            
            # Try exact match on picking type name
            for pt in all_picking_types:
                if pt['name'].lower() == picking_type_value.lower():
                    print(f"Found picking type by exact name: {picking_type_value}")
                    return pt['id']
                    
            # Try partial match on picking type name
            for pt in all_picking_types:
                if picking_type_value.lower() in pt['name'].lower():
                    print(f"Found picking type by partial name: {picking_type_value}")
                    return pt['id']
            
            # Try warehouse name match
            for pt in all_picking_types:
                if pt['warehouse_id']:
                    warehouse_name = warehouse_dict.get(pt['warehouse_id'][0], '')
                    if picking_type_value.lower() in warehouse_name.lower():
                        print(f"Found picking type by warehouse name: {picking_type_value}")
                        return pt['id']
                    
        except Exception as e:
            print(f"Error searching picking types: {e}")
            if not ensure_connection():
                return False

        # If no match found, get all picking types and print them for debugging
        try:
            all_picking_types = models.execute_kw(
                db, uid, password, 'stock.picking.type', 'search_read',
                [[['code', '=', 'incoming']]],
                {'fields': ['name', 'warehouse_id']}
            )
            print(f"\nAvailable picking types:")
            for pt in all_picking_types:
                print(f"- {pt['name']} (ID: {pt['id']})")
        except Exception as e:
            print(f"Error getting all picking types: {e}")
            if not ensure_connection():
                return False

        print(f"\nCould not find picking type for value: {picking_type_value}")
        return get_default_picking_type()
        
    except Exception as e:
        error_msg = str(e)
        print(f"Error in search_picking_type: {error_msg}")
        return get_default_picking_type()

def create_or_update_po(po_data, row_index=None):
    """Create or update a purchase order in Odoo"""
    try:
        po_name = po_data['name']
        try:
            po_ids = models.execute_kw(
                db, uid, password, 'purchase.order', 'search',
                [[['name', '=', po_name]]]
            )
        except Exception as e:
            error_msg = f"Error searching PO {po_name}: {e}"
            log_error(po_name, 'N/A', 'N/A', error_msg, row_index)
            if not ensure_connection():
                return False
            return False

        if po_ids:
            print(f"Updating existing PO: {po_name}")
            po_id = po_ids[0]
            
            try:
                existing_lines = models.execute_kw(
                    db, uid, password, 'purchase.order.line', 'search_read',
                    [[['order_id', '=', po_id]]],
                    {'fields': ['id', 'product_id', 'product_qty', 'price_unit', 'taxes_id']}
                )
            except Exception as e:
                print(f"Error reading PO lines for {po_name}: {e}")
                if not ensure_connection():
                    return False
                return False
            
            try:
                models.execute_kw(
                    db, uid, password, 'purchase.order', 'write',
                    [po_id, {
                        'partner_id': po_data['partner_id'],
                        'partner_ref': po_data.get('partner_ref', ''),
                        'date_order': po_data['date_order'],
                        'date_planned': po_data['date_planned'],
                        'picking_type_id': po_data['picking_type_id'],
                        'notes': po_data.get('notes', ''),
                    }]
                )
            except Exception as e:
                print(f"Error updating PO {po_name}: {e}")
                if not ensure_connection():
                    return False
                return False
            
            # Process all lines as new lines
            for line in po_data['order_line']:
                try:
                    product_id = line[2].get('product_id')
                    if not product_id:
                        print(f"Warning: Missing product ID in line data for PO {po_name}")
                        continue

                    # Create new line for each entry
                    line_data = line[2].copy()
                    line_data['order_id'] = po_id
                    
                    # Validate quantity before create
                    if 'product_qty' in line_data:
                        qty = safe_float_conversion(line_data['product_qty'])
                        if qty <= 0:
                            print(f"Warning: Invalid quantity in new line: {line_data['product_qty']}")
                            continue
                        line_data['product_qty'] = qty

                    new_line_id = models.execute_kw(
                        db, uid, password, 'purchase.order.line', 'create',
                        [line_data]
                    )
                except Exception as e:
                    print(f"Error processing line for PO {po_name}: {e}")
                    if not ensure_connection():
                        return False
                    continue

            # Remove lines that were not updated (optional - uncomment if needed)
            # unused_lines = [line['id'] for line in existing_lines if line['id'] not in updated_line_ids]
            # if unused_lines:
            #     try:
            #         models.execute_kw(
            #             db, uid, password, 'purchase.order.line', 'unlink',
            #             [unused_lines]
            #         )
            #     except Exception as e:
            #         print(f"Error removing unused lines for PO {po_name}: {e}")
            #         if not ensure_connection():
            #             return False
            
            print(f"Successfully updated PO: {po_name}")
            return True
        else:
            print(f"Creating new PO: {po_name}")
            try:
                po_id = models.execute_kw(
                    db, uid, password, 'purchase.order', 'create',
                    [po_data]
                )
                print(f"Successfully created PO: {po_name}")
                return True
            except Exception as e:
                print(f"Error creating PO {po_name}: {e}")
                if not ensure_connection():
                    return False
                return False
    except Exception as e:
        error_msg = str(e)
        print(f"Error creating/updating PO: {error_msg}")
        log_error(po_data.get('name', 'N/A'), 'N/A', 'N/A', f"PO Creation/Update Error: {error_msg}")
        return False

def safe_float_conversion(value, row_index=None):
    """Safely convert various input formats to float"""
    try:
        if pd.isna(value):
            return 0.0
            
        if isinstance(value, (int, float)):
            return float(value)
            
        if isinstance(value, str):
            # Handle special cases
            value = value.strip()
            if value in ['-', '', 'N/A', 'NA', 'None', 'null']:
                return 0.0
                
            # Remove any currency symbols, thousand separators and spaces
            value = value.replace('฿', '').replace(',', '').replace(' ', '')
            
            if not value:  # If empty after cleaning
                return 0.0
                
            try:
                return float(value)
            except ValueError as e:
                error_msg = f"Could not convert '{value}' to float: {str(e)}"
                logger.warning(error_msg)
                if row_index is not None:
                    log_error('N/A', 'N/A', 'N/A', f"Value conversion error: {error_msg}", row_index)
                return 0.0
                
        return 0.0
        
    except (ValueError, TypeError) as e:
        error_msg = f"Error converting value '{value}' to float: {str(e)}"
        logger.warning(error_msg)
        if row_index is not None:
            log_error('N/A', 'N/A', 'N/A', f"Value conversion error: {error_msg}", row_index)
        return 0.0

def process_po_batch(batch_df, batch_num, total_batches):
    """Process a batch of purchase orders"""
    print(f"\nProcessing batch {batch_num}/{total_batches} ({len(batch_df)} rows)")
    
    success_count = 0
    error_count = 0
    MAX_LINES_PER_PO = 500  # Maximum lines per PO
    
    # Group by PO number within the batch
    for po_name, po_group in batch_df.groupby('name'):
        try:
            print(f"\nProcessing PO: {po_name}")
            
            # Get first row for PO header data
            first_row = po_group.iloc[0]
            
            # Find vendor
            vendor_id = search_vendor(
                partner_name=None,
                partner_code=None,
                partner_id=first_row['partner_id'] if pd.notna(first_row['partner_id']) else None
            )
            
            if not vendor_id:
                error_msg = f"Vendor not found for PO {po_name}"
                log_error(po_name, 'N/A', 'N/A', error_msg, first_row.name)
                print(f"Warning: {error_msg}")
                continue
            
            # Get picking type
            picking_type_id = search_picking_type(first_row['picking_type_id'] if pd.notna(first_row.get('picking_type_id')) else None)
            if not picking_type_id:
                error_msg = f"Could not find picking type for PO {po_name}"
                log_error(po_name, 'N/A', 'N/A', error_msg, first_row.name)
                print(f"Warning: {error_msg}")
                continue
            
            # Process all lines first to check products
            all_lines = []
            valid_lines_count = 0
            
            for _, line in po_group.iterrows():
                # Try to find product by default_code first
                product_ids = search_product(line['default_code']) if pd.notna(line.get('default_code')) else []
                
                # If not found by default_code, try old_product_code
                if not product_ids:
                    product_ids = search_product(line['old_product_code'])
                
                if not product_ids:
                    error_msg = f"Product not found: {line.get('default_code', line['old_product_code'])}"
                    log_error(po_name, valid_lines_count + 1, line.get('default_code', line['old_product_code']), error_msg, line.name)
                    print(error_msg)
                    continue
                
                # Process quantity with improved validation
                quantity = safe_float_conversion(line['product_qty'], line.name)
                if quantity <= 0:
                    error_msg = f"Zero or negative quantity ({line['product_qty']}) for product {line['old_product_code']}"
                    log_error(po_name, valid_lines_count + 1, line['old_product_code'], error_msg, line.name)
                    print(f"Warning: {error_msg}")
                    continue
                
                valid_lines_count += 1
                
                # Prepare the description with note and date_planned if available
                description = str(line['description']) if 'description' in line and pd.notna(line['description']) else line['old_product_code']
                
                # Add date_planned to description if available
                date_planned = convert_date(line['date_planned']) if pd.notna(line['date_planned']) else False
                if date_planned:
                    description = f"{description}\nExpected Arrival: {date_planned}"
                
                # Add note if available
                if 'note' in line and pd.notna(line['note']):
                    description = f"{description}\nNote: {line['note']}"

                line_data = {
                    'product_id': product_ids[0],
                    'name': description,
                    'product_qty': quantity,
                    'price_unit': float(line['price_unit']) if pd.notna(line['price_unit']) else 0.0,
                    'date_planned': date_planned,
                    'taxes_id': [(6, 0, [])]  # Set empty tax (VAT = 0)
                }
                
                all_lines.append((0, 0, line_data))
            
            if valid_lines_count == 0:
                print(f"Warning: No valid lines found for PO {po_name}")
                continue
            
            # Split into multiple POs if needed
            po_count = (len(all_lines) + MAX_LINES_PER_PO - 1) // MAX_LINES_PER_PO
            
            for po_index in range(po_count):
                start_idx = po_index * MAX_LINES_PER_PO
                end_idx = start_idx + MAX_LINES_PER_PO
                current_lines = all_lines[start_idx:end_idx]
                
                # Create PO name with suffix if split
                current_po_name = po_name if po_count == 1 else f"{po_name}-{po_index + 1}"
                
                # Prepare PO data
                po_data = {
                    'name': current_po_name,
                    'partner_id': vendor_id,
                    'partner_ref': first_row.get('partner_ref', ''),
                    'date_order': convert_date(first_row['date_order']),
                    'date_planned': convert_date(first_row['date_planned']),
                    'picking_type_id': picking_type_id,
                    'order_line': current_lines
                }
                
                if create_or_update_po(po_data):
                    success_count += 1
                    print(f"Successfully created PO: {current_po_name} with {len(current_lines)} lines")
                else:
                    error_count += 1
                    log_error(current_po_name, 'N/A', 'N/A', f"Failed to add order lines to PO")
                    print(f"Failed to create/update PO: {current_po_name}")
                
        except Exception as e:
            error_count += 1
            print(f"Error processing PO {po_name}: {str(e)}")
            log_error(po_name, 'N/A', 'N/A', f"Processing Error: {str(e)}")
    
    return success_count, error_count

def main():
    total_success = 0
    total_errors = 0
    
    try:
        # Read Excel file
        df = pd.read_excel(excel_file)
        print(f"\nOriginal Excel columns: {df.columns.tolist()}")
        print(f"\nExcel file '{excel_file}' read successfully. Number of rows = {len(df)}")
        
        # Process in batches
        total_rows = len(df)
        total_batches = (total_rows + BATCH_SIZE - 1) // BATCH_SIZE
        
        print(f"\nProcessing {total_rows} rows in {total_batches} batches (batch size: {BATCH_SIZE})")
        
        # Process each batch
        for batch_num in range(total_batches):
            start_idx = batch_num * BATCH_SIZE
            end_idx = min(start_idx + BATCH_SIZE, total_rows)
            batch_df = df.iloc[start_idx:end_idx]
            
            success_count, error_count = process_po_batch(batch_df, batch_num + 1, total_batches)
            total_success += success_count
            total_errors += error_count
            
            # Print batch summary
            print(f"\nBatch {batch_num + 1} Summary:")
            print(f"Successful POs: {success_count}")
            print(f"Failed POs: {error_count}")
            
            # Optional: Add a small delay between batches to prevent overloading
            if batch_num < total_batches - 1:
                time.sleep(1)  # 1 second delay between batches
        
        # Print final summary
        print("\nFinal Import Summary:")
        print(f"Total Successful POs: {total_success}")
        print(f"Total Failed POs: {total_errors}")
        print(f"Total Processed: {total_success + total_errors}")
        
    except Exception as e:
        print(f"Error in main function: {e}")
        log_error('N/A', 'N/A', 'N/A', f"Main Function Error: {str(e)}")
    
    finally:
        # Save error log if there were any errors
        save_error_log()
        print("\nImport process completed.")

if __name__ == "__main__":
    main()