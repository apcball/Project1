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
import threading
from contextlib import contextmanager

# Try to import psutil, use alternative if not available
try:
    import psutil
    PSUTIL_AVAILABLE = True
except ImportError:
    PSUTIL_AVAILABLE = False
    import os
    print("Warning: psutil not installed. Using basic memory monitoring.")
    print("To install psutil, run: pip install psutil")

# Configure logging with more detailed format
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(name)s - %(threadName)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
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

# Memory and Performance Monitoring
class MemoryMonitor:
    def __init__(self):
        self.initial_memory = self.get_memory_usage()
        self.memory_samples = []
        self.sample_interval = 5  # seconds
        self.last_sample_time = time.time()
        self.lock = threading.Lock()

    def get_memory_usage(self):
        """Get current memory usage in MB"""
        try:
            if PSUTIL_AVAILABLE:
                process = psutil.Process()
                return process.memory_info().rss / 1024 / 1024
            else:
                # Alternative method using os.getpid() if psutil is not available
                import resource
                return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024  # Convert KB to MB
        except Exception as e:
            logger.warning(f"Could not get memory usage: {str(e)}")
            return 0

    def sample_memory(self):
        """Take a memory sample if interval has passed"""
        current_time = time.time()
        with self.lock:
            if current_time - self.last_sample_time >= self.sample_interval:
                memory_used = self.get_memory_usage()
                self.memory_samples.append({
                    'timestamp': current_time,
                    'memory_mb': memory_used,
                    'delta_mb': memory_used - self.initial_memory,
                    'thread_id': threading.get_ident()
                })
                self.last_sample_time = current_time
                
                # Log warning if memory usage is high
                if memory_used > 1000:  # Warning at 1GB
                    logger.warning(f"High memory usage detected: {memory_used:.2f} MB | Delta: {memory_used - self.initial_memory:.2f} MB | Thread: {threading.get_ident()} | Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}")

    def get_memory_stats(self):
        """Get memory usage statistics"""
        if not self.memory_samples:
            return "No memory samples collected"

        current_memory = self.get_memory_usage()
        peak_memory = max(sample['memory_mb'] for sample in self.memory_samples)
        avg_memory = sum(sample['memory_mb'] for sample in self.memory_samples) / len(self.memory_samples)
        
        # Get memory growth rate (MB/minute)
        if len(self.memory_samples) >= 2:
            first_sample = self.memory_samples[0]
            last_sample = self.memory_samples[-1]
            time_diff_minutes = (last_sample['timestamp'] - first_sample['timestamp']) / 60
            if time_diff_minutes > 0:
                memory_growth_rate = (last_sample['memory_mb'] - first_sample['memory_mb']) / time_diff_minutes
            else:
                memory_growth_rate = 0
        else:
            memory_growth_rate = 0

        stats = {
            'current_mb': round(current_memory, 2),
            'peak_mb': round(peak_memory, 2),
            'average_mb': round(avg_memory, 2),
            'initial_mb': round(self.initial_memory, 2),
            'delta_mb': round(current_memory - self.initial_memory, 2),
            'growth_rate_mb_per_min': round(memory_growth_rate, 2),
            'samples_count': len(self.memory_samples),
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3],
            'thread_id': threading.get_ident()
        }

        # Trigger garbage collection if memory usage is high
        if current_memory > 1000:  # 1GB threshold
            gc.collect()
            stats['after_gc_mb'] = round(self.get_memory_usage(), 2)
            stats['gc_freed_mb'] = round(stats['current_mb'] - stats['after_gc_mb'], 2)

        return stats

class ProgressTracker:
    def __init__(self, total_records):
        self.total_records = total_records
        self.processed_records = 0
        self.start_time = None
        self.last_update_time = None
        self.update_interval = 1  # seconds
        self.lock = threading.Lock()
        self.error_counts = {}
        self.stage_timings = {}
        self.current_stage = None

    def start(self):
        self.start_time = time.time()
        self.last_update_time = self.start_time

    def update(self, count=1, stage=None):
        with self.lock:
            current_time = time.time()
            self.processed_records += count
            
            if stage and stage != self.current_stage:
                if self.current_stage:
                    stage_duration = current_time - self.stage_timings[self.current_stage]['start']
                    self.stage_timings[self.current_stage]['duration'] = stage_duration
                self.current_stage = stage
                self.stage_timings[stage] = {'start': current_time, 'duration': 0}

            if current_time - self.last_update_time >= self.update_interval:
                self._report_progress()
                self.last_update_time = current_time

    def add_error(self, error_type):
        with self.lock:
            self.error_counts[error_type] = self.error_counts.get(error_type, 0) + 1

    def _report_progress(self):
        if not self.start_time:
            return

        current_time = time.time()
        elapsed_time = current_time - self.start_time
        progress = (self.processed_records / self.total_records) * 100 if self.total_records > 0 else 0
        records_per_second = self.processed_records / elapsed_time if elapsed_time > 0 else 0

        # Calculate ETA
        if records_per_second > 0:
            remaining_records = self.total_records - self.processed_records
            eta_seconds = remaining_records / records_per_second
            eta = time.strftime('%H:%M:%S', time.gmtime(eta_seconds))
        else:
            eta = 'Unknown'
            
        # Get memory usage
        memory_usage = 'N/A'
        if PSUTIL_AVAILABLE:
            try:
                process = psutil.Process()
                memory_usage = f"{process.memory_info().rss / 1024 / 1024:.2f} MB"
            except:
                pass

        # Get detailed error breakdown
        error_breakdown = '\n'.join([f"  - {err_type}: {count}" for err_type, count in self.error_counts.items()])
        if not error_breakdown:
            error_breakdown = "  None"

        # Get stage timing information
        stage_timing_info = ''
        for stage, timing in self.stage_timings.items():
            if 'duration' in timing and timing['duration'] > 0:
                stage_timing_info += f"  - {stage}: {timing['duration']:.2f}s\n"
            else:
                stage_timing_info += f"  - {stage}: In progress\n"

        logger.info(
            f"\nDetailed Progress Report at {datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}:\n"
            f"Current Stage: {self.current_stage}\n"
            f"Progress: {progress:.1f}% ({self.processed_records}/{self.total_records})\n"
            f"Speed: {records_per_second:.1f} records/sec\n"
            f"Memory Usage: {memory_usage}\n"
            f"Elapsed Time: {time.strftime('%H:%M:%S', time.gmtime(elapsed_time))}\n"
            f"ETA: {eta}\n"
            f"Total Errors: {sum(self.error_counts.values())}\n"
            f"Error Breakdown:\n{error_breakdown}\n"
            f"Stage Timings:\n{stage_timing_info}"
        )

class PerformanceMonitor:
    def __init__(self):
        self.start_time = None
        self.records_processed = 0
        self.lock = threading.Lock()
        self.last_gc_count = 0
        self.last_check_time = time.time()
        self.processing_times = []  # Track processing time per batch
        self.memory_monitor = MemoryMonitor()
        self.stage_timings = {}
        
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
            
            # Perform memory cleanup if needed
            if self.records_processed % 1000 == 0:
                self._cleanup_memory()
            
            self.last_check_time = current_time
            
    def _cleanup_memory(self):
        # Force garbage collection
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
        
        stats = (
            f"Processed {self.records_processed} records in {elapsed_time:.2f} seconds\n"
            f"Overall speed: {records_per_second:.2f} records/sec\n"
            f"Recent speed: {recent_speed:.2f} records/sec"
        )
        return stats

performance_monitor = PerformanceMonitor()

def log_error(po_name, line_number, product_code, error_message, row_index=None, row_data=None):
    """Log error details for failed imports with thread safety and complete row data"""
    with failed_imports_lock:
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
        error_entry = {
            'PO Number': po_name if po_name != 'N/A' else '',
            'Line Number': line_number if line_number != 'N/A' else '',
            'Product Code': product_code if product_code != 'N/A' else '',
            'Error Message': error_message,
            'Date Time': timestamp,
            'Excel Row': f'Row {row_index}' if row_index is not None else '',
            'Thread ID': threading.get_ident()
        }
        
        # Add complete row data if available
        if row_data is not None and isinstance(row_data, pd.Series):
            for column, value in row_data.items():
                error_entry[f'Original_{column}'] = value
                
        failed_imports.append(error_entry)
        error_messages.append(f"Error in PO {po_name}, Line {line_number}, Row {row_index}: {error_message}")
        
        # Enhanced logging with row data and more context
        log_message = f"Import error - PO: {po_name}, Line: {line_number}, Row: {row_index}, Thread: {threading.get_ident()}, Error: {error_message}"
        if row_data is not None:
            row_dict = dict(row_data)
            # Format row data for better readability
            formatted_row = {k: (str(v)[:50] + '...' if isinstance(v, str) and len(str(v)) > 50 else v) for k, v in row_dict.items()}
            log_message += f"\nComplete Row Data: {formatted_row}"
        logger.error(log_message)

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
    """Save error log to Excel file with enhanced error tracking and complete row data"""
    if failed_imports:
        try:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            log_file = f'logs/import_errors_{timestamp}.xlsx'
            
            with pd.ExcelWriter(log_file, engine='openpyxl') as writer:
                # Sheet 1: Detailed Failed Records with Complete Data
                df_failed = pd.DataFrame(failed_imports)
                
                # Separate original columns and error information
                error_info_cols = ['PO Number', 'Line Number', 'Product Code', 'Error Message', 'Date Time', 'Excel Row']
                original_data_cols = [col for col in df_failed.columns if col.startswith('Original_')]
                
                # Reorder columns to group original data together
                ordered_cols = error_info_cols + original_data_cols
                df_failed = df_failed[ordered_cols]
                
                # Rename original data columns to remove 'Original_' prefix
                df_failed.columns = [col.replace('Original_', '') if col.startswith('Original_') else col for col in df_failed.columns]
                
                # Format the worksheet
                df_failed.to_excel(writer, sheet_name='Failed Records Detail', index=False)
                
                # Sheet 2: Error Summary with Categories
                error_categories = {}
                error_details = {}
                
                for item in failed_imports:
                    error_msg = item['Error Message']
                    # Extract main error category (text before ':' if exists)
                    error_category = error_msg.split(':')[0] if ':' in error_msg else error_msg
                    
                    # Update category counts
                    error_categories[error_category] = error_categories.get(error_category, 0) + 1
                    
                    # Store detailed information
                    if error_category not in error_details:
                        error_details[error_category] = {
                            'examples': [],
                            'affected_pos': set(),
                            'affected_products': set()
                        }
                    
                    detail = error_details[error_category]
                    if len(detail['examples']) < 3:  # Store up to 3 examples
                        detail['examples'].append(error_msg)
                    detail['affected_pos'].add(item['PO Number'])
                    detail['affected_products'].add(item['Product Code'])
                
                # Create summary DataFrame
                summary_data = []
                for category, count in error_categories.items():
                    detail = error_details[category]
                    summary_data.append({
                        'Error Category': category,
                        'Count': count,
                        'Affected POs': len(detail['affected_pos']),
                        'Affected Products': len(detail['affected_products']),
                        'Example Errors': '\n'.join(detail['examples'])
                    })
                
                error_summary = pd.DataFrame(summary_data)
                error_summary = error_summary.sort_values('Count', ascending=False)
                error_summary.to_excel(writer, sheet_name='Error Summary', index=False)
                
                # Sheet 3: Statistical Analysis
                stats_data = {
                    'Metric': [
                        'Total Failed Records',
                        'Total Unique POs Affected',
                        'Total Unique Products Affected',
                        'Most Common Error Category',
                        'Average Errors per PO',
                        'Timestamp Range'
                    ],
                    'Value': [
                        len(failed_imports),
                        len(set(item['PO Number'] for item in failed_imports)),
                        len(set(item['Product Code'] for item in failed_imports)),
                        error_summary.iloc[0]['Error Category'] if not error_summary.empty else 'N/A',
                        len(failed_imports) / len(set(item['PO Number'] for item in failed_imports)) if failed_imports else 0,
                        f"{min(item['Date Time'] for item in failed_imports)} to {max(item['Date Time'] for item in failed_imports)}"
                    ]
                }
                
                stats_df = pd.DataFrame(stats_data)
                stats_df.to_excel(writer, sheet_name='Statistics', index=False)
            
            # Log summary information
            logger.info(f"\nDetailed Error Log saved to: {log_file}")
            logger.info(f"Total failed records: {len(failed_imports)} | Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}")
            
            # Log error distribution by PO
            po_errors = {}
            for item in failed_imports:
                po_name = item['PO Number']
                po_errors[po_name] = po_errors.get(po_name, 0) + 1
            
            if po_errors:
                logger.info("\nError distribution by PO:")
                for po_name, count in sorted(po_errors.items(), key=lambda x: x[1], reverse=True)[:10]:  # Top 10 POs with most errors
                    logger.info(f"  - PO {po_name}: {count} errors")
            
            logger.info("\nTop 5 most common error categories:")
            for _, row in error_summary.head().iterrows():
                logger.info(f"  - {row['Error Category']}: {row['Count']} records ({row['Affected POs']} POs affected)")
                
        except Exception as e:
            logger.error(f"Error saving detailed error log: {str(e)}")
            logger.exception("Full error traceback:")
            
        finally:
            # Memory cleanup
            gc.collect()

# --- Import Management Classes ---
class TransactionManager:
    def __init__(self, odoo_connection):
        self.connection = odoo_connection
        self.transaction_data = []
        self.lock = threading.Lock()
        self.rollback_points = []

    def start_transaction(self):
        """Start a new transaction and create a rollback point"""
        with self.lock:
            self.rollback_points.append(len(self.transaction_data))

    def add_operation(self, operation_type, data, rollback_func):
        """Add an operation to the current transaction"""
        with self.lock:
            self.transaction_data.append({
                'type': operation_type,
                'data': data,
                'rollback_func': rollback_func,
                'timestamp': time.time()
            })

    def commit(self):
        """Commit the current transaction"""
        with self.lock:
            if self.rollback_points:
                self.rollback_points.pop()

    def rollback(self):
        """Rollback to the last rollback point"""
        with self.lock:
            if self.rollback_points:
                rollback_point = self.rollback_points.pop()
                operations_to_rollback = self.transaction_data[rollback_point:]
                
                # Rollback operations in reverse order
                for operation in reversed(operations_to_rollback):
                    try:
                        operation['rollback_func'](operation['data'])
                    except Exception as e:
                        logger.error(f"Error during rollback: {str(e)}")
                
                # Remove rolled back operations
                self.transaction_data = self.transaction_data[:rollback_point]

    @contextmanager
    def transaction(self):
        """Context manager for transaction handling"""
        self.start_transaction()
        try:
            yield self
            self.commit()
        except Exception as e:
            logger.error(f"Transaction failed: {str(e)}")
            self.rollback()
            raise

class ImportManager:
    def __init__(self, file_path, batch_size=BATCH_SIZE):
        self.file_path = file_path
        self.batch_size = batch_size
        self.odoo_connection = OdooConnection.get_instance()
        self.transaction_manager = TransactionManager(self.odoo_connection)
        self.memory_monitor = MemoryMonitor()
        self.progress_tracker = None
        
    def _get_total_records(self):
        """Get total number of records in Excel file"""
        try:
            df = pd.read_excel(self.file_path, nrows=0)
            return len(df)
        except Exception as e:
            logger.error(f"Error counting records: {str(e)}")
            return 0

    def process_file(self):
        """Process the import file with transaction management and monitoring"""
        total_records = self._get_total_records()
        self.progress_tracker = ProgressTracker(total_records)
        self.progress_tracker.start()

        try:
            for chunk in pd.read_excel(self.file_path, chunksize=self.batch_size):
                with self.transaction_manager.transaction():
                    self._process_chunk(chunk)
                    
                # Monitor memory usage
                self.memory_monitor.sample_memory()
                memory_stats = self.memory_monitor.get_memory_stats()
                
                # Log memory usage if it exceeds threshold
                if isinstance(memory_stats, dict) and memory_stats['current_mb'] > 1000:  # 1GB threshold
                    logger.warning(
                        f"High memory usage during file processing: {memory_stats['current_mb']}MB | "
                        f"Delta: {memory_stats['delta_mb']}MB | "
                        f"Growth Rate: {memory_stats.get('growth_rate_mb_per_min', 'N/A')}MB/min | "
                        f"Thread: {threading.get_ident()} | "
                        f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}"
                    )
                    
        except Exception as e:
            logger.error(f"Error processing file: {str(e)}")
            raise
        finally:
            self._cleanup()

    def _process_chunk(self, chunk):
        """Process a chunk of records with error handling"""
        self.progress_tracker.update(stage="Processing Chunk")
        
        for index, row in chunk.iterrows():
            try:
                with self.transaction_manager.transaction():
                    # Your existing import logic here
                    self._import_record(row)
                    self.progress_tracker.update(1)
            except Exception as e:
                error_type = type(e).__name__
                self.progress_tracker.add_error(error_type)
                log_error(
                    po_name=row.get('PO Number', 'Unknown'),
                    line_number=row.get('Line Number', 'Unknown'),
                    product_code=row.get('Product Code', 'Unknown'),
                    error_message=str(e),
                    row_index=index,
                    row_data=row
                )

    def _import_record(self, row):
        """Import a single record with rollback support"""
        # Implementation will be added based on your specific import logic
        pass

    def _cleanup(self):
        """Cleanup resources after import"""
        gc.collect()
        memory_stats = self.memory_monitor.get_memory_stats()
        logger.info(f"Final memory usage: {memory_stats}")
        if self.progress_tracker:
            self.progress_tracker._report_progress()  # Final progress report

# --- Connection Settings ---
url = 'http://mogth.work:8069/'
db = 'MOG_LIVE'
username = 'parinya@mogen.co.th'
password = 'mogen'

# --- Data File Settings ---
<<<<<<< HEAD
excel_file = 'Data_file/import_OB7.xlsx'
=======
excel_file = 'Data_file/import_OBสินค้าชำรุด.xlsx'
>>>>>>> 762faafb5c2486461d27e239ac0d0579e45a44c8

class OdooConnection:
    _instances = {}  # Connection pool
    _pool_lock = threading.Lock()
    _last_keepalive = {}  # Track last keepalive for each connection
    _request_counts = {}  # Track request counts per connection
    _connection_semaphore = threading.Semaphore(CONNECTION_POOL_SIZE)
    _connection_stats = {}  # Track connection performance
    _memory_monitor = MemoryMonitor()  # Add memory monitoring

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

    def execute_with_retry(self, model, method, *args, **kwargs):
        """Execute Odoo method with retry and transaction support"""
        for attempt in range(MAX_RETRIES):
            try:
                start_time = time.time()
                
                # Monitor memory before execution
                self._memory_monitor.sample_memory()
                
                result = self.models.execute_kw(
                    self.db, self.uid, self.password,
                    model, method, args, kwargs
                )
                
                # Track execution time
                execution_time = time.time() - start_time
                self._track_request_performance(execution_time)
                
                return result
                
            except Exception as e:
                if attempt == MAX_RETRIES - 1:
                    raise
                    
                wait_time = RETRY_DELAY * (2 ** attempt)  # Exponential backoff
                logger.warning(f"Retry attempt {attempt + 1} after {wait_time}s: {str(e)}")
                time.sleep(wait_time)
                
                # Check memory usage after error
                memory_stats = self._memory_monitor.get_memory_stats()
                if isinstance(memory_stats, dict) and memory_stats['current_mb'] > 1000:
                    logger.warning(
                        f"High memory usage after error: {memory_stats['current_mb']}MB | "
                        f"Delta: {memory_stats['delta_mb']}MB | "
                        f"Thread: {threading.get_ident()} | "
                        f"Error Type: {type(e).__name__} | "
                        f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}"
                    )
                    gc.collect()

    def _track_request_performance(self, execution_time):
        """Track request performance metrics"""
        thread_id = threading.get_ident()
        with self._pool_lock:
            stats = self._connection_stats.setdefault(thread_id, {
                'response_times': [],
                'error_count': 0,
                'last_error_time': None
            })
            
            stats['response_times'].append(execution_time)
            if len(stats['response_times']) > 100:  # Keep last 100 responses
                stats['response_times'] = stats['response_times'][-100:]

    def get_performance_stats(self):
        """Get connection performance statistics"""
        thread_id = threading.get_ident()
        stats = self._connection_stats.get(thread_id, {})
        
        if not stats or not stats.get('response_times'):
            return "No performance data available"
            
        response_times = stats['response_times']
        return {
            'avg_response_time': sum(response_times) / len(response_times),
            'max_response_time': max(response_times),
            'min_response_time': min(response_times),
            'total_requests': len(response_times),
            'error_count': stats.get('error_count', 0),
            'memory_usage': self._memory_monitor.get_memory_stats()
        }

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
        log_error('', '', '', f"Vendor Search Error: {error_msg}")
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
        log_error('', '', product_value, f"Product not found in system: {product_value}")
        return []
        
    except Exception as e:
        error_msg = str(e)
        print(f"Error searching product: {error_msg}")
        log_error('', '', product_value, f"Product Search Error: {error_msg}")
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
        log_error(po_data.get('name', ''), '', '', f"PO Creation/Update Error: {error_msg}")
        return False

def safe_float_conversion(value, row_index=None, row_data=None):
    """Safely convert various input formats to float"""
    try:
        if pd.isna(value):
            logger.debug(f"Converting NaN value to 0.0 | Row: {row_index}")
            return 0.0
            
        if isinstance(value, (int, float)):
            return float(value)
            
        if isinstance(value, str):
            # Handle special cases
            value = value.strip()
            if value in ['-', '', 'N/A', 'NA', 'None', 'null']:
                logger.debug(f"Converting special value '{value}' to 0.0 | Row: {row_index}")
                return 0.0
                
            # Remove any currency symbols, thousand separators and spaces
            original_value = value
            value = value.replace('฿', '').replace(',', '').replace(' ', '')
            
            if value != original_value:
                logger.debug(f"Cleaned value from '{original_value}' to '{value}' | Row: {row_index}")
            
            if not value:  # If empty after cleaning
                return 0.0
                
            try:
                return float(value)
            except ValueError as e:
                error_msg = f"Could not convert '{value}' to float: {str(e)}"
                logger.warning(f"{error_msg} | Row: {row_index} | Thread: {threading.get_ident()} | Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}")
                if row_index is not None and row_data is not None and isinstance(row_data, pd.Series):
                    product_code = row_data.get('default_code', row_data.get('old_product_code', ''))
                    po_name = row_data.get('name', '')
                    log_error(po_name, row_index, product_code, f"Value conversion error: {error_msg}", row_index, row_data)
                else:
                    log_error('', '', '', f"Value conversion error: {error_msg}", row_index)
                return 0.0
                
        return 0.0
        
    except (ValueError, TypeError) as e:
        error_msg = f"Error converting value '{value}' to float: {str(e)}"
        logger.warning(f"{error_msg} | Row: {row_index} | Thread: {threading.get_ident()} | Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}")
        if row_index is not None and row_data is not None and isinstance(row_data, pd.Series):
            product_code = row_data.get('default_code', row_data.get('old_product_code', ''))
            po_name = row_data.get('name', '')
            log_error(po_name, row_index, product_code, f"Value conversion error: {error_msg}", row_index, row_data)
        else:
            log_error('', '', '', f"Value conversion error: {error_msg}", row_index)
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
                log_error(po_name, '', '', error_msg, first_row.name, first_row)
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
                quantity = safe_float_conversion(line['product_qty'], line.name, line)
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
                    log_error(current_po_name, '', '', f"Failed to add order lines to PO")
                    print(f"Failed to create/update PO: {current_po_name}")
                
        except Exception as e:
            error_count += 1
            print(f"Error processing PO {po_name}: {str(e)}")
            log_error(po_name, '', '', f"Processing Error: {str(e)}")
    
    return success_count, error_count

def main():
    total_success = 0
    total_errors = 0
    start_time = time.time()
    
    try:
        # Log start of process with detailed information
        logger.info(f"\n{'='*50}")
        logger.info(f"IMPORT PROCESS STARTED AT {datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}")
        logger.info(f"File: {excel_file} | Thread: {threading.get_ident()} | Process ID: {os.getpid()}")
        logger.info(f"{'='*50}\n")
        
        # Read Excel file
        df = pd.read_excel(excel_file)
        logger.info(f"Excel columns: {df.columns.tolist()}")
        logger.info(f"Excel file '{excel_file}' read successfully. Number of rows = {len(df)}")
        
        # Process in batches
        total_rows = len(df)
        total_batches = (total_rows + BATCH_SIZE - 1) // BATCH_SIZE
        
        logger.info(f"Processing {total_rows} rows in {total_batches} batches (batch size: {BATCH_SIZE})")
        
        # Process each batch
        for batch_num in range(total_batches):
            batch_start_time = time.time()
            start_idx = batch_num * BATCH_SIZE
            end_idx = min(start_idx + BATCH_SIZE, total_rows)
            batch_df = df.iloc[start_idx:end_idx]
            
            logger.info(f"\n{'*'*30}")
            logger.info(f"Starting Batch {batch_num + 1}/{total_batches} at {datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}")
            logger.info(f"Rows {start_idx} to {end_idx-1} | Total rows in batch: {len(batch_df)}")
            
            success_count, error_count = process_po_batch(batch_df, batch_num + 1, total_batches)
            total_success += success_count
            total_errors += error_count
            
            batch_time = time.time() - batch_start_time
            
            # Print batch summary
            logger.info(f"\nBatch {batch_num + 1} Summary:")
            logger.info(f"Successful POs: {success_count} | Failed POs: {error_count} | Time: {batch_time:.2f}s")
            logger.info(f"Average time per record: {batch_time/len(batch_df):.4f}s")
            logger.info(f"{'*'*30}\n")
            
            # Optional: Add a small delay between batches to prevent overloading
            if batch_num < total_batches - 1:
                time.sleep(1)  # 1 second delay between batches
        
        # Calculate total processing time
        total_time = time.time() - start_time
        records_per_second = total_rows / total_time if total_time > 0 else 0
        
        # Print final summary
        logger.info(f"\n{'='*50}")
        logger.info(f"IMPORT PROCESS COMPLETED AT {datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}")
        logger.info(f"Total Successful POs: {total_success}")
        logger.info(f"Total Failed POs: {total_errors}")
        logger.info(f"Total Processed: {total_success + total_errors}")
        logger.info(f"Total Processing Time: {time.strftime('%H:%M:%S', time.gmtime(total_time))}")
        logger.info(f"Overall Speed: {records_per_second:.2f} records/sec")
        logger.info(f"{'='*50}\n")
        
    except Exception as e:
        logger.error(f"Error in main function: {str(e)} | Thread: {threading.get_ident()} | Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}")
        logger.exception("Full error traceback:")
        log_error('', '', '', f"Main Function Error: {str(e)}")
    
    finally:
        # Save error log if there were any errors
        save_error_log()
        logger.info(f"Import process completed at {datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}.")

if __name__ == "__main__":
    main()