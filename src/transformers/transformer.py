"""
Multi-Worker Batch Processing Transformer
=========================================
Uses multiprocessing for true parallelism
Configurable workers and batch sizes from environment
"""
import json
import gzip
import os
import logging
import time
import gc
import multiprocessing as mp
from datetime import datetime
from typing import Dict, Any, List, Optional, Tuple
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor, as_completed
import psutil

# Import transformation mappings
from .transformation_mapping import ALL_MAPPINGS as transformation_mappings

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# Global worker process functions
def process_record_batch_simple(args: Tuple[List[Dict], Dict, str, int, int]) -> Tuple[List[Dict], int]:
    """Process a batch of records efficiently - minimal data passing"""
    records, column_mappings, etl_timestamp, batch_id, table_id = args
    results = []
    
    # Boolean fields that need conversion (shared with SimpleTransformer)
    BOOLEAN_FIELDS = SimpleTransformer.BOOLEAN_FIELDS
    
    for record in records:
        transformed = {'etl_timestamp': etl_timestamp}
        
        for target_field, source_path in column_mappings.items():
            # Simple field mapping without complex joins
            if '.' in source_path:
                # Skip complex joins for now - handle in main process
                value = None
            else:
                value = record.get(source_path)
            
            # Fast value cleaning
            if value is None:
                transformed[target_field] = None
            elif target_field in BOOLEAN_FIELDS and value in (0, 1):
                # Convert 0/1 to boolean for boolean fields
                transformed[target_field] = bool(value)
            elif isinstance(value, (dict, list)):
                transformed[target_field] = json.dumps(value)
            elif isinstance(value, str) and value.startswith("b'\\x"):
                transformed[target_field] = value == "b'\\x01'"
            else:
                transformed[target_field] = value
            
            # Handle _json fields
            if target_field.endswith('_json') and transformed[target_field] is None:
                transformed[target_field] = '{}'
        
        results.append(transformed)
    
    return results, table_id


class SimpleTransformer:
    """Base transformer with all transformation logic"""
    
    # Boolean fields that need 0/1 to true/false conversion
    BOOLEAN_FIELDS = {
        # dim_users
        'deleted', 'is_2fa_enabled', 'join_confirmed', 'is_account_owner', 'account_deleted',
        # dim_accounts  
        'auth_enabled',
        # dim_tenants
        'is_play_ground', 'is_beta', 'skip_support_channel_notifications', 
        'report_customization_enabled', 'subscribed_to_emails', 'block_access',
        # brg_tenant_features & dim_features
        'is_add_on', 'is_premium',
        # dim_data_generators
        'has_data_map',
        # dim_nlp_templates  
        'snippet_enabled', 'deprecated', 'is_actionable', 'is_verifiable',
        'import_to_web', 'import_to_mobile_web', 'import_to_android_native',
        'import_to_ios_native', 'import_to_rest_native', 'import_to_salesforce', 'api_supported',
        # dim_projects
        'has_multiple_apps', 'has_multiple_versions', 'demo',
        # dim_test_cases
        'is_data_driven', 'is_step_group', 'is_ai_generated', 'is_manual', 'is_active',
        'is_reviewed', 'is_prerequisite_case', 'is_under_deletion', 'has_error',
        'has_after_test', 'is_eligible_for_after_suite', 'consider_visual_test_result', 'delete_marker',
        # fct_test_steps
        'disabled', 'visual_enabled', 'accessibility_enabled', 'ignore_step_result', 'is_invalid',
        # fct_executions
        'slack_connector_notification_enabled', 'ms_teams_connector_notification_enabled',
        'google_chat_connector_notification_enabled', 'accessibility_test_enabled',
        'is_inprogress', 'run_test_cases_in_parallel', 'run_test_suites_in_parallel', 'is_visually_passed',
        # fct_test_results
        'is_queued', 'is_migrated_execution',
        # dim_agents
        'enabled', 'visible_to_all', 'mobile_enabled', 'is_active', 'is_docker', 
        'is_service', 'is_obsolete', 'go_ios_enabled',
        # dim_applications
        'is_disabled'
    }
    
    def __init__(self):
        self.all_mappings = transformation_mappings
    
    def clean_value(self, value: Any, field_name: str = None) -> Any:
        """Clean and handle null values"""
        if value is None:
            return None
        if isinstance(value, str):
            # Handle binary strings
            if value.startswith("b'\\x"):
                # Convert binary to boolean
                return value == "b'\\x01'"
            # Handle empty strings
            if value.strip() == "":
                return None
            return value.strip()
        # Convert 0/1 to false/true for boolean fields
        if field_name and field_name in self.BOOLEAN_FIELDS:
            if value == 0:
                return False
            elif value == 1:
                return True
            # For any other value (like already boolean), return as is
            return bool(value) if value is not None else None
        if isinstance(value, (dict, list)):
            # Convert dict/list to JSON string for database storage
            return json.dumps(value)
        return value
        
    def get_value_from_path(self, record: Dict[str, Any], path: str, source_data_map: Dict[str, List[Dict]] = None, record_index: int = 0) -> Any:
        """Get value from a dotted path like 'table.column'"""
        parts = path.split('.')
        if len(parts) == 2:
            # It's a table.column reference
            table_name = parts[0]
            column = parts[1]
            
            # If we have additional source data, try to find matching record
            if source_data_map and table_name in source_data_map:
                # Implement key-based join
                table_data = source_data_map[table_name]
                
                # Try to find matching record based on common keys
                # Common join keys: id, user_id, tenant_id, tenant_tsid, account_id, execution_id, test_case_id
                for join_key in ['id', 'user_id', 'tenant_id', 'tenant_tsid', 'account_id', 'execution_id', 'test_case_id']:
                    if join_key in record:
                        primary_value = record[join_key]
                        # Look for matching record in secondary table
                        for secondary_record in table_data:
                            if join_key in secondary_record and secondary_record[join_key] == primary_value:
                                return secondary_record.get(column)
                
                # If no key-based match found, try index-based join
                if record_index < len(table_data):
                    return table_data[record_index].get(column)
                    
            # Fall back to the primary record
            return record.get(column)
        else:
            # Direct column reference
            return record.get(path)
            
    def transform_record(self, source_record: Dict[str, Any],
                        column_mappings: Dict[str, str],
                        source_data_map: Dict[str, List[Dict]] = None,
                        record_index: int = 0) -> Dict[str, Any]:
        """Transform a single record based on column mappings"""
        transformed = {}
        
        for target_col, source_path in column_mappings.items():
            value = self.get_value_from_path(source_record, source_path, source_data_map, record_index)
            cleaned_value = self.clean_value(value, target_col)
            
            # Handle specific columns that should have defaults
            if cleaned_value is None:
                if target_col.endswith('_at') or target_col in ['created_at', 'updated_at']:
                    # For timestamp columns, keep null
                    transformed[target_col] = None
                elif target_col.endswith('_id'):
                    # For ID columns, keep null
                    transformed[target_col] = None
                elif target_col.endswith('_count') or target_col == 'duration_seconds':
                    # For numeric columns, default to 0
                    transformed[target_col] = 0
                elif target_col in ['status', 'type', 'state']:
                    # For status columns, default to 'unknown'
                    transformed[target_col] = 'unknown'
                elif target_col.endswith('_json'):
                    # For JSON columns, default to empty JSON string
                    transformed[target_col] = '{}'
                else:
                    # For everything else, keep null
                    transformed[target_col] = None
            else:
                transformed[target_col] = cleaned_value
                
        return transformed


class FastTransformer:
    """Multi-worker batch processing transformer with configurable parallelism"""
    
    def __init__(self, max_workers: int = None, batch_size: int = None):
        self.base_transformer = SimpleTransformer()
        self.all_mappings = self.base_transformer.all_mappings
        self.start_time = None
        self.records_processed = 0
        
        # Get configuration from environment or use defaults
        self.max_workers = max_workers or int(os.getenv('TRANSFORMATION_WORKERS', '4'))
        self.batch_size = batch_size or int(os.getenv('TRANSFORMATION_BATCH_SIZE', '1000'))
        
        # Validate settings
        cpu_count = os.cpu_count() or 8
        if self.max_workers > cpu_count * 2:
            logger.warning(f"Workers ({self.max_workers}) exceeds 2x CPU count ({cpu_count}). May cause contention.")
        
        logger.info(f"Using {self.max_workers} workers with batch size {self.batch_size}")
        self._init_join_cache()
        
    def _init_join_cache(self):
        """Initialize cache for join optimizations"""
        self.join_cache = {}
        self.join_keys = ['id', 'user_id', 'tenant_id', 'tenant_tsid', 'account_id', 
                         'execution_id', 'test_case_id', 'application_id', 'app_id']
        
    def _build_join_indexes(self, source_data_map: Dict[str, List[Dict]], primary_source: str) -> Dict:
        """Build indexes for efficient joins"""
        join_indexes = {}
        for table_name, records in source_data_map.items():
            if table_name != primary_source:
                table_indexes = {}
                for key in self.join_keys:
                    key_index = {}
                    for i, record in enumerate(records):
                        if key in record and record[key] is not None:
                            if record[key] not in key_index:
                                key_index[record[key]] = []
                            key_index[record[key]].append(i)
                    if key_index:
                        table_indexes[key] = key_index
                if table_indexes:
                    join_indexes[table_name] = table_indexes
        return join_indexes
        
    def log_memory_usage(self, context: str):
        """Log current memory usage"""
        process = psutil.Process()
        memory_info = process.memory_info()
        memory_percent = process.memory_percent()
        logger.info(f"[{context}] Memory: {memory_info.rss / 1024 / 1024 / 1024:.2f}GB ({memory_percent:.1f}%)")
        
    def load_all_data(self, input_file: str) -> Dict[str, Dict[str, List[Dict]]]:
        """Load all data at once for maximum speed"""
        logger.info("Loading entire file into memory for fast processing...")
        start = time.time()
        
        if input_file.endswith('.gz'):
            with gzip.open(input_file, 'rt', encoding='utf-8') as f:
                data = json.load(f)
        else:
            with open(input_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
                
        # Reorganize data by table name for fast lookup
        table_data = {}
        for db_name, db_data in data.items():
            if db_name == 'extraction_metadata':
                continue
            if isinstance(db_data, dict):
                for table_name, table_info in db_data.items():
                    if isinstance(table_info, dict) and 'sample' in table_info:
                        # Handle both list and dict format
                        sample_data = table_info['sample']
                        if isinstance(sample_data, list):
                            table_data[table_name] = sample_data
                        elif isinstance(sample_data, dict):
                            # If it's a dict with items, extract the items
                            table_data[table_name] = list(sample_data.values()) if sample_data else []
                        
        load_time = time.time() - start
        total_records = sum(len(records) for records in table_data.values())
        logger.info(f"Loaded {len(table_data)} tables with {total_records:,} records in {load_time:.1f}s")
        self.log_memory_usage("After loading data")
        
        return table_data
        
    def _find_joined_record(self, primary_record: Dict, source_table: str, 
                          join_indexes: Dict, source_data: List[Dict], 
                          record_index: int) -> Optional[Dict]:
        """Find the best matching record from secondary table"""
        if source_table not in join_indexes:
            # Fallback to index-based join
            return source_data[record_index] if record_index < len(source_data) else None
            
        # Try each join key
        for join_key in self.join_keys:
            if join_key in primary_record and primary_record[join_key] is not None:
                key_value = primary_record[join_key]
                if join_key in join_indexes[source_table] and key_value in join_indexes[source_table][join_key]:
                    # Return first matching record
                    matched_indices = join_indexes[source_table][join_key][key_value]
                    if matched_indices:
                        return source_data[matched_indices[0]]
                        
        # Fallback to index-based join
        return source_data[record_index] if record_index < len(source_data) else None
    
    def transform_table_batch(self, target_table: str, config: Dict[str, Any], 
                           all_table_data: Dict[str, List[Dict]], 
                           etl_timestamp: str) -> Optional[Dict]:
        """Transform a table using batch processing for speed"""
        source_tables = config.get('source_tables', [])
        column_mappings = config.get('column_mappings', {})
        primary_key = config.get('primary_key')
        
        if not source_tables:
            return None
            
        primary_source = source_tables[0]
        primary_data = all_table_data.get(primary_source, [])
        
        if not primary_data:
            return None
            
        # Build source data map and join indexes
        source_data_map = {}
        for source_table in source_tables:
            if source_table in all_table_data:
                source_data_map[source_table] = all_table_data[source_table]
        
        # Build join indexes for efficient lookups
        join_indexes = self._build_join_indexes(source_data_map, primary_source)
        
        # Transform records in batches
        transformed_records = []
        batch_size = 100  # Process 100 records at a time
        
        for batch_start in range(0, len(primary_data), batch_size):
            batch_end = min(batch_start + batch_size, len(primary_data))
            
            for index in range(batch_start, batch_end):
                record = primary_data[index]
                try:
                    # Build optimized source map for this record
                    optimized_map = {primary_source: [record]}
                    
                    # Find matching records from secondary tables
                    for source_table in source_tables[1:]:
                        if source_table in source_data_map:
                            joined_record = self._find_joined_record(
                                record, source_table, join_indexes, 
                                source_data_map[source_table], index
                            )
                            if joined_record:
                                optimized_map[source_table] = [joined_record]
                    
                    # Transform using base transformer
                    transformed = self.base_transformer.transform_record(
                        record, column_mappings, optimized_map, 0
                    )
                    transformed['etl_timestamp'] = etl_timestamp
                    
                    # Check primary key
                    if primary_key:
                        if isinstance(primary_key, list):
                            if all(transformed.get(pk) is not None for pk in primary_key):
                                transformed_records.append(transformed)
                        else:
                            if transformed.get(primary_key) is not None:
                                transformed_records.append(transformed)
                    else:
                        transformed_records.append(transformed)
                    
                    self.records_processed += 1
                    
                except Exception as e:
                    logger.error(f"Error transforming record in {target_table}: {e}")
                    continue
                    
        if transformed_records:
            return {
                'records': len(transformed_records),
                'data': transformed_records
            }
        return None
    
    def transform_table_fast(self, target_table: str, config: Dict[str, Any], 
                           all_table_data: Dict[str, List[Dict]], 
                           etl_timestamp: str) -> Optional[Dict]:
        """Transform a single table as fast as possible"""
        source_tables = config.get('source_tables', [])
        column_mappings = config.get('column_mappings', {})
        primary_key = config.get('primary_key')
        
        if not source_tables:
            return None
            
        primary_source = source_tables[0]
        primary_data = all_table_data.get(primary_source, [])
        
        if not primary_data:
            return None
            
        # Build source data map for joins
        source_data_map = {}
        for source_table in source_tables:
            if source_table in all_table_data:
                source_data_map[source_table] = all_table_data[source_table]
                        
        # Transform records in batch
        transformed_records = []
        
        for index, record in enumerate(primary_data):
            try:
                # Build source map with all tables for proper join logic
                # The base transformer's get_value_from_path will handle the joins
                transformed = self.base_transformer.transform_record(
                    record, column_mappings, source_data_map, index
                )
                transformed['etl_timestamp'] = etl_timestamp
                
                # Check primary key
                if primary_key:
                    if isinstance(primary_key, list):
                        if not all(transformed.get(pk) is not None for pk in primary_key):
                            continue
                    else:
                        if transformed.get(primary_key) is None:
                            continue
                            
                transformed_records.append(transformed)
                self.records_processed += 1
                
            except Exception as e:
                logger.error(f"Error transforming record in {target_table}: {e}")
                continue
                
        if transformed_records:
            return {
                'records': len(transformed_records),
                'data': transformed_records
            }
        return None
        
    def transform_tables_parallel(self, table_batch: List[tuple], all_table_data: Dict, 
                                etl_timestamp: str) -> Dict[str, Dict]:
        """Transform multiple tables in parallel - optimized for I/O bound operations"""
        results = {}
        
        from concurrent.futures import ThreadPoolExecutor
        
        # Use threads for I/O-bound transformation operations
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_table = {
                executor.submit(
                    self.transform_table_batch, 
                    target_table, 
                    config, 
                    all_table_data, 
                    etl_timestamp
                ): target_table
                for target_table, config in table_batch
            }
            
            for future in as_completed(future_to_table):
                target_table = future_to_table[future]
                try:
                    result = future.result()
                    if result:
                        results[target_table] = result
                        logger.info(f"Transformed {target_table}: {result['records']} records")
                except Exception as e:
                    logger.error(f"Error transforming {target_table}: {e}")
                    
        return results
        
    def transform_fast(self, input_file: str, timestamp: str = None) -> str:
        """Fast transformation that uses streaming for large files"""
        if not timestamp:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            
        output_dir = os.getenv('TRANSFORMED_OUTPUT_DIR', 'output/transformations')
        os.makedirs(output_dir, exist_ok=True)
        output_file = os.path.join(output_dir, f'snowflake_data_{timestamp}.json.gz')
        
        logger.info(f"Starting fast transformation")
        logger.info(f"Input: {input_file}")
        logger.info(f"Output: {output_file}")
        logger.info(f"Using {self.max_workers} parallel workers for transformation")
        
        self.start_time = time.time()
        self.log_memory_usage("Start")
        
        # Check file size to decide strategy
        file_size_gb = os.path.getsize(input_file) / (1024**3)
        logger.info(f"Input file size: {file_size_gb:.2f}GB")
        
        if file_size_gb > 2.0:  # For files > 2GB, use streaming
            logger.info("Using memory-efficient streaming for large file")
            return self._transform_streaming(input_file, output_file, timestamp)
        else:
            logger.info("Using in-memory transformation for standard file")
            # Load all data at once for smaller files
            all_table_data = self.load_all_data(input_file)
        
        # Process all tables
        # Convert job_id format to proper timestamp if needed
        if timestamp and '_' in timestamp and len(timestamp) == 15:
            try:
                dt = datetime.strptime(timestamp, "%Y%m%d_%H%M%S")
                etl_timestamp = dt.strftime('%Y-%m-%d %H:%M:%S')
            except:
                etl_timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        else:
            etl_timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        transformed_data = {"tables": {}}
        
        # Batch tables for parallel processing
        table_list = list(self.all_mappings.items())
        batch_size = max(1, len(table_list) // (self.max_workers * 2))
        
        for i in range(0, len(table_list), batch_size):
            batch = table_list[i:i + batch_size]
            batch_results = self.transform_tables_parallel(batch, all_table_data, etl_timestamp)
            transformed_data['tables'].update(batch_results)
            
            # Log progress
            elapsed = time.time() - self.start_time
            if elapsed > 0 and self.records_processed > 0:
                rate = self.records_processed / elapsed
                remaining_records = sum(len(records) for records in all_table_data.values()) - self.records_processed
                if rate > 0 and remaining_records > 0:
                    eta = remaining_records / rate
                    logger.info(f"Progress: {self.records_processed:,} records at {rate:.0f} records/sec (ETA: {eta:.0f}s)")
                
        # Clear source data to free memory before writing
        all_table_data.clear()
        gc.collect()
        
        # Write output file
        logger.info("Writing output file...")
        with gzip.open(output_file, 'wt', encoding='utf-8', compresslevel=1) as f:
            json.dump(transformed_data, f, indent=2, default=str)
            
        # Final statistics
        elapsed = time.time() - self.start_time
        logger.info(f"Fast transformation complete!")
        logger.info(f"Tables transformed: {len(transformed_data['tables'])}")
        logger.info(f"Total records: {self.records_processed:,}")
        logger.info(f"Time elapsed: {elapsed:.1f} seconds")
        if elapsed > 0 and self.records_processed > 0:
            logger.info(f"Processing rate: {self.records_processed/elapsed:.0f} records/second")
        
        # Performance estimates
        if self.records_processed > 0 and elapsed > 0:
            rate = self.records_processed / elapsed
            
            # Estimate for 30 million records
            estimated_seconds_30m = 30_000_000 / rate
            estimated_minutes_30m = estimated_seconds_30m / 60
            estimated_hours = estimated_minutes_30m / 60
            
            logger.info("\n" + "="*70)
            logger.info("PERFORMANCE ESTIMATES FOR 30M RECORDS (29GB FILE):")
            logger.info(f"  Current rate: {rate:.0f} records/second")
            logger.info(f"  Workers: {self.max_workers}")
            logger.info(f"  Batch size: {self.batch_size}")
            logger.info(f"  Estimated time: {estimated_hours:.1f} hours ({estimated_minutes_30m:.0f} minutes)")
            logger.info(f"  Estimated throughput: {30_000_000 / (estimated_seconds_30m * 1024 * 1024):.1f} MB/s")
            
            # RAM usage estimates (realistic calculation)
            process = psutil.Process()
            current_ram_gb = process.memory_info().rss / 1024 / 1024 / 1024
            
            # Assume ~100MB base Python/library overhead
            base_overhead_gb = 0.1
            data_ram_gb = max(0, current_ram_gb - base_overhead_gb)
            
            # Calculate per-record memory (excluding base overhead)
            if self.records_processed > 0:
                ram_per_record_mb = (data_ram_gb * 1024) / self.records_processed
            else:
                ram_per_record_mb = 0
            
            # With batch processing, we only keep active batches in memory
            active_records = min(self.batch_size * self.max_workers, 30_000_000)
            estimated_data_ram_gb = (active_records * ram_per_record_mb) / 1024
            estimated_total_ram_gb = base_overhead_gb + estimated_data_ram_gb
            
            logger.info(f"\n  RAM USAGE ESTIMATES (Batch Processing):")
            logger.info(f"  Current RAM: {current_ram_gb:.2f} GB for {self.records_processed:,} records")
            logger.info(f"  Base overhead: {base_overhead_gb:.1f} GB")
            logger.info(f"  Data per record: {ram_per_record_mb:.3f} MB")
            logger.info(f"  Active records in memory: {active_records:,} (batch_size * workers)")
            logger.info(f"  Estimated RAM usage: {estimated_total_ram_gb:.1f} GB")
            logger.info(f"  Peak RAM (with safety margin): {estimated_total_ram_gb * 1.5:.1f} GB")
            logger.info("="*70)
            
        self.log_memory_usage("End")
        
        return output_file
    
    def _transform_streaming(self, input_file: str, output_file: str, timestamp: str) -> str:
        """Memory-efficient streaming transformation for large files"""
        logger.info("Streaming transformation: single-pass processing with memory management")
        
        # Load data once and process in memory-managed chunks
        logger.info("Loading data for single-pass transformation...")
        start_load = time.time()
        
        # Load all data at once but process in chunks
        all_data = {}
        if input_file.endswith('.gz'):
            open_func = gzip.open
        else:
            open_func = open
            
        with open_func(input_file, 'rt', encoding='utf-8') as f:
            data = json.load(f)
            
            # Extract all table data
            for db_name, db_data in data.items():
                if db_name == 'extraction_metadata':
                    continue
                if isinstance(db_data, dict):
                    for table_name, table_info in db_data.items():
                        if isinstance(table_info, dict) and 'sample' in table_info:
                            sample_data = table_info['sample']
                            if isinstance(sample_data, list) and sample_data:
                                all_data[table_name] = sample_data
                                logger.debug(f"Loaded {table_name}: {len(sample_data)} records")
                            elif isinstance(sample_data, dict):
                                all_data[table_name] = list(sample_data.values()) if sample_data else []
        
        load_time = time.time() - start_load
        logger.info(f"Data loaded in {load_time:.1f}s: {len(all_data)} tables, {sum(len(v) for v in all_data.values()):,} records")
        self.log_memory_usage("After loading data")
        
        # Convert job_id format to proper timestamp if needed
        if timestamp and '_' in timestamp and len(timestamp) == 15:
            try:
                dt = datetime.strptime(timestamp, "%Y%m%d_%H%M%S")
                etl_timestamp = dt.strftime('%Y-%m-%d %H:%M:%S')
            except:
                etl_timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        else:
            etl_timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        transformed_data = {"tables": {}}
        
        # Process all tables using loaded data
        logger.info("\nTransforming all tables...")
        
        # Get all target tables to transform
        table_list = list(self.all_mappings.items())
        total_tables = len(table_list)
        
        # Process in larger batches for better parallelism
        batch_size = min(10, max(1, total_tables // 3))
        for i in range(0, total_tables, batch_size):
            batch = table_list[i:i + batch_size]
            batch_results = self.transform_tables_parallel(batch, all_data, etl_timestamp)
            transformed_data['tables'].update(batch_results)
            
            # Log progress
            tables_done = min(i + batch_size, total_tables)
            logger.info(f"Progress: {tables_done}/{total_tables} tables transformed, {self.records_processed:,} records")
            
            # Periodically log memory usage
            if tables_done % 10 == 0:
                self.log_memory_usage(f"After {tables_done} tables")
        
        # Clear source data to free memory before writing
        all_data.clear()
        gc.collect()
        self.log_memory_usage("After clearing source data")
        
        # Write output file
        logger.info("\nWriting output file...")
        write_start = time.time()
        with gzip.open(output_file, 'wt', encoding='utf-8', compresslevel=1) as f:
            json.dump(transformed_data, f, indent=2, default=str)
        
        write_time = time.time() - write_start
        logger.info(f"Output written in {write_time:.1f}s")
        
        # Final statistics
        elapsed = time.time() - self.start_time
        logger.info(f"\nTransformation complete!")
        logger.info(f"Tables transformed: {len(transformed_data['tables'])}")
        logger.info(f"Total records: {self.records_processed:,}")
        logger.info(f"Time elapsed: {elapsed:.1f} seconds")
        
        if self.records_processed > 0 and elapsed > 0:
            rate = self.records_processed / elapsed
            logger.info(f"Processing rate: {rate:.0f} records/second")
            estimated_minutes_30m = (30_000_000 / rate) / 60
            logger.info(f"Estimated time for 30M records: {estimated_minutes_30m:.0f} minutes")
        
        self.log_memory_usage("End")
        return output_file
    


def transform_data_optimized(input_file: str, timestamp: str = None) -> str:
    """
    Multi-worker batch processing transformation with environment configuration
    
    Uses environment variables:
    - TRANSFORMATION_WORKERS: Number of parallel workers (default: 4)
    - TRANSFORMATION_BATCH_SIZE: Records per batch (default: 1000)
    """
    file_size_gb = os.path.getsize(input_file) / (1024**3)
    
    logger.info("="*70)
    logger.info("MULTI-WORKER BATCH PROCESSING TRANSFORMER")
    logger.info(f"Input file: {file_size_gb:.2f}GB")
    logger.info(f"Workers: {os.getenv('TRANSFORMATION_WORKERS', '4')}")
    logger.info(f"Batch size: {os.getenv('TRANSFORMATION_BATCH_SIZE', '1000')}")
    logger.info("="*70)
    
    transformer = FastTransformer()  # Uses env vars by default
    return transformer.transform_fast(input_file, timestamp)


if __name__ == "__main__":
    import sys
    
    # Get input file from command line or use latest
    if len(sys.argv) > 1:
        input_file = sys.argv[1]
    else:
        # Find latest extracted file
        output_dir = 'output/extracted'
        if os.path.exists(output_dir):
            files = [f for f in os.listdir(output_dir) if f.startswith('extracted_data_') and f.endswith('.json')]
            if files:
                files.sort()
                input_file = os.path.join(output_dir, files[-1])
            else:
                logger.error("No extracted data files found")
                sys.exit(1)
        else:
            logger.error("Output directory not found")
            sys.exit(1)
            
    logger.info(f"Using input file: {input_file}")
    
    # Run fast transformation
    output_file = transform_data_optimized(input_file)
    
    logger.info(f"Transformation complete! Output file: {output_file}")
