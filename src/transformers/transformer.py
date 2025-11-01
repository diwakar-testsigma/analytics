"""
Fast Transformer for Large ETL Files
====================================
Optimized for speed - targets 40-50 minutes for 30M records
Uses more RAM for better performance
"""
import json
import gzip
import os
import logging
import time
import gc
from datetime import datetime
from typing import Dict, Any, List, Optional
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
import psutil

# Import transformation mappings
from .transformation_mapping import ALL_MAPPINGS as transformation_mappings

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class SimpleTransformer:
    """Base transformer with all transformation logic"""
    
    def __init__(self):
        self.all_mappings = transformation_mappings
        
    def clean_value(self, value: Any) -> Any:
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
            cleaned_value = self.clean_value(value)
            
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
    """Fast transformer optimized for speed"""
    
    def __init__(self, max_workers: int = 8):
        self.base_transformer = SimpleTransformer()
        self.all_mappings = self.base_transformer.all_mappings
        self.start_time = None
        self.records_processed = 0
        self.max_workers = max_workers
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
        """Transform multiple tables in parallel"""
        results = {}
        
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
        """Fast transformation that uses more RAM for speed"""
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
        
        # Load all data at once
        all_table_data = self.load_all_data(input_file)
        
        # Process all tables
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
        
        # Estimate for 30 million records
        if self.records_processed > 0 and elapsed > 0:
            rate = self.records_processed / elapsed
            estimated_seconds_30m = 30_000_000 / rate
            estimated_minutes_30m = estimated_seconds_30m / 60
            logger.info(f"Estimated time for 30M records: {estimated_minutes_30m:.0f} minutes")
            
        self.log_memory_usage("End")
        
        return output_file


def transform_data_optimized(input_file: str, timestamp: str = None) -> str:
    """Main entry point for optimized data transformation"""
    # Use more workers for larger files and available CPU cores
    file_size_gb = os.path.getsize(input_file) / (1024**3)
    cpu_count = os.cpu_count() or 4
    
    # Scale workers with file size and available CPUs
    max_workers = min(cpu_count, max(8, int(file_size_gb * 2)))
    logger.info(f"Using {max_workers} workers (File: {file_size_gb:.2f}GB, CPUs: {cpu_count})")
    
    transformer = FastTransformer(max_workers=max_workers)
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
