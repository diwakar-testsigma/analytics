"""
Streaming join preprocessor - processes large files with minimal memory usage
Following the same patterns as the transformer
"""

import json
import gzip
import os
import gc
import ijson
from datetime import datetime
from typing import Dict, Any, List, Optional, Tuple
import logging
from collections import defaultdict
from decimal import Decimal

logger = logging.getLogger(__name__)


def convert_decimals(obj):
    """Convert Decimal objects to strings for JSON serialization"""
    if isinstance(obj, Decimal):
        return str(obj)
    elif isinstance(obj, dict):
        return {k: convert_decimals(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [convert_decimals(item) for item in obj]
    return obj


class StreamingJoinPreprocessor:
    """Streaming preprocessor that handles joins without loading entire file"""
    
    def __init__(self, config: Optional[Dict] = None):
        """Initialize preprocessor"""
        self.logger = logging.getLogger(__name__)
        
        if config:
            self.output_dir = config.get('output_dir', 'output/preprocessed')
        else:
            from ..config import settings
            # Create preprocessed directory similar to transformed
            self.output_dir = os.path.join(os.path.dirname(settings.TRANSFORMED_OUTPUT_DIR), 'preprocessed')
        
        os.makedirs(self.output_dir, exist_ok=True)
        
        # Join cache for small lookup tables
        self.join_cache = defaultdict(lambda: defaultdict(dict))
        self.cache_size_limit = 10000  # Max records per table in cache
        
    def preprocess_file(self, filepath: str, timestamp: str = None) -> str:
        """
        Preprocess file with streaming joins
        
        Args:
            filepath: Input file path
            timestamp: Timestamp to use for output filename (for consistency across pipeline)
            
        Returns:
            Path to preprocessed file
        """
        self.logger.info(f"Starting streaming preprocessing: {filepath}")
        
        # Use provided timestamp or generate new one
        if not timestamp:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Output file
        output_filename = f"preprocessed_data_{timestamp}.json"
        if filepath.endswith('.gz'):
            output_filename += '.gz'
        output_path = os.path.join(self.output_dir, output_filename)
        
        # First pass: Build join cache for smaller tables
        self._build_join_cache(filepath)
        
        # Second pass: Process and write with joins
        self._process_with_joins(filepath, output_path)
        
        self.logger.info(f"Preprocessing completed: {output_path}")
        return output_path
    
    def _build_join_cache(self, filepath: str):
        """First pass: Build cache of join tables"""
        self.logger.info("Building join cache for lookup tables...")
        
        # Tables to cache (smaller lookup tables)
        cache_tables = {
            'identity': ['user_preferences', 'user_accounts', 'authentication_modules', 
                        'smtp_configuration', 'organization_policy'],
            'master': ['subscriptions', 'billing_addresses', 'data_generator_files'],
            'tenant': ['test_case_type', 'test_case_priorities', 'application_version',
                      'execution_result', 'api_step_result_response']
        }
        
        # Open input file
        if filepath.endswith('.gz'):
            infile = gzip.open(filepath, 'rb')
        else:
            infile = open(filepath, 'rb')
        
        with infile:
            parser = ijson.parse(infile)
            
            current_db = None
            current_table = None
            current_record = None
            current_field = None
            record_count = defaultdict(int)
            
            for prefix, event, value in parser:
                # Track current database
                if event == 'map_key' and prefix == '' and value != 'extraction_metadata':
                    current_db = value
                
                # Track current table
                elif event == 'map_key' and prefix == current_db:
                    current_table = value
                
                # Check if we should cache this table
                if current_db and current_table:
                    db_type = 'tenant' if 'tenant' in current_db else current_db
                    if db_type in cache_tables and current_table in cache_tables.get(db_type, []):
                        
                        # Record start
                        if event == 'start_map' and prefix == f"{current_db}.{current_table}.sample.item":
                            current_record = {}
                        
                        # Field name
                        elif event == 'map_key' and current_record is not None and prefix == f"{current_db}.{current_table}.sample.item":
                            current_field = value
                        
                        # Field value
                        elif current_record is not None and current_field and prefix == f"{current_db}.{current_table}.sample.item.{current_field}":
                            if event in ('string', 'number', 'boolean', 'null'):
                                current_record[current_field] = value
                                current_field = None
                        
                        # Record end - add to cache
                        elif event == 'end_map' and prefix == f"{current_db}.{current_table}.sample.item" and current_record is not None:
                            if record_count[f"{current_db}.{current_table}"] < self.cache_size_limit:
                                # Store by common lookup keys
                                if 'id' in current_record:
                                    self.join_cache[current_db][current_table][current_record['id']] = current_record.copy()
                                elif 'user_id' in current_record:
                                    self.join_cache[current_db][current_table][current_record['user_id']] = current_record.copy()
                                elif 'tenant_id' in current_record:
                                    self.join_cache[current_db][current_table][current_record['tenant_id']] = current_record.copy()
                                elif 'account_id' in current_record:
                                    self.join_cache[current_db][current_table][current_record['account_id']] = current_record.copy()
                                
                                record_count[f"{current_db}.{current_table}"] += 1
                            
                            current_record = None
        
        # Log cache statistics
        total_cached = sum(len(tables) for db_tables in self.join_cache.values() 
                          for tables in db_tables.values())
        self.logger.info(f"Cached {total_cached} records from lookup tables")
        gc.collect()
    
    def _process_with_joins(self, input_path: str, output_path: str):
        """Second pass: Process records with joins and write output"""
        self.logger.info("Processing file with joins...")
        
        # Open files
        if input_path.endswith('.gz'):
            infile = gzip.open(input_path, 'rb')
        else:
            infile = open(input_path, 'rb')
        
        if output_path.endswith('.gz'):
            outfile = gzip.open(output_path, 'wt', encoding='utf-8', compresslevel=1)
        else:
            outfile = open(output_path, 'w', encoding='utf-8')
        
        with infile, outfile:
            parser = ijson.parse(infile)
            
            # State tracking
            current_db = None
            current_table = None
            current_record = None
            current_field = None
            is_first = True
            in_sample = False
            sample_is_first = True
            record_count = 0
            
            # Write opening brace
            outfile.write('{\n')
            
            for prefix, event, value in parser:
                # Database level
                if event == 'map_key' and prefix == '':
                    if value != 'extraction_metadata':
                        current_db = value
                        if not is_first:
                            outfile.write(',\n')
                        is_first = False
                        outfile.write(f'  "{current_db}": {{')
                        current_table = None
                        table_is_first = True
                    else:
                        # Handle extraction_metadata
                        if not is_first:
                            outfile.write(',\n')
                        is_first = False
                        outfile.write(f'  "extraction_metadata": ')
                        
                # Table level
                elif event == 'map_key' and current_db and prefix == current_db:
                    current_table = value
                    if not table_is_first:
                        outfile.write(',\n')
                    table_is_first = False
                    outfile.write(f'\n    "{current_table}": {{')
                
                # Handle table structure
                elif current_table and prefix == f"{current_db}.{current_table}":
                    if event == 'map_key':
                        if value == 'records':
                            outfile.write(f'\n      "records": ')
                        elif value == 'sample':
                            outfile.write(',\n      "sample": [')
                            in_sample = True
                            sample_is_first = True
                    elif event == 'number' and prefix == f"{current_db}.{current_table}.records":
                        outfile.write(str(value))
                
                # Process records in sample
                elif in_sample and event == 'start_map' and prefix == f"{current_db}.{current_table}.sample.item":
                    current_record = {}
                    
                # Field name in record
                elif event == 'map_key' and current_record is not None and prefix == f"{current_db}.{current_table}.sample.item":
                    current_field = value
                
                # Field value in record
                elif current_record is not None and current_field and prefix == f"{current_db}.{current_table}.sample.item.{current_field}":
                    if event in ('string', 'number', 'boolean', 'null'):
                        current_record[current_field] = value
                        current_field = None
                
                # Record end - apply joins and write
                elif event == 'end_map' and prefix == f"{current_db}.{current_table}.sample.item" and current_record is not None:
                    # Apply joins based on table
                    joined_record = self._apply_joins(current_db, current_table, current_record)
                    
                    # Write record
                    if not sample_is_first:
                        outfile.write(',')
                    sample_is_first = False
                    outfile.write('\n        ')
                    # Convert Decimal objects before serialization
                    cleaned_record = convert_decimals(joined_record)
                    json.dump(cleaned_record, outfile, ensure_ascii=False)
                    
                    record_count += 1
                    if record_count % 10000 == 0:
                        self.logger.info(f"Processed {record_count:,} records")
                        if record_count % 50000 == 0:
                            gc.collect()
                    
                    current_record = None
                
                # Handle closing brackets
                elif event == 'end_array' and in_sample:
                    outfile.write('\n      ]')
                    in_sample = False
                    sample_is_first = True
                
                elif event == 'end_map':
                    if prefix == f"{current_db}.{current_table}":
                        outfile.write('\n    }')
                    elif prefix == current_db:
                        outfile.write('\n  }')
                
                # Handle extraction_metadata specially
                elif prefix == 'extraction_metadata' and event in ('start_map', 'end_map', 'map_key', 'string', 'number', 'boolean', 'null'):
                    if event == 'start_map':
                        outfile.write('{')
                    elif event == 'end_map':
                        outfile.write('}')
                    elif event == 'map_key':
                        if prefix.count('.') > 1:  # Nested keys
                            outfile.write(f', "{value}": ')
                        else:
                            if not prefix.endswith('extraction_metadata'):
                                outfile.write(', ')
                            outfile.write(f'"{value}": ')
                    else:
                        json.dump(value, outfile, ensure_ascii=False)
            
            # Close main object
            outfile.write('\n}\n')
        
        self.logger.info(f"Processed {record_count:,} total records")
    
    def _apply_joins(self, db_name: str, table_name: str, record: Dict) -> Dict:
        """Apply joins to a record based on table configuration"""
        # Return record as-is if not a table that needs joins
        join_configs = self._get_join_config(db_name, table_name)
        if not join_configs:
            return record
        
        # Create joined record
        joined_record = record.copy()
        
        for join_config in join_configs:
            join_table = join_config['table']
            join_key = join_config['key']
            main_key = join_config['main_key']
            prefix = join_config.get('prefix', join_table)
            
            # Get value to join on
            main_value = record.get(main_key)
            if main_value is None:
                continue
            
            # Look up in cache
            if (db_name in self.join_cache and 
                join_table in self.join_cache[db_name] and
                main_value in self.join_cache[db_name][join_table]):
                
                join_record = self.join_cache[db_name][join_table][main_value]
                
                # Add fields with prefix
                for field, value in join_record.items():
                    joined_record[f"{prefix}.{field}"] = value
        
        return joined_record
    
    def _get_join_config(self, db_name: str, table_name: str) -> List[Dict]:
        """Get join configuration for a specific table"""
        # This mirrors the logic in resilient_join_preprocessor.py
        
        if db_name == 'identity':
            if table_name == 'users':
                return [
                    {'table': 'user_preferences', 'key': 'user_id', 'main_key': 'id', 'prefix': 'user_preferences'},
                    {'table': 'user_accounts', 'key': 'user_id', 'main_key': 'id', 'prefix': 'user_accounts'}
                ]
            elif table_name == 'accounts':
                return [
                    {'table': 'authentication_modules', 'key': 'account_id', 'main_key': 'id', 'prefix': 'authentication_modules'},
                    {'table': 'smtp_configuration', 'key': 'account_id', 'main_key': 'id', 'prefix': 'smtp_configuration'}
                ]
        
        elif db_name == 'master':
            if table_name == 'data_generators':
                return [{'table': 'data_generator_files', 'key': 'id', 'main_key': 'file_id', 'prefix': 'data_generator_files'}]
            elif table_name == 'tenants':
                return [
                    {'table': 'subscriptions', 'key': 'tenant_id', 'main_key': 'id', 'prefix': 'subscriptions'},
                    {'table': 'billing_addresses', 'key': 'tenant_id', 'main_key': 'id', 'prefix': 'billing_addresses'}
                ]
        
        elif 'tenant' in db_name:
            if table_name == 'execution':
                return [
                    {'table': 'execution_result', 'key': 'execution_id', 'main_key': 'id', 'prefix': 'execution_result'},
                    {'table': 'application_version', 'key': 'id', 'main_key': 'app_version_id', 'prefix': 'application_version'}
                ]
            elif table_name == 'test_case':
                return [
                    {'table': 'test_case_type', 'key': 'id', 'main_key': 'type', 'prefix': 'test_case_type'},
                    {'table': 'test_case_priorities', 'key': 'id', 'main_key': 'proirity_id', 'prefix': 'test_case_priorities'},
                    {'table': 'application_version', 'key': 'id', 'main_key': 'application_version_id', 'prefix': 'application_version'}
                ]
            elif table_name == 'test_case_group':
                return [{'table': 'application_version', 'key': 'id', 'main_key': 'app_version_id', 'prefix': 'application_version'}]
            elif table_name == 'test_plan_result_metrics':
                return [{'table': 'execution_result', 'key': 'id', 'main_key': 'test_plan_result_id', 'prefix': 'execution_result'}]
            elif table_name == 'api_steps':
                return [{'table': 'api_step_result_response', 'key': 'api_step_id', 'main_key': 'id', 'prefix': 'api_step_result_response'}]
        
        return []


# Function to maintain backward compatibility
def preprocess_file(filepath: str, timestamp: str = None) -> str:
    """
    Backward compatible function that uses streaming preprocessor
    
    Args:
        filepath: Input file path
        timestamp: Optional timestamp for consistency across pipeline files
        
    Returns:
        Path to preprocessed file
    """
    preprocessor = StreamingJoinPreprocessor()
    return preprocessor.preprocess_file(filepath, timestamp)
