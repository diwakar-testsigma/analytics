"""
Data Transformer

Transforms extracted data to match target schema design.
Handles mapping from source tables to target tables based on
transformation mappings.
"""

import json
import os
import math
import gc
import gzip
import ijson
from datetime import datetime
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Any, Optional, Iterator, Tuple, IO
import logging
from .transformation_mapping import ALL_MAPPINGS, get_source_tables, get_column_mappings, list_all_tables


class DataTransformer:
    """Transforms extracted data to match target schema"""
    
    def __init__(self, config: Optional[Dict] = None):
        """
        Initialize data transformer
        
        Args:
            config: Optional configuration dictionary
        """
        if config:
            self.config = config
        else:
            from ..config import settings
            self.config = {
                'workers': settings.TRANSFORMATION_WORKERS,
                'output_dir': settings.TRANSFORMED_OUTPUT_DIR,
                'enable_concurrent': settings.ENABLE_CONCURRENT_PROCESSING,
                'batch_size': getattr(settings, 'TRANSFORMATION_BATCH_SIZE', 100),
                'enable_compression': getattr(settings, 'ENABLE_COMPRESSION', True)
            }
        
        self.logger = logging.getLogger(__name__)
        
        # Build reverse mapping for efficiency: source_table -> target_tables
        self.source_to_targets = {}
        for target_table, mapping in ALL_MAPPINGS.items():
            for source_table in mapping.get('source_tables', []):
                if source_table not in self.source_to_targets:
                    self.source_to_targets[source_table] = []
                self.source_to_targets[source_table].append({
                    'target': target_table,
                    'columns': mapping.get('column_mappings', {})
                })
        
        # Ensure output directory exists
        os.makedirs(self.config['output_dir'], exist_ok=True)
        
        # Streaming settings
        self.batch_size = self.config.get('batch_size', 100)
        self.enable_compression = self.config.get('enable_compression', True)
    
    def _open_file(self, filepath: str, mode: str = 'r') -> IO:
        """
        Open file with automatic compression detection
        
        Args:
            filepath: Path to file
            mode: Open mode ('r' for read, 'w' for write)
            
        Returns:
            File handle
        """
        if filepath.endswith('.gz'):
            if 'b' not in mode:
                mode = mode.replace('r', 'rt').replace('w', 'wt')
            return gzip.open(filepath, mode, encoding='utf-8' if 't' in mode else None)
        else:
            return open(filepath, mode, encoding='utf-8' if 'b' not in mode else None)
    
    def _stream_records(self, filepath: str) -> Iterator[Tuple[str, str, Dict]]:
        """
        Stream records from file yielding (database, table, record) tuples
        
        Args:
            filepath: Path to extracted data file
            
        Yields:
            Tuples of (database_name, table_name, record)
        """
        with self._open_file(filepath, 'r') as f:
            parser = ijson.parse(f)
            
            current_database = None
            current_table = None
            current_field = None
            in_record = False
            record = None
            
            for prefix, event, value in parser:
                # Simplified parsing focused on records only
                if event == 'map_key':
                    if prefix.count('.') == 1 and value != 'extraction_metadata':
                        current_database = value
                    elif prefix.count('.') == 3:
                        current_table = value
                    elif in_record:
                        current_field = value
                
                elif prefix.endswith('.sample.item') and event == 'start_map':
                    in_record = True
                    record = {}
                
                elif in_record and current_field and event in ('string', 'number', 'boolean', 'null'):
                    record[current_field] = value
                    current_field = None
                
                elif in_record and event == 'end_map' and record:
                    if current_database and current_table:
                        yield current_database, current_table, record
                    in_record = False
                    record = None
    
    def sanitize_value(self, value: Any) -> Any:
        """
        Sanitize values to be JSON serializable
        Handles NaN, Infinity, and other problematic values
        
        Args:
            value: Value to sanitize
            
        Returns:
            Sanitized value safe for JSON serialization
        """
        if value is None:
            return None
        if isinstance(value, float):
            if math.isnan(value) or math.isinf(value):
                return None
        if isinstance(value, (list, tuple)):
            return [self.sanitize_value(v) for v in value]
        if isinstance(value, dict):
            return {k: self.sanitize_value(v) for k, v in value.items()}
        return value
    
    def sanitize_records(self, records: List[Dict]) -> List[Dict]:
        """
        Sanitize all records in a list
        
        Args:
            records: List of records to sanitize
            
        Returns:
            List of sanitized records
        """
        return [
            {k: self.sanitize_value(v) for k, v in record.items()}
            for record in records
        ]
    
    def transform_table_data(self, source_table: str, source_data: List[Dict]) -> Dict[str, List[Dict]]:
        """
        Transform data from source table to target Snowflake tables
        
        Args:
            source_table: Source table name
            source_data: List of records from source table
            
        Returns:
            Dictionary mapping Snowflake table names to transformed records
        """
        transformed_data = {}
        
        # Find all target tables that use this source table
        target_tables = []
        for target_table, mapping in ALL_MAPPINGS.items():
            if source_table in mapping.get('source_tables', []):
                target_tables.append(target_table)
        
        if not target_tables:
            self.logger.debug(f"No target tables found for source table: {source_table}")
            return transformed_data
        
        # Transform data for each target table
        for target_table in target_tables:
            mapping = ALL_MAPPINGS[target_table]
            column_mappings = mapping.get('column_mappings', {})
            
            if not column_mappings:
                continue
                
            # Transform records for this target table
            target_records = []
            for record in source_data:
                try:
                    transformed_record = {}
                    
                    # Map columns from source to target
                    for target_column, source_field in column_mappings.items():
                        # Handle nested field references (e.g., "users.id")
                        if '.' in source_field:
                            table_name, field_name = source_field.split('.', 1)
                            if table_name == source_table and field_name in record:
                                value = record[field_name]
                                transformed_record[target_column] = self._clean_value(value, target_column, target_table)
                        elif source_field in record:
                            value = record[source_field]
                            transformed_record[target_column] = self._clean_value(value, target_column, target_table)
                    
                    # Only add record if it has some data
                    if transformed_record:
                        target_records.append(transformed_record)
                        
                except Exception as e:
                    self.logger.error(f"Error transforming record from {source_table} to {target_table}: {e}")
                    continue
            
            if target_records:
                transformed_data[target_table] = target_records
        
        return transformed_data
    
    def _clean_value(self, value: Any, column_name: str, table_name: str) -> Any:
        """
        Clean and convert values for Snowflake compatibility
        
        Args:
            value: Raw value from MySQL
            column_name: Target column name
            table_name: Target table name
            
        Returns:
            Cleaned value for Snowflake
        """
        # Handle MySQL TINYINT(1) boolean conversion
        if column_name == 'auth_enabled' and table_name == 'dim_accounts':
            if isinstance(value, bytes):
                # Convert byte string to boolean
                return bool(int.from_bytes(value, byteorder='big'))
            elif value is not None:
                return bool(value)
            return None
        
        # Handle NULL values for non-nullable columns
        if column_name == 'tenant_id' and table_name == 'fct_audit_events':
            if value is None:
                # Use a default tenant_id of 0 for NULL values
                self.logger.warning(f"NULL tenant_id found for {table_name}, using default value 0")
                return 0
        
        # Handle other byte string conversions
        if isinstance(value, bytes):
            try:
                # Try to decode as UTF-8 string
                return value.decode('utf-8')
            except:
                # If decoding fails, convert to string representation
                return str(value)
        
        return value
    
    def transform_database_data(self, database: str, database_data: Dict) -> Dict[str, List[Dict]]:
        """
        Transform all tables from a database with proper table joins
        
        Args:
            database: Database name
            database_data: Dictionary of table data
            
        Returns:
            Dictionary mapping Snowflake table names to transformed records
        """
        all_transformed_data = {table: [] for table in ALL_MAPPINGS.keys()}
        
        # Process each target table by joining its source tables
        for target_table, mapping in ALL_MAPPINGS.items():
            source_tables = mapping.get('source_tables', [])
            column_mappings = mapping.get('column_mappings', {})
            primary_key = mapping.get('primary_key')
            
            if not source_tables or not column_mappings:
                continue
            
            # Check if all required source tables are available in this database
            available_tables = {}
            for source_table in source_tables:
                if source_table in database_data:
                    table_info = database_data[source_table]
                    source_data = table_info.get('sample', [])
                    if source_data:
                        available_tables[source_table] = source_data
            
            if not available_tables:
                self.logger.debug(f"No source data available for target table: {target_table}")
                continue
            
            # Join tables and create consolidated records
            joined_records = self._join_source_tables(target_table, available_tables, column_mappings, primary_key)
            
            if joined_records:
                all_transformed_data[target_table] = joined_records
                self.logger.debug(f"Created {len(joined_records)} records for {target_table}")
        
        return all_transformed_data
    
    def _join_source_tables(self, target_table: str, available_tables: Dict[str, List[Dict]], 
                           column_mappings: Dict[str, str], primary_key: str) -> List[Dict]:
        """
        Join multiple source tables to create consolidated records for a target table
        
        Args:
            target_table: Name of the target table
            available_tables: Dictionary of source table data
            column_mappings: Column mappings from source to target
            primary_key: Primary key column name
            
        Returns:
            List of consolidated records
        """
        if not available_tables:
            return []
        
        # Find the main table (usually the one with the primary key)
        main_table = None
        main_table_data = None
        
        for source_table, source_data in available_tables.items():
            # Check if this table contains the primary key field
            # Handle both single primary key and composite keys
            if isinstance(primary_key, list):
                # For composite keys, use the first key
                pk_to_check = primary_key[0]
            else:
                pk_to_check = primary_key
                
            primary_key_mapping = column_mappings.get(pk_to_check, "")
            if source_data and '.' in primary_key_mapping:
                # Extract the table name from the primary key mapping
                table_name = primary_key_mapping.split('.')[0]
                if table_name == source_table:
                    main_table = source_table
                    main_table_data = source_data
                    break
        
        # If no main table found, use the first available table
        if not main_table:
            main_table = list(available_tables.keys())[0]
            main_table_data = available_tables[main_table]
        
        # Create consolidated records
        consolidated_records = []
        
        for main_record in main_table_data:
            try:
                consolidated_record = {}
                
                # Map all columns from all source tables
                for target_column, source_field in column_mappings.items():
                    if '.' in source_field:
                        table_name, field_name = source_field.split('.', 1)
                        
                        # Get data from the appropriate source table
                        if table_name == main_table and field_name in main_record:
                            value = main_record[field_name]
                            consolidated_record[target_column] = self._clean_value(value, target_column, target_table)
                        elif table_name in available_tables:
                            # Find related record in other table
                            related_record = self._find_related_record(
                                main_record, main_table, 
                                available_tables[table_name], table_name, 
                                target_column, source_field
                            )
                            if related_record and field_name in related_record:
                                value = related_record[field_name]
                                consolidated_record[target_column] = self._clean_value(value, target_column, target_table)
                    else:
                        # Direct field mapping
                        if source_field in main_record:
                            value = main_record[source_field]
                            consolidated_record[target_column] = self._clean_value(value, target_column, target_table)
                
                # Only add record if it has meaningful data
                if consolidated_record and any(v is not None and v != "" for v in consolidated_record.values()):
                    consolidated_records.append(consolidated_record)
                    
            except Exception as e:
                self.logger.error(f"Error joining records for {target_table}: {e}")
                continue
        
        return consolidated_records
    
    def _find_related_record(self, main_record: Dict, main_table: str, 
                           related_data: List[Dict], related_table: str,
                           target_column: str, source_field: str) -> Optional[Dict]:
        """
        Find a related record in another table based on common keys
        
        Args:
            main_record: Record from the main table
            main_table: Name of the main table
            related_data: Data from the related table
            related_table: Name of the related table
            target_column: Target column name
            source_field: Source field mapping
            
        Returns:
            Related record if found, None otherwise
        """
        # For now, use simple heuristics to find related records
        # This can be enhanced with proper foreign key relationships
        
        # Common join patterns based on table names
        join_patterns = {
            ('users', 'user_preferences'): 'user_id',
            ('users', 'user_accounts'): 'user_id',
            ('user_accounts', 'users'): 'user_id',
            ('user_preferences', 'users'): 'user_id',
            ('organizations', 'organization_policy'): 'organization_id',
            ('accounts', 'authentication_modules'): 'account_id',
            ('accounts', 'smtp_configuration'): 'account_id',
            ('tenants', 'subscriptions'): 'tenant_id',
            ('tenants', 'billing_addresses'): 'tenant_id',
            ('test_case', 'application_version'): 'application_id',
            ('execution', 'execution_result'): 'execution_id',
            ('test_case_group', 'application_version'): 'application_id'
        }
        
        # Try to find a common key
        join_key = join_patterns.get((main_table, related_table))
        if not join_key:
            join_key = join_patterns.get((related_table, main_table))
        
        if join_key and join_key in main_record:
            main_key_value = main_record[join_key]
            
            # Find matching record in related table
            for related_record in related_data:
                if join_key in related_record and related_record[join_key] == main_key_value:
                    return related_record
        
        # If no specific join pattern, return the first record (simple approach)
        return related_data[0] if related_data else None
    
    def transform_file_streaming(self, filepath: str) -> str:
        """
        Transform data from an extracted file using streaming
        
        Args:
            filepath: Path to extracted data file
            
        Returns:
            Path to transformed data file
        """
        self.logger.info(f"Transforming file (streaming): {filepath}")
        
        # Prepare output file
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_filename = f"snowflake_data_{timestamp}.json"
        if self.enable_compression:
            output_filename += '.gz'
        output_path = os.path.join(self.config['output_dir'], output_filename)
        
        # Track statistics
        source_record_count = 0
        transformed_count = {}
        written_tables = set()
        
        with self._open_file(output_path, 'w') as out_file:
            # Write header
            out_file.write('{\n  "etl_timestamp": "')
            out_file.write(datetime.now().isoformat())
            out_file.write('",\n  "tables": {\n')
            
            first_table = True
            table_buffers = {}
            
            # Process records in streaming fashion
            for database, source_table, record in self._stream_records(filepath):
                source_record_count += 1
                
                # Only process if we have mappings for this source table
                if source_table not in self.source_to_targets:
                    continue
                
                # Process only relevant target tables (much faster!)
                for target_info in self.source_to_targets[source_table]:
                    target_table = target_info['target']
                    column_mappings = target_info['columns']
                    
                    if not column_mappings:
                        continue
                    
                    # Transform record inline for efficiency
                    transformed = {}
                    for target_col, source_field in column_mappings.items():
                        if '.' in source_field:
                            table_name, field_name = source_field.split('.', 1)
                            if table_name == source_table and field_name in record:
                                transformed[target_col] = self._clean_value(
                                    record[field_name], target_col, target_table
                                )
                        elif source_field in record:
                            transformed[target_col] = self._clean_value(
                                record[source_field], target_col, target_table
                            )
                    
                    if not transformed:
                        continue
                    
                    # Sanitize values
                    transformed = {k: self.sanitize_value(v) for k, v in transformed.items()}
                    
                    # Buffer management
                    if target_table not in table_buffers:
                        table_buffers[target_table] = []
                    
                    table_buffers[target_table].append(transformed)
                    
                    # Write batch when full
                    if len(table_buffers[target_table]) >= self.batch_size:
                        self._write_table_batch(out_file, target_table, table_buffers[target_table],
                                              written_tables, first_table)
                        if target_table not in written_tables:
                            written_tables.add(target_table)
                            first_table = False
                        table_buffers[target_table] = []
                
                # Progress logging and memory management
                if source_record_count % 10000 == 0:
                    self.logger.info(f"Processed {source_record_count:,} source records...")
                    if source_record_count % 50000 == 0:
                        gc.collect()
            
            # Write remaining buffers
            for target_table, records in table_buffers.items():
                if records:
                    self._write_table_batch(out_file, target_table, records, written_tables, first_table)
                    if target_table not in written_tables:
                        written_tables.add(target_table)
                        first_table = False
                    transformed_count[target_table] = transformed_count.get(target_table, 0) + len(records)
            
            # Close all open table arrays
            for i, table in enumerate(written_tables):
                out_file.write('\n    ]')
                if i < len(written_tables) - 1:
                    out_file.write(',')
            
            # Close JSON structure
            out_file.write('\n  }\n}\n')
        
        # Log summary
        total_transformed = sum(transformed_count.values())
        self.logger.info(
            f"Transformation complete: {source_record_count:,} source records -> "
            f"{total_transformed:,} transformed records"
        )
        
        return output_path
    
    def _write_table_batch(self, out_file: IO, table_name: str, records: List[Dict],
                          written_tables: set, first_table: bool) -> None:
        """
        Write a batch of records for a table to the output file
        """
        if not records:
            return
        
        # Write table opening if first time
        if table_name not in written_tables:
            if not first_table:
                out_file.write(',\n')
            out_file.write(f'    "{table_name}": [\n')
        else:
            # Continue existing table
            out_file.write(',\n')
        
        # Write records compactly
        for i, record in enumerate(records):
            if i > 0:
                out_file.write(',\n')
            out_file.write('      ')
            json.dump(record, out_file, separators=(',', ':'))
    
    def transform_file(self, filepath: str, use_streaming: bool = True) -> str:
        """
        Transform data from an extracted file
        
        Args:
            filepath: Path to extracted data file
            use_streaming: Whether to use streaming for large files
            
        Returns:
            Path to transformed data file
        """
        # Check file size to decide on streaming
        file_size_mb = os.path.getsize(filepath) / (1024 * 1024)
        
        # Use streaming for files > 100MB or if explicitly requested
        if use_streaming or file_size_mb > 100:
            return self.transform_file_streaming(filepath)
        
        # For small files, use the original method
        self.logger.info(f"Transforming file: {filepath}")
        
        # Load extracted data
        with open(filepath, 'r') as f:
            extracted_data = json.load(f)
        
        # Initialize transformed data structure
        all_transformed_data = {table: [] for table in ALL_MAPPINGS.keys()}
        
        # Process data based on file structure
        if isinstance(extracted_data, dict):
            # Check if it's a multi-database format
            if all(isinstance(v, dict) for v in extracted_data.values() if v != 'extraction_metadata'):
                # Multi-database format
                for database, database_data in extracted_data.items():
                    if database == 'extraction_metadata':
                        continue
                    self.logger.info(f"Processing database: {database}")
                    transformed_data = self.transform_database_data(database, database_data)
                    
                    # Merge transformed data
                    for table, records in transformed_data.items():
                        all_transformed_data[table].extend(records)
            else:
                # Single table format
                table_name = extracted_data.get('table')
                source_data = extracted_data.get('data', [])
                
                if table_name and source_data:
                    transformed_data = self.transform_table_data(table_name, source_data)
                    
                    # Merge transformed data
                    for table, records in transformed_data.items():
                        all_transformed_data[table].extend(records)
        
        # Sanitize all transformed data to ensure JSON compatibility
        sanitized_tables = {}
        for table_name, records in all_transformed_data.items():
            if records:
                sanitized_tables[table_name] = self.sanitize_records(records)
            else:
                sanitized_tables[table_name] = records
        
        # Create output data structure
        output_data = {
            'etl_timestamp': datetime.now().isoformat(),
            'tables': sanitized_tables
        }
        
        # Save transformed data
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_filename = f"snowflake_data_{timestamp}.json"
        output_path = os.path.join(self.config['output_dir'], output_filename)
        
        with open(output_path, 'w') as f:
            json.dump(output_data, f, indent=2, default=str, ensure_ascii=False)
        
        # Log summary
        total_records = sum(len(records) for records in sanitized_tables.values())
        self.logger.info(
            f"Transformation complete: {total_records} total records across "
            f"{len([t for t, r in sanitized_tables.items() if r])} tables (data sanitized for JSON)"
        )
        
        return output_path
    
    def transform_files_parallel(self, filepaths: List[str]) -> str:
        """
        Transform multiple files in parallel
        
        Args:
            filepaths: List of file paths to transform
            
        Returns:
            Path to consolidated transformed data file
        """
        self.logger.info(f"Transforming {len(filepaths)} files in parallel")
        
        # Initialize consolidated data
        all_transformed_data = {table: [] for table in ALL_MAPPINGS.keys()}
        
        if self.config.get('enable_concurrent', True):
            # Process files in parallel
            with ThreadPoolExecutor(max_workers=self.config.get('workers', 4)) as executor:
                future_to_file = {
                    executor.submit(self._process_file_for_parallel, filepath): filepath
                    for filepath in filepaths
                }
                
                for future in as_completed(future_to_file):
                    filepath = future_to_file[future]
                    try:
                        file_transformed_data = future.result()
                        
                        # Merge results
                        for table, records in file_transformed_data.items():
                            all_transformed_data[table].extend(records)
                            
                    except Exception as e:
                        self.logger.error(f"Failed to transform {filepath}: {e}")
        else:
            # Sequential processing
            for filepath in filepaths:
                try:
                    file_transformed_data = self._process_file_for_parallel(filepath)
                    
                    # Merge results
                    for table, records in file_transformed_data.items():
                        all_transformed_data[table].extend(records)
                        
                except Exception as e:
                    self.logger.error(f"Failed to transform {filepath}: {e}")
        
        # Sanitize all transformed data to ensure JSON compatibility
        sanitized_tables = {}
        for table_name, records in all_transformed_data.items():
            if records:
                sanitized_tables[table_name] = self.sanitize_records(records)
            else:
                sanitized_tables[table_name] = records
        
        # Create output data structure
        output_data = {
            'etl_timestamp': datetime.now().isoformat(),
            'tables': sanitized_tables
        }
        
        # Save consolidated transformed data
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_filename = f"snowflake_data_{timestamp}.json"
        output_path = os.path.join(self.config['output_dir'], output_filename)
        
        with open(output_path, 'w') as f:
            json.dump(output_data, f, indent=2, default=str, ensure_ascii=False)
        
        return output_path
    
    def _process_file_for_parallel(self, filepath: str) -> Dict[str, List[Dict]]:
        """
        Process a single file for parallel transformation
        
        Args:
            filepath: Path to file to process
            
        Returns:
            Dictionary of transformed data
        """
        # Load extracted data
        with open(filepath, 'r') as f:
            extracted_data = json.load(f)
        
        # Initialize result
        result = {table: [] for table in ALL_MAPPINGS.keys()}
        
        # Process data based on file structure
        if isinstance(extracted_data, dict):
            # Check if it's a multi-database format
            if all(isinstance(v, dict) for v in extracted_data.values()):
                # Multi-database format
                for database, database_data in extracted_data.items():
                    transformed_data = self.transform_database_data(database, database_data)
                    
                    # Merge transformed data
                    for table, records in transformed_data.items():
                        result[table].extend(records)
            else:
                # Single table format
                table_name = extracted_data.get('table')
                source_data = extracted_data.get('data', [])
                
                if table_name and source_data:
                    transformed_data = self.transform_table_data(table_name, source_data)
                    
                    # Merge transformed data
                    for table, records in transformed_data.items():
                        result[table].extend(records)
        
        return result
    
    def get_transformation_stats(self, transformed_file: str) -> Dict:
        """Get statistics about transformed data"""
        with open(transformed_file, 'r') as f:
            data = json.load(f)
        
        tables = data.get('tables', {})
        
        stats = {
            'total_tables': len([t for t, r in tables.items() if r]),
            'total_records': sum(len(records) for records in tables.values()),
            'tables': {table: len(records) for table, records in tables.items() if records}
        }
        
        return stats


if __name__ == "__main__":
    # Example usage
    transformer = DataTransformer()
    
    # Transform a single file
    transformed_file = transformer.transform_file("output/extracted/sample_data.json")
    print(f"Transformation complete: {transformed_file}")
