"""
Streaming Data Transformer

Transforms extracted data to match target schema design using streaming JSON processing
to handle large files efficiently without loading everything into memory.
"""

import json
import os
import gzip
import math
import ijson
import orjson
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any, Optional, Iterator, Tuple, IO
import logging
from .transformation_mapping import ALL_MAPPINGS, get_source_tables, get_column_mappings, list_all_tables


class StreamingDataTransformer:
    """Transforms extracted data using streaming to minimize memory usage"""
    
    def __init__(self, config: Optional[Dict] = None):
        """
        Initialize streaming data transformer
        
        Args:
            config: Optional configuration dictionary
        """
        if config:
            self.config = config
        else:
            from ..config import settings
            self.config = {
                'output_dir': settings.TRANSFORMED_OUTPUT_DIR,
                'batch_size': getattr(settings, 'TRANSFORMATION_BATCH_SIZE', 100),  # Reduced for memory efficiency
                'enable_compression': getattr(settings, 'ENABLE_COMPRESSION', True),
                'compression_level': getattr(settings, 'COMPRESSION_LEVEL', 6)
            }
        
        self.logger = logging.getLogger(__name__)
        self.batch_size = self.config.get('batch_size', 100)  # Small batches for large files
        
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
    
    def _transform_record(self, source_table: str, record: Dict, target_table: str, column_mappings: Dict) -> Optional[Dict]:
        """
        Transform a single record from source to target format
        
        Args:
            source_table: Source table name
            record: Source record
            target_table: Target table name
            column_mappings: Column mappings for this target table
            
        Returns:
            Transformed record or None if no valid data
        """
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
            
            # Only return record if it has some data
            if transformed_record:
                return {k: self.sanitize_value(v) for k, v in transformed_record.items()}
            
        except Exception as e:
            self.logger.error(f"Error transforming record from {source_table} to {target_table}: {e}")
        
        return None
    
    def _stream_database_data(self, filepath: str) -> Iterator[Tuple[str, str, Dict]]:
        """
        Stream database data from file yielding (database, table, record) tuples
        
        Args:
            filepath: Path to extracted data file
            
        Yields:
            Tuples of (database_name, table_name, record)
        """
        with self._open_file(filepath, 'r') as f:
            # Parse the JSON structure to identify databases and tables
            parser = ijson.parse(f)
            
            current_database = None
            current_table = None
            in_sample = False
            record_builder = None
            
            for prefix, event, value in parser:
                # Track current database
                if prefix.endswith('.item') and event == 'start_map' and prefix.count('.') == 1:
                    current_database = None
                elif prefix.count('.') == 1 and event == 'map_key':
                    if value != 'extraction_metadata':
                        current_database = value
                
                # Track current table
                elif current_database and prefix.endswith('.item') and event == 'start_map' and prefix.count('.') == 3:
                    current_table = None
                elif current_database and prefix.count('.') == 3 and event == 'map_key':
                    current_table = value
                
                # Track if we're in the sample array
                elif current_database and current_table and prefix.endswith('.sample') and event == 'start_array':
                    in_sample = True
                elif current_database and current_table and prefix.endswith('.sample') and event == 'end_array':
                    in_sample = False
                
                # Build records from sample data
                elif in_sample and prefix.endswith('.item') and event == 'start_map':
                    record_builder = {}
                elif in_sample and record_builder is not None and event == 'map_key':
                    field_name = value
                elif in_sample and record_builder is not None and event in ('string', 'number', 'boolean', 'null'):
                    # Get the field name from the prefix
                    parts = prefix.split('.')
                    if len(parts) >= 2:
                        field_name = parts[-1]
                        record_builder[field_name] = value
                elif in_sample and prefix.endswith('.item') and event == 'end_map' and record_builder is not None:
                    if current_database and current_table:
                        yield current_database, current_table, record_builder
                    record_builder = None
    
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
        if self.config.get('enable_compression', True):
            output_filename += '.gz'
        output_path = os.path.join(self.config['output_dir'], output_filename)
        
        # Track statistics
        source_record_count = 0
        transformed_count = {}
        
        # Open output file for streaming write
        with self._open_file(output_path, 'w') as out_file:
            # Write opening structure
            out_file.write('{\n')
            out_file.write(f'  "etl_timestamp": "{datetime.now().isoformat()}",\n')
            out_file.write('  "tables": {\n')
            
            # Buffer for each target table (create on demand)
            table_buffers = {}
            first_table_written = False
            
            # Process records in streaming fashion
            for database, source_table, record in self._stream_database_data(filepath):
                source_record_count += 1
                
                # Only process if we have mappings for this source table
                if source_table in self.source_to_targets:
                    # Process only relevant target tables (much faster!)
                    for target_info in self.source_to_targets[source_table]:
                        target_table = target_info['target']
                        column_mappings = target_info['columns']
                        if column_mappings:
                            # Transform the record
                            transformed = self._transform_record(source_table, record, target_table, column_mappings)
                            if transformed:
                                # Create buffer on first use
                                if target_table not in table_buffers:
                                    table_buffers[target_table] = []
                                
                                table_buffers[target_table].append(transformed)
                                
                                # Write batch if buffer is full
                                if len(table_buffers[target_table]) >= self.batch_size:
                                    self._write_table_batch(out_file, target_table, table_buffers[target_table], 
                                                          first_table_written)
                                    if table_buffers[target_table]:  # If we wrote something
                                        first_table_written = True
                                    table_buffers[target_table] = []
                
                # Log progress periodically
                if source_record_count % 10000 == 0:
                    self.logger.info(f"Processed {source_record_count:,} source records...")
            
            # Write remaining records in buffers
            for target_table, records in table_buffers.items():
                if records:
                    self._write_table_batch(out_file, target_table, records, first_table_written)
                    first_table_written = True
                    transformed_count[target_table] = transformed_count.get(target_table, 0) + len(records)
            
            # Skip writing empty tables - not needed and wastes memory
            
            # Close all open table arrays
            if hasattr(self, '_written_tables'):
                for i, table in enumerate(self._written_tables):
                    out_file.write('\n    ]')
                    if i < len(self._written_tables) - 1:
                        out_file.write(',')
            
            # Close JSON structure
            out_file.write('\n  }\n}\n')
        
        # Log summary
        total_transformed = sum(transformed_count.values())
        self.logger.info(
            f"Streaming transformation complete: {source_record_count:,} source records -> "
            f"{total_transformed:,} transformed records across {len(transformed_count)} tables"
        )
        if self.config.get('enable_compression', True):
            self.logger.info(f"Output compressed with gzip")
        
        return output_path
    
    def _write_table_batch(self, out_file: IO, table_name: str, records: List[Dict], 
                          need_comma: bool) -> None:
        """
        Write a batch of records for a table to the output file
        
        Args:
            out_file: Output file handle
            table_name: Target table name
            records: Records to write
            need_comma: Whether to prepend a comma
        """
        if not records:
            return
        
        # Update tracking
        if not hasattr(self, '_written_tables'):
            self._written_tables = set()
        
        # Write table opening if first time
        if table_name not in self._written_tables:
            if need_comma:
                out_file.write(',\n')
            out_file.write(f'    "{table_name}": [\n')
            self._written_tables.add(table_name)
            first_record = True
        else:
            # Continue existing table
            first_record = False
            out_file.write(',\n')  # Comma before new records
        
        # Write records using orjson for efficiency
        for i, record in enumerate(records):
            if not first_record or i > 0:
                out_file.write(',\n')
            # Write compactly to save memory
            out_file.write('      ')
            json.dump(record, out_file, separators=(',', ':'))
            first_record = False
    
    def transform_file_with_compression(self, filepath: str, compress_output: bool = True) -> str:
        """
        Transform file with optional output compression
        
        Args:
            filepath: Path to input file
            compress_output: Whether to compress the output
            
        Returns:
            Path to output file
        """
        # Override compression setting temporarily
        original_setting = self.config.get('enable_compression', True)
        self.config['enable_compression'] = compress_output
        
        try:
            return self.transform_file_streaming(filepath)
        finally:
            self.config['enable_compression'] = original_setting


# Factory function to get appropriate transformer
def get_transformer(streaming: bool = True, config: Optional[Dict] = None):
    """
    Get appropriate transformer based on requirements
    
    Args:
        streaming: Whether to use streaming transformer
        config: Optional configuration
        
    Returns:
        Transformer instance
    """
    if streaming:
        return StreamingDataTransformer(config)
    else:
        # Fall back to regular transformer
        from .transformer import DataTransformer
        return DataTransformer(config)


if __name__ == "__main__":
    # Example usage
    import sys
    
    if len(sys.argv) > 1:
        input_file = sys.argv[1]
        transformer = StreamingDataTransformer()
        output_file = transformer.transform_file_streaming(input_file)
        print(f"Transformation complete: {output_file}")
    else:
        print("Usage: python streaming_transformer.py <input_file>")
