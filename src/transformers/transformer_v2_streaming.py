"""
Transformer V2 Streaming - True streaming with ijson

Processes JSON incrementally without loading the entire file.
Optimized for very large files (10GB+).
"""

import json
import gzip
import gc
import os
import ijson
from datetime import datetime
from typing import Dict, Any, IO, Iterator, Tuple, Optional
import logging

from .transformation_mapping import ALL_MAPPINGS


class StreamingTransformerV2:
    """Ultra-efficient streaming transformer"""
    
    def __init__(self, output_dir: str = "output/transformations"):
        self.output_dir = output_dir
        self.logger = logging.getLogger(__name__)
        
        # Pre-compute mappings
        self.source_mappings = self._build_source_mappings()
        
        os.makedirs(output_dir, exist_ok=True)
    
    def _build_source_mappings(self) -> Dict[str, list]:
        """Build reverse mapping for O(1) lookup"""
        mappings = {}
        
        for target_table, config in ALL_MAPPINGS.items():
            for source_table in config.get('source_tables', []):
                if source_table not in mappings:
                    mappings[source_table] = []
                
                mappings[source_table].append({
                    'target': target_table,
                    'columns': config.get('column_mappings', {})
                })
        
        self.logger.info(f"Built mappings for {len(mappings)} source tables")
        return mappings
    
    def transform_file(self, input_file: str) -> str:
        """Transform with true streaming - never loads full file"""
        # Output file
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = os.path.join(self.output_dir, f"snowflake_data_{timestamp}.json.gz")
        
        self.logger.info(f"Starting streaming transformation: {input_file}")
        
        # Process with streaming
        with gzip.open(output_file, 'wt', encoding='utf-8', compresslevel=1) as outfile:
            # Write header
            outfile.write('{\n')
            outfile.write(f'  "etl_timestamp": "{datetime.now().isoformat()}",\n')
            outfile.write('  "tables": {\n')
            
            # Process records
            table_states = {}  # Track open tables
            record_count = 0
            
            # Stream records from input
            for db_name, table_name, record in self._stream_records(input_file):
                record_count += 1
                
                # Skip if no mappings
                if table_name not in self.source_mappings:
                    continue
                
                # Transform for each target table
                for mapping in self.source_mappings[table_name]:
                    target = mapping['target']
                    columns = mapping['columns']
                    
                    # Transform record
                    transformed = self._transform_record(record, table_name, target, columns)
                    if not transformed:
                        continue
                    
                    # Write record (open table if needed)
                    if target not in table_states:
                        if table_states:  # Not first table
                            outfile.write(',\n')
                        outfile.write(f'    "{target}": [\n      ')
                        table_states[target] = {'first': True}
                    else:
                        outfile.write(',\n      ')
                    
                    # Write record
                    json.dump(transformed, outfile, separators=(',', ':'))
                
                # Progress & cleanup
                if record_count % 10000 == 0:
                    self.logger.info(f"Processed {record_count:,} records")
                    if record_count % 50000 == 0:
                        gc.collect()  # Force cleanup
            
            # Close tables
            for table in table_states:
                outfile.write(f'\n    ]')
                if table != list(table_states.keys())[-1]:
                    outfile.write(',')
            
            # Close JSON
            outfile.write('\n  }\n}\n')
        
        self.logger.info(f"Complete: {record_count:,} records -> {output_file}")
        return output_file
    
    def _stream_records(self, input_file: str) -> Iterator[Tuple[str, str, Dict]]:
        """Stream records using ijson - truly incremental parsing"""
        # Open file
        if input_file.endswith('.gz'):
            f = gzip.open(input_file, 'rb')
        else:
            f = open(input_file, 'rb')
        
        try:
            # Parse incrementally
            parser = ijson.parse(f)
            
            # State tracking
            path = []
            current_db = None
            current_table = None
            current_record = None
            field_name = None
            in_sample = False
            
            for prefix, event, value in parser:
                # Track path through JSON
                if event == 'map_key':
                    if len(path) == 0 and value != 'extraction_metadata':
                        current_db = value
                    elif len(path) == 1 and current_db:
                        current_table = value
                    elif in_sample and current_record is not None:
                        field_name = value
                
                elif event == 'start_map':
                    path.append('map')
                    # Check if starting a record in sample array
                    if prefix.endswith('.sample.item'):
                        current_record = {}
                
                elif event == 'end_map':
                    path.pop()
                    # Check if ending a record
                    if current_record is not None and prefix.endswith('.sample.item'):
                        if current_db and current_table:
                            yield current_db, current_table, current_record
                        current_record = None
                
                elif event == 'start_array':
                    path.append('array')
                    if prefix.endswith('.sample'):
                        in_sample = True
                
                elif event == 'end_array':
                    path.pop()
                    if prefix.endswith('.sample'):
                        in_sample = False
                
                # Capture field values
                elif current_record is not None and field_name and event in ('string', 'number', 'boolean', 'null'):
                    current_record[field_name] = value
                    field_name = None
                    
        finally:
            f.close()
    
    def _transform_record(self, record: Dict, source_table: str, 
                         target_table: str, mappings: Dict) -> Optional[Dict]:
        """Transform single record"""
        result = {}
        
        for target_col, source_spec in mappings.items():
            try:
                value = None
                
                if '.' in source_spec:
                    table, col = source_spec.split('.', 1)
                    if table == source_table and col in record:
                        value = record[col]
                elif source_spec in record:
                    value = record[source_spec]
                
                if value is not None:
                    # Clean value
                    if isinstance(value, bytes):
                        value = value.decode('utf-8', errors='replace')
                    elif target_col == 'tenant_id' and value is None:
                        value = 0
                    elif target_col == 'auth_enabled' and target_table == 'dim_accounts':
                        value = bool(value) if value is not None else None
                    
                    result[target_col] = value
                    
            except Exception as e:
                self.logger.debug(f"Transform error: {e}")
                
        return result if result else None


# Simple entry point
def transform(input_file: str) -> str:
    """Transform file using streaming V2"""
    transformer = StreamingTransformerV2()
    return transformer.transform_file(input_file)


if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1:
        output = transform(sys.argv[1])
        print(f"Output: {output}")
    else:
        print("Usage: python transformer_v2_streaming.py <input_file>")
