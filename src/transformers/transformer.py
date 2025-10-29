"""
Data Transformer - Memory-efficient streaming
"""

import json
import gzip
import os
import gc
import ijson
from datetime import datetime
from typing import Dict, Any, Optional
import logging

from .transformation_mapping import ALL_MAPPINGS


class DataTransformer:
    """Streaming transformer for large files"""
    
    def __init__(self, config: Optional[Dict] = None):
        """Initialize transformer"""
        self.logger = logging.getLogger(__name__)
        
        if config:
            self.output_dir = config.get('output_dir', 'output/transformations')
        else:
            from ..config import settings
            self.output_dir = settings.TRANSFORMED_OUTPUT_DIR
        
        # Pre-compute source mappings
        self.source_mappings = {}
        for target, mapping in ALL_MAPPINGS.items():
            for source in mapping.get('source_tables', []):
                if source not in self.source_mappings:
                    self.source_mappings[source] = []
                self.source_mappings[source].append({
                    'target': target,
                    'columns': mapping.get('column_mappings', {})
                })
        
        os.makedirs(self.output_dir, exist_ok=True)
        
        # ETL timestamp for this run - all records will use the same timestamp
        self.etl_timestamp = None
    
    def transform_file(self, filepath: str) -> str:
        """Transform file with streaming"""
        self.logger.info(f"Starting transformation: {filepath}")
        
        # Set ETL timestamp for this run - all records will have the same timestamp
        self.etl_timestamp = datetime.now().isoformat()
        self.logger.info(f"ETL timestamp for this run: {self.etl_timestamp}")
        
        # Output file
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_path = os.path.join(self.output_dir, f"snowflake_data_{timestamp}.json.gz")
        
        # Process and collect records
        table_data = self._process_file(filepath)
        
        # Write output
        with gzip.open(output_path, 'wt', encoding='utf-8', compresslevel=1) as f:
            json.dump({
                "etl_timestamp": self.etl_timestamp,
                "tables": table_data
            }, f, indent=2)
        
        total_records = sum(len(records) for records in table_data.values())
        self.logger.info(f"Complete: {total_records:,} transformed records -> {output_path}")
        return output_path
    
    def _process_file(self, filepath: str) -> Dict[str, list]:
        """Process file and return transformed data"""
        table_buffers = {}
        record_count = 0
        
        # Open input
        if filepath.endswith('.gz'):
            infile = gzip.open(filepath, 'rb')
        else:
            infile = open(filepath, 'rb')
        
        with infile:
            parser = ijson.parse(infile)
            
            # State
            current_db = None
            current_table = None
            current_record = None
            current_field = None
            
            for prefix, event, value in parser:
                # Database
                if event == 'map_key' and prefix == '' and value != 'extraction_metadata':
                    current_db = value
                
                # Table  
                elif event == 'map_key' and prefix == current_db:
                    current_table = value
                
                # Record start
                elif event == 'start_map' and prefix == f"{current_db}.{current_table}.sample.item":
                    current_record = {}
                
                # Field name
                elif event == 'map_key' and current_record is not None and prefix == f"{current_db}.{current_table}.sample.item":
                    current_field = value
                
                # Field value
                elif current_record is not None and current_field and prefix == f"{current_db}.{current_table}.sample.item.{current_field}":
                    if event in ('string', 'number', 'boolean', 'null'):
                        current_record[current_field] = value
                        current_field = None
                
                # Record end
                elif event == 'end_map' and prefix == f"{current_db}.{current_table}.sample.item" and current_record is not None:
                    # Transform and buffer
                    if current_table in self.source_mappings:
                        for mapping in self.source_mappings[current_table]:
                            transformed = self._transform_record(
                                current_record, current_table, 
                                mapping['target'], mapping['columns']
                            )
                            if transformed:
                                target = mapping['target']
                                if target not in table_buffers:
                                    table_buffers[target] = []
                                table_buffers[target].append(transformed)
                    
                    record_count += 1
                    if record_count % 10000 == 0:
                        self.logger.info(f"Processed {record_count:,} records")
                        if record_count % 50000 == 0:
                            gc.collect()
                    
                    current_record = None
        
        return table_buffers
    
    def _transform_record(self, record: Dict, source_table: str, 
                         target_table: str, column_mappings: Dict) -> Optional[Dict]:
        """Transform a single record"""
        try:
            transformed = {}
            
            for target_col, source_field in column_mappings.items():
                value = None
                
                if '.' in source_field:
                    table_name, field_name = source_field.split('.', 1)
                    if table_name == source_table and field_name in record:
                        value = record[field_name]
                elif source_field in record:
                    value = record[source_field]
                
                if value is not None:
                    # Clean value
                    if isinstance(value, bytes):
                        value = value.decode('utf-8', errors='replace')
                    
                    # Special cases
                    if target_col == 'tenant_id' and value is None:
                        value = 0
                    elif target_col == 'auth_enabled' and target_table == 'dim_accounts':
                        value = bool(value) if value is not None else None
                    
                    transformed[target_col] = value
            
            # Add ETL_TIMESTAMP to every record
            # This will be used by Snowflake to track when the record was processed
            # All records in this run will have the same timestamp
            transformed['etl_timestamp'] = self.etl_timestamp
            
            return transformed if transformed else None
            
        except Exception as e:
            self.logger.error(f"Error transforming record: {e}")
            return None