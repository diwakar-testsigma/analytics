"""
Minimal Streaming Transformer - Truly processes one record at a time
NO accumulation, NO full file loading, NO buffers
"""

import json
import gzip
import os
import ijson
from datetime import datetime
from typing import Dict, Any, IO
import logging

from .transformation_mapping import ALL_MAPPINGS


class MinimalStreamingTransformer:
    """Minimal transformer - processes one record, writes it, moves on"""
    
    def __init__(self, output_dir: str):
        self.output_dir = output_dir
        self.logger = logging.getLogger(__name__)
        
        # Build simple lookup
        self.lookup = {}
        for target, config in ALL_MAPPINGS.items():
            for source in config.get('source_tables', []):
                if source not in self.lookup:
                    self.lookup[source] = []
                self.lookup[source].append({
                    'target': target,
                    'columns': config.get('column_mappings', {})
                })
        
        os.makedirs(output_dir, exist_ok=True)
    
    def transform(self, input_file: str) -> str:
        """Transform with absolutely minimal memory"""
        output_file = os.path.join(
            self.output_dir, 
            f"snowflake_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json.gz"
        )
        
        self.logger.info(f"Transforming {input_file}")
        
        with self._open_in(input_file) as infile, \
             gzip.open(output_file, 'wt', encoding='utf-8') as outfile:
            
            # Write header
            outfile.write('{\n  "etl_timestamp": "')
            outfile.write(datetime.now().isoformat())
            outfile.write('",\n  "tables": {\n')
            
            # Process with ijson streaming
            record_count = 0
            tables = {}  # Track which tables we've started
            
            # Use ijson's basic_items for simple streaming
            parser = ijson.items(infile, 'database.item')
            
            for db_item in parser:
                db_name = db_item
                parser.next()
                
                for table_data in parser:
                    table_name = table_data
                    
                    # Get records
                    for record in ijson.items(infile, f'{db_name}.{table_name}.sample.item'):
                        record_count += 1
                        
                        # Transform if we have mappings
                        if table_name in self.lookup:
                            for mapping in self.lookup[table_name]:
                                transformed = self._transform(record, mapping)
                                if transformed:
                                    self._write_record(outfile, mapping['target'], 
                                                      transformed, tables)
                        
                        if record_count % 10000 == 0:
                            self.logger.info(f"Processed {record_count:,} records")
            
            # Close all tables
            for i, table in enumerate(tables):
                outfile.write('\n    ]')
                if i < len(tables) - 1:
                    outfile.write(',')
            
            outfile.write('\n  }\n}\n')
        
        self.logger.info(f"Complete: {record_count:,} records -> {output_file}")
        return output_file
    
    def _open_in(self, filepath: str) -> IO:
        if filepath.endswith('.gz'):
            return gzip.open(filepath, 'rb')
        return open(filepath, 'rb')
    
    def _transform(self, record: Dict, mapping: Dict) -> Dict:
        """Transform one record"""
        result = {}
        for target_col, source_field in mapping['columns'].items():
            if '.' in source_field:
                _, col = source_field.rsplit('.', 1)
                value = record.get(col)
            else:
                value = record.get(source_field)
            
            if value is not None:
                if isinstance(value, bytes):
                    value = value.decode('utf-8', errors='replace')
                result[target_col] = value
        
        return result if result else None
    
    def _write_record(self, outfile: IO, table: str, record: Dict, tables: dict):
        """Write one record immediately"""
        if table not in tables:
            if tables:
                outfile.write(',\n')
            outfile.write(f'    "{table}": [\n      ')
            tables[table] = True
        else:
            outfile.write(',\n      ')
        
        json.dump(record, outfile, separators=(',', ':'))
