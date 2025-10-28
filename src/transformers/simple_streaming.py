"""
Simple Streaming - Read one record, write one record, repeat
Nothing more, nothing less.
"""

import json
import gzip
import os
import ijson
from datetime import datetime
import logging

from .transformation_mapping import ALL_MAPPINGS


class SimpleStreaming:
    """Simple one-record-at-a-time transformer"""
    
    def __init__(self, output_dir: str):
        self.output_dir = output_dir
        self.logger = logging.getLogger(__name__)
        os.makedirs(output_dir, exist_ok=True)
        
        # Pre-build lookups
        self.mappings = self._build_mappings()
    
    def transform(self, input_file: str) -> str:
        """Transform with streaming - truly one at a time"""
        output = os.path.join(
            self.output_dir,
            f"snowflake_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json.gz"
        )
        
        self.logger.info(f"Transforming {input_file} -> {output}")
        
        f = gzip.open(input_file, 'rb') if input_file.endswith('.gz') else open(input_file, 'rb')
        
        with f:
            with gzip.open(output, 'wt') as out:
                # Write header
                out.write('{\n  "etl_timestamp": "')
                out.write(datetime.now().isoformat())
                out.write('",\n  "tables": {\n')
                
                written = {}  # Track what we've written
                count = 0
                
                # Get all databases
                for db_name in ijson.items(f, ''):
                    # Get all tables in database
                    for table_name, table_data in ijson.items(f, f'{db_name}.'):
                        # Skip metadata
                        if db_name == 'extraction_metadata':
                            continue
                        
                        # Get sample array
                        for record in ijson.items(f, f'{db_name}.{table_name}.sample.item'):
                            count += 1
                            
                            # Transform if we have mappings
                            if table_name in self.mappings:
                                for target, cols in self.mappings[table_name].items():
                                    trans = self._do_transform(record, cols)
                                    if trans:
                                        self._write(out, target, trans, written)
                            
                            if count % 10000 == 0:
                                self.logger.info(f"{count:,} records...")
                
                # Close all tables
                for i, (table, _) in enumerate(written.items()):
                    out.write('\n    ]')
                    if i < len(written) - 1:
                        out.write(',')
                
                out.write('\n  }\n}\n')
        
        self.logger.info(f"Done: {count:,} records")
        return output
    
    def _build_mappings(self) -> dict:
        """Build simple lookup"""
        result = {}
        for target, cfg in ALL_MAPPINGS.items():
            for source in cfg.get('source_tables', []):
                if source not in result:
                    result[source] = {}
                result[source][target] = cfg.get('column_mappings', {})
        return result
    
    def _do_transform(self, record: dict, cols: dict) -> dict:
        """Transform one record"""
        result = {}
        for tcol, sfield in cols.items():
            val = record.get(sfield.rsplit('.', 1)[-1] if '.' in sfield else sfield)
            if val:
                result[tcol] = val
        return result if result else None
    
    def _write(self, out, table: str, record: dict, written: dict):
        """Write immediately"""
        if table not in written:
            out.write(',\n' if written else '')
            out.write(f'    "{table}": [\n      ')
            written[table] = True
        else:
            out.write(',\n      ')
        
        json.dump(record, out, separators=(',', ':'))
