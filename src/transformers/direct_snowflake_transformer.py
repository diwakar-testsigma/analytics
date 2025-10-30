"""
Direct Snowflake Transformer with Joins - Optimized Version
High-performance streaming transformation with direct output writing
Balances speed and memory efficiency for large datasets
"""
import json
import gzip
import os
import ijson
from datetime import datetime
from decimal import Decimal
from collections import OrderedDict
import gc
from src.transformers.transformation_mapping import (
    IDENTITY_MAPPINGS, MASTER_MAPPINGS, TENANT_MAPPINGS
)
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class BufferedTableWriter:
    """Manages writing table data with proper JSON structure"""
    def __init__(self, outfile):
        self.outfile = outfile
        self.tables_written = set()
        self.table_counts = {}
        self.first_table = True
        
    def start_table(self, table_name, record_count):
        """Start a new table section"""
        if table_name not in self.tables_written:
            if not self.first_table:
                self.outfile.write(',\n')
            self.first_table = False
            
            self.outfile.write(f'    "{table_name}": {{\n')
            self.outfile.write(f'      "records": {record_count},\n')
            self.outfile.write('      "data": [\n')
            self.tables_written.add(table_name)
            self.table_counts[table_name] = 0
            return True
        return False
    
    def write_record(self, table_name, record):
        """Write a record to the table"""
        if table_name in self.tables_written:
            if self.table_counts[table_name] > 0:
                self.outfile.write(',\n')
            self.outfile.write('        ')
            json.dump(record, self.outfile, default=str)
            self.table_counts[table_name] += 1
    
    def close_table(self, table_name):
        """Close a table section"""
        if table_name in self.tables_written:
            self.outfile.write('\n      ]\n    }')


def transform_to_snowflake(input_file: str, timestamp: str = None) -> str:
    """
    Optimized streaming transformation
    - Single pass through data
    - Direct writing to output
    - Minimal memory usage
    - Handles pre-joined data correctly
    """
    if not timestamp:
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    output_dir = 'output/transformations'
    os.makedirs(output_dir, exist_ok=True)
    output_file = os.path.join(output_dir, f'snowflake_data_{timestamp}.json.gz')
    
    # Get all mappings
    all_mappings = {**IDENTITY_MAPPINGS, **MASTER_MAPPINGS, **TENANT_MAPPINGS}
    
    logger.info(f"Starting optimized transformation of {input_file}")
    
    # Build source table -> target mappings
    source_to_targets = {}
    for target_table, config in all_mappings.items():
        primary_source = config.get("source_tables", [None])[0]
        if primary_source:
            source_to_targets.setdefault(primary_source, []).append((target_table, config))
    
    # We need to know record counts upfront for proper JSON structure
    # Quick pass to count records per target table
    logger.info("Quick pass: Counting target records...")
    target_counts = {}
    
    infile = gzip.open(input_file, 'rb') if input_file.endswith('.gz') else open(input_file, 'rb')
    with infile:
        parser = ijson.parse(infile)
        current_db = None
        current_table = None
        record_count = 0
        
        for prefix, event, value in parser:
            if 'extraction_metadata' in prefix:
                continue
            
            if event == 'map_key' and prefix == '':
                current_db = value
            elif event == 'map_key' and current_db and prefix == current_db:
                current_table = value
            elif event == 'end_map' and current_table and f"{current_db}.{current_table}.sample.item" == prefix:
                if current_table in source_to_targets:
                    for target_table, _ in source_to_targets[current_table]:
                        target_counts[target_table] = target_counts.get(target_table, 0) + 1
                record_count += 1
                if record_count % 100000 == 0:
                    logger.info(f"  Counted {record_count:,} records...")
    
    logger.info(f"Count complete: {sum(target_counts.values()):,} target records")
    
    # Sort tables by size (largest first) for better performance
    sorted_tables = sorted(target_counts.items(), key=lambda x: x[1], reverse=True)
    table_order = {table: idx for idx, (table, _) in enumerate(sorted_tables)}
    
    # Main transformation pass
    logger.info("Main pass: Transforming and writing data...")
    
    with gzip.open(output_file, 'wt', encoding='utf-8', compresslevel=1) as outfile:
        # Write header
        outfile.write('{\n  "tables": {\n')
        
        writer = BufferedTableWriter(outfile)
        total_records = 0
        
        # Process data
        infile = gzip.open(input_file, 'rb') if input_file.endswith('.gz') else open(input_file, 'rb')
        with infile:
            parser = ijson.parse(infile)
            
            # State tracking
            current_db = None
            current_table = None
            current_record = None
            in_sample = False
            
            # Collect records by table to write in order
            table_buffers = {table: [] for table in target_counts}
            
            for prefix, event, value in parser:
                # Skip metadata
                if 'extraction_metadata' in prefix:
                    continue
                    
                # Database
                if event == 'map_key' and prefix == '':
                    current_db = value
                    
                # Table  
                elif event == 'map_key' and current_db and prefix == current_db:
                    current_table = value
                    
                # Sample array
                elif event == 'start_array' and current_table and prefix == f"{current_db}.{current_table}.sample":
                    in_sample = True
                    
                # Record
                elif in_sample and event == 'start_map' and prefix == f"{current_db}.{current_table}.sample.item":
                    current_record = {}
                    
                # Field value
                elif current_record is not None and prefix.startswith(f"{current_db}.{current_table}.sample.item.") and event in ('string', 'number', 'boolean', 'null'):
                    # Extract field name - could be simple or contain table prefix
                    field_parts = prefix.split('.')[4:]  # Skip db.table.sample.item
                    field_name = '.'.join(field_parts)
                    
                    # Convert special values
                    if isinstance(value, Decimal):
                        value = str(value)
                    elif isinstance(value, str) and value.startswith("b'\\x"):
                        value = value == "b'\\x01'"
                        
                    current_record[field_name] = value
                        
                # End record
                elif event == 'end_map' and prefix == f"{current_db}.{current_table}.sample.item" and current_record:
                    # Check if this table maps to any targets
                    if current_table in source_to_targets:
                        for target_table, config in source_to_targets[current_table]:
                            # Transform record - handle pre-joined data
                            transformed = {}
                            for target_col, source_col in config.get("column_mappings", {}).items():
                                if '.' in source_col:
                                    src_table, src_col = source_col.split('.', 1)
                                    if src_table == current_table:
                                        # Direct field from current table
                                        transformed[target_col] = current_record.get(src_col)
                                    else:
                                        # Check if data is already joined (field exists with table prefix)
                                        joined_field = f"{src_table}.{src_col}"
                                        if joined_field in current_record:
                                            # Use pre-joined data
                                            value = current_record.get(joined_field)
                                            # Convert special values
                                            if isinstance(value, str) and value.startswith("b'\\x"):
                                                value = value == "b'\\x01'"
                                            transformed[target_col] = value
                                        else:
                                            # Field not found - set to None
                                            transformed[target_col] = None
                                else:
                                    transformed[target_col] = current_record.get(source_col)
                            
                            if transformed:
                                # Start table if needed
                                if writer.start_table(target_table, target_counts[target_table]):
                                    # Flush any buffered records for this table
                                    for buffered_record in table_buffers[target_table]:
                                        writer.write_record(target_table, buffered_record)
                                    table_buffers[target_table] = []
                                
                                # Write record directly if table started, else buffer
                                if target_table in writer.tables_written:
                                    writer.write_record(target_table, transformed)
                                else:
                                    table_buffers[target_table].append(transformed)
                                
                                total_records += 1
                                
                                # Log progress
                                if total_records % 10000 == 0:
                                    logger.info(f"  Written {total_records:,} records...")
                                    if total_records % 100000 == 0:
                                        gc.collect()  # Periodic cleanup
                    
                    current_record = None
                    
                # End sample
                elif event == 'end_array' and in_sample:
                    in_sample = False
        
        # Write any remaining buffered tables
        for target_table in sorted(table_buffers.keys(), key=lambda t: table_order.get(t, 999)):
            if table_buffers[target_table]:
                writer.start_table(target_table, target_counts[target_table])
                for record in table_buffers[target_table]:
                    writer.write_record(target_table, record)
        
        # Close all tables
        for table_name in writer.tables_written:
            writer.close_table(table_name)
        
        # Close JSON structure
        outfile.write('\n  }\n}\n')
    
    logger.info(f"Transformation complete: {output_file}")
    logger.info(f"  Total records: {total_records:,}")
    logger.info(f"  Tables: {len(writer.tables_written)}")
    logger.info(f"  File size: {os.path.getsize(output_file) / 1024 / 1024:.1f} MB")
    
    return output_file


if __name__ == "__main__":
    import sys
    input_file = sys.argv[1] if len(sys.argv) > 1 else 'output/extracted/extracted_data_20251030_025323.json'
    transform_to_snowflake(input_file)