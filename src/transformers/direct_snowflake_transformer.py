"""
Direct Snowflake Transformer with Joins
Streams extracted data directly to Snowflake format with necessary joins
"""
import json
import gzip
import os
import ijson
from datetime import datetime
from decimal import Decimal
from src.transformers.transformation_mapping import (
    IDENTITY_MAPPINGS, MASTER_MAPPINGS, TENANT_MAPPINGS
)
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def transform_to_snowflake(input_file: str, timestamp: str = None) -> str:
    """Direct streaming transformation to Snowflake format with joins"""
    if not timestamp:
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    output_dir = 'output/transformations'
    os.makedirs(output_dir, exist_ok=True)
    output_file = os.path.join(output_dir, f'snowflake_data_{timestamp}.json.gz')
    
    # Get all mappings
    all_mappings = {**IDENTITY_MAPPINGS, **MASTER_MAPPINGS, **TENANT_MAPPINGS}
    
    logger.info(f"Processing with pre-joined data...")
    
    # Build source table -> target mappings
    source_to_targets = {}
    for target_table, config in all_mappings.items():
        primary_source = config.get("source_tables", [None])[0]
        if primary_source:
            source_to_targets.setdefault(primary_source, []).append((target_table, config))
    
    logger.info(f"Processing {input_file}")
    
    # Open files
    infile = gzip.open(input_file, 'rb') if input_file.endswith('.gz') else open(input_file, 'rb')
    outfile = gzip.open(output_file, 'wt', encoding='utf-8', compresslevel=1)
    
    with infile, outfile:
        parser = ijson.parse(infile)
        
        # State tracking
        current_db = None
        current_table = None
        current_record = None
        in_sample = False
        
        # Output tracking
        table_buffers = {}  # target_table -> list of records
        tables_processed = set()
        
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
            elif event == 'start_array' and prefix == f"{current_db}.{current_table}.sample":
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
                            if target_table not in table_buffers:
                                table_buffers[target_table] = []
                            table_buffers[target_table].append(transformed)
                            tables_processed.add(target_table)
                
                current_record = None
                
            # End sample
            elif event == 'end_array' and in_sample:
                in_sample = False
        
        # Write output
        logger.info(f"Writing {len(table_buffers)} Snowflake tables")
        outfile.write('{\n')
        outfile.write('  "tables": {\n')
        
        first = True
        for table, records in table_buffers.items():
            if not first:
                outfile.write(',\n')
            first = False
            
            outfile.write(f'    "{table}": {{\n')
            outfile.write(f'      "records": {len(records)},\n')
            outfile.write('      "data": [\n')
            
            for i, record in enumerate(records):
                if i > 0:
                    outfile.write(',\n')
                outfile.write('        ')
                json.dump(record, outfile, default=str)
            
            outfile.write('\n      ]\n    }')
        
        outfile.write('\n  }\n}\n')
    
    logger.info(f"Created: {output_file}")
    return output_file

if __name__ == "__main__":
    import sys
    input_file = sys.argv[1] if len(sys.argv) > 1 else 'output/extracted/extracted_data_20251030_025323.json'
    transform_to_snowflake(input_file)
