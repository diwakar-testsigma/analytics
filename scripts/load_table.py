#!/usr/bin/env python3
"""
Simple script to load a specific table from transformed JSON

Usage:
    python scripts/load_table.py <transformed_file> <table_name>
    python scripts/load_table.py output/transformations/snowflake_data_20251029_123456.json.gz dim_test_cases
"""

import sys
import json
import gzip
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.loaders.data_sources import SnowflakeDataSource
from src.config import settings

def load_table(file_path, table_name):
    # Read file
    if file_path.endswith('.gz'):
        with gzip.open(file_path, 'rt') as f:
            data = json.load(f)
    else:
        with open(file_path, 'r') as f:
            data = json.load(f)
    
    # Get table data
    tables = data.get('tables', {})
    if table_name not in tables:
        print(f"Table {table_name} not found. Available: {list(tables.keys())}")
        return
    
    records = tables[table_name]
    print(f"Found {len(records)} records for {table_name}")
    
    if not records:
        print("No records to load")
        return
    
    # Connect and load
    snowflake = SnowflakeDataSource(settings.SNOWFLAKE_CONNECTION_URL)
    try:
        snowflake.connect()
        snowflake.create_table_if_not_exists(table_name, records[0])
        
        # Force use of COPY command by calling the internal method directly
        # This is faster than INSERT for any amount of data
        print(f"Using COPY command to load {len(records)} records...")
        success = snowflake._insert_batch_with_copy(table_name, records)
        
        if success:
            print(f"✅ Successfully loaded {len(records)} records using COPY")
        else:
            print("❌ COPY failed, falling back to INSERT...")
            snowflake.insert_batch(table_name, records)
            print(f"✅ Loaded {len(records)} records using INSERT")
            
    except Exception as e:
        print(f"❌ Error: {e}")
        # Print sample record for debugging
        print(f"Sample record: {json.dumps(records[0], indent=2)}")
    finally:
        snowflake.disconnect()

if __name__ == '__main__':
    if len(sys.argv) != 3:
        print("Usage: python scripts/load_table.py <file> <table>")
        sys.exit(1)
    
    load_table(sys.argv[1], sys.argv[2])
