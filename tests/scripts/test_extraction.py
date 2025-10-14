#!/usr/bin/env python3

import sys
import json
import os
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent.parent / "src"))

from extractors.extractor import DataExtractor
from config import settings

def test_extraction():
    """Test MySQL extraction using config from .env file"""
    print("Testing MySQL extraction...")
    print("Reading configuration from .env file")
    
    try:
        # Initialize extractor with config from .env
        config = {
            'mysql_host': settings.MYSQL_HOST,
            'mysql_port': settings.MYSQL_PORT,
            'mysql_user': settings.MYSQL_USER,
            'mysql_password': settings.MYSQL_PASSWORD,
            'extraction_workers': settings.PARALLEL_WORKERS,
            'extraction_batch_size': settings.BATCH_SIZE,
            'output_dir': settings.OUTPUT_DIR + '/extracted',
            'log_level': settings.LOG_LEVEL,
            'extract_ignore_db_keywords': ','.join(settings.EXTRACT_IGNORE_DB_KEYWORDS)
        }
        
        extractor = DataExtractor(config=config)
        
        # Test connection first
        print("\nTesting database connection...")
        conn = extractor.get_connection(extractor.config)
        cursor = conn.cursor()
        cursor.execute("SELECT VERSION()")
        version = cursor.fetchone()[0]
        print(f"Connected to MySQL {version}")
        cursor.close()
        conn.close()
        
        # List available databases
        print("\nListing available databases...")
        all_databases = extractor.list_databases()
        print(f"Found {len(all_databases)} databases: {', '.join(all_databases)}")
        
        # Extract from all databases
        print(f"\nExtracting from all {len(all_databases)} databases...")
        extracted_file = extractor.extract()
        
        # Show results
        print(f"\nExtraction completed!")
        if extracted_file:
            file_size = os.path.getsize(extracted_file) / 1024  # KB
            print(f"Single consolidated file created: {os.path.basename(extracted_file)} ({file_size:.1f} KB)")
            
            # Show structure summary
            with open(extracted_file, 'r') as f:
                data = json.load(f)
            
            print(f"\nExtraction structure:")
            total_tables = 0
            total_records = 0
            for db_name, db_data in data.items():
                if db_name == 'extraction_metadata':
                    # Skip metadata, it's not a database
                    continue
                print(f"  Database: {db_name} ({len(db_data)} tables)")
                for table_name, table_data in db_data.items():
                    if isinstance(table_data, dict):
                        record_count = table_data.get('records', 0)
                        print(f"    - {table_name}: {record_count} records")
                        total_tables += 1
                        total_records += record_count
            
            print(f"\nSummary: {total_tables} tables, {total_records} total records")
        else:
            print("No data extracted!")
        
        return extracted_file
        
    except Exception as e:
        print(f"Error during extraction: {e}")
        return None

if __name__ == "__main__":
    # Test extraction
    test_extraction()
