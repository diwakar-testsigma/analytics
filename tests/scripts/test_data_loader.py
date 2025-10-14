#!/usr/bin/env python3
"""
Data Loader Test

Test script for data loader functionality.
Uses environment-driven configuration to determine data source.
Requires filename as command line argument.
"""

import sys
import os
import argparse
from pathlib import Path

# Add src directory to path
sys.path.append(str(Path(__file__).parent.parent.parent / "src"))

from loaders.loader import DataLoader


def test_data_loader(filename: str):
    """
    Test data loader with a transformed JSON file
    
    Args:
        filename: Path to the transformed JSON file (required)
    """
    # Check if file exists
    if not os.path.exists(filename):
        print(f"❌ File not found: {filename}")
        return False
    
    print(f"🗄️  Testing data loader with: {filename}")
    
    # Initialize loader - data source determined by environment variables
    loader = DataLoader()
    
    # Show configuration
    data_store = loader.data_store
    print(f"📊 Using data store: {data_store}")
    
    if data_store == "sqlite":
        connection_url = loader.config['sqlite']['connection_url']
        print(f"📁 SQLite connection URL: {connection_url}")
    elif data_store == "snowflake":
        connection_url = loader.config['snowflake']['connection_url']
        print(f"☁️  Snowflake connection URL: {connection_url}")
    
    # Load data
    success = loader.load(filename)
    
    if success:
        print("✅ Data loading test completed successfully!")
        
        # Show some stats for SQLite
        if data_store == "sqlite":
            import sqlite3
            # Extract db_path from connection URL
            connection_url = loader.config['sqlite']['connection_url']
            if connection_url.startswith("sqlite:///"):
                db_path = connection_url[10:]
            else:
                db_path = connection_url
            
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            
            # Get table counts
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
            tables = cursor.fetchall()
            
            print(f"\n📊 Database contains {len(tables)} tables:")
            for table in tables:
                table_name = table[0]
                cursor.execute(f"SELECT COUNT(*) FROM '{table_name}'")
                count = cursor.fetchone()[0]
                if count > 0:
                    print(f"  - {table_name}: {count} records")
            
            conn.close()
            print(f"\n💾 Database location: {os.path.abspath(db_path)}")
        
    else:
        print("❌ Data loading test failed!")
    
    return success


def main():
    """Main function with argument parsing"""
    parser = argparse.ArgumentParser(
        description="Test data loader with environment-driven configuration",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Use SQLite (default)
  python3 test_sqlite_loader.py output/transformations/snowflake_data_20251009_100000.json
  
  # Use Snowflake (set DATA_STORE=snowflake in .env)
  DATA_STORE=snowflake python3 test_sqlite_loader.py output/transformations/snowflake_data_20251009_100000.json
  
Environment Variables (all required):
  DATA_STORE              - Data store type (sqlite, snowflake)
  LOG_LEVEL               - Logging level
  BATCH_SIZE              - Batch size for loading
  
  # SQLite (when DATA_STORE=sqlite)
  SQLITE_CONNECTION_URL   - SQLite connection URL (e.g., sqlite:///path/to/db.db)
  
  # Snowflake (when DATA_STORE=snowflake)
  SNOWFLAKE_CONNECTION_URL - Snowflake connection URL (e.g., snowflake://user:pass@account.region.snowflakecomputing.com/db/schema?warehouse=wh&role=role)
        """
    )
    
    parser.add_argument(
        'filename',
        help='Path to the transformed JSON file to load'
    )
    
    args = parser.parse_args()
    
    print("🧪 Data Loader Test")
    print("=" * 50)
    
    success = test_data_loader(args.filename)
    
    if success:
        print("\n🎉 Test completed successfully!")
        sys.exit(0)
    else:
        print("\n💥 Test failed!")
        sys.exit(1)


if __name__ == "__main__":
    main()