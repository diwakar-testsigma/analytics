#!/usr/bin/env python3
"""
Test script for Snowflake data loading
This script demonstrates how to load transformed data into Snowflake
"""

import os
import json
import sys
from pathlib import Path

# Add parent directory to path
sys.path.append(str(Path(__file__).parent.parent.parent))

from src.loaders.loader import DataLoader
from src.config import settings


def test_snowflake_load(transformation_file: str):
    """
    Test loading transformed data into Snowflake
    
    Args:
        transformation_file: Path to the transformation JSON file
    """
    print(f"🚀 Testing Snowflake data load")
    print(f"📁 Transformation file: {transformation_file}")
    print(f"🔧 Environment: {settings.ENVIRONMENT}")
    
    # For testing, we need to ensure we're in production mode
    if settings.ENVIRONMENT != 'production':
        print("⚠️  WARNING: Not in production mode. Snowflake loading requires ENVIRONMENT=production")
        print("   Set ENVIRONMENT=production in your .env file")
        return
    
    # Check if Snowflake credentials are set
    if not settings.SNOWFLAKE_CONNECTION_URL:
        print("❌ ERROR: SNOWFLAKE_CONNECTION_URL not set in environment")
        print("   Example: snowflake://user:password@account/database/schema?warehouse=WH&role=ROLE")
        return
    
    # Initialize the loader
    print("\n📦 Initializing DataLoader...")
    loader = DataLoader()
    
    # Load the data
    print(f"\n📤 Loading data to Snowflake...")
    try:
        success = loader.load(transformation_file)
        
        if success:
            print("\n✅ Data successfully loaded to Snowflake!")
            
            # Print summary of what was loaded
            with open(transformation_file, 'r') as f:
                data = json.load(f)
                tables = data.get('tables', {})
                
                print(f"\n📊 Summary:")
                print(f"   Total tables: {len(tables)}")
                total_records = sum(len(records) for records in tables.values())
                print(f"   Total records: {total_records}")
                
                print(f"\n📋 Tables loaded:")
                for table_name, records in tables.items():
                    print(f"   - {table_name}: {len(records)} records")
        else:
            print("\n❌ Failed to load data to Snowflake")
            
    except Exception as e:
        print(f"\n❌ Error during loading: {e}")
        import traceback
        traceback.print_exc()


def main():
    """Main function"""
    # Check command line arguments
    if len(sys.argv) < 2:
        print("Usage: python test_snowflake_load.py <transformation_file>")
        print("\nExample:")
        print("  python test_snowflake_load.py output/transformations/snowflake_data_20251013_095058.json")
        sys.exit(1)
    
    transformation_file = sys.argv[1]
    
    # Check if file exists
    if not os.path.exists(transformation_file):
        print(f"❌ ERROR: File not found: {transformation_file}")
        sys.exit(1)
    
    # Run the test
    test_snowflake_load(transformation_file)


if __name__ == "__main__":
    main()
