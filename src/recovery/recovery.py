#!/usr/bin/env python3
"""
ETL Recovery Tool

This tool helps recover from partial ETL failures by:
1. Identifying which tables were successfully loaded
2. Retrying only the failed tables
3. Providing options to skip problematic tables
"""

import json
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Optional, Set
import logging

from ..config import settings
from ..loaders.loader import DataLoader
from ..loaders.data_sources import SnowflakeDataSource, SQLiteDataSource


class ETLRecovery:
    """Handles recovery from partial ETL failures"""
    
    def __init__(self):
        self.logger = self._setup_logging()
        self.settings = settings
    
    def _setup_logging(self) -> logging.Logger:
        """Setup logging for recovery process"""
        logger = logging.getLogger(__name__)
        logger.setLevel(logging.INFO)
        
        # Console handler
        console = logging.StreamHandler()
        console.setFormatter(logging.Formatter(
            '%(asctime)s - %(levelname)s - %(message)s'
        ))
        logger.addHandler(console)
        
        return logger
    
    def get_loaded_tables(self) -> Set[str]:
        """Get list of tables already loaded in the target database"""
        self.logger.info(f"Checking loaded tables in {self.settings.DATA_STORE}...")
        
        loaded_tables = set()
        
        if self.settings.DATA_STORE == 'snowflake':
            data_source = SnowflakeDataSource(self.settings.SNOWFLAKE_CONNECTION_URL)
            try:
                data_source.connect()
                cursor = data_source.cursor
                
                # Get all tables in the current schema
                cursor.execute("SHOW TABLES")
                tables = cursor.fetchall()
                
                for table in tables:
                    table_name = table[1]  # Table name is in second column
                    loaded_tables.add(table_name.lower())
                    
                    # Get row count
                    cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
                    count = cursor.fetchone()[0]
                    self.logger.info(f"  ✓ {table_name}: {count:,} records")
                
            finally:
                data_source.disconnect()
        
        elif self.settings.DATA_STORE == 'sqlite':
            data_source = SQLiteDataSource(self.settings.SQLITE_CONNECTION_URL)
            try:
                data_source.connect()
                cursor = data_source.cursor
                
                # Get all tables
                cursor.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                )
                tables = cursor.fetchall()
                
                for (table_name,) in tables:
                    loaded_tables.add(table_name.lower())
                    
                    # Get row count
                    cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
                    count = cursor.fetchone()[0]
                    self.logger.info(f"  ✓ {table_name}: {count:,} records")
                    
            finally:
                data_source.disconnect()
        
        return loaded_tables
    
    def find_latest_transformation_file(self) -> Optional[str]:
        """Find the most recent transformation file"""
        transformation_dir = Path(self.settings.TRANSFORMED_OUTPUT_DIR)
        
        if not transformation_dir.exists():
            return None
        
        # Find all transformation files
        files = list(transformation_dir.glob("snowflake_data_*.json"))
        
        if not files:
            return None
        
        # Sort by modification time and return the latest
        latest_file = max(files, key=lambda f: f.stat().st_mtime)
        return str(latest_file)
    
    def recover_from_failure(
        self, 
        transformation_file: Optional[str] = None,
        skip_tables: Optional[List[str]] = None,
        retry_failed: bool = True
    ) -> bool:
        """
        Recover from partial ETL failure
        
        Args:
            transformation_file: Path to transformation file to use
            skip_tables: List of tables to skip (e.g., problematic ones)
            retry_failed: Whether to retry previously failed tables
        
        Returns:
            True if recovery successful, False otherwise
        """
        # Find transformation file if not provided
        if not transformation_file:
            transformation_file = self.find_latest_transformation_file()
            if not transformation_file:
                self.logger.error("No transformation file found!")
                return False
        
        self.logger.info(f"Using transformation file: {transformation_file}")
        
        # Load transformation data
        with open(transformation_file, 'r') as f:
            data = json.load(f)
        
        # Handle new format with etl_timestamp and tables
        if 'tables' in data:
            tables_data = data['tables']
        else:
            tables_data = data
        
        # Get already loaded tables
        loaded_tables = self.get_loaded_tables()
        self.logger.info(f"\nFound {len(loaded_tables)} tables already loaded")
        
        # Determine which tables to load
        skip_tables = set(skip_tables or [])
        skip_tables = {t.lower() for t in skip_tables}  # Normalize to lowercase
        
        tables_to_load = []
        for table_name in tables_data.keys():
            table_name_lower = table_name.lower()
            
            if table_name_lower in loaded_tables:
                self.logger.info(f"  → Skipping {table_name} (already loaded)")
            elif table_name_lower in skip_tables:
                self.logger.warning(f"  → Skipping {table_name} (in skip list)")
            else:
                tables_to_load.append(table_name)
                self.logger.info(f"  → Will load {table_name}")
        
        if not tables_to_load:
            self.logger.info("\nNo tables to load - recovery complete!")
            return True
        
        # Create filtered data with only tables to load
        filtered_data = {
            table: tables_data[table] 
            for table in tables_to_load
        }
        
        # Save filtered data to temporary file
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        recovery_file = Path(self.settings.TRANSFORMED_OUTPUT_DIR) / f"recovery_data_{timestamp}.json"
        
        with open(recovery_file, 'w') as f:
            json.dump(filtered_data, f, indent=2)
        
        self.logger.info(f"\nCreated recovery file: {recovery_file}")
        self.logger.info(f"Loading {len(tables_to_load)} tables...")
        
        # Use DataLoader to load the filtered data
        try:
            loader = DataLoader()
            success = loader.load(str(recovery_file))
            
            if success:
                self.logger.info("\n✅ Recovery completed successfully!")
            else:
                self.logger.error("\n❌ Recovery failed!")
            
            return success
            
        except Exception as e:
            self.logger.error(f"\n❌ Recovery failed with error: {e}")
            return False
        
        finally:
            # Optionally clean up recovery file
            if recovery_file.exists():
                recovery_file.unlink()
                self.logger.info(f"Cleaned up recovery file")
    
    def validate_data_before_load(self, transformation_file: str) -> Dict[str, List[str]]:
        """
        Validate transformation data before loading
        
        Returns:
            Dict of table_name -> list of issues
        """
        self.logger.info("Validating transformation data...")
        
        with open(transformation_file, 'r') as f:
            data = json.load(f)
        
        # Handle new format with etl_timestamp and tables
        if 'tables' in data:
            tables_data = data['tables']
        else:
            tables_data = data
        
        issues = {}
        
        for table_name, records in tables_data.items():
            table_issues = []
            
            if not records:
                table_issues.append("No records to load")
                continue
            
            # Check first record for structure
            sample_record = records[0]
            
            # Check for NULL values in likely required fields
            for field, value in sample_record.items():
                if field.endswith('_id') and value is None:
                    table_issues.append(f"NULL value in {field}")
            
            # Check specific known issues
            if table_name == 'dim_features' and 'feature_id' not in sample_record:
                table_issues.append("Missing required field: feature_id")
            
            if table_issues:
                issues[table_name] = table_issues
                self.logger.warning(f"  ⚠️  {table_name}: {', '.join(table_issues)}")
            else:
                self.logger.info(f"  ✓ {table_name}: OK")
        
        return issues


def main():
    """Main entry point for recovery tool"""
    recovery = ETLRecovery()
    
    # Command line usage
    if len(sys.argv) > 1:
        if sys.argv[1] == 'check':
            # Just check what's loaded
            recovery.get_loaded_tables()
        
        elif sys.argv[1] == 'validate':
            # Validate transformation file
            if len(sys.argv) > 2:
                issues = recovery.validate_data_before_load(sys.argv[2])
                if issues:
                    print(f"\n⚠️  Found issues in {len(issues)} tables")
            else:
                print("Usage: etl_recovery.py validate <transformation_file>")
        
        elif sys.argv[1] == 'recover':
            # Recover from failure
            skip_tables = []
            transformation_file = None
            
            # Parse arguments
            i = 2
            while i < len(sys.argv):
                if sys.argv[i] == '--skip':
                    i += 1
                    if i < len(sys.argv):
                        skip_tables.extend(sys.argv[i].split(','))
                elif sys.argv[i] == '--file':
                    i += 1
                    if i < len(sys.argv):
                        transformation_file = sys.argv[i]
                i += 1
            
            recovery.recover_from_failure(
                transformation_file=transformation_file,
                skip_tables=skip_tables
            )
    
    else:
        print("""
ETL Recovery Tool
================

Usage:
  python etl_recovery.py check                    # Check what tables are loaded
  python etl_recovery.py validate <file>          # Validate transformation file
  python etl_recovery.py recover                  # Recover using latest file
  python etl_recovery.py recover --skip dim_features,dim_test_data
  python etl_recovery.py recover --file output/transformations/specific_file.json
        """)


if __name__ == "__main__":
    main()
