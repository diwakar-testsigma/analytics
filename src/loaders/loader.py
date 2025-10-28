"""
Data Loader

This module provides a unified interface for loading transformed data into various data stores.
Automatically selects SQLite for local environment and Snowflake for production.
"""

import json
import logging
import os
from pathlib import Path
from typing import Dict, Any, List, Optional
from .base import BaseLoader
from .data_sources import SQLiteDataSource, SnowflakeDataSource
from src.config import settings
from src.utils.memory_monitor import MemoryMonitor


class DataLoader(BaseLoader):
    """Unified data loader for different data stores"""
    
    def __init__(self):
        """Initialize DataLoader with environment-based configuration"""
        self.settings = settings
        self.data_source = None
        
        # Determine data store from configuration
        self.data_store = self.settings.DATA_STORE
        
        super().__init__()
        
        # Initialize memory monitor
        self.memory_monitor = MemoryMonitor(
            max_memory_mb=settings.MAX_MEMORY_USAGE * 10 if settings.ENABLE_MEMORY_LIMIT else None,
            enable_limit=settings.ENABLE_MEMORY_LIMIT
        )
        
        self.logger.info(f"DataLoader initialized for {self.data_store} (Environment: {self.settings.ENVIRONMENT})")
    
    def _load_config(self) -> Dict:
        """Load configuration based on environment"""
        base_config = {
            'log_level': self.settings.LOG_LEVEL or 'INFO',
            'data_store': self.data_store,
            'batch_size': self.settings.BATCH_SIZE
        }
        
        if self.data_store == 'sqlite':
            # Use SQLite
            return {
                **base_config,
                'sqlite': {
                    'connection_url': self.settings.SQLITE_CONNECTION_URL,
                    'batch_size': self.settings.BATCH_SIZE
                }
            }
        else:
            # Use Snowflake
            return {
                **base_config,
                'snowflake': {
                    'connection_url': self.settings.SNOWFLAKE_CONNECTION_URL,
                    'batch_size': self.settings.BATCH_SIZE
                }
            }
    
    def _get_data_source(self):
        """Get the appropriate data source instance"""
        if self.data_source is None:
            config = self._load_config()
            
            if self.data_store == "sqlite":
                self.logger.debug(f"Creating SQLite data source: {config['sqlite']['connection_url']}")
                self.data_source = SQLiteDataSource(
                    connection_url=config['sqlite']['connection_url']
                )
            elif self.data_store == "snowflake":
                self.logger.debug("Creating Snowflake data source")
                self.data_source = SnowflakeDataSource(
                    connection_url=config['snowflake']['connection_url']
                )
            else:
                raise ValueError(f"Unsupported data store: {self.data_store}")
        
        return self.data_source
    
    def load(self, filepath: str) -> bool:
        """
        Load transformed data from JSON file into the configured data store
        
        Args:
            filepath: Path to the transformed JSON file
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            self.logger.info(f"Starting data load process")
            self.logger.info(f"Source file: {filepath}")
            self.logger.info(f"Target: {self.data_store.upper()}")
            
            # Check file size
            file_size_mb = os.path.getsize(filepath) / (1024 * 1024)
            self.logger.info(f"File size: {file_size_mb:.2f} MB")
            
            # For large files, use streaming approach
            if file_size_mb > 50:  # 50MB threshold for loading
                self.logger.info("Large file detected - using streaming loader")
                return self._load_streaming(filepath)
            
            # For smaller files, use original approach
            self.logger.debug("Loading JSON data from file...")
            data = self.load_json_file(filepath)
            
            # Get tables from the data
            tables = data.get('tables', {})
            
            if not tables:
                self.logger.error("No tables found in the JSON file")
                return False
            
            self.logger.info(f"Found {len(tables)} tables to load")
            
            # Get data source and connect
            data_source = self._get_data_source()
            self.logger.info(f"Connecting to {self.data_store}...")
            data_source.connect()
            self.logger.info("Connection established successfully")
            
            try:
                # Track loading statistics
                total_records = 0
                loaded_tables = 0
                failed_tables = []
                skipped_tables = []
                
                # Log loading strategy
                self.logger.info(f"Loading strategy: {self.settings.LOAD_STRATEGY}")
                
                # Load each table
                for table_name, records in tables.items():
                    if not records:
                        self.logger.warning(f"Table '{table_name}' has no records, skipping")
                        skipped_tables.append(table_name)
                        continue
                    
                    record_count = len(records)
                    self.logger.info(f"Loading table '{table_name}' ({record_count:,} records)...")
                    
                    try:
                        # Create table if it doesn't exist
                        self.logger.debug(f"Creating table '{table_name}' if not exists...")
                        data_source.create_table_if_not_exists(table_name, records[0])
                        
                        # Insert data in batches
                        batch_size = self.settings.BATCH_SIZE
                        self.logger.debug(f"Inserting data in batches of {batch_size}...")
                        
                        success = data_source.insert_batch(table_name, records)
                        if not success:
                            self.logger.error(f"Failed to load data into table '{table_name}'")
                            failed_tables.append({'table': table_name, 'error': 'Insert returned False'})
                            
                            if self.settings.LOAD_STRATEGY == 'fail_fast':
                                self.logger.error("Load strategy is 'fail_fast' - stopping ETL")
                                return False
                            else:
                                continue
                        
                        total_records += record_count
                        loaded_tables += 1
                        self.logger.info(f"✅ Successfully loaded {record_count:,} records into '{table_name}'")
                        
                    except Exception as e:
                        error_msg = str(e)
                        self.logger.error(f"Error loading table '{table_name}': {error_msg}")
                        failed_tables.append({'table': table_name, 'error': error_msg})
                        
                        if self.settings.LOAD_STRATEGY == 'fail_fast':
                            self.logger.error("Load strategy is 'fail_fast' - stopping ETL")
                            raise
                        else:
                            self.logger.warning(f"Load strategy is 'continue_on_error' - skipping table '{table_name}'")
                            continue
                
                # Final summary
                self.logger.info("="*50)
                self.logger.info("LOADING SUMMARY")
                self.logger.info("="*50)
                self.logger.info(f"Tables loaded successfully: {loaded_tables}")
                self.logger.info(f"Tables failed: {len(failed_tables)}")
                self.logger.info(f"Tables skipped (empty): {len(skipped_tables)}")
                self.logger.info(f"Total records loaded: {total_records:,}")
                
                # Report failed tables
                if failed_tables:
                    self.logger.warning("\nFailed Tables:")
                    for failure in failed_tables:
                        self.logger.warning(f"  ❌ {failure['table']}: {failure['error']}")
                
                # Report skipped tables
                if skipped_tables:
                    self.logger.info("\nSkipped Tables (no data):")
                    for table in skipped_tables:
                        self.logger.info(f"  ⏭️  {table}")
                
                self.logger.info("="*50)
                
                # Determine overall success
                if self.settings.LOAD_STRATEGY == 'continue_on_error':
                    # Success if at least some tables loaded
                    success = loaded_tables > 0
                    if success and failed_tables:
                        self.logger.warning("Load completed with errors - some tables failed")
                    elif success:
                        self.logger.info("Load completed successfully")
                    else:
                        self.logger.error("Load failed - no tables loaded successfully")
                else:
                    # fail_fast mode - we only get here if all succeeded
                    success = True
                    self.logger.info("Load completed successfully")
                
                # Return detailed results
                return {
                    'success': success,
                    'loaded_tables': loaded_tables,
                    'failed_tables': failed_tables,
                    'total_records': total_records,
                    'skipped_tables': skipped_tables
                }
                
            finally:
                # Always disconnect
                self.logger.debug("Closing database connection...")
                data_source.disconnect()
                self.logger.debug("Connection closed")
                
        except Exception as e:
            self.logger.error(f"Error loading data: {e}")
            self.logger.exception("Detailed error information:")
            return {
                'success': False,
                'loaded_tables': 0,
                'failed_tables': [],
                'total_records': 0,
                'skipped_tables': [],
                'error': str(e)
            }
    
    def _load_streaming(self, filepath: str) -> bool:
        """
        Load data using streaming approach for large files
        
        This method processes the JSON file table by table without loading
        the entire file into memory.
        
        Args:
            filepath: Path to the transformed JSON file
            
        Returns:
            bool: True if successful, False otherwise
        """
        import gc
        import json
        
        try:
            # Get data source and connect
            data_source = self._get_data_source()
            self.logger.info(f"Connecting to {self.data_store}...")
            data_source.connect()
            self.logger.info("Connection established successfully")
            
            # Track statistics
            total_records = 0
            loaded_tables = 0
            failed_tables = []
            skipped_tables = []
            
            # First, extract table names from the file
            self.logger.info("Analyzing file structure...")
            table_names = self._extract_table_names(filepath)
            self.logger.info(f"Found {len(table_names)} tables to load")
            
            # Process each table one by one
            for idx, table_name in enumerate(table_names):
                self.logger.info(f"[{idx+1}/{len(table_names)}] Loading table: {table_name}")
                
                try:
                    # Check memory before loading table
                    self.memory_monitor.check_memory(f"before loading {table_name}")
                    
                    # Extract just this table's data from the file
                    table_data = self._extract_single_table(filepath, table_name)
                    
                    if not table_data:
                        self.logger.warning(f"Table '{table_name}' has no records, skipping")
                        skipped_tables.append(table_name)
                        continue
                    
                    # Determine loading method
                    if self.settings.LOAD_STRATEGY == 'optimized' and \
                       self.data_store == 'snowflake' and \
                       len(table_data) > self.settings.SNOWFLAKE_COPY_THRESHOLD:
                        # Use COPY command for large datasets
                        success = data_source.load_data_bulk(table_name, table_data)
                    else:
                        # Use INSERT for smaller datasets
                        success = data_source.load_data(table_name, table_data)
                    
                    if success:
                        loaded_tables += 1
                        total_records += len(table_data)
                        self.logger.info(f"  Successfully loaded {len(table_data):,} records into '{table_name}'")
                    else:
                        failed_tables.append(table_name)
                        self.logger.error(f"  Failed to load table '{table_name}'")
                    
                    # Clear memory after each table
                    del table_data
                    gc.collect()
                    
                    # Log memory status
                    self.memory_monitor.log_memory_status(f"After loading {table_name}")
                    
                except Exception as e:
                    self.logger.error(f"Error loading table '{table_name}': {str(e)}")
                    failed_tables.append(table_name)
            
            # Disconnect
            self.logger.debug("Closing database connection...")
            data_source.disconnect()
            self.logger.debug("Connection closed")
            
            # Log final summary
            self.logger.info("=" * 60)
            self.logger.info("STREAMING LOADING SUMMARY:")
            self.logger.info(f"  Tables loaded: {loaded_tables}")
            self.logger.info(f"  Tables failed: {len(failed_tables)}")
            self.logger.info(f"  Tables skipped: {len(skipped_tables)}")
            self.logger.info(f"  Total records: {total_records:,}")
            
            if failed_tables:
                self.logger.error(f"  Failed tables: {failed_tables}")
            
            self.logger.info("=" * 60)
            
            return {
                'success': len(failed_tables) == 0,
                'loaded_tables': loaded_tables,
                'failed_tables': failed_tables,
                'total_records': total_records,
                'skipped_tables': skipped_tables
            }
            
        except Exception as e:
            self.logger.error(f"Error in streaming loader: {str(e)}")
            import traceback
            traceback.print_exc()
            return {
                'success': False,
                'loaded_tables': 0,
                'failed_tables': [],
                'total_records': 0,
                'skipped_tables': [],
                'error': str(e)
            }
    
    def _extract_table_names(self, filepath: str) -> List[str]:
        """
        Extract table names from the JSON file without loading the entire file
        """
        table_names = []
        
        with open(filepath, 'r') as f:
            # Use a simple state machine to find table names
            in_tables_section = False
            in_string = False
            current_key = ""
            depth = 0
            
            while True:
                char = f.read(1)
                if not char:
                    break
                
                if char == '"':
                    if not in_string:
                        in_string = True
                        current_key = ""
                    else:
                        in_string = False
                        # Check if we just read "tables"
                        if current_key == "tables" and not in_tables_section:
                            in_tables_section = True
                            depth = 0
                        # If we're in tables section at depth 1, this is a table name
                        elif in_tables_section and depth == 1:
                            table_names.append(current_key)
                elif in_string:
                    current_key += char
                elif char == '{':
                    depth += 1
                elif char == '}':
                    depth -= 1
                    if in_tables_section and depth == 0:
                        break  # We've finished the tables section
        
        return table_names
    
    def _extract_single_table(self, filepath: str, table_name: str) -> List[Dict]:
        """
        Extract a single table's data from the JSON file using ijson for streaming
        """
        try:
            # Try to use ijson for efficient streaming if available
            import ijson
            
            with open(filepath, 'rb') as f:
                parser = ijson.items(f, f'tables.{table_name}.item')
                return list(parser)
                
        except ImportError:
            # Fallback to manual parsing if ijson not available
            self.logger.warning("ijson not available, using fallback parser")
            return self._extract_single_table_fallback(filepath, table_name)
    
    def _extract_single_table_fallback(self, filepath: str, table_name: str) -> List[Dict]:
        """
        Fallback method to extract table data without ijson
        """
        import json
        
        # This is less efficient but still better than loading the entire file
        with open(filepath, 'r') as f:
            # Read file in chunks and look for our table
            buffer = ""
            found_table = False
            bracket_count = 0
            capture_start = -1
            
            while True:
                chunk = f.read(8192)  # 8KB chunks
                if not chunk:
                    break
                
                buffer += chunk
                
                # Look for table name pattern
                if not found_table:
                    pattern = f'"{table_name}":'
                    idx = buffer.find(pattern)
                    if idx != -1:
                        found_table = True
                        # Find the opening bracket
                        start_idx = buffer.find('[', idx)
                        if start_idx != -1:
                            capture_start = start_idx
                            bracket_count = 1
                            buffer = buffer[start_idx:]
                
                # Count brackets to find the end
                if found_table and bracket_count > 0:
                    for i, char in enumerate(buffer):
                        if char == '[':
                            bracket_count += 1
                        elif char == ']':
                            bracket_count -= 1
                            if bracket_count == 0:
                                # Found the complete array
                                array_json = buffer[:i+1]
                                return json.loads(array_json)
                
                # Keep only last part of buffer
                if len(buffer) > 100000:
                    buffer = buffer[-10000:]
        
        return []
    
    def get_connection(self):
        """Get connection to the data store"""
        data_source = self._get_data_source()
        data_source.connect()
        return data_source.get_connection()