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


class DataLoader(BaseLoader):
    """Unified data loader for different data stores"""
    
    def __init__(self):
        """Initialize DataLoader with environment-based configuration"""
        self.settings = settings
        self.data_source = None
        
        # Determine data store from configuration
        self.data_store = self.settings.DATA_STORE
        
        super().__init__()
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
            
            # Load JSON data
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
    
    def get_connection(self):
        """Get connection to the data store"""
        data_source = self._get_data_source()
        data_source.connect()
        return data_source.get_connection()