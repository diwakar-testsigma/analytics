"""
MySQL Data Extractor

Extracts data from MySQL databases with support for:
- Multiple databases extraction
- Parallel extraction
- Batch processing
- Consolidated JSON output
"""

import json
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional, Tuple
import mysql.connector
import atexit
import gc

from .base import BaseExtractor
from ..extraction_checkpoint import checkpoint


class DataExtractor(BaseExtractor):
    """Extracts data from source databases into consolidated JSON format"""
    
    def __init__(self, config: Optional[Dict] = None):
        """
        Initialize data extractor
        
        Args:
            config: Optional configuration dictionary. If not provided, uses environment variables
        """
        self.custom_config = config or {}
        super().__init__()
        
        # Register cleanup function to prevent fatal errors
        atexit.register(self._cleanup)
    
    def _cleanup(self):
        """Cleanup function to prevent MySQL connector fatal errors"""
        try:
            # Force garbage collection to clean up MySQL connections
            gc.collect()
        except Exception:
            pass  # Ignore cleanup errors
        
    def _load_config(self) -> Dict:
        """Load configuration from custom config or environment variables"""
        # Always load environment settings first
        from ..config import settings
        
        # Check if we have a full custom config (backward compatibility)
        if self.custom_config and 'output_dir' in self.custom_config:
            # For backward compatibility, build connection URL if individual params are provided
            if 'mysql_connection_url' in self.custom_config:
                mysql_url = self.custom_config['mysql_connection_url']
            else:
                # Build URL from individual parameters
                host = self.custom_config.get('mysql_host', 'localhost')
                port = self.custom_config.get('mysql_port', 3306)
                user = self.custom_config.get('mysql_user', 'root')
                password = self.custom_config.get('mysql_password', '')
                mysql_url = f"mysql://{user}:{password}@{host}:{port}"
            
            return {
                'mysql_connection_url': mysql_url,
                'extraction': {
                    'workers': self.custom_config.get('extraction_workers', 10),
                    'batch_size': self.custom_config.get('extraction_batch_size', 5000)
                },
                'output_dir': self.custom_config.get('output_dir'),
                'log_level': self.custom_config.get('log_level'),
                'extract_ignore_db_keywords': self.custom_config.get('extract_ignore_db_keywords', '')
            }
        else:
            # Load from environment variables
            # Priority: 1. Override from custom config, 2. Checkpoint, 3. Environment variable
            start_date = None
            
            # Check if we have an override from custom config
            if self.custom_config and 'extraction_start_date_override' in self.custom_config:
                start_date = self.custom_config['extraction_start_date_override']
                # Logger not available yet, will log after initialization
            else:
                # Get start date from checkpoint if not explicitly set
                start_date = settings.EXTRACT_START_DATE
                if settings.AUTO_UPDATE_START_DATE and not start_date:
                    start_date = checkpoint.get_last_extraction_date()
                    # Logger not available yet, will log after initialization
            
            return {
                'mysql_connection_url': settings.MYSQL_CONNECTION_URL,
                'extraction': {
                    'workers': settings.EXTRACTION_WORKERS,
                    'batch_size': settings.EXTRACTION_BATCH_SIZE
                },
                'output_dir': settings.EXTRACTED_OUTPUT_DIR,
                'extract_tables': settings.EXTRACT_TABLES,
                'log_level': settings.LOG_LEVEL,
                'extract_db_keywords': settings.EXTRACT_DB_KEYWORDS,
                'date_filtering': {
                    'start_date': start_date,
                    'end_date': settings.EXTRACT_END_DATE,
                    'days_count': settings.EXTRACT_DAYS_COUNT,
                    'date_column': settings.EXTRACT_DATE_COLUMN
                }
            }
    
    @staticmethod
    def get_connection(config: Dict, database: Optional[str] = None):
        """
        Create MySQL connection
        
        Args:
            config: Configuration dictionary
            database: Optional database name to connect to
            
        Returns:
            MySQL connection object
        """
        from urllib.parse import urlparse
        
        # Get connection URL
        if 'mysql_connection_url' in config:
            connection_url = config['mysql_connection_url']
        else:
            # Fallback for old config format
            connection_url = config.get('mysql', {}).get('connection_url')
        
        if not connection_url:
            raise ValueError("MySQL connection URL not found in configuration")
        
        # Parse the URL
        parsed = urlparse(connection_url)
        
        connection_params = {
            'host': parsed.hostname,
            'port': parsed.port if parsed.port else 3306,
            'user': parsed.username,
            'password': parsed.password
        }
        
        # Add database if specified in URL or parameter
        if database:
            connection_params['database'] = database
        elif parsed.path and len(parsed.path) > 1:
            connection_params['database'] = parsed.path[1:]
            
        return mysql.connector.connect(**connection_params)
    
    def list_databases(self) -> List[str]:
        """Get list of databases, excluding system and ignored databases"""
        conn = self.get_connection(self.config)
        cursor = conn.cursor()
        
        cursor.execute("SHOW DATABASES")
        all_databases = [db[0] for db in cursor.fetchall()]
        
        cursor.close()
        conn.close()
        
        # Filter out system databases
        system_dbs = ['information_schema', 'mysql', 'performance_schema', 'sys']
        databases = [db for db in all_databases if db not in system_dbs]
        
        # Filter databases by include keywords (if provided)
        include_keywords = self.config['extract_db_keywords'].split(',')
        include_keywords = [kw.strip().lower() for kw in include_keywords if kw.strip()]
        
        if include_keywords:
            # Only include databases that match one of the keywords
            databases = [
                db for db in databases 
                if any(keyword in db.lower() for keyword in include_keywords)
            ]
        
        return databases
    
    def _get_date_filter_params(self) -> Tuple[Optional[str], Optional[str]]:
        """
        Get start and end dates for filtering based on configuration
        
        Returns:
            Tuple of (start_date, end_date) in YYYY-MM-DD format
        """
        date_config = self.config.get('date_filtering', {})
        start_date = date_config.get('start_date')
        end_date = date_config.get('end_date')
        days_count = date_config.get('days_count')
        
        # If days_count is specified, calculate end_date from start_date
        if start_date and days_count:
            try:
                start_dt = datetime.strptime(start_date, '%Y-%m-%d')
                # Convert days_count to int if it's a string
                days_count_int = int(days_count) if isinstance(days_count, str) else days_count
                end_dt = start_dt + timedelta(days=days_count_int)
                end_date = end_dt.strftime('%Y-%m-%d')
                self.logger.info(f"Calculated end_date: {end_date} (start_date: {start_date} + {days_count_int} days)")
            except ValueError as e:
                self.logger.error(f"Invalid start_date format: {start_date}. Expected YYYY-MM-DD")
                return None, None
        
        return start_date, end_date
    
    def _has_date_column(self, database: str, table_name: str) -> bool:
        """
        Check if table has the configured date column
        
        Args:
            database: Database name
            table_name: Table name
            
        Returns:
            True if table has the date column, False otherwise
        """
        try:
            conn = self.get_connection(self.config, database)
            cursor = conn.cursor()
            
            date_column = self.config.get('date_filtering', {}).get('date_column', 'updated_at')
            cursor.execute(f"SHOW COLUMNS FROM {table_name} LIKE %s", (date_column,))
            result = cursor.fetchone()
            
            cursor.close()
            conn.close()
            
            return result is not None
            
        except Exception as e:
            self.logger.warning(f"Could not check date column for {database}.{table_name}: {e}")
            return False
    
    def _build_date_filter_query(self, table_name: str, base_query: str) -> Tuple[str, List]:
        """
        Build query with date filtering
        
        Args:
            table_name: Table name
            base_query: Base SELECT query
            
        Returns:
            Tuple of (modified_query, parameters)
        """
        start_date, end_date = self._get_date_filter_params()
        
        if not start_date and not end_date:
            return base_query, []
        
        date_column = self.config.get('date_filtering', {}).get('date_column', 'updated_at')
        params = []
        
        # Add WHERE clause for date filtering
        where_conditions = []
        
        if start_date:
            where_conditions.append(f"{date_column} >= %s")
            params.append(start_date)
        
        if end_date:
            where_conditions.append(f"{date_column} <= %s")
            params.append(end_date)
        
        if where_conditions:
            where_clause = " AND ".join(where_conditions)
            if "WHERE" in base_query.upper():
                # Add to existing WHERE clause
                query = base_query.replace("WHERE", f"WHERE {where_clause} AND")
            else:
                # Add new WHERE clause
                query = f"{base_query} WHERE {where_clause}"
        else:
            query = base_query
        
        return query, params
    
    def list_tables_in_database(self, database: str) -> List[str]:
        """Get list of tables in a database"""
        conn = self.get_connection(self.config, database)
        cursor = conn.cursor()
        
        cursor.execute("SHOW TABLES")
        tables = [table[0] for table in cursor.fetchall()]
        
        cursor.close()
        conn.close()
        
        return tables
    
    def get_table_count(self, database: str, table_name: str) -> int:
        """Get row count for a table with optional date filtering"""
        conn = self.get_connection(self.config, database)
        cursor = conn.cursor()
        
        # Check if table has date column for filtering
        has_date_column = self._has_date_column(database, table_name)
        
        if has_date_column:
            # Use date filtering
            base_query = f"SELECT COUNT(*) FROM {table_name}"
            query, params = self._build_date_filter_query(table_name, base_query)
            cursor.execute(query, params)
        else:
            # No date filtering
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        
        count = cursor.fetchone()[0]
        
        cursor.close()
        conn.close()
        
        return count
    
    def extract_table_batch(self, database: str, table_name: str, offset: int) -> List[Dict[str, Any]]:
        """Extract a batch of rows from a table with optional date filtering"""
        try:
            conn = self.get_connection(self.config, database)
            cursor = conn.cursor(dictionary=True)
            
            # Check if table has date column for filtering
            has_date_column = self._has_date_column(database, table_name)
            
            if has_date_column:
                # Use date filtering
                base_query = f"SELECT * FROM {table_name} LIMIT %s OFFSET %s"
                query, params = self._build_date_filter_query(table_name, base_query)
                # Add LIMIT and OFFSET parameters
                params.extend([self.config['extraction']['batch_size'], offset])
                cursor.execute(query, params)
            else:
                # No date filtering
                query = f"SELECT * FROM {table_name} LIMIT %s OFFSET %s"
                cursor.execute(query, (self.config['extraction']['batch_size'], offset))
            
            results = cursor.fetchall()
            
            cursor.close()
            conn.close()
            
            return results
            
        except Exception as e:
            self.logger.error(f"Error extracting batch from {database}.{table_name} at offset {offset}: {e}")
            return []
    
    def extract_table_data(self, database: str, table_name: str) -> List[Dict[str, Any]]:
        """Extract all data from a table using parallel batch processing"""
        total_rows = self.get_table_count(database, table_name)
        self.logger.info(f"Extracting {total_rows} rows from {database}.{table_name}")
        
        if total_rows == 0:
            return []
        
        all_data = []
        offsets = list(range(0, total_rows, self.config['extraction']['batch_size']))
        
        # Use ThreadPoolExecutor for parallel extraction
        with ThreadPoolExecutor(max_workers=self.config['extraction']['workers']) as executor:
            future_to_offset = {
                executor.submit(self.extract_table_batch, database, table_name, offset): offset 
                for offset in offsets
            }
            
            for future in as_completed(future_to_offset):
                offset = future_to_offset[future]
                try:
                    data = future.result()
                    all_data.extend(data)
                    self.logger.debug(f"Extracted {len(data)} rows from offset {offset}")
                except Exception as e:
                    self.logger.error(f"Failed to extract batch at offset {offset}: {e}")
        
        return all_data
    
    def extract_table(self, database: str, table_name: str) -> str:
        """
        Extract a single table and save to JSON file
        
        Args:
            database: Database name
            table_name: Table name
            
        Returns:
            Path to saved JSON file
        """
        self.logger.info(f"Extracting table: {database}.{table_name}")
        
        # Extract data
        data = self.extract_table_data(database, table_name)
        
        # Prepare output data
        start_date, end_date = self._get_date_filter_params()
        has_date_column = self._has_date_column(database, table_name)
        
        output_data = {
            'database': database,
            'table': table_name,
            'row_count': len(data),
            'extraction_timestamp': datetime.now().isoformat(),
            'date_filtering': {
                'enabled': has_date_column and (start_date or end_date),
                'start_date': start_date,
                'end_date': end_date,
                'date_column': self.config.get('date_filtering', {}).get('date_column', 'updated_at'),
                'table_has_date_column': has_date_column
            },
            'data': data
        }
        
        # Save to file
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{database}_{table_name}_{timestamp}.json"
        filepath = os.path.join(self.config['output_dir'], filename)
        
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        
        with open(filepath, 'w') as f:
            json.dump(output_data, f, default=str, indent=2)
        
        self.logger.info(f"Saved {len(data)} rows to {filepath}")
        return filepath
    
    def extract_database_to_dict(self, database: str, table_names: Optional[List[str]] = None) -> Dict:
        """
        Extract all tables from a database into a dictionary
        
        Args:
            database: Database name
            table_names: Optional list of specific tables to extract
            
        Returns:
            Dictionary with table data
        """
        if not table_names:
            table_names = self.list_tables_in_database(database)
        
        database_data = {}
        
        for table in table_names:
            try:
                self.logger.info(f"Extracting table: {database}.{table}")
                table_data = self.extract_table_data(database, table)
                
                if table_data:
                    database_data[table] = {
                        'records': len(table_data),
                        'sample': table_data  # Note: This is all data, not just a sample
                    }
                    
            except Exception as e:
                self.logger.error(f"Failed to extract table {database}.{table}: {e}")
        
        return database_data
    
    def extract_all_databases(self, table_names: Optional[List[str]] = None) -> str:
        """
        Extract from all databases into a single consolidated JSON file
        
        Args:
            table_names: Optional list of specific tables to extract from each database
            
        Returns:
            Path to consolidated JSON file
        """
        # Log extraction start date source
        date_config = self.config.get('date_filtering', {})
        start_date = date_config.get('start_date')
        if start_date:
            if self.custom_config and 'extraction_start_date_override' in self.custom_config:
                self.logger.info(f"Using override extraction start date: {start_date}")
            elif checkpoint.get_last_extraction_date() == start_date:
                self.logger.info(f"Using checkpoint start date: {start_date}")
            else:
                self.logger.info(f"Using environment start date: {start_date}")
        
        databases = self.list_databases()
        self.logger.info(f"Found {len(databases)} databases: {', '.join(databases)}")
        
        consolidated_data = {}
        
        for database in databases:
            self.logger.info(f"Processing database: {database}")
            try:
                database_tables = self.extract_database_to_dict(database, table_names)
                if database_tables:
                    consolidated_data[database] = database_tables
                    self.logger.info(f"Extracted {len(database_tables)} tables from {database}")
            except Exception as e:
                self.logger.error(f"Failed to extract from database {database}: {e}")
        
        return self.save_consolidated_json(consolidated_data)
    
    def save_consolidated_json(self, consolidated_data: Dict) -> str:
        """Save consolidated data to a single JSON file with date filtering metadata"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"extracted_data_{timestamp}.json"
        filepath = os.path.join(self.config['output_dir'], filename)
        
        # Add date filtering metadata to consolidated data
        start_date, end_date = self._get_date_filter_params()
        consolidated_data['extraction_metadata'] = {
            'extraction_timestamp': datetime.now().isoformat(),
            'date_filtering': {
                'enabled': bool(start_date or end_date),
                'start_date': start_date,
                'end_date': end_date,
                'date_column': self.config.get('date_filtering', {}).get('date_column', 'updated_at'),
                'days_count': self.config.get('date_filtering', {}).get('days_count')
            },
            'total_databases': len(consolidated_data),
            'total_tables': sum(len(tables) for tables in consolidated_data.values() if isinstance(tables, dict)),
            'total_records': sum(
                sum(table_data.get('records', 0) for table_data in tables.values() if isinstance(table_data, dict))
                for tables in consolidated_data.values() if isinstance(tables, dict)
            )
        }
        
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        
        with open(filepath, 'w') as f:
            json.dump(consolidated_data, f, default=str, indent=2)
        
        self.logger.info(f"Saved consolidated data to {filepath}")
        return filepath
    
    def extract(self, table_names: Optional[List[str]] = None, 
                databases: Optional[List[str]] = None) -> str:
        """
        Main extraction method - extracts data to consolidated JSON
        
        Args:
            table_names: Optional list of specific tables to extract
            databases: Optional list of specific databases to extract from
            
        Returns:
            Path to consolidated JSON file
        """
        if databases:
            # Extract from specific databases
            consolidated_data = {}
            for database in databases:
                try:
                    database_tables = self.extract_database_to_dict(database, table_names)
                    if database_tables:
                        consolidated_data[database] = database_tables
                except Exception as e:
                    self.logger.error(f"Failed to extract from database {database}: {e}")
            return self.save_consolidated_json(consolidated_data)
        else:
            # Extract from all databases
            return self.extract_all_databases(table_names)


if __name__ == "__main__":
    # Example usage
    extractor = MySQLExtractor()
    extracted_file = extractor.extract()
    print(f"Extraction complete: {extracted_file}")