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
import pymysql
import pymysql.cursors
import atexit
import gc

from .base import BaseExtractor
from .extraction_mapping import REQUIRED_TABLES, SKIP_TABLES, should_extract_table


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
        
        # Performance optimization caches
        self._date_column_cache = {}
        self._date_filter_cache = None
        
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
            # Priority: 1. Override from custom config, 2. Environment variable
            # Initialize date configuration
            extract_date = None
            extract_direction = None
            
            # Check if we have an override from custom config
            if self.custom_config and 'extraction_start_date_override' in self.custom_config:
                extract_date = self.custom_config['extraction_start_date_override']
                extract_direction = settings.EXTRACT_DIRECTION or ''  # Use config value even for overrides
                # Logger not available yet, will log after initialization
            else:
                # Get extraction date - explicit empty value means NO filtering
                extract_date = settings.EXTRACT_DATE
                extract_direction = settings.EXTRACT_DIRECTION
            
            return {
                'mysql_connection_url': settings.MYSQL_CONNECTION_URL,
                'extraction': {
                    'workers': settings.EXTRACTION_WORKERS,
                    'batch_size': settings.EXTRACTION_BATCH_SIZE,
                    'db_workers': settings.EXTRACTION_DB_WORKERS
                },
                'output_dir': settings.EXTRACTED_OUTPUT_DIR,
                'extract_tables': settings.EXTRACT_TABLES,
                'log_level': settings.LOG_LEVEL,
                'extract_db_keywords': settings.EXTRACT_DB_KEYWORDS,
                'extract_db_exclude_keywords': settings.EXTRACT_DB_EXCLUDE_KEYWORDS,
                'date_filtering': {
                    'extract_date': extract_date,
                    'extract_direction': extract_direction,
                    'days_count': settings.EXTRACT_DAYS_COUNT,
                    'hours_count': settings.EXTRACT_HOURS_COUNT
                }
            }
    
    @staticmethod
    def get_connection(config: Dict, database: Optional[str] = None):
        """
        Create MySQL connection using PyMySQL (more stable than mysql-connector)
        
        Args:
            config: Configuration dictionary
            database: Optional database name to connect to
            
        Returns:
            PyMySQL connection object
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
            'password': parsed.password,
            'charset': 'utf8mb4',
            'autocommit': True,
            'connect_timeout': 30,
            'read_timeout': 300,  # 5 minutes for large queries
        }
        
        # Add database if specified in URL or parameter
        if database:
            connection_params['database'] = database
        elif parsed.path and len(parsed.path) > 1:
            connection_params['database'] = parsed.path[1:]
            
        return pymysql.connect(**connection_params)
    
    def list_databases(self) -> List[str]:
        """Get list of databases, excluding system and ignored databases"""
        conn = None
        cursor = None
        try:
            conn = self.get_connection(self.config)
            cursor = conn.cursor()
            
            cursor.execute("SHOW DATABASES")
            all_databases = [db[0] for db in cursor.fetchall()]
            
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
            
            # Filter out databases by exclude keywords (if provided)
            exclude_kw_str = self.config.get('extract_db_exclude_keywords') or ''
            exclude_keywords = exclude_kw_str.split(',')
            exclude_keywords = [kw.strip().lower() for kw in exclude_keywords if kw.strip()]
            
            if exclude_keywords:
                original_count = len(databases)
                # Exclude databases that match any exclude keyword
                databases = [
                    db for db in databases
                    if not any(exclude_kw in db.lower() for exclude_kw in exclude_keywords)
                ]
                excluded_count = original_count - len(databases)
                # Excluded databases are not logged to reduce spam
            
            return databases
        finally:
            # Always close connections
            if cursor:
                try:
                    cursor.close()
                except:
                    pass
            if conn:
                try:
                    conn.close()
                except:
                    pass
    
    def _get_date_filter_params(self) -> Tuple[Optional[str], Optional[str]]:
        """
        Get start and end dates/times for filtering based on configuration
        
        Supports both days and hours for flexible time-based extraction.
        Cached for performance.
        
        Returns:
            Tuple of (start_datetime, end_datetime) in format (can be date or datetime string)
        """
        # Return cached result if already computed
        if self._date_filter_cache is not None:
            return self._date_filter_cache
        
        date_config = self.config.get('date_filtering', {})
        extract_date = (date_config.get('extract_date') or '').strip()
        extract_direction = (date_config.get('extract_direction') or '').strip().lower()
        days_count = (date_config.get('days_count') or '').strip()
        hours_count = (date_config.get('hours_count') or '').strip()
        
        # Clean up inline comments from .env file
        if extract_date and '#' in extract_date:
            extract_date = extract_date.split('#')[0].strip()
        if days_count and '#' in days_count:
            days_count = days_count.split('#')[0].strip()
        if hours_count and '#' in hours_count:
            hours_count = hours_count.split('#')[0].strip()
        
        # If all three are empty, extract ALL data without filtering
        if not extract_date and not days_count and not hours_count:
            return None, None
        
        # If extract_date is missing but counts are provided, use relative date filtering
        if not extract_date:
            # If we have days or hours count, extract data older than (now - days - hours)
            if days_count or hours_count:
                days_count_int = int(days_count) if days_count else 0
                hours_count_int = int(hours_count) if hours_count else 0
                
                # Calculate the cutoff datetime
                cutoff_dt = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
                
                # Add days first
                if days_count_int > 0:
                    cutoff_dt = cutoff_dt + timedelta(days=days_count_int)
                
                # Then add hours
                if hours_count_int > 0:
                    cutoff_dt = cutoff_dt + timedelta(hours=hours_count_int)
                
                # Return None for start_date and cutoff as end_date
                # This will create a "less than" filter only
                self.logger.info(f"Extracting data older than {cutoff_dt.strftime('%Y-%m-%d %H:%M:%S')}")
                self._date_filter_cache = (None, cutoff_dt.strftime('%Y-%m-%d %H:%M:%S'))
                return self._date_filter_cache
            else:
                # No filtering at all
                return None, None
        
        # Convert empty strings to 0 for days and hours (treat empty as 0, not as "extract to now")
        days_count_int = int(days_count) if days_count else 0
        hours_count_int = int(hours_count) if hours_count else 0
        
        # Calculate start and end date/time based on direction
        try:
            # Parse extract_date (could be date or datetime)
            if ' ' in extract_date:
                # Already has time component
                base_dt = datetime.strptime(extract_date, '%Y-%m-%d %H:%M:%S')
            else:
                # Date only, assume 00:00:00
                base_dt = datetime.strptime(extract_date, '%Y-%m-%d')
            
            # Validate direction - must be explicitly set
            if not extract_direction:
                self.logger.error("EXTRACT_DIRECTION not set in configuration")
                self._date_filter_cache = (None, None)
                return None, None
                
            if extract_direction == 'old':
                # Extract data OLDER than the specified date
                if days_count_int == 0 and hours_count_int == 0:
                    # Extract everything before extract_date
                    start_date = None
                    end_date = base_dt.strftime('%Y-%m-%d %H:%M:%S')
                else:
                    # Extract data in range: (extract_date - days/hours) to extract_date
                    start_dt = base_dt - timedelta(days=days_count_int, hours=hours_count_int)
                    start_date = start_dt.strftime('%Y-%m-%d %H:%M:%S')
                    end_date = base_dt.strftime('%Y-%m-%d %H:%M:%S')
            elif extract_direction == 'new':
                # Extract data NEWER than the specified date
                if days_count_int == 0 and hours_count_int == 0:
                    # Extract from extract_date to NOW
                    start_date = base_dt.strftime('%Y-%m-%d %H:%M:%S')
                    end_date = None  # Will extract to current time
                else:
                    # Extract data in range: extract_date to (extract_date + days/hours)
                    end_dt = base_dt + timedelta(days=days_count_int, hours=hours_count_int)
                    start_date = base_dt.strftime('%Y-%m-%d %H:%M:%S')
                    end_date = end_dt.strftime('%Y-%m-%d %H:%M:%S')
            else:
                self.logger.error(f"Invalid EXTRACT_DIRECTION: {extract_direction}. Must be 'new' or 'old'")
                self._date_filter_cache = (None, None)
                return None, None
                
        except (ValueError, TypeError) as e:
            self.logger.warning(f"Invalid date configuration: extract_date={extract_date}. Disabling date filtering.")
            self._date_filter_cache = (None, None)
            return None, None
        
        # Cache the result
        self._date_filter_cache = (start_date, end_date)
        return start_date, end_date
    
    def _has_date_column(self, database: str, table_name: str) -> tuple:
        """
        Check if table has a date column for filtering and return the column name
        
        Cached for performance to avoid repeated SHOW COLUMNS queries.
        
        Args:
            database: Database name
            table_name: Table name
            
        Returns:
            Tuple of (has_column: bool, column_name: str or None)
        """
        # Check cache first
        cache_key = f"{database}.{table_name}"
        if cache_key in self._date_column_cache:
            return self._date_column_cache[cache_key]
        
        conn = None
        cursor = None
        try:
            conn = self.get_connection(self.config, database)
            cursor = conn.cursor()
            
            # Hardcoded priority: Check for epoch columns first, then regular date columns
            # Priority 1: Epoch columns (most accurate for filtering)
            # Note: updated_at_epoch preferred over created_at_epoch for incremental updates
            epoch_columns = ['updated_at_epoch', 'updated_epoch', 'modified_at_epoch', 'last_updated_epoch']
            
            for col in epoch_columns:
                cursor.execute(f"SHOW COLUMNS FROM {table_name} LIKE %s", (col,))
                result = cursor.fetchone()
                if result:
                    # Cache the result
                    self._date_column_cache[cache_key] = (True, col)
                    return True, col
            
            # Priority 2: created_at_epoch as fallback for static/reference tables
            cursor.execute(f"SHOW COLUMNS FROM {table_name} LIKE %s", ('created_at_epoch',))
            result = cursor.fetchone()
            if result:
                # Cache the result
                self._date_column_cache[cache_key] = (True, 'created_at_epoch')
                return True, 'created_at_epoch'
            
            # Priority 3: Regular date/datetime columns
            date_columns = ['updated_at', 'updated_date', 'modified_at', 'modified_date', 'last_updated', 'update_time', 'created_at', 'created_date']
            
            for col in date_columns:
                cursor.execute(f"SHOW COLUMNS FROM {table_name} LIKE %s", (col,))
                result = cursor.fetchone()
                if result:
                    # Cache the result
                    self._date_column_cache[cache_key] = (True, col)
                    return True, col
            
            # Cache the result
            self._date_column_cache[cache_key] = (False, None)
            return False, None
            
        except Exception as e:
            self.logger.warning(f"Could not check date column for {database}.{table_name}: {e}")
            # Cache failure as no date column
            self._date_column_cache[cache_key] = (False, None)
            return False, None
        finally:
            # Always close connections in all scenarios
            if cursor:
                try:
                    cursor.close()
                except:
                    pass
            if conn:
                try:
                    conn.close()
                except:
                    pass
    
    def _build_date_filter_query(self, table_name: str, base_query: str, date_column: str = None) -> Tuple[str, List]:
        """
        Build query with date filtering
        
        Args:
            table_name: Table name
            base_query: Base SELECT query
            date_column: Date column to use for filtering (optional)
            
        Returns:
            Tuple of (modified_query, parameters)
        """
        start_date, end_date = self._get_date_filter_params()
        
        if not start_date and not end_date:
            return base_query, []
        
        # Use provided date column or fall back to configured one
        if not date_column:
            date_column = self.config.get('date_filtering', {}).get('date_column', 'updated_at')
        
        params = []
        
        # Add WHERE clause for date filtering
        where_conditions = []
        
        # Check if this is an epoch column
        is_epoch_column = '_epoch' in date_column.lower()
        
        if start_date:
            if is_epoch_column:
                # Convert date/datetime to epoch
                try:
                    if ' ' in start_date:
                        # Has time component
                        start_dt = datetime.strptime(start_date, '%Y-%m-%d %H:%M:%S')
                    else:
                        # Date only, assume 00:00:00
                        start_dt = datetime.strptime(start_date, '%Y-%m-%d')
                        start_dt = start_dt.replace(hour=0, minute=0, second=0)
                    
                    start_epoch = int(start_dt.timestamp() * 1000)  # Convert to milliseconds
                    where_conditions.append(f"{date_column} >= %s")
                    params.append(start_epoch)
                except ValueError as e:
                    self.logger.error(f"Invalid start_date format: {start_date}")
                    return base_query, []
            else:
                where_conditions.append(f"{date_column} >= %s")
                params.append(start_date)
        
        if end_date:
            if is_epoch_column:
                # Convert date/datetime to epoch
                try:
                    if ' ' in end_date:
                        # Has time component
                        end_dt = datetime.strptime(end_date, '%Y-%m-%d %H:%M:%S')
                    else:
                        # Date only, assume 23:59:59 for end of day
                        end_dt = datetime.strptime(end_date, '%Y-%m-%d')
                        end_dt = end_dt.replace(hour=23, minute=59, second=59)
                    
                    end_epoch = int(end_dt.timestamp() * 1000)  # Convert to milliseconds
                    where_conditions.append(f"{date_column} <= %s")
                    params.append(end_epoch)
                except ValueError as e:
                    self.logger.error(f"Invalid end_date format: {end_date}")
                    return base_query, []
            else:
                where_conditions.append(f"{date_column} <= %s")
                params.append(end_date)
        
        if where_conditions:
            where_clause = " AND ".join(where_conditions)
            
            # Check if query has LIMIT/OFFSET - WHERE must come before them
            if "LIMIT" in base_query.upper():
                # Insert WHERE clause before LIMIT
                parts = base_query.upper().split("LIMIT")
                select_part = base_query[:len(parts[0])]
                limit_part = base_query[len(parts[0]):]
                
                if "WHERE" in select_part.upper():
                    # Add to existing WHERE clause
                    query = select_part.replace("WHERE", f"WHERE {where_clause} AND") + limit_part
                else:
                    # Add WHERE clause before LIMIT
                    query = f"{select_part} WHERE {where_clause} {limit_part}"
            elif "WHERE" in base_query.upper():
                # Add to existing WHERE clause (no LIMIT)
                query = base_query.replace("WHERE", f"WHERE {where_clause} AND")
            else:
                # Add new WHERE clause (no LIMIT)
                query = f"{base_query} WHERE {where_clause}"
        else:
            query = base_query
        
        return query, params
    
    def list_tables_in_database(self, database: str) -> List[str]:
        """Get list of tables in a database"""
        conn = None
        cursor = None
        try:
            conn = self.get_connection(self.config, database)
            cursor = conn.cursor()
            
            cursor.execute("SHOW TABLES")
            tables = [table[0] for table in cursor.fetchall()]
            
            return tables
        finally:
            # Always close connections
            if cursor:
                try:
                    cursor.close()
                except:
                    pass
            if conn:
                try:
                    conn.close()
                except:
                    pass
    
    def get_table_count(self, database: str, table_name: str) -> int:
        """Get row count for a table with optional date filtering - optimized"""
        conn = None
        cursor = None
        try:
            conn = self.get_connection(self.config, database)
            cursor = conn.cursor()
            
            # Check if table has date column for filtering
            has_date_column, date_column = self._has_date_column(database, table_name)
            
            # Get date filter params to check if filtering is actually enabled
            start_date, end_date = self._get_date_filter_params()
            
            if has_date_column and (start_date or end_date):
                # Use date filtering with EXPLAIN first to check if query will return results
                # This is faster than running the full count
                base_query = f"SELECT COUNT(*) FROM {table_name}"
                query, params = self._build_date_filter_query(table_name, base_query, date_column)
                cursor.execute(query, params)
            else:
                # No date filtering - use fast count
                cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            
            result = cursor.fetchone()
            count = result[0] if result else 0
            
            return count
        finally:
            # Ensure cleanup
            if cursor:
                try:
                    cursor.close()
                except:
                    pass
            if conn:
                try:
                    conn.close()
                except:
                    pass
    
    def extract_table_batch(self, database: str, table_name: str, offset: int) -> List[Dict[str, Any]]:
        """Extract a batch of rows from a table with optional date filtering"""
        conn = None
        cursor = None
        try:
            conn = self.get_connection(self.config, database)
            # Use DictCursor for table data extraction to get dictionaries
            cursor = conn.cursor(pymysql.cursors.DictCursor)
            
            # Check if table has date column for filtering
            has_date_column, date_column = self._has_date_column(database, table_name)
            
            # Get date filter params to check if filtering is actually enabled
            start_date, end_date = self._get_date_filter_params()
            
            if has_date_column and (start_date or end_date):
                # Use date filtering
                base_query = f"SELECT * FROM {table_name} LIMIT %s OFFSET %s"
                query, params = self._build_date_filter_query(table_name, base_query, date_column)
                # Add LIMIT and OFFSET parameters
                params.extend([self.config['extraction']['batch_size'], offset])
                cursor.execute(query, params)
            else:
                # No date filtering - extract all data
                query = f"SELECT * FROM {table_name} LIMIT %s OFFSET %s"
                cursor.execute(query, (self.config['extraction']['batch_size'], offset))
            
            # Fetch all results - PyMySQL handles None values properly
            results = cursor.fetchall()
            
            # PyMySQL already returns proper dictionaries, no conversion needed
            return results if results else []
            
        except Exception as e:
            self.logger.error(f"Error extracting batch from {database}.{table_name} at offset {offset}: {e}")
            return []
        finally:
            # Ensure proper cleanup in all cases
            try:
                if cursor is not None:
                    cursor.close()
            except:
                pass
            try:
                if conn is not None:
                    conn.close()
            except:
                pass
    
    def extract_table_data(self, database: str, table_name: str) -> List[Dict[str, Any]]:
        """Extract all data from a table - optimized for speed and stability"""
        # Quick check: Use extraction mapping to determine if table should be extracted
        # Tables in SKIP_TABLES are not required by transformation mappings
        if not should_extract_table(table_name):
            return []
        
        total_rows = self.get_table_count(database, table_name)
        
        if total_rows == 0:
            return []
        
        # For small tables (<10k rows), extract in single batch (faster, no threading overhead)
        if total_rows < 10000:
            return self.extract_table_batch(database, table_name, 0)
        
        # For large tables, use parallel extraction with optimized worker count
        all_data = []
        offsets = list(range(0, total_rows, self.config['extraction']['batch_size']))
        
        # Limit workers based on table size to avoid overhead
        optimal_workers = min(self.config['extraction']['workers'], max(1, len(offsets) // 2))
        
        # Use ThreadPoolExecutor for parallel extraction
        with ThreadPoolExecutor(max_workers=optimal_workers) as executor:
            future_to_offset = {
                executor.submit(self.extract_table_batch, database, table_name, offset): offset 
                for offset in offsets
            }
            
            for future in as_completed(future_to_offset):
                offset = future_to_offset[future]
                try:
                    data = future.result()
                    all_data.extend(data)
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
        # Extract data
        data = self.extract_table_data(database, table_name)
        
        # Prepare output data
        start_date, end_date = self._get_date_filter_params()
        has_date_column, date_column = self._has_date_column(database, table_name)
        
        output_data = {
            'database': database,
            'table': table_name,
            'row_count': len(data),
            'extraction_timestamp': datetime.now().isoformat(),
            'date_filtering': {
                'enabled': has_date_column and (start_date or end_date),
                'start_date': start_date,
                'end_date': end_date,
                'date_column': date_column if date_column else self.config.get('date_filtering', {}).get('date_column', 'updated_at'),
                'table_has_date_column': has_date_column,
                'actual_column_used': date_column if has_date_column else None
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
        
        tables_with_data = 0
        for table in table_names:
            try:
                table_data = self.extract_table_data(database, table)
                
                if table_data:
                    database_data[table] = {
                        'records': len(table_data),
                        'sample': table_data
                    }
                    tables_with_data += 1
                    
            except Exception as e:
                self.logger.error(f"Failed to extract {database}.{table}: {e}")
        
        return database_data
    
    def extract_all_databases(self, table_names: Optional[List[str]] = None) -> str:
        """
        Extract from all databases into a single consolidated JSON file
        
        Args:
            table_names: Optional list of specific tables to extract from each database
            
        Returns:
            Path to consolidated JSON file
        """
        from datetime import datetime as dt
        extraction_start = dt.now()
        
        # Get configuration
        date_config = self.config.get('date_filtering', {})
        
        # Get date filter parameters and log them clearly
        start_date, end_date = self._get_date_filter_params()
        
        databases = self.list_databases()
        
        # Log extraction start info with clear date range
        self.logger.info("=" * 60)
        self.logger.info("📅 EXTRACTION DATE RANGE:")
        if start_date and end_date:
            self.logger.info(f"   FROM: {start_date}")
            self.logger.info(f"   TO:   {end_date}")
            self.logger.info(f"   MODE: Date range extraction")
        elif start_date and not end_date:
            self.logger.info(f"   FROM: {start_date}")
            self.logger.info(f"   TO:   NOW (current time)")
            self.logger.info(f"   MODE: Incremental extraction (new data)")
        elif not start_date and end_date:
            self.logger.info(f"   FROM: Beginning of time")
            self.logger.info(f"   TO:   {end_date}")
            self.logger.info(f"   MODE: Historical extraction (old data)")
        else:
            self.logger.info("   FROM: Beginning of time")
            self.logger.info("   TO:   End of time")
            self.logger.info("   MODE: FULL EXTRACTION (no date filtering)")
        self.logger.info("=" * 60)
        self.logger.info(f"Databases to extract: {len(databases)}")
        
        consolidated_data = {}
        db_stats = {}
        
        # Use parallel processing for databases when extracting many (much faster!)
        if len(databases) > 5:
            # Use configured database workers (default 10, max 20)
            configured_db_workers = self.config.get('extraction', {}).get('db_workers', 10)
            max_db_workers = min(configured_db_workers, 20, len(databases))
            
            # Progress tracking (log every 20%)
            total_dbs = len(databases)
            last_logged_pct = 0
            
            with ThreadPoolExecutor(max_workers=max_db_workers) as executor:
                future_to_db = {
                    executor.submit(self.extract_database_to_dict, db, table_names): db
                    for db in databases
                }
                
                completed = 0
                total_records_so_far = 0
                
                for future in as_completed(future_to_db):
                    database = future_to_db[future]
                    completed += 1
                    
                    try:
                        database_tables = future.result()
                        if database_tables:
                            consolidated_data[database] = database_tables
                            total_records = sum(t.get('records', 0) for t in database_tables.values())
                            db_stats[database] = {'tables': len(database_tables), 'records': total_records}
                            total_records_so_far += total_records
                    except Exception as e:
                        self.logger.error(f"[{completed}/{total_dbs}] Error in {database}: {e}")
                    
                    # Log every 20% or at completion
                    current_pct = int((completed / total_dbs) * 100)
                    if current_pct >= last_logged_pct + 20 or completed == total_dbs:
                        elapsed = (dt.now() - extraction_start).total_seconds()
                        remaining = (elapsed / completed) * (total_dbs - completed) if completed > 0 else 0
                        self.logger.info(
                            f"Progress: {current_pct}% ({completed}/{total_dbs} DBs) | "
                            f"{total_records_so_far:,} records | "
                            f"ETA: {remaining:.0f}s"
                        )
                        last_logged_pct = current_pct
        else:
            # Sequential for small number of databases (less overhead)
            for idx, database in enumerate(databases, 1):
                try:
                    database_tables = self.extract_database_to_dict(database, table_names)
                    if database_tables:
                        consolidated_data[database] = database_tables
                        total_records = sum(t.get('records', 0) for t in database_tables.values())
                        db_stats[database] = {'tables': len(database_tables), 'records': total_records}
                except Exception as e:
                    self.logger.error(f"Failed to extract from database {database}: {e}")
        
        # Log extraction summary
        extraction_end = dt.now()
        duration = (extraction_end - extraction_start).total_seconds()
        total_tables = sum(s['tables'] for s in db_stats.values())
        total_records = sum(s['records'] for s in db_stats.values())
        
        self.logger.info("=" * 60)
        self.logger.info("EXTRACTION SUMMARY")
        self.logger.info("=" * 60)
        self.logger.info(f"Databases: {len(consolidated_data)}/{len(databases)}")
        self.logger.info(f"Tables: {total_tables:,}")
        self.logger.info(f"Records: {total_records:,}")
        self.logger.info(f"Duration: {duration:.1f}s")
        self.logger.info(f"Speed: {total_records/duration if duration > 0 else 0:,.0f} records/sec")
        self.logger.info("=" * 60)
        
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
        
        # Write without indentation for faster I/O (use indent=None for compact JSON)
        with open(filepath, 'w') as f:
            json.dump(consolidated_data, f, default=str)
        
        self.logger.info(f"Saved to: {filepath}")
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
    extractor = DataExtractor()
    extracted_file = extractor.extract()
    print(f"Extraction complete: {extracted_file}")