"""
MySQL Data Extractor - Simplified and Optimized

Extracts data from MySQL databases with:
- Connection pooling for resource management  
- Controlled parallelism to prevent connection explosion
- Automatic shutdown on critical errors
- Support for multiple databases
"""

import json
import os
import atexit
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed
import pymysql
import pymysql.cursors
import logging
import gc

from .base import BaseExtractor
from .extraction_mapping import should_extract_table
from ..utils.memory_monitor import MemoryMonitor, estimate_table_memory

class DataExtractor(BaseExtractor):
    """Simplified MySQL data extractor with proper resource management"""
    
    def __init__(self, config: Optional[Dict] = None):
        """Initialize extractor with connection pool and processors"""
        self.custom_config = config or {}
        super().__init__()
        
        # Performance optimization caches
        self._date_column_cache = {}
        self._date_filter_cache = None
        self._connection_pool = {}  # Connection pooling for reuse
        self._table_columns_cache = {}  # Cache all columns per database
        
        # Initialize memory monitor
        from ..config import settings
        # Calculate actual memory limit based on system RAM percentage
        if settings.ENABLE_MEMORY_LIMIT:
            import psutil
            total_ram_mb = psutil.virtual_memory().total / (1024 * 1024)
            memory_limit_mb = int(total_ram_mb * settings.MEMORY_LIMIT_PERCENT / 100)
        else:
            memory_limit_mb = None
            
        self.memory_monitor = MemoryMonitor(
            max_memory_mb=memory_limit_mb,
            enable_limit=settings.ENABLE_MEMORY_LIMIT
        )
        
        # Register cleanup function to prevent fatal errors
        atexit.register(self._cleanup)
        
    
    def _cleanup(self):
        """Cleanup function to prevent MySQL connector fatal errors"""
        try:
            # Close all pooled connections
            for conn in self._connection_pool.values():
                try:
                    conn.close()
                except:
                    pass
            self._connection_pool.clear()
            
            # Force garbage collection to clean up MySQL connections
            gc.collect()
        except Exception:
            pass  # Ignore cleanup errors
        
    def _load_config(self) -> Dict:
        """Load configuration from settings"""
        from src.config import settings
        
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
                'identity_mysql_connection_url': settings.IDENTITY_MYSQL_CONNECTION_URL,
                'master_mysql_connection_url': settings.MASTER_MYSQL_CONNECTION_URL,
                'tenant_mysql_connection_url': settings.TENANT_MYSQL_CONNECTION_URL,
                'extraction': {
                    'workers': settings.EXTRACTION_WORKERS,  # Now defaults to 1
                    'batch_size': 5000,  # Smaller batch size for safety
                    'db_workers': 1      # Single DB worker for safety
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
                },
                'max_memory_per_table_mb': settings.MAX_MEMORY_PER_TABLE_MB
            }
    
    def get_connection(self, config: Dict, database: Optional[str] = None):
        """
        Create MySQL connection using PyMySQL (more stable than mysql-connector)
        
        Args:
            config: Configuration dictionary
            database: Optional database name to connect to
            
        Returns:
            PyMySQL connection object
        """
        from urllib.parse import urlparse
        
        # Determine which connection URL to use based on database name
        if database:
            if database.lower() == 'identity':
                connection_url = config.get('identity_mysql_connection_url')
            elif database.lower() == 'master':
                connection_url = config.get('master_mysql_connection_url')
            else:
                # All other databases are considered tenant databases
                connection_url = config.get('tenant_mysql_connection_url')
        else:
            # Default to tenant URL when no database is specified
            # This is used for initial connection to list databases
            connection_url = config.get('tenant_mysql_connection_url')
        
        if not connection_url:
            raise ValueError(f"MySQL connection URL not found for database: {database}")
        
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
        
        # Check connection pool first
        pool_key = f"{parsed.hostname}:{parsed.port}:{database or 'default'}"
        if pool_key in self._connection_pool:
            conn = self._connection_pool[pool_key]
            try:
                # Test if connection is still alive
                conn.ping(reconnect=True)
                return conn
            except:
                # Connection is dead, remove from pool
                del self._connection_pool[pool_key]
        
        # Create new connection and add to pool
        conn = pymysql.connect(**connection_params)
        self._connection_pool[pool_key] = conn
        return conn
    
    def list_databases(self) -> List[str]:
        """Get list of databases from all MySQL servers, excluding system and ignored databases"""
        all_databases = []
        
        # Define servers to check with their specific databases
        servers = [
            ('identity', 'identity_mysql_connection_url', ['identity']),
            ('master', 'master_mysql_connection_url', ['master']),
            ('tenant', 'tenant_mysql_connection_url', None)  # None means get all databases
        ]
        
        for server_name, url_key, specific_dbs in servers:
            conn = None
            cursor = None
            try:
                # Skip if URL not configured
                if not self.config.get(url_key):
                    self.logger.warning(f"No connection URL configured for {server_name} server")
                    continue
                
                # For specific databases, just add them if connection works
                if specific_dbs:
                    # Test connection by connecting to the specific database
                    for db_name in specific_dbs:
                        try:
                            test_conn = self.get_connection(self.config, db_name)
                            # Connection successful, add database
                            all_databases.append(db_name)
                            self.logger.info(f"Found {server_name} database: {db_name}")
                        except Exception as e:
                            self.logger.warning(f"Could not connect to {db_name} database: {e}")
                else:
                    # For tenant server, get all databases dynamically
                    temp_config = {url_key: self.config[url_key]}
                    # Create temporary connection to list databases
                    from urllib.parse import urlparse
                    parsed = urlparse(self.config[url_key])
                    
                    import pymysql
                    conn = pymysql.connect(
                        host=parsed.hostname,
                        port=parsed.port if parsed.port else 3306,
                        user=parsed.username,
                        password=parsed.password,
                        charset='utf8mb4',
                        autocommit=True,
                        connect_timeout=30
                    )
                    cursor = conn.cursor()
                    
                    cursor.execute("SHOW DATABASES")
                    server_databases = [db[0] for db in cursor.fetchall()]
                    
                    # Filter out system databases
                    system_dbs = ['information_schema', 'mysql', 'performance_schema', 'sys']
                    server_databases = [db for db in server_databases if db not in system_dbs]
                    
                    all_databases.extend(server_databases)
                    self.logger.info(f"Found {len(server_databases)} databases on {server_name} server")
                    
            except Exception as e:
                self.logger.error(f"Error listing databases from {server_name} server: {e}")
            finally:
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
        
        # Remove duplicates while preserving order
        databases = list(dict.fromkeys(all_databases))
        
        # Apply filtering logic
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
            # Exclude databases that match any exclude keyword
            databases = [
                db for db in databases
                if not any(exclude_kw in db.lower() for exclude_kw in exclude_keywords)
            ]
        
        return databases
    
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
    
    def _get_all_table_columns(self, database: str) -> Dict[str, List[str]]:
        """
        Get all columns for all tables in a database in one query.
        Cached for performance.
        
        Returns:
            Dict mapping table_name to list of column names
        """
        if database in self._table_columns_cache:
            return self._table_columns_cache[database]
        
        conn = None
        cursor = None
        try:
            conn = self.get_connection(self.config, database)
            cursor = conn.cursor()
            
            # Get all columns for all tables in one query
            cursor.execute("""
                SELECT TABLE_NAME, COLUMN_NAME 
                FROM INFORMATION_SCHEMA.COLUMNS 
                WHERE TABLE_SCHEMA = %s
                ORDER BY TABLE_NAME, ORDINAL_POSITION
            """, (database,))
            
            table_columns = {}
            for table_name, column_name in cursor.fetchall():
                if table_name not in table_columns:
                    table_columns[table_name] = []
                table_columns[table_name].append(column_name)
            
            self._table_columns_cache[database] = table_columns
            return table_columns
            
        except Exception as e:
            self.logger.warning(f"Could not fetch columns for database {database}: {e}")
            return {}
        finally:
            if cursor:
                try:
                    cursor.close()
                except:
                    pass
            # Don't close connection - keep in pool
    
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
        
        # Get all columns for the database (batch operation)
        table_columns = self._get_all_table_columns(database)
        columns = table_columns.get(table_name, [])
        
        # Hardcoded priority: Check for epoch columns first, then regular date columns
        # Priority 1: Epoch columns (most accurate for filtering)
        # Note: updated_at_epoch preferred over created_at_epoch for incremental updates
        epoch_columns = ['updated_at_epoch', 'updated_epoch', 'modified_at_epoch', 'last_updated_epoch']
        
        for col in epoch_columns:
            if col in columns:
                # Cache the result
                self._date_column_cache[cache_key] = (True, col)
                return True, col
        
        # Priority 2: created_at_epoch as fallback for static/reference tables
        if 'created_at_epoch' in columns:
            # Cache the result
            self._date_column_cache[cache_key] = (True, 'created_at_epoch')
            return True, 'created_at_epoch'
        
        # Priority 3: Regular date/datetime columns
        date_columns = ['updated_at', 'updated_date', 'modified_at', 'modified_date', 'last_updated', 'update_time', 'created_at', 'created_date']
        
        for col in date_columns:
            if col in columns:
                # Cache the result
                self._date_column_cache[cache_key] = (True, col)
                return True, col
        
        # Cache the result
        self._date_column_cache[cache_key] = (False, None)
        return False, None
    
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
            # Only close cursor, keep connection in pool
            if cursor:
                try:
                    cursor.close()
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
                # For large tables with date filtering, use approximate count if possible
                # First try to get table stats from information_schema (very fast)
                cursor.execute("""
                    SELECT TABLE_ROWS 
                    FROM INFORMATION_SCHEMA.TABLES 
                    WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
                """, (database, table_name))
                result = cursor.fetchone()
                approx_count = result[0] if result and result[0] else None
                
                # If table is very large (>100k rows), use LIMIT 1 to check if any rows match
                if approx_count and approx_count > 100000:
                    base_query = f"SELECT 1 FROM {table_name}"
                    query, params = self._build_date_filter_query(table_name, base_query, date_column)
                    query += " LIMIT 1"
                    cursor.execute(query, params)
                    has_rows = cursor.fetchone() is not None
                    
                    # If no rows match filter, return 0 quickly
                    if not has_rows:
                        return 0
                
                # For smaller tables or if rows exist, get exact count
                base_query = f"SELECT COUNT(*) FROM {table_name}"
                query, params = self._build_date_filter_query(table_name, base_query, date_column)
                cursor.execute(query, params)
            else:
                # No date filtering - use fast count from information_schema when possible
                cursor.execute("""
                    SELECT TABLE_ROWS 
                    FROM INFORMATION_SCHEMA.TABLES 
                    WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
                """, (database, table_name))
                result = cursor.fetchone()
                
                # Use approximate count for large tables (>10k rows)
                if result and result[0] and result[0] > 10000:
                    return result[0]
                
                # Get exact count for small tables
                cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            
            result = cursor.fetchone()
            count = result[0] if result else 0
            
            return count
        finally:
            # Only close cursor, keep connection in pool
            if cursor:
                try:
                    cursor.close()
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
            # Only close cursor, keep connection in pool
            if cursor:
                try:
                    cursor.close()
                except:
                    pass
    
    def extract_table_data(self, database: str, table_name: str, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """Extract all data from a table - optimized for speed and stability"""
        # Quick check: Use extraction mapping to determine if table should be extracted
        # Tables in SKIP_TABLES are not required by transformation mappings
        if not should_extract_table(table_name):
            return []
        
        total_rows = self.get_table_count(database, table_name)
        
        if total_rows == 0:
            return []
        
        # Apply limit if specified
        if limit and total_rows > limit:
            total_rows = limit
        
        # For small tables (<10k rows), extract in single batch (faster, no threading overhead)
        if total_rows < 10000:
            data = self.extract_table_batch(database, table_name, 0)
            return data[:limit] if limit else data
        
        # For large tables, use parallel extraction with optimized worker count
        all_data = []
        offsets = list(range(0, total_rows, self.config['extraction']['batch_size']))
        
        # Optimize worker count based on table size and batch count
        # Too many workers cause connection overhead and contention
        batch_count = len(offsets)
        if batch_count <= 2:
            optimal_workers = 1  # Sequential is faster for 1-2 batches
        elif batch_count <= 5:
            optimal_workers = 2  # Light parallelism
        elif batch_count <= 10:
            optimal_workers = 3  # Moderate parallelism
        else:
            # For many batches, use more workers but cap at 5 to avoid contention
            optimal_workers = min(5, self.config['extraction']['workers'])
        
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
        
        # Pre-fetch all table columns for this database (batch optimization)
        self._get_all_table_columns(database)
        
        database_data = {}
        
        # Filter tables early using extraction mapping
        tables_to_extract = []
        for table in table_names:
            if should_extract_table(table):
                tables_to_extract.append(table)
        
        tables_with_data = 0
        for table in tables_to_extract:
            try:
                # Check memory before extracting table
                self.memory_monitor.check_memory(f"before extracting {database}.{table}")
                
                # Check if table is too large for available memory
                count = self.get_table_count(database, table)
                if count > 0:
                    # Estimate memory needed (rough estimate: 1KB per row)
                    estimated_mb = count / 1000
                    # Use 10% of total memory limit as max per table
                    max_table_mb = self.memory_monitor.max_memory_mb * 0.1 if self.memory_monitor.max_memory_mb else 500
                    if self.memory_monitor.enable_limit and estimated_mb > max_table_mb:
                        self.logger.warning(
                            f"Table {database}.{table} too large ({count:,} rows, ~{estimated_mb:.1f}MB), "
                            f"exceeds 10% of memory limit ({max_table_mb:.0f}MB) - sampling instead"
                        )
                        # Extract only a sample for very large tables
                        table_data = self.extract_table_data(database, table, limit=10000)
                    else:
                        table_data = self.extract_table_data(database, table)
                else:
                    table_data = []
                
                if table_data:
                    database_data[table] = {
                        'records': count if count > 0 else len(table_data),
                        'sample': table_data
                    }
                    tables_with_data += 1
                    
                    # Log memory after extracting
                    self.memory_monitor.log_memory_status(f"After {database}.{table}")
                    
            except MemoryError as e:
                self.logger.error(f"Memory limit exceeded for {database}.{table}: {e}")
                # Try to continue with other tables after freeing memory
                gc.collect()
                self.memory_monitor.log_memory_status("After garbage collection")
            except Exception as e:
                self.logger.error(f"Failed to extract {database}.{table}: {e}")
        
        return database_data
    
    def extract_all_databases(self, table_names: Optional[List[str]] = None, etl_id: Optional[str] = None) -> str:
        """
        Extract from all databases into a single consolidated JSON file
        
        Args:
            table_names: Optional list of specific tables to extract from each database
            etl_id: Optional ETL run ID for organizing output files
            
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
        
        # Initialize progress tracker if etl_id provided
        from ..utils.progress_tracker import ProgressTracker
        tracker = ProgressTracker(etl_id) if etl_id else None
        
        if tracker:
            tracker.start_phase("Extraction", len(databases))
        
        # Sequential processing for safety (no parallelism)
        for idx, database in enumerate(databases, 1):
            self.logger.info(f"[{idx}/{len(databases)}] Extracting database: {database}")
            
            try:
                database_tables = self.extract_database_to_dict(database, table_names)
                if database_tables:
                    consolidated_data[database] = database_tables
                    total_records = sum(t.get('records', 0) for t in database_tables.values())
                    db_stats[database] = {'tables': len(database_tables), 'records': total_records}
                    self.logger.info(f"  ✓ Extracted {len(database_tables)} tables, {total_records:,} records")
                else:
                    self.logger.info(f"  ✓ No data extracted (empty or no matching tables)")
                    
                # Update progress
                if tracker:
                    tracker.update_progress(1)
                    
            except Exception as e:
                self.logger.error(f"  ✗ Failed to extract {database}: {e}")
        
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
        
        return self.save_consolidated_json(consolidated_data, etl_id)
    
    def save_consolidated_json(self, consolidated_data: Dict, etl_id: Optional[str] = None) -> str:
        """Save consolidated data to a single JSON file with date filtering metadata"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Create ETL-specific directory if etl_id is provided
        if etl_id:
            output_dir = os.path.join(self.config['output_dir'], etl_id)
            os.makedirs(output_dir, exist_ok=True)
        else:
            output_dir = self.config['output_dir']
            
        filename = f"extracted_data_{timestamp}.json"
        filepath = os.path.join(output_dir, filename)
        
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
        
        # Write without indentation for faster I/O (compact JSON)
        with open(filepath, 'w') as f:
            json.dump(consolidated_data, f, default=str, separators=(',', ':'))
        
        self.logger.info(f"Saved to: {filepath}")
        return filepath
    
    def extract(self, table_names: Optional[List[str]] = None, 
                databases: Optional[List[str]] = None) -> str:
        """
        Main extraction method
        
        Args:
            table_names: Optional list of specific tables to extract
            databases: Optional list of specific databases to extract from
            
        Returns:
            Path to consolidated JSON file
        """
        start_time = datetime.now()
        
        try:
            # Get databases to extract
            if not databases:
                databases = self.list_databases()
            
            self.logger.info(f"Starting extraction from {len(databases)} databases")
            
            # For very large database counts, process in batches
            if len(databases) > 50:
                consolidated_data = self._extract_many_databases(databases, table_names)
            else:
                # Normal processing
                consolidated_data = self._extract_databases(databases, table_names)
            
            # Save results
            result_path = self.save_consolidated_json(consolidated_data)
            
            duration = (datetime.now() - start_time).total_seconds()
            total_records = sum(
                sum(table.get('records', 0) for table in db_data.values())
                for db_data in consolidated_data.values() 
                if isinstance(db_data, dict)
            )
            
            self.logger.info("=" * 60)
            self.logger.info(f"Extraction completed successfully!")
            self.logger.info(f"Databases: {len(consolidated_data)}")
            self.logger.info(f"Total records: {total_records:,}")
            self.logger.info(f"Duration: {duration:.1f}s")
            self.logger.info(f"Output: {result_path}")
            self.logger.info("=" * 60)
            
            return result_path
            
        except Exception as e:
            self.logger.error(f"Extraction failed: {e}")
            # Ensure cleanup on error
            self.pool.close_all()
            raise
    
    def _extract_databases(self, databases: List[str], 
                          table_names: Optional[List[str]]) -> Dict:
        """Extract from multiple databases with controlled parallelism"""
        results = self.db_processor.process_items(
            databases,
            lambda db: self._extract_database_safe(db, table_names),
            "databases"
        )
        
        return {db: data for db, data in results.items() if data}
    
    def _extract_many_databases(self, databases: List[str], 
                               table_names: Optional[List[str]]) -> Dict:
        """Extract from many databases in batches to control resources"""
        consolidated_data = {}
        batch_size = 20  # Process 20 databases at a time
        
        for i in range(0, len(databases), batch_size):
            batch = databases[i:i + batch_size]
            batch_num = i // batch_size + 1
            total_batches = (len(databases) + batch_size - 1) // batch_size
            
            self.logger.info(f"Processing database batch {batch_num}/{total_batches}")
            
            batch_results = self._extract_databases(batch, table_names)
            consolidated_data.update(batch_results)
            
            # Force garbage collection between batches
            gc.collect()
        
        return consolidated_data
    
    def _extract_database_safe(self, database: str, 
                              table_names: Optional[List[str]]) -> Dict:
        """Extract database with error handling"""
        try:
            return self.extract_database_to_dict(database, table_names)
        except Exception as e:
            self.logger.error(f"Failed to extract database {database}: {e}")
            return {}
    
    def extract_database_to_dict(self, database: str, 
                                table_names: Optional[List[str]] = None) -> Dict:
        """Extract all tables from a database"""
        # Get tables to extract
        if not table_names:
            table_names = self.list_tables_in_database(database)
        
        # Filter tables based on extraction rules
        tables_to_extract = [t for t in table_names if should_extract_table(t)]
        
        if not tables_to_extract:
            return {}
        
        self.logger.info(f"Extracting {len(tables_to_extract)} tables from {database}")
        
        database_data = {}
        
        # Extract tables sequentially to limit connections per database
        for table in tables_to_extract:
            try:
                data = self.extract_table_data(database, table)
                if data:
                    database_data[table] = {
                        'records': len(data),
                        'sample': data
                    }
            except Exception as e:
                self.logger.error(f"Failed to extract {database}.{table}: {e}")
                # Check for critical errors
                if self._is_critical_error(e):
                    self.logger.error("Critical error detected, stopping extraction")
                    raise
        
        return database_data
    
    def extract_table_data(self, database: str, table_name: str) -> List[Dict]:
        """Extract all data from a table"""
        total_rows = self.get_table_count(database, table_name)
        
        if total_rows == 0:
            return []
        
        batch_size = self.config['extraction']['batch_size']
        
        # For small tables, extract in one batch
        if total_rows <= batch_size:
            return self.extract_table_batch(database, table_name, 0)
        
        # For large tables, extract in batches
        all_data = []
        
        for offset in range(0, total_rows, batch_size):
            try:
                batch = self.extract_table_batch(database, table_name, offset)
                all_data.extend(batch)
                
                # Progress logging for very large tables
                if total_rows > 100000 and offset > 0 and offset % (batch_size * 10) == 0:
                    progress = (len(all_data) / total_rows) * 100
                    self.logger.info(f"{database}.{table_name}: {progress:.1f}% ({len(all_data):,}/{total_rows:,})")
                    
            except Exception as e:
                self.logger.error(f"Error extracting batch at offset {offset}: {e}")
                if self._is_critical_error(e):
                    raise
                # Continue with next batch for non-critical errors
        
        return all_data
    
    def extract_table_batch(self, database: str, table_name: str, offset: int) -> List[Dict]:
        """Extract a batch of rows from a table"""
        conn = None
        cursor = None
        try:
            conn = self.get_connection(self.config, database)
            cursor = conn.cursor(pymysql.cursors.DictCursor)
            # Check if table has date filtering
            has_date_col, date_col = self._check_date_column(database, table_name)
            start_date, end_date = self._get_date_filter_params()
            
            # Build query
            if has_date_col and (start_date or end_date):
                query = self._build_date_filter_query(
                    table_name, 
                    f"SELECT * FROM {table_name} LIMIT %s OFFSET %s",
                    date_col,
                    start_date,
                    end_date
                )
                params = query[1] + [self.config['extraction']['batch_size'], offset]
                query = query[0]
            else:
                query = f"SELECT * FROM {table_name} LIMIT %s OFFSET %s"
                params = [self.config['extraction']['batch_size'], offset]
            
            cursor.execute(query, params)
            return cursor.fetchall()
        except Exception as e:
            self.logger.error(f"Error extracting batch from {database}.{table_name}: {e}")
            return []
        finally:
            if cursor:
                try:
                    cursor.close()
                except:
                    pass
    
    def get_table_count(self, database: str, table_name: str) -> int:
        """Get row count for a table"""
        conn = None
        cursor = None
        try:
            conn = self.get_connection(self.config, database)
            cursor = conn.cursor()
            # Check for date filtering
            has_date_col, date_col = self._check_date_column(database, table_name)
            start_date, end_date = self._get_date_filter_params()
            
            if has_date_col and (start_date or end_date):
                # Count with date filter
                query = self._build_date_filter_query(
                    table_name,
                    f"SELECT COUNT(*) FROM {table_name}",
                    date_col,
                    start_date,
                    end_date
                )
                cursor.execute(query[0], query[1])
            else:
                # Try fast approximate count first
                cursor.execute("""
                    SELECT TABLE_ROWS 
                    FROM INFORMATION_SCHEMA.TABLES 
                    WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
                """, (database, table_name))
                
                result = cursor.fetchone()
                if result and result[0] is not None and result[0] > 0:
                    # Add 10% buffer for safety
                    return int(result[0] * 1.1)
                
                # Fallback to exact count
                cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            
            return cursor.fetchone()[0] or 0
        except Exception as e:
            self.logger.error(f"Error getting count for {database}.{table_name}: {e}")
            return 0
        finally:
            if cursor:
                try:
                    cursor.close()
                except:
                    pass
    
    def list_databases(self) -> List[str]:
        """List all databases from the three connection URLs"""
        databases = self._list_databases_from_urls()
        
        # Apply filters
        databases = self._apply_database_filters(databases)
        
        self.logger.info(f"Found {len(databases)} databases to extract")
        return databases
    
    def list_tables_in_database(self, database: str) -> List[str]:
        """List all tables in a database"""
        conn = None
        cursor = None
        try:
            conn = self.get_connection(self.config, database)
            cursor = conn.cursor()
            cursor.execute("SHOW TABLES")
            return [row[0] for row in cursor.fetchall()]
        except Exception as e:
            self.logger.error(f"Error listing tables in {database}: {e}")
            return []
        finally:
            if cursor:
                try:
                    cursor.close()
                except:
                    pass
    
    def save_consolidated_json(self, consolidated_data: Dict) -> str:
        """Save consolidated data to JSON file"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"extracted_data_{timestamp}.json"
        filepath = os.path.join(self.config['output_dir'], filename)
        
        # Add metadata
        start_date, end_date = self._get_date_filter_params()
        consolidated_data['extraction_metadata'] = {
            'extraction_timestamp': datetime.now().isoformat(),
            'environment': self.config.get('environment', 'unknown'),
            'date_filtering': {
                'enabled': bool(start_date or end_date),
                'start_date': start_date,
                'end_date': end_date
            },
            'total_databases': len([k for k in consolidated_data.keys() if k != 'extraction_metadata']),
            'total_tables': sum(
                len(db_data) for db_data in consolidated_data.values() 
                if isinstance(db_data, dict)
            ),
            'total_records': sum(
                sum(table.get('records', 0) for table in db_data.values())
                for db_data in consolidated_data.values() 
                if isinstance(db_data, dict)
            )
        }
        
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        
        # Write compact JSON
        with open(filepath, 'w') as f:
            json.dump(consolidated_data, f, default=str, separators=(',', ':'))
        
        return filepath
    
    # === Helper Methods ===
    
    
    def _list_databases_from_urls(self) -> List[str]:
        """List databases from the three connection URLs"""
        all_databases = []
        
        # Define connections and their filters
        connections = [
            ('identity_mysql_connection_url', 'identity', lambda db: db.lower() == 'identity'),
            ('master_mysql_connection_url', 'master', lambda db: db.lower() == 'master'),
            ('tenant_mysql_connection_url', 'tenant', lambda db: db.lower().startswith('tenant'))
        ]
        
        for url_key, group, filter_func in connections:
            url = self.config.get(url_key)
            if not url:
                continue
            
            conn = None
            cursor = None
            try:
                # Create a temporary config with the URL
                temp_config = self.config.copy()
                temp_config[url_key] = url
                
                # Use the first matching database for connection
                # For identity/master, use their own names; for tenant, use None to get all
                test_db = group if group in ['identity', 'master'] else None
                conn = self.get_connection(temp_config, test_db)
                cursor = conn.cursor()
                
                cursor.execute("SHOW DATABASES")
                databases = [row[0] for row in cursor.fetchall()]
                
                # Apply group filter
                filtered = [db for db in databases if filter_func(db)]
                all_databases.extend(filtered)
                
                self.logger.info(f"{group.capitalize()}: found {len(filtered)} databases")
                        
            except Exception as e:
                self.logger.warning(f"Could not list {group} databases: {e}")
            finally:
                if cursor:
                    try:
                        cursor.close()
                    except:
                        pass
        
        return all_databases
    
    def _apply_database_filters(self, databases: List[str]) -> List[str]:
        """Apply include/exclude filters to database list"""
        # Include filter
        include_keywords = self.config.get('extract_db_keywords', '').split(',')
        include_keywords = [kw.strip().lower() for kw in include_keywords if kw.strip()]
        
        if include_keywords:
            databases = [
                db for db in databases 
                if any(kw in db.lower() for kw in include_keywords)
            ]
        
        # Exclude filter
        exclude_keywords = self.config.get('extract_db_exclude_keywords', '').split(',')
        exclude_keywords = [kw.strip().lower() for kw in exclude_keywords if kw.strip()]
        
        if exclude_keywords:
            databases = [
                db for db in databases
                if not any(kw in db.lower() for kw in exclude_keywords)
            ]
        
        return databases
    
    def _get_date_filter_params(self) -> Tuple[Optional[str], Optional[str]]:
        """Get date filtering parameters"""
        date_config = self.config.get('date_filtering', {})
        extract_date = date_config.get('extract_date', '').strip()
        extract_direction = date_config.get('extract_direction', '').strip()
        days_count = date_config.get('days_count', '').strip()
        hours_count = date_config.get('hours_count', '').strip()
        
        if not extract_date:
            return None, None
        
        try:
            # Parse base date
            if ' ' in extract_date:
                base_dt = datetime.strptime(extract_date, '%Y-%m-%d %H:%M:%S')
            else:
                base_dt = datetime.strptime(extract_date, '%Y-%m-%d')
            
            # Calculate date range
            days = int(days_count) if days_count else 0
            hours = int(hours_count) if hours_count else 0
            
            if extract_direction == 'old':
                if days == 0 and hours == 0:
                    return None, base_dt.strftime('%Y-%m-%d %H:%M:%S')
                else:
                    start_dt = base_dt - timedelta(days=days, hours=hours)
                    return start_dt.strftime('%Y-%m-%d %H:%M:%S'), base_dt.strftime('%Y-%m-%d %H:%M:%S')
            elif extract_direction == 'new':
                if days == 0 and hours == 0:
                    return base_dt.strftime('%Y-%m-%d %H:%M:%S'), None
                else:
                    end_dt = base_dt + timedelta(days=days, hours=hours)
                    return base_dt.strftime('%Y-%m-%d %H:%M:%S'), end_dt.strftime('%Y-%m-%d %H:%M:%S')
        except:
            return None, None
        
        return None, None
    
    def _check_date_column(self, database: str, table_name: str) -> Tuple[bool, Optional[str]]:
        """Check if table has a date column for filtering"""
        cache_key = f"{database}.{table_name}"
        if cache_key in self._date_column_cache:
            return self._date_column_cache[cache_key]
        
        # Get table columns
        columns = self._get_table_columns(database, table_name)
        
        # Check for date columns in priority order
        date_columns = [
            'updated_at_epoch', 'updated_epoch', 'modified_at_epoch',
            'created_at_epoch', 'updated_at', 'modified_at', 
            'created_at', 'update_time', 'created_date'
        ]
        
        for col in date_columns:
            if col in columns:
                self._date_column_cache[cache_key] = (True, col)
                return True, col
        
        self._date_column_cache[cache_key] = (False, None)
        return False, None
    
    def _get_table_columns(self, database: str, table_name: str) -> List[str]:
        """Get column names for a table"""
        if database not in self._table_columns_cache:
            self._table_columns_cache[database] = {}
        
        if table_name in self._table_columns_cache[database]:
            return self._table_columns_cache[database][table_name]
        
        conn = None
        cursor = None
        try:
            conn = self.get_connection(self.config, database)
            cursor = conn.cursor()
            cursor.execute(f"SHOW COLUMNS FROM {table_name}")
            columns = [row[0] for row in cursor.fetchall()]
            self._table_columns_cache[database][table_name] = columns
            return columns
        except Exception as e:
            self.logger.error(f"Error getting columns for {database}.{table_name}: {e}")
            return []
        finally:
            if cursor:
                try:
                    cursor.close()
                except:
                    pass
    
    def _build_date_filter_query(self, table_name: str, base_query: str, 
                                date_column: str, start_date: Optional[str], 
                                end_date: Optional[str]) -> Tuple[str, List]:
        """Build query with date filtering"""
        conditions = []
        params = []
        
        if start_date:
            if 'epoch' in date_column:
                # Convert to epoch milliseconds
                dt = datetime.strptime(start_date, '%Y-%m-%d %H:%M:%S')
                conditions.append(f"{date_column} >= %s")
                params.append(int(dt.timestamp() * 1000))
            else:
                conditions.append(f"{date_column} >= %s")
                params.append(start_date)
        
        if end_date:
            if 'epoch' in date_column:
                dt = datetime.strptime(end_date, '%Y-%m-%d %H:%M:%S')
                conditions.append(f"{date_column} <= %s")
                params.append(int(dt.timestamp() * 1000))
            else:
                conditions.append(f"{date_column} <= %s")
                params.append(end_date)
        
        if conditions:
            where_clause = " AND ".join(conditions)
            if "WHERE" in base_query.upper():
                query = base_query.replace("WHERE", f"WHERE {where_clause} AND", 1)
            else:
                # Insert WHERE before LIMIT
                if "LIMIT" in base_query:
                    parts = base_query.split("LIMIT")
                    query = f"{parts[0]} WHERE {where_clause} LIMIT{parts[1]}"
                else:
                    query = f"{base_query} WHERE {where_clause}"
            return query, params
        
        return base_query, []
    
    def _is_critical_error(self, error: Exception) -> bool:
        """Check if error is critical and should stop extraction"""
        error_msg = str(error).lower()
        critical_patterns = [
            'out of memory',
            'too many connections',
            'connection limit',
            'max_connections',
            'lost connection',
            'mysql server has gone away'
        ]
        return any(pattern in error_msg for pattern in critical_patterns)
