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
import signal
import sys
import threading
import time
from contextlib import contextmanager

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
        self._table_columns_cache = {}  # Cache all columns per database
        
        # Thread-safe connection management
        self._thread_local = threading.local()
        self._connection_lock = threading.Lock()
        self._active_connections = []  # Track all active connections for cleanup
        self._connection_in_use = {}  # Track which connections are actively in use
        
        # Connection pool settings (from config)
        from src.config import settings
        self._max_connections_per_thread = settings.MAX_CONNECTIONS_PER_THREAD
        self._connection_timeout = 300  # 5 minutes
        self._max_retries = 3
        self._retry_delay = 1  # seconds
        self._global_connection_limit = settings.GLOBAL_CONNECTION_LIMIT
        
        # Shutdown handling
        self._shutdown = False
        self._executors = []  # Track all executors for cleanup
        
        # Register cleanup function to prevent fatal errors
        atexit.register(self._cleanup)
        
        # Register signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
        
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully"""
        self.logger.info(f"Received signal {signum}, initiating graceful shutdown...")
        self._shutdown = True
        self._cleanup()
        sys.exit(0)
    
    def _cleanup(self):
        """Cleanup function to prevent MySQL connector fatal errors"""
        try:
            # Set shutdown flag
            self._shutdown = True
            
            # Shutdown all executors first
            for executor in self._executors:
                try:
                    # Try to cancel pending futures
                    try:
                        executor.shutdown(wait=False, cancel_futures=True)
                    except TypeError:
                        # Fallback for older Python versions
                        executor.shutdown(wait=False)
                except:
                    pass
            
            # Close all active connections with thread safety
            with self._connection_lock:
                for conn_info in self._active_connections[:]:  # Copy list to avoid modification during iteration
                    try:
                        conn = conn_info.get('connection')
                        if conn and hasattr(conn, 'close'):
                            conn.close()
                        self._active_connections.remove(conn_info)
                    except Exception as e:
                        self.logger.debug(f"Error closing connection: {e}")
                        pass
            
            # Clear thread-local storage
            if hasattr(self, '_thread_local'):
                try:
                    # Try to access thread-local connections if they exist
                    if hasattr(self._thread_local, 'connections'):
                        for conn in self._thread_local.connections.values():
                            try:
                                conn.close()
                            except:
                                pass
                        self._thread_local.connections.clear()
                except:
                    pass
            
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
                'identity_mysql_connection_url': settings.IDENTITY_MYSQL_CONNECTION_URL,
                'master_mysql_connection_url': settings.MASTER_MYSQL_CONNECTION_URL,
                'tenant_mysql_connection_url': settings.TENANT_MYSQL_CONNECTION_URL,
                'environment': settings.ENVIRONMENT,
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
    
    def get_connection(self, config: Dict, database: Optional[str] = None):
        """
        Create MySQL connection using thread-safe approach
        
        Args:
            config: Configuration dictionary
            database: Optional database name to connect to
            
        Returns:
            PyMySQL connection object
        """
        # Initialize thread-local storage if needed
        if not hasattr(self._thread_local, 'connections'):
            self._thread_local.connections = {}
            self._thread_local.last_used = {}
        
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
        
        # Create unique key for this connection
        pool_key = f"{parsed.hostname}:{parsed.port}:{database or 'default'}"
        
        # Check thread-local connection pool
        if pool_key in self._thread_local.connections:
            conn = self._thread_local.connections[pool_key]
            last_used = self._thread_local.last_used.get(pool_key, 0)
            
            # Check if connection is still valid and not too old
            if time.time() - last_used < self._connection_timeout:
                try:
                    # Test if connection is still alive
                    conn.ping(reconnect=True)
                    self._thread_local.last_used[pool_key] = time.time()
                    return conn
                except Exception as e:
                    self.logger.debug(f"Connection ping failed: {e}")
                    # Connection is dead, remove it
                    self._close_connection(pool_key)
        
        # Clean up old connections in this thread if we have too many
        if len(self._thread_local.connections) >= self._max_connections_per_thread:
            self._cleanup_thread_connections()
        
        # Create new connection with retry logic
        connection_params = {
            'host': parsed.hostname,
            'port': parsed.port if parsed.port else 3306,
            'user': parsed.username,
            'password': parsed.password,
            'charset': 'utf8mb4',
            'autocommit': True,
            'connect_timeout': 30,
            'read_timeout': 300,  # 5 minutes for large queries
            'write_timeout': 60,  # 1 minute for writes
            'max_allowed_packet': 1024 * 1024 * 64,  # 64MB
        }
        
        # Add database if specified
        if database:
            connection_params['database'] = database
        elif parsed.path and len(parsed.path) > 1:
            connection_params['database'] = parsed.path[1:]
            
        # Check global connection limit before creating new connection
        with self._connection_lock:
            if len(self._active_connections) >= self._global_connection_limit:
                # Try to clean up old connections first
                self._cleanup_old_connections()
                
                # If still over limit, wait and retry
                if len(self._active_connections) >= self._global_connection_limit:
                    self.logger.warning(f"Global connection limit reached ({self._global_connection_limit}). Waiting...")
                    time.sleep(1)
                    
                    # Force cleanup of oldest connections
                    self._force_cleanup_oldest_connections()
        
        # Try to create connection with retries
        for attempt in range(self._max_retries):
            try:
                conn = pymysql.connect(**connection_params)
                
                # Store in thread-local pool
                self._thread_local.connections[pool_key] = conn
                self._thread_local.last_used[pool_key] = time.time()
                
                # Track connection for cleanup
                with self._connection_lock:
                    self._active_connections.append({
                        'connection': conn,
                        'thread_id': threading.current_thread().ident,
                        'pool_key': pool_key,
                        'created_at': time.time()
                    })
                
                return conn
                
            except Exception as e:
                if attempt < self._max_retries - 1:
                    self.logger.warning(f"Connection attempt {attempt + 1} failed: {e}. Retrying...")
                    time.sleep(self._retry_delay * (attempt + 1))  # Exponential backoff
                else:
                    raise
    
    def _close_connection(self, pool_key: str):
        """Close and remove a connection from thread-local pool"""
        if hasattr(self._thread_local, 'connections') and pool_key in self._thread_local.connections:
            try:
                conn = self._thread_local.connections[pool_key]
                conn.close()
                
                # Also remove from global tracking
                with self._connection_lock:
                    self._active_connections = [
                        c for c in self._active_connections 
                        if c.get('connection') != conn
                    ]
            except:
                pass
            finally:
                del self._thread_local.connections[pool_key]
                if pool_key in self._thread_local.last_used:
                    del self._thread_local.last_used[pool_key]
    
    def _cleanup_thread_connections(self):
        """Clean up old connections in current thread"""
        if not hasattr(self._thread_local, 'connections'):
            return
        
        current_time = time.time()
        keys_to_remove = []
        
        # Find connections that are too old
        for pool_key, last_used in self._thread_local.last_used.items():
            if current_time - last_used > self._connection_timeout:
                keys_to_remove.append(pool_key)
        
        # Close and remove old connections
        for pool_key in keys_to_remove:
            self._close_connection(pool_key)
    
    def _cleanup_old_connections(self):
        """Clean up connections older than timeout across all threads"""
        current_time = time.time()
        connections_to_remove = []
        
        for conn_info in self._active_connections[:]:  # Copy to avoid modification during iteration
            if current_time - conn_info['created_at'] > self._connection_timeout:
                connections_to_remove.append(conn_info)
        
        for conn_info in connections_to_remove:
            try:
                conn = conn_info.get('connection')
                if conn and hasattr(conn, 'close'):
                    conn.close()
                self._active_connections.remove(conn_info)
            except Exception as e:
                self.logger.debug(f"Error cleaning up old connection: {e}")
    
    def _force_cleanup_oldest_connections(self, count=5):
        """Force cleanup of oldest connections to free up resources"""
        if not self._active_connections:
            return
        
        # Sort by creation time and remove oldest
        sorted_connections = sorted(self._active_connections, key=lambda x: x['created_at'])
        
        closed_count = 0
        for conn_info in sorted_connections:
            if closed_count >= count:
                break
                
            try:
                conn = conn_info.get('connection')
                if not conn:
                    continue
                    
                # Check if connection is in use
                conn_id = id(conn)
                if conn_id in self._connection_in_use and self._connection_in_use[conn_id]:
                    self.logger.debug(f"Skipping in-use connection from thread {conn_info['thread_id']}")
                    continue
                
                # Close the connection
                if hasattr(conn, 'close'):
                    conn.close()
                self._active_connections.remove(conn_info)
                self.logger.info(f"Force closed connection from thread {conn_info['thread_id']}")
                closed_count += 1
            except Exception as e:
                self.logger.debug(f"Error force closing connection: {e}")
        
        if closed_count < count:
            self.logger.warning(f"Only closed {closed_count} connections out of requested {count} (others in use)")
    
    @contextmanager
    def get_db_connection(self, config: Dict, database: Optional[str] = None):
        """
        Context manager for safe database connection handling
        
        Usage:
            with self.get_db_connection(config, database) as conn:
                cursor = conn.cursor()
                # ... do work ...
        
        Ensures connection is properly handled even if errors occur
        """
        conn = None
        conn_id = None
        try:
            conn = self.get_connection(config, database)
            # Mark connection as in use
            conn_id = id(conn)
            with self._connection_lock:
                self._connection_in_use[conn_id] = True
            yield conn
        except Exception as e:
            # Log but re-raise the exception
            self.logger.error(f"Error during database operation: {e}")
            raise
        finally:
            # Mark connection as not in use
            if conn_id:
                with self._connection_lock:
                    self._connection_in_use.pop(conn_id, None)
            # Don't close the connection here - let thread-local pool manage it
            # This allows connection reuse within the same thread
            pass
    
    def _execute_query_with_retry(self, config: Dict, database: str, query: str, params=None, 
                                   use_dict_cursor: bool = False, max_retries: int = None):
        """
        Execute a query with automatic retry on connection errors
        
        Args:
            config: Connection configuration
            database: Database name
            query: SQL query to execute
            params: Query parameters
            use_dict_cursor: Whether to use DictCursor
            max_retries: Override default max retries
            
        Returns:
            Query results
        """
        if max_retries is None:
            max_retries = self._max_retries
        
        last_error = None
        for attempt in range(max_retries):
            try:
                conn = self.get_connection(config, database)
                
                # Choose cursor type
                if use_dict_cursor:
                    cursor = conn.cursor(pymysql.cursors.DictCursor)
                else:
                    cursor = conn.cursor()
                
                try:
                    # Execute query
                    if params:
                        cursor.execute(query, params)
                    else:
                        cursor.execute(query)
                    
                    # Fetch results if it's a SELECT query
                    if query.strip().upper().startswith('SELECT') or query.strip().upper().startswith('SHOW'):
                        results = cursor.fetchall()
                        return results
                    else:
                        # For non-SELECT queries, return affected rows
                        return cursor.rowcount
                        
                finally:
                    cursor.close()
                    
            except (pymysql.err.OperationalError, pymysql.err.InterfaceError) as e:
                last_error = e
                error_code = getattr(e, 'args', [None])[0]
                
                # Connection errors that should be retried
                if error_code in (2003, 2006, 2013):  # Can't connect, MySQL gone away, Lost connection
                    if attempt < max_retries - 1:
                        self.logger.warning(f"Connection error on attempt {attempt + 1}: {e}. Retrying...")
                        # Remove bad connection from pool
                        pool_key = f"{config.get('host')}:{config.get('port', 3306)}:{database or 'default'}"
                        self._close_connection(pool_key)
                        time.sleep(self._retry_delay * (attempt + 1))
                        continue
                
                # Non-retryable errors or last attempt
                raise
                
            except Exception as e:
                # Non-connection errors, don't retry
                raise
        
        # If we get here, all retries failed
        if last_error:
            raise last_error
        else:
            raise Exception(f"Query failed after {max_retries} attempts")
    
    def _log_connection_stats(self):
        """Log connection pool statistics for monitoring"""
        try:
            with self._connection_lock:
                active_count = len(self._active_connections)
                thread_count = len(set(conn['thread_id'] for conn in self._active_connections))
                
                # Group by thread
                thread_connections = {}
                for conn_info in self._active_connections:
                    thread_id = conn_info['thread_id']
                    if thread_id not in thread_connections:
                        thread_connections[thread_id] = 0
                    thread_connections[thread_id] += 1
                
                # Try to get file descriptor count
                try:
                    import resource
                    soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
                    self.logger.info(f"Connection pool: {active_count} connections, {thread_count} threads | File descriptor limit: {soft}/{hard}")
                except:
                    self.logger.info(f"Connection pool: {active_count} connections across {thread_count} threads")
                
                # Log per-thread stats if there are issues
                if active_count > 15:  # Lowered threshold
                    self.logger.warning(f"High connection count: {active_count}")
                    for thread_id, count in thread_connections.items():
                        if count > 3:
                            self.logger.warning(f"Thread {thread_id} has {count} connections")
                    # Force cleanup when high
                    self._cleanup_old_connections()
        except Exception as e:
            self.logger.debug(f"Error logging connection stats: {e}")
    
    def _get_connection_for_database(self, database_name: str = None) -> tuple:
        """
        Get appropriate connection URL for a database based on environment
        
        Returns:
            Tuple of (connection_config, database_group)
        """
        # For local environment, use single connection URL
        if self.config.get('environment', 'local').lower() != 'production':
            return {'mysql_connection_url': self.config['mysql_connection_url']}, 'all'
        
        # For production, map databases to their specific connection URLs
        if not database_name:
            # Return default for listing all databases
            return {'mysql_connection_url': self.config['mysql_connection_url']}, 'all'
        
        # Database to connection URL mapping for production
        db_lower = database_name.lower()
        
        # Strict mapping in production:
        # - identity connection: ONLY 'identity' database
        # - master connection: ONLY 'master' database  
        # - tenant connection: ALL 'tenant*' databases
        
        if db_lower == 'identity':
            url = self.config.get('identity_mysql_connection_url')
            if url:
                return {'mysql_connection_url': url}, 'identity'
        elif db_lower == 'master':
            url = self.config.get('master_mysql_connection_url')
            if url:
                return {'mysql_connection_url': url}, 'master'
        elif db_lower.startswith('tenant'):
            url = self.config.get('tenant_mysql_connection_url')
            if url:
                return {'mysql_connection_url': url}, 'tenant'
        
        # Default to main connection URL
        return {'mysql_connection_url': self.config['mysql_connection_url']}, 'all'
    
    def list_databases(self) -> List[str]:
        """Get list of databases, excluding system and ignored databases"""
        # For production with separate connections, we need to query each connection
        if self.config.get('environment', 'local').lower() == 'production':
            all_databases = []
            connection_configs = []
            
            # Check which production connections are configured
            if self.config.get('identity_mysql_connection_url'):
                connection_configs.append(({'mysql_connection_url': self.config['identity_mysql_connection_url']}, 'identity'))
            if self.config.get('master_mysql_connection_url'):
                connection_configs.append(({'mysql_connection_url': self.config['master_mysql_connection_url']}, 'master'))
            if self.config.get('tenant_mysql_connection_url'):
                connection_configs.append(({'mysql_connection_url': self.config['tenant_mysql_connection_url']}, 'tenant'))
            
            # If no specific connections configured, fall back to main connection
            if not connection_configs:
                connection_configs.append(({'mysql_connection_url': self.config['mysql_connection_url']}, 'all'))
            
            # Query each connection for its databases
            for conn_config, group in connection_configs:
                conn = None
                cursor = None
                try:
                    conn = self.get_connection(conn_config)
                    cursor = conn.cursor()
                    
                    cursor.execute("SHOW DATABASES")
                    databases = [db[0] for db in cursor.fetchall()]
                    
                    # Filter databases based on connection group in production
                    original_dbs = databases[:]
                    if group == 'identity':
                        # Only extract 'identity' database from identity connection
                        databases = [db for db in databases if db.lower() == 'identity']
                        self.logger.info(f"Identity connection: extracting {databases}")
                    elif group == 'master':
                        # Only extract 'master' database from master connection
                        databases = [db for db in databases if db.lower() == 'master']
                        self.logger.info(f"Master connection: extracting {databases}")
                    elif group == 'tenant':
                        # Extract all tenant databases from tenant connection
                        databases = [db for db in databases if db.lower().startswith('tenant')]
                        self.logger.info(f"Tenant connection: extracting {len(databases)} tenant databases")
                    
                    # Log skipped databases
                    skipped_dbs = set(original_dbs) - set(databases)
                    if skipped_dbs:
                        # Filter out system databases from skipped list
                        system_dbs = ['information_schema', 'mysql', 'performance_schema', 'sys']
                        non_system_skipped = [db for db in skipped_dbs if db not in system_dbs]
                        if non_system_skipped:
                            self.logger.warning(f"{group.capitalize()} connection: skipping non-matching databases: {non_system_skipped}")
                    
                    all_databases.extend(databases)
                    
                except Exception as e:
                    self.logger.warning(f"Could not list databases from {group} connection: {e}")
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
            
            # Remove duplicates
            all_databases = list(set(all_databases))
        else:
            # Local environment - use single connection
            conn = None
            cursor = None
            try:
                conn = self.get_connection(self.config)
                cursor = conn.cursor()
                
                cursor.execute("SHOW DATABASES")
                all_databases = [db[0] for db in cursor.fetchall()]
            finally:
                if cursor:
                    try:
                        cursor.close()
                    except:
                        pass
            
            # Filter out system databases
            system_dbs = ['information_schema', 'mysql', 'performance_schema', 'sys']
            databases = [db for db in all_databases if db not in system_dbs]
            
            # Filter databases by include keywords (if provided)
            include_keywords = self.config['extract_db_keywords'].split(',')
            include_keywords = [kw.strip().lower() for kw in include_keywords if kw.strip()]
            
            if include_keywords:
                # In production, this further filters the already-filtered databases
                original_count = len(databases)
                databases = [
                    db for db in databases 
                    if any(keyword in db.lower() for keyword in include_keywords)
                ]
                if self.config.get('environment', 'local').lower() == 'production' and original_count != len(databases):
                    self.logger.info(f"Keyword filter reduced databases from {original_count} to {len(databases)}")
            
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
            
        # Log final database list in production
        if self.config.get('environment', 'local').lower() == 'production':
            self.logger.info("=" * 60)
            self.logger.info("PRODUCTION DATABASE EXTRACTION PLAN:")
            self.logger.info(f"  - Total databases to extract: {len(databases)}")
            if databases:
                # Group by type
                identity_dbs = [db for db in databases if db.lower() == 'identity']
                master_dbs = [db for db in databases if db.lower() == 'master']
                tenant_dbs = [db for db in databases if db.lower().startswith('tenant')]
                other_dbs = [db for db in databases if db not in identity_dbs + master_dbs + tenant_dbs]
                
                if identity_dbs:
                    self.logger.info(f"  - Identity databases: {identity_dbs}")
                if master_dbs:
                    self.logger.info(f"  - Master databases: {master_dbs}")
                if tenant_dbs:
                    self.logger.info(f"  - Tenant databases ({len(tenant_dbs)}): {', '.join(sorted(tenant_dbs)[:5])}{'...' if len(tenant_dbs) > 5 else ''}")
                if other_dbs:
                    self.logger.info(f"  - Other databases: {other_dbs}")
            self.logger.info("=" * 60)
            
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
            # Get appropriate connection for this database
            conn_config, _ = self._get_connection_for_database(database)
            conn = self.get_connection(conn_config, database)
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
            # Get appropriate connection for this database
            conn_config, _ = self._get_connection_for_database(database)
            conn = self.get_connection(conn_config, database)
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
            # Get appropriate connection for this database
            conn_config, _ = self._get_connection_for_database(database)
            conn = self.get_connection(conn_config, database)
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
        try:
            # Get appropriate connection for this database
            conn_config, _ = self._get_connection_for_database(database)
            
            # Check if table has date column for filtering
            has_date_column, date_column = self._has_date_column(database, table_name)
            
            # Get date filter params to check if filtering is actually enabled
            start_date, end_date = self._get_date_filter_params()
            
            # Build query
            if has_date_column and (start_date or end_date):
                # Use date filtering
                base_query = f"SELECT * FROM {table_name} LIMIT %s OFFSET %s"
                query, params = self._build_date_filter_query(table_name, base_query, date_column)
                # Add LIMIT and OFFSET parameters
                params.extend([self.config['extraction']['batch_size'], offset])
            else:
                # No date filtering - extract all data
                query = f"SELECT * FROM {table_name} LIMIT %s OFFSET %s"
                params = (self.config['extraction']['batch_size'], offset)
            
            # Execute query with retry logic
            results = self._execute_query_with_retry(
                conn_config, 
                database, 
                query, 
                params, 
                use_dict_cursor=True,
                max_retries=3
            )
            
            # PyMySQL already returns proper dictionaries, no conversion needed
            return results if results else []
            
        except Exception as e:
            # Log the specific error type for debugging
            error_type = type(e).__name__
            self.logger.error(f"Error extracting batch from {database}.{table_name} at offset {offset}: [{error_type}] {e}")
            
            # For packet sequence errors, try to recover by clearing thread connections
            if "Packet sequence" in str(e):
                self.logger.warning(f"Packet sequence error detected, clearing thread connections")
                if hasattr(self._thread_local, 'connections'):
                    for key in list(self._thread_local.connections.keys()):
                        self._close_connection(key)
            
            return []
    
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
        
        # Optimize worker count based on table size and batch count
        # Too many workers cause connection overhead and contention
        batch_count = len(offsets)
        if batch_count <= 2:
            optimal_workers = 1  # Sequential is faster for 1-2 batches
        elif batch_count <= 5:
            optimal_workers = 2  # Light parallelism
        elif batch_count <= 10:
            optimal_workers = 2  # Keep low to avoid file descriptor exhaustion
        else:
            # For many batches, use more workers but cap at 3 to avoid contention
            optimal_workers = min(3, self.config['extraction']['workers'])
        
        # Use ThreadPoolExecutor for parallel extraction with proper shutdown handling
        executor = ThreadPoolExecutor(max_workers=optimal_workers)
        self._executors.append(executor)  # Track for cleanup
        
        try:
            # Check for shutdown before starting
            if self._shutdown:
                self.logger.warning("Extraction cancelled due to shutdown")
                return all_data
            
            future_to_offset = {
                executor.submit(self.extract_table_batch, database, table_name, offset): offset 
                for offset in offsets
            }
            
            # Process completed futures with timeout
            for future in as_completed(future_to_offset, timeout=300):  # 5 minute timeout
                # Check for shutdown
                if self._shutdown:
                    self.logger.warning("Extraction interrupted due to shutdown")
                    break
                    
                offset = future_to_offset[future]
                try:
                    data = future.result(timeout=60)  # 1 minute timeout per batch
                    if data:  # Only extend if we got data
                        all_data.extend(data)
                except Exception as e:
                    self.logger.error(f"Failed to extract batch at offset {offset}: {e}")
        except Exception as e:
            self.logger.error(f"Error during parallel extraction: {e}")
        finally:
            # Ensure executor is properly shut down
            try:
                # Python 3.9+ has cancel_futures parameter
                executor.shutdown(wait=True, cancel_futures=True)
            except TypeError:
                # Fallback for older Python versions
                executor.shutdown(wait=True)
            # Remove from tracked executors
            if executor in self._executors:
                self._executors.remove(executor)
        
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
            # Check for shutdown
            if self._shutdown:
                self.logger.warning(f"Table extraction interrupted due to shutdown at {database}.{table}")
                break
                
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
            # Optimize database worker count to avoid connection pool exhaustion
            # Too many concurrent database connections cause contention and file descriptor exhaustion
            configured_db_workers = self.config.get('extraction', {}).get('db_workers', 10)
            if len(databases) <= 10:
                max_db_workers = min(2, len(databases))  # Very light parallelism
            elif len(databases) <= 20:
                max_db_workers = 3  # Reduced parallelism
            else:
                max_db_workers = min(4, configured_db_workers)  # Cap at 4 for many DBs
            
            # Progress tracking (log every 20%)
            total_dbs = len(databases)
            last_logged_pct = 0
            
            executor = ThreadPoolExecutor(max_workers=max_db_workers)
            self._executors.append(executor)  # Track for cleanup
            
            try:
                # Check for shutdown before starting
                if self._shutdown:
                    self.logger.warning("Database extraction cancelled due to shutdown")
                    return consolidated_data
                
                future_to_db = {
                    executor.submit(self.extract_database_to_dict, db, table_names): db
                    for db in databases
                }
                
                completed = 0
                total_records_so_far = 0
                
                # Process with timeout to prevent hanging
                for future in as_completed(future_to_db, timeout=600):  # 10 minute timeout for all DBs
                    # Check for shutdown
                    if self._shutdown:
                        self.logger.warning("Database extraction interrupted due to shutdown")
                        break
                        
                    database = future_to_db[future]
                    completed += 1
                    
                    try:
                        database_tables = future.result(timeout=120)  # 2 minute timeout per database
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
                        
                        # Log connection stats periodically
                        self._log_connection_stats()
            except Exception as e:
                self.logger.error(f"Error during parallel database extraction: {e}")
            finally:
                # Ensure executor is properly shut down
                try:
                    # Python 3.9+ has cancel_futures parameter
                    executor.shutdown(wait=True, cancel_futures=True)
                except TypeError:
                    # Fallback for older Python versions
                    executor.shutdown(wait=True)
                # Remove from tracked executors
                if executor in self._executors:
                    self._executors.remove(executor)
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
        
        # Write without indentation for faster I/O (compact JSON)
        with open(filepath, 'w') as f:
            json.dump(consolidated_data, f, default=str, separators=(',', ':'))
        
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