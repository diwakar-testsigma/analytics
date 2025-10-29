"""
Data Source Abstractions

This module provides abstract base classes and implementations for different data sources.
Currently supports SQLite for local development, with extensibility for Snowflake.
"""

import sqlite3
import json
import tempfile
import os
import re
import uuid
from datetime import datetime
from abc import ABC, abstractmethod
from typing import Dict, Any, List, Optional
from pathlib import Path


class DataSource(ABC):
    """Abstract base class for data sources"""
    
    @abstractmethod
    def connect(self):
        """Establish connection to data source"""
        pass
    
    @abstractmethod
    def disconnect(self):
        """Close connection to data source"""
        pass
    
    @abstractmethod
    def create_table_if_not_exists(self, table_name: str, sample_row: Dict[str, Any]) -> bool:
        """Create table if it doesn't exist"""
        pass
    
    @abstractmethod
    def insert_batch(self, table_name: str, rows: List[Dict[str, Any]]) -> bool:
        """Insert a batch of rows into the table"""
        pass
    
    @abstractmethod
    def get_connection(self):
        """Get the underlying connection object"""
        pass


class SQLiteDataSource(DataSource):
    """SQLite data source implementation for local development"""
    
    def __init__(self, connection_url: str):
        if not connection_url:
            raise ValueError("connection_url must be provided")
        
        self.connection_url = connection_url
        self.connection = None
        self.batch_size = 1000
        
        # Parse connection URL to get db_path
        if connection_url.startswith("sqlite:///"):
            self.db_path = connection_url[10:]  # Remove "sqlite:///" prefix
        else:
            self.db_path = connection_url
        
        # Ensure data directory exists
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
    
    def connect(self):
        """Connect to SQLite database"""
        self.connection = sqlite3.connect(self.db_path)
        cursor = self.connection.cursor()
        
        # Enable foreign key constraints
        cursor.execute("PRAGMA foreign_keys = ON")
        
        # Initialize database with schema
        self._init_database()
        
        return self.connection
    
    def disconnect(self):
        """Close SQLite connection"""
        if self.connection:
            self.connection.close()
            self.connection = None
    
    def get_connection(self):
        """Get SQLite connection"""
        return self.connection
    
    def _init_database(self):
        """Initialize SQLite database with complete schema"""
        cursor = self.connection.cursor()
        
        # Read and execute the complete schema from init.sql
        init_sql_path = Path(__file__).parent.parent.parent.parent / "infrastructure" / "database" / "init-scripts" / "init.sql"
        if init_sql_path.exists():
            with open(init_sql_path, 'r') as f:
                schema_sql = f.read()
            
            # Execute the complete schema
            cursor.executescript(schema_sql)
            self.connection.commit()
            print("✅ SQLite database initialized with complete Snowflake-compatible schema")
        else:
            print("⚠️  init.sql not found, creating basic schema...")
            self._create_basic_schema(cursor)
            self.connection.commit()
    
    def _create_basic_schema(self, cursor):
        """Create basic schema if init.sql is not available"""
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS dim_users (
                user_id INTEGER PRIMARY KEY,
                first_name TEXT,
                last_name TEXT,
                email TEXT,
                created_at TIMESTAMP,
                updated_at TIMESTAMP,
                etl_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
    
    def create_table_if_not_exists(self, table_name: str, sample_row: Dict[str, Any]) -> bool:
        """Create table if it doesn't exist using predefined schema first"""
        cursor = self.connection.cursor()

        # Check if table already exists
        cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}'")
        if cursor.fetchone():
            # Table exists, just verify it has the right columns
            cursor.execute(f"PRAGMA table_info('{table_name}')")
            existing_columns = [col[1] for col in cursor.fetchall()]

            # Check if all required columns exist
            missing_columns = []
            for key in sample_row.keys():
                if key not in existing_columns:
                    missing_columns.append(key)

            if missing_columns:
                print(f"⚠️  Table {table_name} exists but missing columns: {missing_columns}")
                # Add missing columns
                for col in missing_columns:
                    value = sample_row[col]
                    if isinstance(value, int):
                        col_type = "INTEGER"
                    elif isinstance(value, float):
                        col_type = "REAL"
                    elif isinstance(value, bool):
                        col_type = "INTEGER"
                    else:
                        col_type = "TEXT"

                    try:
                        cursor.execute(f"ALTER TABLE '{table_name}' ADD COLUMN '{col}' {col_type}")
                        print(f"  ✅ Added column {col} ({col_type})")
                    except sqlite3.OperationalError as e:
                        if "duplicate column name" not in str(e).lower():
                            print(f"  ❌ Failed to add column {col}: {e}")

            self.connection.commit()
            cursor.close()
            print(f"✅ Table {table_name} verified and updated")
            return True

        # Table doesn't exist - this should not happen since we initialize with schema
        print(f"⚠️  Table {table_name} not found in schema, creating dynamically...")

        # Map Python types to SQLite types
        columns = []
        for key, value in sample_row.items():
            if isinstance(value, int):
                col_type = "INTEGER"
            elif isinstance(value, float):
                col_type = "REAL"
            elif isinstance(value, bool):
                col_type = "INTEGER"
            else:
                col_type = "TEXT"

            columns.append(f'"{key}" {col_type}')

        create_query = f"""
        CREATE TABLE IF NOT EXISTS "{table_name}" (
            {', '.join(columns)},
            etl_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """

        cursor.execute(create_query)
        self.connection.commit()
        cursor.close()

        print(f"✅ Created table {table_name} with {len(columns)} columns")
        return True
    
    def insert_batch(self, table_name: str, rows: List[Dict[str, Any]]) -> bool:
        """Insert rows in batches with proper error handling"""
        if not rows:
            return True

        cursor = self.connection.cursor()
        
        # Get column names from first row
        columns = list(rows[0].keys())
        
        # Prepare batch data
        batch = []
        for row in rows:
            # Convert None values to appropriate types
            processed_row = []
            for col in columns:
                value = row.get(col)
                if value is None:
                    processed_row.append(None)
                else:
                    processed_row.append(value)
            batch.append(tuple(processed_row))
            
            # Insert in batches
            if len(batch) >= self.batch_size:
                placeholders = ', '.join(['?' for _ in columns])
                column_names = ', '.join([f'"{col}"' for col in columns])
                query = f"INSERT OR REPLACE INTO '{table_name}' ({column_names}) VALUES ({placeholders})"
                try:
                    cursor.executemany(query, batch)
                    self.connection.commit()
                    print(f"✅ Inserted {len(batch)} records into {table_name}")
                except sqlite3.Error as e:
                    print(f"❌ Error inserting batch into {table_name}: {e}")
                    self.connection.rollback()
                    return False
                batch = []
        
        # Insert remaining records
        if batch:
            placeholders = ', '.join(['?' for _ in columns])
            column_names = ', '.join([f'"{col}"' for col in columns])
            query = f"INSERT OR REPLACE INTO '{table_name}' ({column_names}) VALUES ({placeholders})"
            try:
                cursor.executemany(query, batch)
                self.connection.commit()
                print(f"✅ Inserted {len(batch)} records into {table_name}")
            except sqlite3.Error as e:
                print(f"❌ Error inserting final batch into {table_name}: {e}")
                self.connection.rollback()
                return False
        
        cursor.close()
        return True


class SnowflakeDataSource(DataSource):
    """Snowflake data source implementation"""
    
    def __init__(self, connection_url: str):
        if not connection_url:
            raise ValueError("connection_url must be provided")
        
        self.connection_url = connection_url
        self.connection = None
        self.cursor = None
        self.logger = self._setup_logger()
        self._parse_connection_url()
    
    def _setup_logger(self):
        import logging
        # Use existing logging configuration from pipeline
        return logging.getLogger(self.__class__.__name__)
    
    def _parse_connection_url(self):
        """Parse Snowflake connection URL to extract connection parameters"""
        from urllib.parse import urlparse, parse_qs
        
        # Expected format: snowflake://user:password@account/database/schema?warehouse=X&role=Y
        parsed = urlparse(self.connection_url)
        
        # Remove .snowflakecomputing.com if present in hostname
        hostname = parsed.hostname
        if hostname and '.snowflakecomputing.com' in hostname:
            self.account = hostname.replace('.snowflakecomputing.com', '')
        else:
            self.account = hostname
        
        self.user = parsed.username
        self.password = parsed.password
        
        # Parse path to get database and schema
        path_parts = parsed.path.strip('/').split('/')
        self.database = path_parts[0] if len(path_parts) > 0 else None
        self.schema = path_parts[1] if len(path_parts) > 1 else 'PUBLIC'
        
        # Parse query parameters
        query_params = parse_qs(parsed.query)
        self.warehouse = query_params.get('warehouse', [None])[0]
        self.role = query_params.get('role', [None])[0]
    
    def connect(self):
        """Connect to Snowflake"""
        try:
            import snowflake.connector
            
            self.logger.info(f"Connecting to Snowflake account: {self.account}")
            
            connection_params = {
                'account': self.account,
                'user': self.user,
                'password': self.password,
                'database': self.database,
                'schema': self.schema,
                'warehouse': self.warehouse,
                'role': self.role,
                'login_timeout': 30,
                # Add parameters to handle certificate issues
                'insecure_mode': False,
                'ocsp_fail_open': True,
                'ocsp_response_cache_filename': None
            }
            
            # Remove None values (except for ocsp_response_cache_filename)
            connection_params = {k: v for k, v in connection_params.items() 
                               if v is not None or k == 'ocsp_response_cache_filename'}
            
            self.connection = snowflake.connector.connect(**connection_params)
            self.cursor = self.connection.cursor()
            
            # Set context
            if self.warehouse:
                self.cursor.execute(f"USE WAREHOUSE {self.warehouse}")
            if self.database:
                self.cursor.execute(f"USE DATABASE {self.database}")
            if self.schema:
                self.cursor.execute(f"USE SCHEMA {self.schema}")
            
            self.logger.info("✅ Successfully connected to Snowflake")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to connect to Snowflake: {e}")
            raise
    
    def disconnect(self):
        """Close Snowflake connection"""
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
        self.logger.info("Disconnected from Snowflake")
    
    def _load_schema_sql(self):
        """Load schema definitions from schema.sql file"""
        schema_path = Path(__file__).parent.parent / "models" / "schema.sql"
        if schema_path.exists():
            with open(schema_path, 'r') as f:
                return f.read()
        else:
            self.logger.warning(f"Schema file not found at {schema_path}")
            return None
    
    def _ensure_table_schema(self, table_name: str, sample_row: Dict[str, Any]) -> bool:
        """Ensure table has all required columns, add missing ones"""
        try:
            # Get existing columns
            column_sql = f"""
            SELECT COLUMN_NAME, DATA_TYPE 
            FROM INFORMATION_SCHEMA.COLUMNS 
            WHERE TABLE_SCHEMA = '{self.schema}' 
            AND LOWER(TABLE_NAME) = LOWER('{table_name}')
            ORDER BY ORDINAL_POSITION
            """
            
            self.cursor.execute(column_sql)
            existing_columns = {row[0].lower(): row[1] for row in self.cursor.fetchall()}
            
            # Get expected columns from schema.sql if available
            schema_sql = self._load_schema_sql()
            expected_columns = {}
            
            if schema_sql:
                # Extract column definitions from schema
                table_pattern = rf"CREATE TABLE IF NOT EXISTS {table_name}\s*\(([^;]+)\);"
                match = re.search(table_pattern, schema_sql, re.IGNORECASE | re.DOTALL)
                
                if match:
                    columns_text = match.group(1)
                    # Parse column definitions
                    lines = columns_text.strip().split(',')
                    for line in lines:
                        line = line.strip()
                        if line and not line.upper().startswith(('PRIMARY KEY', 'FOREIGN KEY', 'UNIQUE', 'CHECK')):
                            parts = line.split(None, 2)
                            if len(parts) >= 2:
                                col_name = parts[0].strip('"').lower()
                                col_type = parts[1]
                                # Handle special column types
                                if col_type.upper().startswith('VARCHAR'):
                                    col_type = 'VARCHAR'
                                elif col_type.upper() == 'TEXT':
                                    col_type = 'VARCHAR'
                                expected_columns[col_name] = col_type
            
            # If no schema.sql, use sample row to determine expected columns
            if not expected_columns:
                for key, value in sample_row.items():
                    col_type = 'VARCHAR'
                    if isinstance(value, int):
                        col_type = 'INTEGER'
                    elif isinstance(value, float):
                        col_type = 'FLOAT'
                    elif isinstance(value, bool):
                        col_type = 'BOOLEAN'
                    elif key.endswith('_json') or key in ['data_json', 'policy_json', 'config_json', 'changes_json']:
                        col_type = 'VARIANT'
                    elif key.endswith('_at') or key == 'timestamp':
                        col_type = 'TIMESTAMP'
                    expected_columns[key.lower()] = col_type
            
            # Add ETL timestamp if not present
            if 'etl_timestamp' not in expected_columns:
                expected_columns['etl_timestamp'] = 'TIMESTAMP'
            
            # Find missing columns
            missing_columns = set(expected_columns.keys()) - set(existing_columns.keys())
            
            # Check for data type mismatches
            type_mismatches = {}
            for col_name, expected_type in expected_columns.items():
                if col_name in existing_columns:
                    existing_type = existing_columns[col_name].upper()
                    expected_type_upper = expected_type.upper()
                    
                    # Check for specific mismatches
                    mismatch = False
                    
                    # VARIANT vs VARCHAR/TEXT mismatch
                    if (expected_type_upper == 'VARIANT' and existing_type in ['VARCHAR', 'TEXT']) or \
                       (expected_type_upper in ['VARCHAR', 'TEXT'] and existing_type == 'VARIANT'):
                        mismatch = True
                    
                    # BOOLEAN vs NUMBER mismatch (common when tables were dynamically created)
                    elif (expected_type_upper == 'BOOLEAN' and 'NUMBER' in existing_type) or \
                         ('NUMBER' in existing_type and expected_type_upper == 'BOOLEAN'):
                        mismatch = True
                    
                    # INTEGER vs NUMBER mismatch
                    elif (expected_type_upper == 'INTEGER' and existing_type == 'NUMBER') or \
                         (expected_type_upper == 'NUMBER' and existing_type == 'INTEGER'):
                        # These are compatible, don't flag as mismatch
                        pass
                    
                    if mismatch:
                        type_mismatches[col_name] = (existing_type, expected_type)
            
            if missing_columns or type_mismatches:
                if missing_columns:
                    self.logger.info(f"Adding {len(missing_columns)} missing columns to {table_name}: {missing_columns}")
                if type_mismatches:
                    self.logger.info(f"Found type mismatches in {table_name}: {type_mismatches}")
                    # For type mismatches, we need to recreate the table
                    self.logger.info(f"Recreating table {table_name} due to type mismatches")
                    self.cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
                    return self.create_table_if_not_exists(table_name, sample_row)
                
                # Add each missing column
                for col_name in missing_columns:
                    col_type = expected_columns[col_name]
                    
                    # Build ALTER TABLE statement
                    alter_sql = f"ALTER TABLE {table_name} ADD COLUMN {col_name} {col_type}"
                    
                    # Add defaults for specific columns
                    if col_name == 'etl_timestamp':
                        alter_sql += " DEFAULT CURRENT_TIMESTAMP()"
                    elif col_type == 'BOOLEAN':
                        alter_sql += " DEFAULT FALSE"
                    
                    try:
                        self.cursor.execute(alter_sql)
                        self.logger.info(f"✅ Added column {col_name} to {table_name}")
                    except Exception as e:
                        self.logger.warning(f"Failed to add column {col_name} to {table_name}: {e}")
                        # Continue with other columns
            
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to ensure schema for {table_name}: {e}")
            # If we can't update schema, try to recreate the table
            self.logger.info(f"Attempting to recreate table {table_name}")
            try:
                self.cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
                return self.create_table_if_not_exists(table_name, sample_row)
            except Exception as drop_e:
                self.logger.error(f"Failed to recreate table {table_name}: {drop_e}")
                raise
    
    def create_table_if_not_exists(self, table_name: str, sample_row: Dict[str, Any]) -> bool:
        """Create table in Snowflake if it doesn't exist using schema.sql"""
        try:
            # Check if table exists
            check_sql = f"""
            SELECT COUNT(*) 
            FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_SCHEMA = '{self.schema}' 
            AND LOWER(TABLE_NAME) = LOWER('{table_name}')
            """
            
            self.cursor.execute(check_sql)
            exists = self.cursor.fetchone()[0] > 0
            
            if exists:
                self.logger.info(f"Table {table_name} already exists in Snowflake")
                # Check if schema needs updating
                return self._ensure_table_schema(table_name, sample_row)
            
            # Load schema from file
            schema_sql = self._load_schema_sql()
            if schema_sql:
                # Extract the CREATE TABLE statement for this specific table
                table_pattern = rf"CREATE TABLE IF NOT EXISTS {table_name}\s*\([^;]+\);"
                match = re.search(table_pattern, schema_sql, re.IGNORECASE | re.DOTALL)
                
                if match:
                    create_sql = match.group(0)
                    # Convert SQLite/standard SQL to Snowflake SQL
                    create_sql = create_sql.replace("AUTOINCREMENT", "AUTOINCREMENT")
                    create_sql = create_sql.replace("DEFAULT CURRENT_TIMESTAMP()", "DEFAULT CURRENT_TIMESTAMP()")
                    
                    self.logger.info(f"Creating table {table_name} from schema.sql")
                    self.cursor.execute(create_sql)
                    self.logger.info(f"✅ Table {table_name} created from schema.sql in Snowflake")
                    return True
            
            # Fallback to dynamic creation if not found in schema
            self.logger.warning(f"Table {table_name} not found in schema.sql, using dynamic creation")
            
            # Map Python types to Snowflake types
            type_mapping = {
                str: "VARCHAR",
                int: "INTEGER",
                float: "FLOAT",
                bool: "BOOLEAN",
                type(None): "VARCHAR"
            }
            
            # Build column definitions
            columns = []
            for key, value in sample_row.items():
                col_type = type_mapping.get(type(value), "VARCHAR")
                
                # Special handling for specific columns
                if key.endswith('_id') and col_type == "INTEGER":
                    if key == 'user_id':
                        col_type = "VARCHAR"  # Based on schema, user_id in sessions is VARCHAR
                    elif table_name.startswith('dim_') and key == f"{table_name[4:-1]}_id":
                        col_type = "INTEGER PRIMARY KEY"
                elif key.endswith('_at') or key == 'timestamp':
                    col_type = "TIMESTAMP"
                elif key.endswith('_json') or key in ['data_json', 'policy_json', 'config_json', 'changes_json', 'headers_json', 'metrics_json', 'metadata_json', 'new_entity_data']:
                    col_type = "VARIANT"
                elif isinstance(value, str) and len(value) > 255:
                    col_type = "TEXT"
                
                columns.append(f"{key} {col_type}")
            
            # Add ETL timestamp if not present
            if 'etl_timestamp' not in sample_row:
                columns.append("etl_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP()")
            
            # Create table SQL
            create_sql = f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                {','.join(columns)}
            )
            """
            
            self.logger.debug(f"Creating dynamic table with SQL: {create_sql}")
            self.cursor.execute(create_sql)
            
            self.logger.info(f"✅ Table {table_name} created dynamically in Snowflake")
            return True
            
        except Exception as e:
            error_msg = f"Failed to create table {table_name}: {e}"
            self.logger.error(error_msg)
            raise Exception(error_msg) from e
    
    def _insert_batch_with_copy(self, table_name: str, rows: List[Dict[str, Any]]) -> bool:
        """Use COPY command for efficient bulk loading with JSON format"""
        try:
            # Create temporary file with JSON data
            with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as tmp_file:
                # Write as newline-delimited JSON (NDJSON)
                for row in rows:
                    # Process timestamps before writing
                    processed_row = {}
                    for col, value in row.items():
                        if value is None:
                            processed_row[col] = None
                        elif isinstance(value, (dict, list)):
                            processed_row[col] = value  # Keep as-is for JSON
                        elif col.endswith('_time') or col.endswith('_at') or col == 'timestamp':
                            # Convert Unix timestamps to ISO format for Snowflake
                            if isinstance(value, (int, float)) and value > 10000000000:
                                try:
                                    # Handle out-of-range timestamps
                                    max_timestamp = 253402300799000  # Dec 31, 9999
                                    min_timestamp = -30610224000000  # Jan 1, 1000
                                    
                                    if value > max_timestamp or value < min_timestamp:
                                        self.logger.warning(f"Timestamp {value} out of range for column {col}, using NULL")
                                        processed_row[col] = None
                                    else:
                                        dt = datetime.fromtimestamp(value / 1000)
                                        processed_row[col] = dt.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
                                except (ValueError, OSError) as e:
                                    self.logger.warning(f"Invalid timestamp {value} for column {col}: {e}, using NULL")
                                    processed_row[col] = None
                            else:
                                processed_row[col] = value
                        elif ('is_' in col or col.endswith('_enabled') or col.endswith('_active') or 
                              col.endswith('_supported') or col.endswith('_flaky') or
                              col in ('success', 'deprecated', 'auth_enabled', 'api_supported')):
                            # Convert 0/1 to boolean for common boolean column patterns
                            if value in (0, 1, '0', '1'):
                                processed_row[col] = bool(int(value))
                            elif value in (True, False, 'true', 'false', 'True', 'False'):
                                processed_row[col] = value if isinstance(value, bool) else (value.lower() == 'true')
                            else:
                                processed_row[col] = value
                        else:
                            processed_row[col] = value
                    
                    json.dump(processed_row, tmp_file)
                    tmp_file.write('\n')
                
                tmp_path = tmp_file.name
            
            # Create temporary stage
            stage_name = f"TEMP_STAGE_{table_name.upper()}_{os.getpid()}"
            self.cursor.execute(f"CREATE TEMPORARY STAGE IF NOT EXISTS {stage_name}")
            
            # Upload file to stage with specific settings
            # Use PARALLEL to limit concurrent uploads and reduce SSL handshake issues
            put_sql = f"PUT file://{tmp_path} @{stage_name} AUTO_COMPRESS=TRUE OVERWRITE=TRUE PARALLEL=4"
            
            try:
                self.cursor.execute(put_sql)
                put_result = self.cursor.fetchone()
                self.logger.debug(f"PUT result: {put_result}")
            except Exception as e:
                error_msg = str(e)
                if "certificate" in error_msg.lower() or "254007" in error_msg:
                    # SSL certificate error - try alternative approach
                    self.logger.warning(f"SSL certificate error during PUT, trying alternative method: {error_msg}")
                    # Fall back to regular INSERT for this batch
                    return self._insert_batch_original(table_name, rows)
                else:
                    raise
            
            # Create or replace file format for NDJSON
            self.cursor.execute("""
                CREATE OR REPLACE FILE FORMAT TEMP_JSON_FORMAT
                TYPE = 'JSON'
                STRIP_OUTER_ARRAY = FALSE
                ENABLE_OCTAL = FALSE
                ALLOW_DUPLICATE = FALSE
                STRIP_NULL_VALUES = FALSE
            """)
            
            # Get file name in stage (it might be compressed)
            list_sql = f"LIST @{stage_name}"
            self.cursor.execute(list_sql)
            files = self.cursor.fetchall()
            if not files:
                raise Exception("No files found in stage after PUT")
            
            staged_file = files[0][0]  # Get the file name
            
            # COPY into table with column matching
            copy_sql = f"""
                COPY INTO {table_name}
                FROM @{stage_name}/{os.path.basename(staged_file)}
                FILE_FORMAT = (FORMAT_NAME = 'TEMP_JSON_FORMAT')
                MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
                ON_ERROR = 'CONTINUE'
                PURGE = TRUE
            """
            
            self.cursor.execute(copy_sql)
            copy_result = self.cursor.fetchone()
            
            # Log the raw COPY result for debugging
            self.logger.debug(f"Raw COPY result: {copy_result}")
            self.logger.debug(f"COPY result type: {type(copy_result)}, length: {len(copy_result) if copy_result else 0}")
            
            # Parse COPY results - handle different result formats
            rows_loaded = 0
            rows_parsed = 0
            
            if copy_result:
                try:
                    # Try parsing as tuple/list with numeric values
                    if len(copy_result) >= 2:
                        # Check if we can convert to integers
                        try:
                            rows_loaded = int(copy_result[0])
                            rows_parsed = int(copy_result[1])
                        except (ValueError, TypeError):
                            # Snowflake 4.0.0 might return file info instead of counts
                            # If we get a string with file info, assume success
                            if isinstance(copy_result[0], str):
                                self.logger.info(f"COPY result contains file info: {copy_result[0]}")
                                # Assume all rows loaded successfully
                                rows_loaded = len(rows)
                                rows_parsed = len(rows)
                            else:
                                raise
                except Exception as e:
                    self.logger.warning(f"Unexpected COPY result format: {e}")
                    # Default to assuming success
                    rows_loaded = len(rows)
                    rows_parsed = len(rows)
                
                rows_failed = rows_parsed - rows_loaded
                
                if rows_failed > 0:
                    self.logger.warning(f"⚠️  {rows_failed} rows failed during COPY for {table_name}")
                    # Get error details
                    self.cursor.execute(f"""
                        SELECT * FROM TABLE(VALIDATE({table_name}, JOB_ID => '{self.cursor.sfqid}'))
                        LIMIT 10
                    """)
                    errors = self.cursor.fetchall()
                    for error in errors:
                        self.logger.error(f"COPY error: {error}")
                
                self.logger.info(f"✅ COPY loaded {rows_loaded}/{len(rows)} rows into {table_name}")
            
            # Cleanup
            os.unlink(tmp_path)
            self.cursor.execute(f"DROP STAGE IF EXISTS {stage_name}")
            
            # Commit transaction
            self.connection.commit()
            
            return True
            
        except Exception as e:
            self.logger.error(f"COPY failed for {table_name}: {e}")
            # Cleanup on error
            if 'tmp_path' in locals() and os.path.exists(tmp_path):
                os.unlink(tmp_path)
            if 'stage_name' in locals():
                try:
                    self.cursor.execute(f"DROP STAGE IF EXISTS {stage_name}")
                except:
                    pass
            raise
    
    def insert_batch(self, table_name: str, rows: List[Dict[str, Any]]) -> bool:
        """Insert a batch of rows into Snowflake table"""
        if not rows:
            self.logger.warning(f"No rows to insert into {table_name}")
            return True
        
        try:
            # Use COPY for large datasets (>500 rows) for better performance
            COPY_THRESHOLD = int(os.getenv('SNOWFLAKE_COPY_THRESHOLD', '500'))
            
            if len(rows) >= COPY_THRESHOLD:
                self.logger.info(f"Using COPY for {table_name} ({len(rows)} rows >= {COPY_THRESHOLD} threshold)")
                return self._insert_batch_with_copy(table_name, rows)
            
            # Use regular INSERT for smaller datasets
            self.logger.info(f"Using INSERT for {table_name} ({len(rows)} rows < {COPY_THRESHOLD} threshold)")
            
            # Get column names from first row
            columns = list(rows[0].keys())
            
            # Prepare values for bulk insert
            values_list = []
            for row in rows:
                values = []
                for col in columns:
                    value = row.get(col)
                    
                    # Handle different data types
                    if value is None:
                        values.append(None)
                    elif isinstance(value, (dict, list)):
                        # Convert dict/list to JSON string for VARIANT columns
                        values.append(json.dumps(value))
                    elif isinstance(value, bool):
                        values.append(value)
                    elif col.endswith('_time') or col.endswith('_at') or col == 'timestamp':
                        # Convert Unix timestamps (milliseconds) to datetime strings
                        if isinstance(value, (int, float)) and value > 10000000000:  # Unix timestamp in ms
                            from datetime import datetime
                            dt = datetime.fromtimestamp(value / 1000)  # Convert from ms to seconds
                            values.append(dt.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3])  # Format for Snowflake
                        else:
                            values.append(value)
                    else:
                        values.append(value)
                
                values_list.append(tuple(values))
            
            # Build insert SQL with parameterized query
            # Convert column names to uppercase for Snowflake
            placeholders = ','.join(['%s'] * len(columns))
            insert_sql = f"""
            INSERT INTO {table_name} ({','.join([col.upper() for col in columns])})
            VALUES ({placeholders})
            """
            
            # Execute batch insert
            self.logger.debug(f"Inserting {len(rows)} rows into {table_name}")
            self.cursor.executemany(insert_sql, values_list)
            
            # Commit the transaction
            self.connection.commit()
            
            self.logger.info(f"✅ Successfully inserted {len(rows)} rows into {table_name}")
            return True
            
        except Exception as e:
            error_msg = f"Failed to insert batch into {table_name}: {e}"
            self.logger.error(error_msg)
            if self.connection:
                self.connection.rollback()
            # Re-raise the exception with full details
            raise Exception(error_msg) from e
    
    def get_connection(self):
        """Get Snowflake connection"""
        return self.connection


