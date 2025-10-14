"""
Data Source Abstractions

This module provides abstract base classes and implementations for different data sources.
Currently supports SQLite for local development, with extensibility for Snowflake.
"""

import sqlite3
import json
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
                'login_timeout': 30
            }
            
            # Remove None values
            connection_params = {k: v for k, v in connection_params.items() if v is not None}
            
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
    
    def create_table_if_not_exists(self, table_name: str, sample_row: Dict[str, Any]) -> bool:
        """Create table in Snowflake if it doesn't exist"""
        try:
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
                elif key.endswith('_json') or key in ['data_json', 'policy_json', 'config_json', 'changes_json', 'headers_json', 'metrics_json', 'metadata_json']:
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
            
            self.logger.debug(f"Creating table with SQL: {create_sql}")
            self.cursor.execute(create_sql)
            
            self.logger.info(f"✅ Table {table_name} ready in Snowflake")
            return True
            
        except Exception as e:
            error_msg = f"Failed to create table {table_name}: {e}"
            self.logger.error(error_msg)
            raise Exception(error_msg) from e
    
    def insert_batch(self, table_name: str, rows: List[Dict[str, Any]]) -> bool:
        """Insert a batch of rows into Snowflake table"""
        if not rows:
            self.logger.warning(f"No rows to insert into {table_name}")
            return True
        
        try:
            # First, truncate existing data (optional - remove if you want to append)
            # self.cursor.execute(f"TRUNCATE TABLE {table_name}")
            
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
            placeholders = ','.join(['%s'] * len(columns))
            insert_sql = f"""
            INSERT INTO {table_name} ({','.join(columns)})
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


