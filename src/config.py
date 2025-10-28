"""
Configuration management for ETL pipeline

This module handles all configuration loading from environment variables.
"""

import os
from typing import Optional, List
from dataclasses import dataclass, field
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()


@dataclass
class Settings:
    """Application settings loaded from environment variables"""
    
    # Environment
    ENVIRONMENT: str = os.getenv('ENVIRONMENT')
    
    # Database Connection URLs
    SNOWFLAKE_CONNECTION_URL: str = os.getenv('SNOWFLAKE_CONNECTION_URL')
    SQLITE_CONNECTION_URL: str = os.getenv('SQLITE_CONNECTION_URL')
    
    # MySQL Connection URLs (for separate databases)
    IDENTITY_MYSQL_CONNECTION_URL: str = os.getenv('IDENTITY_MYSQL_CONNECTION_URL')
    MASTER_MYSQL_CONNECTION_URL: str = os.getenv('MASTER_MYSQL_CONNECTION_URL')
    TENANT_MYSQL_CONNECTION_URL: str = os.getenv('TENANT_MYSQL_CONNECTION_URL')
    
    # Data Store Configuration
    DATA_STORE: str = os.getenv('DATA_STORE')
    
    # Logging Configuration
    LOG_LEVEL: str = os.getenv('LOG_LEVEL')
    
    # Directory Configuration
    OUTPUT_DIR: str = os.getenv('OUTPUT_DIR')
    LOG_DIR: str = os.getenv('LOG_DIR')
    TRANSFORMED_OUTPUT_DIR: str = os.getenv('TRANSFORMED_OUTPUT_DIR')
    
    # Extraction Configuration
    EXTRACT_TABLES: str = os.getenv('EXTRACT_TABLES')  # Comma-separated list of tables
    EXTRACT_DB_KEYWORDS: str = os.getenv('EXTRACT_DB_KEYWORDS')
    EXTRACT_DB_EXCLUDE_KEYWORDS: str = os.getenv('EXTRACT_DB_EXCLUDE_KEYWORDS')
    
    # Memory Protection (IMPORTANT - prevents EC2 crashes)
    ENABLE_MEMORY_LIMIT: bool = os.getenv('ENABLE_MEMORY_LIMIT', 'false').lower() == 'true'
    MEMORY_LIMIT_PERCENT: int = int(os.getenv('MEMORY_LIMIT_PERCENT', '50'))  # % of system RAM
    
    # Skip Configuration
    SKIP_EXTRACTION: bool = (os.getenv('SKIP_EXTRACTION') or '').lower() == 'true'
    
    # Database Connection
    CONNECTION_TIMEOUT: int = int(os.getenv('CONNECTION_TIMEOUT', '30'))
    
    # Workers (reduced for safety)
    EXTRACTION_WORKERS: int = int(os.getenv('EXTRACTION_WORKERS', '1'))  # Single threaded for safety
    TRANSFORMATION_WORKERS: int = int(os.getenv('TRANSFORMATION_WORKERS', '1'))  # Single threaded for safety
    
    # Snowflake Settings
    SNOWFLAKE_COPY_THRESHOLD: int = int(os.getenv('SNOWFLAKE_COPY_THRESHOLD', '10000'))
    LOAD_STRATEGY: str = os.getenv('LOAD_STRATEGY', 'bulk')
    
    # Derived paths
    EXTRACTED_OUTPUT_DIR: str = os.path.join(os.getenv('OUTPUT_DIR'), 'extracted')
    
    def __post_init__(self):
        """Create necessary directories after initialization"""
        # Create directories
        for directory in [self.OUTPUT_DIR, self.LOG_DIR, self.EXTRACTED_OUTPUT_DIR, self.TRANSFORMED_OUTPUT_DIR]:
            os.makedirs(directory, exist_ok=True)
    
    @property
    def is_local(self) -> bool:
        """Check if running in local environment"""
        return self.ENVIRONMENT == 'local'
    
    @property
    def is_production(self) -> bool:
        """Check if running in production environment"""
        return self.ENVIRONMENT == 'production'


# Create a singleton instance
settings = Settings()