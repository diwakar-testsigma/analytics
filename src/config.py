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
    IDENTITY_MYSQL_CONNECTION_URL: str = os.getenv('IDENTITY_MYSQL_CONNECTION_URL')
    MASTER_MYSQL_CONNECTION_URL: str = os.getenv('MASTER_MYSQL_CONNECTION_URL')
    TENANT_MYSQL_CONNECTION_URL: str = os.getenv('TENANT_MYSQL_CONNECTION_URL')
    SNOWFLAKE_CONNECTION_URL: str = os.getenv('SNOWFLAKE_CONNECTION_URL')
    SQLITE_CONNECTION_URL: str = os.getenv('SQLITE_CONNECTION_URL')
    
    # Data Store Configuration
    DATA_STORE: str = os.getenv('DATA_STORE')
    
    # ETL Configuration
    BATCH_SIZE: int = int(os.getenv('BATCH_SIZE'))
    PARALLEL_WORKERS: int = int(os.getenv('PARALLEL_WORKERS'))
    LOG_LEVEL: str = os.getenv('LOG_LEVEL')
    
    # Directory Configuration
    OUTPUT_DIR: str = os.getenv('OUTPUT_DIR')
    LOG_DIR: str = os.getenv('LOG_DIR')
    TRANSFORMED_OUTPUT_DIR: str = os.getenv('TRANSFORMED_OUTPUT_DIR')
    
    # Extraction Configuration
    EXTRACTION_WORKERS: int = int(os.getenv('EXTRACTION_WORKERS'))
    EXTRACTION_BATCH_SIZE: int = int(os.getenv('EXTRACTION_BATCH_SIZE'))
    EXTRACTION_DB_WORKERS: int = int(os.getenv('EXTRACTION_DB_WORKERS'))
    EXTRACT_TABLES: str = os.getenv('EXTRACT_TABLES')
    EXTRACT_DB_KEYWORDS: str = os.getenv('EXTRACT_DB_KEYWORDS')
    EXTRACT_DB_EXCLUDE_KEYWORDS: str = os.getenv('EXTRACT_DB_EXCLUDE_KEYWORDS')
    
    # Date Filtering Configuration (strip inline comments)
    EXTRACT_DATE: str = (os.getenv('EXTRACT_DATE') or '').split('#')[0].strip()
    EXTRACT_DIRECTION: str = (os.getenv('EXTRACT_DIRECTION') or '').split('#')[0].strip()
    EXTRACT_DAYS_COUNT: str = (os.getenv('EXTRACT_DAYS_COUNT') or '').split('#')[0].strip()
    EXTRACT_HOURS_COUNT: str = (os.getenv('EXTRACT_HOURS_COUNT') or '').split('#')[0].strip()
    
    # Skip Configuration
    SKIP_EXTRACTION: bool = (os.getenv('SKIP_EXTRACTION') or '').lower() == 'true'
    SKIP_TRANSFORMATION: bool = (os.getenv('SKIP_TRANSFORMATION') or '').lower() == 'true'
    
    # Transformation Configuration
    TRANSFORMATION_WORKERS: int = int(os.getenv('TRANSFORMATION_WORKERS'))
    TRANSFORMATION_BATCH_SIZE: int = int(os.getenv('TRANSFORMATION_BATCH_SIZE'))
    TRANSFORMATION_TIMEOUT: int = int(os.getenv('TRANSFORMATION_TIMEOUT'))
    ENABLE_STREAMING_TRANSFORMATION: bool = os.getenv('ENABLE_STREAMING_TRANSFORMATION', 'true').lower() == 'true'
    
    # Database Connection Configuration
    CONNECTION_TIMEOUT: int = int(os.getenv('CONNECTION_TIMEOUT'))
    CONNECTION_RETRY_COUNT: int = int(os.getenv('CONNECTION_RETRY_COUNT'))
    CONNECTION_RETRY_DELAY: int = int(os.getenv('CONNECTION_RETRY_DELAY'))
    
    # Performance Configuration
    MAX_MEMORY_USAGE: int = int(os.getenv('MAX_MEMORY_USAGE'))
    CLEANUP_TEMP_FILES: bool = os.getenv('CLEANUP_TEMP_FILES').lower() == 'true'
    ENABLE_CONCURRENT_PROCESSING: bool = os.getenv('ENABLE_CONCURRENT_PROCESSING').lower() == 'true'
    
    # Compression Configuration
    ENABLE_COMPRESSION: bool = os.getenv('ENABLE_COMPRESSION', 'true').lower() == 'true'
    COMPRESSION_LEVEL: int = int(os.getenv('COMPRESSION_LEVEL', '6'))
    
    # Snowflake Optimization Configuration
    SNOWFLAKE_COPY_THRESHOLD: int = int(os.getenv('SNOWFLAKE_COPY_THRESHOLD'))
    
    # Loading Strategy Configuration
    LOAD_STRATEGY: str = os.getenv('LOAD_STRATEGY')
    
    # Notification Configuration
    ENABLE_NOTIFICATIONS: bool = os.getenv('ENABLE_NOTIFICATIONS').lower() == 'true'
    SLACK_WEBHOOK_URL: str = os.getenv('SLACK_WEBHOOK_URL')
    NOTIFICATION_ON_SUCCESS: bool = os.getenv('NOTIFICATION_ON_SUCCESS').lower() == 'true'
    NOTIFICATION_ON_FAILURE: bool = os.getenv('NOTIFICATION_ON_FAILURE').lower() == 'true'
    NOTIFICATION_ON_PARTIAL: bool = os.getenv('NOTIFICATION_ON_PARTIAL').lower() == 'true'
    
    # Scheduling Configuration
    ETL_SCHEDULE_CRON: str = os.getenv('ETL_SCHEDULE_CRON')
    RUN_ON_STARTUP: bool = os.getenv('RUN_ON_STARTUP').lower() == 'true'
    
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