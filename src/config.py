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
    ENVIRONMENT: str = os.getenv('ENVIRONMENT', 'local')
    
    # Database Connection URLs
    MYSQL_CONNECTION_URL: str = os.getenv('MYSQL_CONNECTION_URL')
    SNOWFLAKE_CONNECTION_URL: str = os.getenv('SNOWFLAKE_CONNECTION_URL')
    SQLITE_CONNECTION_URL: str = os.getenv('SQLITE_CONNECTION_URL')
    
    # Data Store Configuration
    DATA_STORE: str = os.getenv('DATA_STORE', 'sqlite')
    
    # ETL Configuration
    BATCH_SIZE: int = int(os.getenv('BATCH_SIZE', '1000'))
    PARALLEL_WORKERS: int = int(os.getenv('PARALLEL_WORKERS', '4'))
    LOG_LEVEL: str = os.getenv('LOG_LEVEL', 'INFO')
    
    # Directory Configuration
    OUTPUT_DIR: str = os.getenv('OUTPUT_DIR', 'output')
    LOG_DIR: str = os.getenv('LOG_DIR', 'logs')
    TRANSFORMED_OUTPUT_DIR: str = os.getenv('TRANSFORMED_OUTPUT_DIR', 'output/transformations')
    
    # Extraction Configuration
    EXTRACTION_WORKERS: int = int(os.getenv('EXTRACTION_WORKERS', '10'))
    EXTRACTION_BATCH_SIZE: int = int(os.getenv('EXTRACTION_BATCH_SIZE', '5000'))
    EXTRACT_TABLES: str = os.getenv('EXTRACT_TABLES', '')
    EXTRACT_DB_KEYWORDS: str = os.getenv('EXTRACT_DB_KEYWORDS', '')
    
    # Date Filtering Configuration
    EXTRACT_START_DATE: str = os.getenv('EXTRACT_START_DATE', '')
    EXTRACT_END_DATE: str = os.getenv('EXTRACT_END_DATE', '')
    EXTRACT_DAYS_COUNT: str = os.getenv('EXTRACT_DAYS_COUNT', '')
    
    # Transformation Configuration
    TRANSFORMATION_WORKERS: int = int(os.getenv('TRANSFORMATION_WORKERS', '10'))
    TRANSFORMATION_BATCH_SIZE: int = int(os.getenv('TRANSFORMATION_BATCH_SIZE', '5000'))
    TRANSFORMATION_TIMEOUT: int = int(os.getenv('TRANSFORMATION_TIMEOUT', '300'))
    
    # Database Connection Configuration
    CONNECTION_TIMEOUT: int = int(os.getenv('CONNECTION_TIMEOUT', '30'))
    CONNECTION_RETRY_COUNT: int = int(os.getenv('CONNECTION_RETRY_COUNT', '3'))
    CONNECTION_RETRY_DELAY: int = int(os.getenv('CONNECTION_RETRY_DELAY', '5'))
    
    # Performance Configuration
    MAX_MEMORY_USAGE: int = int(os.getenv('MAX_MEMORY_USAGE', '80'))
    CLEANUP_TEMP_FILES: bool = os.getenv('CLEANUP_TEMP_FILES', 'true').lower() == 'true'
    ENABLE_CONCURRENT_PROCESSING: bool = os.getenv('ENABLE_CONCURRENT_PROCESSING', 'true').lower() == 'true'
    
    # Snowflake Optimization Configuration
    SNOWFLAKE_COPY_THRESHOLD: int = int(os.getenv('SNOWFLAKE_COPY_THRESHOLD', '500'))
    
    # Loading Strategy Configuration
    LOAD_STRATEGY: str = os.getenv('LOAD_STRATEGY', 'continue_on_error')  # Options: continue_on_error, fail_fast
    
    # Incremental Loading Configuration
    AUTO_UPDATE_START_DATE: bool = os.getenv('AUTO_UPDATE_START_DATE', 'true').lower() == 'true'
    EXTRACT_CHECKPOINT_FILE: str = os.getenv('EXTRACT_CHECKPOINT_FILE', '.extract_checkpoint')
    
    # Notification Configuration
    ENABLE_NOTIFICATIONS: bool = os.getenv('ENABLE_NOTIFICATIONS', 'false').lower() == 'true'
    SLACK_WEBHOOK_URL: str = os.getenv('SLACK_WEBHOOK_URL', '')
    NOTIFICATION_ON_SUCCESS: bool = os.getenv('NOTIFICATION_ON_SUCCESS', 'false').lower() == 'true'
    NOTIFICATION_ON_FAILURE: bool = os.getenv('NOTIFICATION_ON_FAILURE', 'true').lower() == 'true'
    NOTIFICATION_ON_PARTIAL: bool = os.getenv('NOTIFICATION_ON_PARTIAL', 'true').lower() == 'true'
    
    # Scheduling Configuration
    ETL_SCHEDULE_CRON: str = os.getenv('ETL_SCHEDULE_CRON', '0 2 * * *')
    RUN_ON_STARTUP: bool = os.getenv('RUN_ON_STARTUP', 'false').lower() == 'true'
    
    # Derived paths
    EXTRACTED_OUTPUT_DIR: str = os.path.join(os.getenv('OUTPUT_DIR', 'output'), 'extracted')
    
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