"""
Main ETL Pipeline for Analytics Data Processing

This module orchestrates the Extract, Transform, and Load process
for analytics data from MySQL to Snowflake/SQLite.
"""

import json
import logging
import gzip
import ijson
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from src.extractors.extractor import DataExtractor
from src.loaders.loader import DataLoader
from src.transformers.direct_snowflake_transformer import transform_to_snowflake
from src.config import settings
from src.notifications import notifier
from src.utils.env_updater import update_extraction_state, reset_skip_flags, update_transformation_state


class Pipeline:
    """Main ETL pipeline orchestrator"""
    
    def __init__(self, extraction_start_date: Optional[str] = None, 
                 extracted_file: Optional[str] = None,
                 transformed_file: Optional[str] = None):
        """
        Initialize ETL Pipeline
        
        Args:
            extraction_start_date: Optional override for extraction start date (YYYY-MM-DD)
            extracted_file: Optional specific extracted file to use when SKIP_EXTRACTION=true
            transformed_file: Optional specific transformed file to use when SKIP_TRANSFORMATION=true
        """
        self.config = settings
        self.extraction_start_date_override = extraction_start_date
        self.extracted_file_override = extracted_file
        self.transformed_file_override = transformed_file
        self.logger = self._setup_logging()
        self.metrics = self._initialize_metrics()
        self.job_id = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.logger.info(f"Initializing ETL Pipeline in {self.config.ENVIRONMENT} mode")
        self.logger.info(f"Job ID: {self.job_id}")
        if extraction_start_date:
            self.logger.info(f"Extraction start date override: {extraction_start_date}")
        if extracted_file:
            self.logger.info(f"Extracted file override: {extracted_file}")
        if transformed_file:
            self.logger.info(f"Transformed file override: {transformed_file}")
        
    def _setup_logging(self) -> logging.Logger:
        """Configure logging for the ETL pipeline"""
        # Configure root logger to capture all logs
        root_logger = logging.getLogger()
        root_logger.setLevel(getattr(logging, self.config.LOG_LEVEL or 'INFO'))
        
        # Clear existing handlers to avoid duplicates
        root_logger.handlers.clear()
        
        # Common formatter
        log_format = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - [%(funcName)s:%(lineno)d] - %(message)s'
        )
        
        # Console handler
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(log_format)
        root_logger.addHandler(console_handler)
        
        # File handler for all ETL logs
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        log_dir = Path(self.config.LOG_DIR)
        log_dir.mkdir(exist_ok=True)
        
        log_file = log_dir / f"etl_pipeline_{timestamp}.log"
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(log_format)
        root_logger.addHandler(file_handler)
        
        # Return pipeline-specific logger
        return logging.getLogger(__name__)
    
    def _initialize_metrics(self) -> Dict:
        """Initialize metrics tracking dictionary"""
        return {
            'start_time': None,
            'end_time': None,
            'duration_seconds': None,
            'extraction': {
                'records_extracted': 0,
                'tables_extracted': []
            },
            'transformation': {
                'records_transformed': 0,
                'tables_transformed': []
            },
            'loading': {
                'records_loaded': 0,
                'tables_loaded': [],
                'tables_loaded_count': 0,
                'failed_tables': []
            },
            'errors': [],
            'success': False
        }
    
    def extract(self) -> str:
        """
        Extract data from source databases
        
        Returns:
            Path to consolidated extracted data file
        """
        # Check if extraction should be skipped
        if settings.SKIP_EXTRACTION:
            self.logger.info("=" * 60)
            self.logger.info("EXTRACTION PHASE SKIPPED (SKIP_EXTRACTION=true)")
            self.logger.info("=" * 60)
            
            if self.extracted_file_override:
                # Use the specified file
                output_dir = Path(self.config.OUTPUT_DIR) / "extracted"
                specified_file = output_dir / self.extracted_file_override
                
                if not specified_file.exists():
                    # Also check if it's an absolute path
                    specified_file = Path(self.extracted_file_override)
                    if not specified_file.exists():
                        raise FileNotFoundError(f"Specified extracted file not found: {self.extracted_file_override}")
                
                latest_file = specified_file
                self.logger.info(f"Using specified extracted file: {latest_file}")
            else:
                # Find the latest extracted file
                output_dir = Path(self.config.OUTPUT_DIR) / "extracted"
                extracted_files = list(output_dir.glob("extracted_data_*.json"))
                
                if not extracted_files:
                    raise FileNotFoundError("No extracted files found to skip extraction")
                
                # Get the most recent file
                latest_file = max(extracted_files, key=lambda p: p.stat().st_mtime)
                self.logger.info(f"Using latest extracted file: {latest_file}")
            
            # Skip metrics for large files when skipping extraction
            file_size_mb = latest_file.stat().st_size / (1024 * 1024)
            if file_size_mb > 1000:  # Skip metrics for files > 1GB
                self.logger.info(f"Skipping metrics for large file ({file_size_mb:.0f}MB)")
                self.metrics['extraction']['records_extracted'] = -1  # Unknown
                self.metrics['extraction']['tables_extracted'] = []
            else:
                # Update metrics for smaller files
                table_counts, total_records = self._get_file_metrics_streaming(str(latest_file))
                self.metrics['extraction']['records_extracted'] = total_records
                self.metrics['extraction']['tables_extracted'] = list(table_counts.keys())
            
            return str(latest_file)
        
        self.logger.info("=" * 60)
        self.logger.info("EXTRACTION PHASE STARTED")
        self.logger.info("=" * 60)
        
        extraction_start = datetime.now()
        
        try:
            self.logger.info("MySQL Connections:")
            self.logger.info(f"  - Identity: {self.config.IDENTITY_MYSQL_CONNECTION_URL}")
            self.logger.info(f"  - Master: {self.config.MASTER_MYSQL_CONNECTION_URL}")
            self.logger.info(f"  - Tenant: {self.config.TENANT_MYSQL_CONNECTION_URL}")
            self.logger.info(f"DB Keywords Filter: {self.config.EXTRACT_DB_KEYWORDS}")
            
            # Create custom config if we have an override
            custom_config = None
            if self.extraction_start_date_override:
                custom_config = {
                    'extraction_start_date_override': self.extraction_start_date_override
                }
                self.logger.info(f"Using extraction start date override: {self.extraction_start_date_override}")
            
            extractor = DataExtractor(custom_config)
            
            # Extract from all configured databases
            self.logger.info("Initiating database extraction...")
            extracted_file = extractor.extract_all_databases()
            
            # Update metrics without loading entire file
            table_counts, total_records = self._get_file_metrics_streaming(extracted_file)
            self.metrics['extraction']['records_extracted'] = total_records
            self.metrics['extraction']['tables_extracted'] = list(table_counts.keys())
            
            # Count databases
            databases = {}
            for table_key in table_counts.keys():
                db_name = table_key.split('.')[0]
                if db_name not in databases:
                    databases[db_name] = {'tables': 0, 'records': 0}
                databases[db_name]['tables'] += 1
                databases[db_name]['records'] += table_counts[table_key]
            
            self.logger.info(f"Successfully extracted data from {len(databases)} databases")
            
            for db_name, db_stats in databases.items():
                self.logger.info(f"  - Database '{db_name}': {db_stats['tables']} tables, {db_stats['records']:,} records")
            
            extraction_time = (datetime.now() - extraction_start).total_seconds()
            
            self.logger.info("=" * 60)
            self.logger.info(
                f"EXTRACTION COMPLETED in {extraction_time:.2f}s: "
                f"{self.metrics['extraction']['records_extracted']:,} records "
                f"from {len(self.metrics['extraction']['tables_extracted'])} tables"
            )
            self.logger.info(f"Output file: {extracted_file}")
            self.logger.info("=" * 60)
            
            # Update extraction state in .env (but don't set skip_extraction yet)
            extraction_timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            # Keep the current direction from settings
            current_direction = settings.EXTRACT_DIRECTION or ''
            update_extraction_state(extraction_timestamp, current_direction, skip_extraction=False)
            self.logger.info(f"💾 Updated .env: EXTRACT_DATE={extraction_timestamp}, EXTRACT_DIRECTION={current_direction}")
            
            return extracted_file
            
        except Exception as e:
            error_msg = f"Extraction failed: {str(e)}"
            self.logger.error(error_msg)
            self.logger.exception("Detailed extraction error:")
            self.metrics['errors'].append(error_msg)
            raise
    
    def transform(self, extracted_file: str) -> str:
        """
        Preprocess joins and transform extracted data to match target schema
        
        Note: When SKIP_TRANSFORMATION=true, both preprocessing and transformation are skipped
        
        Args:
            extracted_file: Path to extracted data file
            
        Returns:
            Path to transformed data file
        """
        # Check if transformation should be skipped
        if settings.SKIP_TRANSFORMATION:
            self.logger.info("=" * 60)
            self.logger.info("PREPROCESSING AND TRANSFORMATION PHASES SKIPPED (SKIP_TRANSFORMATION=true)")
            self.logger.info("=" * 60)
            
            if self.transformed_file_override:
                # Use the specified file
                output_dir = Path(self.config.TRANSFORMED_OUTPUT_DIR)
                specified_file = output_dir / self.transformed_file_override
                
                if not specified_file.exists():
                    # Also check if it's an absolute path
                    specified_file = Path(self.transformed_file_override)
                    if not specified_file.exists():
                        raise FileNotFoundError(f"Specified transformed file not found: {self.transformed_file_override}")
                
                latest_file = specified_file
                self.logger.info(f"Using specified transformed file: {latest_file}")
            else:
                # Find the latest transformed file
                output_dir = Path(self.config.TRANSFORMED_OUTPUT_DIR)
                transformed_files = list(output_dir.glob("snowflake_data_*.json*"))
                
                if not transformed_files:
                    raise FileNotFoundError("No transformed files found to skip transformation")
                
                # Get the most recent file
                latest_file = max(transformed_files, key=lambda p: p.stat().st_mtime)
                self.logger.info(f"Using latest transformed file: {latest_file}")
            
            # Skip metrics for large files when skipping transformation
            file_size_mb = latest_file.stat().st_size / (1024 * 1024)
            if file_size_mb > 100:  # Skip metrics for files > 100MB
                self.logger.info(f"Skipping metrics for large file ({file_size_mb:.0f}MB)")
                self.metrics['transformation']['records_transformed'] = -1  # Unknown
                self.metrics['transformation']['tables_transformed'] = []
            else:
                # Update metrics for smaller files
                table_counts, total_records = self._get_file_metrics_streaming(str(latest_file))
                self.metrics['transformation']['records_transformed'] = total_records
                self.metrics['transformation']['tables_transformed'] = list(table_counts.keys())
            
            return str(latest_file)
        
        self.logger.info("=" * 60)
        self.logger.info("PREPROCESSING AND TRANSFORMATION PHASE STARTED")
        self.logger.info("=" * 60)
        
        transformation_start = datetime.now()
        
        try:
            self.logger.info(f"Input file: {extracted_file}")
            
            # Direct transformation to Snowflake format
            self.logger.info("-" * 60) 
            self.logger.info("Transforming directly to Snowflake format...")
            
            # Transform with consistent timestamp
            transformed_file = transform_to_snowflake(extracted_file, timestamp=self.job_id)
            
            # Update metrics without loading entire file
            table_counts, total_records = self._get_file_metrics_streaming(transformed_file)
            
            self.logger.info(f"Successfully transformed {len(table_counts)} tables:")
            
            for table_name, record_count in table_counts.items():
                self.metrics['transformation']['records_transformed'] += record_count
                self.metrics['transformation']['tables_transformed'].append(table_name)
                self.logger.info(f"  - {table_name}: {record_count:,} records")
            
            transformation_time = (datetime.now() - transformation_start).total_seconds()
            
            self.logger.info("=" * 60)
            self.logger.info(
                f"PREPROCESSING AND TRANSFORMATION COMPLETED in {transformation_time:.2f}s: "
                f"{self.metrics['transformation']['records_transformed']:,} records "
                f"in {len(self.metrics['transformation']['tables_transformed'])} tables"
            )
            self.logger.info(f"Transformed file: {transformed_file}")
            self.logger.info("=" * 60)
            
            return transformed_file
            
        except Exception as e:
            error_msg = f"Preprocessing/Transformation failed: {str(e)}"
            self.logger.error(error_msg)
            self.logger.exception("Detailed preprocessing/transformation error:")
            self.metrics['errors'].append(error_msg)
            
            transformation_time = (datetime.now() - transformation_start).total_seconds()
            self.logger.error("=" * 60)
            self.logger.error(
                f"PREPROCESSING/TRANSFORMATION PHASE FAILED after {transformation_time:.2f}s"
            )
            self.logger.error("=" * 60)
            raise
    
    def load(self, transformed_file: str) -> bool:
        """
        Load transformed data to target database
        
        Args:
            transformed_file: Path to transformed data file
            
        Returns:
            True if successful, False otherwise
        """
        self.logger.info("=" * 60)
        self.logger.info("LOADING PHASE STARTED")
        self.logger.info("=" * 60)
        
        loading_start = datetime.now()
        
        try:
            self.logger.info(f"Input file: {transformed_file}")
            
            # Log based on actual DATA_STORE configuration
            data_store = self.config.DATA_STORE
            self.logger.info(f"Target: {data_store.upper()}")
            
            if data_store == 'snowflake':
                self.logger.info(f"Snowflake Connection: {self.config.SNOWFLAKE_CONNECTION_URL}")
            else:
                self.logger.info(f"SQLite Connection: {self.config.SQLITE_CONNECTION_URL}")
            
            loader = DataLoader()
            
            # Load the data
            self.logger.info("Initiating data load...")
            result = loader.load(transformed_file)
            
            # Handle backward compatibility - if result is boolean
            if isinstance(result, bool):
                success = result
                if success:
                    # Old behavior - update from file metrics without loading it
                    table_counts, total_records = self._get_file_metrics_streaming(transformed_file)
                    self.metrics['loading']['records_loaded'] = total_records
                    self.metrics['loading']['tables_loaded'] = list(table_counts.keys())
            else:
                # New behavior - use detailed result
                success = result['success']
                self.metrics['loading']['records_loaded'] = result['total_records']
                self.metrics['loading']['tables_loaded_count'] = result['loaded_tables']
                self.metrics['loading']['failed_tables'] = result['failed_tables']
                
                # Log detailed results
                if result['failed_tables']:
                    self.logger.warning(f"Loading completed with {len(result['failed_tables'])} failed tables")
                
            loading_time = (datetime.now() - loading_start).total_seconds()
            
            self.logger.info("=" * 60)
            self.logger.info(
                f"LOADING PHASE {'COMPLETED' if success else 'FAILED'} in {loading_time:.2f}s"
            )
            if isinstance(result, dict):
                self.logger.info(f"Tables loaded: {result['loaded_tables']}")
                self.logger.info(f"Tables failed: {len(result['failed_tables'])}")
                self.logger.info(f"Records loaded: {result['total_records']:,}")
            self.logger.info("=" * 60)
            
            if not success:
                raise Exception("Loading failed - check logs for details")
            
            return success
            
        except Exception as e:
            # Log the full error details
            self.logger.error("=" * 60)
            self.logger.error("LOADING PHASE FAILED")
            self.logger.error("=" * 60)
            self.logger.error(f"Error: {str(e)}")
            self.logger.error(f"Error Type: {type(e).__name__}")
            self.logger.error("Full error details:")
            self.logger.exception(e)  # This logs the full traceback
            self.logger.error("=" * 60)
            
            # Store detailed error in metrics
            error_details = {
                'phase': 'loading',
                'error': str(e),
                'error_type': type(e).__name__,
                'timestamp': datetime.now().isoformat()
            }
            self.metrics['errors'].append(error_details)
            
            raise
    
    def run(self) -> bool:
        """
        Run the complete ETL pipeline
        
        Returns:
            True if successful, False otherwise
        """
        self.logger.info("#" * 60)
        self.logger.info("ETL PIPELINE STARTED")
        self.logger.info(f"Environment: {self.config.ENVIRONMENT}")
        self.logger.info(f"Start Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        self.logger.info("#" * 60)
        
        self.metrics['start_time'] = datetime.now()
        
        # Send start notification
        notifier.notify_etl_started(self.job_id)
        
        try:
            # Extract
            extracted_file = self.extract()
            self.metrics['extraction']['success'] = True
            
            # Transform
            transformed_file = self.transform(extracted_file)
            self.metrics['transformation']['success'] = True
            
            # Load
            success = self.load(transformed_file)
            self.metrics['success'] = success
            
            # If everything succeeded, reset skip flags
            if success:
                reset_skip_flags()
                self.logger.info("✅ Reset SKIP_EXTRACTION=false and SKIP_TRANSFORMATION=false for next run")
            
            self.metrics['end_time'] = datetime.now()
            self.metrics['duration_seconds'] = (
                self.metrics['end_time'] - self.metrics['start_time']
            ).total_seconds()
            
            self._save_metrics()
            
            self.logger.info("#" * 60)
            self.logger.info(
                f"ETL PIPELINE COMPLETED SUCCESSFULLY in {self.metrics['duration_seconds']:.2f} seconds"
            )
            self.logger.info(f"Total Records Processed: {self.metrics['loading']['records_loaded']:,}")
            self.logger.info(f"End Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            self.logger.info("#" * 60)
            
            # Send completion notification
            notifier.notify_etl_completed(self.job_id, self.metrics)
            
            return success
            
        except Exception as e:
            self.metrics['end_time'] = datetime.now()
            self.metrics['duration_seconds'] = (
                self.metrics['end_time'] - self.metrics['start_time']
            ).total_seconds()
            
            # Mark phases that didn't succeed
            if 'extraction' not in self.metrics or not self.metrics['extraction'].get('success'):
                self.metrics['extraction']['success'] = False
            if 'transformation' not in self.metrics or not self.metrics['transformation'].get('success'):
                self.metrics['transformation']['success'] = False
            
            # If extraction succeeded but something else failed, set SKIP_EXTRACTION=true
            if self.metrics.get('extraction', {}).get('success', False):
                # Extraction succeeded but transformation or loading failed
                current_direction = settings.EXTRACT_DIRECTION or ''
                extraction_timestamp = settings.EXTRACT_DATE or datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                update_extraction_state(extraction_timestamp, current_direction, skip_extraction=True)
                self.logger.info("⚠️ Extraction succeeded but pipeline failed - setting SKIP_EXTRACTION=true for retry")
            
            # If preprocessing/transformation succeeded but loading failed, set SKIP_TRANSFORMATION=true
            if self.metrics.get('transformation', {}).get('success', False) and not self.metrics.get('success', False):
                # Preprocessing/Transformation succeeded but loading failed
                update_transformation_state(skip_transformation=True)
                self.logger.info("⚠️ Preprocessing/Transformation succeeded but loading failed - setting SKIP_TRANSFORMATION=true for retry")
            
            self._save_metrics()
            
            self.logger.error("#" * 60)
            self.logger.error(f"ETL PIPELINE FAILED: {str(e)}")
            self.logger.error(f"Duration: {self.metrics['duration_seconds']:.2f} seconds")
            self.logger.error(f"Errors: {len(self.metrics['errors'])}")
            self.logger.error("#" * 60)
            
            # Send failure notification
            notifier.notify_etl_completed(self.job_id, self.metrics)
            
            return False
    
    def run_from_file(self, source_file: str) -> bool:
        """
        Run pipeline starting from an already extracted file
        
        Args:
            source_file: Path to source data file
            
        Returns:
            True if successful, False otherwise
        """
        self.logger.info(f"Starting ETL pipeline from file: {source_file}")
        self.metrics['start_time'] = datetime.now()
        
        # Send start notification
        notifier.notify_etl_started(self.job_id)
        
        try:
            # Check if file needs transformation by peeking at structure
            is_transformed = False
            
            if source_file.endswith('.gz'):
                f = gzip.open(source_file, 'rb')
            else:
                f = open(source_file, 'rb')
            
            with f:
                # Just check first few events to see if it has 'tables' key
                parser = ijson.parse(f)
                for i, (prefix, event, value) in enumerate(parser):
                    if event == 'map_key' and prefix == '' and value == 'tables':
                        is_transformed = True
                        break
                    if i > 10:  # Don't check too many events
                        break
            
            # If file has 'tables' key, it's already transformed
            if is_transformed:
                transformed_file = source_file
                self.metrics['transformation']['success'] = True
            else:
                # Transform the file
                transformed_file = self.transform(source_file)
                self.metrics['transformation']['success'] = True
            
            # Mark extraction as skipped but successful (using existing file)
            self.metrics['extraction']['success'] = True
            
            # Load
            success = self.load(transformed_file)
            self.metrics['success'] = success
            
            self.metrics['end_time'] = datetime.now()
            self.metrics['duration_seconds'] = (
                self.metrics['end_time'] - self.metrics['start_time']
            ).total_seconds()
            
            self._save_metrics()
            
            # Send completion notification
            notifier.notify_etl_completed(self.job_id, self.metrics)
            
            return success
            
        except Exception as e:
            self.metrics['end_time'] = datetime.now()
            self.metrics['duration_seconds'] = (
                self.metrics['end_time'] - self.metrics['start_time']
            ).total_seconds()
            
            # Mark failure
            self.metrics['success'] = False
            if 'transformation' not in self.metrics or not self.metrics['transformation'].get('success'):
                self.metrics['transformation']['success'] = False
            
            # Add error to metrics
            self.metrics['errors'].append({
                'phase': 'loading' if 'tables' in locals() else 'transformation',
                'error': str(e),
                'error_type': type(e).__name__,
                'timestamp': datetime.now().isoformat()
            })
            
            self._save_metrics()
            
            self.logger.error(f"ETL pipeline from file failed: {str(e)}")
            
            # Send failure notification
            notifier.notify_etl_completed(self.job_id, self.metrics)
            
            return False
    
    def _get_file_metrics_streaming(self, filepath: str) -> Tuple[Dict[str, int], int]:
        """Get file metrics without loading entire file into memory
        
        Returns:
            Tuple of (table_counts, total_records)
        """
        table_counts = {}
        total_records = 0
        
        try:
            # Check if file has extraction or transformation structure
            if filepath.endswith('.gz'):
                f = gzip.open(filepath, 'rb')
            else:
                f = open(filepath, 'rb')
            
            with f:
                parser = ijson.parse(f)
                
                # Detect file type by looking for 'tables' key
                for prefix, event, value in parser:
                    if event == 'map_key' and prefix == '' and value == 'tables':
                        # Transformed file structure
                        return self._count_transformed_records(filepath)
                    elif event == 'map_key' and prefix == '' and value != 'extraction_metadata':
                        # Extracted file structure
                        return self._count_extracted_records(filepath)
            
        except Exception as e:
            self.logger.warning(f"Could not get metrics for {filepath}: {e}")
        
        return table_counts, total_records
    
    def _count_extracted_records(self, filepath: str) -> Tuple[Dict[str, int], int]:
        """Count records in extracted file format"""
        table_counts = {}
        total_records = 0
        
        if filepath.endswith('.gz'):
            f = gzip.open(filepath, 'rb')
        else:
            f = open(filepath, 'rb')
        
        with f:
            parser = ijson.parse(f)
            for prefix, event, value in parser:
                # Look for "records" field at database.table.records
                if event == 'number' and prefix.endswith('.records'):
                    parts = prefix.split('.')
                    if len(parts) >= 3:  # database.table.records
                        table_key = f"{parts[0]}.{parts[1]}"
                        table_counts[table_key] = value
                        total_records += value
        
        return table_counts, total_records
    
    def _count_transformed_records(self, filepath: str) -> Tuple[Dict[str, int], int]:
        """Count records in transformed file by streaming array lengths"""
        table_counts = {}
        total_records = 0
        current_table = None
        current_count = 0
        
        if filepath.endswith('.gz'):
            f = gzip.open(filepath, 'rb')
        else:
            f = open(filepath, 'rb')
        
        with f:
            parser = ijson.parse(f)
            for prefix, event, value in parser:
                # Track current table
                if event == 'map_key' and prefix == 'tables':
                    if current_table and current_count > 0:
                        table_counts[current_table] = current_count
                        total_records += current_count
                    current_table = value
                    current_count = 0
                # Count items in array
                elif event == 'start_map' and current_table and prefix == f'tables.{current_table}.item':
                    current_count += 1
            
            # Don't forget last table
            if current_table and current_count > 0:
                table_counts[current_table] = current_count
                total_records += current_count
        
        return table_counts, total_records
    
    def _save_metrics(self):
        """Save pipeline metrics to file"""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        metrics_dir = Path(self.config.LOG_DIR)
        metrics_file = metrics_dir / f"etl_metrics_{timestamp}.json"
        
        with open(metrics_file, 'w') as f:
            json.dump(self.metrics, f, indent=2, default=str)
        
        self.logger.info(f"Metrics saved to {metrics_file}")


if __name__ == "__main__":
    import sys
    import argparse
    
    # Create argument parser
    parser = argparse.ArgumentParser(
        description='ETL Pipeline - Extract, Transform and Load data',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
Examples:
  # Run full pipeline
  python -m src.pipeline
  
  # Skip extraction and use specific extracted file
  SKIP_EXTRACTION=true python -m src.pipeline --extracted-file extracted_data_20251030_123456.json
  
  
  # Skip both extraction and transformation with specific transformed file
  SKIP_EXTRACTION=true SKIP_TRANSFORMATION=true python -m src.pipeline --transformed-file snowflake_data_20251030_123456.json.gz
  
  # Set extraction start date
  python -m src.pipeline --start-date 2025-10-01
        '''
    )
    
    parser.add_argument(
        '--extracted-file',
        help='Specific extracted file to use when SKIP_EXTRACTION=true (filename or full path)'
    )
    
    parser.add_argument(
        '--transformed-file',
        help='Specific transformed file to use when SKIP_TRANSFORMATION=true (filename or full path)'
    )
    
    parser.add_argument(
        '--start-date',
        help='Extraction start date override (YYYY-MM-DD format)'
    )
    
    # For backward compatibility, also accept positional argument
    parser.add_argument(
        'file',
        nargs='?',
        help='File to use (extracted if SKIP_EXTRACTION, preprocessed if SKIP_PREPROCESSING, transformed if SKIP_TRANSFORMATION)'
    )
    
    args = parser.parse_args()
    
    # Handle backward compatibility with positional argument
    extracted_file = args.extracted_file
    transformed_file = args.transformed_file
    
    if args.file:
        # Positional argument provided - determine which type based on skip flags
        if settings.SKIP_TRANSFORMATION:
            transformed_file = args.file
        elif settings.SKIP_EXTRACTION:
            extracted_file = args.file
    
    # Run the ETL pipeline with specified files
    pipeline = Pipeline(
        extraction_start_date=args.start_date,
        extracted_file=extracted_file,
        transformed_file=transformed_file
    )
    pipeline.run()
