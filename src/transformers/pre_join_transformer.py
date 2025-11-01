"""
Pre-Join Aware Transformer
==========================
Simplified transformer that uses pre-joined data from extraction.
No complex join logic needed!
"""

import json
import os
import gzip
from datetime import datetime
from typing import Dict, List, Any, Optional
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class PreJoinTransformer:
    """Transformer that uses pre-joined data for fast and accurate transformation"""
    
    # Boolean fields that need 0/1 to true/false conversion
    BOOLEAN_FIELDS = {
        # dim_users
        'deleted', 'is_2fa_enabled', 'join_confirmed', 'is_account_owner', 'account_deleted',
        # dim_accounts  
        'auth_enabled',
        # dim_tenants
        'is_play_ground', 'is_beta', 'skip_support_channel_notifications', 
        'report_customization_enabled', 'subscribed_to_emails', 'block_access',
        # brg_tenant_features & dim_features
        'is_add_on', 'is_premium',
        # dim_data_generators
        'has_data_map',
        # dim_nlp_templates  
        'snippet_enabled', 'deprecated', 'is_actionable', 'is_verifiable',
        'import_to_web', 'import_to_mobile_web', 'import_to_android_native',
        'import_to_ios_native', 'import_to_rest_native', 'import_to_salesforce', 'api_supported',
        # dim_projects
        'has_multiple_apps', 'has_multiple_versions', 'demo',
        # dim_test_cases
        'is_data_driven', 'is_step_group', 'is_ai_generated', 'is_manual', 'is_active',
        'is_reviewed', 'is_prerequisite_case', 'is_under_deletion', 'has_error',
        'has_after_test', 'is_eligible_for_after_suite', 'consider_visual_test_result', 'delete_marker',
        # fct_test_steps
        'disabled', 'visual_enabled', 'accessibility_enabled', 'ignore_step_result', 'is_invalid',
        # fct_executions
        'slack_connector_notification_enabled', 'ms_teams_connector_notification_enabled',
        'google_chat_connector_notification_enabled', 'accessibility_test_enabled',
        'is_inprogress', 'run_test_cases_in_parallel', 'run_test_suites_in_parallel', 'is_visually_passed',
        # fct_test_results
        'is_queued', 'is_migrated_execution',
        # dim_agents
        'enabled', 'visible_to_all', 'mobile_enabled', 'is_active', 'is_docker', 
        'is_service', 'is_obsolete', 'go_ios_enabled',
        # dim_applications
        'is_disabled'
    }
    
    def __init__(self):
        from .transformation_mapping import ALL_MAPPINGS
        self.all_mappings = ALL_MAPPINGS
        self.etl_timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    def clean_value(self, value: Any, field_name: str = None) -> Any:
        """Clean a single value"""
        if value is None:
            return None
        
        # Handle binary string values from MySQL
        if isinstance(value, str) and value.startswith("b'\\x"):
            return value == "b'\\x01'"
        
        # Convert 0/1 to false/true for boolean fields
        if field_name and field_name in self.BOOLEAN_FIELDS:
            if value == 0:
                return False
            elif value == 1:
                return True
            # For any other value (like already boolean), return as is
            return bool(value) if value is not None else None
        
        # Convert dict/list to JSON string
        if isinstance(value, (dict, list)):
            return json.dumps(value)
        
        return value
    
    def transform_record(self, record: Dict, column_mappings: Dict = None) -> Dict:
        """Transform a single pre-joined record - fields are already named correctly!"""
        # Since pre-joined queries already have correct column names, 
        # we just need to clean values and add etl_timestamp
        transformed = {'etl_timestamp': self.etl_timestamp}
        
        # Copy all fields from the pre-joined record
        for field_name, value in record.items():
            # Clean the value - pass field_name for boolean detection
            cleaned_value = self.clean_value(value, field_name)
            transformed[field_name] = cleaned_value
            
            # Handle _json fields
            if field_name.endswith('_json') and cleaned_value is None:
                transformed[field_name] = '{}'
        
        return transformed
    
    def transform_table(self, target_table: str, pre_joined_data: List[Dict], 
                       mapping_config: Dict) -> Dict:
        """Transform pre-joined data for a target table"""
        # Pre-joined data already has correct field names from SQL aliases
        # We just need to clean values and add etl_timestamp
        
        # Transform all records
        transformed_records = []
        for record in pre_joined_data:
            transformed = self.transform_record(record)
            transformed_records.append(transformed)
        
        return {
            'records': len(transformed_records),
            'data': transformed_records
        }
    
    def transform_all(self, extracted_data: Dict) -> Dict:
        """Transform all pre-joined data"""
        logger.info("Starting pre-join aware transformation...")
        
        transformed_data = {
            'transformation_timestamp': self.etl_timestamp,
            'tables': {}
        }
        
        # Process each database
        for db_name, db_data in extracted_data.items():
            if isinstance(db_data, dict) and db_name != 'extraction_metadata':
                # Look for pre-joined data
                for table_key, table_info in db_data.items():
                    if table_key.startswith('_prejoined_'):
                        # Extract target table name
                        target_table = table_key.replace('_prejoined_', '')
                        
                        if target_table in self.all_mappings:
                            logger.info(f"Transforming pre-joined data for {target_table}")
                            
                            # Get the pre-joined data
                            pre_joined_data = table_info.get('sample', [])
                            mapping_config = self.all_mappings[target_table]
                            
                            # Transform the data
                            result = self.transform_table(
                                target_table, pre_joined_data, mapping_config
                            )
                            
                            transformed_data['tables'][target_table] = result
                            logger.info(f"Transformed {target_table}: {result['records']} records")
        
        # Fall back to regular transformation for tables without pre-joined data
        logger.info("\nChecking for tables without pre-joined data...")
        missing_tables = set(self.all_mappings.keys()) - set(transformed_data['tables'].keys())
        
        if missing_tables:
            logger.warning(f"Tables without pre-joined data: {missing_tables}")
            logger.info("Falling back to regular transformation for missing tables...")
            
            # Import the regular transformer with full join logic
            logger.info("Using regular transformer for tables without pre-joins...")
            
            # Save the pre-joined data to a temp file for the regular transformer
            import tempfile
            with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as tmp:
                json.dump(extracted_data, tmp)
                temp_file = tmp.name
            
            try:
                # Use the regular transformer that has all the complex join logic
                from .transformer import transform_data_optimized
                
                # Transform the entire file
                regular_output = transform_data_optimized(temp_file, timestamp=self.etl_timestamp)
                
                # Load the regular transformer output
                import gzip
                with gzip.open(regular_output, 'rt') as f:
                    regular_data = json.load(f)
                
                # Add only the missing tables from regular transformation
                for table_name in missing_tables:
                    if table_name in regular_data.get('tables', {}):
                        transformed_data['tables'][table_name] = regular_data['tables'][table_name]
                        if os.getenv('DEBUG', '').lower() == 'true':
                            logger.info(f"✓ Transformed {table_name}: {regular_data['tables'][table_name]['records']} records")
                    else:
                        if os.getenv('DEBUG', '').lower() == 'true':
                            logger.warning(f"✗ Table {table_name} not found in regular transformation")
                
                # Clean up temp files
                os.unlink(temp_file)
                os.unlink(regular_output)
                
            except Exception as e:
                logger.error(f"Failed to use regular transformer for fallback: {e}")
                import traceback
                traceback.print_exc()
        
        return transformed_data
    
    def save_transformed_data(self, data: Dict, output_dir: str) -> str:
        """Save transformed data to compressed JSON file"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"snowflake_data_{timestamp}.json.gz"
        filepath = os.path.join(output_dir, filename)
        
        # Ensure directory exists
        os.makedirs(output_dir, exist_ok=True)
        
        # Save as compressed JSON
        with gzip.open(filepath, 'wt', encoding='utf-8') as f:
            json.dump(data, f, default=str)
        
        logger.info(f"Saved transformed data to: {filepath}")
        file_size_mb = os.path.getsize(filepath) / (1024 * 1024)
        logger.info(f"File size: {file_size_mb:.2f} MB (compressed)")
        
        return filepath


def transform_with_pre_joins(input_file: str, timestamp: str = None) -> str:
    """
    Transform data using pre-joined extraction
    
    Args:
        input_file: Path to extracted data file with pre-joined data
        timestamp: Optional ETL timestamp
        
    Returns:
        Path to transformed file
    """
    logger.info("=" * 70)
    logger.info("PRE-JOIN AWARE TRANSFORMER")
    logger.info("Fast, memory-efficient, and accurate!")
    logger.info("=" * 70)
    
    # Load extracted data
    logger.info(f"Loading extracted data from: {input_file}")
    with open(input_file, 'r') as f:
        extracted_data = json.load(f)
    
    # Transform
    transformer = PreJoinTransformer()
    if timestamp:
        # Convert job_id format (20251102_002335) to proper timestamp
        if '_' in timestamp and len(timestamp) == 15:
            # Parse YYYYMMDD_HHMMSS format
            try:
                dt = datetime.strptime(timestamp, "%Y%m%d_%H%M%S")
                transformer.etl_timestamp = dt.strftime('%Y-%m-%d %H:%M:%S')
            except:
                transformer.etl_timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        else:
            transformer.etl_timestamp = timestamp
    
    transformed_data = transformer.transform_all(extracted_data)
    
    # Save
    output_dir = os.path.join(os.path.dirname(input_file), '..', 'transformations')
    output_path = transformer.save_transformed_data(transformed_data, output_dir)
    
    # Summary
    total_records = sum(
        table_data['records'] 
        for table_data in transformed_data['tables'].values()
    )
    logger.info(f"\nTransformation complete!")
    logger.info(f"Tables: {len(transformed_data['tables'])}")
    logger.info(f"Records: {total_records:,}")
    
    return output_path
