"""
Data Joiner - Performs table joins after extraction, before transformation
This module reads extracted data and performs necessary joins based on transformation mappings
"""

import json
import logging
from typing import Dict, Any, List, Optional
from collections import defaultdict

from .transformation_mapping import ALL_MAPPINGS


class DataJoiner:
    """Joins extracted data tables based on transformation requirements"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        
        # Build join configuration from transformation mappings
        self.join_configs = self._build_join_configs()
        
    def _build_join_configs(self) -> Dict:
        """
        Build join configurations from transformation mappings
        Returns dict with structure:
        {
            'target_table': {
                'source_tables': ['table1', 'table2'],
                'primary_table': 'table1',
                'joins': [
                    {'table': 'table2', 'on': 'table1.id = table2.table1_id'}
                ]
            }
        }
        """
        join_configs = {}
        
        # Common join patterns based on analysis
        join_patterns = {
            # Identity schema joins
            'dim_users': {
                'primary_table': 'users',
                'joins': [
                    {'table': 'user_preferences', 'on': 'users.id = user_preferences.user_id'},
                    {'table': 'user_accounts', 'on': 'users.id = user_accounts.user_id'}
                ]
            },
            
            # Tenant schema joins
            'fct_executions': {
                'primary_table': 'execution',
                'joins': [
                    {'table': 'execution_result', 'on': 'execution.id = execution_result.execution_id'},
                    {'table': 'application_version', 'on': 'execution.app_version_id = application_version.id'}
                ]
            },
            
            'dim_test_cases': {
                'primary_table': 'test_case',
                'joins': [
                    {'table': 'test_case_priorities', 'on': 'test_case.proirity_id = test_case_priorities.id'},
                    {'table': 'application_version', 'on': 'test_case.application_version_id = application_version.id'}
                ]
            }
            
            # Add more join patterns as needed
        }
        
        return join_patterns
    
    def join_database_tables(self, database_name: str, database_data: Dict) -> Dict:
        """
        Join tables within a database based on join configurations
        
        Args:
            database_name: Name of the database (identity, master, tenant)
            database_data: Extracted data for the database
            
        Returns:
            Modified database data with joined virtual tables
        """
        if not isinstance(database_data, dict):
            return database_data
            
        # Create a copy to avoid modifying original
        joined_data = database_data.copy()
        
        # For each target table that requires joins
        for target_table, join_config in self.join_configs.items():
            if target_table not in ALL_MAPPINGS:
                continue
                
            mapping = ALL_MAPPINGS[target_table]
            source_tables = mapping.get('source_tables', [])
            
            # Check if all source tables exist in this database
            tables_exist = all(
                table in database_data and 
                isinstance(database_data[table], dict) and 
                'sample' in database_data[table]
                for table in source_tables
            )
            
            if not tables_exist or len(source_tables) <= 1:
                continue
                
            # Perform the join
            try:
                joined_table = self._perform_join(
                    database_data,
                    source_tables,
                    join_config,
                    target_table
                )
                
                if joined_table:
                    # Add the joined table with a special prefix
                    joined_table_name = f"_joined_{target_table}"
                    joined_data[joined_table_name] = joined_table
                    
                    self.logger.info(
                        f"Created joined table {joined_table_name} from {source_tables}"
                    )
                    
            except Exception as e:
                self.logger.error(f"Failed to join tables for {target_table}: {e}")
                
        return joined_data
    
    def _perform_join(self, database_data: Dict, source_tables: List[str], 
                     join_config: Optional[Dict], target_table: str) -> Optional[Dict]:
        """
        Perform actual join operation
        
        Simple implementation - can be optimized for large datasets
        """
        if not join_config:
            # No specific join config, try simple merge
            return self._simple_merge(database_data, source_tables)
            
        primary_table = join_config.get('primary_table')
        if not primary_table or primary_table not in database_data:
            return None
            
        # Start with primary table data
        primary_data = database_data[primary_table].get('sample', [])
        if not primary_data:
            return None
            
        joined_records = []
        
        # For each record in primary table
        for primary_record in primary_data:
            joined_record = primary_record.copy()
            
            # Join with other tables
            for join in join_config.get('joins', []):
                join_table = join['table']
                if join_table not in database_data:
                    continue
                    
                join_data = database_data[join_table].get('sample', [])
                if not join_data:
                    continue
                    
                # Parse join condition (simple implementation)
                # Format: "table1.field = table2.field"
                join_on = join['on']
                if ' = ' in join_on:
                    left, right = join_on.split(' = ')
                    left_table, left_field = left.split('.')
                    right_table, right_field = right.split('.')
                    
                    # Find matching record
                    if left_table == primary_table:
                        primary_value = primary_record.get(left_field)
                        if primary_value is not None:
                            # Find matching record in join table
                            for join_record in join_data:
                                if join_record.get(right_field) == primary_value:
                                    # Merge fields (prefix with table name to avoid conflicts)
                                    for key, value in join_record.items():
                                        prefixed_key = f"{join_table}.{key}"
                                        joined_record[prefixed_key] = value
                                    break
            
            joined_records.append(joined_record)
        
        return {
            'records': len(joined_records),
            'sample': joined_records
        }
    
    def _simple_merge(self, database_data: Dict, source_tables: List[str]) -> Optional[Dict]:
        """
        Simple merge when no specific join config exists
        Just combines all records - transformer will handle field extraction
        """
        merged_records = []
        
        # Get the first table as base
        base_table = source_tables[0]
        if base_table not in database_data:
            return None
            
        base_data = database_data[base_table].get('sample', [])
        
        # For now, just return the base table data
        # The transformer will only extract fields that exist
        return {
            'records': len(base_data),
            'sample': base_data
        }
    
    def process_extracted_data(self, extracted_data: Dict) -> Dict:
        """
        Process all extracted data and perform necessary joins
        
        Args:
            extracted_data: Complete extracted data with all databases
            
        Returns:
            Modified extracted data with joined tables added
        """
        joined_data = extracted_data.copy()
        
        # Process each database
        for database_name, database_data in extracted_data.items():
            if database_name == 'extraction_metadata':
                continue
                
            self.logger.info(f"Processing joins for database: {database_name}")
            
            # Join tables within this database
            joined_database_data = self.join_database_tables(
                database_name, database_data
            )
            
            joined_data[database_name] = joined_database_data
            
        return joined_data
