"""
Override transformation mappings for tables that have been joined
This ensures the transformer only processes the primary table with joined data
"""

# Tables that have been joined by the preprocessor
# These should only use their primary source table
JOINED_TABLE_OVERRIDES = {
    # Identity database tables
    'dim_users': {
        'source_tables': ['users'],  # Only process users table (has all joined data)
        'primary_key': 'user_id'
    },
    'dim_accounts': {
        'source_tables': ['accounts'],  # Only process accounts table (has all joined data)
        'primary_key': 'account_id'
    },
    'dim_organizations': {
        'source_tables': ['organizations'],  # Primary table - organizations
        'primary_key': 'organization_id'
    },
    
    # Master database tables
    'dim_data_generators': {
        'source_tables': ['data_generators'],  # Only process data_generators table (has all joined data)
        'primary_key': 'generator_id'
    },
    
    # Tenant database tables
    'fct_executions': {
        'source_tables': ['execution'],  # Only process execution table (has all joined data)
        'primary_key': 'execution_id'
    },
    'dim_test_cases': {
        'source_tables': ['test_case'],  # Only process test_case table (has all joined data)
        'primary_key': 'test_case_id'
    },
    'dim_tenants': {
        'source_tables': ['tenants'],  # Only process tenants table (has all joined data)
        'primary_key': 'tenant_id'
    },
    'dim_test_suites': {
        'source_tables': ['test_case_group'],  # Only process test_case_group table (has all joined data)
        'primary_key': 'test_suite_id'
    },
    'fct_test_plan_results': {
        'source_tables': ['test_plan_result_metrics'],  # Only process test_plan_result_metrics table (has all joined data)
        'primary_key': 'test_plan_result_id'
    },
    'fct_api_steps': {
        'source_tables': ['api_steps'],  # Only process api_steps table (no joins needed)
        'primary_key': 'api_step_id'
    },
    'dim_elements': {
        'source_tables': ['element_locators'],  # Only process element_locators table (has all joined data)
        'primary_key': 'element_id'
    },
    'fct_accessibility_results': {
        'source_tables': ['accessibility_page_metrics'],  # Only process accessibility_page_metrics table (has all joined data)
        'primary_key': 'result_id'
    },
    'fct_agent_activity': {
        'source_tables': ['ai_agent_activity_log'],  # Only process ai_agent_activity_log table (has all joined data)
        'primary_key': 'activity_id'
    },
    'fct_cross_tenant_metrics': {
        'source_tables': ['test_plan_result_metrics'],  # Only process test_plan_result_metrics table (has all joined data)
        'primary_key': 'metric_id'
    }
}

def apply_join_overrides(mappings):
    """Apply overrides to transformation mappings for joined tables"""
    for table, override in JOINED_TABLE_OVERRIDES.items():
        if table in mappings:
            # Only override source_tables, keep column_mappings intact
            mappings[table]['source_tables'] = override['source_tables']
    return mappings
