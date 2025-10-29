"""
Static/Reference Tables Configuration
====================================

These tables contain reference data that rarely changes and should
ALWAYS be extracted regardless of date filtering.
"""

# Master database static tables
MASTER_STATIC_TABLES = {
    # Data generator configuration
    'data_generators',
    'data_generator_files',
    'data_generator_arguments',
    'data_generator_examples',
    
    # NLP templates and configuration
    'nlp_test_case_step_templates',
    'nlp_template_examples',
    
    # Object type definitions
    'object_types',
    'object_method_details',
    'object_method_params',
    'object_type_template_mappings',
    
    # Platform configuration
    'platform_browser_versions',
    'platform_devices', 
    'platform_os_versions',
    'platform_screen_resolutions',
    
    # Application types
    'application_types',
    
    # Other static config
    'driver_capabilities',
    'test_plan_lab_vendor_configs',
    'restricted_domains',
}

# Identity database static tables
IDENTITY_STATIC_TABLES = {
    'products',
    'restricted_domains',
}

# Tenant database static tables (reference data)
TENANT_STATIC_TABLES = {
    'accessibility_rules',
    'accessibility_rule_categories',
    'before_test_case_config',
    'after_test_case_config',
    'data_type',
    'element_repository_type',
    'environment_param_set',
    'result_column_preference',
    'test_case_priorities',
    'test_case_type',
    'test_step_condition',
}

# All static tables combined
ALL_STATIC_TABLES = MASTER_STATIC_TABLES | IDENTITY_STATIC_TABLES | TENANT_STATIC_TABLES


def is_static_table(table_name: str) -> bool:
    """
    Check if a table is a static/reference table that should always be extracted.
    
    Args:
        table_name: Name of the table
        
    Returns:
        True if table is static/reference data, False otherwise
    """
    return table_name in ALL_STATIC_TABLES
