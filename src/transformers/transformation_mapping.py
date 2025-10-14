"""
Transformation Mapping Configuration
===================================

This file defines the column mappings from source tables to Snowflake target tables.
It's designed to be human-readable and easily maintainable.

Structure:
- Each table has a clear mapping section
- Source tables are grouped logically
- Column mappings are explicit and documented
- Easy to add/remove/modify mappings

Usage:
- Import this file in transformation logic
- Use the mappings to transform data
- Modify mappings as needed for new requirements
"""

# ===== IDENTITY SCHEMA MAPPINGS =====

IDENTITY_MAPPINGS = {
    "dim_users": {
        "source_tables": ["users", "user_preferences", "user_accounts"],
        "primary_key": "user_id",
        "column_mappings": {
            "user_id": "users.id",
            "first_name": "users.first_name",
            "last_name": "users.last_name",
            "email": "users.email",
            "status": "users.status",
            "authenticity_token": "users.authenticity_token",
            "password_hash": "users.password",
            "last_logged_in_at": "users.last_logged_in_at",
            "created_at": "users.created_at",
            "updated_at": "users.updated_at",
            "is_2fa_enabled": "user_preferences.is_2fa_enabled",
            "user_account_id": "user_accounts.id",
            "account_id": "user_accounts.account_id",
            "is_account_owner": "user_accounts.account_owner",
            "account_status": "user_accounts.status"
        }
    },
    
    "dim_organizations": {
        "source_tables": ["organizations", "organization_policy"],
        "primary_key": "organization_id",
        "column_mappings": {
            "organization_id": "organizations.tsid",
            "organization_uuid": "organizations.uuid",
            "name": "organizations.name",
            "type": "organizations.type",
            "policy_json": "organizations.policy_json",
            "created_by_id": "organizations.created_by",
            "updated_by_id": "organizations.updated_by",
            "created_at": "organizations.created_date",
            "updated_at": "organizations.updated_date",
            "password_expiry_days": "organization_policy.password_expiry_days",
            "idle_session_timeout": "organization_policy.idle_session_timeout",
            "max_failed_login_attempts": "organization_policy.max_failed_login_attempts"
        }
    },
    
    "dim_accounts": {
        "source_tables": ["accounts", "authentication_modules", "smtp_configuration"],
        "primary_key": "account_id",
        "column_mappings": {
            "account_id": "accounts.id",
            "name": "accounts.name",
            "smtp_type": "smtp_configuration.type",
            "smtp_host": "smtp_configuration.host",
            "smtp_port": "smtp_configuration.port",
            "auth_module_type": "authentication_modules.type",
            "auth_enabled": "authentication_modules.enabled",
            "created_at": "accounts.created_date",
            "updated_at": "accounts.updated_date"
        }
    },
    
    "fct_user_sessions": {
        "source_tables": ["SPRING_SESSION", "SPRING_SESSION_ATTRIBUTES"],
        "primary_key": "session_id",
        "column_mappings": {
            "session_id": "SPRING_SESSION.SESSION_ID",
            "user_id": "SPRING_SESSION.PRINCIPAL_NAME",
            "organization_id": "SPRING_SESSION.ORGANIZATION_ID",
            "login_time": "SPRING_SESSION.CREATION_TIME",
            "logout_time": "SPRING_SESSION.EXPIRY_TIME",
            "duration_minutes": "SPRING_SESSION.DURATION_MINUTES",
            "ip_address": "SPRING_SESSION.IP_ADDRESS",
            "user_agent": "SPRING_SESSION.USER_AGENT",
            "last_access_time": "SPRING_SESSION.LAST_ACCESS_TIME"
        }
    }
}

# ===== MASTER SCHEMA MAPPINGS =====

MASTER_MAPPINGS = {
    "dim_tenants": {
        "source_tables": ["tenants", "subscriptions", "billing_addresses"],
        "primary_key": "tenant_id",
        "column_mappings": {
            "tenant_id": "tenants.id",
            "tenant_tsid": "tenants.tsid",
            "name": "tenants.company",
            "billing_country": "billing_addresses.country",
            "billing_state": "billing_addresses.state",
            "billing_city": "billing_addresses.city",
            "billing_address": "billing_addresses.address_line1",
            "billing_zip": "billing_addresses.zip_code",
            "billing_phone": "tenants.phone",
            "billing_email": "tenants.email",
            "created_at": "tenants.created_date",
            "updated_at": "tenants.updated_date",
            "plan_type": "subscriptions.plan",
            "subscription_status": "subscriptions.status",
            "allowed_users": "subscriptions.allowed_users",
            "max_projects": "subscriptions.no_of_projects",
            "cloud_automated_minutes_per_month": "subscriptions.cloud_automated_minutes_per_month",
            "local_automated_minutes_per_month": "subscriptions.local_automated_minutes_per_month",
            "total_parallel_runs": "subscriptions.no_of_parallel_runs",
            "next_renewal_at": "subscriptions.next_payment_date"
        }
    },
    
    "brg_tenant_features": {
        "source_tables": ["tenant_features"],
        "primary_key": ["tenant_id", "feature_id"],
        "column_mappings": {
            "tenant_id": "tenant_features.tenant_id",
            "feature_id": "tenant_features.feature_id",
            "is_enabled": "tenant_features.is_enabled",
            "created_at": "tenant_features.created_date",
            "updated_at": "tenant_features.updated_date",
            "tenant_feature_id": "tenant_features.id",
            "feature_name": "tenant_features.name",
            "is_add_on": "tenant_features.is_add_on",
            "enabled_at": "tenant_features.enabled_at"
        }
    },
    
    "dim_features": {
        "source_tables": ["features", "tenant_features"],
        "primary_key": "feature_id",
        "column_mappings": {
            "feature_id": "features.id",
            "name": "features.name",
            "description": "features.description",
            "category": "features.category",
            "is_active": "features.is_active",
            "created_at": "features.created_date",
            "updated_at": "features.updated_date",
            "is_premium": "tenant_features.is_add_on",
            "tenant_id": "tenant_features.tenant_id",
            "enabled_at": "tenant_features.enabled_at"
        }
    },
    
    "dim_data_generators": {
        "source_tables": ["data_generators", "data_generator_files"],
        "primary_key": "generator_id",
        "column_mappings": {
            "generator_id": "data_generators.id",
            "name": "data_generators.name",
            "type": "data_generators.type",
            "config_json": "data_generators.config_json",
            "is_active": "data_generators.is_active",
            "created_at": "data_generators.created_at_epoch",
            "updated_at": "data_generators.updated_at_epoch",
            "class_package": "data_generator_files.class_package",
            "class_name": "data_generator_files.class_name",
            "lib_type": "data_generator_files.lib_type_enum"
        }
    },
    
    "dim_nlp_templates": {
        "source_tables": ["nlp_test_case_step_templates"],
        "primary_key": "template_id",
        "column_mappings": {
            "template_id": "nlp_test_case_step_templates.id",
            "name": "nlp_test_case_step_templates.display_name",
            "content": "nlp_test_case_step_templates.content",
            "language": "nlp_test_case_step_templates.language",
            "category": "nlp_test_case_step_templates.category",
            "is_active": "nlp_test_case_step_templates.is_active",
            "created_at": "nlp_test_case_step_templates.created_at_epoch",
            "updated_at": "nlp_test_case_step_templates.updated_at_epoch",
            "keyword": "nlp_test_case_step_templates.keyword",
            "application_type": "nlp_test_case_step_templates.app_type_enum",
            "is_deprecated": "nlp_test_case_step_templates.deprecated",
            "is_actionable": "nlp_test_case_step_templates.is_actionable",
            "is_verifiable": "nlp_test_case_step_templates.is_verifiable",
            "type": "nlp_test_case_step_templates.type",
            "api_supported": "nlp_test_case_step_templates.api_supported"
        }
    },
    
    "dim_object_types": {
        "source_tables": ["object_types"],
        "primary_key": "object_type_id",
        "column_mappings": {
            "object_type_id": "object_types.id",
            "name": "object_types.name",
            "description": "object_types.description",
            "category": "object_types.category",
            "is_active": "object_types.is_active",
            "created_at": "object_types.created_at_epoch",
            "updated_at": "object_types.updated_at_epoch",
            "display_name": "object_types.object_display_name",
            "class_name": "object_types.object_class_name",
            "technology": "object_types.technology",
            "package_name": "object_types.package_name"
        }
    }
}

# ===== TENANT SCHEMA MAPPINGS =====

TENANT_MAPPINGS = {
    "dim_applications": {
        "source_tables": ["application", "application_version"],
        "primary_key": "app_id",
        "column_mappings": {
            "app_id": "application.id",
            "app_uuid": "application.uuid",
            "name": "application.name",
            "description": "application.description",
            "tenant_id": "application.tenant_id",
            "status": "application.status",
            "created_by_id": "application.created_by",
            "updated_by_id": "application.updated_by",
            "created_at": "application.created_date",
            "updated_at": "application.updated_date"
        }
    },
    
    "dim_test_cases": {
        "source_tables": ["test_case", "test_case_type", "test_case_priorities", "application_version"],
        "primary_key": "test_case_id",
        "column_mappings": {
            "test_case_id": "test_case.id",
            "test_case_uuid": "test_case.tsid",
            "name": "test_case.name",
            "description": "test_case.description",
            "app_id": "application_version.application_id",
            "tenant_id": "test_case.tenant_tsid",
            "status": "test_case.status_enum",
            "priority": "test_case_priorities.name",
            "created_by_id": "test_case.created_by_id",
            "updated_by_id": "test_case.updated_by_id",
            "created_at": "test_case.created_date",
            "updated_at": "test_case.updated_date"
        }
    },
    
    "fct_test_steps": {
        "source_tables": ["nlp_test_case_step"],
        "primary_key": "step_id",
        "column_mappings": {
            "step_id": "nlp_test_case_step.id",
            "test_case_id": "nlp_test_case_step.test_case_id",
            "tenant_id": "nlp_test_case_step.tenant_tsid",
            "user_id": "nlp_test_case_step.created_by_id",
            "step_order": "nlp_test_case_step.template_id",
            "step_type": "nlp_test_case_step.teststep_key",
            "step_data": "nlp_test_case_step.action",
            "expected_result": "nlp_test_case_step.excepted_result",
            "created_at": "nlp_test_case_step.created_date",
            "updated_at": "nlp_test_case_step.updated_date",
            "element_id": "nlp_test_case_step.step_group_id"
        }
    },
    
    "fct_executions": {
        "source_tables": ["execution", "execution_result", "application_version"],
        "primary_key": "execution_id",
        "column_mappings": {
            "execution_id": "execution.id",
            "execution_uuid": "execution.tsid",
            "tenant_id": "execution.tenant_tsid",
            "user_id": "execution.created_by_id",
            "app_id": "application_version.application_id",
            "test_suite_id": "execution.id",
            "status": "execution_result.result_enum",
            "start_time": "execution_result.start_time",
            "end_time": "execution_result.end_time",
            "duration_seconds": "execution_result.duration",
            "triggered_by": "execution_result.triggered_type",
            "environment_type": "execution.execution_type_enum",
            "created_at": "execution.created_date",
            "updated_at": "execution.updated_date"
        }
    },
    
    "fct_test_results": {
        "source_tables": ["test_case_result"],
        "primary_key": "result_id",
        "column_mappings": {
            "result_id": "test_case_result.id",
            "execution_id": "test_case_result.execution_result_id",
            "test_case_id": "test_case_result.test_case_id",
            "tenant_id": "test_case_result.tenant_tsid",
            "user_id": "test_case_result.created_by_id",
            "status": "test_case_result.result",
            "start_time": "test_case_result.start_time",
            "end_time": "test_case_result.end_time",
            "duration_seconds": "test_case_result.duration",
            "error_message": "test_case_result.message",
            "retry_count": "test_case_result.retry_count",
            "is_flaky": "test_case_result.is_flaky",
            "created_at": "test_case_result.created_date",
            "updated_at": "test_case_result.updated_date"
        }
    },
    
    "dim_elements": {
        "source_tables": ["element_locators"],
        "primary_key": "element_id",
        "column_mappings": {
            "element_id": "element_locators.id",
            "element_uuid": "element_locators.uuid",
            "name": "element_locators.name",
            "type": "element_locators.type",
            "locator": "element_locators.locator_value",
            "app_id": "element_locators.application_id",
            "tenant_id": "element_locators.tenant_id",
            "created_by_id": "element_locators.created_by",
            "updated_by_id": "element_locators.updated_by",
            "created_at": "element_locators.created_date",
            "updated_at": "element_locators.updated_date",
            "locator_type": "element_locators.locator_type"
        }
    },
    
    "dim_test_data": {
        "source_tables": ["test_data"],
        "primary_key": "test_data_id",
        "column_mappings": {
            "test_data_id": "test_data.id",
            "test_data_uuid": "test_data.uuid",
            "name": "test_data.name",
            "type": "test_data.type",
            "data_json": "test_data.data_json",
            "app_id": "test_data.app_id",
            "tenant_id": "test_data.tenant_id",
            "created_by_id": "test_data.created_by",
            "updated_by_id": "test_data.updated_by",
            "created_at": "test_data.created_date",
            "updated_at": "test_data.updated_date",
            "data_id": "test_data.id"
        }
    },
    
    "dim_agents": {
        "source_tables": ["agents"],
        "primary_key": "agent_id",
        "column_mappings": {
            "agent_id": "agents.id",
            "agent_uuid": "agents.uuid",
            "name": "agents.name",
            "type": "agents.agent_type",
            "config_json": "agents.config_json",
            "status": "agents.status",
            "tenant_id": "agents.tenant_id",
            "created_by_id": "agents.created_by",
            "updated_by_id": "agents.updated_by",
            "created_at": "agents.created_date",
            "updated_at": "agents.updated_date",
            "version": "agents.agent_version",
            "is_active": "agents.is_active",
            "last_pinged_at": "agents.last_pinged_at"
        }
    },
    
    "fct_api_steps": {
        "source_tables": ["api_steps", "api_step_result_response"],
        "primary_key": "api_step_id",
        "column_mappings": {
            "api_step_id": "api_steps.id",
            "test_case_id": "api_steps.test_case_id",
            "tenant_id": "api_steps.tenant_tsid",
            "user_id": "api_steps.created_by_id",
            "step_order": "api_steps.step_order",
            "method": "api_steps.method",
            "url": "api_steps.url",
            "headers_json": "api_steps.headers_json",
            "body": "api_steps.body",
            "expected_status": "api_steps.expected_status",
            "created_at": "api_steps.created_date",
            "updated_at": "api_steps.updated_date"
        }
    },
    
    "fct_accessibility_results": {
        "source_tables": ["accessibility_page_metrics"],
        "primary_key": "result_id",
        "column_mappings": {
            "result_id": "accessibility_page_metrics.id",
            "test_case_id": "accessibility_page_metrics.test_case_result_id",
            "execution_id": "accessibility_page_metrics.execution_result_id",
            "tenant_id": "accessibility_page_metrics.tenant_tsid",
            "user_id": "accessibility_page_metrics.created_by_id",
            "check_type": "accessibility_page_metrics.entity_type",
            "status": "accessibility_page_metrics.status",
            "message": "accessibility_page_metrics.action",
            "severity": "accessibility_page_metrics.status",
            "created_at": "accessibility_page_metrics.created_date",
            "updated_at": "accessibility_page_metrics.updated_date",
            "page_url": "accessibility_page_metrics.page_url",
            "critical_issues": "accessibility_page_metrics.critical_issues_count",
            "minor_issues": "accessibility_page_metrics.minor_issues_count",
            "serious_issues": "accessibility_page_metrics.serious_issues_count",
            "moderate_issues": "accessibility_page_metrics.moderate_issues_count"
        }
    },
    
    "fct_infrastructure": {
        "source_tables": ["infrastructure_metrics"],
        "primary_key": "infrastructure_id",
        "column_mappings": {
            "infrastructure_id": "infrastructure_metrics.id",
            "tenant_id": "infrastructure_metrics.tenant_id",
            "resource_type": "infrastructure_metrics.resource_type",
            "resource_name": "infrastructure_metrics.resource_name",
            "status": "infrastructure_metrics.status",
            "metrics_json": "infrastructure_metrics.metrics_json",
            "timestamp": "infrastructure_metrics.timestamp",
            "created_at": "infrastructure_metrics.created_date",
            "updated_at": "infrastructure_metrics.updated_date"
        }
    },
    
    "fct_cross_tenant_metrics": {
        "source_tables": ["test_plan_result_metrics"],
        "primary_key": "metric_id",
        "column_mappings": {
            "metric_id": "test_plan_result_metrics.id",
            "tenant_id": "test_plan_result_metrics.tenant_id",
            "metric_name": "test_plan_result_metrics.metric_name",
            "metric_value": "test_plan_result_metrics.metric_value",
            "metric_unit": "test_plan_result_metrics.metric_unit",
            "timestamp": "test_plan_result_metrics.timestamp",
            "created_at": "test_plan_result_metrics.created_date",
            "updated_at": "test_plan_result_metrics.updated_date"
        }
    },
    
    "dim_test_suites": {
        "source_tables": ["test_case_group", "application_version"],
        "primary_key": "test_suite_id",
        "column_mappings": {
            "test_suite_id": "test_case_group.id",
            "test_suite_uuid": "test_case_group.tsid",
            "name": "test_case_group.name",
            "description": "test_case_group.description",
            "app_id": "application_version.application_id",
            "tenant_id": "test_case_group.tenant_tsid",
            "status": "test_case_group.entity_type",
            "created_by_id": "test_case_group.created_by_id",
            "updated_by_id": "test_case_group.updated_by_id",
            "created_at": "test_case_group.created_date",
            "updated_at": "test_case_group.updated_date",
            "suite_id": "test_case_group.id"
        }
    },
    
    "fct_agent_activity": {
        "source_tables": ["ai_agent_activity_log"],
        "primary_key": "activity_id",
        "column_mappings": {
            "activity_id": "ai_agent_activity_log.id",
            "agent_id": "ai_agent_activity_log.agent_id",
            "tenant_id": "ai_agent_activity_log.tenant_tsid",
            "user_id": "ai_agent_activity_log.created_by_id",
            "activity_type": "ai_agent_activity_log.activity_type",
            "description": "ai_agent_activity_log.description",
            "metadata_json": "ai_agent_activity_log.metadata_json",
            "timestamp": "ai_agent_activity_log.timestamp",
            "created_at": "ai_agent_activity_log.created_date",
            "updated_at": "ai_agent_activity_log.updated_date"
        }
    },
    
    "fct_audit_events": {
        "source_tables": ["audit_history"],
        "primary_key": "event_id",
        "column_mappings": {
            "event_id": "audit_history.id",
            "event_uuid": "audit_history.uuid",
            "event_type": "audit_history.entity_tag",
            "entity_type": "audit_history.entity_model",
            "entity_id": "audit_history.entity_id",
            "user_id": "audit_history.actor_id",
            "tenant_id": "audit_history.tenant_tsid",
            "changes_json": "audit_history.new_entity_data",
            "ip_address": "audit_history.client_ip_address",
            "user_agent": "audit_history.user_agent",
            "timestamp": "audit_history.action_date",
            "created_at": "audit_history.created_date",
            "updated_at": "audit_history.updated_date",
            "actor_id": "audit_history.actor_id",
            "action": "audit_history.entity_action",
            "event_time": "audit_history.action_date",
            "status": "audit_history.status"
        }
    }
}

# ===== COMBINED MAPPINGS =====

ALL_MAPPINGS = {
    **IDENTITY_MAPPINGS,
    **MASTER_MAPPINGS,
    **TENANT_MAPPINGS
}

# ===== UTILITY FUNCTIONS =====

def get_table_mapping(table_name):
    """Get the mapping configuration for a specific table."""
    return ALL_MAPPINGS.get(table_name)

def get_source_tables(table_name):
    """Get the source tables for a specific target table."""
    mapping = get_table_mapping(table_name)
    return mapping.get("source_tables", []) if mapping else []

def get_column_mappings(table_name):
    """Get the column mappings for a specific target table."""
    mapping = get_table_mapping(table_name)
    return mapping.get("column_mappings", {}) if mapping else {}

def get_primary_key(table_name):
    """Get the primary key column for a specific target table."""
    mapping = get_table_mapping(table_name)
    return mapping.get("primary_key") if mapping else None

def list_all_tables():
    """Get a list of all target table names."""
    return list(ALL_MAPPINGS.keys())

def get_tables_by_schema(schema_name):
    """Get tables for a specific schema (identity, master, tenant)."""
    if schema_name.lower() == "identity":
        return list(IDENTITY_MAPPINGS.keys())
    elif schema_name.lower() == "master":
        return list(MASTER_MAPPINGS.keys())
    elif schema_name.lower() == "tenant":
        return list(TENANT_MAPPINGS.keys())
    else:
        return []

def validate_mappings():
    """Validate that all mappings are properly configured."""
    errors = []
    
    for table_name, mapping in ALL_MAPPINGS.items():
        if "source_tables" not in mapping:
            errors.append(f"Missing source_tables for {table_name}")
        if "primary_key" not in mapping:
            errors.append(f"Missing primary_key for {table_name}")
        if "column_mappings" not in mapping:
            errors.append(f"Missing column_mappings for {table_name}")
        elif not mapping["column_mappings"]:
            errors.append(f"Empty column_mappings for {table_name}")
    
    return errors

if __name__ == "__main__":
    # Run validation when script is executed directly
    errors = validate_mappings()
    if errors:
        print("Validation errors found:")
        for error in errors:
            print(f"  - {error}")
    else:
        print("All mappings are valid!")
        print(f"Total tables configured: {len(ALL_MAPPINGS)}")
        print(f"Identity schema tables: {len(IDENTITY_MAPPINGS)}")
        print(f"Master schema tables: {len(MASTER_MAPPINGS)}")
        print(f"Tenant schema tables: {len(TENANT_MAPPINGS)}")