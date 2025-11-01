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
            "organization_id": "users.organization_tsid",
            "uuid": "users.uuid",
            "first_name": "users.first_name",
            "last_name": "users.last_name",
            "company_name": "users.company_name",
            "phone": "users.phone",
            "email": "users.email",
            "status": "users.status",
            "type": "users.type",
            "deleted": "users.deleted",
            "last_accessed_account": "users.last_accessed_account",
            "password_last_updated_at": "users.password_last_updated_at",
            "expiry_remainder_notified_at": "users.expiry_remainder_notified_at",
            "last_logged_in_at": "users.last_logged_in_at",
            "created_at": "users.created_date",
            "updated_at": "users.updated_date",
            "is_2fa_enabled": "user_preferences.is_2fa_enabled",
            "user_account_id": "user_accounts.id",
            "account_id": "user_accounts.account_id",
            "account_uuid": "user_accounts.uuid",
            "join_confirmed": "user_accounts.join_confirmed",
            "is_account_owner": "user_accounts.account_owner",
            "account_deleted": "user_accounts.deleted",
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
            "idle_session_timeout": "organization_policy.idle_session_timeout",
            "password_expiry_days": "organization_policy.password_expiry_days",
            "max_failed_login_attempts": "organization_policy.max_failed_login_attempts",
            "created_by_id": "organizations.created_by_id",
            "updated_by_id": "organizations.updated_by_id",
            "created_at": "organizations.created_date",
            "updated_at": "organizations.updated_date"
        }
    },
    
    "dim_accounts": {
        "source_tables": ["accounts", "authentication_modules", "smtp_configuration"],
        "primary_key": "account_id",
        "column_mappings": {
            "account_id": "accounts.id",
            "organization_tsid": "accounts.organization_tsid",
            "uuid": "accounts.uuid",
            "name": "accounts.name",
            "url": "accounts.url",
            "status": "accounts.status",
            "product_id": "accounts.product_id",
            "smtp_type": "smtp_configuration.type",
            "smtp_host": "smtp_configuration.host",
            "smtp_port": "smtp_configuration.port",
            "smtp_email": "smtp_configuration.email",
            "auth_module_type": "authentication_modules.type",
            "auth_provider": "authentication_modules.provider",
            "auth_enabled": "authentication_modules.enabled",
            "created_by_id": "accounts.created_by_id",
            "updated_by_id": "accounts.updated_by_id",
            "created_at": "accounts.created_date",
            "updated_at": "accounts.updated_date"
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
            "first_name": "tenants.first_name",
            "last_name": "tenants.last_name",
            "email": "tenants.email",
            "company": "tenants.company",
            "phone": "tenants.phone",
            "domain": "tenants.domain",
            "alternative_email": "tenants.alternative_email",
            "features": "tenants.features",
            "lead_id": "tenants.lead_id",
            "contact_id": "tenants.contact_id",
            "hubspot_contact_id": "tenants.hubspot_contact_id",
            "is_play_ground": "tenants.is_play_ground",
            "referrer": "tenants.referrer",
            "deleted": "tenants.deleted",
            "shard": "tenants.shard",
            "database_name": "tenants.database_name",
            "is_beta": "tenants.is_beta",
            "signup_type": "tenants.signup_type",
            "skip_support_channel_notifications": "tenants.skip_support_channel_notifications",
            "status_enum": "tenants.status_enum",
            "maintenance_enum": "tenants.maintenance_enum",
            "report_customization_enabled": "tenants.report_customization_enabled",
            "salesforce_account_id": "tenants.salesforce_account_id",
            "subscribed_to_emails": "tenants.subscribed_to_emails",
            "identity_account_uuid": "tenants.identity_account_uuid",
            "identity_organization_tsid": "tenants.identity_organization_tsid",
            "created_at": "tenants.created_date",
            "updated_at": "tenants.updated_date",
            "subscription_id": "subscriptions.id",
            "plan": "subscriptions.plan",
            "plan_type": "subscriptions.plan",
            "chargebee_id": "subscriptions.chargebee_id",
            "no_of_projects": "subscriptions.no_of_projects",
            "cloud_automated_minutes_per_month": "subscriptions.cloud_automated_minutes_per_month",
            "local_automated_minutes_per_month": "subscriptions.local_automated_minutes_per_month",
            "block_access": "subscriptions.block_access",
            "trial_period": "subscriptions.trial_period",
            "allowed_users": "subscriptions.allowed_users",
            "max_projects": "subscriptions.max_projects",
            "next_renewal_at": "subscriptions.next_renewal_at",
            "total_parallel_runs": "subscriptions.total_parallel_runs",
            "additional_parallel_runs_on_cloud": "subscriptions.additional_parallel_runs_on_cloud",
            "only_local_parallel_runs": "subscriptions.only_local_parallel_runs",
            "additional_parallel_runs_on_local": "subscriptions.additional_parallel_runs_on_local",
            "free_additional_users": "subscriptions.free_additional_users",
            "allowed_queue_size": "subscriptions.allowed_queue_size",
            "free_local_only_mobile_sessions": "subscriptions.free_local_only_mobile_sessions",
            "subscription_status": "subscriptions.status_enum",
            "cancelled_date": "subscriptions.cancelled_date",
            "deployment_modal": "subscriptions.deployment_modal",
            "billing_address_line1": "billing_addresses.address_line1",
            "billing_address_line2": "billing_addresses.address_line2",
            "billing_country": "billing_addresses.country",
            "billing_state": "billing_addresses.state",
            "billing_city": "billing_addresses.city",
            "billing_zip": "billing_addresses.zip_code"
        }
    },
    
    "brg_tenant_features": {
        "source_tables": ["tenant_features"],
        "primary_key": ["tenant_id", "feature_id"],
        "column_mappings": {
            "tenant_id": "tenant_features.tenant_id",
            "feature_id": "tenant_features.id",
            "created_at": "tenant_features.created_date",
            "updated_at": "tenant_features.updated_date",
            "tenant_feature_id": "tenant_features.id",
            "feature_name": "tenant_features.name",
            "is_add_on": "tenant_features.is_add_on"
        }
    },
    
    "dim_features": {
        "source_tables": ["tenant_features"],
        "primary_key": "feature_id",
        "column_mappings": {
            "feature_id": "tenant_features.id",
            "name": "tenant_features.name",
            "is_premium": "tenant_features.is_add_on",
            "tenant_id": "tenant_features.tenant_id",
            "created_at": "tenant_features.created_date",
            "updated_at": "tenant_features.updated_date"
        }
    },
    
    "dim_data_generators": {
        "source_tables": ["data_generators", "data_generator_files"],
        "primary_key": "generator_id",
        "column_mappings": {
            "generator_id": "data_generators.id",
            "generator_tsid": "data_generators.tsid",
            "name": "data_generators.name",
            "display_name": "data_generators.display_name",
            "unique_id": "data_generators.unique_id",
            "fixed_arguments": "data_generators.fixed_arguments",
            "has_data_map": "data_generators.has_data_map",
            "file_id": "data_generators.file_id",
            "created_at": "data_generators.created_at_epoch",
            "updated_at": "data_generators.updated_at_epoch",
            "class_package": "data_generator_files.class_package",
            "class_name": "data_generator_files.class_name",
            "file_display_name": "data_generator_files.display_name",
            "lib_type": "data_generator_files.lib_type_enum"
        }
    },
    
    "dim_nlp_templates": {
        "source_tables": ["nlp_test_case_step_templates"],
        "primary_key": "template_id",
        "column_mappings": {
            "template_id": "nlp_test_case_step_templates.id",
            "template_tsid": "nlp_test_case_step_templates.tsid",
            "keyword": "nlp_test_case_step_templates.keyword",
            "grammer": "nlp_test_case_step_templates.grammer",
            "display_name": "nlp_test_case_step_templates.display_name",
            "snippet_enabled": "nlp_test_case_step_templates.snippet_enabled",
            "snippet_class": "nlp_test_case_step_templates.snippet_class",
            "deprecated": "nlp_test_case_step_templates.deprecated",
            "is_actionable": "nlp_test_case_step_templates.is_actionable",
            "is_verifiable": "nlp_test_case_step_templates.is_verifiable",
            "condition_type": "nlp_test_case_step_templates.condition_type",
            "type": "nlp_test_case_step_templates.type",
            "import_to_web": "nlp_test_case_step_templates.import_to_web",
            "import_to_mobile_web": "nlp_test_case_step_templates.import_to_mobile_web",
            "import_to_android_native": "nlp_test_case_step_templates.import_to_android_native",
            "import_to_ios_native": "nlp_test_case_step_templates.import_to_ios_native",
            "import_to_rest_native": "nlp_test_case_step_templates.import_to_rest_native",
            "import_to_salesforce": "nlp_test_case_step_templates.import_to_salesforce",
            "app_type_enum": "nlp_test_case_step_templates.app_type_enum",
            "loop_type": "nlp_test_case_step_templates.loop_type",
            "position": "nlp_test_case_step_templates.position",
            "application_type_id": "nlp_test_case_step_templates.application_type_id",
            "optional_template_id": "nlp_test_case_step_templates.optional_template_id",
            "api_supported": "nlp_test_case_step_templates.api_supported",
            "screenshot_strategy": "nlp_test_case_step_templates.screenshot_strategy",
            "created_at": "nlp_test_case_step_templates.created_at_epoch",
            "updated_at": "nlp_test_case_step_templates.updated_at_epoch"
        }
    },
    
    "dim_object_types": {
        "source_tables": ["object_types"],
        "primary_key": "object_type_id",
        "column_mappings": {
            "object_type_id": "object_types.id",
            "name": "object_types.object_display_name",
            "display_name": "object_types.object_display_name",
            "class_name": "object_types.object_class_name",
            "technology": "object_types.technology",
            "package_name": "object_types.package_name",
            "created_at": "object_types.created_at_epoch",
            "updated_at": "object_types.updated_at__epoch"
        }
    }
}

# ===== TENANT SCHEMA MAPPINGS =====

TENANT_MAPPINGS = {
    "dim_projects": {
        "source_tables": ["project"],
        "primary_key": "project_id",
        "column_mappings": {
            "project_id": "project.id",
            "project_tsid": "project.tsid",
            "name": "project.name",
            "description": "project.description",
            "tenant_tsid": "project.tenant_tsid",
            "has_multiple_apps": "project.has_multiple_apps",
            "has_multiple_versions": "project.has_multiple_versions",
            "demo": "project.demo",
            "time_zone": "project.time_zone",
            "project_type_enum": "project.project_type_enum",
            "created_by_id": "project.created_by_id",
            "updated_by_id": "project.updated_by_id",
            "created_at": "project.created_date",
            "updated_at": "project.updated_date",
            "created_at_epoch": "project.created_at_epoch",
            "updated_at_epoch": "project.updated_at_epoch"
        }
    },
    
    "dim_applications": {
        "source_tables": ["application", "application_version"],
        "primary_key": "app_id",
        "column_mappings": {
            "app_id": "application.id",
            "app_tsid": "application.tsid",
            "name": "application.name",
            "description": "application.description",
            "tenant_tsid": "application.tenant_tsid",
            "project_id": "application.project_id",
            "custom_fields": "application.custom_fields",
            "type_enum": "application.type_enum",
            "created_by_id": "application.created_by_id",
            "updated_by_id": "application.updated_by_id",
            "created_at": "application.created_date",
            "updated_at": "application.updated_date",
            "created_at_epoch": "application.created_at_epoch",
            "updated_at_epoch": "application.updated_at_epoch"
        }
    },
    
    "dim_test_cases": {
        "source_tables": ["test_case", "test_case_type", "test_case_priorities", "application_version"],
        "primary_key": "test_case_id",
        "column_mappings": {
            "test_case_id": "test_case.id",
            "test_case_tsid": "test_case.tsid",
            "name": "test_case.name",
            "description": "test_case.description",
            "tenant_tsid": "test_case.tenant_tsid",
            "priority_id": "test_case.proirity_id",
            "priority": "test_case_priorities.name",
            "type": "test_case.type",
            "application_version_id": "test_case.application_version_id",
            "app_id": "application_version.application_id",
            "is_data_driven": "test_case.is_data_driven",
            "is_step_group": "test_case.is_step_group",
            "is_ai_generated": "test_case.is_ai_generated",
            "is_manual": "test_case.is_manual",
            "is_active": "test_case.is_active",
            "is_reviewed": "test_case.is_reviewed",
            "is_prerequisite_case": "test_case.is_prerequisite_case",
            "is_under_deletion": "test_case.is_under_deletion",
            "status_enum": "test_case.status_enum",
            "create_type_enum": "test_case.create_type_enum",
            "update_type_enum": "test_case.update_type_enum",
            "reviewed_by": "test_case.reviewed_by",
            "last_run_id": "test_case.last_run_id",
            "pre_requisite": "test_case.pre_requisite",
            "assignee": "test_case.assignee",
            "deleted": "test_case.deleted",
            "has_error": "test_case.has_error",
            "reviewed_at": "test_case.reviewed_at",
            "review_submitted_at": "test_case.review_submitted_at",
            "review_submitted_by": "test_case.review_submitted_by",
            "draft_at": "test_case.draft_at",
            "draft_by": "test_case.draft_by",
            "obsolete_by": "test_case.obsolete_by",
            "obsolete_at": "test_case.obsolete_At",
            "ready_at": "test_case.ready_at",
            "ready_by": "test_case.ready_by",
            "has_after_test": "test_case.has_after_test",
            "after_test_parent_id": "test_case.after_test_parent_id",
            "is_eligible_for_after_suite": "test_case.is_eligible_for_after_suite",
            "consider_visual_test_result": "test_case.consider_visual_test_result",
            "step_group_usage_count": "test_case.step_group_usage_count",
            "base_url": "test_case.base_url",
            "base_app_id": "test_case.base_app_id",
            "base_app_version_id": "test_case.base_app_version_id",
            "scenario_id": "test_case.scenario_id",
            "ai_description": "test_case.ai_description",
            "entity_version": "test_case.entity_version",
            "original_entity_id": "test_case.original_entity_id",
            "delete_marker": "test_case.delete_marker",
            "created_by_id": "test_case.created_by_id",
            "updated_by_id": "test_case.updated_by_id",
            "created_at": "test_case.created_date",
            "updated_at": "test_case.updated_date",
            "created_at_epoch": "test_case.created_at_epoch",
            "updated_at_epoch": "test_case.updated_at_epoch"
        }
    },
    
    "fct_test_steps": {
        "source_tables": ["nlp_test_case_step"],
        "primary_key": "step_id",
        "column_mappings": {
            "step_id": "nlp_test_case_step.id",
            "step_tsid": "nlp_test_case_step.tsid",
            "test_case_id": "nlp_test_case_step.test_case_id",
            "tenant_tsid": "nlp_test_case_step.tenant_tsid",
            "pre_requisite": "nlp_test_case_step.pre_requisite",
            "step_description": "nlp_test_case_step.step_description",
            "step_order": "nlp_test_case_step.step_order",
            "priority_enum": "nlp_test_case_step.priority_enum",
            "condition_type_enum": "nlp_test_case_step.condition_type_enum",
            "parent_id": "nlp_test_case_step.parent_id",
            "type_enum": "nlp_test_case_step.type_enum",
            "wait_time": "nlp_test_case_step.wait_time",
            "disabled": "nlp_test_case_step.disabled",
            "template_id": "nlp_test_case_step.template_id",
            "step_group_id": "nlp_test_case_step.step_group_id",
            "max_iterations": "nlp_test_case_step.max_iterations",
            "max_retries": "nlp_test_case_step.max_retries",
            "action": "nlp_test_case_step.action",
            "teststep_key": "nlp_test_case_step.teststep_key",
            "expected_result": "nlp_test_case_step.excepted_result",
            "visual_enabled": "nlp_test_case_step.visual_enabled",
            "accessibility_enabled": "nlp_test_case_step.accessibility_enabled",
            "ignore_step_result": "nlp_test_case_step.ignore_step_result",
            "is_manual": "nlp_test_case_step.is_manual",
            "is_invalid": "nlp_test_case_step.is_invalid",
            "custom_fields": "nlp_test_case_step.custom_fields",
            "data_map": "nlp_test_case_step.data_map",
            "copied_from": "nlp_test_case_step.copied_from",
            "phone_number_id": "nlp_test_case_step.phone_number_id",
            "mail_box_id": "nlp_test_case_step.mail_box_id",
            "block_id": "nlp_test_case_step.block_id",
            "kibbutz_plugin_nlp_data": "nlp_test_case_step.kibbutz_plugin_nlp_data",
            "kibbutz_plugin_tdf_data": "nlp_test_case_step.kibbutz_plugin_tdf_data",
            "kibbutz_plugin_nlp_id": "nlp_test_case_step.kibbutz_plugin_nlp_id",
            "screenshot_name": "nlp_test_case_step.screenshot_name",
            "pagesource_name": "nlp_test_case_step.pagesource_name",
            "step_level_screenshot": "nlp_test_case_step.step_level_screenshot",
            "entity_version": "nlp_test_case_step.entity_version",
            "delete_marker": "nlp_test_case_step.delete_marker",
            "original_entity_id": "nlp_test_case_step.original_entity_id",
            "create_type_enum": "nlp_test_case_step.create_type_enum",
            "update_type_enum": "nlp_test_case_step.update_type_enum",
            "created_by_id": "nlp_test_case_step.created_by_id",
            "updated_by_id": "nlp_test_case_step.updated_by_id",
            "created_at": "nlp_test_case_step.created_date",
            "updated_at": "nlp_test_case_step.updated_date",
            "created_at_epoch": "nlp_test_case_step.created_at_epoch",
            "updated_at_epoch": "nlp_test_case_step.updated_at_epoch"
        }
    },
    
    "fct_executions": {
        "source_tables": ["execution", "execution_result", "application_version"],
        "primary_key": "execution_id",
        "column_mappings": {
            "execution_id": "execution.id",
            "execution_tsid": "execution.tsid",
            "name": "execution.name",
            "tenant_tsid": "execution.tenant_tsid",
            "app_version_id": "execution.app_version_id",
            "description": "execution.description",
            "step_time_out": "execution.step_time_out",
            "page_time_out": "execution.page_time_out",
            "is_manual": "execution.is_manual",
            "type": "execution.type",
            "last_run_id": "execution.last_run_id",
            "match_browser_version": "execution.match_browser_version",
            "entity_type": "execution.entity_type",
            "screen_shot_enum": "execution.screen_shot_enum",
            "screen_shot_mode": "execution.screen_shot_mode",
            "execution_type_enum": "execution.execution_type_enum",
            "recovery_action_enum": "execution.recovery_action_enum",
            "on_aborted_action_enum": "execution.on_aborted_action_enum",
            "re_run_on_failure_enum": "execution.re_run_on_failure_enum",
            "on_group_prequisite_fail_enum": "execution.on_group_prequisite_fail_enum",
            "on_testcase_prerequisite_fail_enum": "execution.on_testcase_prerequisite_fail_enum",
            "on_step_prequisite_fail_enum": "execution.on_step_prequisite_fail_enum",
            "slack_connector_notification_enabled": "execution.slack_connector_notification_enabled",
            "ms_teams_connector_notification_enabled": "execution.ms_teams_connector_notification_enabled",
            "google_chat_connector_notification_enabled": "execution.google_chat_connector_notification_enabled",
            "visual_test_result_time_out": "execution.visual_test_result_time_out",
            "accessibility_test_enabled": "execution.accessibility_test_enabled",
            "wcag_version": "execution.wcag_version",
            "app_id": "application_version.application_id",
            "result_id": "execution_result.id",
            "result_tsid": "execution_result.tsid",
            "result_execution_id": "execution_result.execution_id",
            "result_enum": "execution_result.result_enum",
            "status_enum": "execution_result.status_enum",
            "message": "execution_result.message",
            "start_time": "execution_result.start_time",
            "end_time": "execution_result.end_time",
            "duration": "execution_result.duration",
            "executed_by": "execution_result.executed_by",
            "triggered_by": "execution_result.executed_by",
            "build_no": "execution_result.build_no",
            "is_inprogress": "execution_result.is_inprogress",
            "triggered_type": "execution_result.triggered_type",
            "trigger_type": "execution_result.triggered_type",
            "scheduled_id": "execution_result.scheduled_id",
            "re_run_parent_id": "execution_result.re_run_parent_id",
            "re_run_type_enum": "execution_result.re_run_type_enum",
            "latest_result": "execution_result.latest_result",
            "automation_result": "execution_result.automation_result",
            "result_type": "execution_result.result_type",
            "consolidated_duration": "execution_result.consolidated_duration",
            "run_test_cases_in_parallel": "execution_result.run_test_cases_in_parallel",
            "run_test_suites_in_parallel": "execution_result.run_test_suites_in_parallel",
            "execution_visual_result": "execution_result.execution_visual_result",
            "design_asset_visual_result": "execution_result.design_asset_visual_result",
            "consolidated_visual_result": "execution_result.consolidated_visual_result",
            "total_count": "execution_result.total_count",
            "passed_count": "execution_result.passed_count",
            "failed_count": "execution_result.failed_count",
            "stopped_count": "execution_result.stopped_count",
            "not_executed_count": "execution_result.not_executed_count",
            "queued_count": "execution_result.queued_count",
            "running_count": "execution_result.running_count",
            "is_visually_passed": "execution_result.is_visually_passed",
            "created_by_id": "execution.created_by_id",
            "updated_by_id": "execution.updated_by_id",
            "created_at": "execution.created_date",
            "updated_at": "execution.updated_date",
            "created_at_epoch": "execution.created_at_epoch",
            "updated_at_epoch": "execution.updated_at_epoch"
        }
    },
    
    "fct_test_results": {
        "source_tables": ["test_case_result"],
        "primary_key": "result_id",
        "column_mappings": {
            "result_id": "test_case_result.id",
            "result_tsid": "test_case_result.tsid",
            "test_case_id": "test_case_result.test_case_id",
            "test_case_tsid": "test_case_result.test_case_tsid",
            "test_case_type_id": "test_case_result.test_case_type_id",
            "test_case_priority_id": "test_case_result.test_case_priority_id",
            "execution_result_id": "test_case_result.execution_result_id",
            "execution_result_tsid": "test_case_result.execution_result_tsid",
            "tenant_id": "test_case_result.tenant_id",
            "tenant_tsid": "test_case_result.tenant_tsid",
            "project_id": "test_case_result.project_id",
            "project_tsid": "test_case_result.project_tsid",
            "test_suite_id": "test_case_result.test_suite_id",
            "test_suite_tsid": "test_case_result.test_suite_tsid",
            "test_suite_result_id": "test_case_result.test_suite_result_id",
            "test_suite_result_tsid": "test_case_result.test_suite_result_tsid",
            "result_enum": "test_case_result.result_enum",
            "status_enum": "test_case_result.status_enum",
            "is_step_group": "test_case_result.is_step_group",
            "is_data_driven": "test_case_result.is_data_driven",
            "data_driven_test_case_id": "test_case_result.data_driven_test_case_id",
            "data_driven_test_case_tsid": "test_case_result.data_driven_test_case_tsid",
            "test_data_id": "test_case_result.test_data_id",
            "test_data_tsid": "test_case_result.test_data_tsid",
            "data_iteration": "test_case_result.data_iteration",
            "group_result_id": "test_case_result.group_result_id",
            "group_result_tsid": "test_case_result.group_result_tsid",
            "parent_id": "test_case_result.parent_id",
            "parent_tsid": "test_case_result.parent_tsid",
            "prerequisite_test_case_result_id": "test_case_result.prerequisite_test_case_result_id",
            "prerequisite_test_case_result_tsid": "test_case_result.prerequisite_test_case_result_tsid",
            "re_run_parent_id": "test_case_result.re_run_parent_id",
            "re_run_parent_tsid": "test_case_result.re_run_parent_tsid",
            "build_number": "test_case_result.build_number",
            "message": "test_case_result.message",
            "user_marked_status": "test_case_result.user_marked_status",
            "start_time": "test_case_result.start_time",
            "end_time": "test_case_result.end_time",
            "duration": "test_case_result.duration",
            "position": "test_case_result.position",
            "is_visual_test_case": "test_case_result.is_visual_test_case",
            "is_fixed": "test_case_result.is_fixed",
            "is_flaky": "test_case_result.is_flaky",
            "flaky_action": "test_case_result.flaky_action",
            "test_case_status": "test_case_result.test_case_status",
            "created_by_id": "test_case_result.created_by_id",
            "updated_by_id": "test_case_result.updated_by_id",
            "created_at": "test_case_result.created_date",
            "updated_at": "test_case_result.updated_date",
            "created_at_epoch": "test_case_result.created_at_epoch",
            "updated_at_epoch": "test_case_result.updated_at_epoch"
        }
    },
    
    "dim_elements": {
        "source_tables": ["element_locators"],
        "primary_key": "element_id",
        "column_mappings": {
            "element_id": "element_locators.id",
            "element_tsid": "element_locators.tsid",
            "name": "element_locators.name",
            "element_name": "element_locators.element_name",
            "screen_name_id": "element_locators.screen_name_id",
            "screen_name_tsid": "element_locators.screen_name_tsid",
            "project_id": "element_locators.project_id",
            "project_tsid": "element_locators.project_tsid",
            "tenant_id": "element_locators.tenant_id",
            "tenant_tsid": "element_locators.tenant_tsid",
            "application_version_id": "element_locators.application_version_id",
            "value": "element_locators.value",
            "type": "element_locators.type",
            "is_duplicated": "element_locators.is_duplicated",
            "is_dynamic": "element_locators.is_dynamic",
            "created_by_id": "element_locators.created_by_id",
            "updated_by_id": "element_locators.updated_by_id",
            "created_at": "element_locators.created_date",
            "updated_at": "element_locators.updated_date",
            "created_at_epoch": "element_locators.created_at_epoch",
            "updated_at_epoch": "element_locators.updated_at_epoch"
        }
    },
    
    "dim_test_data": {
        "source_tables": ["test_data"],
        "primary_key": "test_data_id",
        "column_mappings": {
            "test_data_id": "test_data.id",
            "test_data_uuid": "test_data.tsid",
            "name": "test_data.test_data_name",
            "data_json": "test_data.columns",
            "app_id": "test_data.version_id",
            "tenant_id": "test_data.tenant_tsid",
            "created_by_id": "test_data.created_by_id",
            "updated_by_id": "test_data.updated_by_id",
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
            "agent_uuid": "agents.tsid",
            "name": "agents.unique_id",
            "config_json": "agents.browser_list",
            "status": "agents.status",
            "tenant_id": "agents.tenant_tsid",
            "system_id": "agents.system_id",
            "version": "agents.agent_version",
            "enabled": "agents.enabled",
            "visible_to_all": "agents.visible_to_all",
            "mobile_enabled": "agents.mobile_enabled",
            "is_active": "agents.is_active",
            "is_docker": "agents.is_docker",
            "is_service": "agents.is_service",
            "is_obsolete": "agents.is_obsolete",
            "agent_type": "agents.agent_type",
            "drifter_version": "agents.drifter_version",
            "max_sessions": "agents.max_sessions",
            "log_level": "agents.log_level",
            "last_pinged_at": "agents.last_pinged_at",
            "created_by_id": "agents.created_by_id",
            "updated_by_id": "agents.updated_by_id",
            "created_at": "agents.created_date",
            "updated_at": "agents.updated_date",
            "created_at_epoch": "agents.created_at_epoch",
            "updated_at_epoch": "agents.updated_at_epoch"
        }
    },
    
    "fct_api_steps": {
        "source_tables": ["api_steps", "api_step_result_response"],
        "primary_key": "api_step_id",
        "column_mappings": {
            "api_step_id": "api_steps.id",
            "test_step_id": "api_steps.step_id",  # Foreign key to nlp_test_case_step
            "tenant_id": "api_steps.tenant_tsid",
            "user_id": "api_steps.created_by_id",
            "method": "api_steps.request_type",  # request_type is the HTTP method
            "url": "api_steps.url",
            "api_type": "api_steps.api_type",
            "authentication_type": "api_steps.authentication_type",
            "body_type": "api_steps.body_type",
            "title": "api_steps.title",  # Added title field
            "url_type": "api_steps.url_type",  # Added url_type
            "step_result_uuid": "api_steps.step_result_uuid",  # Added
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
    
    "dim_test_suites": {
        "source_tables": ["test_case_group", "application_version"],
        "primary_key": "test_suite_id",
        "column_mappings": {
            "test_suite_id": "test_case_group.id",
            "test_suite_tsid": "test_case_group.tsid",
            "name": "test_case_group.name",
            "description": "test_case_group.description",
            "tenant_tsid": "test_case_group.tenant_tsid",
            "app_version_id": "test_case_group.app_version_id",
            "action_id": "test_case_group.action_id",
            "pre_requisite": "test_case_group.pre_requisite",
            "is_manual": "test_case_group.is_manual",
            "entity_type": "test_case_group.entity_type",
            "total_test_cases_count": "test_case_group.total_test_cases_count",
            "after_suite_parent_id": "test_case_group.after_suite_parent_id",
            "has_after_suite": "test_case_group.has_after_suite",
            "dynamic_filter_id": "test_case_group.dynamic_filter_id",
            "dynamic_filter_query": "test_case_group.dynamic_filter_query",
            "dynamic_pageable": "test_case_group.dynamic_pageable",
            "copied_from": "test_case_group.copied_from",
            "last_run_id": "test_case_group.last_run_id",
            "created_by_id": "test_case_group.created_by_id",
            "updated_by_id": "test_case_group.updated_by_id",
            "created_at": "test_case_group.created_date",
            "updated_at": "test_case_group.updated_date",
            "created_at_epoch": "test_case_group.created_at_epoch",
            "updated_at_epoch": "test_case_group.updated_at_epoch",
            "app_id": "application_version.application_id",
            "suite_id": "test_case_group.id"
        }
    },
    
    "fct_cross_tenant_metrics": {
        "source_tables": ["test_plan_result_metrics"],
        "primary_key": "metric_id",
        "column_mappings": {
            "metric_id": "test_plan_result_metrics.id",
            "tenant_id": "test_plan_result_metrics.tenant_tsid",
            "test_plan_result_id": "test_plan_result_metrics.test_plan_result_id",
            "result": "test_plan_result_metrics.result",
            "latest_result": "test_plan_result_metrics.latest_result",
            "total_count": "test_plan_result_metrics.total_count",
            "failed_count": "test_plan_result_metrics.failed_count",
            "passed_count": "test_plan_result_metrics.passed_count",
            "stopped_count": "test_plan_result_metrics.stopped_count",
            "not_executed_count": "test_plan_result_metrics.not_executed_count",
            "running_count": "test_plan_result_metrics.running_count",
            "queued_count": "test_plan_result_metrics.queued_count",
            "duration": "test_plan_result_metrics.duration",
            "consolidated_duration": "test_plan_result_metrics.consolidated_duration",
            "created_at": "test_plan_result_metrics.created_date",
            "updated_at": "test_plan_result_metrics.updated_date"
        }
    },
    
    "fct_test_plan_results": {
        "source_tables": ["test_plan_result_metrics", "execution_result"],
        "primary_key": "test_plan_result_id",
        "column_mappings": {
            "test_plan_result_id": "test_plan_result_metrics.test_plan_result_id",
            "test_plan_id": "execution_result.execution_id",
            "tenant_id": "test_plan_result_metrics.tenant_tsid",
            "user_id": "execution_result.executed_by",
            "status": "test_plan_result_metrics.result",
            "latest_status": "test_plan_result_metrics.latest_result",
            "start_time": "execution_result.start_time",
            "end_time": "execution_result.end_time",
            "duration_seconds": "execution_result.duration",
            "total_count": "test_plan_result_metrics.total_count",
            "passed_count": "test_plan_result_metrics.passed_count",
            "failed_count": "test_plan_result_metrics.failed_count",
            "trigger_type": "execution_result.triggered_type",
            "created_at": "execution_result.created_date",
            "updated_at": "execution_result.updated_date"
        }
    },
    
    "fct_agent_activity": {
        "source_tables": ["ai_agent_activity_log"],
        "primary_key": "activity_id",
        "column_mappings": {
            "activity_id": "ai_agent_activity_log.id",
            "agent_id": "ai_agent_activity_log.ai_agent_workflow_id",
            "tenant_id": "ai_agent_activity_log.tenant_tsid",
            "user_id": "ai_agent_activity_log.created_by_id",
            "activity_type": "ai_agent_activity_log.agent_type",
            "description": "ai_agent_activity_log.message",
            "status": "ai_agent_activity_log.status",
            "start_time": "ai_agent_activity_log.start_time",
            "end_time": "ai_agent_activity_log.end_time",
            "created_at": "ai_agent_activity_log.created_date",
            "updated_at": "ai_agent_activity_log.updated_date"
        }
    },
    
    "fct_audit_events": {
        "source_tables": ["audit_history"],
        "primary_key": "event_id",
        "column_mappings": {
            "event_id": "audit_history.id",
            "event_tsid": "audit_history.tsid",
            "event_uuid": "audit_history.uuid",
            "tenant_tsid": "audit_history.tenant_tsid",
            "entity_tag": "audit_history.entity_tag",
            "entity_type": "audit_history.entity_model",
            "entity_parent_id": "audit_history.entity_parent_id",
            "entity_id": "audit_history.entity_id",
            "action": "audit_history.entity_action",
            "previous_entity_data": "audit_history.previous_entity_data",
            "new_entity_data": "audit_history.new_entity_data",
            "changes_json": "audit_history.transform_context",
            "actor_id": "audit_history.actor_id",
            "actor_account_uuid": "audit_history.actor_account_uuid",
            "actor_uuid": "audit_history.actor_uuid",
            "ip_address": "audit_history.client_ip_address",
            "timestamp": "audit_history.action_date",
            "custom_fields": "audit_history.custom_fields",
            "status": "audit_history.status",
            "message": "audit_history.message",
            "created_by_id": "audit_history.created_by_id",
            "updated_by_id": "audit_history.updated_by_id",
            "created_at": "audit_history.created_date",
            "updated_at": "audit_history.updated_date",
            "created_at_epoch": "audit_history.created_at_epoch",
            "updated_at_epoch": "audit_history.updated_at_epoch"
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