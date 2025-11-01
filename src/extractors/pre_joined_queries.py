"""
Pre-Joined Extraction Queries
=============================
All joins done in MySQL for efficiency and correctness.
No cross-database joins needed!
"""

from typing import Optional

# Identity Database Queries
IDENTITY_QUERIES = {
    "dim_users": """
        SELECT 
            u.id as user_id,
            u.organization_tsid,
            u.uuid,
            u.first_name,
            u.last_name,
            u.company_name,
            u.phone,
            u.email,
            u.status,
            u.type,
            u.deleted,
            u.last_accessed_account,
            u.password_last_updated_at,
            u.expiry_remainder_notified_at,
            u.last_logged_in_at,
            u.created_date as created_at,
            u.updated_date as updated_at,
            -- User preferences
            COALESCE(up.is_2fa_enabled, 0) as is_2fa_enabled,
            -- User accounts (get primary account)
            ua.id as user_account_id,
            ua.account_id,
            ua.uuid as account_uuid,
            ua.join_confirmed,
            ua.account_owner as is_account_owner,
            ua.deleted as account_deleted,
            ua.status as account_status
        FROM users u
        LEFT JOIN user_preferences up ON u.id = up.user_id
        LEFT JOIN user_accounts ua ON u.id = ua.user_id 
            -- Get any account (no is_primary field)
    """,
    
    "dim_organizations": """
        SELECT 
            o.tsid as organization_id,
            o.uuid as organization_uuid,
            o.name,
            op.idle_session_timeout,
            op.password_expiry_days,
            op.max_failed_login_attempts,
            o.created_by_id,
            o.updated_by_id,
            o.created_date as created_at,
            o.updated_date as updated_at
        FROM organizations o
        LEFT JOIN organization_policy op ON o.tsid = op.organization_tsid
    """,
    
    "dim_accounts": """
        SELECT 
            a.id as account_id,
            a.organization_tsid,
            a.uuid,
            a.name,
            a.url,
            a.status,
            a.product_id,
            -- SMTP configuration (these tables might not exist in all setups)
            sc.type as smtp_type,
            sc.host as smtp_host,
            sc.port as smtp_port,
            sc.email as smtp_email,
            -- Auth modules (these tables might not exist in all setups)
            am.type as auth_module_type,
            am.provider as auth_provider,
            am.enabled as auth_enabled,
            a.created_by_id,
            a.updated_by_id,
            a.created_date as created_at,
            a.updated_date as updated_at
        FROM accounts a
        LEFT JOIN smtp_configuration sc ON a.id = sc.account_id
        LEFT JOIN authentication_modules am ON a.id = am.account_id
    """
}

# Master Database Queries
MASTER_QUERIES = {
    "dim_tenants": """
        SELECT 
            t.id as tenant_id,
            t.tsid as tenant_tsid,
            t.first_name,
            t.last_name,
            t.email,
            t.company,
            t.phone,
            t.domain,
            t.alternative_email,
            t.features,
            t.lead_id,
            t.contact_id,
            t.hubspot_contact_id,
            t.is_play_ground,
            t.referrer,
            t.deleted,
            t.shard,
            t.database_name,
            t.is_beta,
            t.signup_type,
            t.skip_support_channel_notifications,
            t.status_enum,
            t.maintenance_enum,
            t.report_customization_enabled,
            t.salesforce_account_id,
            t.subscribed_to_emails,
            t.identity_account_uuid,
            t.identity_organization_tsid,
            t.created_date as created_at,
            t.updated_date as updated_at,
            -- Subscription details
            s.id as subscription_id,
            s.plan,
            s.plan as plan_type,
            s.chargebee_id,
            s.no_of_projects,
            s.cloud_automated_minutes_per_month,
            s.local_automated_minutes_per_month,
            s.block_access,
            s.trial_period,
            s.allowed_users,
            s.max_projects,
            s.next_renewal_at,
            s.total_parallel_runs,
            s.additional_parallel_runs_on_cloud,
            s.only_local_parallel_runs,
            s.additional_parallel_runs_on_local,
            s.free_additional_users,
            s.allowed_queue_size,
            s.free_local_only_mobile_sessions,
            s.status_enum as subscription_status,
            s.cancelled_date,
            s.deployment_modal,
            -- Billing address
            ba.address_line1 as billing_address_line1,
            ba.address_line2 as billing_address_line2,
            ba.country as billing_country,
            ba.state as billing_state,
            ba.city as billing_city,
            ba.zip_code as billing_zip
        FROM tenants t
        LEFT JOIN subscriptions s ON t.id = s.tenant_id
        LEFT JOIN billing_addresses ba ON t.id = ba.tenant_id
    """,
    
    "brg_tenant_features": """
        SELECT 
            CAST(tf.tenant_id AS SIGNED) as tenant_id,
            CAST(tf.id AS SIGNED) as feature_id,
            tf.created_date as created_at,
            tf.updated_date as updated_at,
            CAST(tf.id AS SIGNED) as tenant_feature_id,
            CAST(tf.name AS CHAR CHARACTER SET utf8mb4) as feature_name,
            CAST(tf.is_add_on AS SIGNED) as is_add_on
        FROM tenant_features tf
        WHERE tf.tenant_id IS NOT NULL
        AND tf.id IS NOT NULL
    """,
    
    "dim_features": """
        SELECT DISTINCT
            CAST(tf.id AS SIGNED) as feature_id,
            CAST(tf.name AS CHAR CHARACTER SET utf8mb4) as name,
            CAST(tf.is_add_on AS SIGNED) as is_premium,
            MIN(CAST(tf.tenant_id AS SIGNED)) as tenant_id,
            MIN(tf.created_date) as created_at,
            MAX(tf.updated_date) as updated_at
        FROM tenant_features tf
        WHERE tf.id IS NOT NULL
        AND tf.name IS NOT NULL
        GROUP BY tf.id, tf.name, tf.is_add_on
    """,
    
    "dim_data_generators": """
        SELECT 
            dg.id as generator_id,
            dg.tsid as generator_tsid,
            dg.name,
            dg.display_name,
            dg.unique_id,
            dg.fixed_arguments,
            dg.has_data_map,
            dg.file_id,
            dg.created_at_epoch,
            dg.updated_at_epoch,
            dgf.class_package,
            dgf.class_name,
            dgf.display_name as file_display_name,
            dgf.lib_type_enum as lib_type
        FROM data_generators dg
        LEFT JOIN data_generator_files dgf ON dg.file_id = dgf.id
    """,
    
    "dim_nlp_templates": """
        SELECT 
            ntcst.id as template_id,
            ntcst.tsid as template_tsid,
            ntcst.keyword,
            ntcst.grammer,
            ntcst.display_name,
            ntcst.snippet_enabled,
            ntcst.snippet_class,
            ntcst.deprecated,
            ntcst.is_actionable,
            ntcst.is_verifiable,
            ntcst.condition_type,
            ntcst.type,
            ntcst.import_to_web,
            ntcst.import_to_mobile_web,
            ntcst.import_to_android_native,
            ntcst.import_to_ios_native,
            ntcst.import_to_rest_native,
            ntcst.import_to_salesforce,
            ntcst.app_type_enum,
            ntcst.loop_type,
            ntcst.position,
            ntcst.application_type_id,
            ntcst.optional_template_id,
            ntcst.api_supported,
            ntcst.screenshot_strategy,
            ntcst.created_at_epoch,
            ntcst.updated_at_epoch
        FROM nlp_test_case_step_templates ntcst
    """,
    
    "dim_object_types": """
        SELECT 
            ot.id as object_type_id,
            ot.name,
            ot.display_name,
            ot.description,
            ot.priority,
            ot.deprecated,
            ot.created_at_epoch,
            ot.updated_at_epoch
        FROM object_types ot
    """
}

# Tenant Database Queries (per tenant DB)
TENANT_QUERIES = {
    "dim_projects": """
        SELECT 
            p.id as project_id,
            p.tsid as project_tsid,
            p.name,
            p.description,
            p.tenant_tsid,
            p.has_multiple_apps,
            p.has_multiple_versions,
            p.demo,
            p.time_zone,
            p.project_type_enum,
            p.created_by_id,
            p.updated_by_id,
            p.created_date as created_at,
            p.updated_date as updated_at,
            p.created_at_epoch,
            p.updated_at_epoch
        FROM project p
    """,
    
    "dim_applications": """
        SELECT 
            a.id as app_id,
            a.tsid as app_tsid,
            a.name,
            a.description,
            a.tenant_tsid,
            a.project_id,
            a.custom_fields,
            a.type_enum,
            a.created_by_id,
            a.updated_by_id,
            a.created_date as created_at,
            a.updated_date as updated_at,
            a.created_at_epoch,
            a.updated_at_epoch
        FROM application a
    """,
    
    "dim_test_cases": """
        SELECT 
            tc.id as test_case_id,
            tc.tsid as test_case_tsid,
            tc.name,
            tc.description,
            tc.tenant_tsid,
            tc.proirity_id as priority_id,
            tcp.name as priority,
            tc.type,
            tc.application_version_id,
            av.application_id as app_id,
            tc.is_data_driven,
            tc.is_step_group,
            tc.is_ai_generated,
            tc.is_manual,
            tc.is_active,
            tc.is_reviewed,
            tc.is_prerequisite_case,
            tc.is_under_deletion,
            tc.status_enum,
            tc.create_type_enum,
            tc.update_type_enum,
            tc.reviewed_by,
            tc.last_run_id,
            tc.pre_requisite,
            tc.assignee,
            tc.deleted,
            tc.has_error,
            tc.reviewed_at,
            tc.review_submitted_at,
            tc.review_submitted_by,
            tc.draft_at,
            tc.draft_by,
            tc.obsolete_by,
            tc.obsolete_At as obsolete_at,
            tc.ready_at,
            tc.ready_by,
            tc.has_after_test,
            tc.after_test_parent_id,
            tc.is_eligible_for_after_suite,
            tc.consider_visual_test_result,
            tc.step_group_usage_count,
            tc.base_url,
            tc.base_app_id,
            tc.base_app_version_id,
            tc.scenario_id,
            tc.ai_description,
            tc.entity_version,
            tc.original_entity_id,
            tc.delete_marker,
            tc.created_by_id,
            tc.updated_by_id,
            tc.created_date as created_at,
            tc.updated_date as updated_at,
            tc.created_at_epoch,
            tc.updated_at_epoch
        FROM test_case tc
        LEFT JOIN test_case_priorities tcp ON tc.proirity_id = tcp.id
        LEFT JOIN application_version av ON tc.application_version_id = av.id
    """,
    
    "fct_executions": """
        SELECT 
            er.id as execution_id,
            er.tsid as execution_tsid,
            e.name,
            e.tenant_tsid,
            av.id as app_version_id,
            e.description,
            e.step_time_out,
            e.page_time_out,
            e.is_manual,
            e.type,
            er.last_run_id,
            e.match_browser_version,
            e.screen_shot_enum,
            e.screen_shot_mode,
            e.execution_type_enum,
            e.recovery_action_enum,
            e.on_aborted_action_enum,
            e.re_run_on_failure_enum,
            e.on_group_prequisite_fail_enum,
            e.on_testcase_prerequisite_fail_enum,
            e.on_step_prequisite_fail_enum,
            e.slack_connector_notification_enabled,
            e.ms_teams_connector_notification_enabled,
            e.google_chat_connector_notification_enabled,
            e.visual_test_result_time_out,
            e.accessibility_test_enabled,
            e.wcag_version,
            av.application_id as app_id,
            -- Result fields
            er.id as result_id,
            er.tsid as result_tsid,
            er.execution_id as result_execution_id,
            er.result_enum,
            er.status_enum,
            er.message,
            er.start_time,
            er.end_time,
            er.duration,
            er.executed_by,
            er.executed_by as triggered_by,
            er.build_no,
            er.is_inprogress,
            er.triggered_type,
            er.triggered_type as trigger_type,
            er.scheduled_id,
            er.re_run_parent_id,
            er.re_run_type_enum,
            er.latest_result,
            er.automation_result,
            er.result_type,
            er.consolidated_duration,
            er.run_test_cases_in_parallel,
            er.run_test_suites_in_parallel,
            er.execution_visual_result,
            er.design_asset_visual_result,
            er.consolidated_visual_result,
            er.total_count,
            er.passed_count,
            er.failed_count,
            er.stopped_count,
            er.not_executed_count,
            er.queued_count,
            er.running_count,
            er.is_visually_passed,
            er.created_by_id,
            er.updated_by_id,
            er.created_date as created_at,
            er.updated_date as updated_at,
            er.created_at_epoch,
            er.updated_at_epoch
        FROM execution_result er
        LEFT JOIN execution e ON er.execution_id = e.id
        LEFT JOIN application_version av ON e.application_version_id = av.id
    """,
    
    "fct_test_steps": """
        SELECT 
            ntcs.id as step_id,
            ntcs.tsid as step_tsid,
            ntcs.test_case_id,
            ntcs.tenant_tsid,
            ntcs.pre_requisite,
            ntcs.step_description,
            ntcs.step_order,
            ntcs.priority_enum,
            ntcs.condition_type_enum,
            ntcs.parent_id,
            ntcs.type_enum,
            ntcs.wait_time,
            ntcs.disabled,
            ntcs.template_id,
            ntcs.step_group_id,
            ntcs.max_iterations,
            ntcs.max_retries,
            ntcs.action,
            ntcs.teststep_key,
            ntcs.excepted_result as expected_result,
            ntcs.visual_enabled,
            ntcs.accessibility_enabled,
            ntcs.ignore_step_result,
            ntcs.is_manual,
            ntcs.is_invalid,
            ntcs.custom_fields,
            ntcs.data_map,
            ntcs.copied_from,
            ntcs.phone_number_id,
            ntcs.mail_box_id,
            ntcs.block_id,
            ntcs.kibbutz_plugin_nlp_data,
            ntcs.kibbutz_plugin_tdf_data,
            ntcs.kibbutz_nlp_data_map,
            ntcs.kibbutz_tdf_data_map,
            ntcs.data_source_id,
            ntcs.iteration_data_type_enum,
            ntcs.mobile_step_action,
            ntcs.mobile_test_data,
            ntcs.mobile_input,
            ntcs.mobile_element,
            ntcs.platform,
            ntcs.type_override,
            ntcs.wait_time_override,
            ntcs.data_map_override,
            ntcs.parent_id_override,
            ntcs.natural_text_action,
            ntcs.natural_text_action_type,
            ntcs.rest_api_nlp_data,
            ntcs.original_step_id,
            ntcs.exception_is_expected_json,
            ntcs.entity_version,
            ntcs.original_entity_id,
            ntcs.created_by_id,
            ntcs.updated_by_id,
            ntcs.created_date as created_at,
            ntcs.updated_date as updated_at,
            ntcs.created_at_epoch,
            ntcs.updated_at_epoch
        FROM nlp_test_case_step ntcs
    """,
    
    "dim_test_suites": """
        SELECT 
            ts.id as test_suite_id,
            ts.tsid as test_suite_tsid,
            ts.name,
            ts.description,
            ts.tenant_tsid,
            ts.project_id,
            ts.application_version_id,
            av.application_id as app_id,
            ts.is_disabled,
            ts.created_by_id,
            ts.updated_by_id,
            ts.created_date as created_at,
            ts.updated_date as updated_at,
            ts.created_at_epoch,
            ts.updated_at_epoch
        FROM test_suite ts
        LEFT JOIN application_version av ON ts.application_version_id = av.id
    """,
    
    "fct_test_results": """
        SELECT 
            tr.id as test_result_id,
            tr.tsid as test_result_tsid,
            tr.test_case_id,
            tr.test_suite_id,
            tr.execution_result_id,
            tr.test_plan_result_id,
            tr.tenant_tsid,
            tr.start_time,
            tr.end_time,
            tr.duration,
            tr.result_enum,
            tr.status_enum,
            tr.is_visually_passed,
            tr.message,
            tr.metadata,
            tr.api_test_result_id,
            tr.iteration,
            tr.test_machine_id,
            tr.re_run_parent_id,
            tr.re_run_type_enum,
            tr.entity_version,
            tr.original_entity_id,
            tr.is_queued,
            tr.automation_result,
            tr.is_migrated_execution,
            tr.failed_or_aborted_by,
            tr.result_type,
            tr.test_case_order,
            tr.test_suite_order,
            tr.execution_initiated_date,
            tr.execution_order,
            tr.visual_result,
            tr.design_asset_visual_result,
            tr.consolidated_visual_result,
            tr.created_by_id,
            tr.updated_by_id,
            tr.created_date as created_at,
            tr.updated_date as updated_at,
            tr.created_at_epoch,
            tr.updated_at_epoch
        FROM test_result tr
    """,
    
    "dim_test_data": """
        SELECT 
            td.id as test_data_id,
            td.tsid as test_data_tsid,
            td.name,
            td.tenant_tsid,
            td.description,
            td.last_used_at_epoch,
            td.created_by_id,
            td.updated_by_id,
            td.created_date as created_at,
            td.updated_date as updated_at,
            td.created_at_epoch,
            td.updated_at_epoch
        FROM test_data td
    """,
    
    "dim_agents": """
        SELECT 
            a.id as agent_id,
            a.tsid as agent_tsid,
            a.name,
            a.tenant_tsid,
            a.description,
            a.ip_address,
            a.port,
            a.is_active,
            a.disable,
            a.created_by_id,
            a.updated_by_id,
            a.created_date as created_at,
            a.updated_date as updated_at,
            a.created_at_epoch,
            a.updated_at_epoch
        FROM agent a
    """,
    
    "fct_agent_activity": """
        SELECT 
            al.id as activity_id,
            al.tsid as activity_tsid,
            al.agent_id,
            al.tenant_tsid,
            al.test_result_id,
            al.start_time,
            al.end_time,
            al.action,
            al.created_date as created_at,
            al.updated_date as updated_at,
            al.created_at_epoch,
            al.updated_at_epoch
        FROM agent_logs al
    """,
    
    "fct_audit_events": """
        SELECT 
            ae.id as event_id,
            ae.tsid as event_tsid,
            ae.tenant_tsid,
            ae.user_id,
            ae.event_type,
            ae.event_data_json,
            ae.event_time,
            ae.created_date as created_at,
            ae.updated_date as updated_at,
            ae.created_at_epoch,
            ae.updated_at_epoch
        FROM audit_event ae
    """,
    
    "dim_elements": """
        SELECT 
            e.id as element_id,
            e.tsid as element_tsid,
            e.name,
            e.tenant_tsid,
            e.application_version_id,
            e.screen_name,
            e.locator_type,
            e.locator_value,
            e.created_by_id,
            e.updated_by_id,
            e.created_date as created_at,
            e.updated_date as updated_at,
            e.created_at_epoch,
            e.updated_at_epoch
        FROM element e
    """,
    
    "fct_api_steps": """
        SELECT 
            ast.id as api_step_id,
            ast.tsid as api_step_tsid,
            ast.test_result_id,
            ast.tenant_tsid,
            ast.step_order,
            ast.request_url,
            ast.request_method,
            ast.response_status,
            ast.response_time,
            ast.created_date as created_at,
            ast.updated_date as updated_at,
            ast.created_at_epoch,
            ast.updated_at_epoch
        FROM api_step ast
    """,
    
    "fct_accessibility_results": """
        SELECT 
            ar.id as accessibility_result_id,
            ar.tsid as accessibility_result_tsid,
            ar.test_result_id,
            ar.tenant_tsid,
            ar.rule_id,
            ar.impact,
            ar.help_url,
            ar.created_date as created_at,
            ar.updated_date as updated_at,
            ar.created_at_epoch,
            ar.updated_at_epoch
        FROM accessibility_result ar
    """,
    
    "fct_cross_tenant_metrics": """
        SELECT 
            ctm.id as metric_id,
            ctm.tsid as metric_tsid,
            ctm.tenant_id,
            ctm.metric_date,
            ctm.total_users,
            ctm.active_users,
            ctm.total_test_cases,
            ctm.total_executions,
            ctm.created_date as created_at,
            ctm.updated_date as updated_at,
            ctm.created_at_epoch,
            ctm.updated_at_epoch
        FROM cross_tenant_metrics ctm
    """,
    
    "fct_test_plan_results": """
        SELECT 
            tpr.id as test_plan_result_id,
            tpr.tsid as test_plan_result_tsid,
            tpr.test_plan_id,
            tpr.tenant_tsid,
            tpr.execution_result_id,
            tpr.start_time,
            tpr.end_time,
            tpr.duration,
            tpr.result,
            tpr.created_date as created_at,
            tpr.updated_date as updated_at,
            tpr.created_at_epoch,
            tpr.updated_at_epoch
        FROM test_plan_result tpr
    """
}

def get_query_for_table(table_name: str, database_type: str) -> Optional[str]:
    """Get the pre-joined query for a specific table"""
    if database_type == 'identity':
        return IDENTITY_QUERIES.get(table_name)
    elif database_type == 'master':
        return MASTER_QUERIES.get(table_name)
    elif database_type == 'tenant':
        return TENANT_QUERIES.get(table_name)
    return None
