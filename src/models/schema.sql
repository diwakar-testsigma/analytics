-- Analytics database schema

-- ===== IDENTITY SCHEMA (4 tables) =====

CREATE TABLE IF NOT EXISTS dim_users (
    user_id INTEGER PRIMARY KEY,
    organization_id INTEGER,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    email VARCHAR(255),
    status VARCHAR(50),
    authenticity_token VARCHAR(255),
    password_hash VARCHAR(255),
    last_logged_in_at TIMESTAMP,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    is_2fa_enabled BOOLEAN DEFAULT FALSE,
    user_account_id INTEGER,
    account_id INTEGER,
    is_account_owner BOOLEAN DEFAULT FALSE,
    account_status VARCHAR(50),
    etl_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS dim_organizations (
    organization_id INTEGER PRIMARY KEY,
    organization_uuid VARCHAR(255),
    name VARCHAR(255) NOT NULL,
    created_by_id INTEGER,
    updated_by_id INTEGER,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    password_expiry_days INTEGER,
    idle_session_timeout INTEGER,
    max_failed_login_attempts INTEGER,
    etl_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS dim_accounts (
    account_id INTEGER PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    smtp_type VARCHAR(100),
    smtp_host VARCHAR(255),
    smtp_port INTEGER,
    auth_module_type VARCHAR(100),
    auth_enabled BOOLEAN,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    etl_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS fct_user_sessions (
    session_id VARCHAR(255) PRIMARY KEY,
    user_id VARCHAR(255),
    login_time TIMESTAMP NOT NULL,
    logout_time TIMESTAMP,
    last_access_time TIMESTAMP,
    max_inactive_interval INTEGER,
    etl_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- ===== MASTER SCHEMA (6 tables) =====

CREATE TABLE IF NOT EXISTS dim_tenants (
    tenant_id INTEGER PRIMARY KEY,
    tenant_tsid BIGINT,
    name VARCHAR(255),
    billing_country VARCHAR(100),
    billing_state VARCHAR(100),
    billing_city VARCHAR(100),
    billing_address VARCHAR(500),
    billing_zip VARCHAR(20),
    billing_phone VARCHAR(50),
    billing_email VARCHAR(255),
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    plan_type VARCHAR(100),
    subscription_status VARCHAR(50),
    allowed_users INTEGER,
    max_projects INTEGER,
    cloud_automated_minutes_per_month INTEGER,
    local_automated_minutes_per_month INTEGER,
    total_parallel_runs INTEGER,
    next_renewal_at TIMESTAMP,
    etl_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS brg_tenant_features (
    tenant_feature_id INTEGER PRIMARY KEY,
    tenant_id INTEGER NOT NULL,
    feature_id INTEGER,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    feature_name VARCHAR(255) NOT NULL,
    is_add_on BOOLEAN DEFAULT FALSE,
    etl_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS dim_features (
    feature_id INTEGER PRIMARY KEY,
    name VARCHAR(255),
    is_premium BOOLEAN DEFAULT FALSE,
    tenant_id INTEGER,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    etl_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS dim_data_generators (
    generator_id INTEGER PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    display_name VARCHAR(255),
    description TEXT,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    class_package VARCHAR(500),
    class_name VARCHAR(255),
    lib_type VARCHAR(100),
    etl_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS dim_nlp_templates (
    template_id INTEGER PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    content TEXT,
    language VARCHAR(50),
    category VARCHAR(100),
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    keyword VARCHAR(255),
    application_type VARCHAR(100),
    is_deprecated BOOLEAN DEFAULT FALSE,
    is_actionable BOOLEAN,
    is_verifiable BOOLEAN,
    type VARCHAR(100),
    api_supported BOOLEAN,
    etl_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS dim_object_types (
    object_type_id INTEGER PRIMARY KEY,
    name VARCHAR(255),
    display_name VARCHAR(255) NOT NULL,
    class_name VARCHAR(255),
    technology VARCHAR(100),
    package_name VARCHAR(500),
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    etl_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- ===== TENANT SCHEMA (17 tables) =====

CREATE TABLE IF NOT EXISTS dim_projects (
    project_id INTEGER PRIMARY KEY,
    project_uuid BIGINT,
    name VARCHAR(255),
    description TEXT,
    tenant_id BIGINT NOT NULL,
    created_by_id INTEGER,
    updated_by_id INTEGER,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    etl_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS dim_applications (
    app_id INTEGER PRIMARY KEY,
    app_uuid VARCHAR(255),
    name VARCHAR(255) NOT NULL,
    description TEXT,
    tenant_id INTEGER NOT NULL,
    status VARCHAR(50),
    created_by_id INTEGER,
    updated_by_id INTEGER,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    etl_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS dim_test_cases (
    test_case_id INTEGER PRIMARY KEY,
    test_case_uuid BIGINT,
    name VARCHAR(500),
    description TEXT,
    app_id INTEGER NOT NULL,
    tenant_id BIGINT,
    status VARCHAR(50),
    priority VARCHAR(100),
    created_by_id INTEGER,
    updated_by_id INTEGER,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    creation_type VARCHAR(50),
    is_manual BOOLEAN,
    is_ai_generated BOOLEAN,
    is_data_driven BOOLEAN,
    is_step_group BOOLEAN,
    etl_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS fct_test_steps (
    step_id INTEGER PRIMARY KEY,
    test_case_id INTEGER NOT NULL,
    tenant_id BIGINT,
    user_id INTEGER,
    step_order INTEGER NOT NULL,
    step_type VARCHAR(100),
    step_data TEXT,
    expected_result TEXT,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    element_id INTEGER,
    etl_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS fct_executions (
    execution_id INTEGER PRIMARY KEY,
    execution_uuid BIGINT,
    tenant_id BIGINT NOT NULL,
    user_id INTEGER,
    app_id INTEGER,
    test_suite_id INTEGER,
    status VARCHAR(50) NOT NULL,
    start_time TIMESTAMP NOT NULL,
    end_time TIMESTAMP,
    duration_seconds INTEGER,
    triggered_by VARCHAR(100),
    trigger_type VARCHAR(100),
    environment_type VARCHAR(50),
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    etl_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS fct_test_results (
    result_id INTEGER PRIMARY KEY,
    execution_id INTEGER NOT NULL,
    test_case_id INTEGER NOT NULL,
    tenant_id BIGINT,
    user_id INTEGER,
    status VARCHAR(50) NOT NULL,
    start_time TIMESTAMP NOT NULL,
    end_time TIMESTAMP,
    duration_seconds INTEGER,
    error_message TEXT,
    retry_count INTEGER DEFAULT 0,
    is_flaky BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    etl_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS dim_elements (
    element_id INTEGER PRIMARY KEY,
    element_uuid VARCHAR(255),
    locator TEXT,
    app_id INTEGER NOT NULL,
    tenant_id INTEGER,
    created_by_id INTEGER,
    updated_by_id INTEGER,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    locator_type VARCHAR(50),
    etl_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS dim_test_data (
    test_data_id INTEGER PRIMARY KEY,
    test_data_uuid VARCHAR(255),
    name VARCHAR(255),
    data_json TEXT,
    app_id INTEGER,
    tenant_id INTEGER,
    created_by_id INTEGER,
    updated_by_id INTEGER,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    data_id INTEGER,
    etl_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS dim_agents (
    agent_id INTEGER PRIMARY KEY,
    agent_uuid VARCHAR(255),
    name VARCHAR(255),
    config_json TEXT,
    status VARCHAR(50),
    tenant_id INTEGER NOT NULL,
    created_by_id INTEGER,
    updated_by_id INTEGER,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    version VARCHAR(50),
    is_active BOOLEAN,
    last_pinged_at TIMESTAMP,
    etl_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS fct_api_steps (
    api_step_id INTEGER PRIMARY KEY,
    test_step_id INTEGER NOT NULL,
    tenant_id BIGINT,
    user_id INTEGER,
    method VARCHAR(20),
    url TEXT,
    api_type VARCHAR(50),
    authentication_type VARCHAR(50),
    body_type VARCHAR(50),
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    etl_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS fct_accessibility_results (
    result_id INTEGER PRIMARY KEY,
    test_case_id INTEGER NOT NULL,
    execution_id INTEGER,
    tenant_id BIGINT,
    user_id INTEGER,
    check_type VARCHAR(100),
    status VARCHAR(50),
    message TEXT,
    severity VARCHAR(50),
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    page_url TEXT,
    critical_issues INTEGER DEFAULT 0,
    minor_issues INTEGER DEFAULT 0,
    serious_issues INTEGER DEFAULT 0,
    moderate_issues INTEGER DEFAULT 0,
    etl_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS dim_test_suites (
    test_suite_id INTEGER PRIMARY KEY,
    test_suite_uuid BIGINT,
    name VARCHAR(255) NOT NULL,
    description TEXT,
    app_id INTEGER,
    tenant_id BIGINT NOT NULL,
    status VARCHAR(50),
    created_by_id INTEGER,
    updated_by_id INTEGER,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    suite_id INTEGER,
    etl_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS fct_cross_tenant_metrics (
    metric_id INTEGER PRIMARY KEY,
    tenant_id BIGINT NOT NULL,
    test_plan_result_id INTEGER,
    result VARCHAR(100),
    latest_result VARCHAR(100),
    total_count INTEGER DEFAULT 0,
    failed_count INTEGER DEFAULT 0,
    passed_count INTEGER DEFAULT 0,
    stopped_count INTEGER DEFAULT 0,
    not_executed_count INTEGER DEFAULT 0,
    running_count INTEGER DEFAULT 0,
    queued_count INTEGER DEFAULT 0,
    duration INTEGER,
    consolidated_duration INTEGER,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    etl_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS fct_test_plan_results (
    test_plan_result_id INTEGER,
    test_plan_id INTEGER NOT NULL,
    tenant_id BIGINT NOT NULL,
    user_id INTEGER,
    status VARCHAR(50),
    latest_status VARCHAR(50),
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    duration_seconds INTEGER,
    total_count INTEGER,
    passed_count INTEGER,
    failed_count INTEGER,
    trigger_type VARCHAR(100),
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    etl_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS fct_agent_activity (
    activity_id INTEGER PRIMARY KEY,
    agent_id INTEGER,
    tenant_id BIGINT NOT NULL,
    user_id INTEGER,
    activity_type VARCHAR(100),
    description TEXT,
    status VARCHAR(50),
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    etl_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS fct_audit_events (
    event_id INTEGER PRIMARY KEY,
    event_uuid VARCHAR(255),
    event_type VARCHAR(100),
    entity_type VARCHAR(100),
    entity_id INTEGER,
    user_id INTEGER,
    tenant_id BIGINT NOT NULL,
    changes_json TEXT,
    ip_address VARCHAR(100),
    timestamp TIMESTAMP,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    actor_id INTEGER,
    action VARCHAR(100),
    event_time TIMESTAMP,
    status VARCHAR(50),
    etl_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- ETL Monitoring Table
CREATE TABLE IF NOT EXISTS etl_run_log (
    run_id INTEGER AUTOINCREMENT PRIMARY KEY,
    start_time TIMESTAMP NOT NULL,
    end_time TIMESTAMP,
    status VARCHAR(50) NOT NULL,
    records_processed INTEGER DEFAULT 0,
    records_failed INTEGER DEFAULT 0,
    error_message TEXT,
    duration_seconds INTEGER,
    etl_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- ===== CLUSTERING STRATEGY =====
-- Optimized clustering based on query patterns from all dashboards
-- Clustering reduces data scanning and improves query performance significantly

-- DIMENSION TABLES
-- Users: Heavily queried by account_id and login activity
ALTER TABLE dim_users CLUSTER BY (account_id, last_logged_in_at);

-- Test Cases: Filtered by tenant, creation date, and status
ALTER TABLE dim_test_cases CLUSTER BY (tenant_id, created_at, status);

-- Accounts: Primary key lookups and joins
ALTER TABLE dim_accounts CLUSTER BY (account_id);

-- Tenants: Filtered by subscription status and tenant_id
ALTER TABLE dim_tenants CLUSTER BY (subscription_status, tenant_id);

-- Applications: Filtered by tenant and app_id
ALTER TABLE dim_applications CLUSTER BY (tenant_id, app_id);

-- Test Suites: Tenant-based filtering
ALTER TABLE dim_test_suites CLUSTER BY (tenant_id, created_at);

-- FACT TABLES
-- Executions: Most queries filter by tenant and time range
ALTER TABLE fct_executions CLUSTER BY (tenant_id, start_time);

-- Test Results: Joined on execution_id and filtered by time
ALTER TABLE fct_test_results CLUSTER BY (execution_id, start_time);

-- Test Steps: Always joined on test_case_id
ALTER TABLE fct_test_steps CLUSTER BY (test_case_id);

-- Test Plan Results: Filtered by tenant and time
ALTER TABLE fct_test_plan_results CLUSTER BY (tenant_id, created_at);

-- Cross Tenant Metrics: Tenant-based aggregations
ALTER TABLE fct_cross_tenant_metrics CLUSTER BY (tenant_id, created_at);

-- Audit Events: Time-series queries by tenant
ALTER TABLE fct_audit_events CLUSTER BY (tenant_id, timestamp);

-- Agent Activity: Filtered by tenant and time
ALTER TABLE fct_agent_activity CLUSTER BY (tenant_id, start_time);

-- API Steps: Filtered by tenant
ALTER TABLE fct_api_steps CLUSTER BY (tenant_id, created_at);

-- Accessibility Results: Tenant and execution based queries
ALTER TABLE fct_accessibility_results CLUSTER BY (tenant_id, execution_id);