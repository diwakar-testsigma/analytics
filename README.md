# Analytics ETL Pipeline

A production-ready ETL service that extracts data from MySQL, transforms it according to a Snowflake schema, and loads it into Snowflake/SQLite. Features include incremental loading, failure recovery, and Slack notifications.

## Features

- **Incremental Loading**: Automatically tracks extraction dates and updates start date after successful runs
- **Slack Notifications**: Real-time alerts for ETL status (start, success, partial success, failure)
- **Failure Recovery**: API endpoints to check status, validate data, and recover from partial failures
- **Configurable Loading**: Choose between `continue_on_error` or `fail_fast` strategies
- **Multi-Database Support**: Extract from multiple MySQL databases with keyword filtering
- **Web API**: RESTful endpoints for triggering ETL, checking status, and recovery operations

## Quick Start

### 1. Setup Environment

Create a `.env` file with your configuration:

```bash
# Environment
ENVIRONMENT=local  # or 'production' for Snowflake

# MySQL Source
MYSQL_CONNECTION_URL=mysql://user:password@host:3306/database

# For local development (SQLite)
SQLITE_CONNECTION_URL=sqlite:///data/analytics.db

# For production (Snowflake)
SNOWFLAKE_CONNECTION_URL=snowflake://user:password@account/database/schema?warehouse=WH&role=ROLE

# Target database selection
DATA_STORE=snowflake  # or 'sqlite' for local

# Incremental Loading
AUTO_UPDATE_START_DATE=true
EXTRACT_START_DATE=2024-01-01  # Leave empty to use checkpoint

# Slack Notifications
ENABLE_NOTIFICATIONS=true
SLACK_WEBHOOK_URL=https://hooks.slack.com/services/YOUR/WEBHOOK/URL

# Database Filtering
EXTRACT_DB_KEYWORDS=prod,main  # Only extract DBs containing these keywords
```

### 2. Install Dependencies

```bash
pip install -r requirements.txt
```

### 3. Run the ETL Pipeline

#### Option A: Using Docker (Recommended)

```bash
# Build and run
docker-compose up -d

# Trigger ETL
curl -X POST http://localhost:8000/etl/run -d '{"run_async": true}'

# Check status
curl http://localhost:8000/etl/status
```

#### Option B: Run Locally

```bash
# Start the web service
python app.py

# In another terminal, trigger ETL
curl -X POST http://localhost:8000/etl/run -d '{"run_async": true}'
```

## API Endpoints

### ETL Operations
- `GET /` - Service info
- `GET /health` - Health check
- `POST /etl/run` - Trigger ETL pipeline
- `GET /etl/status` - Check ETL status

### Checkpoint & Incremental Loading
- `GET /checkpoint/status` - Check extraction checkpoint and recommended start date

### Recovery Operations
- `GET /recovery/status` - Check loaded tables and record counts
- `GET /recovery/empty-tables` - List tables with 0 records
- `POST /recovery/validate` - Validate transformation file for issues
- `POST /recovery/recover` - Recover from partial failures

### Monitoring
- `GET /metrics` - Application metrics
- `GET /docs` - Interactive API documentation

## Project Structure

```
analytics/
├── app.py              # Web service entry point
├── src/
│   ├── config.py       # Configuration management
│   ├── pipeline.py     # Main ETL pipeline
│   ├── extractors/     # MySQL data extraction
│   ├── transformers/   # Data transformation logic
│   └── loaders/        # SQLite/Snowflake loading
├── config/
│   └── env_template.txt  # Environment variable template
├── output/
│   ├── extracted/      # Raw extracted data
│   └── transformations/# Transformed data ready for loading
└── logs/               # Application logs
```

## Configuration

The pipeline automatically:
- Uses SQLite when `ENVIRONMENT=local`
- Uses Snowflake when `ENVIRONMENT=production`
- Extracts from all MySQL databases (or only those matching keywords in `EXTRACT_DB_KEYWORDS` if provided)
- Transforms data according to the schema mappings
- Loads into the target database

### Loading Strategy

Control how the pipeline handles failures during the loading phase:

```bash
# Continue loading other tables if one fails (default)
LOAD_STRATEGY=continue_on_error

# Stop immediately on first failure
LOAD_STRATEGY=fail_fast
```

See `docs/LOAD_STRATEGIES.md` for detailed comparison.

## Testing Individual Components

```bash
# Test extraction only
python tests/scripts/test_extraction.py

# Test transformation with a specific file
python tests/scripts/test_transformation.py output/extracted/extracted_data_*.json

# Test Snowflake loading (requires ENVIRONMENT=production)
python tests/scripts/test_snowflake_load.py output/transformations/snowflake_data_*.json
```

## Monitoring

- Logs are saved in `logs/` directory
- Metrics are saved as JSON files in `logs/`
- Check Docker logs: `docker-compose logs -f`

## Troubleshooting

1. **MySQL Connection Issues**
   - For Docker: Use `host.docker.internal` instead of `localhost`
   - Check credentials and network connectivity

2. **Snowflake Connection Issues**
   - Verify account format includes region (e.g., `account.us-east-1`)
   - Ensure warehouse is running
   - Check role permissions

3. **Data Issues**
   - Check extraction output in `output/extracted/`
   - Verify transformation output in `output/transformations/`
   - Review logs for detailed error messages

## New Features

### Incremental Loading
The pipeline automatically tracks the last successful extraction date and uses it for the next run:
- Set `AUTO_UPDATE_START_DATE=true` to enable
- Leave `EXTRACT_START_DATE` empty to use the checkpoint
- Check checkpoint status: `GET /checkpoint/status`

### Slack Notifications
Get real-time alerts for ETL status:
1. Create a Slack webhook URL in your Slack workspace
2. Set `ENABLE_NOTIFICATIONS=true` and provide `SLACK_WEBHOOK_URL`
3. Configure when to receive notifications:
   - `NOTIFICATION_ON_SUCCESS=false` (usually not needed)
   - `NOTIFICATION_ON_FAILURE=true` (always notify on failure)
   - `NOTIFICATION_ON_PARTIAL=true` (notify when some tables fail)

### Failure Recovery
When ETL fails or partially succeeds:
1. Check status: `GET /recovery/status`
2. Validate data: `POST /recovery/validate`
3. Recover with skip list: `POST /recovery/recover {"skip_tables": ["problematic_table"]}`

## Environment Variables

Key configuration options:

- `DATA_STORE`: Choose between `sqlite` or `snowflake`
- `LOAD_STRATEGY`: `continue_on_error` or `fail_fast`
- `AUTO_UPDATE_START_DATE`: Enable incremental loading
- `ENABLE_NOTIFICATIONS`: Enable Slack notifications
- `EXTRACT_DB_KEYWORDS`: Filter databases by keywords (comma-separated)
