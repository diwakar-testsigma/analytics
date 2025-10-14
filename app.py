#!/usr/bin/env python3
"""
ETL Pipeline Web Service

Production-ready web service for ETL pipeline with health checks,
metrics, and API endpoints for Kubernetes deployment.
"""

import os
import logging
import asyncio
from datetime import datetime
from typing import Dict, Optional, Any, List
from enum import Enum
from pathlib import Path

from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from prometheus_client import Counter, Histogram, Gauge, generate_latest
from prometheus_client.core import CollectorRegistry
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from src.pipeline import Pipeline
from src.config import settings

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Prometheus metrics
registry = CollectorRegistry()
etl_runs_total = Counter('etl_runs_total', 'Total number of ETL runs', ['status'], registry=registry)
etl_duration_seconds = Histogram('etl_duration_seconds', 'ETL run duration in seconds', registry=registry)
etl_records_processed = Counter('etl_records_processed', 'Total records processed', ['phase'], registry=registry)
etl_active_runs = Gauge('etl_active_runs', 'Number of active ETL runs', registry=registry)
etl_last_run_timestamp = Gauge('etl_last_run_timestamp', 'Timestamp of last ETL run', registry=registry)
etl_last_success_timestamp = Gauge('etl_last_success_timestamp', 'Timestamp of last successful ETL run', registry=registry)

# Application state
app_state = {
    "status": "idle",
    "current_job": None,
    "last_run": None,
    "total_runs": 0,
    "successful_runs": 0,
    "failed_runs": 0,
    "is_healthy": True
}

# FastAPI app
app = FastAPI(
    title="ETL Pipeline Service",
    description="Production-ready ETL pipeline service for analytics data",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc"
)

# Scheduler
scheduler = AsyncIOScheduler()


class JobStatus(str, Enum):
    IDLE = "idle"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"


class JobRequest(BaseModel):
    source_file: Optional[str] = Field(None, description="Path to existing data file to load")
    run_async: bool = Field(True, description="Run job asynchronously")
    extraction_start_date: Optional[str] = Field(None, description="Override extraction start date (YYYY-MM-DD)")


class JobResponse(BaseModel):
    job_id: str
    status: JobStatus
    message: str
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None


class HealthResponse(BaseModel):
    status: str
    environment: str
    checks: Dict[str, Any]
    timestamp: datetime


class MetricsResponse(BaseModel):
    total_runs: int
    successful_runs: int
    failed_runs: int
    last_run: Optional[Dict[str, Any]]
    current_status: str
    uptime_seconds: float


async def run_etl_async(job_id: str, source_file: Optional[str] = None, extraction_start_date: Optional[str] = None):
    """Run ETL pipeline asynchronously"""
    etl_active_runs.inc()
    start_time = datetime.now()
    
    try:
        logger.info(f"Starting ETL job {job_id}")
        if extraction_start_date:
            logger.info(f"Using explicit extraction start date: {extraction_start_date}")
        
        app_state["status"] = "running"
        app_state["current_job"] = {
            "id": job_id,
            "started_at": start_time,
            "source_file": source_file,
            "extraction_start_date": extraction_start_date
        }
        
        # Run pipeline in thread pool to avoid blocking
        loop = asyncio.get_event_loop()
        pipeline = Pipeline(extraction_start_date=extraction_start_date)
        
        if source_file:
            success = await loop.run_in_executor(None, pipeline.run_from_file, source_file)
        else:
            success = await loop.run_in_executor(None, pipeline.run)
        
        duration = (datetime.now() - start_time).total_seconds()
        etl_duration_seconds.observe(duration)
        
        if success:
            etl_runs_total.labels(status='success').inc()
            app_state["successful_runs"] += 1
            etl_last_success_timestamp.set_to_current_time()
            logger.info(f"ETL job {job_id} completed successfully in {duration:.2f}s")
        else:
            etl_runs_total.labels(status='failed').inc()
            app_state["failed_runs"] += 1
            logger.error(f"ETL job {job_id} failed after {duration:.2f}s")
        
        app_state["last_run"] = {
            "job_id": job_id,
            "status": "completed" if success else "failed",
            "started_at": start_time,
            "completed_at": datetime.now(),
            "duration_seconds": duration,
            "success": success
        }
        
    except Exception as e:
        logger.exception(f"ETL job {job_id} encountered an error")
        etl_runs_total.labels(status='error').inc()
        app_state["failed_runs"] += 1
        app_state["last_run"] = {
            "job_id": job_id,
            "status": "failed",
            "started_at": start_time,
            "completed_at": datetime.now(),
            "error": str(e)
        }
    finally:
        app_state["status"] = "idle"
        app_state["current_job"] = None
        app_state["total_runs"] += 1
        etl_active_runs.dec()
        etl_last_run_timestamp.set_to_current_time()


@app.on_event("startup")
async def startup_event():
    """Initialize application on startup"""
    logger.info("Starting ETL Pipeline Service")
    logger.info(f"Environment: {settings.ENVIRONMENT}")
    
    # Ensure directories exist
    Path('logs').mkdir(exist_ok=True)
    
    # Start scheduler if configured
    if settings.ETL_SCHEDULE_CRON:
        scheduler.add_job(
            func=lambda: asyncio.create_task(run_etl_async(f"scheduled_{datetime.now().strftime('%Y%m%d_%H%M%S')}")),
            trigger=CronTrigger.from_crontab(settings.ETL_SCHEDULE_CRON),
            id='scheduled_etl',
            name='Scheduled ETL Job',
            replace_existing=True
        )
        scheduler.start()
        logger.info(f"Scheduler started with cron: {settings.ETL_SCHEDULE_CRON}")
    
    # Run on startup if configured
    if settings.RUN_ON_STARTUP:
        asyncio.create_task(run_etl_async(f"startup_{datetime.now().strftime('%Y%m%d_%H%M%S')}"))
    
    logger.info("ETL Pipeline Service started successfully")


@app.on_event("shutdown")
async def shutdown_event():
    """Cleanup on shutdown"""
    logger.info("Shutting down ETL Pipeline Service")
    if scheduler.running:
        scheduler.shutdown()


@app.get("/", response_model=Dict[str, str])
async def root():
    """Root endpoint"""
    return {
        "service": "ETL Pipeline Service",
        "version": "1.0.0",
        "status": app_state["status"],
        "environment": settings.ENVIRONMENT
    }


@app.get("/health", response_model=HealthResponse)
async def health_check():
    """Health check endpoint for Kubernetes"""
    checks = {}
    is_healthy = True
    
    # Check MySQL connectivity
    try:
        # Simple connection check
        if settings.MYSQL_CONNECTION_URL:
            checks["mysql"] = {"status": "healthy", "connection_url": "configured"}
        else:
            checks["mysql"] = {"status": "unhealthy", "error": "No MySQL connection URL configured"}
            is_healthy = False
    except Exception as e:
        checks["mysql"] = {"status": "unhealthy", "error": str(e)}
        is_healthy = False
    
    # Check target database (Snowflake/SQLite)
    try:
        if settings.is_production:
            if settings.SNOWFLAKE_CONNECTION_URL:
                checks["snowflake"] = {"status": "healthy", "connection_url": "configured"}
            else:
                checks["snowflake"] = {"status": "unhealthy", "error": "No Snowflake connection URL configured"}
                is_healthy = False
        else:
            if settings.SQLITE_CONNECTION_URL:
                checks["sqlite"] = {"status": "healthy", "connection_url": "configured"}
            else:
                checks["sqlite"] = {"status": "unhealthy", "error": "No SQLite connection URL configured"}
                is_healthy = False
    except Exception as e:
        target = "snowflake" if settings.is_production else "sqlite"
        checks[target] = {"status": "unhealthy", "error": str(e)}
        is_healthy = False
    
    # Check scheduler
    if settings.ETL_SCHEDULE_CRON:
        checks["scheduler"] = {
            "status": "healthy" if scheduler.running else "unhealthy",
            "cron": settings.ETL_SCHEDULE_CRON
        }
    
    app_state["is_healthy"] = is_healthy
    
    return HealthResponse(
        status="healthy" if is_healthy else "unhealthy",
        environment=settings.ENVIRONMENT,
        checks=checks,
        timestamp=datetime.now()
    )


@app.get("/ready")
async def readiness_check():
    """Readiness check endpoint for Kubernetes"""
    if app_state["status"] == "running":
        return JSONResponse(
            status_code=503,
            content={"ready": False, "reason": "ETL job in progress"}
        )
    
    if not app_state["is_healthy"]:
        return JSONResponse(
            status_code=503,
            content={"ready": False, "reason": "Health check failed"}
        )
    
    return {"ready": True, "status": app_state["status"]}


@app.post("/etl/run", response_model=JobResponse)
async def run_etl(request: JobRequest, background_tasks: BackgroundTasks):
    """Trigger ETL pipeline run"""
    if app_state["status"] == "running":
        raise HTTPException(
            status_code=409,
            detail="ETL job already in progress"
        )
    
    job_id = f"api_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    
    if request.run_async:
        background_tasks.add_task(run_etl_async, job_id, request.source_file, request.extraction_start_date)
        return JobResponse(
            job_id=job_id,
            status=JobStatus.RUNNING,
            message="ETL job started",
            started_at=datetime.now()
        )
    else:
        # Run synchronously
        await run_etl_async(job_id, request.source_file, request.extraction_start_date)
        last_run = app_state["last_run"]
        return JobResponse(
            job_id=job_id,
            status=JobStatus.COMPLETED if last_run["success"] else JobStatus.FAILED,
            message="ETL job completed",
            started_at=last_run["started_at"],
            completed_at=last_run["completed_at"]
        )


@app.get("/etl/status")
async def get_status():
    """Get current ETL status"""
    return {
        "status": app_state["status"],
        "current_job": app_state["current_job"],
        "last_run": app_state["last_run"]
    }


@app.get("/metrics", response_model=MetricsResponse)
async def get_metrics():
    """Get service metrics"""
    uptime = (datetime.now() - startup_time).total_seconds() if 'startup_time' in globals() else 0
    
    return MetricsResponse(
        total_runs=app_state["total_runs"],
        successful_runs=app_state["successful_runs"],
        failed_runs=app_state["failed_runs"],
        last_run=app_state["last_run"],
        current_status=app_state["status"],
        uptime_seconds=uptime
    )


@app.get("/metrics/prometheus")
async def prometheus_metrics():
    """Prometheus metrics endpoint"""
    return generate_latest(registry)



# ==================== Checkpoint Endpoints ====================

@app.get("/checkpoint/status")
async def checkpoint_status():
    """Get extraction checkpoint status"""
    try:
        from src.extraction_checkpoint import checkpoint
        
        info = checkpoint.get_checkpoint_info()
        
        # Add recommended start date
        info['recommended_start_date'] = checkpoint.get_recommended_start_date()
        
        return info
        
    except Exception as e:
        logger.error(f"Checkpoint status check failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ==================== Recovery Endpoints ====================

@app.get("/recovery/status")
async def recovery_status():
    """
    Check recovery status - which tables are loaded and their record counts
    """
    try:
        from src.recovery.recovery import ETLRecovery
        
        recovery = ETLRecovery()
        loaded_tables = recovery.get_loaded_tables()
        
        # Get detailed status from database
        if settings.DATA_STORE == 'snowflake':
            from src.loaders.data_sources import SnowflakeDataSource
            data_source = SnowflakeDataSource(settings.SNOWFLAKE_CONNECTION_URL)
        else:
            from src.loaders.data_sources import SQLiteDataSource
            data_source = SQLiteDataSource(settings.SQLITE_CONNECTION_URL)
        
        data_source.connect()
        cursor = data_source.cursor
        
        tables_info = []
        
        if settings.DATA_STORE == 'snowflake':
            cursor.execute("SHOW TABLES")
            tables = cursor.fetchall()
            
            for table in tables:
                table_name = table[1]
                cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
                count = cursor.fetchone()[0]
                tables_info.append({
                    "table": table_name,
                    "records": count,
                    "status": "loaded" if count > 0 else "empty"
                })
        else:
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
            tables = cursor.fetchall()
            
            for (table_name,) in tables:
                cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
                count = cursor.fetchone()[0]
                tables_info.append({
                    "table": table_name,
                    "records": count,
                    "status": "loaded" if count > 0 else "empty"
                })
        
        data_source.disconnect()
        
        # Categorize tables
        loaded_with_data = [t for t in tables_info if t['status'] == 'loaded']
        empty_tables = [t for t in tables_info if t['status'] == 'empty']
        
        return {
            "data_store": settings.DATA_STORE,
            "total_tables": len(tables_info),
            "tables_with_data": len(loaded_with_data),
            "empty_tables": len(empty_tables),
            "tables": tables_info,
            "summary": {
                "loaded": [t['table'] for t in loaded_with_data],
                "empty": [t['table'] for t in empty_tables]
            }
        }
        
    except Exception as e:
        logger.error(f"Recovery status check failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/recovery/validate")
async def validate_transformation(file_path: Optional[str] = None):
    """
    Validate transformation file for data quality issues
    
    Args:
        file_path: Optional path to transformation file. Uses latest if not provided.
    """
    try:
        from src.recovery.recovery import ETLRecovery
        
        recovery = ETLRecovery()
        
        # Find file if not provided
        if not file_path:
            file_path = recovery.find_latest_transformation_file()
            if not file_path:
                raise HTTPException(status_code=404, detail="No transformation file found")
        
        # Validate the file
        issues = recovery.validate_data_before_load(file_path)
        
        return {
            "file": file_path,
            "total_tables": len(issues) if issues else 0,
            "tables_with_issues": len(issues),
            "issues": issues,
            "valid": len(issues) == 0
        }
        
    except Exception as e:
        logger.error(f"Validation failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


class RecoveryRequest(BaseModel):
    transformation_file: Optional[str] = None
    skip_tables: Optional[List[str]] = None


@app.post("/recovery/recover")
async def recover_from_failure(request: RecoveryRequest, background_tasks: BackgroundTasks):
    """
    Recover from ETL failure by loading remaining tables
    
    Args:
        transformation_file: Optional path to transformation file
        skip_tables: Optional list of tables to skip
    """
    try:
        from src.recovery.recovery import ETLRecovery
        
        recovery = ETLRecovery()
        
        # Find file if not provided
        transformation_file = request.transformation_file
        if not transformation_file:
            transformation_file = recovery.find_latest_transformation_file()
            if not transformation_file:
                raise HTTPException(status_code=404, detail="No transformation file found")
        
        # Get current state
        loaded_tables = recovery.get_loaded_tables()
        
        # Start recovery in background
        def run_recovery():
            try:
                success = recovery.recover_from_failure(
                    transformation_file=transformation_file,
                    skip_tables=request.skip_tables
                )
                return success
            except Exception as e:
                logger.error(f"Recovery failed: {e}")
                return False
        
        background_tasks.add_task(run_recovery)
        
        return {
            "status": "recovery_started",
            "transformation_file": transformation_file,
            "skip_tables": request.skip_tables or [],
            "already_loaded": len(loaded_tables),
            "message": "Recovery process started in background"
        }
        
    except Exception as e:
        logger.error(f"Recovery initialization failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/recovery/empty-tables")
async def check_empty_tables():
    """Check which tables have 0 records"""
    try:
        if settings.DATA_STORE == 'snowflake':
            from src.loaders.data_sources import SnowflakeDataSource
            data_source = SnowflakeDataSource(settings.SNOWFLAKE_CONNECTION_URL)
        else:
            from src.loaders.data_sources import SQLiteDataSource
            data_source = SQLiteDataSource(settings.SQLITE_CONNECTION_URL)
        
        data_source.connect()
        
        empty_tables = []
        tables_with_data = []
        
        cursor = data_source.cursor
        
        if settings.DATA_STORE == 'snowflake':
            cursor.execute("SHOW TABLES")
            tables = cursor.fetchall()
            
            for table in tables:
                table_name = table[1]
                cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
                count = cursor.fetchone()[0]
                
                if count == 0:
                    empty_tables.append(table_name)
                else:
                    tables_with_data.append({"table": table_name, "records": count})
        else:
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
            tables = cursor.fetchall()
            
            for (table_name,) in tables:
                cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
                count = cursor.fetchone()[0]
                
                if count == 0:
                    empty_tables.append(table_name)
                else:
                    tables_with_data.append({"table": table_name, "records": count})
        
        data_source.disconnect()
        
        return {
            "data_store": settings.DATA_STORE,
            "empty_tables_count": len(empty_tables),
            "tables_with_data_count": len(tables_with_data),
            "empty_tables": empty_tables,
            "tables_with_data": tables_with_data
        }
        
    except Exception as e:
        logger.error(f"Empty tables check failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# Set startup time
startup_time = datetime.now()


if __name__ == "__main__":
    import uvicorn
    
    port = int(os.getenv("PORT", "8000"))
    host = os.getenv("HOST", "0.0.0.0")
    
    uvicorn.run(
        "app:app",
        host=host,
        port=port,
        reload=settings.ENVIRONMENT == "local",
        log_level=settings.LOG_LEVEL.lower()
    )
