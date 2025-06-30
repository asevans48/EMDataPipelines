"""
Data Ingestion Jobs for Emergency Management Pipeline
Modular jobs for ingesting data from multiple sources
"""

from dagster import (
    job,
    define_asset_job,
    AssetSelection,
    Config,
    op,
    OpExecutionContext,
    get_dagster_logger,
)
from typing import Dict, Any
from pydantic import Field


class IngestionConfig(Config):
    """Configuration for data ingestion jobs"""
    sources: list[str] = Field(default=["fema", "noaa"], description="Data sources to ingest")
    batch_size: int = Field(default=1000, description="Batch size for processing")
    max_retries: int = Field(default=3, description="Maximum retry attempts")
    timeout_seconds: int = Field(default=300, description="Timeout for each source")


# Primary data ingestion job using asset selection
data_ingestion_job = define_asset_job(
    name="data_ingestion_job",
    description="Ingest raw data from all emergency management sources",
    selection=AssetSelection.groups("raw_data"),
    config={
        "execution": {
            "config": {
                "multiprocess": {
                    "max_concurrent": 4,  # Process sources in parallel
                }
            }
        }
    }
)


@op(
    description="Validate data source availability",
    config_schema={
        "sources_to_check": [str],
        "timeout_seconds": int,
    }
)
def validate_data_sources(context: OpExecutionContext) -> Dict[str, bool]:
    """
    Validate that all required data sources are available
    Returns availability status for each source
    """
    logger = get_dagster_logger()
    
    sources = context.op_config["sources_to_check"]
    timeout = context.op_config.get("timeout_seconds", 30)
    
    # Simulate data source validation
    # In production, this would check actual API endpoints
    source_status = {}
    
    for source in sources:
        try:
            # Simulate health check
            if source == "fema":
                # Check FEMA OpenFEMA API
                status = True  # requests.get("https://www.fema.gov/api/open/v2", timeout=timeout).status_code == 200
            elif source == "noaa":
                # Check NOAA Weather API
                status = True  # requests.get("https://api.weather.gov/alerts/active", timeout=timeout).status_code == 200
            elif source == "coagmet":
                # Check CoAgMet API
                status = True  # Custom health check
            elif source == "usda":
                # Check USDA API
                status = True  # Custom health check
            else:
                status = False
                
            source_status[source] = status
            
            if status:
                logger.info(f"Data source '{source}' is available")
            else:
                logger.warning(f"Data source '{source}' is unavailable")
                
        except Exception as e:
            logger.error(f"Error checking data source '{source}': {str(e)}")
            source_status[source] = False
    
    # Log overall status
    available_sources = sum(source_status.values())
    total_sources = len(source_status)
    
    logger.info(f"Data source availability: {available_sources}/{total_sources} sources available")
    
    return source_status


@op(
    description="Setup streaming infrastructure",
)
def setup_streaming_infrastructure(
    context: OpExecutionContext,
    source_status: Dict[str, bool]
) -> Dict[str, Any]:
    """
    Set up Kafka topics and Flink jobs for real-time streaming
    """
    logger = get_dagster_logger()
    
    available_sources = [source for source, status in source_status.items() if status]
    
    if not available_sources:
        logger.error("No data sources available - skipping infrastructure setup")
        return {"status": "skipped", "reason": "no_sources_available"}
    
    # Simulate infrastructure setup
    setup_results = {
        "kafka_topics_created": [],
        "flink_jobs_started": [],
        "setup_timestamp": context.run.run_id,
        "status": "success"
    }
    
    # Set up topics for available sources
    topic_mapping = {
        "fema": "fema_disasters",
        "noaa": "noaa_weather_alerts", 
        "coagmet": "coagmet_weather",
        "usda": "usda_agricultural_data"
    }
    
    for source in available_sources:
        if source in topic_mapping:
            topic_name = topic_mapping[source]
            setup_results["kafka_topics_created"].append(topic_name)
            setup_results["flink_jobs_started"].append(f"{source}_stream_job")
            logger.info(f"Set up streaming for {source} -> {topic_name}")
    
    logger.info(f"Streaming infrastructure setup completed for {len(available_sources)} sources")
    
    return setup_results


@op(
    description="Monitor ingestion progress",
)
def monitor_ingestion_progress(
    context: OpExecutionContext,
    infrastructure_status: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Monitor the progress of data ingestion across all sources
    """
    logger = get_dagster_logger()
    
    if infrastructure_status.get("status") != "success":
        logger.warning("Infrastructure setup failed - limited monitoring available")
        return {"status": "limited", "monitoring_active": False}
    
    # Simulate ingestion monitoring
    monitoring_results = {
        "monitoring_start_time": context.run.run_id,
        "sources_monitored": len(infrastructure_status.get("kafka_topics_created", [])),
        "total_records_ingested": 0,
        "ingestion_rates": {},
        "error_counts": {},
        "status": "active"
    }
    
    # Simulate metrics for each topic
    for topic in infrastructure_status.get("kafka_topics_created", []):
        # Simulate ingestion metrics
        records_count = 100 + hash(topic + context.run.run_id) % 1000
        error_count = max(0, hash(topic) % 10 - 7)  # Occasional errors
        
        monitoring_results["total_records_ingested"] += records_count
        monitoring_results["ingestion_rates"][topic] = f"{records_count} records/batch"
        monitoring_results["error_counts"][topic] = error_count
        
        if error_count > 0:
            logger.warning(f"Ingestion errors detected for {topic}: {error_count} errors")
        else:
            logger.info(f"Clean ingestion for {topic}: {records_count} records")
    
    logger.info(f"Monitoring results: {monitoring_results['total_records_ingested']} total records ingested")
    
    return monitoring_results


@job(
    description="Emergency data refresh with monitoring and validation",
    config={
        "ops": {
            "validate_data_sources": {
                "config": {
                    "sources_to_check": ["fema", "noaa", "coagmet", "usda"],
                    "timeout_seconds": 30
                }
            }
        }
    }
)
def emergency_data_refresh_job():
    """
    Comprehensive emergency data refresh job with validation and monitoring
    """
    source_status = validate_data_sources()
    infrastructure_status = setup_streaming_infrastructure(source_status)
    monitor_ingestion_progress(infrastructure_status)


@op(
    description="Create database tables for new sources",
    config_schema={
        "tables_to_create": [str],
        "force_recreate": bool,
    }
)
def create_database_tables(context: OpExecutionContext) -> Dict[str, bool]:
    """
    Create necessary database tables for emergency data
    """
    logger = get_dagster_logger()
    
    tables = context.op_config["tables_to_create"]
    force_recreate = context.op_config.get("force_recreate", False)
    
    # Table schemas for emergency data
    table_schemas = {
        "fema_disaster_declarations": {
            "disaster_number": "VARCHAR(50) PRIMARY KEY",
            "state": "VARCHAR(2)",
            "declaration_date": "DATE",
            "incident_type": "VARCHAR(100)",
            "declaration_type": "VARCHAR(50)",
            "title": "TEXT",
            "incident_begin_date": "DATE",
            "incident_end_date": "DATE",
            "designated_area": "TEXT",
            "fy_declared": "INT"
        },
        "noaa_weather_alerts": {
            "alert_id": "VARCHAR(100) PRIMARY KEY",
            "event": "VARCHAR(100)",
            "severity": "VARCHAR(50)",
            "urgency": "VARCHAR(50)",
            "certainty": "VARCHAR(50)",
            "headline": "TEXT",
            "description": "TEXT",
            "instruction": "TEXT",
            "effective": "DATETIME",
            "expires": "DATETIME",
            "area_desc": "TEXT"
        },
        "coagmet_weather_data": {
            "station_id": "VARCHAR(50)",
            "timestamp": "DATETIME",
            "temperature": "DECIMAL(5,2)",
            "humidity": "DECIMAL(5,2)",
            "wind_speed": "DECIMAL(5,2)",
            "precipitation": "DECIMAL(8,2)",
            "station_name": "VARCHAR(200)",
            "latitude": "DECIMAL(10,6)",
            "longitude": "DECIMAL(10,6)",
            "PRIMARY KEY": "(station_id, timestamp)"
        },
        "usda_agricultural_data": {
            "program_year": "INT",
            "state_code": "VARCHAR(10)",
            "county_code": "VARCHAR(10)",
            "commodity": "VARCHAR(100)",
            "practice": "VARCHAR(100)",
            "coverage_level": "DECIMAL(5,2)",
            "premium_amount": "DECIMAL(12,2)",
            "liability_amount": "DECIMAL(12,2)",
            "indemnity_amount": "DECIMAL(12,2)",
            "PRIMARY KEY": "(program_year, state_code, county_code, commodity)"
        }
    }
    
    creation_results = {}
    
    for table_name in tables:
        if table_name not in table_schemas:
            logger.error(f"Unknown table schema: {table_name}")
            creation_results[table_name] = False
            continue
        
        try:
            # Simulate table creation
            schema = table_schemas[table_name]
            logger.info(f"Creating table {table_name} with {len(schema)} columns")
            
            if force_recreate:
                logger.info(f"Force recreating table: {table_name}")
            
            # In production, this would use StarRocksResource.create_table_if_not_exists()
            creation_results[table_name] = True
            logger.info(f"Successfully created/verified table: {table_name}")
            
        except Exception as e:
            logger.error(f"Error creating table {table_name}: {str(e)}")
            creation_results[table_name] = False
    
    success_count = sum(creation_results.values())
    total_count = len(creation_results)
    
    logger.info(f"Table creation completed: {success_count}/{total_count} tables successful")
    
    return creation_results


@job(
    description="Initialize database schema for emergency data",
    config={
        "ops": {
            "create_database_tables": {
                "config": {
                    "tables_to_create": [
                        "fema_disaster_declarations",
                        "noaa_weather_alerts", 
                        "coagmet_weather_data",
                        "usda_agricultural_data"
                    ],
                    "force_recreate": False
                }
            }
        }
    }
)
def database_initialization_job():
    """
    Initialize database schema for emergency management data
    """
    create_database_tables()


@op(
    description="Backfill historical data",
    config_schema={
        "sources": [str],
        "start_date": str,
        "end_date": str,
        "batch_size": int,
    }
)
def backfill_historical_data(context: OpExecutionContext) -> Dict[str, Any]:
    """
    Backfill historical emergency data for analysis
    """
    logger = get_dagster_logger()
    
    sources = context.op_config["sources"]
    start_date = context.op_config["start_date"]
    end_date = context.op_config["end_date"]
    batch_size = context.op_config.get("batch_size", 1000)
    
    backfill_results = {
        "start_date": start_date,
        "end_date": end_date,
        "sources_processed": [],
        "total_records_backfilled": 0,
        "processing_time_seconds": 0,
        "status": "success"
    }
    
    for source in sources:
        try:
            # Simulate historical data backfill
            records_backfilled = batch_size * (hash(source + start_date) % 10 + 1)
            
            backfill_results["sources_processed"].append(source)
            backfill_results["total_records_backfilled"] += records_backfilled
            
            logger.info(f"Backfilled {records_backfilled} records for {source} from {start_date} to {end_date}")
            
        except Exception as e:
            logger.error(f"Error backfilling {source}: {str(e)}")
            backfill_results["status"] = "partial_failure"
    
    logger.info(f"Historical backfill completed: {backfill_results['total_records_backfilled']} total records")
    
    return backfill_results


@job(
    description="Backfill historical emergency data",
    config={
        "ops": {
            "backfill_historical_data": {
                "config": {
                    "sources": ["fema", "noaa"],
                    "start_date": "2020-01-01",
                    "end_date": "2024-01-01",
                    "batch_size": 5000
                }
            }
        }
    }
)
def historical_backfill_job():
    """
    Backfill historical emergency management data
    """
    backfill_historical_data()