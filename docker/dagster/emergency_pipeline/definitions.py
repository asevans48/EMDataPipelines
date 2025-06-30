"""
Emergency Management Data Pipeline Definitions
Modular and scalable pipeline for federal emergency data processing
"""

from dagster import (
    Definitions,
    DefaultScheduleStatus,
    ScheduleDefinition,
    define_asset_job,
    AssetSelection,
    SensorDefinition,
    DefaultSensorStatus,
    EnvVar,
)
from dagster_dbt import DbtCliResource, dbt_assets

from .assets import (
    raw_data_assets,
    processed_data_assets,
    ml_assets,
)
from .resources import (
    starrocks_resource,
    flink_resource,
    kafka_resource,
    data_quality_resource,
)
from .sensors import data_freshness_sensor, error_monitoring_sensor
from .jobs import (
    data_ingestion_job,
    data_processing_job,
    data_quality_job,
    ml_pipeline_job,
)

# DBT Integration
dbt_project_dir = "/opt/dagster/app/dbt_project"
dbt_resource = DbtCliResource(project_dir=dbt_project_dir)

@dbt_assets(
    manifest=f"{dbt_project_dir}/target/manifest.json",
    select="tag:emergency_data"
)
def dbt_emergency_assets(context, dbt: DbtCliResource):
    """DBT assets for emergency data transformation"""
    yield from dbt.cli(["build"], context=context).stream()

# Define schedules for regular data ingestion
data_ingestion_schedule = ScheduleDefinition(
    job=data_ingestion_job,
    cron_schedule="*/15 * * * *",  # Every 15 minutes
    default_status=DefaultScheduleStatus.RUNNING,
)

data_processing_schedule = ScheduleDefinition(
    job=data_processing_job,
    cron_schedule="0 */2 * * *",  # Every 2 hours
    default_status=DefaultScheduleStatus.RUNNING,
)

data_quality_schedule = ScheduleDefinition(
    job=data_quality_job,
    cron_schedule="0 6 * * *",  # Daily at 6 AM
    default_status=DefaultScheduleStatus.RUNNING,
)

ml_pipeline_schedule = ScheduleDefinition(
    job=ml_pipeline_job,
    cron_schedule="0 2 * * *",  # Daily at 2 AM
    default_status=DefaultScheduleStatus.RUNNING,
)

# Define sensors for event-driven processing
freshness_sensor = SensorDefinition(
    name="data_freshness_sensor",
    job=data_processing_job,
    default_status=DefaultSensorStatus.RUNNING,
)

error_sensor = SensorDefinition(
    name="error_monitoring_sensor",
    job=data_quality_job,
    default_status=DefaultSensorStatus.RUNNING,
)

# Resource definitions with federal compliance configurations
resources = {
    "starrocks": starrocks_resource.configured({
        "host": EnvVar("STARROCKS_HOST").get_value("starrocks-fe"),
        "port": 9030,
        "user": "root",
        "password": "",
        "database": "emergency_data",
        "ssl_enabled": True,  # Required for federal compliance
        "connection_timeout": 30,
    }),
    "flink": flink_resource.configured({
        "jobmanager_host": EnvVar("FLINK_JOBMANAGER_HOST").get_value("flink-jobmanager"),
        "jobmanager_port": 8081,
        "parallelism": 4,
        "checkpoint_interval": 60000,  # 1 minute
        "state_backend": "filesystem",
    }),
    "kafka": kafka_resource.configured({
        "bootstrap_servers": EnvVar("KAFKA_BOOTSTRAP_SERVERS").get_value("kafka:29092"),
        "security_protocol": "PLAINTEXT",  # Use SSL in production
        "compression_type": "gzip",
    }),
    "data_quality": data_quality_resource.configured({
        "expectation_store_backend": "filesystem",
        "validation_store_backend": "filesystem",
        "data_docs_backend": "filesystem",
    }),
    "dbt": dbt_resource,
}

# All assets grouped by type
all_assets = [
    *raw_data_assets,
    *processed_data_assets,
    *ml_assets,
    dbt_emergency_assets,
]

# All schedules
all_schedules = [
    data_ingestion_schedule,
    data_processing_schedule,
    data_quality_schedule,
    ml_pipeline_schedule,
]

# All sensors
all_sensors = [
    data_freshness_sensor,
    error_monitoring_sensor,
]

# All jobs
all_jobs = [
    data_ingestion_job,
    data_processing_job,
    data_quality_job,
    ml_pipeline_job,
]

# Main definitions object
defs = Definitions(
    assets=all_assets,
    schedules=all_schedules,
    sensors=all_sensors,
    jobs=all_jobs,
    resources=resources,
)