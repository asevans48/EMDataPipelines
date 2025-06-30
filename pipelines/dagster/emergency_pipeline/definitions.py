"""
Emergency Management Data Pipeline Definitions
Supports both public data mode and future tenant separation
"""

import os
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

# Import assets based on mode
from .assets import (
    raw_data_assets,
    processed_data_assets,
    ml_assets,
)

# Check if we're in public data mode
IS_PUBLIC_MODE = os.getenv("DATA_CLASSIFICATION", "PUBLIC") == "PUBLIC"

if IS_PUBLIC_MODE:
    # Public data mode - simplified resources
    from .public_resources import (
        PublicDataStarRocksResource,
        PublicDataKafkaResource,
        OrganizationAwareFlinkResource,
        PublicDataUsageTracker,
    )
    from .assets.public_assets import public_api_assets
    from .jobs.public_jobs import (
        public_api_refresh_job,
        usage_analytics_job,
        public_data_quality_job,
    )
    from .sensors.public_sensors import (
        public_api_health_sensor,
        usage_spike_sensor,
    )
else:
    # Full tenant separation mode
    from .tenant_resources import (
        TenantAwareStarRocksResource,
        TenantAwareKafkaResource,
        TenantEncryptionManager,
        TenantAuditLogger,
    )
    from .sensors import (
        data_freshness_sensor,
        error_monitoring_sensor,
        federal_compliance_sensor,
    )

# Common imports
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

# Configure schedules based on mode
if IS_PUBLIC_MODE:
    # Public data schedules - more frequent for real-time public access
    data_ingestion_schedule = ScheduleDefinition(
        job=data_ingestion_job,
        cron_schedule="*/10 * * * *",  # Every 10 minutes for public freshness
        default_status=DefaultScheduleStatus.RUNNING,
    )
    
    public_api_refresh_schedule = ScheduleDefinition(
        job=public_api_refresh_job,
        cron_schedule="*/5 * * * *",   # Every 5 minutes for API views
        default_status=DefaultScheduleStatus.RUNNING,
    )
    
    usage_analytics_schedule = ScheduleDefinition(
        job=usage_analytics_job,
        cron_schedule="0 * * * *",     # Hourly usage analytics
        default_status=DefaultScheduleStatus.RUNNING,
    )
    
    # Define public mode schedules list
    mode_schedules = [
        public_api_refresh_schedule,
        usage_analytics_schedule,
    ]
    
    # Define public mode sensors
    mode_sensors = [
        public_api_health_sensor,
        usage_spike_sensor,
    ]
    
    # Define public mode jobs
    mode_jobs = [
        public_api_refresh_job,
        usage_analytics_job,
        public_data_quality_job,
    ]
    
    # Public mode assets
    mode_assets = public_api_assets
    
else:
    # Tenant separation schedules - focus on compliance and security
    data_ingestion_schedule = ScheduleDefinition(
        job=data_ingestion_job,
        cron_schedule="*/15 * * * *",  # Every 15 minutes
        default_status=DefaultScheduleStatus.RUNNING,
    )
    
    # Define tenant mode schedules list
    mode_schedules = []
    
    # Define tenant mode sensors
    mode_sensors = [
        data_freshness_sensor,
        error_monitoring_sensor,
        federal_compliance_sensor,
    ]
    
    # Define tenant mode jobs
    mode_jobs = []
    
    # Tenant mode assets (empty for now, would include tenant-specific assets)
    mode_assets = []

# Common schedules for both modes
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

# Configure resources based on mode
if IS_PUBLIC_MODE:
    resources = {
        "starrocks": PublicDataStarRocksResource(
            host=EnvVar("STARROCKS_HOST").get_value("starrocks-fe"),
            port=9030,
            user="public_user",
            password="",
            database="emergency_public_data",
        ),
        "kafka": PublicDataKafkaResource(
            bootstrap_servers=EnvVar("KAFKA_BOOTSTRAP_SERVERS").get_value("kafka:29092"),
            security_protocol="PLAINTEXT",
        ),
        "flink": OrganizationAwareFlinkResource(
            jobmanager_host=EnvVar("FLINK_JOBMANAGER_HOST").get_value("flink-jobmanager"),
            jobmanager_port=8081,
            parallelism=4,
        ),
        "usage_tracker": PublicDataUsageTracker(),
        "dbt": dbt_resource,
    }
else:
    resources = {
        "starrocks": TenantAwareStarRocksResource(
            host=EnvVar("STARROCKS_HOST").get_value("starrocks-fe"),
            port=9030,
            admin_user="root",
            admin_password="",
        ),
        "kafka": TenantAwareKafkaResource(
            bootstrap_servers=EnvVar("KAFKA_BOOTSTRAP_SERVERS").get_value("kafka:29092"),
            security_protocol="PLAINTEXT",
        ),
        "encryption_manager": TenantEncryptionManager(),
        "audit_logger": TenantAuditLogger(),
        "dbt": dbt_resource,
    }

# All assets grouped by type
all_assets = [
    *raw_data_assets,
    *processed_data_assets,
    *ml_assets,
    *mode_assets,  # Public or tenant-specific assets
    dbt_emergency_assets,
]

# All schedules
all_schedules = [
    data_ingestion_schedule,
    data_processing_schedule,
    data_quality_schedule,
    ml_pipeline_schedule,
    *mode_schedules,  # Mode-specific schedules
]

# All sensors
all_sensors = mode_sensors  # Mode-specific sensors

# All jobs
all_jobs = [
    data_ingestion_job,
    data_processing_job,
    data_quality_job,
    ml_pipeline_job,
    *mode_jobs,  # Mode-specific jobs
]

# Main definitions object
defs = Definitions(
    assets=all_assets,
    schedules=all_schedules,
    sensors=all_sensors,
    jobs=all_jobs,
    resources=resources,
)