"""
Emergency Management Pipeline Sensors
Event-driven processing with federal compliance monitoring
"""

from dagster import (
    sensor,
    SensorEvaluationContext,
    RunRequest,
    SkipReason,
    SensorResult,
    get_dagster_logger,
    AssetMaterialization,
    MetadataValue,
)
from datetime import datetime, timedelta
from typing import List, Dict, Any
import json


@sensor(
    name="data_freshness_sensor",
    description="Monitor data freshness and trigger processing when stale",
    minimum_interval_seconds=300,  # Check every 5 minutes
)
def data_freshness_sensor(context: SensorEvaluationContext):
    """
    Monitor data freshness across all sources
    Trigger processing jobs when data becomes stale
    """
    logger = get_dagster_logger()
    
    # Define freshness thresholds (in hours)
    freshness_thresholds = {
        "fema_disaster_data": 24,      # Daily updates expected
        "noaa_weather_data": 1,        # Hourly updates expected  
        "coagmet_data": 1,             # Hourly updates expected
        "usda_data": 168,              # Weekly updates expected
    }
    
    stale_sources = []
    current_time = datetime.now()
    
    # Check each data source for freshness
    for source, threshold_hours in freshness_thresholds.items():
        # In production, this would query actual timestamps from StarRocks
        # For demo, simulate random staleness
        last_update = current_time - timedelta(hours=threshold_hours + 1)
        staleness_hours = (current_time - last_update).total_seconds() / 3600
        
        if staleness_hours > threshold_hours:
            stale_sources.append({
                "source": source,
                "staleness_hours": staleness_hours,
                "threshold_hours": threshold_hours,
                "last_update": last_update.isoformat()
            })
    
    if stale_sources:
        logger.warning(f"Found {len(stale_sources)} stale data sources")
        
        # Create run request to refresh stale data
        return RunRequest(
            run_key=f"data_freshness_{context.cursor}",
            run_config={
                "ops": {
                    "refresh_stale_data": {
                        "config": {
                            "stale_sources": stale_sources
                        }
                    }
                }
            },
            tags={
                "trigger": "data_freshness",
                "stale_count": str(len(stale_sources))
            }
        )
    else:
        return SkipReason("All data sources are fresh")


@sensor(
    name="error_monitoring_sensor", 
    description="Monitor for errors and data quality issues",
    minimum_interval_seconds=600,  # Check every 10 minutes
)
def error_monitoring_sensor(context: SensorEvaluationContext):
    """
    Monitor system for errors and quality issues
    Trigger remediation workflows when problems detected
    """
    logger = get_dagster_logger()
    
    # Simulate error detection (in production, would query logs/metrics)
    detected_issues = []
    
    # Check for common issues
    issues_to_check = [
        {
            "type": "data_quality",
            "severity": "medium",
            "description": "Data quality scores below threshold",
            "affected_tables": ["fema_disaster_data"]
        },
        {
            "type": "ingestion_failure",
            "severity": "high", 
            "description": "API ingestion failures detected",
            "affected_sources": ["noaa_weather_api"]
        }
    ]
    
    # Simulate issue detection based on cursor (for demo)
    if context.cursor and int(context.cursor) % 10 == 0:
        detected_issues = issues_to_check[:1]  # Simulate occasional issues
    
    if detected_issues:
        high_severity_issues = [i for i in detected_issues if i["severity"] == "high"]
        
        if high_severity_issues:
            logger.error(f"Detected {len(high_severity_issues)} high severity issues")
            
            return RunRequest(
                run_key=f"error_remediation_{context.cursor}",
                run_config={
                    "ops": {
                        "handle_critical_errors": {
                            "config": {
                                "issues": high_severity_issues
                            }
                        }
                    }
                },
                tags={
                    "trigger": "error_monitoring",
                    "severity": "high",
                    "issue_count": str(len(high_severity_issues))
                }
            )
        else:
            logger.info(f"Detected {len(detected_issues)} medium/low severity issues")
            return SkipReason("Only low/medium severity issues detected")
    else:
        return SkipReason("No issues detected")


@sensor(
    name="weather_alert_sensor",
    description="Monitor for severe weather alerts requiring immediate response",
    minimum_interval_seconds=60,  # Check every minute for critical alerts
)
def weather_alert_sensor(context: SensorEvaluationContext):
    """
    Monitor weather alerts for severe conditions
    Trigger emergency response workflows for critical alerts
    """
    logger = get_dagster_logger()
    
    # In production, this would query Kafka or StarRocks for recent alerts
    # Simulate severe weather alert detection
    
    critical_events = [
        "Tornado Warning",
        "Flash Flood Warning", 
        "Blizzard Warning",
        "Extreme Wind Warning"
    ]
    
    # Simulate alert detection (replace with actual data query)
    current_alerts = []
    if context.cursor and int(context.cursor) % 20 == 0:  # Simulate occasional alerts
        current_alerts = [
            {
                "alert_id": f"NWS_ALERT_{context.cursor}",
                "event": "Tornado Warning",
                "severity": "Extreme",
                "urgency": "Immediate",
                "certainty": "Observed",
                "area": "Adams County, CO",
                "effective": datetime.now().isoformat(),
                "expires": (datetime.now() + timedelta(hours=2)).isoformat()
            }
        ]
    
    critical_alerts = [
        alert for alert in current_alerts 
        if alert.get("event") in critical_events and alert.get("severity") == "Extreme"
    ]
    
    if critical_alerts:
        logger.warning(f"Detected {len(critical_alerts)} critical weather alerts")
        
        return [
            RunRequest(
                run_key=f"weather_emergency_{alert['alert_id']}",
                run_config={
                    "ops": {
                        "process_critical_alert": {
                            "config": {
                                "alert": alert
                            }
                        }
                    }
                },
                tags={
                    "trigger": "weather_alert",
                    "alert_type": alert["event"],
                    "severity": alert["severity"],
                    "area": alert["area"]
                }
            )
            for alert in critical_alerts
        ]
    else:
        return SkipReason("No critical weather alerts detected")


@sensor(
    name="flink_job_health_sensor",
    description="Monitor Flink streaming job health and restart if needed",
    minimum_interval_seconds=300,  # Check every 5 minutes
)
def flink_job_health_sensor(context: SensorEvaluationContext):
    """
    Monitor health of Flink streaming jobs
    Restart failed jobs automatically
    """
    logger = get_dagster_logger()
    
    # In production, this would query Flink REST API for job status
    # Simulate job health monitoring
    
    monitored_jobs = [
        {
            "job_name": "emergency_data_aggregation",
            "job_id": "abc123",
            "status": "RUNNING",
            "uptime_hours": 24.5
        },
        {
            "job_name": "real_time_alerting", 
            "job_id": "def456",
            "status": "FAILED",
            "uptime_hours": 0,
            "failure_reason": "Connection timeout"
        }
    ]
    
    failed_jobs = [job for job in monitored_jobs if job["status"] == "FAILED"]
    
    if failed_jobs:
        logger.error(f"Detected {len(failed_jobs)} failed Flink jobs")
        
        return [
            RunRequest(
                run_key=f"restart_flink_job_{job['job_name']}_{context.cursor}",
                run_config={
                    "ops": {
                        "restart_flink_job": {
                            "config": {
                                "job_name": job["job_name"],
                                "job_id": job["job_id"],
                                "failure_reason": job.get("failure_reason", "Unknown")
                            }
                        }
                    }
                },
                tags={
                    "trigger": "flink_health",
                    "job_name": job["job_name"],
                    "action": "restart"
                }
            )
            for job in failed_jobs
        ]
    else:
        return SkipReason("All Flink jobs are healthy")


@sensor(
    name="disk_space_sensor",
    description="Monitor disk space and trigger cleanup when needed",
    minimum_interval_seconds=3600,  # Check every hour
)
def disk_space_sensor(context: SensorEvaluationContext):
    """
    Monitor disk space usage across the system
    Trigger cleanup jobs when space is low
    """
    logger = get_dagster_logger()
    
    # Simulate disk space monitoring
    disk_usage = {
        "flink_checkpoints": 75,  # Percentage used
        "starrocks_storage": 60,
        "dagster_storage": 45,
        "kafka_logs": 80
    }
    
    # Threshold for triggering cleanup
    cleanup_threshold = 70
    
    volumes_needing_cleanup = [
        volume for volume, usage in disk_usage.items() 
        if usage > cleanup_threshold
    ]
    
    if volumes_needing_cleanup:
        logger.warning(f"Disk space high on volumes: {volumes_needing_cleanup}")
        
        return RunRequest(
            run_key=f"disk_cleanup_{context.cursor}",
            run_config={
                "ops": {
                    "cleanup_disk_space": {
                        "config": {
                            "volumes_to_cleanup": volumes_needing_cleanup,
                            "disk_usage": disk_usage
                        }
                    }
                }
            },
            tags={
                "trigger": "disk_space",
                "cleanup_volumes": ",".join(volumes_needing_cleanup)
            }
        )
    else:
        return SkipReason("Disk space usage is within acceptable limits")


@sensor(
    name="federal_compliance_sensor",
    description="Monitor federal compliance requirements (FedRAMP, DORA)",
    minimum_interval_seconds=3600,  # Check every hour
)
def federal_compliance_sensor(context: SensorEvaluationContext):
    """
    Monitor federal compliance requirements
    Trigger auditing and compliance checks
    """
    logger = get_dagster_logger()
    
    # Simulate compliance monitoring
    compliance_checks = {
        "data_encryption": {"status": "compliant", "last_check": datetime.now()},
        "access_logging": {"status": "compliant", "last_check": datetime.now()},
        "data_retention": {"status": "non_compliant", "last_check": datetime.now()},
        "backup_verification": {"status": "compliant", "last_check": datetime.now()},
    }
    
    non_compliant_items = [
        check for check, details in compliance_checks.items()
        if details["status"] != "compliant"
    ]
    
    if non_compliant_items:
        logger.error(f"Compliance issues detected: {non_compliant_items}")
        
        return RunRequest(
            run_key=f"compliance_remediation_{context.cursor}",
            run_config={
                "ops": {
                    "remediate_compliance_issues": {
                        "config": {
                            "non_compliant_items": non_compliant_items,
                            "compliance_status": compliance_checks
                        }
                    }
                }
            },
            tags={
                "trigger": "compliance",
                "non_compliant_count": str(len(non_compliant_items)),
                "priority": "high"
            }
        )
    else:
        return SkipReason("All compliance checks passed")