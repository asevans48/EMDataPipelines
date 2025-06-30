"""
Data Sensors for Emergency Management Pipeline
Monitor data freshness, quality, and availability across all sources
"""

from dagster import (
    sensor,
    SensorEvaluationContext,
    RunRequest,
    SkipReason,
    get_dagster_logger,
    DefaultSensorStatus,
)
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional
import json


@sensor(
    name="data_freshness_sensor",
    description="Monitor data freshness across all emergency management sources",
    minimum_interval_seconds=600,  # Check every 10 minutes
    default_status=DefaultSensorStatus.RUNNING,
)
def data_freshness_sensor(context: SensorEvaluationContext):
    """
    Monitor data freshness for all emergency data sources
    Trigger refresh jobs when data becomes stale
    """
    logger = get_dagster_logger()
    
    # Define freshness thresholds for different sources
    freshness_thresholds = {
        "fema_disaster_declarations": 240,  # 4 hours
        "noaa_weather_alerts": 15,         # 15 minutes (critical for safety)
        "coagmet_weather_data": 60,        # 1 hour
        "usda_agricultural_data": 1440,    # 24 hours
        "processed_fema_disasters": 360,   # 6 hours
        "unified_emergency_events": 120,   # 2 hours
    }
    
    # Simulate data freshness checks (in production, query actual database)
    current_freshness = {}
    stale_sources = []
    critical_stale = []
    
    for source, threshold_minutes in freshness_thresholds.items():
        # Simulate freshness check
        age_minutes = max(5, hash(source + context.cursor) % (threshold_minutes * 2))
        current_freshness[source] = {
            "age_minutes": age_minutes,
            "threshold_minutes": threshold_minutes,
            "is_stale": age_minutes > threshold_minutes
        }
        
        if age_minutes > threshold_minutes:
            staleness_factor = age_minutes / threshold_minutes
            stale_info = {
                "source": source,
                "age_minutes": age_minutes,
                "threshold_minutes": threshold_minutes,
                "staleness_factor": staleness_factor
            }
            
            stale_sources.append(stale_info)
            
            # Critical if more than 2x the threshold
            if staleness_factor > 2.0:
                critical_stale.append(stale_info)
    
    if critical_stale:
        logger.error(f"Critical data staleness detected: {len(critical_stale)} sources")
        
        return RunRequest(
            run_key=f"critical_freshness_{context.cursor}",
            job_name="emergency_data_refresh_job",
            run_config={
                "ops": {
                    "validate_data_sources": {
                        "config": {
                            "sources_to_check": [s["source"] for s in critical_stale],
                            "timeout_seconds": 60
                        }
                    }
                }
            },
            tags={
                "trigger": "critical_freshness",
                "critical_sources": str(len(critical_stale)),
                "total_stale": str(len(stale_sources))
            }
        )
    
    elif stale_sources:
        logger.warning(f"Data staleness detected: {len(stale_sources)} sources")
        
        return RunRequest(
            run_key=f"freshness_refresh_{context.cursor}",
            job_name="data_ingestion_job",
            tags={
                "trigger": "data_freshness",
                "stale_sources": str(len(stale_sources))
            }
        )
    
    else:
        logger.info("All data sources are fresh")
        return SkipReason("All data sources are within freshness thresholds")


@sensor(
    name="data_volume_anomaly_sensor", 
    description="Detect unusual data volume patterns that may indicate issues",
    minimum_interval_seconds=900,  # Check every 15 minutes
    default_status=DefaultSensorStatus.RUNNING,
)
def data_volume_anomaly_sensor(context: SensorEvaluationContext):
    """
    Monitor data volume patterns to detect anomalies
    Unusual spikes or drops may indicate data source issues
    """
    logger = get_dagster_logger()
    
    # Simulate volume monitoring (in production, query actual metrics)
    current_volumes = {
        "fema_disasters": 45,      # Records in last hour
        "noaa_weather_alerts": 280,  # Very active weather
        "coagmet_weather": 150,    # Normal agricultural monitoring
        "usda_data": 12,          # Typical batch update
    }
    
    # Expected baselines (would be calculated from historical data)
    volume_baselines = {
        "fema_disasters": {"avg": 25, "min": 5, "max": 60, "spike_threshold": 100},
        "noaa_weather_alerts": {"avg": 120, "min": 20, "max": 300, "spike_threshold": 500},
        "coagmet_weather": {"avg": 144, "min": 100, "max": 200, "spike_threshold": 400},
        "usda_data": {"avg": 15, "min": 0, "max": 50, "spike_threshold": 100},
    }
    
    anomalies_detected = []
    critical_anomalies = []
    
    for source, current_volume in current_volumes.items():
        if source not in volume_baselines:
            continue
        
        baseline = volume_baselines[source]
        
        # Check for volume spikes
        if current_volume > baseline["spike_threshold"]:
            anomaly = {
                "source": source,
                "type": "volume_spike",
                "current_volume": current_volume,
                "threshold": baseline["spike_threshold"],
                "severity": "high" if current_volume > baseline["spike_threshold"] * 1.5 else "medium"
            }
            anomalies_detected.append(anomaly)
            
            if anomaly["severity"] == "high":
                critical_anomalies.append(anomaly)
        
        # Check for volume drops (potential data source issues)
        elif current_volume < baseline["min"]:
            anomaly = {
                "source": source,
                "type": "volume_drop",
                "current_volume": current_volume,
                "minimum_expected": baseline["min"],
                "severity": "high" if current_volume == 0 else "medium"
            }
            anomalies_detected.append(anomaly)
            
            if anomaly["severity"] == "high":
                critical_anomalies.append(anomaly)
    
    if critical_anomalies:
        logger.error(f"Critical volume anomalies detected: {len(critical_anomalies)} sources")
        
        return RunRequest(
            run_key=f"volume_anomaly_investigation_{context.cursor}",
            job_name="data_quality_job",
            run_config={
                "ops": {
                    "run_data_quality_checks": {
                        "config": {
                            "quality_rules": {
                                "completeness_threshold": 90.0,
                                "accuracy_threshold": 95.0,
                                "timeliness_threshold_hours": 2.0,
                                "consistency_threshold": 85.0
                            },
                            "tables_to_check": [a["source"] for a in critical_anomalies]
                        }
                    }
                }
            },
            tags={
                "trigger": "volume_anomaly",
                "severity": "critical",
                "anomalies": str(len(critical_anomalies))
            }
        )
    
    elif anomalies_detected:
        logger.warning(f"Volume anomalies detected: {len(anomalies_detected)} sources")
        return SkipReason("Volume anomalies detected but not critical - monitoring")
    
    else:
        return SkipReason("Data volumes are within normal ranges")


@sensor(
    name="source_availability_sensor",
    description="Monitor availability of external emergency data sources",
    minimum_interval_seconds=300,  # Check every 5 minutes
    default_status=DefaultSensorStatus.RUNNING,
)
def source_availability_sensor(context: SensorEvaluationContext):
    """
    Monitor external data source availability
    Trigger fallback procedures when sources are unavailable
    """
    logger = get_dagster_logger()
    
    # Simulate API health checks (in production, actual HTTP checks)
    source_status = {
        "fema_openfema_api": {
            "url": "https://www.fema.gov/api/open/v2/DisasterDeclarationsSummaries",
            "status": "available",
            "response_time_ms": 250,
            "last_check": datetime.now(),
            "consecutive_failures": 0
        },
        "noaa_weather_api": {
            "url": "https://api.weather.gov/alerts/active", 
            "status": "available",
            "response_time_ms": 180,
            "last_check": datetime.now(),
            "consecutive_failures": 0
        },
        "coagmet_api": {
            "url": "https://coagmet.colostate.edu/data_access/web_service",
            "status": "degraded",  # Simulated issue
            "response_time_ms": 3500,
            "last_check": datetime.now(),
            "consecutive_failures": 2
        },
        "usda_nass_api": {
            "url": "https://quickstats.nass.usda.gov/api/api_GET/",
            "status": "unavailable",  # Simulated outage
            "response_time_ms": None,
            "last_check": datetime.now(),
            "consecutive_failures": 5
        }
    }
    
    unavailable_sources = []
    degraded_sources = []
    
    for source_name, details in source_status.items():
        if details["status"] == "unavailable":
            unavailable_sources.append({
                "source": source_name,
                "url": details["url"],
                "consecutive_failures": details["consecutive_failures"],
                "severity": "critical" if details["consecutive_failures"] > 3 else "high"
            })
        elif details["status"] == "degraded":
            degraded_sources.append({
                "source": source_name,
                "url": details["url"],
                "response_time_ms": details["response_time_ms"],
                "consecutive_failures": details["consecutive_failures"],
                "severity": "medium"
            })
    
    if unavailable_sources:
        critical_sources = [s for s in unavailable_sources if s["severity"] == "critical"]
        
        if critical_sources:
            logger.error(f"Critical source outages: {len(critical_sources)} sources down")
            
            return RunRequest(
                run_key=f"source_outage_response_{context.cursor}",
                job_name="emergency_data_refresh_job",
                run_config={
                    "ops": {
                        "validate_data_sources": {
                            "config": {
                                "sources_to_check": ["fema", "noaa", "coagmet", "usda"],
                                "timeout_seconds": 60
                            }
                        }
                    }
                },
                tags={
                    "trigger": "source_outage",
                    "severity": "critical",
                    "unavailable_sources": str(len(unavailable_sources))
                }
            )
        
        else:
            logger.warning(f"Source availability issues: {len(unavailable_sources)} unavailable")
            return SkipReason("Source outages detected but not yet critical")
    
    elif degraded_sources:
        logger.warning(f"Degraded source performance: {len(degraded_sources)} sources slow")
        return SkipReason("Source performance degraded but still functional")
    
    else:
        return SkipReason("All data sources are available and performing well")


@sensor(
    name="data_schema_drift_sensor",
    description="Detect schema changes in incoming data that could break processing",
    minimum_interval_seconds=3600,  # Check every hour
    default_status=DefaultSensorStatus.RUNNING,
)
def data_schema_drift_sensor(context: SensorEvaluationContext):
    """
    Monitor for schema changes in incoming data
    Trigger schema validation and adaptation workflows
    """
    logger = get_dagster_logger()
    
    # Simulate schema drift detection
    schema_changes = {
        "fema_disaster_declarations": {
            "new_fields": ["climate_category", "estimated_damage_usd"],
            "removed_fields": [],
            "type_changes": {},
            "severity": "minor"
        },
        "noaa_weather_alerts": {
            "new_fields": [],
            "removed_fields": ["legacy_id"],
            "type_changes": {"effective": "datetime -> timestamp"},
            "severity": "moderate"
        }
    }
    
    significant_changes = []
    breaking_changes = []
    
    for source, changes in schema_changes.items():
        if changes["severity"] in ["moderate", "major"]:
            significant_changes.append({
                "source": source,
                "changes": changes,
                "impact_assessment": "requires_pipeline_update"
            })
        
        if changes["severity"] == "major" or changes["removed_fields"]:
            breaking_changes.append({
                "source": source,
                "changes": changes,
                "impact_assessment": "breaking_change"
            })
    
    if breaking_changes:
        logger.error(f"Breaking schema changes detected: {len(breaking_changes)} sources")
        
        return RunRequest(
            run_key=f"schema_breaking_change_{context.cursor}",
            job_name="data_quality_job",
            run_config={
                "ops": {
                    "run_data_quality_checks": {
                        "config": {
                            "quality_rules": {
                                "completeness_threshold": 85.0,  # Lower threshold during schema changes
                                "accuracy_threshold": 90.0,
                                "timeliness_threshold_hours": 4.0,
                                "consistency_threshold": 80.0
                            },
                            "tables_to_check": [c["source"] for c in breaking_changes]
                        }
                    }
                }
            },
            tags={
                "trigger": "schema_drift",
                "severity": "breaking",
                "affected_sources": str(len(breaking_changes))
            }
        )
    
    elif significant_changes:
        logger.warning(f"Schema changes detected: {len(significant_changes)} sources")
        return SkipReason("Schema changes detected - monitoring for impact")
    
    else:
        return SkipReason("No significant schema changes detected")


@sensor(
    name="emergency_event_correlation_sensor",
    description="Detect correlated emergency events across different data sources",
    minimum_interval_seconds=1800,  # Check every 30 minutes
    default_status=DefaultSensorStatus.RUNNING,
)
def emergency_event_correlation_sensor(context: SensorEvaluationContext):
    """
    Monitor for correlated emergency events that may indicate major incidents
    Trigger enhanced monitoring and processing when correlations are detected
    """
    logger = get_dagster_logger()
    
    # Simulate correlation detection
    recent_events = {
        "severe_weather_alerts": 15,  # High number of weather alerts
        "new_fema_declarations": 3,   # Multiple disaster declarations
        "agricultural_stress_indicators": 8,  # Agricultural monitoring showing stress
        "social_media_emergency_mentions": 150  # Would integrate social media in production
    }
    
    # Correlation thresholds
    correlation_patterns = [
        {
            "name": "severe_weather_outbreak",
            "conditions": {
                "severe_weather_alerts": 10,
                "new_fema_declarations": 2
            },
            "current_match": recent_events["severe_weather_alerts"] >= 10 and recent_events["new_fema_declarations"] >= 2
        },
        {
            "name": "agricultural_emergency",
            "conditions": {
                "agricultural_stress_indicators": 5,
                "severe_weather_alerts": 5
            },
            "current_match": recent_events["agricultural_stress_indicators"] >= 5 and recent_events["severe_weather_alerts"] >= 5
        },
        {
            "name": "multi_source_emergency",
            "conditions": {
                "severe_weather_alerts": 8,
                "new_fema_declarations": 1,
                "agricultural_stress_indicators": 3
            },
            "current_match": (recent_events["severe_weather_alerts"] >= 8 and 
                            recent_events["new_fema_declarations"] >= 1 and
                            recent_events["agricultural_stress_indicators"] >= 3)
        }
    ]
    
    active_correlations = []
    high_priority_correlations = []
    
    for pattern in correlation_patterns:
        if pattern["current_match"]:
            correlation_info = {
                "pattern": pattern["name"],
                "conditions_met": pattern["conditions"],
                "current_values": recent_events,
                "confidence": 0.85  # Simulated confidence score
            }
            
            active_correlations.append(correlation_info)
            
            if pattern["name"] in ["severe_weather_outbreak", "multi_source_emergency"]:
                high_priority_correlations.append(correlation_info)
    
    if high_priority_correlations:
        logger.error(f"High-priority emergency correlations detected: {len(high_priority_correlations)} patterns")
        
        return RunRequest(
            run_key=f"emergency_correlation_{context.cursor}",
            job_name="unified_analytics_job",
            run_config={
                "ops": {
                    "emergency_response_recommendations": {
                        "config": {
                            "correlation_patterns": [c["pattern"] for c in high_priority_correlations],
                            "enhanced_monitoring": True,
                            "alert_priority": "HIGH"
                        }
                    }
                }
            },
            tags={
                "trigger": "emergency_correlation",
                "priority": "high",
                "patterns": str(len(high_priority_correlations))
            }
        )
    
    elif active_correlations:
        logger.warning(f"Emergency correlations detected: {len(active_correlations)} patterns")
        return SkipReason("Emergency correlations detected but not high priority")
    
    else:
        return SkipReason("No significant emergency correlations detected")