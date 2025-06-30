"""
Public Data Sensors - Monitor public API health and usage patterns
Optimized for public data availability and performance
"""

from dagster import (
    sensor,
    SensorEvaluationContext,
    RunRequest,
    SkipReason,
    get_dagster_logger,
)
from datetime import datetime, timedelta
from typing import Dict, Any
import json


@sensor(
    name="public_api_health_sensor",
    description="Monitor public API health and performance",
    minimum_interval_seconds=300,  # Check every 5 minutes
)
def public_api_health_sensor(context: SensorEvaluationContext):
    """
    Monitor public API endpoints for availability and performance
    Trigger remediation if issues detected
    """
    logger = get_dagster_logger()
    
    # Simulate API health checks
    api_endpoints = {
        "/api/v1/disasters": {"response_time_ms": 120, "status_code": 200, "uptime": 99.9},
        "/api/v1/weather/alerts": {"response_time_ms": 95, "status_code": 200, "uptime": 99.8},
        "/api/v1/weather/agricultural": {"response_time_ms": 180, "status_code": 200, "uptime": 99.5},
        "/api/v1/agricultural/disasters": {"response_time_ms": 250, "status_code": 500, "uptime": 95.2},  # Simulated issue
    }
    
    # Define thresholds
    response_time_threshold = 500  # ms
    uptime_threshold = 98.0  # percent
    
    unhealthy_endpoints = []
    performance_issues = []
    
    for endpoint, metrics in api_endpoints.items():
        if metrics["status_code"] >= 500:
            unhealthy_endpoints.append({
                "endpoint": endpoint,
                "issue": "server_error",
                "status_code": metrics["status_code"]
            })
        
        if metrics["response_time_ms"] > response_time_threshold:
            performance_issues.append({
                "endpoint": endpoint,
                "issue": "slow_response",
                "response_time_ms": metrics["response_time_ms"],
                "threshold_ms": response_time_threshold
            })
        
        if metrics["uptime"] < uptime_threshold:
            unhealthy_endpoints.append({
                "endpoint": endpoint,
                "issue": "low_uptime",
                "uptime": metrics["uptime"],
                "threshold": uptime_threshold
            })
    
    if unhealthy_endpoints:
        logger.error(f"API health issues detected: {len(unhealthy_endpoints)} endpoints affected")
        
        return RunRequest(
            run_key=f"api_health_remediation_{context.cursor}",
            run_config={
                "ops": {
                    "remediate_api_health_issues": {
                        "config": {
                            "unhealthy_endpoints": unhealthy_endpoints,
                            "performance_issues": performance_issues
                        }
                    }
                }
            },
            tags={
                "trigger": "api_health",
                "unhealthy_count": str(len(unhealthy_endpoints)),
                "performance_issues": str(len(performance_issues))
            }
        )
    
    elif performance_issues:
        logger.warning(f"API performance issues detected: {len(performance_issues)} endpoints slow")
        
        return RunRequest(
            run_key=f"api_performance_optimization_{context.cursor}",
            run_config={
                "ops": {
                    "optimize_api_performance": {
                        "config": {
                            "performance_issues": performance_issues
                        }
                    }
                }
            },
            tags={
                "trigger": "api_performance",
                "slow_endpoints": str(len(performance_issues))
            }
        )
    
    else:
        return SkipReason("All API endpoints are healthy")


@sensor(
    name="usage_spike_sensor",
    description="Detect unusual usage patterns in public API",
    minimum_interval_seconds=600,  # Check every 10 minutes
)
def usage_spike_sensor(context: SensorEvaluationContext):
    """
    Monitor for usage spikes that might indicate abuse or viral content
    Trigger rate limiting adjustments if needed
    """
    logger = get_dagster_logger()
    
    # Simulate usage pattern analysis
    current_metrics = {
        "requests_per_minute": 450,  # Current rate
        "unique_ips_per_hour": 280,
        "error_rate_percent": 2.1,
        "top_user_requests_per_hour": 1200,  # Single organization's usage
        "geographic_distribution": {
            "normal_regions": 85,
            "unusual_regions": 15  # Percent from unexpected regions
        }
    }
    
    # Historical baselines (simulated)
    baselines = {
        "requests_per_minute": {"avg": 180, "max": 350, "spike_threshold": 400},
        "unique_ips_per_hour": {"avg": 120, "max": 200, "spike_threshold": 300},
        "error_rate_percent": {"avg": 0.5, "max": 1.5, "spike_threshold": 5.0},
        "top_user_requests_per_hour": {"avg": 500, "max": 800, "spike_threshold": 1000},
    }
    
    spikes_detected = []
    
    # Check for usage spikes
    for metric, value in current_metrics.items():
        if metric in baselines:
            baseline = baselines[metric]
            if value > baseline["spike_threshold"]:
                spikes_detected.append({
                    "metric": metric,
                    "current_value": value,
                    "threshold": baseline["spike_threshold"],
                    "severity": "high" if value > baseline["spike_threshold"] * 1.5 else "medium"
                })
    
    # Check for suspicious geographic patterns
    if current_metrics["geographic_distribution"]["unusual_regions"] > 30:
        spikes_detected.append({
            "metric": "geographic_anomaly",
            "current_value": current_metrics["geographic_distribution"]["unusual_regions"],
            "threshold": 30,
            "severity": "medium"
        })
    
    if spikes_detected:
        high_severity_spikes = [s for s in spikes_detected if s["severity"] == "high"]
        
        if high_severity_spikes:
            logger.warning(f"High severity usage spikes detected: {len(high_severity_spikes)} metrics")
            
            return RunRequest(
                run_key=f"usage_spike_response_{context.cursor}",
                run_config={
                    "ops": {
                        "respond_to_usage_spike": {
                            "config": {
                                "spikes_detected": spikes_detected,
                                "current_metrics": current_metrics,
                                "response_level": "immediate"
                            }
                        }
                    }
                },
                tags={
                    "trigger": "usage_spike",
                    "severity": "high",
                    "spike_count": str(len(spikes_detected))
                }
            )
        else:
            logger.info(f"Medium severity usage spikes detected: {len(spikes_detected)} metrics")
            return SkipReason("Medium severity spikes - monitoring only")
    
    else:
        return SkipReason("Usage patterns are within normal limits")


@sensor(
    name="data_freshness_public_sensor",
    description="Monitor freshness of public emergency data",
    minimum_interval_seconds=900,  # Check every 15 minutes
)
def data_freshness_public_sensor(context: SensorEvaluationContext):
    """
    Monitor data freshness for public emergency data sources
    Ensure timely updates for public safety information
    """
    logger = get_dagster_logger()
    
    # Define freshness thresholds for public data (more stringent for safety)
    freshness_thresholds = {
        "noaa_weather_alerts": 15,     # 15 minutes - critical for safety
        "fema_disaster_declarations": 240,  # 4 hours - important for response
        "coagmet_weather_data": 60,    # 1 hour - agricultural monitoring
        "usda_agricultural_data": 1440, # 24 hours - less time-critical
    }
    
    # Simulate data freshness checks
    current_freshness = {
        "noaa_weather_alerts": 8,      # 8 minutes - good
        "fema_disaster_declarations": 45,   # 45 minutes - good
        "coagmet_weather_data": 75,    # 75 minutes - stale
        "usda_agricultural_data": 360, # 6 hours - good
    }
    
    stale_sources = []
    critical_stale = []
    
    for source, age_minutes in current_freshness.items():
        threshold = freshness_thresholds[source]
        
        if age_minutes > threshold:
            staleness_info = {
                "source": source,
                "age_minutes": age_minutes,
                "threshold_minutes": threshold,
                "staleness_factor": age_minutes / threshold
            }
            
            stale_sources.append(staleness_info)
            
            # Critical if more than 2x the threshold
            if age_minutes > threshold * 2:
                critical_stale.append(staleness_info)
    
    if critical_stale:
        logger.error(f"Critical data staleness detected: {len(critical_stale)} sources")
        
        return RunRequest(
            run_key=f"critical_freshness_alert_{context.cursor}",
            run_config={
                "ops": {
                    "handle_critical_data_staleness": {
                        "config": {
                            "critical_stale_sources": critical_stale,
                            "all_stale_sources": stale_sources,
                            "alert_level": "critical"
                        }
                    }
                }
            },
            tags={
                "trigger": "data_freshness",
                "severity": "critical",
                "stale_sources": str(len(critical_stale))
            }
        )
    
    elif stale_sources:
        logger.warning(f"Data staleness detected: {len(stale_sources)} sources")
        
        return RunRequest(
            run_key=f"freshness_remediation_{context.cursor}",
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
                "severity": "warning",
                "stale_sources": str(len(stale_sources))
            }
        )
    
    else:
        return SkipReason("All public data sources are fresh")


@sensor(
    name="public_data_quality_sensor",
    description="Monitor quality of public data for API consumers",
    minimum_interval_seconds=1800,  # Check every 30 minutes
)
def public_data_quality_sensor(context: SensorEvaluationContext):
    """
    Monitor data quality issues that could affect public API consumers
    Trigger quality improvement workflows
    """
    logger = get_dagster_logger()
    
    # Simulate data quality monitoring
    quality_metrics = {
        "fema_disaster_declarations": {
            "completeness": 97.8,
            "duplicates": 0.2,
            "format_errors": 0.1,
            "missing_critical_fields": 1.5
        },
        "noaa_weather_alerts": {
            "completeness": 99.1,
            "duplicates": 0.0,
            "format_errors": 0.3,
            "missing_critical_fields": 0.5
        },
        "coagmet_weather_data": {
            "completeness": 94.2,  # Below threshold
            "duplicates": 0.5,
            "format_errors": 1.2,
            "missing_critical_fields": 4.1  # High
        },
        "usda_agricultural_data": {
            "completeness": 96.5,
            "duplicates": 0.8,
            "format_errors": 0.4,
            "missing_critical_fields": 2.1
        }
    }
    
    # Quality thresholds
    thresholds = {
        "completeness": 95.0,
        "duplicates": 1.0,
        "format_errors": 2.0,
        "missing_critical_fields": 3.0
    }
    
    quality_issues = []
    
    for source, metrics in quality_metrics.items():
        source_issues = []
        
        for metric, value in metrics.items():
            threshold = thresholds[metric]
            
            if metric == "completeness" and value < threshold:
                source_issues.append({
                    "metric": metric,
                    "value": value,
                    "threshold": threshold,
                    "severity": "high" if value < threshold * 0.9 else "medium"
                })
            elif metric != "completeness" and value > threshold:
                source_issues.append({
                    "metric": metric,
                    "value": value,
                    "threshold": threshold,
                    "severity": "high" if value > threshold * 2 else "medium"
                })
        
        if source_issues:
            quality_issues.append({
                "source": source,
                "issues": source_issues,
                "issue_count": len(source_issues)
            })
    
    if quality_issues:
        high_severity_issues = [
            issue for issue in quality_issues 
            if any(i["severity"] == "high" for i in issue["issues"])
        ]
        
        if high_severity_issues:
            logger.error(f"High severity data quality issues: {len(high_severity_issues)} sources affected")
            
            return RunRequest(
                run_key=f"quality_remediation_{context.cursor}",
                run_config={
                    "ops": {
                        "remediate_data_quality_issues": {
                            "config": {
                                "quality_issues": quality_issues,
                                "severity": "high",
                                "affected_sources": [i["source"] for i in high_severity_issues]
                            }
                        }
                    }
                },
                tags={
                    "trigger": "data_quality",
                    "severity": "high",
                    "affected_sources": str(len(high_severity_issues))
                }
            )
        else:
            logger.info(f"Medium severity data quality issues: {len(quality_issues)} sources affected")
            return SkipReason("Only medium severity quality issues detected")
    
    else:
        return SkipReason("Data quality is within acceptable limits")


@sensor(
    name="external_api_availability_sensor",
    description="Monitor availability of external data source APIs",
    minimum_interval_seconds=600,  # Check every 10 minutes
)
def external_api_availability_sensor(context: SensorEvaluationContext):
    """
    Monitor external API availability for data sources
    Trigger failover or alerting when sources are unavailable
    """
    logger = get_dagster_logger()
    
    # Simulate external API health checks
    api_sources = {
        "fema_openfema_api": {
            "url": "https://www.fema.gov/api/open/v2/DisasterDeclarationsSummaries",
            "status": "available",
            "response_time_ms": 250,
            "last_success": datetime.now() - timedelta(minutes=2)
        },
        "noaa_weather_api": {
            "url": "https://api.weather.gov/alerts/active",
            "status": "available",
            "response_time_ms": 180,
            "last_success": datetime.now() - timedelta(minutes=1)
        },
        "coagmet_api": {
            "url": "https://coagmet.colostate.edu/data_access/web_service",
            "status": "degraded",  # Simulated issue
            "response_time_ms": 2500,
            "last_success": datetime.now() - timedelta(minutes=15)
        },
        "usda_api": {
            "url": "https://quickstats.nass.usda.gov/api/api_GET/",
            "status": "unavailable",  # Simulated outage
            "response_time_ms": None,
            "last_success": datetime.now() - timedelta(hours=2)
        }
    }
    
    unavailable_apis = []
    degraded_apis = []
    
    for api_name, details in api_sources.items():
        if details["status"] == "unavailable":
            unavailable_apis.append({
                "api": api_name,
                "url": details["url"],
                "last_success": details["last_success"].isoformat(),
                "outage_duration_minutes": (datetime.now() - details["last_success"]).total_seconds() / 60
            })
        elif details["status"] == "degraded":
            degraded_apis.append({
                "api": api_name,
                "url": details["url"],
                "response_time_ms": details["response_time_ms"],
                "issue": "slow_response"
            })
    
    if unavailable_apis:
        logger.error(f"External API outages detected: {len(unavailable_apis)} APIs unavailable")
        
        return RunRequest(
            run_key=f"api_outage_response_{context.cursor}",
            run_config={
                "ops": {
                    "handle_api_outages": {
                        "config": {
                            "unavailable_apis": unavailable_apis,
                            "degraded_apis": degraded_apis,
                            "response_level": "immediate"
                        }
                    }
                }
            },
            tags={
                "trigger": "api_availability",
                "unavailable_count": str(len(unavailable_apis)),
                "degraded_count": str(len(degraded_apis))
            }
        )
    
    elif degraded_apis:
        logger.warning(f"External API performance issues: {len(degraded_apis)} APIs degraded")
        
        return RunRequest(
            run_key=f"api_degradation_response_{context.cursor}",
            run_config={
                "ops": {
                    "handle_api_degradation": {
                        "config": {
                            "degraded_apis": degraded_apis
                        }
                    }
                }
            },
            tags={
                "trigger": "api_availability",
                "degraded_count": str(len(degraded_apis))
            }
        )
    
    else:
        return SkipReason("All external APIs are available and performing well")


@sensor(
    name="public_feedback_sensor",
    description="Monitor public feedback and API usage issues",
    minimum_interval_seconds=3600,  # Check every hour
)
def public_feedback_sensor(context: SensorEvaluationContext):
    """
    Monitor public feedback, GitHub issues, and user-reported problems
    Trigger response workflows for user experience issues
    """
    logger = get_dagster_logger()
    
    # Simulate monitoring of various feedback channels
    feedback_channels = {
        "github_issues": {
            "new_issues": 3,
            "high_priority": 1,
            "api_related": 2,
            "data_quality": 1
        },
        "api_error_reports": {
            "429_rate_limit": 15,  # Rate limit errors
            "500_server_errors": 2,
            "404_not_found": 8,
            "timeout_errors": 5
        },
        "user_feedback": {
            "positive_reviews": 12,
            "negative_reviews": 3,
            "feature_requests": 7,
            "bug_reports": 4
        }
    }
    
    # Thresholds for triggering responses
    response_needed = []
    
    # Check GitHub issues
    if feedback_channels["github_issues"]["high_priority"] > 0:
        response_needed.append({
            "channel": "github_issues",
            "priority": "high",
            "count": feedback_channels["github_issues"]["high_priority"],
            "action": "immediate_response"
        })
    
    # Check API errors
    total_api_errors = sum(feedback_channels["api_error_reports"].values())
    if total_api_errors > 20:  # Threshold for total errors per hour
        response_needed.append({
            "channel": "api_errors",
            "priority": "medium",
            "count": total_api_errors,
            "action": "investigate_errors"
        })
    
    # Check negative feedback ratio
    total_reviews = (feedback_channels["user_feedback"]["positive_reviews"] + 
                    feedback_channels["user_feedback"]["negative_reviews"])
    if total_reviews > 0:
        negative_ratio = feedback_channels["user_feedback"]["negative_reviews"] / total_reviews
        if negative_ratio > 0.3:  # More than 30% negative
            response_needed.append({
                "channel": "user_feedback",
                "priority": "medium",
                "negative_ratio": negative_ratio,
                "action": "improve_user_experience"
            })
    
    if response_needed:
        high_priority_responses = [r for r in response_needed if r["priority"] == "high"]
        
        if high_priority_responses:
            logger.warning(f"High priority public feedback requiring immediate attention: {len(high_priority_responses)} items")
            
            return RunRequest(
                run_key=f"public_feedback_response_{context.cursor}",
                run_config={
                    "ops": {
                        "respond_to_public_feedback": {
                            "config": {
                                "feedback_items": response_needed,
                                "priority_level": "high",
                                "feedback_summary": feedback_channels
                            }
                        }
                    }
                },
                tags={
                    "trigger": "public_feedback",
                    "priority": "high",
                    "response_count": str(len(response_needed))
                }
            )
        else:
            logger.info(f"Medium priority public feedback items: {len(response_needed)}")
            return SkipReason("Only medium priority feedback - will be addressed in next cycle")
    
    else:
        return SkipReason("No significant public feedback requiring immediate attention")


# Export sensors for use in definitions
public_sensors = [
    public_api_health_sensor,
    usage_spike_sensor,
    data_freshness_public_sensor,
    public_data_quality_sensor,
    external_api_availability_sensor,
    public_feedback_sensor,
]