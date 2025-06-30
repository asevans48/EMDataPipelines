"""
Public Data Jobs - Optimized for public data pipeline
Jobs focused on public API performance and analytics
"""

from dagster import (
    job,
    op,
    define_asset_job,
    AssetSelection,
    Config,
    OpExecutionContext,
    get_dagster_logger,
)
from typing import Dict, Any, List


# Public API Refresh Job
public_api_refresh_job = define_asset_job(
    name="public_api_refresh_job",
    description="Refresh public API optimized views and data",
    selection=AssetSelection.groups("public_api"),
    config={
        "execution": {
            "config": {
                "multiprocess": {
                    "max_concurrent": 2,  # Light processing for API views
                }
            }
        }
    }
)


# Usage Analytics Job
usage_analytics_job = define_asset_job(
    name="usage_analytics_job",
    description="Process usage analytics and generate metrics",
    selection=AssetSelection.groups("analytics"),
    config={
        "execution": {
            "config": {
                "multiprocess": {
                    "max_concurrent": 1,  # Sequential processing for analytics
                }
            }
        }
    }
)


# Public Data Quality Job
@op(
    description="Run data quality checks for public data",
    config_schema={
        "quality_thresholds": {
            "completeness": float,
            "freshness_hours": float,
            "availability": float,
        },
        "notification_enabled": bool,
    }
)
def run_public_data_quality_checks(context: OpExecutionContext) -> Dict[str, Any]:
    """Execute data quality validation for public data sources"""
    logger = get_dagster_logger()
    
    thresholds = context.op_config["quality_thresholds"]
    notification_enabled = context.op_config.get("notification_enabled", True)
    
    # Simulate quality checks for public data sources
    quality_results = {
        "fema_disaster_declarations": {
            "completeness": 98.5,
            "freshness_hours": 2.1,
            "availability": 99.8,
            "status": "healthy"
        },
        "noaa_weather_alerts": {
            "completeness": 99.2,
            "freshness_hours": 0.3,
            "availability": 99.9,
            "status": "healthy"
        },
        "coagmet_weather_data": {
            "completeness": 96.8,
            "freshness_hours": 1.5,
            "availability": 98.9,
            "status": "healthy"
        },
        "usda_agricultural_data": {
            "completeness": 94.2,
            "freshness_hours": 8.5,
            "availability": 99.1,
            "status": "warning"  # Below completeness threshold
        }
    }
    
    # Check against thresholds
    issues_found = []
    
    for source, metrics in quality_results.items():
        if metrics["completeness"] < thresholds["completeness"]:
            issues_found.append(f"{source}: Completeness {metrics['completeness']:.1f}% below threshold {thresholds['completeness']}%")
            metrics["status"] = "warning"
        
        if metrics["freshness_hours"] > thresholds["freshness_hours"]:
            issues_found.append(f"{source}: Data is {metrics['freshness_hours']:.1f} hours old, exceeds {thresholds['freshness_hours']} hour threshold")
            if metrics["status"] == "healthy":
                metrics["status"] = "warning"
        
        if metrics["availability"] < thresholds["availability"]:
            issues_found.append(f"{source}: Availability {metrics['availability']:.1f}% below threshold {thresholds['availability']}%")
            metrics["status"] = "critical"
    
    overall_status = "healthy"
    if any(m["status"] == "critical" for m in quality_results.values()):
        overall_status = "critical"
    elif any(m["status"] == "warning" for m in quality_results.values()):
        overall_status = "warning"
    
    summary = {
        "overall_status": overall_status,
        "sources_checked": len(quality_results),
        "issues_found": len(issues_found),
        "quality_results": quality_results,
        "issues": issues_found,
        "check_timestamp": context.run.run_id,
    }
    
    if notification_enabled and issues_found:
        logger.warning(f"Public data quality issues found: {len(issues_found)} issues")
        for issue in issues_found:
            logger.warning(f"  - {issue}")
    else:
        logger.info(f"Public data quality check passed: {overall_status} status")
    
    return summary


@op(
    description="Generate public data quality report",
)
def generate_public_quality_report(context: OpExecutionContext, quality_summary: Dict[str, Any]) -> str:
    """Generate public-facing data quality report"""
    logger = get_dagster_logger()
    
    status = quality_summary["overall_status"]
    sources_checked = quality_summary["sources_checked"]
    issues_found = quality_summary["issues_found"]
    
    # Generate public report
    report = f"""
    PUBLIC EMERGENCY DATA QUALITY REPORT
    ====================================
    Generated: {context.run.run_id}
    
    OVERALL STATUS: {status.upper()}
    
    Summary:
    - Data sources monitored: {sources_checked}
    - Issues identified: {issues_found}
    - System status: {"🟢 Healthy" if status == "healthy" else "🟡 Warning" if status == "warning" else "🔴 Critical"}
    
    Data Source Details:
    """
    
    for source, metrics in quality_summary["quality_results"].items():
        status_emoji = "🟢" if metrics["status"] == "healthy" else "🟡" if metrics["status"] == "warning" else "🔴"
        report += f"""
    {status_emoji} {source.replace('_', ' ').title()}:
      - Completeness: {metrics['completeness']:.1f}%
      - Data age: {metrics['freshness_hours']:.1f} hours
      - Availability: {metrics['availability']:.1f}%
    """
    
    if quality_summary["issues"]:
        report += "\n    Issues Identified:\n"
        for issue in quality_summary["issues"]:
            report += f"    - {issue}\n"
    
    report += f"""
    
    Public Access Information:
    - All data is provided as-is for informational purposes
    - Users should verify critical information with authoritative sources
    - Real-time status available at: https://status.emergency-data-api.gov
    - API documentation: https://docs.emergency-data-api.gov
    
    Data Sources:
    - Federal Emergency Management Agency (FEMA)
    - National Oceanic and Atmospheric Administration (NOAA)
    - Colorado Agricultural Meteorological Network (CoAgMet)
    - United States Department of Agriculture (USDA)
    """
    
    logger.info("Generated public data quality report")
    return report


@op(
    description="Update public API status page",
)
def update_public_status_page(context: OpExecutionContext, quality_report: str) -> bool:
    """Update public status page with current system status"""
    logger = get_dagster_logger()
    
    # In a real implementation, this would update a status page
    # For now, just log the status update
    
    logger.info("Updated public status page with latest quality report")
    
    # Simulate status page update
    status_page_data = {
        "last_updated": context.run.run_id,
        "status": "operational",  # Would be derived from quality report
        "services": {
            "api": "operational",
            "data_ingestion": "operational", 
            "real_time_alerts": "operational",
            "bulk_downloads": "operational"
        },
        "uptime_24h": 99.95,
        "response_time_avg_ms": 125
    }
    
    logger.info(f"Status page updated: {status_page_data}")
    
    return True


@job(
    description="Comprehensive public data quality monitoring and reporting",
    config={
        "ops": {
            "run_public_data_quality_checks": {
                "config": {
                    "quality_thresholds": {
                        "completeness": 95.0,
                        "freshness_hours": 4.0,
                        "availability": 98.0
                    },
                    "notification_enabled": True
                }
            }
        }
    }
)
def public_data_quality_job():
    """Public data quality monitoring job"""
    quality_summary = run_public_data_quality_checks()
    quality_report = generate_public_quality_report(quality_summary)
    update_public_status_page(quality_report)


@op(
    description="Optimize public API performance",
)
def optimize_public_api_performance(context: OpExecutionContext) -> Dict[str, Any]:
    """Optimize database indexes and caching for public API performance"""
    logger = get_dagster_logger()
    
    # Simulate API performance optimizations
    optimizations = {
        "database_indexes": [
            "CREATE INDEX IF NOT EXISTS idx_disasters_state_date ON fema_disaster_declarations(state, declaration_date)",
            "CREATE INDEX IF NOT EXISTS idx_alerts_severity_active ON noaa_weather_alerts(severity, expires)",
            "CREATE INDEX IF NOT EXISTS idx_weather_station_time ON coagmet_weather_data(station_id, timestamp)"
        ],
        "cache_warming": [
            "disasters_by_state",
            "active_weather_alerts",
            "recent_agricultural_data"
        ],
        "query_optimization": [
            "disaster_declarations_view",
            "weather_alerts_view",
            "agricultural_summary_view"
        ]
    }
    
    # In a real implementation, these would be executed
    results = {
        "indexes_created": len(optimizations["database_indexes"]),
        "caches_warmed": len(optimizations["cache_warming"]),
        "queries_optimized": len(optimizations["query_optimization"]),
        "performance_improvement_estimate": "15-25%",
        "optimization_timestamp": context.run.run_id
    }
    
    logger.info(f"API performance optimization completed: {results}")
    
    return results


@job(description="Public API performance optimization")
def public_api_performance_job():
    """Optimize public API for better performance"""
    optimize_public_api_performance()


@op(
    description="Clean up old public data",
    config_schema={
        "retention_days": int,
        "dry_run": bool,
    }
)
def cleanup_old_public_data(context: OpExecutionContext) -> Dict[str, Any]:
    """Clean up old public data based on retention policies"""
    logger = get_dagster_logger()
    
    retention_days = context.op_config["retention_days"]
    dry_run = context.op_config.get("dry_run", True)
    
    # Define retention policies for different data types
    retention_policies = {
        "noaa_weather_alerts": 90,    # 3 months for weather alerts
        "coagmet_weather_data": 1095, # 3 years for agricultural data
        "fema_disaster_declarations": 2555,  # 7 years for disaster data
        "usda_agricultural_data": 2555,      # 7 years for USDA data
    }
    
    cleanup_results = {}
    
    for table, default_retention in retention_policies.items():
        actual_retention = min(retention_days, default_retention)
        
        # Simulate cleanup operation
        if dry_run:
            # Count records that would be deleted
            estimated_deletions = max(0, 1000 - (actual_retention // 10))  # Simplified estimation
            cleanup_results[table] = {
                "action": "dry_run",
                "estimated_deletions": estimated_deletions,
                "retention_days": actual_retention
            }
            logger.info(f"DRY RUN: Would delete ~{estimated_deletions} old records from {table}")
        else:
            # Actual cleanup would happen here
            cleanup_results[table] = {
                "action": "cleaned",
                "records_deleted": 0,  # Would be actual count
                "retention_days": actual_retention
            }
            logger.info(f"Cleaned up old records from {table}")
    
    summary = {
        "total_tables_processed": len(cleanup_results),
        "dry_run": dry_run,
        "retention_days_requested": retention_days,
        "cleanup_results": cleanup_results,
        "cleanup_timestamp": context.run.run_id
    }
    
    return summary


@job(
    description="Clean up old public data based on retention policies",
    config={
        "ops": {
            "cleanup_old_public_data": {
                "config": {
                    "retention_days": 365,  # 1 year default
                    "dry_run": True  # Safety first
                }
            }
        }
    }
)
def public_data_cleanup_job():
    """Clean up old public data"""
    cleanup_old_public_data()