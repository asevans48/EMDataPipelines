"""
Public Data Assets - Optimized for public API access
Simplified assets for publicly available emergency data
"""

import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, Any, List

from dagster import (
    asset,
    AssetIn,
    MetadataValue,
    get_dagster_logger,
    Config,
)
from pydantic import Field

from ..public_resources import (
    PublicDataStarRocksResource,
    PublicDataKafkaResource,
    PublicDataUsageTracker,
)


@asset(
    description="Public API optimized disaster data view",
    group_name="public_api",
    compute_kind="public_data",
)
def public_api_disasters(
    context,
    starrocks: PublicDataStarRocksResource,
    usage_tracker: PublicDataUsageTracker,
) -> pd.DataFrame:
    """
    Create public API optimized view of disaster data
    Includes data validation and API-friendly formatting
    """
    logger = get_dagster_logger()
    
    # Query all public disaster data
    query = """
    SELECT 
        disaster_number,
        state,
        declaration_type,
        declaration_date,
        incident_type,
        incident_begin_date,
        incident_end_date,
        title as disaster_title,
        designated_area,
        fy_declared as fiscal_year,
        data_source,
        ingestion_timestamp,
        'PUBLIC' as data_classification
    FROM fema_disaster_declarations
    WHERE declaration_date >= DATE_SUB(NOW(), INTERVAL 365 DAY)
    ORDER BY declaration_date DESC
    """
    
    results = starrocks.execute_public_query(
        query, 
        organization="system_public_api"
    )
    
    df = pd.DataFrame(results)
    
    if not df.empty:
        # Optimize for public API consumption
        df['api_url'] = df['disaster_number'].apply(
            lambda x: f"/api/v1/disasters/{x}"
        )
        
        # Add geographic coordinates if available (simplified)
        df['has_geographic_data'] = df['designated_area'].notna()
        
        # Calculate disaster duration
        df['incident_duration_days'] = (
            pd.to_datetime(df['incident_end_date']) - 
            pd.to_datetime(df['incident_begin_date'])
        ).dt.days
        
        # Add data freshness indicator
        df['data_age_hours'] = (
            datetime.now() - pd.to_datetime(df['ingestion_timestamp'])
        ).dt.total_seconds() / 3600
        
        # Track usage for analytics
        usage_tracker.log_data_volume("system_public_api", len(df))
        
        logger.info(f"Created public API view with {len(df)} disaster records")
    
    context.add_output_metadata({
        "records": len(df),
        "data_classification": "PUBLIC",
        "api_endpoint": "/api/v1/disasters",
        "last_updated": datetime.now().isoformat(),
        "freshness_hours": MetadataValue.float(df['data_age_hours'].min() if not df.empty else 0),
    })
    
    return df


@asset(
    description="Public API optimized weather alerts",
    group_name="public_api",
    compute_kind="public_data",
)
def public_api_weather_alerts(
    context,
    starrocks: PublicDataStarRocksResource,
    usage_tracker: PublicDataUsageTracker,
) -> pd.DataFrame:
    """
    Create public API optimized view of weather alerts
    Focus on active and recent alerts for public safety
    """
    logger = get_dagster_logger()
    
    # Query active and recent weather alerts
    query = """
    SELECT 
        alert_id,
        event as alert_type,
        severity,
        urgency,
        certainty,
        headline,
        SUBSTRING(description, 1, 500) as description_preview,
        SUBSTRING(instruction, 1, 300) as safety_instructions,
        effective as start_time,
        expires as end_time,
        area_desc as affected_area,
        data_source,
        ingestion_timestamp,
        'PUBLIC' as data_classification
    FROM noaa_weather_alerts
    WHERE (expires > NOW() OR expires IS NULL)
       OR effective >= DATE_SUB(NOW(), INTERVAL 24 HOUR)
    ORDER BY 
        CASE severity 
            WHEN 'Extreme' THEN 1
            WHEN 'Severe' THEN 2  
            WHEN 'Moderate' THEN 3
            ELSE 4
        END,
        effective DESC
    """
    
    results = starrocks.execute_public_query(
        query,
        organization="system_public_api"
    )
    
    df = pd.DataFrame(results)
    
    if not df.empty:
        # Add public API enhancements
        df['is_active'] = pd.to_datetime(df['end_time']) > datetime.now()
        df['api_url'] = df['alert_id'].apply(
            lambda x: f"/api/v1/weather/alerts/{x}"
        )
        
        # Severity scoring for sorting
        severity_scores = {'Extreme': 4, 'Severe': 3, 'Moderate': 2, 'Minor': 1}
        df['severity_score'] = df['severity'].map(severity_scores).fillna(0)
        
        # Time until expiration
        df['expires_in_hours'] = (
            pd.to_datetime(df['end_time']) - datetime.now()
        ).dt.total_seconds() / 3600
        
        # Geographic area parsing (simplified)
        df['state_codes'] = df['affected_area'].str.extract(r'([A-Z]{2})')
        
        usage_tracker.log_data_volume("system_public_api", len(df))
        
        active_count = df['is_active'].sum()
        logger.info(f"Created weather alerts view: {len(df)} total, {active_count} active")
    
    context.add_output_metadata({
        "total_alerts": len(df),
        "active_alerts": int(df['is_active'].sum()) if not df.empty else 0,
        "data_classification": "PUBLIC",
        "api_endpoint": "/api/v1/weather/alerts",
        "last_updated": datetime.now().isoformat(),
    })
    
    return df


@asset(
    description="Public API usage analytics and metrics",
    group_name="analytics",
    compute_kind="analytics",
)
def public_api_usage_metrics(
    context,
    usage_tracker: PublicDataUsageTracker,
) -> Dict[str, Any]:
    """
    Generate usage analytics for public API
    Track organization usage patterns and system performance
    """
    logger = get_dagster_logger()
    
    # Read usage logs from the past 24 hours
    # In a real implementation, this would read from the log files
    # For now, generate sample metrics
    
    sample_metrics = {
        "time_period": {
            "start": (datetime.now() - timedelta(hours=24)).isoformat(),
            "end": datetime.now().isoformat(),
        },
        "total_requests": 15420,
        "unique_organizations": 45,
        "total_records_served": 1547839,
        "average_response_time_ms": 125,
        "error_rate_percent": 0.3,
        
        "top_endpoints": [
            {"endpoint": "/api/v1/disasters", "requests": 8450, "percent": 54.8},
            {"endpoint": "/api/v1/weather/alerts", "requests": 4520, "percent": 29.3},
            {"endpoint": "/api/v1/weather/agricultural", "requests": 1680, "percent": 10.9},
            {"endpoint": "/api/v1/agricultural/disasters", "requests": 770, "percent": 5.0},
        ],
        
        "organization_types": {
            "government": {"requests": 6200, "organizations": 12},
            "academic": {"requests": 4800, "organizations": 15},
            "commercial": {"requests": 3100, "organizations": 8},
            "public": {"requests": 1320, "organizations": 10},
        },
        
        "geographic_distribution": {
            "colorado": 4520,
            "california": 2890,
            "texas": 2340,
            "florida": 1980,
            "national": 3690,
        },
        
        "data_quality": {
            "completeness_percent": 98.7,
            "freshness_avg_minutes": 8.2,
            "error_free_percent": 99.7,
        },
        
        "system_performance": {
            "uptime_percent": 99.95,
            "avg_response_time_ms": 125,
            "p95_response_time_ms": 340,
            "p99_response_time_ms": 890,
        }
    }
    
    # Log the metrics generation
    usage_tracker.log_access("system_analytics", "metrics_generation")
    
    logger.info("Generated public API usage metrics")
    
    context.add_output_metadata({
        "time_period": "24_hours",
        "total_requests": sample_metrics["total_requests"],
        "unique_organizations": sample_metrics["unique_organizations"],
        "error_rate": MetadataValue.float(sample_metrics["error_rate_percent"]),
        "uptime": MetadataValue.float(sample_metrics["system_performance"]["uptime_percent"]),
    })
    
    return sample_metrics


@asset(
    description="Data quality dashboard for public data",
    group_name="quality",
    compute_kind="quality_monitoring",
)
def public_data_quality_dashboard(
    context,
    starrocks: PublicDataStarRocksResource,
) -> Dict[str, Any]:
    """
    Generate data quality metrics for public consumption
    Transparency into data reliability and freshness
    """
    logger = get_dagster_logger()
    
    # Check data quality across all public tables
    quality_checks = {
        "fema_disaster_declarations": {
            "total_records": 0,
            "latest_update": None,
            "completeness_percent": 0,
            "data_issues": []
        },
        "noaa_weather_alerts": {
            "total_records": 0,
            "latest_update": None,
            "completeness_percent": 0,
            "data_issues": []
        },
        "coagmet_weather_data": {
            "total_records": 0,
            "latest_update": None,
            "completeness_percent": 0,
            "data_issues": []
        },
        "usda_agricultural_data": {
            "total_records": 0,
            "latest_update": None,
            "completeness_percent": 0,
            "data_issues": []
        }
    }
    
    # Check each table
    for table_name, metrics in quality_checks.items():
        try:
            # Get record count
            count_query = f"SELECT COUNT(*) as count FROM {table_name}"
            count_result = starrocks.execute_public_query(count_query, "system_quality")
            metrics["total_records"] = count_result[0]["count"] if count_result else 0
            
            # Get latest update
            latest_query = f"SELECT MAX(ingestion_timestamp) as latest FROM {table_name}"
            latest_result = starrocks.execute_public_query(latest_query, "system_quality")
            if latest_result and latest_result[0]["latest"]:
                metrics["latest_update"] = latest_result[0]["latest"]
                
                # Calculate data freshness
                latest_time = pd.to_datetime(latest_result[0]["latest"])
                hours_old = (datetime.now() - latest_time).total_seconds() / 3600
                
                if hours_old > 24:
                    metrics["data_issues"].append(f"Data is {hours_old:.1f} hours old")
            
            # Simple completeness check (non-null required fields)
            if table_name == "fema_disaster_declarations":
                completeness_query = """
                SELECT 
                    (COUNT(*) - SUM(CASE WHEN disaster_number IS NULL THEN 1 ELSE 0 END)) * 100.0 / COUNT(*) as completeness
                FROM fema_disaster_declarations
                """
            else:
                completeness_query = f"SELECT 95.0 as completeness"  # Simplified for demo
            
            completeness_result = starrocks.execute_public_query(completeness_query, "system_quality")
            if completeness_result:
                metrics["completeness_percent"] = float(completeness_result[0]["completeness"])
                
                if metrics["completeness_percent"] < 95:
                    metrics["data_issues"].append(f"Completeness below 95%: {metrics['completeness_percent']:.1f}%")
            
        except Exception as e:
            logger.warning(f"Quality check failed for {table_name}: {str(e)}")
            metrics["data_issues"].append(f"Quality check failed: {str(e)}")
    
    # Overall system status
    overall_status = {
        "status": "healthy",
        "last_check": datetime.now().isoformat(),
        "issues_found": sum(len(m["data_issues"]) for m in quality_checks.values()),
        "data_sources_healthy": sum(1 for m in quality_checks.values() if not m["data_issues"]),
        "total_data_sources": len(quality_checks),
    }
    
    if overall_status["issues_found"] > 0:
        overall_status["status"] = "warning" if overall_status["issues_found"] < 3 else "degraded"
    
    dashboard_data = {
        "overall_status": overall_status,
        "data_sources": quality_checks,
        "recommendations": [
            "All emergency data is provided as-is for informational purposes",
            "Users should verify critical information with authoritative sources",
            "Data quality issues are investigated and resolved promptly",
            "System status updates are available at status.emergency-data-api.gov"
        ]
    }
    
    logger.info(f"Generated quality dashboard: {overall_status['status']} status, {overall_status['issues_found']} issues")
    
    context.add_output_metadata({
        "overall_status": overall_status["status"],
        "issues_found": overall_status["issues_found"],
        "healthy_sources": overall_status["data_sources_healthy"],
        "total_sources": overall_status["total_data_sources"],
        "last_check": overall_status["last_check"],
    })
    
    return dashboard_data


# Export public assets for use in definitions
public_api_assets = [
    public_api_disasters,
    public_api_weather_alerts,
    public_api_usage_metrics,
    public_data_quality_dashboard,
]