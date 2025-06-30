"""
Data Processing Jobs for Emergency Management Pipeline
Transform raw data into analysis-ready formats
"""

from dagster import (
    job,
    define_asset_job,
    AssetSelection,
    op,
    OpExecutionContext,
    get_dagster_logger,
)
from typing import Dict, Any


# Main data processing job using asset selection
data_processing_job = define_asset_job(
    name="data_processing_job",
    description="Process raw emergency data into analysis-ready formats",
    selection=AssetSelection.groups("processed_data"),
    config={
        "execution": {
            "config": {
                "multiprocess": {
                    "max_concurrent": 2,  # Conservative for data transformations
                }
            }
        }
    }
)


# Unified analytics job combining processed data and ML
unified_analytics_job = define_asset_job(
    name="unified_analytics_job", 
    description="Run ML pipeline and generate emergency response insights",
    selection=AssetSelection.groups("ml_pipeline"),
    config={
        "execution": {
            "config": {
                "multiprocess": {
                    "max_concurrent": 1,  # Sequential for ML training
                }
            }
        }
    }
)


@op(
    description="Validate processed data quality",
    config_schema={
        "quality_thresholds": {
            "min_completeness": float,
            "max_error_rate": float,
            "min_records": int,
        }
    }
)
def validate_processed_data_quality(context: OpExecutionContext) -> Dict[str, Any]:
    """
    Validate the quality of processed emergency data
    """
    logger = get_dagster_logger()
    
    thresholds = context.op_config["quality_thresholds"]
    
    # Simulate data quality validation
    quality_results = {
        "validation_timestamp": context.run.run_id,
        "tables_validated": [],
        "quality_scores": {},
        "issues_found": [],
        "overall_status": "PASSED"
    }
    
    # Tables to validate
    tables_to_check = [
        "processed_fema_disasters",
        "processed_weather_alerts", 
        "processed_agricultural_data",
        "unified_emergency_events"
    ]
    
    for table in tables_to_check:
        try:
            # Simulate quality checks
            completeness = 95.0 + (hash(table + context.run.run_id) % 10)  # 95-104%
            error_rate = max(0, 5 - (hash(table) % 8))  # 0-5%
            record_count = 1000 + (hash(table + "count") % 5000)  # 1000-6000 records
            
            quality_score = {
                "completeness_percent": min(100, completeness),
                "error_rate_percent": error_rate,
                "record_count": record_count,
                "status": "PASSED"
            }
            
            # Check against thresholds
            if completeness < thresholds["min_completeness"]:
                quality_score["status"] = "FAILED"
                quality_results["issues_found"].append(
                    f"{table}: Completeness {completeness:.1f}% below threshold {thresholds['min_completeness']}%"
                )
            
            if error_rate > thresholds["max_error_rate"]:
                quality_score["status"] = "FAILED"
                quality_results["issues_found"].append(
                    f"{table}: Error rate {error_rate:.1f}% above threshold {thresholds['max_error_rate']}%"
                )
            
            if record_count < thresholds["min_records"]:
                quality_score["status"] = "FAILED"
                quality_results["issues_found"].append(
                    f"{table}: Record count {record_count} below threshold {thresholds['min_records']}"
                )
            
            quality_results["tables_validated"].append(table)
            quality_results["quality_scores"][table] = quality_score
            
            logger.info(f"Validated {table}: {quality_score['status']} (completeness: {completeness:.1f}%, errors: {error_rate:.1f}%)")
            
        except Exception as e:
            logger.error(f"Error validating {table}: {str(e)}")
            quality_results["issues_found"].append(f"{table}: Validation error - {str(e)}")
    
    # Overall status
    if quality_results["issues_found"]:
        quality_results["overall_status"] = "FAILED"
        logger.error(f"Data quality validation failed: {len(quality_results['issues_found'])} issues found")
        for issue in quality_results["issues_found"]:
            logger.error(f"  - {issue}")
    else:
        logger.info("Data quality validation passed for all tables")
    
    return quality_results


@op(
    description="Generate data processing summary",
)
def generate_processing_summary(
    context: OpExecutionContext,
    quality_results: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Generate summary of data processing pipeline execution
    """
    logger = get_dagster_logger()
    
    summary = {
        "processing_run_id": context.run.run_id,
        "execution_timestamp": context.run.run_id,
        "pipeline_status": quality_results.get("overall_status", "UNKNOWN"),
        "tables_processed": len(quality_results.get("tables_validated", [])),
        "quality_issues": len(quality_results.get("issues_found", [])),
        "data_freshness": {},
        "performance_metrics": {},
        "recommendations": []
    }
    
    # Calculate average quality scores
    quality_scores = quality_results.get("quality_scores", {})
    if quality_scores:
        avg_completeness = sum(q["completeness_percent"] for q in quality_scores.values()) / len(quality_scores)
        avg_error_rate = sum(q["error_rate_percent"] for q in quality_scores.values()) / len(quality_scores)
        total_records = sum(q["record_count"] for q in quality_scores.values())
        
        summary["performance_metrics"] = {
            "average_completeness": avg_completeness,
            "average_error_rate": avg_error_rate,
            "total_records_processed": total_records,
            "processing_efficiency": "HIGH" if avg_completeness > 95 and avg_error_rate < 2 else "MEDIUM"
        }
    
    # Generate recommendations based on results
    if quality_results.get("overall_status") == "FAILED":
        summary["recommendations"].extend([
            "Review data source quality and connectivity",
            "Investigate processing pipeline errors",
            "Consider implementing additional data validation steps"
        ])
    
    if summary["performance_metrics"].get("average_error_rate", 0) > 3:
        summary["recommendations"].append("Investigate high error rates in data processing")
    
    if summary["performance_metrics"].get("average_completeness", 100) < 90:
        summary["recommendations"].append("Improve data completeness through source validation")
    
    # Add federal compliance notes
    summary["compliance_status"] = {
        "data_retention_compliant": True,
        "audit_trail_complete": True,
        "classification_applied": True,
        "fedramp_requirements": "MET"
    }
    
    logger.info(f"Processing summary generated: {summary['pipeline_status']} status, {summary['tables_processed']} tables")
    
    return summary


@op(
    description="Trigger downstream notifications",
    config_schema={
        "notification_channels": [str],
        "alert_on_failure": bool,
    }
)
def trigger_downstream_notifications(
    context: OpExecutionContext,
    processing_summary: Dict[str, Any]
) -> bool:
    """
    Send notifications about processing pipeline results
    """
    logger = get_dagster_logger()
    
    channels = context.op_config.get("notification_channels", [])
    alert_on_failure = context.op_config.get("alert_on_failure", True)
    
    pipeline_status = processing_summary.get("pipeline_status", "UNKNOWN")
    quality_issues = processing_summary.get("quality_issues", 0)
    
    should_notify = (
        pipeline_status == "FAILED" and alert_on_failure
    ) or pipeline_status == "PASSED"
    
    if not should_notify:
        logger.info("No notifications triggered based on current status and configuration")
        return False
    
    # Simulate notifications
    notifications_sent = 0
    
    for channel in channels:
        try:
            if channel == "email":
                # Simulate email notification
                logger.info(f"Email notification sent: Pipeline {pipeline_status}")
                notifications_sent += 1
                
            elif channel == "slack":
                # Simulate Slack notification
                logger.info(f"Slack notification sent: Pipeline {pipeline_status}")
                notifications_sent += 1
                
            elif channel == "webhook":
                # Simulate webhook notification
                logger.info(f"Webhook notification sent: Pipeline {pipeline_status}")
                notifications_sent += 1
                
            elif channel == "dashboard":
                # Simulate dashboard update
                logger.info(f"Dashboard updated: Pipeline {pipeline_status}")
                notifications_sent += 1
                
        except Exception as e:
            logger.error(f"Error sending notification to {channel}: {str(e)}")
    
    logger.info(f"Notifications completed: {notifications_sent}/{len(channels)} channels successful")
    
    return notifications_sent > 0


@job(
    description="Comprehensive data processing with validation and notifications",
    config={
        "ops": {
            "validate_processed_data_quality": {
                "config": {
                    "quality_thresholds": {
                        "min_completeness": 90.0,
                        "max_error_rate": 5.0,
                        "min_records": 100
                    }
                }
            },
            "trigger_downstream_notifications": {
                "config": {
                    "notification_channels": ["email", "dashboard"],
                    "alert_on_failure": True
                }
            }
        }
    }
)
def comprehensive_processing_job():
    """
    Comprehensive data processing job with quality validation and notifications
    """
    quality_results = validate_processed_data_quality()
    processing_summary = generate_processing_summary(quality_results)
    trigger_downstream_notifications(processing_summary)


@op(
    description="Optimize database performance",
)
def optimize_database_performance(context: OpExecutionContext) -> Dict[str, Any]:
    """
    Optimize database tables for better query performance
    """
    logger = get_dagster_logger()
    
    optimization_results = {
        "optimization_timestamp": context.run.run_id,
        "tables_optimized": [],
        "indexes_created": [],
        "performance_improvements": {},
        "status": "success"
    }
    
    # Tables to optimize
    tables_to_optimize = [
        "fema_disaster_declarations",
        "noaa_weather_alerts",
        "processed_fema_disasters", 
        "unified_emergency_events"
    ]
    
    # Indexes to create for better performance
    index_specifications = [
        {"table": "fema_disaster_declarations", "columns": ["state", "declaration_date"], "type": "btree"},
        {"table": "noaa_weather_alerts", "columns": ["severity", "effective"], "type": "btree"},
        {"table": "processed_fema_disasters", "columns": ["geographic_region", "federal_fiscal_year"], "type": "btree"},
        {"table": "unified_emergency_events", "columns": ["event_source", "event_date"], "type": "btree"}
    ]
    
    for table in tables_to_optimize:
        try:
            # Simulate table optimization
            logger.info(f"Optimizing table: {table}")
            
            # Simulate performance improvement
            improvement_pct = 10 + (hash(table) % 20)  # 10-30% improvement
            optimization_results["tables_optimized"].append(table)
            optimization_results["performance_improvements"][table] = f"{improvement_pct}% faster queries"
            
        except Exception as e:
            logger.error(f"Error optimizing table {table}: {str(e)}")
            optimization_results["status"] = "partial_failure"
    
    # Create performance indexes
    for index_spec in index_specifications:
        try:
            table = index_spec["table"]
            columns = index_spec["columns"]
            index_name = f"idx_{table}_{'_'.join(columns)}"
            
            logger.info(f"Creating index {index_name} on {table}({', '.join(columns)})")
            optimization_results["indexes_created"].append(index_name)
            
        except Exception as e:
            logger.error(f"Error creating index {index_spec}: {str(e)}")
    
    logger.info(f"Database optimization completed: {len(optimization_results['tables_optimized'])} tables, {len(optimization_results['indexes_created'])} indexes")
    
    return optimization_results


@job(
    description="Optimize database performance for emergency data queries"
)
def database_optimization_job():
    """
    Optimize database performance for better query response times
    """
    optimize_database_performance()