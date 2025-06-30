"""
Data Quality Jobs for Emergency Management Pipeline
Federal compliance monitoring and data validation
"""

from dagster import (
    job,
    define_asset_job,
    AssetSelection,
    op,
    OpExecutionContext,
    get_dagster_logger,
)
from typing import Dict, Any, List
from datetime import datetime, timedelta


# Main data quality job using asset selection
data_quality_job = define_asset_job(
    name="data_quality_job",
    description="Monitor data quality across all emergency management sources",
    selection=AssetSelection.groups("monitoring"),
    config={
        "execution": {
            "config": {
                "multiprocess": {
                    "max_concurrent": 1,  # Sequential for quality checks
                }
            }
        }
    }
)


@op(
    description="Run comprehensive data quality checks",
    config_schema={
        "quality_rules": {
            "completeness_threshold": float,
            "accuracy_threshold": float,
            "timeliness_threshold_hours": float,
            "consistency_threshold": float,
        },
        "tables_to_check": [str],
    }
)
def run_data_quality_checks(context: OpExecutionContext) -> Dict[str, Any]:
    """
    Run comprehensive data quality checks across all emergency data sources
    """
    logger = get_dagster_logger()
    
    rules = context.op_config["quality_rules"]
    tables = context.op_config["tables_to_check"]
    
    quality_report = {
        "check_timestamp": datetime.now().isoformat(),
        "quality_rules_applied": rules,
        "tables_checked": [],
        "quality_metrics": {},
        "violations": [],
        "overall_score": 0.0,
        "compliance_status": "COMPLIANT"
    }
    
    total_score = 0.0
    tables_processed = 0
    
    for table in tables:
        try:
            logger.info(f"Running quality checks for table: {table}")
            
            # Simulate data quality metrics
            table_metrics = {
                "table_name": table,
                "record_count": 1000 + (hash(table) % 10000),
                "completeness": 95.0 + (hash(table + "completeness") % 10),  # 95-104%
                "accuracy": 96.0 + (hash(table + "accuracy") % 8),  # 96-103%
                "timeliness_hours": max(0.1, 48 - (hash(table + "time") % 50)),  # 0.1-48 hours
                "consistency": 92.0 + (hash(table + "consistency") % 15),  # 92-106%
                "uniqueness": 98.0 + (hash(table + "unique") % 4),  # 98-101%
                "validity": 94.0 + (hash(table + "valid") % 12),  # 94-105%
            }
            
            # Normalize percentages to max 100%
            for metric in ["completeness", "accuracy", "consistency", "uniqueness", "validity"]:
                table_metrics[metric] = min(100.0, table_metrics[metric])
            
            # Check against thresholds
            violations = []
            
            if table_metrics["completeness"] < rules["completeness_threshold"]:
                violations.append({
                    "rule": "completeness",
                    "value": table_metrics["completeness"],
                    "threshold": rules["completeness_threshold"],
                    "severity": "HIGH"
                })
            
            if table_metrics["accuracy"] < rules["accuracy_threshold"]:
                violations.append({
                    "rule": "accuracy", 
                    "value": table_metrics["accuracy"],
                    "threshold": rules["accuracy_threshold"],
                    "severity": "HIGH"
                })
            
            if table_metrics["timeliness_hours"] > rules["timeliness_threshold_hours"]:
                violations.append({
                    "rule": "timeliness",
                    "value": table_metrics["timeliness_hours"],
                    "threshold": rules["timeliness_threshold_hours"],
                    "severity": "MEDIUM"
                })
            
            if table_metrics["consistency"] < rules["consistency_threshold"]:
                violations.append({
                    "rule": "consistency",
                    "value": table_metrics["consistency"],
                    "threshold": rules["consistency_threshold"],
                    "severity": "MEDIUM"
                })
            
            # Calculate table quality score
            table_score = (
                table_metrics["completeness"] * 0.25 +
                table_metrics["accuracy"] * 0.25 +
                (100 - min(100, table_metrics["timeliness_hours"] * 2)) * 0.20 +  # Lower is better for timeliness
                table_metrics["consistency"] * 0.15 +
                table_metrics["uniqueness"] * 0.10 +
                table_metrics["validity"] * 0.05
            ) / 100
            
            table_metrics["quality_score"] = table_score
            table_metrics["violations"] = violations
            table_metrics["status"] = "PASSED" if not violations else "FAILED"
            
            quality_report["tables_checked"].append(table)
            quality_report["quality_metrics"][table] = table_metrics
            quality_report["violations"].extend([{**v, "table": table} for v in violations])
            
            total_score += table_score
            tables_processed += 1
            
            status_log = f"Quality check for {table}: {table_metrics['status']} (score: {table_score:.2f})"
            if violations:
                status_log += f", {len(violations)} violations"
                logger.warning(status_log)
            else:
                logger.info(status_log)
            
        except Exception as e:
            logger.error(f"Error checking quality for table {table}: {str(e)}")
            quality_report["violations"].append({
                "table": table,
                "rule": "system_error",
                "error": str(e),
                "severity": "CRITICAL"
            })
    
    # Calculate overall score
    if tables_processed > 0:
        quality_report["overall_score"] = total_score / tables_processed
    
    # Determine compliance status
    high_severity_violations = [v for v in quality_report["violations"] if v.get("severity") == "HIGH"]
    critical_violations = [v for v in quality_report["violations"] if v.get("severity") == "CRITICAL"]
    
    if critical_violations:
        quality_report["compliance_status"] = "NON_COMPLIANT"
    elif high_severity_violations:
        quality_report["compliance_status"] = "DEGRADED"
    elif quality_report["violations"]:
        quality_report["compliance_status"] = "WARNING"
    
    logger.info(f"Data quality check completed: {quality_report['compliance_status']} status, overall score: {quality_report['overall_score']:.2f}")
    
    return quality_report


@op(
    description="Generate compliance audit report",
)
def generate_compliance_audit_report(
    context: OpExecutionContext,
    quality_report: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Generate federal compliance audit report
    """
    logger = get_dagster_logger()
    
    audit_report = {
        "audit_timestamp": datetime.now().isoformat(),
        "audit_period": {
            "start": (datetime.now() - timedelta(days=1)).isoformat(),
            "end": datetime.now().isoformat()
        },
        "compliance_frameworks": ["FedRAMP", "DORA", "FISMA"],
        "data_classification_compliance": {},
        "retention_compliance": {},
        "security_compliance": {},
        "audit_findings": [],
        "overall_compliance_status": quality_report.get("compliance_status", "UNKNOWN"),
        "recommendations": []
    }
    
    # Data classification compliance
    tables_checked = quality_report.get("tables_checked", [])
    audit_report["data_classification_compliance"] = {
        "tables_classified": len(tables_checked),
        "classification_coverage": 100.0,  # All tables have PUBLIC classification
        "sensitive_data_identified": False,
        "classification_status": "COMPLIANT"
    }
    
    # Retention compliance
    audit_report["retention_compliance"] = {
        "retention_policies_applied": True,
        "data_older_than_retention": 0,  # Simulated
        "automated_cleanup_active": True,
        "retention_status": "COMPLIANT"
    }
    
    # Security compliance
    audit_report["security_compliance"] = {
        "encryption_at_rest": True,
        "encryption_in_transit": True,
        "access_controls_configured": True,
        "audit_logging_enabled": True,
        "security_status": "COMPLIANT"
    }
    
    # Generate audit findings based on quality report
    violations = quality_report.get("violations", [])
    
    for violation in violations:
        severity_mapping = {
            "CRITICAL": "CRITICAL",
            "HIGH": "MAJOR", 
            "MEDIUM": "MINOR",
            "LOW": "OBSERVATION"
        }
        
        finding = {
            "finding_id": f"DQ_{hash(str(violation)) % 10000:04d}",
            "severity": severity_mapping.get(violation.get("severity", "LOW"), "OBSERVATION"),
            "category": "DATA_QUALITY",
            "description": f"Data quality violation in {violation.get('table', 'unknown')}: {violation.get('rule', 'unknown')}",
            "affected_system": violation.get("table", "unknown"),
            "compliance_impact": "Data integrity requirements not met",
            "remediation_required": violation.get("severity") in ["CRITICAL", "HIGH"]
        }
        
        audit_report["audit_findings"].append(finding)
    
    # Generate recommendations
    if audit_report["overall_compliance_status"] != "COMPLIANT":
        audit_report["recommendations"].extend([
            "Implement automated data quality monitoring",
            "Establish data quality SLAs with clear thresholds",
            "Create incident response procedures for data quality issues",
            "Enhance data validation at ingestion points"
        ])
    
    # Add standard federal compliance recommendations
    audit_report["recommendations"].extend([
        "Maintain current security controls and encryption standards",
        "Continue regular compliance monitoring and reporting",
        "Ensure all personnel complete required security training",
        "Review and update data classification policies annually"
    ])
    
    # Compliance score calculation
    compliance_components = [
        audit_report["data_classification_compliance"]["classification_status"] == "COMPLIANT",
        audit_report["retention_compliance"]["retention_status"] == "COMPLIANT", 
        audit_report["security_compliance"]["security_status"] == "COMPLIANT",
        quality_report.get("overall_score", 0) > 0.8
    ]
    
    compliance_score = sum(compliance_components) / len(compliance_components) * 100
    audit_report["compliance_score"] = compliance_score
    
    logger.info(f"Compliance audit completed: {len(audit_report['audit_findings'])} findings, {compliance_score:.1f}% compliance score")
    
    return audit_report


@op(
    description="Archive audit reports for federal compliance",
)
def archive_audit_reports(
    context: OpExecutionContext,
    audit_report: Dict[str, Any]
) -> bool:
    """
    Archive audit reports for federal record keeping requirements
    """
    logger = get_dagster_logger()
    
    try:
        # Simulate audit report archival
        archive_metadata = {
            "archive_timestamp": datetime.now().isoformat(),
            "audit_report_id": f"AUDIT_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
            "retention_period_years": 7,  # Federal requirement
            "archive_location": "/opt/dagster/storage/audit/compliance_reports/",
            "compliance_frameworks": audit_report.get("compliance_frameworks", []),
            "findings_count": len(audit_report.get("audit_findings", [])),
            "overall_status": audit_report.get("overall_compliance_status", "UNKNOWN")
        }
        
        logger.info(f"Archived audit report: {archive_metadata['audit_report_id']}")
        logger.info(f"Archive location: {archive_metadata['archive_location']}")
        logger.info(f"Retention period: {archive_metadata['retention_period_years']} years")
        
        # In production, this would actually store the report
        # starrocks.bulk_insert('audit_report_archive', [audit_report])
        
        return True
        
    except Exception as e:
        logger.error(f"Error archiving audit report: {str(e)}")
        return False


@job(
    description="Comprehensive compliance audit and reporting",
    config={
        "ops": {
            "run_data_quality_checks": {
                "config": {
                    "quality_rules": {
                        "completeness_threshold": 95.0,
                        "accuracy_threshold": 98.0,
                        "timeliness_threshold_hours": 6.0,
                        "consistency_threshold": 90.0
                    },
                    "tables_to_check": [
                        "fema_disaster_declarations",
                        "noaa_weather_alerts",
                        "coagmet_weather_data",
                        "usda_agricultural_data",
                        "processed_fema_disasters",
                        "unified_emergency_events"
                    ]
                }
            }
        }
    }
)
def compliance_audit_job():
    """
    Federal compliance audit job with comprehensive reporting
    """
    quality_report = run_data_quality_checks()
    audit_report = generate_compliance_audit_report(quality_report)
    archive_audit_reports(audit_report)


@op(
    description="Monitor data freshness across sources",
    config_schema={
        "freshness_thresholds": {
            "fema_hours": float,
            "noaa_hours": float,
            "coagmet_hours": float,
            "usda_hours": float,
        }
    }
)
def monitor_data_freshness(context: OpExecutionContext) -> Dict[str, Any]:
    """
    Monitor data freshness to ensure timely emergency information
    """
    logger = get_dagster_logger()
    
    thresholds = context.op_config["freshness_thresholds"]
    
    freshness_report = {
        "check_timestamp": datetime.now().isoformat(),
        "sources_checked": [],
        "freshness_status": {},
        "stale_sources": [],
        "alerts_triggered": [],
        "overall_freshness_status": "FRESH"
    }
    
    # Source-specific freshness checks
    sources_to_check = [
        {"name": "fema", "table": "fema_disaster_declarations", "threshold": thresholds["fema_hours"]},
        {"name": "noaa", "table": "noaa_weather_alerts", "threshold": thresholds["noaa_hours"]},
        {"name": "coagmet", "table": "coagmet_weather_data", "threshold": thresholds["coagmet_hours"]},
        {"name": "usda", "table": "usda_agricultural_data", "threshold": thresholds["usda_hours"]},
    ]
    
    for source in sources_to_check:
        try:
            # Simulate freshness check
            hours_since_update = max(0.1, hash(source["name"] + context.run.run_id) % 72) / 10  # 0.1-7.2 hours
            
            status = {
                "source": source["name"],
                "table": source["table"],
                "hours_since_update": hours_since_update,
                "threshold_hours": source["threshold"],
                "status": "FRESH" if hours_since_update <= source["threshold"] else "STALE"
            }
            
            freshness_report["sources_checked"].append(source["name"])
            freshness_report["freshness_status"][source["name"]] = status
            
            if status["status"] == "STALE":
                freshness_report["stale_sources"].append(source["name"])
                freshness_report["alerts_triggered"].append({
                    "source": source["name"],
                    "alert_type": "STALE_DATA",
                    "hours_overdue": hours_since_update - source["threshold"],
                    "severity": "HIGH" if hours_since_update > source["threshold"] * 2 else "MEDIUM"
                })
                
                logger.warning(f"Stale data detected: {source['name']} ({hours_since_update:.1f}h old, threshold: {source['threshold']}h)")
            else:
                logger.info(f"Fresh data: {source['name']} ({hours_since_update:.1f}h old)")
                
        except Exception as e:
            logger.error(f"Error checking freshness for {source['name']}: {str(e)}")
            freshness_report["alerts_triggered"].append({
                "source": source["name"],
                "alert_type": "CHECK_ERROR",
                "error": str(e),
                "severity": "CRITICAL"
            })
    
    # Overall freshness status
    if freshness_report["stale_sources"]:
        critical_stale = [alert for alert in freshness_report["alerts_triggered"] 
                         if alert.get("severity") == "CRITICAL"]
        freshness_report["overall_freshness_status"] = "CRITICAL" if critical_stale else "STALE"
    
    logger.info(f"Freshness check completed: {freshness_report['overall_freshness_status']} status, {len(freshness_report['stale_sources'])} stale sources")
    
    return freshness_report


@job(
    description="Monitor data freshness for emergency management",
    config={
        "ops": {
            "monitor_data_freshness": {
                "config": {
                    "freshness_thresholds": {
                        "fema_hours": 6.0,
                        "noaa_hours": 1.0,  # Weather alerts need to be very fresh
                        "coagmet_hours": 3.0,
                        "usda_hours": 24.0
                    }
                }
            }
        }
    }
)
def data_freshness_monitoring_job():
    """
    Monitor data freshness across all emergency management sources
    """
    monitor_data_freshness()