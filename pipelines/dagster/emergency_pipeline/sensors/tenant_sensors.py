"""
Tenant-Specific Sensors for Emergency Management Pipeline
Federal compliance monitoring and multi-tenant security
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
    name="federal_compliance_sensor",
    description="Monitor federal compliance requirements across all tenants",
    minimum_interval_seconds=3600,  # Check every hour
    default_status=DefaultSensorStatus.RUNNING,
)
def federal_compliance_sensor(context: SensorEvaluationContext):
    """
    Monitor federal compliance requirements including FedRAMP, DORA, FISMA
    Trigger compliance audits when violations are detected
    """
    logger = get_dagster_logger()
    
    # Simulate compliance monitoring across tenants
    tenant_compliance = {
        "colorado_state": {
            "classification_level": "INTERNAL",
            "compliance_frameworks": ["DORA", "StateCompliance"],
            "data_retention_compliant": True,
            "encryption_compliant": True,
            "audit_logging_compliant": True,
            "access_control_compliant": True,
            "last_audit": datetime.now() - timedelta(days=15),
            "violations": []
        },
        "federal_dhs": {
            "classification_level": "RESTRICTED", 
            "compliance_frameworks": ["FedRAMP", "DORA", "FISMA", "NIST"],
            "data_retention_compliant": True,
            "encryption_compliant": True,
            "audit_logging_compliant": True,
            "access_control_compliant": False,  # Simulated violation
            "last_audit": datetime.now() - timedelta(days=8),
            "violations": [
                {
                    "type": "access_control",
                    "severity": "high", 
                    "description": "Privileged user access detected without MFA",
                    "detected_at": datetime.now() - timedelta(hours=2),
                    "framework": "FedRAMP"
                }
            ]
        },
        "denver_city": {
            "classification_level": "INTERNAL",
            "compliance_frameworks": ["LocalGovCompliance"],
            "data_retention_compliant": True,
            "encryption_compliant": True,
            "audit_logging_compliant": True,
            "access_control_compliant": True,
            "last_audit": datetime.now() - timedelta(days=25),
            "violations": []
        }
    }
    
    compliance_violations = []
    critical_violations = []
    audit_overdue = []
    
    for tenant_id, compliance in tenant_compliance.items():
        # Check for active violations
        for violation in compliance.get("violations", []):
            violation_info = {
                "tenant_id": tenant_id,
                "classification_level": compliance["classification_level"],
                **violation
            }
            
            compliance_violations.append(violation_info)
            
            if violation["severity"] in ["high", "critical"]:
                critical_violations.append(violation_info)
        
        # Check for overdue audits (30 days for federal, 90 days for others)
        last_audit = compliance.get("last_audit")
        if last_audit:
            days_since_audit = (datetime.now() - last_audit).days
            
            # Federal tenants need more frequent audits
            is_federal = "federal" in tenant_id.lower() or "RESTRICTED" in compliance["classification_level"]
            audit_threshold = 30 if is_federal else 90
            
            if days_since_audit > audit_threshold:
                audit_overdue.append({
                    "tenant_id": tenant_id,
                    "days_overdue": days_since_audit - audit_threshold,
                    "classification_level": compliance["classification_level"],
                    "frameworks": compliance["compliance_frameworks"]
                })
    
    if critical_violations:
        logger.error(f"Critical compliance violations: {len(critical_violations)} violations across tenants")
        
        # Focus on the most severe violation
        most_critical = max(critical_violations, key=lambda x: 1 if x["severity"] == "critical" else 0.5)
        
        return RunRequest(
            run_key=f"compliance_violation_response_{context.cursor}",
            job_name="compliance_audit_job",
            run_config={
                "ops": {
                    "run_data_quality_checks": {
                        "config": {
                            "quality_rules": {
                                "completeness_threshold": 98.0,  # Higher for compliance
                                "accuracy_threshold": 99.0,
                                "timeliness_threshold_hours": 1.0,
                                "consistency_threshold": 95.0
                            },
                            "tables_to_check": [
                                f"tenant_{most_critical['tenant_id']}_audit_logs",
                                f"tenant_{most_critical['tenant_id']}_access_logs",
                                f"tenant_{most_critical['tenant_id']}_data_classification"
                            ]
                        }
                    }
                }
            },
            tags={
                "trigger": "compliance_violation",
                "severity": "critical",
                "tenant_id": most_critical["tenant_id"],
                "violation_type": most_critical["type"],
                "framework": most_critical["framework"]
            }
        )
    
    elif audit_overdue:
        federal_overdue = [a for a in audit_overdue if "RESTRICTED" in a["classification_level"]]
        
        if federal_overdue:
            logger.warning(f"Federal compliance audits overdue: {len(federal_overdue)} tenants")
            
            return RunRequest(
                run_key=f"overdue_audit_{context.cursor}",
                job_name="compliance_audit_job",
                tags={
                    "trigger": "overdue_audit",
                    "federal_tenants": str(len(federal_overdue)),
                    "most_overdue": str(max(a["days_overdue"] for a in federal_overdue))
                }
            )
        else:
            logger.info(f"Non-federal audits overdue: {len(audit_overdue)} tenants")
            return SkipReason("Non-federal audits overdue but not critical")
    
    elif compliance_violations:
        logger.warning(f"Compliance violations detected: {len(compliance_violations)} violations")
        return SkipReason("Minor compliance violations detected - monitoring")
    
    else:
        logger.info("All tenants are compliant with federal requirements")
        return SkipReason("All tenants are in compliance")


@sensor(
    name="tenant_isolation_sensor",
    description="Monitor tenant data isolation and prevent cross-tenant data leakage",
    minimum_interval_seconds=1800,  # Check every 30 minutes
    default_status=DefaultSensorStatus.RUNNING,
)
def tenant_isolation_sensor(context: SensorEvaluationContext):
    """
    Monitor tenant data isolation boundaries
    Detect and prevent cross-tenant data access violations
    """
    logger = get_dagster_logger()
    
    # Simulate tenant isolation monitoring
    isolation_checks = {
        "database_isolation": {
            "colorado_state": {
                "schema": "tenant_colorado",
                "unauthorized_access_attempts": 0,
                "cross_tenant_queries": 0,
                "data_leakage_risk": "low"
            },
            "federal_dhs": {
                "schema": "tenant_federal_dhs", 
                "unauthorized_access_attempts": 2,  # Simulated security event
                "cross_tenant_queries": 0,
                "data_leakage_risk": "medium"
            },
            "denver_city": {
                "schema": "tenant_denver",
                "unauthorized_access_attempts": 0,
                "cross_tenant_queries": 1,  # Simulated misconfiguration
                "data_leakage_risk": "low"
            }
        },
        "network_isolation": {
            "colorado_state": {
                "vlan_violations": 0,
                "firewall_blocks": 15,  # Normal security blocking
                "network_isolation_status": "secure"
            },
            "federal_dhs": {
                "vlan_violations": 0,
                "firewall_blocks": 45,
                "network_isolation_status": "secure"
            },
            "denver_city": {
                "vlan_violations": 1,  # Simulated network issue
                "firewall_blocks": 8,
                "network_isolation_status": "warning"
            }
        },
        "encryption_isolation": {
            "colorado_state": {
                "key_isolation_status": "secure",
                "encryption_violations": 0,
                "key_rotation_current": True
            },
            "federal_dhs": {
                "key_isolation_status": "secure", 
                "encryption_violations": 0,
                "key_rotation_current": True
            },
            "denver_city": {
                "key_isolation_status": "secure",
                "encryption_violations": 0,
                "key_rotation_current": False  # Simulated maintenance issue
            }
        }
    }
    
    isolation_violations = []
    critical_violations = []
    
    # Check database isolation
    for tenant_id, db_status in isolation_checks["database_isolation"].items():
        if db_status["unauthorized_access_attempts"] > 0:
            violation = {
                "tenant_id": tenant_id,
                "violation_type": "unauthorized_database_access",
                "severity": "high" if db_status["unauthorized_access_attempts"] > 1 else "medium",
                "details": f"{db_status['unauthorized_access_attempts']} unauthorized attempts",
                "isolation_layer": "database"
            }
            isolation_violations.append(violation)
            
            if violation["severity"] == "high":
                critical_violations.append(violation)
        
        if db_status["cross_tenant_queries"] > 0:
            violation = {
                "tenant_id": tenant_id,
                "violation_type": "cross_tenant_query",
                "severity": "medium",
                "details": f"{db_status['cross_tenant_queries']} cross-tenant queries detected",
                "isolation_layer": "database"
            }
            isolation_violations.append(violation)
    
    # Check network isolation
    for tenant_id, net_status in isolation_checks["network_isolation"].items():
        if net_status["vlan_violations"] > 0:
            violation = {
                "tenant_id": tenant_id,
                "violation_type": "network_isolation_breach",
                "severity": "high",
                "details": f"{net_status['vlan_violations']} VLAN violations",
                "isolation_layer": "network"
            }
            isolation_violations.append(violation)
            critical_violations.append(violation)
    
    # Check encryption isolation
    for tenant_id, enc_status in isolation_checks["encryption_isolation"].items():
        if enc_status["encryption_violations"] > 0:
            violation = {
                "tenant_id": tenant_id,
                "violation_type": "encryption_isolation_breach",
                "severity": "critical",
                "details": f"{enc_status['encryption_violations']} encryption violations",
                "isolation_layer": "encryption"
            }
            isolation_violations.append(violation)
            critical_violations.append(violation)
    
    if critical_violations:
        logger.error(f"Critical tenant isolation violations: {len(critical_violations)} violations")
        
        # Emergency response for isolation breaches
        most_critical = critical_violations[0]
        
        return RunRequest(
            run_key=f"isolation_breach_response_{context.cursor}",
            job_name="compliance_audit_job",
            run_config={
                "ops": {
                    "generate_compliance_audit_report": {
                        "config": {
                            "emergency_audit": True,
                            "focus_tenant": most_critical["tenant_id"],
                            "violation_type": most_critical["violation_type"]
                        }
                    }
                }
            },
            tags={
                "trigger": "isolation_breach",
                "severity": "critical",
                "tenant_id": most_critical["tenant_id"],
                "violation_type": most_critical["violation_type"],
                "isolation_layer": most_critical["isolation_layer"]
            }
        )
    
    elif isolation_violations:
        logger.warning(f"Tenant isolation violations: {len(isolation_violations)} violations")
        return SkipReason("Isolation violations detected but not critical")
    
    else:
        logger.info("All tenant isolation boundaries are secure")
        return SkipReason("Tenant isolation is functioning properly")


@sensor(
    name="security_audit_sensor",
    description="Monitor security events and audit trails across all tenants",
    minimum_interval_seconds=900,  # Check every 15 minutes  
    default_status=DefaultSensorStatus.RUNNING,
)
def security_audit_sensor(context: SensorEvaluationContext):
    """
    Monitor security events and audit trails
    Trigger security investigations when suspicious activity is detected
    """
    logger = get_dagster_logger()
    
    # Simulate security audit monitoring
    security_events = {
        "colorado_state": {
            "failed_login_attempts": 3,
            "privilege_escalation_attempts": 0,
            "suspicious_data_access": 0,
            "after_hours_access": 1,
            "unusual_query_patterns": 0,
            "security_score": 95
        },
        "federal_dhs": {
            "failed_login_attempts": 8,  # Higher activity expected
            "privilege_escalation_attempts": 1,  # Potential security event
            "suspicious_data_access": 2,  # Concerning pattern
            "after_hours_access": 15,  # Normal for 24/7 operations
            "unusual_query_patterns": 1,
            "security_score": 78  # Below threshold
        },
        "denver_city": {
            "failed_login_attempts": 1,
            "privilege_escalation_attempts": 0,
            "suspicious_data_access": 0,
            "after_hours_access": 0,
            "unusual_query_patterns": 0,
            "security_score": 98
        }
    }
    
    # Security thresholds
    security_thresholds = {
        "failed_login_threshold": 10,
        "privilege_escalation_threshold": 0,  # Zero tolerance
        "suspicious_access_threshold": 1,
        "security_score_threshold": 80,
        "unusual_pattern_threshold": 2
    }
    
    security_alerts = []
    critical_alerts = []
    
    for tenant_id, events in security_events.items():
        tenant_alerts = []
        
        # Check failed login attempts
        if events["failed_login_attempts"] > security_thresholds["failed_login_threshold"]:
            tenant_alerts.append({
                "type": "excessive_failed_logins",
                "severity": "medium",
                "count": events["failed_login_attempts"],
                "threshold": security_thresholds["failed_login_threshold"]
            })
        
        # Check privilege escalation (zero tolerance)
        if events["privilege_escalation_attempts"] > security_thresholds["privilege_escalation_threshold"]:
            tenant_alerts.append({
                "type": "privilege_escalation_attempt",
                "severity": "critical",
                "count": events["privilege_escalation_attempts"],
                "threshold": security_thresholds["privilege_escalation_threshold"]
            })
        
        # Check suspicious data access
        if events["suspicious_data_access"] > security_thresholds["suspicious_access_threshold"]:
            tenant_alerts.append({
                "type": "suspicious_data_access",
                "severity": "high",
                "count": events["suspicious_data_access"],
                "threshold": security_thresholds["suspicious_access_threshold"]
            })
        
        # Check unusual query patterns
        if events["unusual_query_patterns"] > security_thresholds["unusual_pattern_threshold"]:
            tenant_alerts.append({
                "type": "unusual_query_patterns",
                "severity": "medium",
                "count": events["unusual_query_patterns"],
                "threshold": security_thresholds["unusual_pattern_threshold"]
            })
        
        # Check overall security score
        if events["security_score"] < security_thresholds["security_score_threshold"]:
            tenant_alerts.append({
                "type": "low_security_score",
                "severity": "high" if events["security_score"] < 70 else "medium",
                "score": events["security_score"],
                "threshold": security_thresholds["security_score_threshold"]
            })
        
        if tenant_alerts:
            for alert in tenant_alerts:
                alert_info = {
                    "tenant_id": tenant_id,
                    **alert,
                    "timestamp": datetime.now().isoformat()
                }
                
                security_alerts.append(alert_info)
                
                if alert["severity"] == "critical":
                    critical_alerts.append(alert_info)
    
    if critical_alerts:
        logger.error(f"Critical security alerts: {len(critical_alerts)} alerts across tenants")
        
        # Focus on the most critical alert
        most_critical = critical_alerts[0]
        
        return RunRequest(
            run_key=f"security_incident_response_{context.cursor}",
            job_name="compliance_audit_job",
            run_config={
                "ops": {
                    "generate_compliance_audit_report": {
                        "config": {
                            "security_incident": True,
                            "incident_tenant": most_critical["tenant_id"],
                            "incident_type": most_critical["type"],
                            "severity": most_critical["severity"]
                        }
                    }
                }
            },
            tags={
                "trigger": "security_incident",
                "severity": "critical",
                "tenant_id": most_critical["tenant_id"],
                "incident_type": most_critical["type"],
                "alert_count": str(len(critical_alerts))
            }
        )
    
    elif security_alerts:
        high_severity_alerts = [a for a in security_alerts if a["severity"] == "high"]
        
        if high_severity_alerts:
            logger.warning(f"High severity security alerts: {len(high_severity_alerts)} alerts")
            return SkipReason("High severity security alerts detected - monitoring closely")
        else:
            logger.info(f"Medium severity security alerts: {len(security_alerts)} alerts")
            return SkipReason("Medium severity security alerts - routine monitoring")
    
    else:
        logger.info("No significant security events detected")
        return SkipReason("Security monitoring is normal across all tenants")


@sensor(
    name="data_classification_sensor",
    description="Monitor data classification compliance and prevent misclassification",
    minimum_interval_seconds=2700,  # Check every 45 minutes
    default_status=DefaultSensorStatus.RUNNING,
)
def data_classification_sensor(context: SensorEvaluationContext):
    """
    Monitor data classification compliance across all tenants
    Ensure data is properly classified and handled according to sensitivity
    """
    logger = get_dagster_logger()
    
    # Simulate data classification monitoring
    classification_status = {
        "colorado_state": {
            "total_records": 150000,
            "classification_distribution": {
                "PUBLIC": 120000,
                "INTERNAL": 30000,
                "RESTRICTED": 0,
                "CONFIDENTIAL": 0
            },
            "misclassified_records": 25,
            "unclassified_records": 15,
            "classification_accuracy": 99.97
        },
        "federal_dhs": {
            "total_records": 500000,
            "classification_distribution": {
                "PUBLIC": 200000,
                "INTERNAL": 150000,
                "RESTRICTED": 100000,
                "CONFIDENTIAL": 50000
            },
            "misclassified_records": 150,  # Higher due to complexity
            "unclassified_records": 5,
            "classification_accuracy": 99.97
        },
        "denver_city": {
            "total_records": 75000,
            "classification_distribution": {
                "PUBLIC": 65000,
                "INTERNAL": 10000,
                "RESTRICTED": 0,
                "CONFIDENTIAL": 0
            },
            "misclassified_records": 8,
            "unclassified_records": 2,
            "classification_accuracy": 99.99
        }
    }
    
    # Classification compliance thresholds
    compliance_thresholds = {
        "min_classification_accuracy": 99.9,
        "max_misclassified_percent": 0.1,
        "max_unclassified_percent": 0.05
    }
    
    classification_violations = []
    critical_violations = []
    
    for tenant_id, status in classification_status.items():
        tenant_violations = []
        
        # Check classification accuracy
        if status["classification_accuracy"] < compliance_thresholds["min_classification_accuracy"]:
            tenant_violations.append({
                "type": "low_classification_accuracy",
                "severity": "high",
                "current_accuracy": status["classification_accuracy"],
                "required_accuracy": compliance_thresholds["min_classification_accuracy"]
            })
        
        # Check misclassification rate
        misclassified_percent = (status["misclassified_records"] / status["total_records"]) * 100
        if misclassified_percent > compliance_thresholds["max_misclassified_percent"]:
            tenant_violations.append({
                "type": "high_misclassification_rate",
                "severity": "medium",
                "misclassified_count": status["misclassified_records"],
                "misclassified_percent": misclassified_percent
            })
        
        # Check unclassified data
        unclassified_percent = (status["unclassified_records"] / status["total_records"]) * 100
        if unclassified_percent > compliance_thresholds["max_unclassified_percent"]:
            tenant_violations.append({
                "type": "unclassified_data_present",
                "severity": "high",
                "unclassified_count": status["unclassified_records"],
                "unclassified_percent": unclassified_percent
            })
        
        # Special checks for federal tenants (higher scrutiny)
        if "federal" in tenant_id.lower():
            # Ensure proper distribution for federal data
            restricted_count = status["classification_distribution"].get("RESTRICTED", 0)
            confidential_count = status["classification_distribution"].get("CONFIDENTIAL", 0)
            
            if restricted_count == 0 and confidential_count == 0:
                tenant_violations.append({
                    "type": "missing_classified_data",
                    "severity": "medium",
                    "message": "Federal tenant has no RESTRICTED or CONFIDENTIAL data - verify classification"
                })
        
        if tenant_violations:
            for violation in tenant_violations:
                violation_info = {
                    "tenant_id": tenant_id,
                    **violation,
                    "tenant_type": "federal" if "federal" in tenant_id.lower() else "state_local"
                }
                
                classification_violations.append(violation_info)
                
                if violation["severity"] == "high":
                    critical_violations.append(violation_info)
    
    if critical_violations:
        logger.error(f"Critical data classification violations: {len(critical_violations)} violations")
        
        # Focus on the most critical violation
        most_critical = critical_violations[0]
        
        return RunRequest(
            run_key=f"classification_remediation_{context.cursor}",
            job_name="compliance_audit_job",
            run_config={
                "ops": {
                    "run_data_quality_checks": {
                        "config": {
                            "quality_rules": {
                                "completeness_threshold": 99.5,
                                "accuracy_threshold": 99.9,  # Higher for classification issues
                                "timeliness_threshold_hours": 2.0,
                                "consistency_threshold": 99.0
                            },
                            "tables_to_check": [
                                f"tenant_{most_critical['tenant_id']}_data_classification",
                                f"tenant_{most_critical['tenant_id']}_metadata"
                            ]
                        }
                    }
                }
            },
            tags={
                "trigger": "classification_violation",
                "severity": "critical",
                "tenant_id": most_critical["tenant_id"],
                "violation_type": most_critical["type"],
                "tenant_type": most_critical["tenant_type"]
            }
        )
    
    elif classification_violations:
        logger.warning(f"Data classification violations: {len(classification_violations)} violations")
        return SkipReason("Classification violations detected but not critical")
    
    else:
        logger.info("Data classification compliance is satisfactory across all tenants")
        return SkipReason("All tenants have proper data classification")


@sensor(
    name="tenant_resource_utilization_sensor",
    description="Monitor resource utilization across tenants and detect quota violations",
    minimum_interval_seconds=600,  # Check every 10 minutes
    default_status=DefaultSensorStatus.RUNNING,
)
def tenant_resource_utilization_sensor(context: SensorEvaluationContext):
    """
    Monitor resource utilization per tenant
    Ensure fair resource allocation and detect quota violations
    """
    logger = get_dagster_logger()
    
    # Simulate tenant resource monitoring
    tenant_resources = {
        "colorado_state": {
            "cpu_usage_percent": 35.2,
            "memory_usage_gb": 8.5,
            "storage_usage_gb": 150.2,
            "network_bandwidth_mbps": 25.8,
            "api_calls_per_hour": 450,
            "quota_limits": {
                "cpu_percent": 50,
                "memory_gb": 16,
                "storage_gb": 500,
                "bandwidth_mbps": 100,
                "api_calls_hour": 1000
            }
        },
        "federal_dhs": {
            "cpu_usage_percent": 78.9,
            "memory_usage_gb": 28.2,
            "storage_usage_gb": 850.7,
            "network_bandwidth_mbps": 145.3,
            "api_calls_per_hour": 3500,
            "quota_limits": {
                "cpu_percent": 80,
                "memory_gb": 32,
                "storage_gb": 1000,
                "bandwidth_mbps": 200,
                "api_calls_hour": 5000
            }
        },
        "denver_city": {
            "cpu_usage_percent": 15.8,
            "memory_usage_gb": 4.2,
            "storage_usage_gb": 45.8,
            "network_bandwidth_mbps": 8.9,
            "api_calls_per_hour": 125,
            "quota_limits": {
                "cpu_percent": 30,
                "memory_gb": 8,
                "storage_gb": 200,
                "bandwidth_mbps": 50,
                "api_calls_hour": 500
            }
        }
    }
    
    quota_violations = []
    approaching_limits = []
    
    for tenant_id, resources in tenant_resources.items():
        limits = resources["quota_limits"]
        
        # Check CPU usage
        cpu_usage_percent = (resources["cpu_usage_percent"] / limits["cpu_percent"]) * 100
        if cpu_usage_percent > 100:
            quota_violations.append({
                "tenant_id": tenant_id,
                "resource": "cpu",
                "usage": resources["cpu_usage_percent"],
                "limit": limits["cpu_percent"],
                "usage_percent": cpu_usage_percent,
                "severity": "high"
            })
        elif cpu_usage_percent > 80:
            approaching_limits.append({
                "tenant_id": tenant_id,
                "resource": "cpu",
                "usage": resources["cpu_usage_percent"],
                "limit": limits["cpu_percent"],
                "usage_percent": cpu_usage_percent
            })
        
        # Check memory usage
        memory_usage_percent = (resources["memory_usage_gb"] / limits["memory_gb"]) * 100
        if memory_usage_percent > 100:
            quota_violations.append({
                "tenant_id": tenant_id,
                "resource": "memory",
                "usage": resources["memory_usage_gb"],
                "limit": limits["memory_gb"],
                "usage_percent": memory_usage_percent,
                "severity": "critical"  # Memory is more critical
            })
        elif memory_usage_percent > 85:
            approaching_limits.append({
                "tenant_id": tenant_id,
                "resource": "memory",
                "usage": resources["memory_usage_gb"],
                "limit": limits["memory_gb"],
                "usage_percent": memory_usage_percent
            })
        
        # Check storage usage
        storage_usage_percent = (resources["storage_usage_gb"] / limits["storage_gb"]) * 100
        if storage_usage_percent > 100:
            quota_violations.append({
                "tenant_id": tenant_id,
                "resource": "storage",
                "usage": resources["storage_usage_gb"],
                "limit": limits["storage_gb"],
                "usage_percent": storage_usage_percent,
                "severity": "high"
            })
        elif storage_usage_percent > 90:
            approaching_limits.append({
                "tenant_id": tenant_id,
                "resource": "storage",
                "usage": resources["storage_usage_gb"],
                "limit": limits["storage_gb"],
                "usage_percent": storage_usage_percent
            })
        
        # Check API rate limits
        api_usage_percent = (resources["api_calls_per_hour"] / limits["api_calls_hour"]) * 100
        if api_usage_percent > 100:
            quota_violations.append({
                "tenant_id": tenant_id,
                "resource": "api_calls",
                "usage": resources["api_calls_per_hour"],
                "limit": limits["api_calls_hour"],
                "usage_percent": api_usage_percent,
                "severity": "medium"
            })
    
    if quota_violations:
        critical_violations = [v for v in quota_violations if v["severity"] == "critical"]
        
        if critical_violations:
            logger.error(f"Critical quota violations: {len(critical_violations)} violations")
            
            most_critical = critical_violations[0]
            
            return RunRequest(
                run_key=f"quota_violation_response_{context.cursor}",
                job_name="database_optimization_job",
                tags={
                    "trigger": "quota_violation",
                    "severity": "critical",
                    "tenant_id": most_critical["tenant_id"],
                    "resource": most_critical["resource"],
                    "usage_percent": str(int(most_critical["usage_percent"]))
                }
            )
        else:
            logger.warning(f"Quota violations: {len(quota_violations)} violations")
            return SkipReason("Quota violations detected but not critical")
    
    elif approaching_limits:
        logger.info(f"Tenants approaching resource limits: {len(approaching_limits)} instances")
        return SkipReason("Some tenants approaching limits - monitoring")
    
    else:
        logger.info("All tenants are within resource quotas")
        return SkipReason("Resource utilization is within limits for all tenants")


@sensor(
    name="cross_tenant_activity_sensor",
    description="Monitor and detect any cross-tenant activity or data access patterns",
    minimum_interval_seconds=1200,  # Check every 20 minutes
    default_status=DefaultSensorStatus.RUNNING,
)
def cross_tenant_activity_sensor(context: SensorEvaluationContext):
    """
    Monitor for cross-tenant activity patterns that could indicate security issues
    Ensure tenant boundaries are strictly maintained
    """
    logger = get_dagster_logger()
    
    # Simulate cross-tenant activity monitoring
    activity_analysis = {
        "query_analysis": {
            "total_queries_analyzed": 5000,
            "cross_tenant_attempts": 3,  # Potential violations
            "blocked_cross_tenant": 2,
            "successful_cross_tenant": 1  # Security concern
        },
        "user_activity": {
            "users_with_multi_tenant_access": 2,  # Admin users
            "unauthorized_tenant_access_attempts": 1,
            "privilege_boundary_violations": 0
        },
        "data_flow_analysis": {
            "inter_tenant_data_transfers": 0,
            "shared_resource_access": 5,  # Shared public resources
            "isolation_boundary_tests": 100,
            "boundary_test_failures": 1
        },
        "api_access_patterns": {
            "cross_tenant_api_calls": 0,
            "tenant_impersonation_attempts": 0,
            "suspicious_access_patterns": 2
        }
    }
    
    security_concerns = []
    critical_concerns = []
    
    # Analyze query patterns
    if activity_analysis["query_analysis"]["successful_cross_tenant"] > 0:
        security_concerns.append({
            "type": "successful_cross_tenant_query",
            "severity": "critical",
            "count": activity_analysis["query_analysis"]["successful_cross_tenant"],
            "category": "data_access_violation"
        })
    
    # Analyze user activity
    if activity_analysis["user_activity"]["unauthorized_tenant_access_attempts"] > 0:
        security_concerns.append({
            "type": "unauthorized_tenant_access",
            "severity": "high",
            "count": activity_analysis["user_activity"]["unauthorized_tenant_access_attempts"],
            "category": "access_control_violation"
        })
    
    # Analyze data flow
    if activity_analysis["data_flow_analysis"]["inter_tenant_data_transfers"] > 0:
        security_concerns.append({
            "type": "inter_tenant_data_transfer",
            "severity": "critical",
            "count": activity_analysis["data_flow_analysis"]["inter_tenant_data_transfers"],
            "category": "data_isolation_violation"
        })
    
    if activity_analysis["data_flow_analysis"]["boundary_test_failures"] > 0:
        security_concerns.append({
            "type": "isolation_boundary_failure",
            "severity": "high",
            "count": activity_analysis["data_flow_analysis"]["boundary_test_failures"],
            "category": "isolation_integrity"
        })
    
    # Analyze API patterns
    if activity_analysis["api_access_patterns"]["tenant_impersonation_attempts"] > 0:
        security_concerns.append({
            "type": "tenant_impersonation",
            "severity": "critical",
            "count": activity_analysis["api_access_patterns"]["tenant_impersonation_attempts"],
            "category": "identity_violation"
        })
    
    # Categorize by severity
    for concern in security_concerns:
        if concern["severity"] == "critical":
            critical_concerns.append(concern)
    
    if critical_concerns:
        logger.error(f"Critical cross-tenant security concerns: {len(critical_concerns)} violations")
        
        most_critical = critical_concerns[0]
        
        return RunRequest(
            run_key=f"cross_tenant_security_response_{context.cursor}",
            job_name="compliance_audit_job",
            run_config={
                "ops": {
                    "generate_compliance_audit_report": {
                        "config": {
                            "emergency_audit": True,
                            "cross_tenant_violation": True,
                            "violation_category": most_critical["category"],
                            "violation_type": most_critical["type"]
                        }
                    }
                }
            },
            tags={
                "trigger": "cross_tenant_violation",
                "severity": "critical",
                "violation_type": most_critical["type"],
                "category": most_critical["category"],
                "violation_count": str(most_critical["count"])
            }
        )
    
    elif security_concerns:
        logger.warning(f"Cross-tenant security concerns: {len(security_concerns)} issues")
        return SkipReason("Cross-tenant security concerns detected but not critical")
    
    else:
        logger.info("No cross-tenant activity violations detected")
        return SkipReason("Cross-tenant isolation is functioning properly")


@sensor(
    name="encryption_key_rotation_sensor",
    description="Monitor encryption key rotation schedules and compliance",
    minimum_interval_seconds=7200,  # Check every 2 hours
    default_status=DefaultSensorStatus.RUNNING,
)
def encryption_key_rotation_sensor(context: SensorEvaluationContext):
    """
    Monitor encryption key rotation schedules across all tenants
    Ensure keys are rotated according to federal compliance requirements
    """
    logger = get_dagster_logger()
    
    # Simulate encryption key monitoring
    key_rotation_status = {
        "colorado_state": {
            "database_key": {
                "last_rotation": datetime.now() - timedelta(days=85),
                "rotation_schedule_days": 90,
                "status": "current",
                "next_rotation": datetime.now() + timedelta(days=5)
            },
            "kafka_key": {
                "last_rotation": datetime.now() - timedelta(days=25),
                "rotation_schedule_days": 30,
                "status": "current",
                "next_rotation": datetime.now() + timedelta(days=5)
            },
            "backup_key": {
                "last_rotation": datetime.now() - timedelta(days=180),
                "rotation_schedule_days": 365,
                "status": "current",
                "next_rotation": datetime.now() + timedelta(days=185)
            }
        },
        "federal_dhs": {
            "database_key": {
                "last_rotation": datetime.now() - timedelta(days=32),
                "rotation_schedule_days": 30,  # Stricter for federal
                "status": "overdue",
                "next_rotation": datetime.now() - timedelta(days=2)
            },
            "kafka_key": {
                "last_rotation": datetime.now() - timedelta(days=8),
                "rotation_schedule_days": 7,   # Very strict for federal
                "status": "current",
                "next_rotation": datetime.now() - timedelta(days=1)
            },
            "backup_key": {
                "last_rotation": datetime.now() - timedelta(days=95),
                "rotation_schedule_days": 90,  # Quarterly for federal
                "status": "overdue",
                "next_rotation": datetime.now() - timedelta(days=5)
            }
        },
        "denver_city": {
            "database_key": {
                "last_rotation": datetime.now() - timedelta(days=45),
                "rotation_schedule_days": 90,
                "status": "current",
                "next_rotation": datetime.now() + timedelta(days=45)
            },
            "kafka_key": {
                "last_rotation": datetime.now() - timedelta(days=15),
                "rotation_schedule_days": 30,
                "status": "current",
                "next_rotation": datetime.now() + timedelta(days=15)
            },
            "backup_key": {
                "last_rotation": datetime.now() - timedelta(days=300),
                "rotation_schedule_days": 365,
                "status": "current",
                "next_rotation": datetime.now() + timedelta(days=65)
            }
        }
    }
    
    overdue_rotations = []
    upcoming_rotations = []
    
    for tenant_id, keys in key_rotation_status.items():
        for key_type, key_info in keys.items():
            if key_info["status"] == "overdue":
                days_overdue = (datetime.now() - key_info["next_rotation"]).days
                overdue_rotations.append({
                    "tenant_id": tenant_id,
                    "key_type": key_type,
                    "days_overdue": days_overdue,
                    "severity": "critical" if days_overdue > 7 else "high",
                    "is_federal": "federal" in tenant_id.lower()
                })
            
            # Check for upcoming rotations (within 7 days)
            if key_info["next_rotation"] <= datetime.now() + timedelta(days=7):
                upcoming_rotations.append({
                    "tenant_id": tenant_id,
                    "key_type": key_type,
                    "days_until_rotation": (key_info["next_rotation"] - datetime.now()).days
                })
    
    if overdue_rotations:
        critical_overdue = [r for r in overdue_rotations if r["severity"] == "critical"]
        federal_overdue = [r for r in overdue_rotations if r["is_federal"]]
        
        if critical_overdue or federal_overdue:
            logger.error(f"Critical key rotation overdue: {len(critical_overdue)} critical, {len(federal_overdue)} federal")
            
            most_critical = critical_overdue[0] if critical_overdue else federal_overdue[0]
            
            return RunRequest(
                run_key=f"key_rotation_emergency_{context.cursor}",
                job_name="compliance_audit_job",
                run_config={
                    "ops": {
                        "generate_compliance_audit_report": {
                            "config": {
                                "key_rotation_emergency": True,
                                "affected_tenant": most_critical["tenant_id"],
                                "overdue_keys": [r["key_type"] for r in overdue_rotations if r["tenant_id"] == most_critical["tenant_id"]]
                            }
                        }
                    }
                },
                tags={
                    "trigger": "key_rotation_overdue",
                    "severity": "critical",
                    "tenant_id": most_critical["tenant_id"],
                    "key_type": most_critical["key_type"],
                    "days_overdue": str(most_critical["days_overdue"])
                }
            )
        else:
            logger.warning(f"Key rotations overdue: {len(overdue_rotations)} keys")
            return SkipReason("Key rotations overdue but not critical")
    
    elif upcoming_rotations:
        logger.info(f"Upcoming key rotations: {len(upcoming_rotations)} keys need rotation soon")
        return SkipReason("Key rotations scheduled - monitoring")
    
    else:
        logger.info("All encryption keys are current with rotation schedules")
        return SkipReason("Encryption key rotation schedules are compliant")


@sensor(
    name="federal_audit_deadline_sensor",
    description="Monitor federal audit deadlines and compliance reporting requirements",
    minimum_interval_seconds=21600,  # Check every 6 hours
    default_status=DefaultSensorStatus.RUNNING,
)
def federal_audit_deadline_sensor(context: SensorEvaluationContext):
    """
    Monitor federal audit deadlines and compliance reporting requirements
    Ensure timely completion of mandatory federal audits
    """
    logger = get_dagster_logger()
    
    # Simulate federal audit tracking
    audit_schedule = {
        "colorado_state": {
            "state_compliance_audit": {
                "due_date": datetime.now() + timedelta(days=45),
                "frequency": "annual",
                "status": "in_progress",
                "completion_percent": 65,
                "frameworks": ["DORA", "StateCompliance"]
            },
            "dora_assessment": {
                "due_date": datetime.now() + timedelta(days=120),
                "frequency": "annual",
                "status": "not_started",
                "completion_percent": 0,
                "frameworks": ["DORA"]
            }
        },
        "federal_dhs": {
            "fedramp_assessment": {
                "due_date": datetime.now() + timedelta(days=15),  # Urgent
                "frequency": "annual",
                "status": "in_progress",
                "completion_percent": 40,
                "frameworks": ["FedRAMP"]
            },
            "fisma_audit": {
                "due_date": datetime.now() + timedelta(days=8),   # Very urgent
                "frequency": "quarterly",
                "status": "not_started",
                "completion_percent": 0,
                "frameworks": ["FISMA", "NIST"]
            },
            "dora_assessment": {
                "due_date": datetime.now() + timedelta(days=90),
                "frequency": "annual", 
                "status": "in_progress",
                "completion_percent": 75,
                "frameworks": ["DORA"]
            },
            "continuous_monitoring": {
                "due_date": datetime.now() + timedelta(days=1),   # Daily
                "frequency": "daily",
                "status": "current",
                "completion_percent": 100,
                "frameworks": ["FedRAMP", "FISMA"]
            }
        },
        "denver_city": {
            "local_compliance_audit": {
                "due_date": datetime.now() + timedelta(days=180),
                "frequency": "annual",
                "status": "not_started",
                "completion_percent": 0,
                "frameworks": ["LocalGovCompliance"]
            }
        }
    }
    
    urgent_audits = []
    overdue_audits = []
    approaching_deadlines = []
    
    for tenant_id, audits in audit_schedule.items():
        for audit_name, audit_info in audits.items():
            days_until_due = (audit_info["due_date"] - datetime.now()).days
            
            if days_until_due < 0:
                # Overdue audit
                overdue_audits.append({
                    "tenant_id": tenant_id,
                    "audit_name": audit_name,
                    "days_overdue": abs(days_until_due),
                    "frameworks": audit_info["frameworks"],
                    "completion_percent": audit_info["completion_percent"],
                    "severity": "critical"
                })
            
            elif days_until_due <= 7:
                # Urgent - due within a week
                urgent_audits.append({
                    "tenant_id": tenant_id,
                    "audit_name": audit_name,
                    "days_until_due": days_until_due,
                    "frameworks": audit_info["frameworks"],
                    "completion_percent": audit_info["completion_percent"],
                    "severity": "high" if days_until_due <= 3 else "medium"
                })
            
            elif days_until_due <= 30:
                # Approaching deadline
                approaching_deadlines.append({
                    "tenant_id": tenant_id,
                    "audit_name": audit_name,
                    "days_until_due": days_until_due,
                    "frameworks": audit_info["frameworks"],
                    "completion_percent": audit_info["completion_percent"]
                })
    
    if overdue_audits:
        logger.error(f"Overdue federal audits: {len(overdue_audits)} audits past deadline")
        
        # Focus on the most overdue audit
        most_overdue = max(overdue_audits, key=lambda x: x["days_overdue"])
        
        return RunRequest(
            run_key=f"overdue_audit_emergency_{context.cursor}",
            job_name="compliance_audit_job",
            run_config={
                "ops": {
                    "generate_compliance_audit_report": {
                        "config": {
                            "emergency_audit": True,
                            "overdue_audit": True,
                            "audit_name": most_overdue["audit_name"],
                            "tenant_id": most_overdue["tenant_id"],
                            "frameworks": most_overdue["frameworks"]
                        }
                    }
                }
            },
            tags={
                "trigger": "audit_overdue",
                "severity": "critical",
                "tenant_id": most_overdue["tenant_id"],
                "audit_name": most_overdue["audit_name"],
                "days_overdue": str(most_overdue["days_overdue"])
            }
        )
    
    elif urgent_audits:
        critical_urgent = [a for a in urgent_audits if a["severity"] == "high"]
        federal_urgent = [a for a in urgent_audits if "federal" in a["tenant_id"].lower()]
        
        if critical_urgent or federal_urgent:
            logger.warning(f"Urgent audit deadlines: {len(urgent_audits)} audits due within 7 days")
            
            most_urgent = critical_urgent[0] if critical_urgent else federal_urgent[0] if federal_urgent else urgent_audits[0]
            
            return RunRequest(
                run_key=f"urgent_audit_prep_{context.cursor}",
                job_name="compliance_audit_job",
                tags={
                    "trigger": "audit_deadline_urgent",
                    "severity": "high",
                    "tenant_id": most_urgent["tenant_id"],
                    "audit_name": most_urgent["audit_name"],
                    "days_until_due": str(most_urgent["days_until_due"])
                }
            )
        else:
            logger.info(f"Upcoming audit deadlines: {len(urgent_audits)} audits due soon")
            return SkipReason("Audit deadlines approaching but not urgent")
    
    elif approaching_deadlines:
        federal_approaching = [a for a in approaching_deadlines if "federal" in a["tenant_id"].lower()]
        if federal_approaching:
            logger.info(f"Federal audits approaching: {len(federal_approaching)} within 30 days")
        return SkipReason("Audit deadlines approaching - monitoring")
    
    else:
        logger.info("All federal audit deadlines are manageable")
        return SkipReason("Federal audit schedules are on track")


@sensor(
    name="pii_detection_sensor",
    description="Monitor for PII data in systems that should not contain it",
    minimum_interval_seconds=3600,  # Check every hour
    default_status=DefaultSensorStatus.RUNNING,
)
def pii_detection_sensor(context: SensorEvaluationContext):
    """
    Monitor for Personally Identifiable Information (PII) in emergency data
    Ensure PII is properly handled according to federal privacy requirements
    """
    logger = get_dagster_logger()
    
    # Simulate PII detection across tenants
    pii_scan_results = {
        "colorado_state": {
            "total_records_scanned": 150000,
            "pii_detected": {
                "social_security_numbers": 0,
                "phone_numbers": 25,      # Some legitimate emergency contacts
                "email_addresses": 150,   # Contact information
                "physical_addresses": 300, # Incident locations (may be OK)
                "names": 50,              # Official names in reports
                "credit_card_numbers": 0,
                "driver_license_numbers": 0
            },
            "false_positives": 15,
            "classification_violations": 2,  # PII in PUBLIC classified data
            "last_scan": datetime.now() - timedelta(hours=2)
        },
        "federal_dhs": {
            "total_records_scanned": 500000,
            "pii_detected": {
                "social_security_numbers": 0,  # Should be zero even for federal
                "phone_numbers": 500,           # More contacts for federal ops
                "email_addresses": 1200,
                "physical_addresses": 800,      # More detailed location data
                "names": 300,                   # Personnel and contact names
                "credit_card_numbers": 0,
                "driver_license_numbers": 0
            },
            "false_positives": 45,
            "classification_violations": 0,    # Federal should have better controls
            "last_scan": datetime.now() - timedelta(hours=1)
        },
        "denver_city": {
            "total_records_scanned": 75000,
            "pii_detected": {
                "social_security_numbers": 1,  # Potential violation
                "phone_numbers": 80,
                "email_addresses": 120,
                "physical_addresses": 200,
                "names": 30,
                "credit_card_numbers": 0,
                "driver_license_numbers": 0
            },
            "false_positives": 8,
            "classification_violations": 1,    # SSN in wrong classification
            "last_scan": datetime.now() - timedelta(hours=3)
        }
    }
    
    # PII violation thresholds
    pii_thresholds = {
        "ssn_tolerance": 0,           # Zero tolerance for SSNs
        "credit_card_tolerance": 0,   # Zero tolerance for credit cards
        "license_tolerance": 0,       # Zero tolerance for driver licenses
        "classification_violations": 0  # Zero tolerance for classification errors
    }
    
    pii_violations = []
    critical_violations = []
    
    for tenant_id, scan_result in pii_scan_results.items():
        pii_detected = scan_result["pii_detected"]
        
        # Check for critical PII types (zero tolerance)
        if pii_detected["social_security_numbers"] > pii_thresholds["ssn_tolerance"]:
            pii_violations.append({
                "tenant_id": tenant_id,
                "violation_type": "ssn_detected",
                "count": pii_detected["social_security_numbers"],
                "severity": "critical",
                "data_type": "social_security_numbers"
            })
        
        if pii_detected["credit_card_numbers"] > pii_thresholds["credit_card_tolerance"]:
            pii_violations.append({
                "tenant_id": tenant_id,
                "violation_type": "credit_card_detected",
                "count": pii_detected["credit_card_numbers"],
                "severity": "critical",
                "data_type": "credit_card_numbers"
            })
        
        if pii_detected["driver_license_numbers"] > pii_thresholds["license_tolerance"]:
            pii_violations.append({
                "tenant_id": tenant_id,
                "violation_type": "license_detected",
                "count": pii_detected["driver_license_numbers"],
                "severity": "critical",
                "data_type": "driver_license_numbers"
            })
        
        # Check for classification violations
        if scan_result["classification_violations"] > pii_thresholds["classification_violations"]:
            pii_violations.append({
                "tenant_id": tenant_id,
                "violation_type": "pii_classification_violation",
                "count": scan_result["classification_violations"],
                "severity": "high",
                "data_type": "classification_error"
            })
        
        # Categorize critical violations
        for violation in pii_violations:
            if violation["tenant_id"] == tenant_id and violation["severity"] == "critical":
                critical_violations.append(violation)
    
    if critical_violations:
        logger.error(f"Critical PII violations detected: {len(critical_violations)} violations")
        
        # Focus on the most severe violation
        most_critical = critical_violations[0]
        
        return RunRequest(
            run_key=f"pii_violation_response_{context.cursor}",
            job_name="compliance_audit_job",
            run_config={
                "ops": {
                    "generate_compliance_audit_report": {
                        "config": {
                            "pii_violation": True,
                            "violation_tenant": most_critical["tenant_id"],
                            "violation_type": most_critical["violation_type"],
                            "data_type": most_critical["data_type"],
                            "violation_count": most_critical["count"]
                        }
                    }
                }
            },
            tags={
                "trigger": "pii_violation",
                "severity": "critical",
                "tenant_id": most_critical["tenant_id"],
                "violation_type": most_critical["violation_type"],
                "data_type": most_critical["data_type"]
            }
        )
    
    elif pii_violations:
        logger.warning(f"PII violations detected: {len(pii_violations)} violations")
        return SkipReason("PII violations detected but not critical")
    
    else:
        logger.info("No significant PII violations detected")
        return SkipReason("PII scanning results are within acceptable parameters")


# Export all tenant sensors for use in definitions
tenant_sensors = [
    federal_compliance_sensor,
    tenant_isolation_sensor,
    security_audit_sensor,
    data_classification_sensor,
    tenant_resource_utilization_sensor,
    cross_tenant_activity_sensor,
    encryption_key_rotation_sensor,
    federal_audit_deadline_sensor,
    pii_detection_sensor,
]