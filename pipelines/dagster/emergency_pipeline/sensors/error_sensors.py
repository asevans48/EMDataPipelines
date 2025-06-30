"""
Error Monitoring Sensors for Emergency Management Pipeline
Monitor system health, errors, and performance degradation
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
    name="error_monitoring_sensor",
    description="Monitor error rates and failure patterns across the pipeline",
    minimum_interval_seconds=300,  # Check every 5 minutes
    default_status=DefaultSensorStatus.RUNNING,
)
def error_monitoring_sensor(context: SensorEvaluationContext):
    """
    Monitor error rates across all pipeline components
    Trigger remediation when error thresholds are exceeded
    """
    logger = get_dagster_logger()
    
    # Simulate error monitoring (in production, query actual error logs/metrics)
    error_metrics = {
        "data_ingestion": {
            "total_operations": 1000,
            "errors": 15,
            "error_rate": 1.5,
            "error_types": {
                "connection_timeout": 8,
                "api_rate_limit": 4,
                "data_format_error": 3
            }
        },
        "data_processing": {
            "total_operations": 800,
            "errors": 45,
            "error_rate": 5.6,
            "error_types": {
                "transformation_error": 25,
                "validation_failure": 12,
                "memory_error": 8
            }
        },
        "database_operations": {
            "total_operations": 2000,
            "errors": 8,
            "error_rate": 0.4,
            "error_types": {
                "connection_error": 3,
                "query_timeout": 3,
                "deadlock": 2
            }
        },
        "ml_pipeline": {
            "total_operations": 50,
            "errors": 2,
            "error_rate": 4.0,
            "error_types": {
                "model_training_failure": 1,
                "feature_extraction_error": 1
            }
        }
    }
    
    # Error thresholds
    error_thresholds = {
        "warning": 5.0,   # 5% error rate
        "critical": 10.0  # 10% error rate
    }
    
    high_error_components = []
    critical_error_components = []
    
    for component, metrics in error_metrics.items():
        error_rate = metrics["error_rate"]
        
        if error_rate > error_thresholds["critical"]:
            critical_error_components.append({
                "component": component,
                "error_rate": error_rate,
                "total_errors": metrics["errors"],
                "error_types": metrics["error_types"],
                "severity": "critical"
            })
        elif error_rate > error_thresholds["warning"]:
            high_error_components.append({
                "component": component,
                "error_rate": error_rate,
                "total_errors": metrics["errors"],
                "error_types": metrics["error_types"],
                "severity": "warning"
            })
    
    if critical_error_components:
        logger.error(f"Critical error rates detected: {len(critical_error_components)} components")
        
        # Identify the most problematic component
        worst_component = max(critical_error_components, key=lambda x: x["error_rate"])
        
        return RunRequest(
            run_key=f"error_remediation_{context.cursor}",
            job_name="data_quality_job",
            run_config={
                "ops": {
                    "run_data_quality_checks": {
                        "config": {
                            "quality_rules": {
                                "completeness_threshold": 85.0,  # Lower threshold during errors
                                "accuracy_threshold": 90.0,
                                "timeliness_threshold_hours": 6.0,
                                "consistency_threshold": 80.0
                            },
                            "tables_to_check": [
                                "fema_disaster_declarations",
                                "noaa_weather_alerts",
                                "processed_fema_disasters"
                            ]
                        }
                    }
                }
            },
            tags={
                "trigger": "critical_errors",
                "worst_component": worst_component["component"],
                "error_rate": str(worst_component["error_rate"]),
                "total_critical": str(len(critical_error_components))
            }
        )
    
    elif high_error_components:
        logger.warning(f"Elevated error rates: {len(high_error_components)} components")
        return SkipReason("Elevated error rates detected but not critical - monitoring")
    
    else:
        logger.info("Error rates are within acceptable limits")
        return SkipReason("Error rates are within normal thresholds")


@sensor(
    name="system_health_sensor",
    description="Monitor overall system health and resource utilization",
    minimum_interval_seconds=600,  # Check every 10 minutes
    default_status=DefaultSensorStatus.RUNNING,
)
def system_health_sensor(context: SensorEvaluationContext):
    """
    Monitor system health metrics across all services
    Trigger scaling or maintenance actions when needed
    """
    logger = get_dagster_logger()
    
    # Simulate system health monitoring
    system_metrics = {
        "dagster": {
            "status": "healthy",
            "cpu_usage": 45.2,
            "memory_usage": 68.7,
            "active_runs": 3,
            "queue_length": 2,
            "last_successful_run": datetime.now() - timedelta(minutes=15)
        },
        "starrocks": {
            "status": "healthy",
            "cpu_usage": 72.1,
            "memory_usage": 81.3,
            "disk_usage": 67.5,
            "active_queries": 8,
            "query_queue_length": 3,
            "last_successful_query": datetime.now() - timedelta(minutes=2)
        },
        "kafka": {
            "status": "healthy",
            "cpu_usage": 35.8,
            "memory_usage": 55.2,
            "disk_usage": 45.1,
            "active_producers": 4,
            "active_consumers": 6,
            "partition_lag": 125
        },
        "flink": {
            "status": "degraded",  # Simulated issue
            "cpu_usage": 89.5,
            "memory_usage": 92.1,
            "active_jobs": 4,
            "failed_jobs": 1,
            "checkpoint_lag": 45000,
            "last_successful_checkpoint": datetime.now() - timedelta(minutes=8)
        }
    }
    
    # Health thresholds
    health_thresholds = {
        "cpu_warning": 80.0,
        "cpu_critical": 95.0,
        "memory_warning": 85.0,
        "memory_critical": 95.0,
        "disk_warning": 80.0,
        "disk_critical": 90.0
    }
    
    unhealthy_services = []
    critical_services = []
    
    for service, metrics in system_metrics.items():
        health_issues = []
        
        # Check CPU usage
        cpu_usage = metrics.get("cpu_usage", 0)
        if cpu_usage > health_thresholds["cpu_critical"]:
            health_issues.append(f"Critical CPU usage: {cpu_usage:.1f}%")
        elif cpu_usage > health_thresholds["cpu_warning"]:
            health_issues.append(f"High CPU usage: {cpu_usage:.1f}%")
        
        # Check memory usage
        memory_usage = metrics.get("memory_usage", 0)
        if memory_usage > health_thresholds["memory_critical"]:
            health_issues.append(f"Critical memory usage: {memory_usage:.1f}%")
        elif memory_usage > health_thresholds["memory_warning"]:
            health_issues.append(f"High memory usage: {memory_usage:.1f}%")
        
        # Check disk usage (if applicable)
        disk_usage = metrics.get("disk_usage", 0)
        if disk_usage > health_thresholds["disk_critical"]:
            health_issues.append(f"Critical disk usage: {disk_usage:.1f}%")
        elif disk_usage > health_thresholds["disk_warning"]:
            health_issues.append(f"High disk usage: {disk_usage:.1f}%")
        
        # Service-specific checks
        if service == "flink":
            if metrics.get("failed_jobs", 0) > 0:
                health_issues.append(f"Failed Flink jobs: {metrics['failed_jobs']}")
            
            checkpoint_lag = metrics.get("checkpoint_lag", 0)
            if checkpoint_lag > 60000:  # 1 minute
                health_issues.append(f"High checkpoint lag: {checkpoint_lag}ms")
        
        elif service == "kafka":
            partition_lag = metrics.get("partition_lag", 0)
            if partition_lag > 1000:
                health_issues.append(f"High partition lag: {partition_lag}")
        
        elif service == "starrocks":
            queue_length = metrics.get("query_queue_length", 0)
            if queue_length > 10:
                health_issues.append(f"High query queue: {queue_length}")
        
        if health_issues:
            service_health = {
                "service": service,
                "status": metrics.get("status", "unknown"),
                "issues": health_issues,
                "metrics": metrics
            }
            
            # Determine severity
            critical_issues = [issue for issue in health_issues 
                             if "Critical" in issue or "Failed" in issue]
            
            if critical_issues or metrics.get("status") == "degraded":
                critical_services.append(service_health)
            else:
                unhealthy_services.append(service_health)
    
    if critical_services:
        logger.error(f"Critical system health issues: {len(critical_services)} services")
        
        # Focus on the most critical service
        most_critical = critical_services[0]
        
        return RunRequest(
            run_key=f"system_health_remediation_{context.cursor}",
            job_name="database_optimization_job",
            tags={
                "trigger": "system_health",
                "severity": "critical",
                "affected_services": str(len(critical_services)),
                "primary_issue": most_critical["service"]
            }
        )
    
    elif unhealthy_services:
        logger.warning(f"System health warnings: {len(unhealthy_services)} services")
        return SkipReason("System health warnings detected but not critical")
    
    else:
        logger.info("All systems are healthy")
        return SkipReason("All systems are within healthy parameters")


@sensor(
    name="performance_degradation_sensor",
    description="Monitor performance metrics and detect degradation trends",
    minimum_interval_seconds=900,  # Check every 15 minutes
    default_status=DefaultSensorStatus.RUNNING,
)
def performance_degradation_sensor(context: SensorEvaluationContext):
    """
    Monitor performance trends and detect degradation
    Trigger optimization jobs when performance degrades significantly
    """
    logger = get_dagster_logger()
    
    # Simulate performance monitoring
    performance_metrics = {
        "data_ingestion": {
            "current_throughput": 85,    # records/second
            "baseline_throughput": 120,   # expected records/second
            "avg_latency_ms": 450,
            "baseline_latency_ms": 250,
            "success_rate": 94.5,
            "baseline_success_rate": 98.0
        },
        "data_processing": {
            "current_throughput": 200,
            "baseline_throughput": 300,
            "avg_latency_ms": 1200,
            "baseline_latency_ms": 800,
            "success_rate": 96.2,
            "baseline_success_rate": 99.0
        },
        "query_performance": {
            "avg_response_time_ms": 850,
            "baseline_response_time_ms": 400,
            "p95_response_time_ms": 2100,
            "baseline_p95_response_time_ms": 1000,
            "queries_per_second": 12,
            "baseline_qps": 25
        },
        "ml_pipeline": {
            "training_time_minutes": 45,
            "baseline_training_time": 25,
            "prediction_latency_ms": 150,
            "baseline_prediction_latency": 80,
            "model_accuracy": 0.87,
            "baseline_accuracy": 0.91
        }
    }
    
    # Performance degradation thresholds
    degradation_thresholds = {
        "throughput_degradation": 0.7,    # 30% degradation
        "latency_degradation": 1.5,       # 50% increase
        "success_rate_degradation": 0.95,  # 5% degradation
        "response_time_degradation": 2.0   # 100% increase
    }
    
    degraded_components = []
    severely_degraded = []
    
    for component, metrics in performance_metrics.items():
        degradation_issues = []
        
        # Check throughput degradation
        if "current_throughput" in metrics and "baseline_throughput" in metrics:
            throughput_ratio = metrics["current_throughput"] / metrics["baseline_throughput"]
            if throughput_ratio < degradation_thresholds["throughput_degradation"]:
                degradation_issues.append({
                    "metric": "throughput",
                    "current": metrics["current_throughput"],
                    "baseline": metrics["baseline_throughput"],
                    "degradation_percent": (1 - throughput_ratio) * 100
                })
        
        # Check latency increase
        if "avg_latency_ms" in metrics and "baseline_latency_ms" in metrics:
            latency_ratio = metrics["avg_latency_ms"] / metrics["baseline_latency_ms"]
            if latency_ratio > degradation_thresholds["latency_degradation"]:
                degradation_issues.append({
                    "metric": "latency",
                    "current": metrics["avg_latency_ms"],
                    "baseline": metrics["baseline_latency_ms"],
                    "increase_percent": (latency_ratio - 1) * 100
                })
        
        # Check success rate degradation
        if "success_rate" in metrics and "baseline_success_rate" in metrics:
            success_ratio = metrics["success_rate"] / metrics["baseline_success_rate"]
            if success_ratio < degradation_thresholds["success_rate_degradation"]:
                degradation_issues.append({
                    "metric": "success_rate",
                    "current": metrics["success_rate"],
                    "baseline": metrics["baseline_success_rate"],
                    "degradation_percent": (1 - success_ratio) * 100
                })
        
        # Check response time for queries
        if component == "query_performance":
            response_ratio = metrics["avg_response_time_ms"] / metrics["baseline_response_time_ms"]
            if response_ratio > degradation_thresholds["response_time_degradation"]:
                degradation_issues.append({
                    "metric": "response_time",
                    "current": metrics["avg_response_time_ms"],
                    "baseline": metrics["baseline_response_time_ms"],
                    "increase_percent": (response_ratio - 1) * 100
                })
        
        if degradation_issues:
            component_degradation = {
                "component": component,
                "issues": degradation_issues,
                "severity": "severe" if len(degradation_issues) >= 2 else "moderate"
            }
            
            degraded_components.append(component_degradation)
            
            if component_degradation["severity"] == "severe":
                severely_degraded.append(component_degradation)
    
    if severely_degraded:
        logger.error(f"Severe performance degradation: {len(severely_degraded)} components")
        
        # Prioritize database optimization for query performance issues
        if any(c["component"] == "query_performance" for c in severely_degraded):
            return RunRequest(
                run_key=f"performance_optimization_{context.cursor}",
                job_name="database_optimization_job",
                tags={
                    "trigger": "performance_degradation",
                    "severity": "severe",
                    "affected_components": str(len(severely_degraded)),
                    "optimization_target": "database"
                }
            )
        else:
            return RunRequest(
                run_key=f"general_optimization_{context.cursor}",
                job_name="comprehensive_processing_job",
                tags={
                    "trigger": "performance_degradation", 
                    "severity": "severe",
                    "affected_components": str(len(severely_degraded))
                }
            )
    
    elif degraded_components:
        logger.warning(f"Performance degradation detected: {len(degraded_components)} components")
        return SkipReason("Performance degradation detected but not severe")
    
    else:
        logger.info("Performance metrics are within acceptable ranges")
        return SkipReason("Performance is within normal parameters")


@sensor(
    name="cascade_failure_sensor",
    description="Detect cascade failure patterns that could impact multiple systems",
    minimum_interval_seconds=180,  # Check every 3 minutes (critical for cascade failures)
    default_status=DefaultSensorStatus.RUNNING,
)
def cascade_failure_sensor(context: SensorEvaluationContext):
    """
    Monitor for cascade failure patterns across system components
    Trigger emergency procedures when cascade failures are detected
    """
    logger = get_dagster_logger()
    
    # Simulate cascade failure detection
    system_status = {
        "kafka": {"status": "healthy", "dependent_services": ["flink", "dagster"]},
        "starrocks": {"status": "degraded", "dependent_services": ["dagster", "api"]},
        "flink": {"status": "unhealthy", "dependent_services": ["starrocks"]},
        "dagster": {"status": "healthy", "dependent_services": []},
        "api": {"status": "degraded", "dependent_services": []}
    }
    
    # Analyze failure propagation
    unhealthy_services = [s for s, status in system_status.items() 
                         if status["status"] in ["unhealthy", "degraded"]]
    
    cascade_risk = []
    active_cascades = []
    
    for service, status in system_status.items():
        if status["status"] == "unhealthy":
            # Check impact on dependent services
            for dependent in status["dependent_services"]:
                if dependent in system_status:
                    dependent_status = system_status[dependent]["status"]
                    if dependent_status in ["degraded", "unhealthy"]:
                        active_cascades.append({
                            "source": service,
                            "target": dependent,
                            "source_status": status["status"],
                            "target_status": dependent_status
                        })
                    elif dependent_status == "healthy":
                        cascade_risk.append({
                            "source": service,
                            "at_risk": dependent,
                            "risk_level": "high"
                        })
    
    if active_cascades:
        logger.error(f"Active cascade failures detected: {len(active_cascades)} failure chains")
        
        return RunRequest(
            run_key=f"cascade_failure_response_{context.cursor}",
            job_name="emergency_data_refresh_job",
            run_config={
                "ops": {
                    "validate_data_sources": {
                        "config": {
                            "sources_to_check": ["fema", "noaa"],  # Fallback to most critical sources
                            "timeout_seconds": 30
                        }
                    }
                }
            },
            tags={
                "trigger": "cascade_failure",
                "severity": "emergency",
                "active_cascades": str(len(active_cascades)),
                "unhealthy_services": str(len(unhealthy_services))
            }
        )
    
    elif cascade_risk:
        logger.warning(f"Cascade failure risk detected: {len(cascade_risk)} services at risk")
        return SkipReason("Cascade failure risk detected - monitoring closely")
    
    elif len(unhealthy_services) >= 2:
        logger.warning(f"Multiple service failures: {len(unhealthy_services)} services affected")
        return SkipReason("Multiple service failures but no cascade detected")
    
    else:
        return SkipReason("No cascade failure patterns detected")


@sensor(
    name="dependency_health_sensor", 
    description="Monitor health of external dependencies and services",
    minimum_interval_seconds=450,  # Check every 7.5 minutes
    default_status=DefaultSensorStatus.RUNNING,
)
def dependency_health_sensor(context: SensorEvaluationContext):
    """
    Monitor external dependencies critical to pipeline operation
    Trigger fallback procedures when dependencies fail
    """
    logger = get_dagster_logger()
    
    # Simulate dependency health monitoring
    dependencies = {
        "external_apis": {
            "fema_api": {"status": "healthy", "response_time": 250, "uptime": 99.9},
            "noaa_api": {"status": "healthy", "response_time": 180, "uptime": 99.8},
            "coagmet_api": {"status": "degraded", "response_time": 2500, "uptime": 95.2},
            "usda_api": {"status": "unhealthy", "response_time": None, "uptime": 78.1}
        },
        "infrastructure": {
            "network_connectivity": {"status": "healthy", "latency": 25, "packet_loss": 0.1},
            "dns_resolution": {"status": "healthy", "resolution_time": 15},
            "storage_backend": {"status": "healthy", "iops": 1500, "latency": 8}
        },
        "security_services": {
            "certificate_authority": {"status": "healthy", "cert_validity": 89},
            "authentication_service": {"status": "healthy", "response_time": 45}
        }
    }
    
    critical_failures = []
    degraded_dependencies = []
    
    for category, deps in dependencies.items():
        for dep_name, status in deps.items():
            if status["status"] == "unhealthy":
                critical_failures.append({
                    "dependency": dep_name,
                    "category": category,
                    "status": status,
                    "impact": "high" if category == "external_apis" else "medium"
                })
            elif status["status"] == "degraded":
                degraded_dependencies.append({
                    "dependency": dep_name,
                    "category": category,
                    "status": status,
                    "impact": "medium" if category == "external_apis" else "low"
                })
    
    if critical_failures:
        high_impact_failures = [f for f in critical_failures if f["impact"] == "high"]
        
        if high_impact_failures:
            logger.error(f"Critical dependency failures: {len(high_impact_failures)} high-impact failures")
            
            return RunRequest(
                run_key=f"dependency_failure_response_{context.cursor}",
                job_name="data_quality_job",
                run_config={
                    "ops": {
                        "monitor_data_freshness": {
                            "config": {
                                "freshness_thresholds": {
                                    "fema_hours": 8.0,    # Relaxed due to API issues
                                    "noaa_hours": 2.0,
                                    "coagmet_hours": 6.0,
                                    "usda_hours": 48.0    # Much more relaxed
                                }
                            }
                        }
                    }
                },
                tags={
                    "trigger": "dependency_failure",
                    "severity": "critical",
                    "failed_dependencies": str(len(critical_failures))
                }
            )
        else:
            logger.warning(f"Dependency failures: {len(critical_failures)} failures (low impact)")
            return SkipReason("Dependency failures detected but low impact")
    
    elif degraded_dependencies:
        logger.warning(f"Degraded dependencies: {len(degraded_dependencies)} dependencies")
        return SkipReason("Some dependencies degraded but still functional")
    
    else:
        return SkipReason("All dependencies are healthy")