"""
Data Quality Operations for Emergency Management Pipeline
Operations for data validation, quality monitoring, and anomaly detection
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional, Tuple

from dagster import (
    op,
    OpExecutionContext,
    Config,
    get_dagster_logger,
    Out,
    In,
)
from pydantic import Field

from ..public_resources import PublicDataStarRocksResource


class QualityConfig(Config):
    """Configuration for data quality validation"""
    completeness_threshold: float = Field(default=0.95, description="Minimum completeness ratio")
    freshness_threshold_hours: int = Field(default=24, description="Maximum data age in hours")
    duplicate_threshold: float = Field(default=0.05, description="Maximum duplicate ratio")
    outlier_threshold: float = Field(default=3.0, description="Z-score threshold for outliers")
    critical_fields: List[str] = Field(default_factory=list, description="Fields that cannot be null")
    quality_dimensions: List[str] = Field(
        default=["completeness", "accuracy", "consistency", "timeliness", "validity"],
        description="Quality dimensions to evaluate"
    )


class FreshnessConfig(Config):
    """Configuration for data freshness checks"""
    source_thresholds: Dict[str, int] = Field(
        default={
            "FEMA_OpenFEMA": 4,      # 4 hours
            "NOAA_Weather_API": 1,    # 1 hour (critical for safety)
            "CoAgMet": 2,            # 2 hours
            "USDA_RMA": 24,          # 24 hours
        },
        description="Freshness thresholds by source in hours"
    )
    critical_sources: List[str] = Field(
        default=["NOAA_Weather_API"],
        description="Sources that require immediate attention if stale"
    )


@op(
    description="Validate data quality across multiple dimensions",
    ins={"data": In(pd.DataFrame)},
    out=Out(Dict[str, Any], description="Data quality assessment results"),
    config_schema=QualityConfig,
)
def validate_data_quality_op(context: OpExecutionContext, data: pd.DataFrame) -> Dict[str, Any]:
    """
    Comprehensive data quality validation for emergency management data
    Evaluates completeness, accuracy, consistency, timeliness, and validity
    """
    logger = get_dagster_logger()
    config = context.op_config
    
    if data.empty:
        return {
            "overall_quality_score": 0.0,
            "quality_status": "no_data",
            "issues": ["No data available for quality assessment"],
            "dimensions": {},
            "record_count": 0
        }
    
    quality_results = {
        "record_count": len(data),
        "assessment_timestamp": datetime.now().isoformat(),
        "source": data['data_source'].iloc[0] if 'data_source' in data.columns else 'unknown',
        "dimensions": {},
        "issues": [],
        "warnings": []
    }
    
    # Evaluate each quality dimension
    dimension_scores = {}
    
    if "completeness" in config["quality_dimensions"]:
        completeness_result = _assess_completeness(data, config)
        quality_results["dimensions"]["completeness"] = completeness_result
        dimension_scores["completeness"] = completeness_result["score"]
    
    if "accuracy" in config["quality_dimensions"]:
        accuracy_result = _assess_accuracy(data, config)
        quality_results["dimensions"]["accuracy"] = accuracy_result
        dimension_scores["accuracy"] = accuracy_result["score"]
    
    if "consistency" in config["quality_dimensions"]:
        consistency_result = _assess_consistency(data, config)
        quality_results["dimensions"]["consistency"] = consistency_result
        dimension_scores["consistency"] = consistency_result["score"]
    
    if "timeliness" in config["quality_dimensions"]:
        timeliness_result = _assess_timeliness(data, config)
        quality_results["dimensions"]["timeliness"] = timeliness_result
        dimension_scores["timeliness"] = timeliness_result["score"]
    
    if "validity" in config["quality_dimensions"]:
        validity_result = _assess_validity(data, config)
        quality_results["dimensions"]["validity"] = validity_result
        dimension_scores["validity"] = validity_result["score"]
    
    # Calculate overall quality score
    overall_score = np.mean(list(dimension_scores.values())) if dimension_scores else 0.0
    quality_results["overall_quality_score"] = overall_score
    
    # Determine quality status
    if overall_score >= 0.95:
        quality_status = "excellent"
    elif overall_score >= 0.90:
        quality_status = "good"
    elif overall_score >= 0.80:
        quality_status = "acceptable"
    elif overall_score >= 0.70:
        quality_status = "poor"
    else:
        quality_status = "critical"
    
    quality_results["quality_status"] = quality_status
    
    # Collect all issues and warnings
    for dimension, result in quality_results["dimensions"].items():
        quality_results["issues"].extend(result.get("issues", []))
        quality_results["warnings"].extend(result.get("warnings", []))
    
    logger.info(f"Data quality assessment completed: {quality_status} ({overall_score:.2f}) for {len(data)} records")
    
    return quality_results


def _assess_completeness(data: pd.DataFrame, config: QualityConfig) -> Dict[str, Any]:
    """Assess data completeness"""
    
    total_cells = len(data) * len(data.columns)
    null_cells = data.isnull().sum().sum()
    completeness_ratio = 1 - (null_cells / total_cells) if total_cells > 0 else 0
    
    # Check critical fields
    critical_field_issues = []
    critical_fields = config.get("critical_fields", [])
    
    for field in critical_fields:
        if field in data.columns:
            null_count = data[field].isnull().sum()
            if null_count > 0:
                critical_field_issues.append(f"Critical field '{field}' has {null_count} null values")
    
    # Per-column completeness
    column_completeness = {}
    for column in data.columns:
        column_completeness[column] = 1 - (data[column].isnull().sum() / len(data))
    
    issues = []
    warnings = []
    
    if completeness_ratio < config["completeness_threshold"]:
        issues.append(f"Completeness {completeness_ratio:.2%} below threshold {config['completeness_threshold']:.2%}")
    
    if critical_field_issues:
        issues.extend(critical_field_issues)
    
    # Identify columns with low completeness
    low_completeness_columns = [
        col for col, ratio in column_completeness.items() 
        if ratio < 0.9 and col not in ['data_source', 'ingestion_timestamp']
    ]
    
    if low_completeness_columns:
        warnings.append(f"Columns with low completeness: {', '.join(low_completeness_columns)}")
    
    return {
        "score": completeness_ratio,
        "threshold": config["completeness_threshold"],
        "total_cells": total_cells,
        "null_cells": null_cells,
        "column_completeness": column_completeness,
        "issues": issues,
        "warnings": warnings
    }


def _assess_accuracy(data: pd.DataFrame, config: QualityConfig) -> Dict[str, Any]:
    """Assess data accuracy through format validation and range checks"""
    
    accuracy_checks = []
    issues = []
    warnings = []
    
    # Date format validation
    date_columns = [col for col in data.columns if 'date' in col.lower() or 'time' in col.lower()]
    valid_dates = 0
    total_date_values = 0
    
    for col in date_columns:
        if col in data.columns:
            non_null_dates = data[col].dropna()
            total_date_values += len(non_null_dates)
            
            try:
                pd.to_datetime(non_null_dates, errors='coerce')
                valid_count = pd.to_datetime(non_null_dates, errors='coerce').notna().sum()
                valid_dates += valid_count
                
                invalid_count = len(non_null_dates) - valid_count
                if invalid_count > 0:
                    warnings.append(f"Column '{col}' has {invalid_count} invalid date values")
            except Exception:
                warnings.append(f"Could not validate dates in column '{col}'")
    
    # Numeric range validation
    numeric_columns = data.select_dtypes(include=[np.number]).columns
    for col in numeric_columns:
        values = data[col].dropna()
        if len(values) > 0:
            # Check for extreme outliers
            z_scores = np.abs((values - values.mean()) / values.std()) if values.std() > 0 else np.zeros(len(values))
            outliers = (z_scores > config["outlier_threshold"]).sum()
            
            if outliers > 0:
                warnings.append(f"Column '{col}' has {outliers} potential outliers")
    
    # State code validation (for emergency management data)
    if 'state' in data.columns:
        valid_states = {
            'AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'FL', 'GA',
            'HI', 'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'ME', 'MD',
            'MA', 'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH', 'NJ',
            'NM', 'NY', 'NC', 'ND', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC',
            'SD', 'TN', 'TX', 'UT', 'VT', 'VA', 'WA', 'WV', 'WI', 'WY',
            'DC', 'PR', 'VI', 'GU', 'AS', 'MP'
        }
        
        invalid_states = data[~data['state'].isin(valid_states)]['state'].unique()
        if len(invalid_states) > 0:
            issues.append(f"Invalid state codes found: {', '.join(invalid_states)}")
    
    # Calculate overall accuracy score
    accuracy_score = valid_dates / total_date_values if total_date_values > 0 else 1.0
    
    # Adjust score based on issues
    if issues:
        accuracy_score *= 0.8  # Reduce score for validation issues
    if warnings:
        accuracy_score *= 0.95  # Slightly reduce score for warnings
    
    return {
        "score": min(accuracy_score, 1.0),
        "date_validation": {
            "valid_dates": valid_dates,
            "total_date_values": total_date_values,
            "accuracy_ratio": valid_dates / total_date_values if total_date_values > 0 else 1.0
        },
        "issues": issues,
        "warnings": warnings
    }


def _assess_consistency(data: pd.DataFrame, config: QualityConfig) -> Dict[str, Any]:
    """Assess data consistency across records"""
    
    issues = []
    warnings = []
    consistency_checks = []
    
    # Check for duplicate records
    duplicate_count = data.duplicated().sum()
    duplicate_ratio = duplicate_count / len(data) if len(data) > 0 else 0
    
    if duplicate_ratio > config["duplicate_threshold"]:
        issues.append(f"Duplicate ratio {duplicate_ratio:.2%} exceeds threshold {config['duplicate_threshold']:.2%}")
    
    # Check consistency of related fields
    consistency_score = 1.0
    
    # For FEMA data, check date consistency
    if 'incidentBeginDate' in data.columns and 'incidentEndDate' in data.columns:
        begin_dates = pd.to_datetime(data['incidentBeginDate'], errors='coerce')
        end_dates = pd.to_datetime(data['incidentEndDate'], errors='coerce')
        
        inconsistent_dates = (begin_dates > end_dates).sum()
        if inconsistent_dates > 0:
            issues.append(f"{inconsistent_dates} records have incident begin date after end date")
            consistency_score *= 0.9
    
    # For weather alerts, check effective/expires consistency
    if 'effective' in data.columns and 'expires' in data.columns:
        effective_dates = pd.to_datetime(data['effective'], errors='coerce')
        expires_dates = pd.to_datetime(data['expires'], errors='coerce')
        
        inconsistent_alerts = (effective_dates > expires_dates).sum()
        if inconsistent_alerts > 0:
            issues.append(f"{inconsistent_alerts} weather alerts have effective date after expiration")
            consistency_score *= 0.9
    
    # Check for consistent formatting within columns
    string_columns = data.select_dtypes(include=['object']).columns
    for col in string_columns:
        if col in ['state', 'incident_type_standardized']:
            unique_values = data[col].dropna().unique()
            if len(unique_values) > 0:
                # Check for mixed case inconsistencies
                case_variations = len(set(val.lower() for val in unique_values if isinstance(val, str)))
                if case_variations < len(unique_values):
                    warnings.append(f"Column '{col}' has case inconsistencies")
    
    # Adjust consistency score for duplicates
    consistency_score *= (1 - duplicate_ratio)
    
    return {
        "score": max(consistency_score, 0.0),
        "duplicate_count": duplicate_count,
        "duplicate_ratio": duplicate_ratio,
        "issues": issues,
        "warnings": warnings
    }


def _assess_timeliness(data: pd.DataFrame, config: QualityConfig) -> Dict[str, Any]:
    """Assess data timeliness and freshness"""
    
    issues = []
    warnings = []
    
    # Check ingestion timestamp freshness
    if 'ingestion_timestamp' in data.columns:
        latest_ingestion = pd.to_datetime(data['ingestion_timestamp'].max())
        data_age_hours = (datetime.now() - latest_ingestion).total_seconds() / 3600
        
        threshold_hours = config.get("freshness_threshold_hours", 24)
        
        if data_age_hours > threshold_hours:
            issues.append(f"Data is {data_age_hours:.1f} hours old, exceeds {threshold_hours} hour threshold")
        
        timeliness_score = max(0, 1 - (data_age_hours / (threshold_hours * 2)))
    else:
        warnings.append("No ingestion timestamp found for timeliness assessment")
        timeliness_score = 0.8  # Partial score without timestamp
        data_age_hours = None
    
    # Check for old events still being reported
    if 'declarationDate' in data.columns:
        declaration_dates = pd.to_datetime(data['declarationDate'], errors='coerce')
        very_old_events = (datetime.now() - declaration_dates > timedelta(days=365)).sum()
        
        if very_old_events > len(data) * 0.1:  # More than 10% very old events
            warnings.append(f"{very_old_events} events are more than 1 year old")
    
    return {
        "score": timeliness_score,
        "data_age_hours": data_age_hours,
        "threshold_hours": config.get("freshness_threshold_hours", 24),
        "issues": issues,
        "warnings": warnings
    }


def _assess_validity(data: pd.DataFrame, config: QualityConfig) -> Dict[str, Any]:
    """Assess data validity against business rules"""
    
    issues = []
    warnings = []
    validity_checks = []
    
    # Business rule validations
    validity_score = 1.0
    
    # For FEMA data - validate disaster numbers
    if 'disasterNumber' in data.columns:
        invalid_disaster_numbers = data[
            (data['disasterNumber'].notna()) & 
            (~data['disasterNumber'].astype(str).str.match(r'^\d{4,5}$'))
        ].shape[0]
        
        if invalid_disaster_numbers > 0:
            issues.append(f"{invalid_disaster_numbers} records have invalid disaster number format")
            validity_score *= 0.95
    
    # For weather data - validate severity levels
    if 'severity' in data.columns:
        valid_severities = {'Extreme', 'Severe', 'Moderate', 'Minor'}
        invalid_severities = data[
            (data['severity'].notna()) & 
            (~data['severity'].isin(valid_severities))
        ].shape[0]
        
        if invalid_severities > 0:
            issues.append(f"{invalid_severities} records have invalid severity levels")
            validity_score *= 0.95
    
    # For geographic data - validate coordinates
    if 'latitude' in data.columns and 'longitude' in data.columns:
        invalid_coords = data[
            (data['latitude'].notna()) & (data['longitude'].notna()) &
            ((data['latitude'] < -90) | (data['latitude'] > 90) |
             (data['longitude'] < -180) | (data['longitude'] > 180))
        ].shape[0]
        
        if invalid_coords > 0:
            issues.append(f"{invalid_coords} records have invalid coordinates")
            validity_score *= 0.9
    
    # For monetary fields - validate positive values
    monetary_fields = ['premium_amount', 'liability_amount', 'indemnity_amount']
    for field in monetary_fields:
        if field in data.columns:
            negative_values = (data[field] < 0).sum()
            if negative_values > 0:
                warnings.append(f"Field '{field}' has {negative_values} negative values")
    
    return {
        "score": validity_score,
        "business_rules_passed": len(validity_checks),
        "issues": issues,
        "warnings": warnings
    }


@op(
    description="Check data freshness across all sources",
    ins={"data": In(pd.DataFrame)},
    out=Out(Dict[str, Any], description="Data freshness assessment"),
    config_schema=FreshnessConfig,
)
def check_data_freshness_op(context: OpExecutionContext, data: pd.DataFrame) -> Dict[str, Any]:
    """
    Check data freshness for emergency management sources
    Identifies stale data that may impact emergency response
    """
    logger = get_dagster_logger()
    config = context.op_config
    
    if data.empty:
        return {
            "overall_freshness": "no_data",
            "stale_sources": [],
            "critical_issues": [],
            "source_freshness": {}
        }
    
    source_thresholds = config["source_thresholds"]
    critical_sources = config["critical_sources"]
    
    freshness_results = {
        "assessment_timestamp": datetime.now().isoformat(),
        "source_freshness": {},
        "stale_sources": [],
        "critical_issues": [],
        "warnings": []
    }
    
    # Group data by source
    if 'data_source' in data.columns:
        for source in data['data_source'].unique():
            source_data = data[data['data_source'] == source]
            
            if 'ingestion_timestamp' in source_data.columns:
                latest_ingestion = pd.to_datetime(source_data['ingestion_timestamp'].max())
                data_age_hours = (datetime.now() - latest_ingestion).total_seconds() / 3600
                
                threshold = source_thresholds.get(source, 24)  # Default 24 hours
                
                source_result = {
                    "source": source,
                    "latest_ingestion": latest_ingestion.isoformat(),
                    "age_hours": data_age_hours,
                    "threshold_hours": threshold,
                    "is_fresh": data_age_hours <= threshold,
                    "is_critical_source": source in critical_sources
                }
                
                freshness_results["source_freshness"][source] = source_result
                
                if not source_result["is_fresh"]:
                    freshness_results["stale_sources"].append(source_result)
                    
                    if source in critical_sources:
                        freshness_results["critical_issues"].append({
                            "source": source,
                            "issue": f"Critical source {source} is {data_age_hours:.1f} hours stale",
                            "age_hours": data_age_hours,
                            "threshold_hours": threshold
                        })
                    else:
                        freshness_results["warnings"].append(
                            f"Source {source} is {data_age_hours:.1f} hours stale (threshold: {threshold}h)"
                        )
    
    # Determine overall freshness status
    if freshness_results["critical_issues"]:
        overall_freshness = "critical"
    elif freshness_results["stale_sources"]:
        overall_freshness = "stale"
    else:
        overall_freshness = "fresh"
    
    freshness_results["overall_freshness"] = overall_freshness
    
    logger.info(f"Freshness assessment: {overall_freshness} - {len(freshness_results['stale_sources'])} stale sources")
    
    return freshness_results


@op(
    description="Detect anomalies in emergency management data",
    ins={"data": In(pd.DataFrame)},
    out=Out(Dict[str, Any], description="Anomaly detection results"),
)
def detect_anomalies_op(context: OpExecutionContext, data: pd.DataFrame) -> Dict[str, Any]:
    """
    Detect anomalies in emergency management data patterns
    Identifies unusual spikes, patterns, or outliers that may indicate issues
    """
    logger = get_dagster_logger()
    
    if data.empty:
        return {
            "anomalies_detected": 0,
            "anomaly_types": {},
            "recommendations": []
        }
    
    anomalies = {
        "detection_timestamp": datetime.now().isoformat(),
        "total_records": len(data),
        "anomalies_detected": 0,
        "anomaly_types": {},
        "anomalies": [],
        "recommendations": []
    }
    
    # Volume anomalies - unusual number of records
    source_name = data['data_source'].iloc[0] if 'data_source' in data.columns else 'unknown'
    record_count = len(data)
    
    # Historical baselines (in practice, would come from historical data)
    typical_volumes = {
        'FEMA_OpenFEMA': {'min': 50, 'max': 500, 'typical': 150},
        'NOAA_Weather_API': {'min': 10, 'max': 1000, 'typical': 100},
        'CoAgMet': {'min': 100, 'max': 2000, 'typical': 500},
        'USDA_RMA': {'min': 20, 'max': 200, 'typical': 75}
    }
    
    if source_name in typical_volumes:
        baseline = typical_volumes[source_name]
        
        if record_count < baseline['min']:
            anomalies["anomalies"].append({
                "type": "volume_anomaly",
                "severity": "low",
                "description": f"Unusually low volume: {record_count} records (typical: {baseline['typical']})",
                "source": source_name
            })
            anomalies["anomalies_detected"] += 1
        
        elif record_count > baseline['max']:
            anomalies["anomalies"].append({
                "type": "volume_anomaly",
                "severity": "high",
                "description": f"Unusually high volume: {record_count} records (typical: {baseline['typical']})",
                "source": source_name
            })
            anomalies["anomalies_detected"] += 1
    
    # Temporal anomalies - unusual patterns over time
    if 'ingestion_timestamp' in data.columns:
        timestamps = pd.to_datetime(data['ingestion_timestamp'])
        
        # Check for data clustering (all data from same time)
        time_span = (timestamps.max() - timestamps.min()).total_seconds() / 3600
        if time_span < 0.1 and len(data) > 10:  # All data within 6 minutes
            anomalies["anomalies"].append({
                "type": "temporal_anomaly",
                "severity": "medium",
                "description": f"All {len(data)} records clustered within {time_span:.2f} hours",
                "source": source_name
            })
            anomalies["anomalies_detected"] += 1
    
    # Geographic anomalies
    if 'state' in data.columns:
        state_counts = data['state'].value_counts()
        
        # Check for unusual state concentration
        if len(state_counts) == 1 and len(data) > 20:
            single_state = state_counts.index[0]
            anomalies["anomalies"].append({
                "type": "geographic_anomaly",
                "severity": "medium",
                "description": f"All {len(data)} records from single state: {single_state}",
                "source": source_name
            })
            anomalies["anomalies_detected"] += 1
        
        # Check for unexpected states for certain sources
        if source_name == 'CoAgMet':
            non_co_records = data[data['state'] != 'CO'].shape[0]
            if non_co_records > 0:
                anomalies["anomalies"].append({
                    "type": "geographic_anomaly",
                    "severity": "high",
                    "description": f"CoAgMet data contains {non_co_records} non-Colorado records",
                    "source": source_name
                })
                anomalies["anomalies_detected"] += 1
    
    # Data value anomalies - statistical outliers
    numeric_columns = data.select_dtypes(include=[np.number]).columns
    for col in numeric_columns:
        if col in ['temperature_celsius', 'wind_speed', 'precipitation']:
            values = data[col].dropna()
            if len(values) > 10:
                z_scores = np.abs((values - values.mean()) / values.std()) if values.std() > 0 else np.zeros(len(values))
                extreme_outliers = (z_scores > 4).sum()  # Very extreme outliers
                
                if extreme_outliers > 0:
                    anomalies["anomalies"].append({
                        "type": "statistical_anomaly",
                        "severity": "medium",
                        "description": f"Column '{col}' has {extreme_outliers} extreme outliers",
                        "source": source_name,
                        "details": {"column": col, "outlier_count": extreme_outliers}
                    })
                    anomalies["anomalies_detected"] += 1
    
    # Source-specific anomaly checks
    if source_name == 'NOAA_Weather_API':
        anomalies = _detect_weather_anomalies(data, anomalies)
    elif source_name == 'FEMA_OpenFEMA':
        anomalies = _detect_disaster_anomalies(data, anomalies)
    
    # Categorize anomaly types
    anomaly_type_counts = {}
    for anomaly in anomalies["anomalies"]:
        atype = anomaly["type"]
        anomaly_type_counts[atype] = anomaly_type_counts.get(atype, 0) + 1
    
    anomalies["anomaly_types"] = anomaly_type_counts
    
    # Generate recommendations
    if anomalies["anomalies_detected"] > 0:
        if any(a["severity"] == "high" for a in anomalies["anomalies"]):
            anomalies["recommendations"].append("Immediate investigation required for high-severity anomalies")
        
        if "volume_anomaly" in anomaly_type_counts:
            anomalies["recommendations"].append("Check data source connectivity and ingestion processes")
        
        if "temporal_anomaly" in anomaly_type_counts:
            anomalies["recommendations"].append("Review data ingestion timing and scheduling")
        
        if "geographic_anomaly" in anomaly_type_counts:
            anomalies["recommendations"].append("Validate geographic filtering and source configuration")
    
    logger.info(f"Anomaly detection completed: {anomalies['anomalies_detected']} anomalies found in {len(data)} records")
    
    return anomalies


def _detect_weather_anomalies(data: pd.DataFrame, anomalies: Dict[str, Any]) -> Dict[str, Any]:
    """Detect weather-specific anomalies"""
    
    # Check for too many extreme weather alerts
    if 'severity' in data.columns:
        extreme_alerts = (data['severity'] == 'Extreme').sum()
        total_alerts = len(data)
        
        if extreme_alerts > total_alerts * 0.5:  # More than 50% extreme
            anomalies["anomalies"].append({
                "type": "weather_anomaly",
                "severity": "high",
                "description": f"Unusually high proportion of extreme weather alerts: {extreme_alerts}/{total_alerts}",
                "source": "NOAA_Weather_API"
            })
            anomalies["anomalies_detected"] += 1
    
    # Check for alerts with very long durations
    if 'alert_duration_hours' in data.columns:
        long_alerts = (data['alert_duration_hours'] > 72).sum()  # More than 3 days
        
        if long_alerts > 0:
            anomalies["anomalies"].append({
                "type": "weather_anomaly",
                "severity": "medium",
                "description": f"{long_alerts} weather alerts with duration > 72 hours",
                "source": "NOAA_Weather_API"
            })
            anomalies["anomalies_detected"] += 1
    
    return anomalies


def _detect_disaster_anomalies(data: pd.DataFrame, anomalies: Dict[str, Any]) -> Dict[str, Any]:
    """Detect disaster-specific anomalies"""
    
    # Check for unusual disaster type concentrations
    if 'incidentType' in data.columns:
        disaster_types = data['incidentType'].value_counts()
        
        # Check if one disaster type dominates
        if len(disaster_types) > 1:
            dominant_ratio = disaster_types.iloc[0] / len(data)
            if dominant_ratio > 0.8:  # One type is 80%+ of all disasters
                dominant_type = disaster_types.index[0]
                anomalies["anomalies"].append({
                    "type": "disaster_anomaly",
                    "severity": "medium",
                    "description": f"Single disaster type '{dominant_type}' dominates: {dominant_ratio:.1%} of records",
                    "source": "FEMA_OpenFEMA"
                })
                anomalies["anomalies_detected"] += 1
    
    # Check for disasters with unrealistic durations
    if 'disaster_duration_days' in data.columns:
        very_long_disasters = (data['disaster_duration_days'] > 365).sum()  # More than 1 year
        
        if very_long_disasters > 0:
            anomalies["anomalies"].append({
                "type": "disaster_anomaly",
                "severity": "low",
                "description": f"{very_long_disasters} disasters with duration > 1 year",
                "source": "FEMA_OpenFEMA"
            })
            anomalies["anomalies_detected"] += 1
    
    return anomalies


@op(
    description="Generate comprehensive data quality report",
    ins={
        "quality_results": In(Dict[str, Any]),
        "freshness_results": In(Dict[str, Any]),
        "anomaly_results": In(Dict[str, Any])
    },
    out=Out(Dict[str, Any], description="Comprehensive quality report"),
)
def generate_quality_report_op(
    context: OpExecutionContext,
    quality_results: Dict[str, Any],
    freshness_results: Dict[str, Any],
    anomaly_results: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Generate comprehensive data quality report
    Combines quality, freshness, and anomaly assessments
    """
    logger = get_dagster_logger()
    
    report = {
        "report_timestamp": datetime.now().isoformat(),
        "executive_summary": {},
        "detailed_results": {
            "quality_assessment": quality_results,
            "freshness_assessment": freshness_results,
            "anomaly_detection": anomaly_results
        },
        "recommendations": [],
        "action_items": [],
        "overall_status": "unknown"
    }
    
    # Calculate overall scores
    quality_score = quality_results.get("overall_quality_score", 0.0)
    freshness_status = freshness_results.get("overall_freshness", "unknown")
    anomaly_count = anomaly_results.get("anomalies_detected", 0)
    
    # Determine overall status
    if quality_score >= 0.9 and freshness_status == "fresh" and anomaly_count == 0:
        overall_status = "excellent"
    elif quality_score >= 0.8 and freshness_status in ["fresh", "stale"] and anomaly_count <= 2:
        overall_status = "good"
    elif quality_score >= 0.7 and freshness_status != "critical" and anomaly_count <= 5:
        overall_status = "acceptable"
    elif quality_score >= 0.6:
        overall_status = "poor"
    else:
        overall_status = "critical"
    
    report["overall_status"] = overall_status
    
    # Executive summary
    report["executive_summary"] = {
        "overall_status": overall_status,
        "data_quality_score": quality_score,
        "freshness_status": freshness_status,
        "anomalies_detected": anomaly_count,
        "critical_issues": len(freshness_results.get("critical_issues", [])),
        "total_records_assessed": quality_results.get("record_count", 0),
        "sources_assessed": len(freshness_results.get("source_freshness", {}))
    }
    
    # Compile recommendations
    recommendations = []
    action_items = []
    
    # Quality-based recommendations
    if quality_score < 0.8:
        recommendations.append("Data quality requires improvement - focus on completeness and accuracy")
        action_items.append({
            "priority": "high",
            "action": "Implement data validation at ingestion points",
            "owner": "data_engineering_team"
        })
    
    # Freshness-based recommendations
    if freshness_status == "critical":
        recommendations.append("Critical data sources are stale - immediate attention required")
        action_items.append({
            "priority": "urgent",
            "action": "Investigate and restore stale data sources",
            "owner": "platform_team"
        })
    elif freshness_status == "stale":
        recommendations.append("Some data sources are stale - schedule maintenance")
        action_items.append({
            "priority": "medium",
            "action": "Review and optimize data ingestion schedules",
            "owner": "data_engineering_team"
        })
    
    # Anomaly-based recommendations
    if anomaly_count > 5:
        recommendations.append("High number of anomalies detected - investigate data sources")
        action_items.append({
            "priority": "high",
            "action": "Investigate anomaly root causes and implement monitoring",
            "owner": "data_quality_team"
        })
    
    # Add specific recommendations from sub-assessments
    recommendations.extend(anomaly_results.get("recommendations", []))
    
    report["recommendations"] = recommendations
    report["action_items"] = action_items
    
    # Generate summary text
    summary_text = f"""
    DATA QUALITY REPORT SUMMARY
    ===========================
    Overall Status: {overall_status.upper()}
    Quality Score: {quality_score:.2f}/1.00
    Freshness Status: {freshness_status.upper()}
    Anomalies Detected: {anomaly_count}
    
    Records Assessed: {report['executive_summary']['total_records_assessed']:,}
    Sources Assessed: {report['executive_summary']['sources_assessed']}
    Critical Issues: {report['executive_summary']['critical_issues']}
    
    Key Findings:
    - Data quality is {quality_results.get('quality_status', 'unknown')}
    - {len(freshness_results.get('stale_sources', []))} sources have stale data
    - {anomaly_count} anomalies require investigation
    
    Next Steps:
    {chr(10).join(f'- {rec}' for rec in recommendations[:3])}
    """
    
    report["summary_text"] = summary_text
    
    logger.info(f"Quality report generated: {overall_status} status with {len(action_items)} action items")
    
    return report