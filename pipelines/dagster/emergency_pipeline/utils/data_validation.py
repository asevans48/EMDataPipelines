"""
Data Validation Utilities for Emergency Management Pipeline
Comprehensive validation for schema, quality, and compliance requirements
"""

import pandas as pd
import numpy as np
import re
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional, Tuple, Union
from abc import ABC, abstractmethod
import logging

logger = logging.getLogger(__name__)


class ValidationResult:
    """Container for validation results"""
    
    def __init__(self, is_valid: bool = True, errors: List[str] = None, warnings: List[str] = None, metrics: Dict[str, Any] = None):
        self.is_valid = is_valid
        self.errors = errors or []
        self.warnings = warnings or []
        self.metrics = metrics or {}
    
    def add_error(self, error: str):
        """Add validation error"""
        self.errors.append(error)
        self.is_valid = False
    
    def add_warning(self, warning: str):
        """Add validation warning"""
        self.warnings.append(warning)
    
    def merge(self, other: 'ValidationResult'):
        """Merge another validation result"""
        self.is_valid = self.is_valid and other.is_valid
        self.errors.extend(other.errors)
        self.warnings.extend(other.warnings)
        self.metrics.update(other.metrics)


class BaseValidator(ABC):
    """Abstract base class for data validators"""
    
    def __init__(self, config: Dict[str, Any] = None):
        self.config = config or {}
    
    @abstractmethod
    def validate(self, data: pd.DataFrame) -> ValidationResult:
        """Validate data and return results"""
        pass


class SchemaValidator(BaseValidator):
    """Validates data schema and structure"""
    
    def __init__(self, schema_config: Dict[str, Any]):
        super().__init__(schema_config)
        self.required_columns = schema_config.get('required_columns', [])
        self.column_types = schema_config.get('column_types', {})
        self.nullable_columns = schema_config.get('nullable_columns', [])
        self.pattern_validations = schema_config.get('pattern_validations', {})
    
    def validate(self, data: pd.DataFrame) -> ValidationResult:
        """Validate schema compliance"""
        result = ValidationResult()
        
        if data.empty:
            result.add_warning("Dataset is empty")
            return result
        
        # Check required columns
        missing_columns = [col for col in self.required_columns if col not in data.columns]
        if missing_columns:
            result.add_error(f"Missing required columns: {missing_columns}")
        
        # Check column data types
        for column, expected_type in self.column_types.items():
            if column in data.columns:
                if not self._validate_column_type(data[column], expected_type):
                    result.add_error(f"Column '{column}' has incorrect data type. Expected: {expected_type}")
        
        # Check nullable constraints
        for column in data.columns:
            if column not in self.nullable_columns:
                null_count = data[column].isnull().sum()
                if null_count > 0:
                    result.add_error(f"Non-nullable column '{column}' contains {null_count} null values")
        
        # Check pattern validations
        for column, pattern in self.pattern_validations.items():
            if column in data.columns:
                invalid_count = self._validate_pattern(data[column], pattern)
                if invalid_count > 0:
                    result.add_error(f"Column '{column}' has {invalid_count} values that don't match required pattern")
        
        # Add schema metrics
        result.metrics = {
            'total_columns': len(data.columns),
            'total_rows': len(data),
            'missing_columns': len(missing_columns),
            'schema_compliance_score': self._calculate_compliance_score(data, result)
        }
        
        return result
    
    def _validate_column_type(self, series: pd.Series, expected_type: str) -> bool:
        """Validate column data type"""
        try:
            if expected_type == 'datetime':
                pd.to_datetime(series.dropna(), errors='raise')
            elif expected_type == 'numeric':
                pd.to_numeric(series.dropna(), errors='raise')
            elif expected_type == 'string':
                # Check if series can be converted to string
                series.dropna().astype(str)
            elif expected_type == 'boolean':
                # Check if values are boolean or can be converted
                unique_vals = set(series.dropna().unique())
                valid_bool_vals = {True, False, 'true', 'false', 'True', 'False', 1, 0, '1', '0'}
                if not unique_vals.issubset(valid_bool_vals):
                    return False
            return True
        except (ValueError, TypeError):
            return False
    
    def _validate_pattern(self, series: pd.Series, pattern: str) -> int:
        """Validate pattern compliance and return count of invalid values"""
        try:
            non_null_series = series.dropna().astype(str)
            invalid_count = (~non_null_series.str.match(pattern)).sum()
            return invalid_count
        except Exception:
            return len(series.dropna())
    
    def _calculate_compliance_score(self, data: pd.DataFrame, result: ValidationResult) -> float:
        """Calculate overall schema compliance score"""
        total_checks = len(self.required_columns) + len(self.column_types) + len(self.pattern_validations)
        if total_checks == 0:
            return 1.0
        
        failed_checks = len(result.errors)
        return max(0.0, (total_checks - failed_checks) / total_checks)


class QualityValidator(BaseValidator):
    """Validates data quality dimensions"""
    
    def __init__(self, quality_config: Dict[str, Any]):
        super().__init__(quality_config)
        self.completeness_threshold = quality_config.get('completeness_threshold', 0.95)
        self.uniqueness_threshold = quality_config.get('uniqueness_threshold', 0.95)
        self.outlier_threshold = quality_config.get('outlier_threshold', 3.0)
        self.freshness_threshold_hours = quality_config.get('freshness_threshold_hours', 24)
        self.critical_columns = quality_config.get('critical_columns', [])
    
    def validate(self, data: pd.DataFrame) -> ValidationResult:
        """Validate data quality"""
        result = ValidationResult()
        
        if data.empty:
            result.add_error("Cannot assess quality of empty dataset")
            return result
        
        # Completeness validation
        completeness_result = self._validate_completeness(data)
        result.merge(completeness_result)
        
        # Uniqueness validation
        uniqueness_result = self._validate_uniqueness(data)
        result.merge(uniqueness_result)
        
        # Consistency validation
        consistency_result = self._validate_consistency(data)
        result.merge(consistency_result)
        
        # Freshness validation
        freshness_result = self._validate_freshness(data)
        result.merge(freshness_result)
        
        # Outlier detection
        outlier_result = self._detect_outliers(data)
        result.merge(outlier_result)
        
        # Calculate overall quality score
        result.metrics['overall_quality_score'] = self._calculate_quality_score(result.metrics)
        
        return result
    
    def _validate_completeness(self, data: pd.DataFrame) -> ValidationResult:
        """Validate data completeness"""
        result = ValidationResult()
        
        total_cells = len(data) * len(data.columns)
        null_cells = data.isnull().sum().sum()
        completeness_ratio = 1 - (null_cells / total_cells) if total_cells > 0 else 0
        
        result.metrics['completeness_ratio'] = completeness_ratio
        result.metrics['null_cells'] = null_cells
        result.metrics['total_cells'] = total_cells
        
        if completeness_ratio < self.completeness_threshold:
            result.add_error(f"Completeness {completeness_ratio:.2%} below threshold {self.completeness_threshold:.2%}")
        
        # Check critical columns
        for column in self.critical_columns:
            if column in data.columns:
                column_completeness = 1 - (data[column].isnull().sum() / len(data))
                if column_completeness < 1.0:
                    result.add_error(f"Critical column '{column}' has {data[column].isnull().sum()} missing values")
        
        return result
    
    def _validate_uniqueness(self, data: pd.DataFrame) -> ValidationResult:
        """Validate data uniqueness"""
        result = ValidationResult()
        
        duplicate_count = data.duplicated().sum()
        uniqueness_ratio = 1 - (duplicate_count / len(data)) if len(data) > 0 else 1
        
        result.metrics['uniqueness_ratio'] = uniqueness_ratio
        result.metrics['duplicate_count'] = duplicate_count
        
        if uniqueness_ratio < self.uniqueness_threshold:
            result.add_warning(f"Uniqueness {uniqueness_ratio:.2%} below threshold {self.uniqueness_threshold:.2%}")
        
        return result
    
    def _validate_consistency(self, data: pd.DataFrame) -> ValidationResult:
        """Validate data consistency"""
        result = ValidationResult()
        
        consistency_issues = []
        
        # Date consistency checks
        if 'incident_begin_date' in data.columns and 'incident_end_date' in data.columns:
            begin_dates = pd.to_datetime(data['incident_begin_date'], errors='coerce')
            end_dates = pd.to_datetime(data['incident_end_date'], errors='coerce')
            
            inconsistent_dates = (begin_dates > end_dates).sum()
            if inconsistent_dates > 0:
                consistency_issues.append(f"{inconsistent_dates} records have begin date after end date")
        
        # State code consistency
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
                consistency_issues.append(f"Invalid state codes: {list(invalid_states)}")
        
        result.metrics['consistency_issues'] = len(consistency_issues)
        
        for issue in consistency_issues:
            result.add_warning(f"Consistency issue: {issue}")
        
        return result
    
    def _validate_freshness(self, data: pd.DataFrame) -> ValidationResult:
        """Validate data freshness"""
        result = ValidationResult()
        
        timestamp_columns = ['ingestion_timestamp', 'timestamp', 'last_updated']
        
        for col in timestamp_columns:
            if col in data.columns:
                try:
                    timestamps = pd.to_datetime(data[col], errors='coerce')
                    latest_timestamp = timestamps.max()
                    
                    if pd.notna(latest_timestamp):
                        age_hours = (datetime.now() - latest_timestamp).total_seconds() / 3600
                        result.metrics['data_age_hours'] = age_hours
                        
                        if age_hours > self.freshness_threshold_hours:
                            result.add_warning(f"Data is {age_hours:.1f} hours old, exceeds {self.freshness_threshold_hours} hour threshold")
                        
                        break
                except Exception:
                    continue
        
        return result
    
    def _detect_outliers(self, data: pd.DataFrame) -> ValidationResult:
        """Detect statistical outliers"""
        result = ValidationResult()
        
        numeric_columns = data.select_dtypes(include=[np.number]).columns
        outlier_counts = {}
        
        for column in numeric_columns:
            values = data[column].dropna()
            if len(values) > 10 and values.std() > 0:
                z_scores = np.abs((values - values.mean()) / values.std())
                outliers = (z_scores > self.outlier_threshold).sum()
                outlier_counts[column] = outliers
                
                if outliers > len(values) * 0.1:  # More than 10% outliers
                    result.add_warning(f"Column '{column}' has {outliers} outliers ({outliers/len(values):.1%})")
        
        result.metrics['outlier_counts'] = outlier_counts
        result.metrics['total_outliers'] = sum(outlier_counts.values())
        
        return result
    
    def _calculate_quality_score(self, metrics: Dict[str, Any]) -> float:
        """Calculate overall quality score"""
        scores = []
        
        # Completeness score
        if 'completeness_ratio' in metrics:
            scores.append(metrics['completeness_ratio'])
        
        # Uniqueness score
        if 'uniqueness_ratio' in metrics:
            scores.append(metrics['uniqueness_ratio'])
        
        # Consistency score (inverse of issues)
        if 'consistency_issues' in metrics:
            consistency_score = max(0, 1 - (metrics['consistency_issues'] / 10))  # Normalize
            scores.append(consistency_score)
        
        # Freshness score
        if 'data_age_hours' in metrics:
            freshness_score = max(0, 1 - (metrics['data_age_hours'] / (self.freshness_threshold_hours * 2)))
            scores.append(freshness_score)
        
        return np.mean(scores) if scores else 0.0


class ComplianceValidator(BaseValidator):
    """Validates federal compliance requirements"""
    
    def __init__(self, compliance_config: Dict[str, Any]):
        super().__init__(compliance_config)
        self.classification_levels = compliance_config.get('classification_levels', ['PUBLIC', 'INTERNAL', 'RESTRICTED', 'CONFIDENTIAL'])
        self.retention_requirements = compliance_config.get('retention_requirements', {})
        self.audit_requirements = compliance_config.get('audit_requirements', [])
        self.pii_detection_enabled = compliance_config.get('pii_detection_enabled', True)
    
    def validate(self, data: pd.DataFrame) -> ValidationResult:
        """Validate compliance requirements"""
        result = ValidationResult()
        
        if data.empty:
            result.add_warning("Cannot assess compliance of empty dataset")
            return result
        
        # Data classification validation
        classification_result = self._validate_classification(data)
        result.merge(classification_result)
        
        # PII detection
        if self.pii_detection_enabled:
            pii_result = self._detect_pii(data)
            result.merge(pii_result)
        
        # Audit trail validation
        audit_result = self._validate_audit_trail(data)
        result.merge(audit_result)
        
        # Retention compliance
        retention_result = self._validate_retention_compliance(data)
        result.merge(retention_result)
        
        # Calculate compliance score
        result.metrics['compliance_score'] = self._calculate_compliance_score(result.metrics)
        
        return result
    
    def _validate_classification(self, data: pd.DataFrame) -> ValidationResult:
        """Validate data classification"""
        result = ValidationResult()
        
        if 'data_classification' in data.columns:
            invalid_classifications = data[~data['data_classification'].isin(self.classification_levels)]['data_classification'].unique()
            
            if len(invalid_classifications) > 0:
                result.add_error(f"Invalid data classifications found: {list(invalid_classifications)}")
            
            # Check for mixed classifications
            unique_classifications = data['data_classification'].unique()
            if len(unique_classifications) > 1:
                result.add_warning(f"Dataset contains mixed classifications: {list(unique_classifications)}")
            
            result.metrics['classification_distribution'] = data['data_classification'].value_counts().to_dict()
        else:
            result.add_warning("No data classification column found")
        
        return result
    
    def _detect_pii(self, data: pd.DataFrame) -> ValidationResult:
        """Detect potential PII in data"""
        result = ValidationResult()
        
        pii_patterns = {
            'ssn': r'\b\d{3}-?\d{2}-?\d{4}\b',
            'phone': r'\b\d{3}[-.]?\d{3}[-.]?\d{4}\b',
            'email': r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b',
            'credit_card': r'\b\d{4}[-\s]?\d{4}[-\s]?\d{4}[-\s]?\d{4}\b'
        }
        
        pii_findings = {}
        
        for column in data.select_dtypes(include=['object']).columns:
            column_pii = {}
            for pii_type, pattern in pii_patterns.items():
                matches = data[column].astype(str).str.contains(pattern, regex=True, na=False).sum()
                if matches > 0:
                    column_pii[pii_type] = matches
            
            if column_pii:
                pii_findings[column] = column_pii
                result.add_error(f"Potential PII detected in column '{column}': {column_pii}")
        
        result.metrics['pii_findings'] = pii_findings
        result.metrics['pii_columns_count'] = len(pii_findings)
        
        return result
    
    def _validate_audit_trail(self, data: pd.DataFrame) -> ValidationResult:
        """Validate audit trail requirements"""
        result = ValidationResult()
        
        required_audit_fields = ['ingestion_timestamp', 'data_source']
        missing_audit_fields = [field for field in required_audit_fields if field not in data.columns]
        
        if missing_audit_fields:
            result.add_error(f"Missing required audit fields: {missing_audit_fields}")
        
        # Check for audit field completeness
        for field in required_audit_fields:
            if field in data.columns:
                null_count = data[field].isnull().sum()
                if null_count > 0:
                    result.add_error(f"Audit field '{field}' has {null_count} missing values")
        
        result.metrics['audit_completeness'] = len(required_audit_fields) - len(missing_audit_fields)
        
        return result
    
    def _validate_retention_compliance(self, data: pd.DataFrame) -> ValidationResult:
        """Validate data retention compliance"""
        result = ValidationResult()
        
        if 'ingestion_timestamp' in data.columns:
            try:
                timestamps = pd.to_datetime(data['ingestion_timestamp'], errors='coerce')
                data_age_days = (datetime.now() - timestamps.min()).days
                
                # Check against retention requirements
                data_source = data['data_source'].iloc[0] if 'data_source' in data.columns else 'unknown'
                retention_days = self.retention_requirements.get(data_source, 2555)  # Default 7 years
                
                if data_age_days > retention_days:
                    result.add_warning(f"Data from {data_source} is {data_age_days} days old, exceeds retention requirement of {retention_days} days")
                
                result.metrics['data_age_days'] = data_age_days
                result.metrics['retention_requirement_days'] = retention_days
                result.metrics['retention_compliance'] = data_age_days <= retention_days
                
            except Exception as e:
                result.add_warning(f"Could not validate retention compliance: {str(e)}")
        
        return result
    
    def _calculate_compliance_score(self, metrics: Dict[str, Any]) -> float:
        """Calculate overall compliance score"""
        scores = []
        
        # Classification score
        if 'classification_distribution' in metrics:
            scores.append(1.0)  # If classifications exist and are valid
        
        # PII score (lower is better)
        if 'pii_columns_count' in metrics:
            pii_score = max(0, 1 - (metrics['pii_columns_count'] / len(metrics.get('pii_findings', {}))))
            scores.append(pii_score)
        
        # Audit score
        if 'audit_completeness' in metrics:
            audit_score = metrics['audit_completeness'] / 2  # 2 required fields
            scores.append(audit_score)
        
        # Retention score
        if 'retention_compliance' in metrics:
            scores.append(1.0 if metrics['retention_compliance'] else 0.5)
        
        return np.mean(scores) if scores else 0.0


class DataValidator:
    """Main data validator that orchestrates all validation types"""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        
        # Initialize sub-validators
        self.schema_validator = SchemaValidator(config.get('schema', {}))
        self.quality_validator = QualityValidator(config.get('quality', {}))
        self.compliance_validator = ComplianceValidator(config.get('compliance', {}))
    
    def validate_all(self, data: pd.DataFrame) -> ValidationResult:
        """Run all validations and return comprehensive results"""
        overall_result = ValidationResult()
        
        # Schema validation
        schema_result = self.schema_validator.validate(data)
        overall_result.merge(schema_result)
        overall_result.metrics['schema_validation'] = {
            'is_valid': schema_result.is_valid,
            'errors': len(schema_result.errors),
            'warnings': len(schema_result.warnings),
            'metrics': schema_result.metrics
        }
        
        # Quality validation
        quality_result = self.quality_validator.validate(data)
        overall_result.merge(quality_result)
        overall_result.metrics['quality_validation'] = {
            'is_valid': quality_result.is_valid,
            'errors': len(quality_result.errors),
            'warnings': len(quality_result.warnings),
            'metrics': quality_result.metrics
        }
        
        # Compliance validation
        compliance_result = self.compliance_validator.validate(data)
        overall_result.merge(compliance_result)
        overall_result.metrics['compliance_validation'] = {
            'is_valid': compliance_result.is_valid,
            'errors': len(compliance_result.errors),
            'warnings': len(compliance_result.warnings),
            'metrics': compliance_result.metrics
        }
        
        # Calculate overall validation score
        scores = []
        if 'schema_compliance_score' in schema_result.metrics:
            scores.append(schema_result.metrics['schema_compliance_score'])
        if 'overall_quality_score' in quality_result.metrics:
            scores.append(quality_result.metrics['overall_quality_score'])
        if 'compliance_score' in compliance_result.metrics:
            scores.append(compliance_result.metrics['compliance_score'])
        
        overall_result.metrics['overall_validation_score'] = np.mean(scores) if scores else 0.0
        
        return overall_result
    
    def validate_by_source(self, data: pd.DataFrame, source_name: str) -> ValidationResult:
        """Validate data with source-specific rules"""
        
        # Get source-specific configuration
        source_config = self.config.get('sources', {}).get(source_name, {})
        
        # Create source-specific validators
        schema_config = {**self.config.get('schema', {}), **source_config.get('schema', {})}
        quality_config = {**self.config.get('quality', {}), **source_config.get('quality', {})}
        compliance_config = {**self.config.get('compliance', {}), **source_config.get('compliance', {})}
        
        source_schema_validator = SchemaValidator(schema_config)
        source_quality_validator = QualityValidator(quality_config)
        source_compliance_validator = ComplianceValidator(compliance_config)
        
        # Run validations
        result = ValidationResult()
        
        schema_result = source_schema_validator.validate(data)
        quality_result = source_quality_validator.validate(data)
        compliance_result = source_compliance_validator.validate(data)
        
        result.merge(schema_result)
        result.merge(quality_result)
        result.merge(compliance_result)
        
        # Add source-specific metadata
        result.metrics['source_name'] = source_name
        result.metrics['validation_timestamp'] = datetime.now().isoformat()
        
        return result