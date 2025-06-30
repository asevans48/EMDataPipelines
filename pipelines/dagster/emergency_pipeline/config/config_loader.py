"""
Configuration Management for Emergency Management Pipeline
Centralized loading and validation of all configuration files
"""

import os
import yaml
from typing import Dict, Any, List, Optional
from dataclasses import dataclass
from pathlib import Path
import logging

logger = logging.getLogger(__name__)


@dataclass
class SourceConfig:
    """Configuration for a data source"""
    name: str
    description: str
    enabled: bool
    api: Dict[str, Any]
    endpoints: Dict[str, Any]
    data_classification: str
    update_frequency: str
    retention_days: int
    filters: Dict[str, Any] = None


@dataclass
class QualityConfig:
    """Configuration for data quality rules"""
    required_fields: List[str]
    field_patterns: Dict[str, str]
    business_rules: List[Dict[str, Any]]
    data_ranges: Dict[str, Dict[str, Any]]
    acceptable_values: Dict[str, List[str]]
    quality_dimensions: Dict[str, Any]


@dataclass
class ComplianceConfig:
    """Configuration for compliance requirements"""
    frameworks: Dict[str, Any]
    data_classification: Dict[str, Any]
    access_control: Dict[str, Any]
    audit_logging: Dict[str, Any]
    encryption: Dict[str, Any]
    incident_response: Dict[str, Any]


class ConfigManager:
    """Main configuration manager for the emergency pipeline"""
    
    def __init__(self, config_dir: str = None):
        if config_dir is None:
            # Default to config directory relative to this file
            self.config_dir = Path(__file__).parent
        else:
            self.config_dir = Path(config_dir)
        
        self.sources_config = {}
        self.quality_config = {}
        self.compliance_config = {}
        self.public_config = {}
        self.tenant_config = {}
        
        self._load_all_configs()
    
    def _load_all_configs(self):
        """Load all configuration files"""
        try:
            self._load_sources_config()
            self._load_quality_config()
            self._load_compliance_config()
            self._load_public_config()
            self._load_tenant_config()
        except Exception as e:
            logger.error(f"Error loading configurations: {str(e)}")
            raise
    
    def _load_sources_config(self):
        """Load data sources configuration"""
        sources_file = self.config_dir / "sources.yaml"
        
        if sources_file.exists():
            with open(sources_file, 'r') as f:
                self.sources_config = yaml.safe_load(f)
        else:
            logger.warning(f"Sources config file not found: {sources_file}")
            self.sources_config = self._get_default_sources_config()
    
    def _load_quality_config(self):
        """Load data quality rules configuration"""
        quality_file = self.config_dir / "quality_rules.yaml"
        
        if quality_file.exists():
            with open(quality_file, 'r') as f:
                self.quality_config = yaml.safe_load(f)
        else:
            logger.warning(f"Quality config file not found: {quality_file}")
            self.quality_config = self._get_default_quality_config()
    
    def _load_compliance_config(self):
        """Load compliance configuration"""
        compliance_file = self.config_dir / "compliance.yaml"
        
        if compliance_file.exists():
            with open(compliance_file, 'r') as f:
                self.compliance_config = yaml.safe_load(f)
        else:
            logger.warning(f"Compliance config file not found: {compliance_file}")
            self.compliance_config = self._get_default_compliance_config()
    
    def _load_public_config(self):
        """Load public data configuration"""
        # Look for public data config in parent directory
        public_file = self.config_dir.parent / "public_data_config.yml"
        
        if public_file.exists():
            with open(public_file, 'r') as f:
                self.public_config = yaml.safe_load(f)
        else:
            logger.warning(f"Public config file not found: {public_file}")
            self.public_config = {}
    
    def _load_tenant_config(self):
        """Load tenant configuration"""
        # Look for tenant config in parent directory  
        tenant_file = self.config_dir.parent / "tenant_config.yml"
        
        if tenant_file.exists():
            with open(tenant_file, 'r') as f:
                self.tenant_config = yaml.safe_load(f)
        else:
            logger.warning(f"Tenant config file not found: {tenant_file}")
            self.tenant_config = {}
    
    def get_source_config(self, source_name: str) -> Optional[SourceConfig]:
        """Get configuration for a specific data source"""
        source_data = self.sources_config.get(source_name)
        
        if not source_data:
            return None
        
        return SourceConfig(
            name=source_data.get('name', source_name),
            description=source_data.get('description', ''),
            enabled=source_data.get('enabled', True),
            api=source_data.get('api', {}),
            endpoints=source_data.get('endpoints', {}),
            data_classification=source_data.get('data_classification', 'PUBLIC'),
            update_frequency=source_data.get('update_frequency', 'hourly'),
            retention_days=source_data.get('retention_days', 365),
            filters=source_data.get('filters', {})
        )
    
    def get_quality_config(self, source_name: str) -> Optional[QualityConfig]:
        """Get quality configuration for a specific source"""
        quality_key = f"{source_name}_quality"
        quality_data = self.quality_config.get(quality_key)
        
        if not quality_data:
            # Fall back to global quality config
            quality_data = self.quality_config.get('global_quality', {})
        
        if not quality_data:
            return None
        
        return QualityConfig(
            required_fields=quality_data.get('required_fields', []),
            field_patterns=quality_data.get('field_patterns', {}),
            business_rules=quality_data.get('business_rules', []),
            data_ranges=quality_data.get('data_ranges', {}),
            acceptable_values=quality_data.get('acceptable_values', {}),
            quality_dimensions=quality_data.get('quality_dimensions', {})
        )
    
    def get_compliance_config(self) -> ComplianceConfig:
        """Get compliance configuration"""
        return ComplianceConfig(
            frameworks=self.compliance_config.get('frameworks', {}),
            data_classification=self.compliance_config.get('data_classification', {}),
            access_control=self.compliance_config.get('access_control', {}),
            audit_logging=self.compliance_config.get('audit_logging', {}),
            encryption=self.compliance_config.get('encryption', {}),
            incident_response=self.compliance_config.get('incident_response', {})
        )
    
    def get_enabled_sources(self) -> List[str]:
        """Get list of enabled data sources"""
        enabled_sources = []
        
        for source_name, config in self.sources_config.items():
            if (isinstance(config, dict) and 
                config.get('enabled', True) and
                source_name not in ['global_settings', 'source_groups', 'pipeline_settings', 'quality_defaults', 'compliance_defaults']):
                enabled_sources.append(source_name)
        
        return enabled_sources
    
    def get_source_group(self, group_name: str) -> List[str]:
        """Get sources in a specific group"""
        source_groups = self.sources_config.get('source_groups', {})
        return source_groups.get(group_name, [])
    
    def get_pipeline_settings(self) -> Dict[str, Any]:
        """Get pipeline configuration settings"""
        return self.sources_config.get('pipeline_settings', {})
    
    def get_tenant_config(self, tenant_id: str) -> Dict[str, Any]:
        """Get configuration for a specific tenant"""
        tenants = self.tenant_config.get('tenants', {})
        return tenants.get(tenant_id, {})
    
    def get_public_api_config(self) -> Dict[str, Any]:
        """Get public API configuration"""
        return self.public_config.get('api_endpoints', {})
    
    def is_public_mode(self) -> bool:
        """Check if running in public data mode"""
        return os.getenv("DATA_CLASSIFICATION", "PUBLIC") == "PUBLIC"
    
    def get_database_config(self, tenant_id: str = None) -> Dict[str, Any]:
        """Get database configuration"""
        if self.is_public_mode():
            return self.public_config.get('database', {})
        elif tenant_id:
            tenant_config = self.get_tenant_config(tenant_id)
            return tenant_config.get('database', {})
        else:
            return {}
    
    def get_api_rate_limits(self, organization_type: str = 'default') -> Dict[str, int]:
        """Get API rate limits based on organization type"""
        if self.is_public_mode():
            organizations = self.public_config.get('organizations', {})
            org_config = organizations.get(organization_type, organizations.get('public', {}))
            return org_config.get('rate_limits', {'api_calls_per_hour': 1000})
        else:
            # Tenant mode would have different rate limiting
            return {'api_calls_per_hour': 10000}
    
    def validate_config(self) -> List[str]:
        """Validate all loaded configurations and return any issues"""
        issues = []
        
        # Validate sources config
        enabled_sources = self.get_enabled_sources()
        if not enabled_sources:
            issues.append("No data sources are enabled")
        
        for source in enabled_sources:
            source_config = self.get_source_config(source)
            if not source_config:
                issues.append(f"Could not load configuration for source: {source}")
                continue
            
            # Check required API configuration
            if not source_config.api.get('base_url'):
                issues.append(f"Source {source} missing base_url in API configuration")
            
            # Check endpoints
            if not source_config.endpoints:
                issues.append(f"Source {source} has no configured endpoints")
        
        # Validate quality config
        for source in enabled_sources:
            quality_config = self.get_quality_config(source)
            if not quality_config:
                issues.append(f"No quality configuration found for source: {source}")
        
        # Validate compliance config
        compliance_config = self.get_compliance_config()
        if not compliance_config.frameworks:
            issues.append("No compliance frameworks configured")
        
        # Validate mode-specific configuration
        if self.is_public_mode():
            if not self.public_config:
                issues.append("Public mode enabled but no public configuration found")
        else:
            if not self.tenant_config:
                issues.append("Tenant mode enabled but no tenant configuration found")
        
        return issues
    
    def _get_default_sources_config(self) -> Dict[str, Any]:
        """Get default sources configuration if file is missing"""
        return {
            'global_settings': {
                'default_timeout_seconds': 30,
                'default_rate_limit_per_minute': 60
            },
            'fema': {
                'name': 'FEMA OpenFEMA',
                'enabled': True,
                'api': {
                    'base_url': 'https://www.fema.gov/api/open/v2/',
                    'rate_limit_per_minute': 60
                },
                'endpoints': {
                    'disaster_declarations': {
                        'path': 'DisasterDeclarationsSummaries'
                    }
                },
                'data_classification': 'PUBLIC',
                'update_frequency': 'hourly',
                'retention_days': 2555
            }
        }
    
    def _get_default_quality_config(self) -> Dict[str, Any]:
        """Get default quality configuration if file is missing"""
        return {
            'global_quality': {
                'completeness_threshold': 0.95,
                'timeliness_threshold_hours': 24,
                'required_fields': ['ingestion_timestamp', 'data_source']
            }
        }
    
    def _get_default_compliance_config(self) -> Dict[str, Any]:
        """Get default compliance configuration if file is missing"""
        return {
            'frameworks': {
                'fedramp': {
                    'name': 'Federal Risk and Authorization Management Program',
                    'level': 'Moderate',
                    'enabled': True
                }
            },
            'data_classification': {
                'levels': {
                    'public': {
                        'label': 'PUBLIC',
                        'encryption_required': False
                    },
                    'internal': {
                        'label': 'INTERNAL', 
                        'encryption_required': True
                    }
                }
            },
            'access_control': {
                'authentication': {
                    'multi_factor_required': True,
                    'session_timeout_minutes': 120
                }
            },
            'audit_logging': {
                'retention_years': 7,
                'real_time_monitoring': True
            },
            'encryption': {
                'at_rest': {
                    'enabled': True,
                    'algorithm': 'AES-256'
                },
                'in_transit': {
                    'enabled': True,
                    'tls_version': '1.3'
                }
            }
        }


def load_emergency_config(config_dir: str = None) -> ConfigManager:
    """Main function to load emergency management configuration"""
    config_manager = ConfigManager(config_dir)
    
    # Validate configuration
    issues = config_manager.validate_config()
    if issues:
        logger.warning(f"Configuration validation issues found: {issues}")
    
    return config_manager


def get_environment_config() -> Dict[str, Any]:
    """Get configuration from environment variables"""
    return {
        'data_classification': os.getenv('DATA_CLASSIFICATION', 'PUBLIC'),
        'deployment_mode': os.getenv('DEPLOYMENT_MODE', 'development'),
        'database_host': os.getenv('STARROCKS_HOST', 'starrocks-fe'),
        'database_port': int(os.getenv('STARROCKS_PORT', '9030')),
        'kafka_servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:29092'),
        'flink_jobmanager': os.getenv('FLINK_JOBMANAGER_HOST', 'flink-jobmanager'),
        
        # API Keys
        'fema_api_key': os.getenv('FEMA_API_KEY', ''),
        'noaa_contact_info': os.getenv('NOAA_CONTACT_INFO', ''),
        'usda_api_key': os.getenv('USDA_API_KEY', ''),
        
        # Security
        'encryption_enabled': os.getenv('ENCRYPTION_ENABLED', 'true').lower() == 'true',
        'audit_logging_enabled': os.getenv('AUDIT_LOGGING_ENABLED', 'true').lower() == 'true',
        
        # Performance
        'max_concurrent_sources': int(os.getenv('MAX_CONCURRENT_SOURCES', '4')),
        'default_timeout_seconds': int(os.getenv('DEFAULT_TIMEOUT_SECONDS', '30')),
        'batch_size': int(os.getenv('DEFAULT_BATCH_SIZE', '1000')),
    }


def merge_configs(base_config: Dict[str, Any], override_config: Dict[str, Any]) -> Dict[str, Any]:
    """Merge two configuration dictionaries, with override taking precedence"""
    merged = base_config.copy()
    
    for key, value in override_config.items():
        if key in merged and isinstance(merged[key], dict) and isinstance(value, dict):
            merged[key] = merge_configs(merged[key], value)
        else:
            merged[key] = value
    
    return merged


def load_config_with_overrides(
    config_dir: str = None,
    env_overrides: bool = True,
    custom_overrides: Dict[str, Any] = None
) -> ConfigManager:
    """Load configuration with environment and custom overrides"""
    
    # Load base configuration
    config_manager = ConfigManager(config_dir)
    
    # Apply environment variable overrides
    if env_overrides:
        env_config = get_environment_config()
        
        # Override specific source configurations based on environment
        for source_name in config_manager.get_enabled_sources():
            source_config = config_manager.sources_config.get(source_name, {})
            api_config = source_config.get('api', {})
            
            # Update API configuration with environment variables
            if source_name == 'fema' and env_config['fema_api_key']:
                api_config['api_key'] = env_config['fema_api_key']
            elif source_name == 'noaa' and env_config['noaa_contact_info']:
                api_config['contact_info'] = env_config['noaa_contact_info']
            elif source_name == 'usda' and env_config['usda_api_key']:
                api_config['api_key'] = env_config['usda_api_key']
            
            # Update timeout and other settings
            api_config['timeout_seconds'] = env_config['default_timeout_seconds']
    
    # Apply custom overrides
    if custom_overrides:
        config_manager.sources_config = merge_configs(
            config_manager.sources_config, 
            custom_overrides.get('sources', {})
        )
        config_manager.quality_config = merge_configs(
            config_manager.quality_config,
            custom_overrides.get('quality', {})
        )
        config_manager.compliance_config = merge_configs(
            config_manager.compliance_config,
            custom_overrides.get('compliance', {})
        )
    
    return config_manager


# Configuration validation utilities
def validate_source_config(source_config: SourceConfig) -> List[str]:
    """Validate a source configuration"""
    issues = []
    
    if not source_config.name:
        issues.append("Source name is required")
    
    if not source_config.api.get('base_url'):
        issues.append("API base_url is required")
    
    if source_config.api.get('api_key_required') and not source_config.api.get('api_key'):
        issues.append("API key is required but not provided")
    
    if not source_config.endpoints:
        issues.append("At least one endpoint must be configured")
    
    valid_classifications = ['PUBLIC', 'INTERNAL', 'RESTRICTED', 'CONFIDENTIAL']
    if source_config.data_classification not in valid_classifications:
        issues.append(f"Invalid data classification: {source_config.data_classification}")
    
    if source_config.retention_days < 1:
        issues.append("Retention days must be positive")
    
    return issues


def validate_quality_config(quality_config: QualityConfig) -> List[str]:
    """Validate a quality configuration"""
    issues = []
    
    if not quality_config.required_fields:
        issues.append("At least one required field should be specified")
    
    for field, pattern in quality_config.field_patterns.items():
        try:
            import re
            re.compile(pattern)
        except re.error:
            issues.append(f"Invalid regex pattern for field {field}: {pattern}")
    
    for rule in quality_config.business_rules:
        if not rule.get('name'):
            issues.append("Business rule missing name")
        if not rule.get('rule'):
            issues.append("Business rule missing rule definition")
    
    return issues


def get_config_summary(config_manager: ConfigManager) -> Dict[str, Any]:
    """Get a summary of the current configuration"""
    enabled_sources = config_manager.get_enabled_sources()
    
    summary = {
        'mode': 'public' if config_manager.is_public_mode() else 'tenant',
        'enabled_sources': enabled_sources,
        'total_sources_configured': len(enabled_sources),
        'compliance_frameworks': list(config_manager.get_compliance_config().frameworks.keys()),
        'pipeline_settings': config_manager.get_pipeline_settings(),
        'configuration_valid': len(config_manager.validate_config()) == 0
    }
    
    # Add source-specific summary
    source_summary = {}
    for source in enabled_sources:
        source_config = config_manager.get_source_config(source)
        if source_config:
            source_summary[source] = {
                'classification': source_config.data_classification,
                'update_frequency': source_config.update_frequency,
                'endpoints_count': len(source_config.endpoints),
                'retention_days': source_config.retention_days
            }
    
    summary['sources_detail'] = source_summary
    
    return summary