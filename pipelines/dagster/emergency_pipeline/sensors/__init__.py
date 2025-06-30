"""
Sensors module for Emergency Management Data Pipeline
Event-driven monitoring and alerting for emergency data systems
"""

from .data_sensors import (
    data_freshness_sensor,
    data_volume_anomaly_sensor,
    source_availability_sensor,
)

from .error_sensors import (
    error_monitoring_sensor,
    system_health_sensor,
    performance_degradation_sensor,
)

# Import mode-specific sensors conditionally
try:
    from .public_sensors import (
        public_api_health_sensor,
        usage_spike_sensor,
        data_freshness_public_sensor,
        public_data_quality_sensor,
        external_api_availability_sensor,
        public_feedback_sensor,
    )
    
    # Public sensors list
    public_sensors = [
        public_api_health_sensor,
        usage_spike_sensor,
        data_freshness_public_sensor,
        public_data_quality_sensor,
        external_api_availability_sensor,
        public_feedback_sensor,
    ]
    
except ImportError:
    # Public sensors not available
    public_sensors = []

# Try to import tenant-specific sensors if available
try:
    from .tenant_sensors import (
        federal_compliance_sensor,
        tenant_isolation_sensor,
        security_audit_sensor,
    )
    
    tenant_sensors = [
        federal_compliance_sensor,
        tenant_isolation_sensor, 
        security_audit_sensor,
    ]
    
except ImportError:
    # Tenant sensors not available
    tenant_sensors = []

# Core sensors that work in all modes
core_sensors = [
    data_freshness_sensor,
    data_volume_anomaly_sensor,
    source_availability_sensor,
    error_monitoring_sensor,
    system_health_sensor,
    performance_degradation_sensor,
]

# All available sensors
all_sensors = core_sensors + public_sensors + tenant_sensors

__all__ = [
    # Core sensors
    "data_freshness_sensor",
    "data_volume_anomaly_sensor", 
    "source_availability_sensor",
    "error_monitoring_sensor",
    "system_health_sensor",
    "performance_degradation_sensor",
    
    # Sensor collections
    "core_sensors",
    "public_sensors",
    "tenant_sensors", 
    "all_sensors",
]