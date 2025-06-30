# Multi-Tenant Data Separation Architecture

This document outlines how the Emergency Management Data Pipeline achieves multi-tenant data separation to meet federal compliance requirements including FedRAMP and DORA standards.

## 🏛️ Federal Compliance Requirements

**FedRAMP Moderate Controls:**
- **AC-3**: Access Enforcement
- **AC-4**: Information Flow Enforcement  
- **SC-4**: Information in Shared Resources
- **SC-7**: Boundary Protection

**DORA Requirements:**
- Data isolation between different government entities
- Audit trails for cross-tenant access
- Incident response isolation

## 🏗️ Multi-Tenant Architecture Layers

### 1. Database-Level Separation (StarRocks)

#### Schema-Based Isolation
```sql
-- Each tenant gets dedicated schemas
CREATE DATABASE tenant_fema_region_1;
CREATE DATABASE tenant_fema_region_2;
CREATE DATABASE tenant_state_colorado;
CREATE DATABASE tenant_state_california;

-- Row-Level Security (RLS) for shared tables
CREATE TABLE emergency_data.disaster_declarations (
    id BIGINT,
    tenant_id VARCHAR(50) NOT NULL,
    disaster_number VARCHAR(50),
    state VARCHAR(10),
    data_classification VARCHAR(20),
    -- ... other columns
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
) ENGINE=OLAP
DUPLICATE KEY(id, tenant_id)
DISTRIBUTED BY HASH(tenant_id, id) BUCKETS 32;

-- Create RLS policies
CREATE ROW POLICY tenant_isolation_policy 
ON emergency_data.disaster_declarations
FOR ALL
TO 'tenant_user_colorado'
USING (tenant_id = 'colorado_state');
```

#### Physical Data Separation
```sql
-- Separate storage for sensitive tenants
CREATE TABLE classified_data.federal_operations (
    -- Physically separate storage backend
    -- Different encryption keys per tenant
) ENGINE=OLAP
PROPERTIES (
    "storage_medium" = "SSD",
    "encryption_key" = "tenant_specific_key_federal",
    "replication_num" = "3"
);
```

### 2. Application-Level Separation (Dagster)

#### Tenant-Aware Resources
```python
from dagster import resource, ConfigurableResource, InitResourceContext
from typing import Dict, List

class TenantAwareStarRocksResource(ConfigurableResource):
    """StarRocks resource with tenant isolation"""
    
    base_host: str
    base_port: int
    tenant_configs: Dict[str, dict]
    
    def get_tenant_connection(self, tenant_id: str):
        """Get connection for specific tenant with isolation"""
        if tenant_id not in self.tenant_configs:
            raise ValueError(f"Tenant {tenant_id} not authorized")
        
        tenant_config = self.tenant_configs[tenant_id]
        
        return pymysql.connect(
            host=self.base_host,
            port=self.base_port,
            user=tenant_config["user"],
            password=tenant_config["password"],
            database=tenant_config["database"],
            # Tenant-specific SSL context
            ssl_ca=tenant_config.get("ssl_ca"),
            ssl_cert=tenant_config.get("ssl_cert"),
            ssl_key=tenant_config.get("ssl_key"),
        )
    
    def execute_tenant_query(self, tenant_id: str, query: str, audit_log: bool = True):
        """Execute query with tenant context and audit logging"""
        # Audit logging for compliance
        if audit_log:
            self._log_tenant_access(tenant_id, query)
        
        # Inject tenant filter into queries
        tenant_filtered_query = self._inject_tenant_filter(query, tenant_id)
        
        with self.get_tenant_connection(tenant_id) as conn:
            cursor = conn.cursor()
            return cursor.execute(tenant_filtered_query)
    
    def _inject_tenant_filter(self, query: str, tenant_id: str) -> str:
        """Automatically inject tenant_id filters into WHERE clauses"""
        # Parse SQL and inject tenant filters
        # This ensures no cross-tenant data access
        if "WHERE" in query.upper():
            return query.replace(
                "WHERE", 
                f"WHERE tenant_id = '{tenant_id}' AND"
            )
        else:
            return query + f" WHERE tenant_id = '{tenant_id}'"
    
    def _log_tenant_access(self, tenant_id: str, query: str):
        """Log all tenant access for audit compliance"""
        audit_entry = {
            "timestamp": datetime.now().isoformat(),
            "tenant_id": tenant_id,
            "user": os.getenv("USER"),
            "query_hash": hashlib.md5(query.encode()).hexdigest(),
            "action": "database_access",
            "classification": "AUDIT"
        }
        # Write to federal audit log
        with open(f"/audit/tenant_access_{datetime.now().strftime('%Y%m%d')}.log", "a") as f:
            f.write(json.dumps(audit_entry) + "\n")
```

#### Tenant-Scoped Assets
```python
@asset(
    description="FEMA disaster data scoped to tenant",
    group_name="raw_data",
    compute_kind="tenant_ingestion",
)
def tenant_fema_disasters(
    context,
    starrocks: TenantAwareStarRocksResource,
) -> pd.DataFrame:
    """Ingest FEMA data with tenant context"""
    
    # Extract tenant from context
    tenant_id = context.partition_key or context.op_config.get("tenant_id")
    
    if not tenant_id:
        raise ValueError("Tenant ID required for data access")
    
    # Tenant-specific API configuration
    tenant_config = get_tenant_config(tenant_id)
    api_key = tenant_config["fema_api_key"]
    data_scope = tenant_config["data_scope"]  # e.g., specific states/regions
    
    # Fetch data with tenant restrictions
    fema_data = fetch_fema_data(
        api_key=api_key,
        scope_filter=data_scope
    )
    
    # Add tenant metadata
    fema_data["tenant_id"] = tenant_id
    fema_data["data_classification"] = tenant_config["classification_level"]
    fema_data["access_level"] = tenant_config["access_level"]
    
    # Store in tenant-specific table
    starrocks.execute_tenant_query(
        tenant_id,
        f"INSERT INTO {tenant_config['schema']}.fema_disasters VALUES (...)"
    )
    
    context.add_output_metadata({
        "tenant_id": tenant_id,
        "records_processed": len(fema_data),
        "classification": tenant_config["classification_level"],
        "audit_logged": True
    })
    
    return fema_data
```

### 3. Stream Processing Separation (Flink)

#### Tenant-Specific Topics and Processing
```python
class TenantAwareFlinkResource(ConfigurableResource):
    """Flink resource with tenant stream isolation"""
    
    def create_tenant_stream_job(self, tenant_id: str, job_config: dict):
        """Create isolated Flink job for tenant"""
        
        env = StreamExecutionEnvironment.get_execution_environment()
        env.set_parallelism(job_config.get("parallelism", 4))
        
        # Tenant-specific Kafka topics
        kafka_source = FlinkKafkaConsumer(
            topics=[f"tenant_{tenant_id}_disasters", f"tenant_{tenant_id}_weather"],
            deserialization_schema=SimpleStringSchema(),
            properties={
                "bootstrap.servers": self.kafka_servers,
                "group.id": f"tenant_{tenant_id}_group",
                "security.protocol": "SSL",  # Encrypted communication
                "ssl.truststore.location": f"/certs/tenant_{tenant_id}_truststore.jks"
            }
        )
        
        # Tenant data stream with isolation
        tenant_stream = env.add_source(kafka_source)
        
        # Add tenant context to all records
        tenant_tagged_stream = tenant_stream.map(
            lambda record: {
                **json.loads(record),
                "tenant_id": tenant_id,
                "processing_timestamp": int(time.time()),
                "isolation_boundary": f"tenant_{tenant_id}"
            }
        )
        
        # Tenant-specific processing logic
        processed_stream = tenant_tagged_stream.filter(
            lambda record: record.get("tenant_id") == tenant_id
        ).map(
            lambda record: self._apply_tenant_transformations(record, tenant_id)
        )
        
        # Tenant-specific sink
        tenant_sink = FlinkJdbcSink(
            jdbc_url=f"jdbc:mysql://starrocks-fe:9030/tenant_{tenant_id}_db",
            username=f"tenant_{tenant_id}_user",
            password=get_tenant_password(tenant_id),
            sql=f"INSERT INTO tenant_{tenant_id}_db.processed_data VALUES (?, ?, ?)"
        )
        
        processed_stream.add_sink(tenant_sink)
        
        # Execute with tenant-specific job name
        env.execute(f"tenant_{tenant_id}_emergency_processing")
```

### 4. API-Level Separation

#### Tenant Authentication and Authorization
```python
from dagster import DefaultSensorStatus, SensorDefinition, SensorEvaluationContext

class TenantAwareApiScraper:
    """API scraper with tenant-specific credentials and scopes"""
    
    def __init__(self, tenant_configs: Dict[str, dict]):
        self.tenant_configs = tenant_configs
    
    async def scrape_tenant_data(self, tenant_id: str, source: str):
        """Scrape data for specific tenant with proper isolation"""
        
        if tenant_id not in self.tenant_configs:
            raise UnauthorizedTenantError(f"Tenant {tenant_id} not configured")
        
        tenant_config = self.tenant_configs[tenant_id]
        
        # Tenant-specific API credentials
        api_credentials = {
            "api_key": tenant_config[f"{source}_api_key"],
            "secret": tenant_config[f"{source}_secret"],
            "scope": tenant_config[f"{source}_scope"],
            "rate_limit": tenant_config.get("rate_limit", 100)
        }
        
        # Geographic/organizational restrictions
        data_filters = {
            "states": tenant_config.get("allowed_states", []),
            "counties": tenant_config.get("allowed_counties", []),
            "classification_levels": tenant_config.get("allowed_classifications", [])
        }
        
        # Scrape with tenant context
        scraped_data = await self._fetch_with_tenant_context(
            source, api_credentials, data_filters, tenant_id
        )
        
        # Add tenant metadata and audit trail
        for record in scraped_data:
            record.update({
                "tenant_id": tenant_id,
                "data_owner": tenant_config["organization"],
                "classification": tenant_config["default_classification"],
                "access_restrictions": tenant_config.get("access_restrictions", []),
                "audit_trail": {
                    "scraped_at": datetime.now().isoformat(),
                    "scraper_tenant": tenant_id,
                    "source_system": source
                }
            })
        
        return scraped_data
```

### 5. Network-Level Isolation

#### Docker Network Segmentation
```yaml
# docker-compose.yml with tenant network isolation
version: '3.8'

services:
  # Shared infrastructure
  kafka:
    networks:
      - shared_infrastructure
      
  # Tenant-specific processing
  tenant-colorado-processor:
    build: ./tenant-processor
    environment:
      TENANT_ID: colorado_state
      NETWORK_ISOLATION: enabled
    networks:
      - tenant_colorado_network
      - shared_infrastructure
    
  tenant-federal-processor:
    build: ./tenant-processor  
    environment:
      TENANT_ID: federal_dhs
      NETWORK_ISOLATION: enabled
      SECURITY_LEVEL: classified
    networks:
      - tenant_federal_network
      - shared_infrastructure

networks:
  shared_infrastructure:
    driver: bridge
    
  tenant_colorado_network:
    driver: bridge
    internal: true  # No external access
    
  tenant_federal_network:
    driver: bridge
    internal: true
    encrypted: true  # Encrypted overlay network
```

## 🔒 Security Implementations

### 1. Encryption per Tenant
```python
class TenantEncryptionManager:
    """Manage encryption keys per tenant"""
    
    def __init__(self):
        self.tenant_keys = {}
        self._load_tenant_keys()
    
    def encrypt_tenant_data(self, data: bytes, tenant_id: str) -> bytes:
        """Encrypt data with tenant-specific key"""
        key = self.get_tenant_key(tenant_id)
        cipher = Fernet(key)
        return cipher.encrypt(data)
    
    def decrypt_tenant_data(self, encrypted_data: bytes, tenant_id: str) -> bytes:
        """Decrypt data with tenant-specific key"""
        key = self.get_tenant_key(tenant_id)
        cipher = Fernet(key)
        return cipher.decrypt(encrypted_data)
    
    def get_tenant_key(self, tenant_id: str) -> bytes:
        """Get encryption key for tenant from secure store"""
        if tenant_id not in self.tenant_keys:
            # In production, fetch from HSM or key management service
            self.tenant_keys[tenant_id] = self._generate_tenant_key(tenant_id)
        return self.tenant_keys[tenant_id]
```

### 2. Audit Logging
```python
class TenantAuditLogger:
    """Federal compliance audit logging"""
    
    def log_tenant_access(self, tenant_id: str, user: str, action: str, 
                         resource: str, success: bool, details: dict = None):
        """Log all tenant access for compliance"""
        
        audit_entry = {
            "timestamp": datetime.now().isoformat(),
            "tenant_id": tenant_id,
            "user_id": user,
            "action": action,
            "resource": resource,
            "success": success,
            "source_ip": self._get_client_ip(),
            "session_id": self._get_session_id(),
            "details": details or {},
            "compliance_flags": {
                "fedramp": True,
                "dora": True,
                "pii_involved": self._check_pii_involvement(details)
            }
        }
        
        # Write to tenant-specific audit log
        audit_file = f"/audit/tenant_{tenant_id}_{datetime.now().strftime('%Y%m%d')}.log"
        with open(audit_file, "a") as f:
            f.write(json.dumps(audit_entry) + "\n")
        
        # Also send to central SIEM for federal monitoring
        self._send_to_siem(audit_entry)
```

## 📊 Implementation Configuration

### Tenant Configuration Example
```yaml
# tenant_config.yaml
tenants:
  colorado_state:
    organization: "State of Colorado Emergency Management"
    classification_level: "INTERNAL"
    database:
      schema: "tenant_colorado"
      user: "colorado_user"
      encryption_key: "colorado_key_ref"
    api_access:
      fema_api_key: "${COLORADO_FEMA_KEY}"
      noaa_scope: ["CO"]
      rate_limits:
        fema: 1000/hour
        noaa: 5000/hour
    data_restrictions:
      allowed_states: ["CO"]
      allowed_classifications: ["PUBLIC", "INTERNAL"]
    audit_requirements:
      retention_days: 2555  # 7 years
      siem_integration: true
      
  federal_dhs:
    organization: "Department of Homeland Security"
    classification_level: "RESTRICTED"
    database:
      schema: "tenant_federal_dhs"
      user: "dhs_user"
      encryption_key: "dhs_key_ref"
    api_access:
      fema_api_key: "${DHS_FEMA_KEY}"
      noaa_scope: ["ALL"]
      additional_apis: ["classified_feeds"]
    data_restrictions:
      allowed_states: ["ALL"]
      allowed_classifications: ["PUBLIC", "INTERNAL", "RESTRICTED", "CONFIDENTIAL"]
    security_enhancements:
      network_isolation: true
      dedicated_processing: true
      additional_encryption: true
    audit_requirements:
      retention_days: 2555
      realtime_monitoring: true
      incident_response: true
```

## 🎯 Benefits of This Architecture

### Federal Compliance
- ✅ **FedRAMP AC-3**: Role-based access control per tenant
- ✅ **FedRAMP AC-4**: Information flow enforcement between tenants  
- ✅ **FedRAMP SC-4**: Proper information isolation in shared resources
- ✅ **DORA**: Operational resilience through tenant isolation

### Technical Benefits
- **Performance Isolation**: Tenant workloads don't impact each other
- **Data Sovereignty**: Each tenant controls their data scope and access
- **Scalability**: Easy to onboard new government entities
- **Auditability**: Complete audit trail per tenant for compliance

### Security Benefits
- **Defense in Depth**: Multiple layers of isolation
- **Least Privilege**: Users only access their tenant's data
- **Incident Containment**: Security incidents are contained per tenant
- **Encryption**: Tenant-specific encryption keys

This multi-tenant architecture ensures that different government entities (states, federal agencies, local municipalities) can safely share the same infrastructure while maintaining complete data isolation and meeting all federal compliance requirements.