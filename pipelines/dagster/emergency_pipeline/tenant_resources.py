"""
Tenant-Aware Resources for Multi-Tenant Data Separation
Implements federal compliance with data isolation
"""

import json
import hashlib
import os
from datetime import datetime
from typing import Dict, Any, List, Optional
from contextlib import contextmanager

import pymysql
from dagster import (
    ConfigurableResource,
    get_dagster_logger,
    OpExecutionContext,
)
from pydantic import Field
from cryptography.fernet import Fernet
import yaml


class TenantConfig:
    """Tenant configuration management"""
    
    def __init__(self, config_path: str = "/opt/dagster/app/tenant_config.yaml"):
        self.config_path = config_path
        self.tenant_configs = self._load_tenant_configs()
    
    def _load_tenant_configs(self) -> Dict[str, Any]:
        """Load tenant configurations from secure file"""
        try:
            with open(self.config_path, 'r') as f:
                config = yaml.safe_load(f)
                return config.get('tenants', {})
        except FileNotFoundError:
            # Return default configuration for development
            return self._get_default_config()
    
    def _get_default_config(self) -> Dict[str, Any]:
        """Default tenant configuration for development"""
        return {
            "colorado_state": {
                "organization": "State of Colorado Emergency Management",
                "classification_level": "INTERNAL",
                "database": {
                    "schema": "tenant_colorado",
                    "user": "colorado_user",
                    "password": os.getenv("COLORADO_DB_PASSWORD", "colorado123"),
                },
                "api_access": {
                    "fema_scope": ["CO"],
                    "noaa_scope": ["CO"],
                    "rate_limits": {"fema": 1000, "noaa": 5000}
                },
                "data_restrictions": {
                    "allowed_states": ["CO"],
                    "allowed_classifications": ["PUBLIC", "INTERNAL"]
                },
                "audit_requirements": {
                    "retention_days": 2555,
                    "siem_integration": True
                }
            },
            "federal_dhs": {
                "organization": "Department of Homeland Security", 
                "classification_level": "RESTRICTED",
                "database": {
                    "schema": "tenant_federal_dhs",
                    "user": "dhs_user", 
                    "password": os.getenv("DHS_DB_PASSWORD", "dhs456"),
                },
                "api_access": {
                    "fema_scope": ["ALL"],
                    "noaa_scope": ["ALL"],
                    "rate_limits": {"fema": 10000, "noaa": 50000}
                },
                "data_restrictions": {
                    "allowed_states": ["ALL"],
                    "allowed_classifications": ["PUBLIC", "INTERNAL", "RESTRICTED", "CONFIDENTIAL"]
                },
                "security_enhancements": {
                    "network_isolation": True,
                    "dedicated_processing": True,
                    "additional_encryption": True
                },
                "audit_requirements": {
                    "retention_days": 2555,
                    "realtime_monitoring": True,
                    "incident_response": True
                }
            }
        }
    
    def get_tenant_config(self, tenant_id: str) -> Dict[str, Any]:
        """Get configuration for specific tenant"""
        if tenant_id not in self.tenant_configs:
            raise ValueError(f"Tenant {tenant_id} not configured")
        return self.tenant_configs[tenant_id]
    
    def get_all_tenants(self) -> List[str]:
        """Get list of all configured tenants"""
        return list(self.tenant_configs.keys())


class TenantAuditLogger:
    """Federal compliance audit logging per tenant"""
    
    def __init__(self, audit_base_dir: str = "/opt/dagster/storage/audit"):
        self.audit_base_dir = audit_base_dir
        os.makedirs(audit_base_dir, exist_ok=True)
    
    def log_tenant_access(self, tenant_id: str, user: str, action: str, 
                         resource: str, success: bool, details: Dict[str, Any] = None):
        """Log tenant access for federal compliance"""
        
        audit_entry = {
            "timestamp": datetime.now().isoformat(),
            "tenant_id": tenant_id,
            "user_id": user,
            "action": action,
            "resource": resource,
            "success": success,
            "source_ip": os.getenv("CLIENT_IP", "unknown"),
            "session_id": os.getenv("SESSION_ID", "unknown"),
            "details": details or {},
            "compliance_flags": {
                "fedramp": True,
                "dora": True,
                "pii_involved": self._check_pii_involvement(details)
            }
        }
        
        # Write to tenant-specific audit log
        audit_date = datetime.now().strftime('%Y%m%d')
        audit_file = f"{self.audit_base_dir}/tenant_{tenant_id}_{audit_date}.log"
        
        with open(audit_file, "a") as f:
            f.write(json.dumps(audit_entry) + "\n")
        
        # Also write to central audit log for federal oversight
        central_audit_file = f"{self.audit_base_dir}/central_audit_{audit_date}.log"
        with open(central_audit_file, "a") as f:
            f.write(json.dumps(audit_entry) + "\n")
    
    def _check_pii_involvement(self, details: Dict[str, Any]) -> bool:
        """Check if operation involves PII data"""
        if not details:
            return False
        
        pii_indicators = [
            "personal_info", "pii", "social_security", "driver_license",
            "personal_name", "address", "phone", "email"
        ]
        
        details_str = json.dumps(details).lower()
        return any(indicator in details_str for indicator in pii_indicators)


class TenantEncryptionManager:
    """Manage encryption keys per tenant"""
    
    def __init__(self, keys_dir: str = "/opt/dagster/storage/keys"):
        self.keys_dir = keys_dir
        self.tenant_keys = {}
        os.makedirs(keys_dir, exist_ok=True)
        self._load_tenant_keys()
    
    def _load_tenant_keys(self):
        """Load or generate tenant-specific encryption keys"""
        for tenant_file in os.listdir(self.keys_dir):
            if tenant_file.endswith('.key'):
                tenant_id = tenant_file.replace('.key', '')
                key_path = os.path.join(self.keys_dir, tenant_file)
                with open(key_path, 'rb') as f:
                    self.tenant_keys[tenant_id] = f.read()
    
    def get_tenant_key(self, tenant_id: str) -> bytes:
        """Get encryption key for tenant"""
        if tenant_id not in self.tenant_keys:
            self.tenant_keys[tenant_id] = self._generate_tenant_key(tenant_id)
        return self.tenant_keys[tenant_id]
    
    def _generate_tenant_key(self, tenant_id: str) -> bytes:
        """Generate new encryption key for tenant"""
        key = Fernet.generate_key()
        key_path = os.path.join(self.keys_dir, f"{tenant_id}.key")
        
        with open(key_path, 'wb') as f:
            f.write(key)
        
        return key
    
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


class TenantAwareStarRocksResource(ConfigurableResource):
    """StarRocks resource with multi-tenant data isolation"""
    
    host: str = Field(description="StarRocks FE host")
    port: int = Field(default=9030, description="StarRocks FE port")
    admin_user: str = Field(default="root", description="Admin user for setup")
    admin_password: str = Field(default="", description="Admin password")
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.tenant_config = TenantConfig()
        self.audit_logger = TenantAuditLogger()
        self.encryption_manager = TenantEncryptionManager()
        self.logger = get_dagster_logger()
    
    @contextmanager
    def get_tenant_connection(self, tenant_id: str, user_context: str = "system"):
        """Get database connection for specific tenant with isolation"""
        tenant_config = self.tenant_config.get_tenant_config(tenant_id)
        db_config = tenant_config["database"]
        
        # Log tenant access
        self.audit_logger.log_tenant_access(
            tenant_id=tenant_id,
            user=user_context,
            action="database_connection",
            resource=f"database:{db_config['schema']}",
            success=True
        )
        
        connection = None
        try:
            connection = pymysql.connect(
                host=self.host,
                port=self.port,
                user=db_config["user"],
                password=db_config["password"],
                database=db_config["schema"],
                charset='utf8mb4',
                autocommit=False,  # Explicit transaction control
            )
            yield connection
            
        except Exception as e:
            self.audit_logger.log_tenant_access(
                tenant_id=tenant_id,
                user=user_context,
                action="database_connection",
                resource=f"database:{db_config['schema']}",
                success=False,
                details={"error": str(e)}
            )
            raise
        finally:
            if connection:
                connection.close()
    
    def execute_tenant_query(self, tenant_id: str, query: str, 
                           params: Optional[tuple] = None, 
                           user_context: str = "system") -> List[Any]:
        """Execute query with tenant context and automatic isolation"""
        
        # Validate tenant authorization
        if not self._validate_tenant_access(tenant_id, user_context):
            raise PermissionError(f"User {user_context} not authorized for tenant {tenant_id}")
        
        # Inject tenant filters for data isolation
        tenant_filtered_query = self._inject_tenant_filter(query, tenant_id)
        
        # Log query execution
        query_hash = hashlib.md5(tenant_filtered_query.encode()).hexdigest()
        self.audit_logger.log_tenant_access(
            tenant_id=tenant_id,
            user=user_context,
            action="query_execution",
            resource=f"query:{query_hash}",
            success=True,
            details={"query_type": self._classify_query(query)}
        )
        
        with self.get_tenant_connection(tenant_id, user_context) as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(tenant_filtered_query, params)
                
                if tenant_filtered_query.strip().upper().startswith('SELECT'):
                    results = cursor.fetchall()
                    
                    # Apply additional tenant filtering to results
                    filtered_results = self._filter_results_by_tenant(results, tenant_id)
                    return filtered_results
                else:
                    conn.commit()
                    return cursor.rowcount
                    
            except Exception as e:
                conn.rollback()
                self.audit_logger.log_tenant_access(
                    tenant_id=tenant_id,
                    user=user_context,
                    action="query_execution",
                    resource=f"query:{query_hash}",
                    success=False,
                    details={"error": str(e), "query_type": self._classify_query(query)}
                )
                raise
            finally:
                cursor.close()
    
    def _inject_tenant_filter(self, query: str, tenant_id: str) -> str:
        """Automatically inject tenant_id filters for data isolation"""
        
        # Skip injection for DDL statements
        if any(query.strip().upper().startswith(ddl) for ddl in ['CREATE', 'DROP', 'ALTER']):
            return query
        
        # For SELECT, UPDATE, DELETE statements, ensure tenant filtering
        if "WHERE" in query.upper():
            return query.replace(
                "WHERE", 
                f"WHERE tenant_id = '{tenant_id}' AND", 
                1  # Only replace first occurrence
            )
        elif any(query.strip().upper().startswith(dml) for dml in ['SELECT', 'UPDATE', 'DELETE']):
            # Add WHERE clause if none exists
            if "FROM" in query.upper():
                from_index = query.upper().find("FROM")
                table_part = query[from_index:].split()[1]  # Get table name
                return query + f" WHERE tenant_id = '{tenant_id}'"
        
        return query
    
    def _validate_tenant_access(self, tenant_id: str, user_context: str) -> bool:
        """Validate user access to tenant data"""
        try:
            tenant_config = self.tenant_config.get_tenant_config(tenant_id)
            # In production, this would check RBAC/LDAP
            # For now, basic validation that tenant exists
            return True
        except ValueError:
            return False
    
    def _classify_query(self, query: str) -> str:
        """Classify query type for audit logging"""
        query_upper = query.strip().upper()
        
        if query_upper.startswith('SELECT'):
            return "READ"
        elif query_upper.startswith(('INSERT', 'UPDATE', 'DELETE')):
            return "WRITE"
        elif query_upper.startswith(('CREATE', 'DROP', 'ALTER')):
            return "DDL"
        else:
            return "OTHER"
    
    def _filter_results_by_tenant(self, results: List[Any], tenant_id: str) -> List[Any]:
        """Apply additional result filtering based on tenant configuration"""
        tenant_config = self.tenant_config.get_tenant_config(tenant_id)
        restrictions = tenant_config.get("data_restrictions", {})
        
        # Apply state-level filtering if configured
        allowed_states = restrictions.get("allowed_states", [])
        if allowed_states and "ALL" not in allowed_states:
            # Filter results based on state restrictions
            # This is a simplified example - in practice, you'd need to know the schema
            filtered_results = []
            for row in results:
                # Assume state is in a predictable position or use column names
                if isinstance(row, dict) and row.get("state") in allowed_states:
                    filtered_results.append(row)
                elif isinstance(row, (list, tuple)) and len(row) > 1:
                    # Assume state is in second column (adjust as needed)
                    if row[1] in allowed_states:
                        filtered_results.append(row)
                else:
                    filtered_results.append(row)  # Default include if unsure
            return filtered_results
        
        return results
    
    def setup_tenant_database(self, tenant_id: str):
        """Set up database schema and user for new tenant"""
        tenant_config = self.tenant_config.get_tenant_config(tenant_id)
        db_config = tenant_config["database"]
        
        with pymysql.connect(
            host=self.host,
            port=self.port, 
            user=self.admin_user,
            password=self.admin_password
        ) as admin_conn:
            cursor = admin_conn.cursor()
            
            # Create tenant database
            cursor.execute(f"CREATE DATABASE IF NOT EXISTS {db_config['schema']}")
            
            # Create tenant user
            cursor.execute(f"""
                CREATE USER IF NOT EXISTS '{db_config['user']}'@'%' 
                IDENTIFIED BY '{db_config['password']}'
            """)
            
            # Grant permissions only to tenant database
            cursor.execute(f"""
                GRANT ALL PRIVILEGES ON {db_config['schema']}.* 
                TO '{db_config['user']}'@'%'
            """)
            
            admin_conn.commit()
            
            self.audit_logger.log_tenant_access(
                tenant_id=tenant_id,
                user="admin",
                action="tenant_setup",
                resource=f"database:{db_config['schema']}",
                success=True,
                details={"action": "database_and_user_created"}
            )


class TenantAwareKafkaResource(ConfigurableResource):
    """Kafka resource with tenant topic isolation"""
    
    bootstrap_servers: str = Field(description="Kafka bootstrap servers")
    security_protocol: str = Field(default="PLAINTEXT", description="Security protocol")
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.tenant_config = TenantConfig()
        self.audit_logger = TenantAuditLogger()
    
    def get_tenant_producer(self, tenant_id: str, **kwargs):
        """Get Kafka producer with tenant-specific configuration"""
        from kafka import KafkaProducer
        
        tenant_config = self.tenant_config.get_tenant_config(tenant_id)
        
        # Tenant-specific producer configuration
        producer_config = {
            'bootstrap_servers': self.bootstrap_servers.split(','),
            'security_protocol': self.security_protocol,
            'value_serializer': lambda v: json.dumps({
                **v,
                'tenant_id': tenant_id,
                'tenant_classification': tenant_config['classification_level']
            }).encode('utf-8'),
            'key_serializer': lambda k: f"tenant_{tenant_id}_{k}".encode('utf-8') if k else None,
            'acks': 'all',
            'retries': 3,
            **kwargs
        }
        
        # Log producer creation
        self.audit_logger.log_tenant_access(
            tenant_id=tenant_id,
            user="system",
            action="kafka_producer_creation",
            resource="kafka_producer",
            success=True
        )
        
        return KafkaProducer(**producer_config)
    
    def get_tenant_consumer(self, tenant_id: str, topics: List[str], group_id: str, **kwargs):
        """Get Kafka consumer with tenant topic restrictions"""
        from kafka import KafkaConsumer
        
        # Ensure only tenant-specific topics
        tenant_topics = [f"tenant_{tenant_id}_{topic}" for topic in topics]
        
        consumer_config = {
            'bootstrap_servers': self.bootstrap_servers.split(','),
            'security_protocol': self.security_protocol,
            'group_id': f"tenant_{tenant_id}_{group_id}",
            'auto_offset_reset': 'earliest',
            'enable_auto_commit': False,  # Manual commit for audit trail
            'value_deserializer': lambda m: json.loads(m.decode('utf-8')),
            'key_deserializer': lambda k: k.decode('utf-8') if k else None,
            **kwargs
        }
        
        self.audit_logger.log_tenant_access(
            tenant_id=tenant_id,
            user="system", 
            action="kafka_consumer_creation",
            resource=f"kafka_topics:{','.join(tenant_topics)}",
            success=True
        )
        
        return KafkaConsumer(*tenant_topics, **consumer_config)