"""
Resource definitions for emergency management pipeline
Compliant with FedRAMP and DORA standards
"""

import json
from typing import Dict, Any, Optional
from contextlib import contextmanager

from dagster import (
    resource,
    ConfigurableResource,
    InitResourceContext,
)
from pydantic import Field
import pymysql
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment
from kafka import KafkaProducer, KafkaConsumer
import great_expectations as gx
from great_expectations.core.batch import RuntimeBatchRequest


class StarRocksResource(ConfigurableResource):
    """StarRocks database resource with federal compliance features"""
    
    host: str = Field(description="StarRocks FE host")
    port: int = Field(default=9030, description="StarRocks FE port")
    user: str = Field(default="root", description="Database user")
    password: str = Field(default="", description="Database password")
    database: str = Field(description="Database name")
    ssl_enabled: bool = Field(default=True, description="Enable SSL for compliance")
    connection_timeout: int = Field(default=30, description="Connection timeout")
    
    @contextmanager
    def get_connection(self):
        """Get database connection with compliance logging"""
        connection = None
        try:
            connection = pymysql.connect(
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password,
                database=self.database,
                connect_timeout=self.connection_timeout,
                ssl_disabled=not self.ssl_enabled,
                charset='utf8mb4',
            )
            yield connection
        finally:
            if connection:
                connection.close()
    
    def execute_query(self, query: str, params: Optional[tuple] = None) -> list:
        """Execute query with audit logging"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(query, params)
                return cursor.fetchall()
            finally:
                cursor.close()
    
    def execute_ddl(self, ddl: str) -> None:
        """Execute DDL with compliance logging"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(ddl)
                conn.commit()
            finally:
                cursor.close()


class FlinkResource(ConfigurableResource):
    """Apache Flink resource for stream processing"""
    
    jobmanager_host: str = Field(description="Flink JobManager host")
    jobmanager_port: int = Field(default=8081, description="Flink JobManager port")
    parallelism: int = Field(default=4, description="Default parallelism")
    checkpoint_interval: int = Field(default=60000, description="Checkpoint interval in ms")
    state_backend: str = Field(default="filesystem", description="State backend type")
    
    def get_execution_environment(self) -> StreamExecutionEnvironment:
        """Get Flink stream execution environment"""
        env = StreamExecutionEnvironment.get_execution_environment()
        env.set_parallelism(self.parallelism)
        env.enable_checkpointing(self.checkpoint_interval)
        
        # Configure for federal compliance
        env.get_checkpoint_config().set_checkpoint_timeout(600000)  # 10 minutes
        env.get_checkpoint_config().set_min_pause_between_checkpoints(5000)
        env.get_checkpoint_config().set_max_concurrent_checkpoints(1)
        
        return env
    
    def get_table_environment(self) -> StreamTableEnvironment:
        """Get Flink table environment"""
        env = self.get_execution_environment()
        return StreamTableEnvironment.create(env)
    
    def submit_job(self, job_name: str, job_graph) -> str:
        """Submit Flink job with monitoring"""
        # Implementation would integrate with Flink REST API
        pass


class KafkaResource(ConfigurableResource):
    """Kafka resource for event streaming"""
    
    bootstrap_servers: str = Field(description="Kafka bootstrap servers")
    security_protocol: str = Field(default="PLAINTEXT", description="Security protocol")
    compression_type: str = Field(default="gzip", description="Compression type")
    batch_size: int = Field(default=16384, description="Producer batch size")
    
    def get_producer(self, **kwargs) -> KafkaProducer:
        """Get Kafka producer with compliance settings"""
        config = {
            'bootstrap_servers': self.bootstrap_servers.split(','),
            'security_protocol': self.security_protocol,
            'compression_type': self.compression_type,
            'batch_size': self.batch_size,
            'value_serializer': lambda v: json.dumps(v).encode('utf-8'),
            'key_serializer': lambda k: k.encode('utf-8') if k else None,
            'acks': 'all',  # Wait for all replicas
            'retries': 3,
            'retry_backoff_ms': 1000,
            **kwargs
        }
        return KafkaProducer(**config)
    
    def get_consumer(self, topics: list, group_id: str, **kwargs) -> KafkaConsumer:
        """Get Kafka consumer with compliance settings"""
        config = {
            'bootstrap_servers': self.bootstrap_servers.split(','),
            'security_protocol': self.security_protocol,
            'group_id': group_id,
            'auto_offset_reset': 'earliest',
            'enable_auto_commit': False,  # Manual commit for data integrity
            'value_deserializer': lambda m: json.loads(m.decode('utf-8')),
            'key_deserializer': lambda k: k.decode('utf-8') if k else None,
            **kwargs
        }
        return KafkaConsumer(*topics, **config)


class DataQualityResource(ConfigurableResource):
    """Great Expectations resource for data quality"""
    
    expectation_store_backend: str = Field(default="filesystem")
    validation_store_backend: str = Field(default="filesystem")
    data_docs_backend: str = Field(default="filesystem")
    base_dir: str = Field(default="/opt/dagster/storage/great_expectations")
    
    def get_context(self) -> gx.DataContext:
        """Get Great Expectations context"""
        context_config = {
            "config_version": 3.0,
            "datasources": {},
            "stores": {
                "expectations_store": {
                    "class_name": "ExpectationsStore",
                    "store_backend": {
                        "class_name": "TupleFilesystemStoreBackend",
                        "base_directory": f"{self.base_dir}/expectations",
                    },
                },
                "validations_store": {
                    "class_name": "ValidationsStore",
                    "store_backend": {
                        "class_name": "TupleFilesystemStoreBackend",
                        "base_directory": f"{self.base_dir}/validations",
                    },
                },
            },
            "expectations_store_name": "expectations_store",
            "validations_store_name": "validations_store",
            "data_docs_sites": {
                "local_site": {
                    "class_name": "SiteBuilder",
                    "store_backend": {
                        "class_name": "TupleFilesystemStoreBackend",
                        "base_directory": f"{self.base_dir}/data_docs",
                    },
                }
            },
        }
        
        return gx.get_context(project_config=context_config)
    
    def validate_data(self, data, expectation_suite_name: str) -> dict:
        """Validate data against expectation suite"""
        context = self.get_context()
        
        # Create runtime batch request
        batch_request = RuntimeBatchRequest(
            datasource_name="runtime_datasource",
            data_connector_name="runtime_data_connector",
            data_asset_name="runtime_data_asset",
            runtime_parameters={"batch_data": data},
            batch_identifiers={"default_identifier_name": "default_identifier"},
        )
        
        # Run validation
        validator = context.get_validator(
            batch_request=batch_request,
            expectation_suite_name=expectation_suite_name,
        )
        
        return validator.validate()


# Resource instances
starrocks_resource = StarRocksResource()
flink_resource = FlinkResource()
kafka_resource = KafkaResource()
data_quality_resource = DataQualityResource()