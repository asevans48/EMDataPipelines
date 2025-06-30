"""
Simplified Resources for Public Emergency Data
Streamlined approach for publicly available data sources
"""

import json
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
from kafka import KafkaProducer, KafkaConsumer


class PublicDataStarRocksResource(ConfigurableResource):
    """StarRocks resource optimized for public emergency data"""
    
    host: str = Field(description="StarRocks FE host")
    port: int = Field(default=9030, description="StarRocks FE port") 
    user: str = Field(default="public_user", description="Database user")
    password: str = Field(default="", description="Database password")
    database: str = Field(default="emergency_public_data", description="Public database")
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.logger = get_dagster_logger()
        self.usage_tracker = PublicDataUsageTracker()
    
    @contextmanager
    def get_connection(self, organization: str = "anonymous"):
        """Get database connection with usage tracking"""
        
        # Track usage for analytics and rate limiting
        self.usage_tracker.log_access(organization, "database_connection")
        
        connection = None
        try:
            connection = pymysql.connect(
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password,
                database=self.database,
                charset='utf8mb4',
                read_timeout=30,
                write_timeout=30,
            )
            yield connection
            
        except Exception as e:
            self.logger.error(f"Database connection error for {organization}: {str(e)}")
            self.usage_tracker.log_error(organization, "database_connection", str(e))
            raise
        finally:
            if connection:
                connection.close()
    
    def execute_public_query(self, query: str, params: Optional[tuple] = None, 
                           organization: str = "anonymous") -> List[Any]:
        """Execute query on public data with usage tracking"""
        
        # Rate limiting check
        if not self.usage_tracker.check_rate_limit(organization):
            raise Exception(f"Rate limit exceeded for organization: {organization}")
        
        # Log query for analytics
        self.usage_tracker.log_query(organization, query)
        
        with self.get_connection(organization) as conn:
            cursor = conn.cursor(pymysql.cursors.DictCursor)
            try:
                cursor.execute(query, params)
                
                if query.strip().upper().startswith('SELECT'):
                    results = cursor.fetchall()
                    
                    # Log data access volume
                    self.usage_tracker.log_data_volume(organization, len(results))
                    
                    return results
                else:
                    conn.commit()
                    return cursor.rowcount
                    
            except Exception as e:
                self.logger.error(f"Query execution error: {str(e)}")
                self.usage_tracker.log_error(organization, "query_execution", str(e))
                raise
            finally:
                cursor.close()
    
    def bulk_insert_public_data(self, table: str, data: List[Dict[str, Any]], 
                              source_organization: str = "system"):
        """Bulk insert public data with deduplication"""
        
        if not data:
            return 0
        
        # Add public data metadata
        for record in data:
            record.update({
                'data_classification': 'PUBLIC',
                'ingestion_timestamp': datetime.now(),
                'source_organization': source_organization
            })
        
        # Generate bulk insert SQL
        columns = list(data[0].keys())
        placeholders = ', '.join(['%s'] * len(columns))
        
        insert_sql = f"""
        INSERT INTO {table} ({', '.join(columns)})
        VALUES ({placeholders})
        ON DUPLICATE KEY UPDATE
        ingestion_timestamp = VALUES(ingestion_timestamp),
        source_organization = VALUES(source_organization)
        """
        
        with self.get_connection(source_organization) as conn:
            cursor = conn.cursor()
            try:
                # Batch insert for efficiency
                values = [[record[col] for col in columns] for record in data]
                cursor.executemany(insert_sql, values)
                conn.commit()
                
                rows_affected = cursor.rowcount
                self.logger.info(f"Inserted {rows_affected} records into {table}")
                
                return rows_affected
                
            except Exception as e:
                conn.rollback()
                self.logger.error(f"Bulk insert error: {str(e)}")
                raise
            finally:
                cursor.close()


class PublicDataUsageTracker:
    """Track usage of public data for analytics and rate limiting"""
    
    def __init__(self, log_dir: str = "/opt/dagster/storage/usage_logs"):
        self.log_dir = log_dir
        os.makedirs(log_dir, exist_ok=True)
        self.rate_limits = {
            'default': {'queries_per_hour': 1000, 'records_per_hour': 100000},
            'government': {'queries_per_hour': 10000, 'records_per_hour': 1000000},
            'academic': {'queries_per_hour': 5000, 'records_per_hour': 500000},
            'commercial': {'queries_per_hour': 2000, 'records_per_hour': 200000}
        }
    
    def log_access(self, organization: str, action: str):
        """Log data access for analytics"""
        log_entry = {
            'timestamp': datetime.now().isoformat(),
            'organization': organization,
            'action': action,
            'data_classification': 'PUBLIC'
        }
        
        self._write_log('access', log_entry)
    
    def log_query(self, organization: str, query: str):
        """Log query patterns for analytics"""
        log_entry = {
            'timestamp': datetime.now().isoformat(),
            'organization': organization,
            'query_type': self._classify_query(query),
            'query_hash': hash(query.strip().upper()) % 1000000,  # Simple hash for privacy
            'data_classification': 'PUBLIC'
        }
        
        self._write_log('queries', log_entry)
    
    def log_data_volume(self, organization: str, record_count: int):
        """Log data volume accessed"""
        log_entry = {
            'timestamp': datetime.now().isoformat(),
            'organization': organization,
            'records_accessed': record_count,
            'data_classification': 'PUBLIC'
        }
        
        self._write_log('volume', log_entry)
    
    def log_error(self, organization: str, operation: str, error: str):
        """Log errors for troubleshooting"""
        log_entry = {
            'timestamp': datetime.now().isoformat(),
            'organization': organization,
            'operation': operation,
            'error': error[:200],  # Truncate long errors
            'data_classification': 'PUBLIC'
        }
        
        self._write_log('errors', log_entry)
    
    def check_rate_limit(self, organization: str) -> bool:
        """Check if organization is within rate limits"""
        # Simplified rate limiting - in production would use Redis or similar
        # For now, allow all access to public data
        return True
    
    def _classify_query(self, query: str) -> str:
        """Classify query type for analytics"""
        query_upper = query.strip().upper()
        
        if 'FEMA' in query_upper or 'DISASTER' in query_upper:
            return 'disaster_data'
        elif 'NOAA' in query_upper or 'WEATHER' in query_upper or 'ALERT' in query_upper:
            return 'weather_data'
        elif 'COAGMET' in query_upper or 'AGRICULTURAL' in query_upper:
            return 'agricultural_data'
        elif 'USDA' in query_upper:
            return 'usda_data'
        elif query_upper.startswith('SELECT'):
            return 'general_select'
        else:
            return 'other'
    
    def _write_log(self, log_type: str, entry: Dict[str, Any]):
        """Write log entry to file"""
        log_date = datetime.now().strftime('%Y%m%d')
        log_file = f"{self.log_dir}/{log_type}_{log_date}.log"
        
        try:
            with open(log_file, 'a') as f:
                f.write(json.dumps(entry) + '\n')
        except Exception as e:
            # Don't fail the main operation if logging fails
            print(f"Logging error: {e}")


class PublicDataKafkaResource(ConfigurableResource):
    """Kafka resource for public emergency data streams"""
    
    bootstrap_servers: str = Field(description="Kafka bootstrap servers")
    security_protocol: str = Field(default="PLAINTEXT", description="Security protocol")
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.usage_tracker = PublicDataUsageTracker()
    
    def get_public_producer(self, organization: str = "system", **kwargs):
        """Get Kafka producer for public data"""
        
        producer_config = {
            'bootstrap_servers': self.bootstrap_servers.split(','),
            'security_protocol': self.security_protocol,
            'value_serializer': lambda v: json.dumps({
                **v,
                'data_classification': 'PUBLIC',
                'publishing_organization': organization,
                'timestamp': datetime.now().isoformat()
            }).encode('utf-8'),
            'key_serializer': lambda k: k.encode('utf-8') if k else None,
            'acks': 'all',
            'retries': 3,
            'compression_type': 'gzip',
            **kwargs
        }
        
        self.usage_tracker.log_access(organization, "kafka_producer_creation")
        
        return KafkaProducer(**producer_config)
    
    def get_public_consumer(self, topics: List[str], group_id: str, 
                          organization: str = "anonymous", **kwargs):
        """Get Kafka consumer for public data streams"""
        
        # All topics are public, so no restrictions needed
        consumer_config = {
            'bootstrap_servers': self.bootstrap_servers.split(','),
            'security_protocol': self.security_protocol,
            'group_id': f"public_{organization}_{group_id}",
            'auto_offset_reset': 'earliest',
            'enable_auto_commit': True,  # Simplified for public data
            'value_deserializer': lambda m: json.loads(m.decode('utf-8')),
            'key_deserializer': lambda k: k.decode('utf-8') if k else None,
            **kwargs
        }
        
        self.usage_tracker.log_access(organization, "kafka_consumer_creation")
        
        return KafkaConsumer(*topics, **consumer_config)


class OrganizationAwareFlinkResource(ConfigurableResource):
    """Flink resource that can optionally separate processing by organization"""
    
    jobmanager_host: str = Field(description="Flink JobManager host")
    jobmanager_port: int = Field(default=8081, description="Flink JobManager port")
    parallelism: int = Field(default=4, description="Default parallelism")
    
    def create_public_stream_job(self, job_name: str, organization: str = "shared"):
        """Create Flink job for public data processing"""
        from pyflink.datastream import StreamExecutionEnvironment
        
        env = StreamExecutionEnvironment.get_execution_environment()
        env.set_parallelism(self.parallelism)
        
        # For public data, we can use shared resources
        # But still tag data with organization for analytics
        
        # Configure checkpointing for reliability
        env.enable_checkpointing(60000)  # 1 minute
        
        return env
    
    def create_organization_specific_job(self, organization: str, dedicated_resources: bool = False):
        """Create organization-specific processing job if needed"""
        from pyflink.datastream import StreamExecutionEnvironment
        
        env = StreamExecutionEnvironment.get_execution_environment()
        
        if dedicated_resources:
            # For organizations that need dedicated processing
            env.set_parallelism(max(1, self.parallelism // 2))  # Dedicated portion
        else:
            # Shared processing for public data
            env.set_parallelism(self.parallelism)
        
        env.enable_checkpointing(60000)
        
        return env