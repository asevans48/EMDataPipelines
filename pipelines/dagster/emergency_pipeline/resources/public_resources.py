"""
Public Resources for Emergency Management Pipeline  
Simplified resources optimized for public emergency data access
"""

import json
import os
import pymysql
from datetime import datetime
from typing import Dict, Any, List, Optional
from contextlib import contextmanager

from kafka import KafkaProducer, KafkaConsumer

from dagster import (
    ConfigurableResource,
    get_dagster_logger,
    EnvVar,
)
from pydantic import Field


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
    
    def get_public_data_stats(self) -> Dict[str, Any]:
        """Get statistics about public data availability"""
        stats = {
            "timestamp": datetime.now().isoformat(),
            "tables": {},
            "total_records": 0,
            "data_freshness": {}
        }
        
        # Check standard public tables
        public_tables = [
            "fema_disaster_declarations",
            "noaa_weather_alerts", 
            "coagmet_weather_data",
            "usda_agricultural_data"
        ]
        
        for table in public_tables:
            try:
                # Get record count
                count_query = f"SELECT COUNT(*) as count FROM {table}"
                count_result = self.execute_public_query(count_query, organization="system")
                record_count = count_result[0]["count"] if count_result else 0
                
                # Get latest update
                freshness_query = f"SELECT MAX(ingestion_timestamp) as latest FROM {table}"
                freshness_result = self.execute_public_query(freshness_query, organization="system")
                latest_update = freshness_result[0]["latest"] if freshness_result else None
                
                stats["tables"][table] = {
                    "record_count": record_count,
                    "latest_update": latest_update.isoformat() if latest_update else None
                }
                stats["total_records"] += record_count
                
                if latest_update:
                    hours_old = (datetime.now() - latest_update).total_seconds() / 3600
                    stats["data_freshness"][table] = f"{hours_old:.1f} hours"
                
            except Exception as e:
                self.logger.warning(f"Error getting stats for {table}: {str(e)}")
                stats["tables"][table] = {"error": str(e)}
        
        return stats


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
    
    def get_usage_metrics(self, hours: int = 24) -> Dict[str, Any]:
        """Get usage metrics for the specified time period"""
        return {
            "time_period_hours": hours,
            "total_requests": 1000,  # Simulated
            "unique_organizations": 25,  # Simulated
            "top_query_types": {
                "disaster_data": 45,
                "weather_data": 35, 
                "agricultural_data": 15,
                "general_select": 5
            },
            "error_rate_percent": 2.1
        }
    
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
    
    def send_public_data(self, topic: str, data: Dict[str, Any], 
                        key: Optional[str] = None, organization: str = "system") -> bool:
        """Send public data to Kafka topic"""
        
        try:
            producer = self.get_public_producer(organization)
            
            # Enhance data with public metadata
            enhanced_data = {
                **data,
                'data_classification': 'PUBLIC',
                'source_organization': organization,
                'pipeline_timestamp': datetime.now().isoformat()
            }
            
            future = producer.send(topic, value=enhanced_data, key=key)
            record_metadata = future.get(timeout=10)
            
            producer.close()
            
            self.usage_tracker.log_access(organization, f"kafka_send_{topic}")
            return True
            
        except Exception as e:
            self.usage_tracker.log_error(organization, f"kafka_send_{topic}", str(e))
            return False
    
    def setup_public_topics(self) -> Dict[str, bool]:
        """Setup standard public emergency data topics"""
        
        topics_status = {}
        
        # Standard public emergency topics
        public_topics = [
            'fema_disasters',
            'noaa_weather_alerts',
            'coagmet_weather', 
            'usda_agricultural_data',
            'emergency_events_unified',
            'public_api_metrics'
        ]
        
        # In a real implementation, this would use KafkaAdminClient
        # For now, simulate topic creation
        for topic in public_topics:
            try:
                # Simulate topic creation
                topics_status[topic] = True
                self.usage_tracker.log_access("system", f"topic_creation_{topic}")
            except Exception as e:
                topics_status[topic] = False
                self.usage_tracker.log_error("system", f"topic_creation_{topic}", str(e))
        
        return topics_status


class OrganizationAwareFlinkResource(ConfigurableResource):
    """Flink resource that can optionally separate processing by organization"""
    
    jobmanager_host: str = Field(description="Flink JobManager host")
    jobmanager_port: int = Field(default=8081, description="Flink JobManager port")
    parallelism: int = Field(default=4, description="Default parallelism")
    
    def create_public_stream_job(self, job_name: str, organization: str = "shared"):
        """Create Flink job for public data processing"""
        # This would use actual PyFlink in production
        
        job_config = {
            'job_name': job_name,
            'organization': organization,
            'data_classification': 'PUBLIC',
            'parallelism': self.parallelism,
            'checkpointing_interval': 60000,  # 1 minute
            'restart_strategy': 'fixed-delay'
        }
        
        # For public data, we can use shared resources
        # But still tag data with organization for analytics
        
        # Simulate job creation
        job_id = f"job_{job_name}_{organization}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        return {
            'job_id': job_id,
            'job_config': job_config,
            'status': 'created',
            'public_accessible': True
        }
    
    def create_organization_specific_job(self, organization: str, dedicated_resources: bool = False):
        """Create organization-specific processing job if needed"""
        
        job_config = {
            'organization': organization,
            'dedicated_resources': dedicated_resources,
            'parallelism': max(1, self.parallelism // 2) if dedicated_resources else self.parallelism,
            'checkpointing_interval': 60000,
            'resource_isolation': dedicated_resources
        }
        
        # Simulate job creation
        job_id = f"org_{organization}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        return {
            'job_id': job_id, 
            'job_config': job_config,
            'status': 'created'
        }
    
    def get_public_job_metrics(self) -> Dict[str, Any]:
        """Get metrics for public data processing jobs"""
        return {
            'active_jobs': 4,
            'total_records_processed': 15000,
            'average_latency_ms': 250,
            'throughput_records_per_sec': 100,
            'error_rate_percent': 0.5,
            'public_jobs': {
                'fema_stream': {'status': 'running', 'records_processed': 5000},
                'noaa_stream': {'status': 'running', 'records_processed': 8000},
                'coagmet_stream': {'status': 'running', 'records_processed': 1500},
                'usda_stream': {'status': 'running', 'records_processed': 500}
            }
        }


class PublicAPIOptimizationResource(ConfigurableResource):
    """Resource for optimizing public API performance and caching"""
    
    cache_ttl_seconds: int = Field(default=300, description="Cache TTL in seconds")
    max_cache_size_mb: int = Field(default=512, description="Maximum cache size in MB")
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.logger = get_dagster_logger()
        self.usage_tracker = PublicDataUsageTracker()
        self._cache = {}  # Simple in-memory cache for development
    
    def cache_public_data(self, cache_key: str, data: Any, ttl_seconds: Optional[int] = None) -> bool:
        """Cache public data for API performance"""
        
        try:
            ttl = ttl_seconds or self.cache_ttl_seconds
            cache_entry = {
                'data': data,
                'timestamp': datetime.now(),
                'ttl_seconds': ttl,
                'classification': 'PUBLIC'
            }
            
            self._cache[cache_key] = cache_entry
            self.usage_tracker.log_access("system", f"cache_write_{cache_key}")
            
            return True
            
        except Exception as e:
            self.logger.error(f"Cache write error for {cache_key}: {str(e)}")
            return False
    
    def get_cached_public_data(self, cache_key: str) -> Optional[Any]:
        """Retrieve cached public data"""
        
        try:
            if cache_key not in self._cache:
                return None
            
            entry = self._cache[cache_key]
            
            # Check if cache entry is still valid
            age_seconds = (datetime.now() - entry['timestamp']).total_seconds()
            if age_seconds > entry['ttl_seconds']:
                del self._cache[cache_key]
                return None
            
            self.usage_tracker.log_access("system", f"cache_hit_{cache_key}")
            return entry['data']
            
        except Exception as e:
            self.logger.error(f"Cache read error for {cache_key}: {str(e)}")
            return None
    
    def warm_public_api_cache(self, starrocks_resource: PublicDataStarRocksResource) -> Dict[str, bool]:
        """Warm cache with frequently accessed public data"""
        
        cache_warming_results = {}
        
        # Common queries to cache
        cache_queries = [
            {
                'key': 'recent_disasters',
                'query': """
                SELECT * FROM fema_disaster_declarations 
                WHERE declaration_date >= DATE_SUB(NOW(), INTERVAL 30 DAY)
                ORDER BY declaration_date DESC LIMIT 100
                """,
                'ttl': 1800  # 30 minutes
            },
            {
                'key': 'active_weather_alerts', 
                'query': """
                SELECT * FROM noaa_weather_alerts
                WHERE expires > NOW() OR expires IS NULL
                ORDER BY severity DESC, effective DESC LIMIT 50
                """,
                'ttl': 300  # 5 minutes
            },
            {
                'key': 'disaster_summary_by_state',
                'query': """
                SELECT state, COUNT(*) as disaster_count, 
                       MAX(declaration_date) as latest_disaster
                FROM fema_disaster_declarations
                WHERE declaration_date >= DATE_SUB(NOW(), INTERVAL 365 DAY)
                GROUP BY state
                ORDER BY disaster_count DESC
                """,
                'ttl': 3600  # 1 hour
            }
        ]
        
        for cache_spec in cache_queries:
            try:
                # Execute query and cache results
                data = starrocks_resource.execute_public_query(
                    cache_spec['query'], 
                    organization="cache_warming"
                )
                
                success = self.cache_public_data(
                    cache_spec['key'], 
                    data, 
                    cache_spec['ttl']
                )
                
                cache_warming_results[cache_spec['key']] = success
                
                if success:
                    self.logger.info(f"Cache warmed for {cache_spec['key']}: {len(data)} records")
                
            except Exception as e:
                self.logger.error(f"Cache warming failed for {cache_spec['key']}: {str(e)}")
                cache_warming_results[cache_spec['key']] = False
        
        return cache_warming_results
    
    def get_cache_metrics(self) -> Dict[str, Any]:
        """Get cache performance metrics"""
        
        current_time = datetime.now()
        valid_entries = 0
        expired_entries = 0
        total_size_estimate = 0
        
        for key, entry in self._cache.items():
            age_seconds = (current_time - entry['timestamp']).total_seconds()
            
            if age_seconds <= entry['ttl_seconds']:
                valid_entries += 1
            else:
                expired_entries += 1
            
            # Rough size estimate
            total_size_estimate += len(str(entry['data']))
        
        return {
            'cache_entries': len(self._cache),
            'valid_entries': valid_entries,
            'expired_entries': expired_entries,
            'estimated_size_bytes': total_size_estimate,
            'cache_hit_rate': 85.5,  # Simulated
            'average_response_improvement_ms': 125  # Simulated
        }