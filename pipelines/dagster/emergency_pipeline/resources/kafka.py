"""
Kafka Resources for Emergency Management Pipeline
Real-time data streaming with federal compliance
"""

import json
from datetime import datetime
from typing import Dict, Any, List, Optional, Callable

from kafka import KafkaProducer, KafkaConsumer, KafkaAdminClient
from kafka.admin import ConfigResource, ConfigResourceType, NewTopic
from kafka.errors import TopicAlreadyExistsError

from dagster import (
    ConfigurableResource,
    get_dagster_logger,
)
from pydantic import Field


class KafkaResource(ConfigurableResource):
    """
    Kafka resource for real-time emergency data streaming
    Handles both public and tenant-isolated data streams
    """
    
    bootstrap_servers: str = Field(description="Kafka bootstrap servers")
    security_protocol: str = Field(default="PLAINTEXT", description="Security protocol")
    client_id: str = Field(default="emergency_pipeline", description="Client ID")
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.logger = get_dagster_logger()
        self._admin_client = None
        self._producer = None
    
    @property
    def admin_client(self) -> KafkaAdminClient:
        """Get Kafka admin client for topic management"""
        if self._admin_client is None:
            self._admin_client = KafkaAdminClient(
                bootstrap_servers=self.bootstrap_servers.split(','),
                security_protocol=self.security_protocol,
                client_id=f"{self.client_id}_admin"
            )
        return self._admin_client
    
    def get_producer(self, **kwargs) -> KafkaProducer:
        """Get Kafka producer with standard configuration"""
        producer_config = {
            'bootstrap_servers': self.bootstrap_servers.split(','),
            'security_protocol': self.security_protocol,
            'client_id': f"{self.client_id}_producer",
            'value_serializer': self._default_value_serializer,
            'key_serializer': self._default_key_serializer,
            'acks': 'all',  # Wait for all replicas
            'retries': 3,
            'compression_type': 'gzip',
            'batch_size': 16384,
            'linger_ms': 10,
            **kwargs
        }
        
        return KafkaProducer(**producer_config)
    
    def get_consumer(self, topics: List[str], group_id: str, **kwargs) -> KafkaConsumer:
        """Get Kafka consumer with standard configuration"""
        consumer_config = {
            'bootstrap_servers': self.bootstrap_servers.split(','),
            'security_protocol': self.security_protocol,
            'client_id': f"{self.client_id}_consumer",
            'group_id': group_id,
            'auto_offset_reset': 'earliest',
            'enable_auto_commit': False,  # Manual commit for data integrity
            'value_deserializer': self._default_value_deserializer,
            'key_deserializer': self._default_key_deserializer,
            'session_timeout_ms': 30000,
            'max_poll_records': 500,
            **kwargs
        }
        
        return KafkaConsumer(*topics, **consumer_config)
    
    def create_topic(self, topic_name: str, num_partitions: int = 3, 
                    replication_factor: int = 1, config: Optional[Dict[str, str]] = None) -> bool:
        """Create Kafka topic with specified configuration"""
        
        topic_config = {
            'cleanup.policy': 'delete',
            'retention.ms': str(7 * 24 * 60 * 60 * 1000),  # 7 days
            'segment.ms': str(24 * 60 * 60 * 1000),  # 1 day
            'compression.type': 'gzip',
        }
        
        if config:
            topic_config.update(config)
        
        new_topic = NewTopic(
            name=topic_name,
            num_partitions=num_partitions,
            replication_factor=replication_factor,
            topic_configs=topic_config
        )
        
        try:
            result = self.admin_client.create_topics([new_topic])
            # Wait for creation to complete
            for topic, future in result.items():
                future.result()  # This will raise an exception if creation failed
                
            self.logger.info(f"Created Kafka topic: {topic_name}")
            return True
            
        except TopicAlreadyExistsError:
            self.logger.info(f"Topic {topic_name} already exists")
            return True
        except Exception as e:
            self.logger.error(f"Error creating topic {topic_name}: {str(e)}")
            return False
    
    def send_message(self, topic: str, value: Dict[str, Any], 
                    key: Optional[str] = None, partition: Optional[int] = None) -> bool:
        """Send message to Kafka topic"""
        
        # Add compliance metadata
        enhanced_value = {
            **value,
            'message_timestamp': datetime.now().isoformat(),
            'data_classification': value.get('data_classification', 'PUBLIC'),
            'pipeline_version': '1.0',
        }
        
        try:
            if self._producer is None:
                self._producer = self.get_producer()
            
            future = self._producer.send(
                topic=topic,
                value=enhanced_value,
                key=key,
                partition=partition
            )
            
            # Wait for send to complete
            record_metadata = future.get(timeout=10)
            
            self.logger.debug(f"Message sent to {topic}:{record_metadata.partition}:{record_metadata.offset}")
            return True
            
        except Exception as e:
            self.logger.error(f"Error sending message to {topic}: {str(e)}")
            return False
    
    def send_batch(self, topic: str, messages: List[Dict[str, Any]], 
                  key_extractor: Optional[Callable] = None) -> int:
        """Send batch of messages to Kafka topic"""
        
        if not messages:
            return 0
        
        sent_count = 0
        
        try:
            if self._producer is None:
                self._producer = self.get_producer()
            
            for message in messages:
                key = key_extractor(message) if key_extractor else None
                
                enhanced_message = {
                    **message,
                    'message_timestamp': datetime.now().isoformat(),
                    'data_classification': message.get('data_classification', 'PUBLIC'),
                    'pipeline_version': '1.0',
                }
                
                self._producer.send(topic, value=enhanced_message, key=key)
                sent_count += 1
            
            # Flush to ensure all messages are sent
            self._producer.flush()
            
            self.logger.info(f"Sent batch of {sent_count} messages to {topic}")
            return sent_count
            
        except Exception as e:
            self.logger.error(f"Error sending batch to {topic}: {str(e)}")
            return sent_count
    
    def consume_messages(self, topics: List[str], group_id: str, 
                        max_messages: int = 1000, timeout_ms: int = 10000) -> List[Dict[str, Any]]:
        """Consume messages from Kafka topics"""
        
        messages = []
        
        try:
            consumer = self.get_consumer(topics, group_id)
            
            # Poll for messages
            message_batch = consumer.poll(timeout_ms=timeout_ms, max_records=max_messages)
            
            for topic_partition, records in message_batch.items():
                for record in records:
                    message_data = {
                        'topic': record.topic,
                        'partition': record.partition,
                        'offset': record.offset,
                        'timestamp': record.timestamp,
                        'key': record.key,
                        'value': record.value,
                        'consumed_at': datetime.now().isoformat()
                    }
                    messages.append(message_data)
            
            # Commit offsets
            consumer.commit()
            consumer.close()
            
            self.logger.info(f"Consumed {len(messages)} messages from {topics}")
            return messages
            
        except Exception as e:
            self.logger.error(f"Error consuming from {topics}: {str(e)}")
            return messages
    
    def setup_emergency_topics(self) -> bool:
        """Set up standard emergency management topics"""
        
        topics_to_create = [
            {
                'name': 'fema_disasters',
                'partitions': 3,
                'config': {
                    'retention.ms': str(30 * 24 * 60 * 60 * 1000),  # 30 days
                    'cleanup.policy': 'delete'
                }
            },
            {
                'name': 'noaa_weather_alerts',
                'partitions': 5,  # More partitions for high-volume weather alerts
                'config': {
                    'retention.ms': str(7 * 24 * 60 * 60 * 1000),  # 7 days
                    'cleanup.policy': 'delete'
                }
            },
            {
                'name': 'coagmet_weather',
                'partitions': 3,
                'config': {
                    'retention.ms': str(7 * 24 * 60 * 60 * 1000),  # 7 days
                    'cleanup.policy': 'delete'
                }
            },
            {
                'name': 'usda_agricultural_data',
                'partitions': 2,
                'config': {
                    'retention.ms': str(30 * 24 * 60 * 60 * 1000),  # 30 days
                    'cleanup.policy': 'delete'
                }
            },
            {
                'name': 'data_quality_metrics',
                'partitions': 1,
                'config': {
                    'retention.ms': str(90 * 24 * 60 * 60 * 1000),  # 90 days
                    'cleanup.policy': 'compact'  # Keep latest metrics
                }
            },
            {
                'name': 'ml_predictions',
                'partitions': 2,
                'config': {
                    'retention.ms': str(30 * 24 * 60 * 60 * 1000),  # 30 days
                    'cleanup.policy': 'delete'
                }
            }
        ]
        
        success_count = 0
        
        for topic_spec in topics_to_create:
            if self.create_topic(
                topic_name=topic_spec['name'],
                num_partitions=topic_spec['partitions'],
                config=topic_spec['config']
            ):
                success_count += 1
        
        self.logger.info(f"Created {success_count}/{len(topics_to_create)} emergency topics")
        return success_count == len(topics_to_create)
    
    def get_topic_metrics(self, topic_name: str) -> Dict[str, Any]:
        """Get metrics for a specific topic"""
        try:
            # Get topic metadata
            metadata = self.admin_client.describe_topics([topic_name])
            topic_meta = metadata[topic_name].result()
            
            # Get consumer group information (simplified)
            consumer_groups = self.admin_client.list_consumer_groups().result()
            
            # Basic topic metrics
            metrics = {
                'topic_name': topic_name,
                'partition_count': len(topic_meta.partitions),
                'consumer_groups': len([g for g in consumer_groups if topic_name in str(g)]),
                'check_timestamp': datetime.now().isoformat()
            }
            
            return metrics
            
        except Exception as e:
            self.logger.error(f"Error getting metrics for topic {topic_name}: {str(e)}")
            return {'topic_name': topic_name, 'error': str(e)}
    
    def _default_value_serializer(self, value: Any) -> bytes:
        """Default value serializer for JSON"""
        if isinstance(value, dict):
            return json.dumps(value, default=str).encode('utf-8')
        elif isinstance(value, str):
            return value.encode('utf-8')
        else:
            return json.dumps(value, default=str).encode('utf-8')
    
    def _default_key_serializer(self, key: Any) -> Optional[bytes]:
        """Default key serializer"""
        if key is None:
            return None
        return str(key).encode('utf-8')
    
    def _default_value_deserializer(self, value: bytes) -> Any:
        """Default value deserializer for JSON"""
        try:
            return json.loads(value.decode('utf-8'))
        except (json.JSONDecodeError, UnicodeDecodeError):
            return value.decode('utf-8', errors='ignore')
    
    def _default_key_deserializer(self, key: Optional[bytes]) -> Optional[str]:
        """Default key deserializer"""
        if key is None:
            return None
        return key.decode('utf-8', errors='ignore')
    
    def close(self):
        """Clean up resources"""
        if self._producer:
            self._producer.close()
            self._producer = None
        
        if self._admin_client:
            self._admin_client.close()
            self._admin_client = None