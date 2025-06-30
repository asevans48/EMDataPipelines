"""
Resources for Emergency Management Data Pipeline
Modular resource definitions for different deployment modes
"""

from .database import StarRocksResource
from .kafka import KafkaResource  
from .flink import FlinkResource

__all__ = [
    "StarRocksResource",
    "KafkaResource", 
    "FlinkResource",
]