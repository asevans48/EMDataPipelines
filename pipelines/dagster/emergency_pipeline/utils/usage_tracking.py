"""
Usage Tracking Utilities for Emergency Management Pipeline
Comprehensive tracking for API usage, performance monitoring, and compliance logging
"""

import os
import json
import time
import hashlib
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional, Union
from collections import defaultdict, deque
import threading
import logging

logger = logging.getLogger(__name__)


class UsageMetrics:
    """Container for usage metrics"""
    
    def __init__(self):
        self.request_count = 0
        self.data_volume_bytes = 0
        self.error_count = 0
        self.response_times = []
        self.start_time = datetime.now()
        self.last_activity = datetime.now()
    
    def add_request(self, response_time_ms: float, data_size_bytes: int = 0, is_error: bool = False):
        """Add request metrics"""
        self.request_count += 1
        self.data_volume_bytes += data_size_bytes
        self.response_times.append(response_time_ms)
        self.last_activity = datetime.now()
        
        if is_error:
            self.error_count += 1
    
    def get_summary(self) -> Dict[str, Any]:
        """Get summary statistics"""
        duration_seconds = (self.last_activity - self.start_time).total_seconds()
        
        return {
            'request_count': self.request_count,
            'data_volume_bytes': self.data_volume_bytes,
            'data_volume_mb': round(self.data_volume_bytes / 1024 / 1024, 2),
            'error_count': self.error_count,
            'error_rate': self.error_count / max(self.request_count, 1),
            'duration_seconds': duration_seconds,
            'requests_per_second': self.request_count / max(duration_seconds, 1),
            'avg_response_time_ms': sum(self.response_times) / max(len(self.response_times), 1),
            'start_time': self.start_time.isoformat(),
            'last_activity': self.last_activity.isoformat()
        }


class RateLimitTracker:
    """Tracks and enforces rate limits"""
    
    def __init__(self, requests_per_hour: int = 1000):
        self.requests_per_hour = requests_per_hour
        self.request_times = deque()
        self.lock = threading.Lock()
    
    def is_rate_limited(self) -> bool:
        """Check if rate limit is exceeded"""
        with self.lock:
            now = datetime.now()
            cutoff = now - timedelta(hours=1)
            
            # Remove old requests
            while self.request_times and self.request_times[0] < cutoff:
                self.request_times.popleft()
            
            return len(self.request_times) >= self.requests_per_hour
    
    def record_request(self):
        """Record a new request"""
        with self.lock:
            self.request_times.append(datetime.now())
    
    def get_remaining_quota(self) -> int:
        """Get remaining requests in current hour"""
        with self.lock:
            now = datetime.now()
            cutoff = now - timedelta(hours=1)
            
            # Remove old requests
            while self.request_times and self.request_times[0] < cutoff:
                self.request_times.popleft()
            
            return max(0, self.requests_per_hour - len(self.request_times))


class UsageTracker:
    """Main usage tracking system"""
    
    def __init__(self, log_directory: str = "/opt/dagster/storage/usage_logs"):
        self.log_directory = log_directory
        os.makedirs(log_directory, exist_ok=True)
        
        # In-memory metrics
        self.organization_metrics: Dict[str, UsageMetrics] = defaultdict(UsageMetrics)
        self.endpoint_metrics: Dict[str, UsageMetrics] = defaultdict(UsageMetrics)
        self.global_metrics = UsageMetrics()
        
        # Rate limiting
        self.rate_limiters: Dict[str, RateLimitTracker] = {}
        self.rate_limit_config = {
            'government': 10000,
            'academic': 5000,
            'commercial': 2000,
            'public': 1000,
            'default': 1000
        }
    
    def track_api_request(
        self,
        organization: str,
        endpoint: str,
        response_time_ms: float,
        status_code: int,
        data_size_bytes: int = 0,
        user_agent: str = "",
        ip_address: str = ""
    ):
        """Track API request with full context"""
        
        is_error = status_code >= 400
        
        # Update metrics
        self.organization_metrics[organization].add_request(response_time_ms, data_size_bytes, is_error)
        self.endpoint_metrics[endpoint].add_request(response_time_ms, data_size_bytes, is_error)
        self.global_metrics.add_request(response_time_ms, data_size_bytes, is_error)
        
        # Log detailed request
        request_log = {
            'timestamp': datetime.now().isoformat(),
            'organization': organization,
            'endpoint': endpoint,
            'response_time_ms': response_time_ms,
            'status_code': status_code,
            'data_size_bytes': data_size_bytes,
            'user_agent': user_agent,
            'ip_address': self._hash_ip(ip_address),  # Hash for privacy
            'is_error': is_error
        }
        
        self._write_log('api_requests', request_log)
        
        # Update rate limiter
        if organization not in self.rate_limiters:
            org_type = self._classify_organization(organization)
            requests_per_hour = self.rate_limit_config.get(org_type, self.rate_limit_config['default'])
            self.rate_limiters[organization] = RateLimitTracker(requests_per_hour)
        
        self.rate_limiters[organization].record_request()
    
    def track_data_ingestion(
        self,
        source: str,
        records_processed: int,
        processing_time_seconds: float,
        success: bool,
        error_message: str = ""
    ):
        """Track data ingestion operations"""
        
        ingestion_log = {
            'timestamp': datetime.now().isoformat(),
            'source': source,
            'records_processed': records_processed,
            'processing_time_seconds': processing_time_seconds,
            'records_per_second': records_processed / max(processing_time_seconds, 0.001),
            'success': success,
            'error_message': error_message
        }
        
        self._write_log('data_ingestion', ingestion_log)
    
    def track_data_quality_event(
        self,
        source: str,
        quality_score: float,
        issues_found: List[str],
        validation_type: str
    ):
        """Track data quality events"""
        
        quality_log = {
            'timestamp': datetime.now().isoformat(),
            'source': source,
            'quality_score': quality_score,
            'issues_count': len(issues_found),
            'issues': issues_found,
            'validation_type': validation_type
        }
        
        self._write_log('data_quality', quality_log)
    
    def check_rate_limit(self, organization: str) -> bool:
        """Check if organization is within rate limits"""
        if organization not in self.rate_limiters:
            org_type = self._classify_organization(organization)
            requests_per_hour = self.rate_limit_config.get(org_type, self.rate_limit_config['default'])
            self.rate_limiters[organization] = RateLimitTracker(requests_per_hour)
        
        return not self.rate_limiters[organization].is_rate_limited()
    
    def get_organization_metrics(self, organization: str) -> Dict[str, Any]:
        """Get metrics for specific organization"""
        if organization not in self.organization_metrics:
            return {}
        
        return self.organization_metrics[organization].get_summary()
    
    def get_endpoint_metrics(self, endpoint: str) -> Dict[str, Any]:
        """Get metrics for specific endpoint"""
        if endpoint not in self.endpoint_metrics:
            return {}
        
        return self.endpoint_metrics[endpoint].get_summary()
    
    def get_global_metrics(self) -> Dict[str, Any]:
        """Get global system metrics"""
        return self.global_metrics.get_summary()
    
    def get_top_organizations(self, limit: int = 10) -> List[Dict[str, Any]]:
        """Get top organizations by usage"""
        org_usage = []
        
        for org, metrics in self.organization_metrics.items():
            summary = metrics.get_summary()
            summary['organization'] = org
            org_usage.append(summary)
        
        # Sort by request count
        org_usage.sort(key=lambda x: x['request_count'], reverse=True)
        
        return org_usage[:limit]
    
    def get_usage_analytics(self, time_window_hours: int = 24) -> Dict[str, Any]:
        """Get comprehensive usage analytics"""
        
        # Read recent logs for detailed analysis
        recent_logs = self._read_recent_logs('api_requests', time_window_hours)
        
        analytics = {
            'time_window_hours': time_window_hours,
            'global_metrics': self.get_global_metrics(),
            'top_organizations': self.get_top_organizations(),
            'endpoint_analysis': self._analyze_endpoints(recent_logs),
            'geographic_analysis': self._analyze_geography(recent_logs),
            'temporal_analysis': self._analyze_temporal_patterns(recent_logs),
            'error_analysis': self._analyze_errors(recent_logs)
        }
        
        return analytics
    
    def _classify_organization(self, organization: str) -> str:
        """Classify organization type for rate limiting"""
        org_lower = organization.lower()
        
        if any(term in org_lower for term in ['gov', 'government', 'state', 'county', 'city', 'federal']):
            return 'government'
        elif any(term in org_lower for term in ['university', 'edu', 'research', 'academic']):
            return 'academic'
        elif any(term in org_lower for term in ['corp', 'inc', 'llc', 'company', 'commercial']):
            return 'commercial'
        else:
            return 'public'
    
    def _hash_ip(self, ip_address: str) -> str:
        """Hash IP address for privacy"""
        if not ip_address:
            return ""
        
        return hashlib.sha256(ip_address.encode()).hexdigest()[:16]
    
    def _write_log(self, log_type: str, entry: Dict[str, Any]):
        """Write log entry to file"""
        log_date = datetime.now().strftime('%Y%m%d')
        log_file = os.path.join(self.log_directory, f"{log_type}_{log_date}.log")
        
        try:
            with open(log_file, 'a') as f:
                f.write(json.dumps(entry) + '\n')
        except Exception as e:
            logger.error(f"Failed to write log entry: {str(e)}")
    
    def _read_recent_logs(self, log_type: str, hours: int = 24) -> List[Dict[str, Any]]:
        """Read recent log entries"""
        logs = []
        cutoff_time = datetime.now() - timedelta(hours=hours)
        
        # Check today's log and yesterday's if needed
        dates_to_check = [datetime.now()]
        if cutoff_time.date() < datetime.now().date():
            dates_to_check.append(datetime.now() - timedelta(days=1))
        
        for date in dates_to_check:
            log_file = os.path.join(self.log_directory, f"{log_type}_{date.strftime('%Y%m%d')}.log")
            
            if os.path.exists(log_file):
                try:
                    with open(log_file, 'r') as f:
                        for line in f:
                            try:
                                entry = json.loads(line.strip())
                                entry_time = datetime.fromisoformat(entry['timestamp'])
                                
                                if entry_time >= cutoff_time:
                                    logs.append(entry)
                            except (json.JSONDecodeError, KeyError, ValueError):
                                continue
                except Exception as e:
                    logger.error(f"Error reading log file {log_file}: {str(e)}")
        
        return logs
    
    def _analyze_endpoints(self, logs: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Analyze endpoint usage patterns"""
        endpoint_stats = defaultdict(lambda: {
            'request_count': 0,
            'total_response_time': 0,
            'error_count': 0,
            'data_volume': 0
        })
        
        for entry in logs:
            endpoint = entry.get('endpoint', 'unknown')
            stats = endpoint_stats[endpoint]
            
            stats['request_count'] += 1
            stats['total_response_time'] += entry.get('response_time_ms', 0)
            stats['data_volume'] += entry.get('data_size_bytes', 0)
            
            if entry.get('is_error', False):
                stats['error_count'] += 1
        
        # Calculate averages and convert to final format
        endpoint_analysis = {}
        for endpoint, stats in endpoint_stats.items():
            endpoint_analysis[endpoint] = {
                'request_count': stats['request_count'],
                'avg_response_time_ms': stats['total_response_time'] / max(stats['request_count'], 1),
                'error_rate': stats['error_count'] / max(stats['request_count'], 1),
                'data_volume_mb': stats['data_volume'] / 1024 / 1024
            }
        
        return endpoint_analysis
    
    def _analyze_geography(self, logs: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Analyze geographic usage patterns"""
        # This is simplified - in practice would use GeoIP lookup
        ip_counts = defaultdict(int)
        
        for entry in logs:
            ip_hash = entry.get('ip_address', 'unknown')
            ip_counts[ip_hash] += 1
        
        return {
            'unique_ips': len(ip_counts),
            'top_ip_hashes': dict(sorted(ip_counts.items(), key=lambda x: x[1], reverse=True)[:10])
        }
    
    def _analyze_temporal_patterns(self, logs: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Analyze temporal usage patterns"""
        hourly_counts = defaultdict(int)
        
        for entry in logs:
            try:
                timestamp = datetime.fromisoformat(entry['timestamp'])
                hour = timestamp.hour
                hourly_counts[hour] += 1
            except (ValueError, KeyError):
                continue
        
        # Find peak usage hour
        peak_hour = max(hourly_counts.items(), key=lambda x: x[1]) if hourly_counts else (0, 0)
        
        return {
            'hourly_distribution': dict(hourly_counts),
            'peak_hour': peak_hour[0],
            'peak_hour_requests': peak_hour[1],
            'total_hours_active': len([h for h, count in hourly_counts.items() if count > 0])
        }
    
    def _analyze_errors(self, logs: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Analyze error patterns"""
        error_codes = defaultdict(int)
        error_endpoints = defaultdict(int)
        
        for entry in logs:
            if entry.get('is_error', False):
                status_code = entry.get('status_code', 0)
                endpoint = entry.get('endpoint', 'unknown')
                
                error_codes[status_code] += 1
                error_endpoints[endpoint] += 1
        
        total_errors = sum(error_codes.values())
        total_requests = len(logs)
        
        return {
            'total_errors': total_errors,
            'error_rate': total_errors / max(total_requests, 1),
            'error_codes': dict(error_codes),
            'error_endpoints': dict(error_endpoints),
            'most_common_error': max(error_codes.