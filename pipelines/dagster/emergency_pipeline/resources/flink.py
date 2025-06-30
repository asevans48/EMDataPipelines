"""
Flink Resources for Emergency Management Pipeline
Stream processing with microbatching for real-time analytics
"""

import json
import requests
from datetime import datetime
from typing import Dict, Any, List, Optional

from dagster import (
    ConfigurableResource,
    get_dagster_logger,
)
from pydantic import Field


class FlinkResource(ConfigurableResource):
    """
    Apache Flink resource for stream processing
    Manages Flink jobs for real-time emergency data processing
    """
    
    jobmanager_host: str = Field(description="Flink JobManager host")
    jobmanager_port: int = Field(default=8081, description="Flink JobManager port")
    parallelism: int = Field(default=4, description="Default parallelism")
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.logger = get_dagster_logger()
        self.base_url = f"http://{self.jobmanager_host}:{self.jobmanager_port}"
    
    def get_cluster_overview(self) -> Dict[str, Any]:
        """Get Flink cluster overview and health status"""
        try:
            response = requests.get(f"{self.base_url}/overview", timeout=10)
            response.raise_for_status()
            
            overview = response.json()
            self.logger.debug(f"Flink cluster overview: {overview}")
            
            return {
                "cluster_status": "healthy" if response.status_code == 200 else "unhealthy",
                "taskmanagers": overview.get("taskmanagers", 0),
                "slots_total": overview.get("slots-total", 0),
                "slots_available": overview.get("slots-available", 0),
                "jobs_running": overview.get("jobs-running", 0),
                "jobs_finished": overview.get("jobs-finished", 0),
                "jobs_cancelled": overview.get("jobs-cancelled", 0),
                "jobs_failed": overview.get("jobs-failed", 0),
                "check_timestamp": datetime.now().isoformat()
            }
            
        except Exception as e:
            self.logger.error(f"Error getting Flink cluster overview: {str(e)}")
            return {
                "cluster_status": "error",
                "error": str(e),
                "check_timestamp": datetime.now().isoformat()
            }
    
    def submit_job(self, job_name: str, jar_path: str, 
                   main_class: str, program_args: List[str] = None,
                   parallelism: Optional[int] = None) -> Optional[str]:
        """Submit a Flink job"""
        
        job_parallelism = parallelism or self.parallelism
        args = program_args or []
        
        # Prepare job submission payload
        job_payload = {
            "entryClass": main_class,
            "programArgs": " ".join(args),
            "parallelism": job_parallelism,
            "savepointPath": None,
            "allowNonRestoredState": False
        }
        
        try:
            # Submit job
            files = {"jarfile": open(jar_path, "rb")}
            data = {"flinkConfiguration": json.dumps(job_payload)}
            
            response = requests.post(
                f"{self.base_url}/jars/upload",
                files=files,
                data=data,
                timeout=60
            )
            response.raise_for_status()
            
            upload_result = response.json()
            jar_id = upload_result.get("filename")
            
            if not jar_id:
                raise Exception("Failed to upload JAR file")
            
            # Run the uploaded JAR
            run_response = requests.post(
                f"{self.base_url}/jars/{jar_id}/run",
                json=job_payload,
                timeout=30
            )
            run_response.raise_for_status()
            
            run_result = run_response.json()
            job_id = run_result.get("jobid")
            
            if job_id:
                self.logger.info(f"Submitted Flink job '{job_name}' with ID: {job_id}")
                return job_id
            else:
                raise Exception("Job submission failed - no job ID returned")
                
        except Exception as e:
            self.logger.error(f"Error submitting Flink job '{job_name}': {str(e)}")
            return None
    
    def get_job_status(self, job_id: str) -> Dict[str, Any]:
        """Get status of a specific Flink job"""
        try:
            response = requests.get(f"{self.base_url}/jobs/{job_id}", timeout=10)
            response.raise_for_status()
            
            job_info = response.json()
            
            return {
                "job_id": job_id,
                "name": job_info.get("name", "unknown"),
                "state": job_info.get("state", "unknown"),
                "start_time": job_info.get("start-time"),
                "end_time": job_info.get("end-time"),
                "duration": job_info.get("duration"),
                "vertices": len(job_info.get("vertices", [])),
                "status_timestamp": datetime.now().isoformat()
            }
            
        except Exception as e:
            self.logger.error(f"Error getting job status for {job_id}: {str(e)}")
            return {
                "job_id": job_id,
                "state": "error",
                "error": str(e),
                "status_timestamp": datetime.now().isoformat()
            }
    
    def cancel_job(self, job_id: str, savepoint_dir: Optional[str] = None) -> bool:
        """Cancel a Flink job, optionally creating a savepoint"""
        try:
            if savepoint_dir:
                # Cancel with savepoint
                cancel_payload = {"target-directory": savepoint_dir}
                response = requests.post(
                    f"{self.base_url}/jobs/{job_id}/savepoints",
                    json=cancel_payload,
                    timeout=30
                )
            else:
                # Cancel without savepoint
                response = requests.patch(
                    f"{self.base_url}/jobs/{job_id}",
                    timeout=30
                )
            
            response.raise_for_status()
            self.logger.info(f"Cancelled Flink job: {job_id}")
            return True
            
        except Exception as e:
            self.logger.error(f"Error cancelling job {job_id}: {str(e)}")
            return False
    
    def list_jobs(self) -> List[Dict[str, Any]]:
        """List all Flink jobs"""
        try:
            response = requests.get(f"{self.base_url}/jobs", timeout=10)
            response.raise_for_status()
            
            jobs_data = response.json()
            jobs = jobs_data.get("jobs", [])
            
            job_list = []
            for job in jobs:
                job_list.append({
                    "job_id": job.get("id"),
                    "name": job.get("name", "unknown"),
                    "state": job.get("status", "unknown"),
                    "start_time": job.get("start-time"),
                    "end_time": job.get("end-time"),
                    "duration": job.get("duration")
                })
            
            return job_list
            
        except Exception as e:
            self.logger.error(f"Error listing jobs: {str(e)}")
            return []
    
    def create_kafka_to_starrocks_job(self, 
                                    kafka_topic: str,
                                    kafka_servers: str,
                                    starrocks_table: str,
                                    starrocks_host: str,
                                    starrocks_port: int = 9030,
                                    job_name: Optional[str] = None) -> Optional[str]:
        """
        Create a Flink job for Kafka to StarRocks streaming
        This is a simplified version - in production, you'd use actual Flink job JARs
        """
        
        job_name = job_name or f"kafka_to_starrocks_{kafka_topic}"
        
        # In a real implementation, this would compile and submit actual Flink jobs
        # For now, we'll simulate the job creation
        
        flink_sql_job = f"""
        CREATE TABLE kafka_source (
            data STRING,
            event_time TIMESTAMP(3) METADATA FROM 'timestamp'
        ) WITH (
            'connector' = 'kafka',
            'topic' = '{kafka_topic}',
            'properties.bootstrap.servers' = '{kafka_servers}',
            'scan.startup.mode' = 'latest-offset',
            'format' = 'json'
        );
        
        CREATE TABLE starrocks_sink (
            data STRING,
            event_time TIMESTAMP(3)
        ) WITH (
            'connector' = 'starrocks',
            'jdbc-url' = 'jdbc:mysql://{starrocks_host}:{starrocks_port}',
            'load-url' = '{starrocks_host}:{starrocks_port}',
            'table-name' = '{starrocks_table}',
            'username' = 'root',
            'password' = '',
            'sink.properties.format' = 'json'
        );
        
        INSERT INTO starrocks_sink 
        SELECT data, event_time 
        FROM kafka_source;
        """
        
        self.logger.info(f"Would create Flink job: {job_name}")
        self.logger.debug(f"Flink SQL: {flink_sql_job}")
        
        # Simulate job submission
        simulated_job_id = f"job_{kafka_topic}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        return simulated_job_id
    
    def setup_emergency_stream_jobs(self, kafka_servers: str, 
                                  starrocks_host: str, starrocks_port: int = 9030) -> Dict[str, str]:
        """Set up standard emergency data streaming jobs"""
        
        jobs_created = {}
        
        # Standard emergency data streams
        stream_configs = [
            {
                "kafka_topic": "fema_disasters",
                "starrocks_table": "fema_disaster_declarations",
                "job_name": "fema_disaster_stream"
            },
            {
                "kafka_topic": "noaa_weather_alerts", 
                "starrocks_table": "noaa_weather_alerts",
                "job_name": "noaa_weather_stream"
            },
            {
                "kafka_topic": "coagmet_weather",
                "starrocks_table": "coagmet_weather_data", 
                "job_name": "coagmet_stream"
            },
            {
                "kafka_topic": "usda_agricultural_data",
                "starrocks_table": "usda_agricultural_data",
                "job_name": "usda_stream"
            }
        ]
        
        for config in stream_configs:
            job_id = self.create_kafka_to_starrocks_job(
                kafka_topic=config["kafka_topic"],
                kafka_servers=kafka_servers,
                starrocks_table=config["starrocks_table"],
                starrocks_host=starrocks_host,
                starrocks_port=starrocks_port,
                job_name=config["job_name"]
            )
            
            if job_id:
                jobs_created[config["job_name"]] = job_id
                self.logger.info(f"Created streaming job: {config['job_name']} -> {job_id}")
        
        return jobs_created
    
    def get_job_metrics(self, job_id: str) -> Dict[str, Any]:
        """Get detailed metrics for a Flink job"""
        try:
            # Get job details
            job_response = requests.get(f"{self.base_url}/jobs/{job_id}", timeout=10)
            job_response.raise_for_status()
            job_data = job_response.json()
            
            # Get job metrics
            metrics_response = requests.get(f"{self.base_url}/jobs/{job_id}/metrics", timeout=10)
            metrics_data = {}
            if metrics_response.status_code == 200:
                metrics_data = metrics_response.json()
            
            return {
                "job_id": job_id,
                "name": job_data.get("name", "unknown"),
                "state": job_data.get("state", "unknown"),
                "start_time": job_data.get("start-time"),
                "duration": job_data.get("duration"),
                "parallelism": job_data.get("parallelism", 0),
                "metrics": metrics_data,
                "vertices_count": len(job_data.get("vertices", [])),
                "metrics_timestamp": datetime.now().isoformat()
            }
            
        except Exception as e:
            self.logger.error(f"Error getting job metrics for {job_id}: {str(e)}")
            return {
                "job_id": job_id,
                "error": str(e),
                "metrics_timestamp": datetime.now().isoformat()
            }
    
    def health_check(self) -> bool:
        """Simple health check for Flink cluster"""
        try:
            response = requests.get(f"{self.base_url}/overview", timeout=5)
            response.raise_for_status()
            
            overview = response.json()
            
            # Basic health checks
            taskmanagers = overview.get("taskmanagers", 0)
            slots_available = overview.get("slots-available", 0)
            
            is_healthy = taskmanagers > 0 and slots_available > 0
            
            if is_healthy:
                self.logger.debug("Flink cluster health check passed")
            else:
                self.logger.warning(f"Flink cluster health check failed: {overview}")
            
            return is_healthy
            
        except Exception as e:
            self.logger.error(f"Flink health check failed: {str(e)}")
            return False