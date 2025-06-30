"""
Emergency Management Data Scrapers
Modular scraping service that writes data to Flink for real-time processing
"""

import asyncio
import logging
import json
import os
from datetime import datetime, timedelta
from typing import Dict, Any, List
from concurrent.futures import ThreadPoolExecutor

import pandas as pd
import requests
from kafka import KafkaProducer
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings
from pyflink.table.descriptors import Schema, Kafka, Json

# Configure logging for federal compliance
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/app/logs/scrapers.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class EmergencyDataScraper:
    """Base class for emergency data scrapers with federal compliance"""
    
    def __init__(self, kafka_servers: str, flink_jobmanager: str):
        self.kafka_servers = kafka_servers
        self.flink_jobmanager = flink_jobmanager
        self.producer = self._create_kafka_producer()
        
    def _create_kafka_producer(self) -> KafkaProducer:
        """Create Kafka producer with compliance settings"""
        return KafkaProducer(
            bootstrap_servers=self.kafka_servers.split(','),
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8') if k else None,
            acks='all',  # Wait for all replicas
            retries=3,
            compression_type='gzip',
            batch_size=16384,
            linger_ms=10,
        )
    
    def send_to_kafka(self, topic: str, data: Dict[str, Any], key: str = None):
        """Send data to Kafka with error handling"""
        try:
            # Add metadata for audit trail
            data['_metadata'] = {
                'ingestion_timestamp': datetime.now().isoformat(),
                'scraper_id': self.__class__.__name__,
                'data_lineage': topic
            }
            
            future = self.producer.send(topic, value=data, key=key)
            future.get(timeout=10)  # Block for up to 10 seconds
            logger.debug(f"Successfully sent data to {topic}")
            
        except Exception as e:
            logger.error(f"Failed to send data to Kafka topic {topic}: {str(e)}")
            raise
    
    def create_flink_source_table(self, table_name: str, topic: str, schema_fields: List[str]):
        """Create Flink source table for real-time processing"""
        try:
            env_settings = EnvironmentSettings.new_instance().in_streaming_mode().use_blink_planner().build()
            table_env = StreamTableEnvironment.create(environment_settings=env_settings)
            
            # Define schema
            schema_ddl = ", ".join(schema_fields)
            
            # Create Kafka connector table
            create_table_sql = f"""
            CREATE TABLE {table_name} (
                {schema_ddl},
                proc_time AS PROCTIME()
            ) WITH (
                'connector' = 'kafka',
                'topic' = '{topic}',
                'properties.bootstrap.servers' = '{self.kafka_servers}',
                'properties.group.id' = 'emergency_data_group',
                'scan.startup.mode' = 'latest-offset',
                'format' = 'json',
                'json.fail-on-missing-field' = 'false',
                'json.ignore-parse-errors' = 'true'
            )
            """
            
            table_env.execute_sql(create_table_sql)
            logger.info(f"Created Flink source table: {table_name}")
            
        except Exception as e:
            logger.error(f"Failed to create Flink table {table_name}: {str(e)}")
            raise


class FEMAScraper(EmergencyDataScraper):
    """FEMA OpenFEMA API scraper"""
    
    def __init__(self, kafka_servers: str, flink_jobmanager: str):
        super().__init__(kafka_servers, flink_jobmanager)
        self.base_url = "https://www.fema.gov/api/open/v2"
        
    async def scrape_disaster_declarations(self):
        """Scrape FEMA disaster declarations"""
        endpoint = f"{self.base_url}/DisasterDeclarationsSummaries"
        
        try:
            # Get recent declarations
            params = {
                '$limit': 1000,
                '$orderby': 'declarationDate desc',
                '$filter': f"declarationDate ge '{(datetime.now() - timedelta(days=7)).isoformat()}'"
            }
            
            response = requests.get(endpoint, params=params, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            declarations = data.get('DisasterDeclarationsSummaries', [])
            
            for declaration in declarations:
                # Clean and standardize data
                processed_declaration = {
                    'disaster_number': declaration.get('disasterNumber'),
                    'state': declaration.get('state'),
                    'declaration_type': declaration.get('declarationType'),
                    'declaration_date': declaration.get('declarationDate'),
                    'incident_type': declaration.get('incidentType'),
                    'incident_begin_date': declaration.get('incidentBeginDate'),
                    'incident_end_date': declaration.get('incidentEndDate'),
                    'title': declaration.get('title', '').strip(),
                    'fy_declared': declaration.get('fyDeclared'),
                    'disaster_closeout_date': declaration.get('disasterCloseoutDate'),
                    'place_code': declaration.get('placeCode'),
                    'designated_area': declaration.get('designatedArea', '').strip(),
                    'declaration_request_number': declaration.get('declarationRequestNumber'),
                }
                
                # Send to Kafka
                key = str(declaration.get('disasterNumber', ''))
                self.send_to_kafka('fema_disaster_declarations', processed_declaration, key)
                
            logger.info(f"Scraped {len(declarations)} FEMA disaster declarations")
            
        except Exception as e:
            logger.error(f"Error scraping FEMA declarations: {str(e)}")
            raise
    
    async def scrape_public_assistance_applicants(self):
        """Scrape FEMA Public Assistance Applicants data"""
        endpoint = f"{self.base_url}/PublicAssistanceApplicants"
        
        try:
            params = {
                '$limit': 1000,
                '$orderby': 'declarationDate desc'
            }
            
            response = requests.get(endpoint, params=params, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            applicants = data.get('PublicAssistanceApplicants', [])
            
            for applicant in applicants:
                processed_applicant = {
                    'disaster_number': applicant.get('disasterNumber'),
                    'state': applicant.get('state'),
                    'applicant_name': applicant.get('applicantName', '').strip(),
                    'county': applicant.get('county', '').strip(),
                    'city': applicant.get('city', '').strip(),
                    'applicant_type': applicant.get('applicantType'),
                    'damage_category': applicant.get('damageCategory'),
                    'program': applicant.get('program'),
                    'project_amount': applicant.get('projectAmount', 0),
                    'federal_share_obligated': applicant.get('federalShareObligated', 0),
                }
                
                key = f"{applicant.get('disasterNumber')}_{applicant.get('applicantName', '')}"
                self.send_to_kafka('fema_public_assistance', processed_applicant, key)
                
            logger.info(f"Scraped {len(applicants)} FEMA public assistance records")
            
        except Exception as e:
            logger.error(f"Error scraping FEMA public assistance: {str(e)}")
            raise


class NOAAScraper(EmergencyDataScraper):
    """NOAA Weather and Climate API scraper"""
    
    def __init__(self, kafka_servers: str, flink_jobmanager: str):
        super().__init__(kafka_servers, flink_jobmanager)
        self.base_url = "https://api.weather.gov"
        self.headers = {'User-Agent': 'EmergencyManagement/1.0'}
        
    async def scrape_active_alerts(self):
        """Scrape active weather alerts"""
        endpoint = f"{self.base_url}/alerts/active"
        
        try:
            response = requests.get(endpoint, headers=self.headers, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            alerts = data.get('features', [])
            
            for alert in alerts:
                properties = alert.get('properties', {})
                geometry = alert.get('geometry', {})
                
                processed_alert = {
                    'alert_id': properties.get('id'),
                    'event': properties.get('event'),
                    'severity': properties.get('severity'),
                    'urgency': properties.get('urgency'),
                    'certainty': properties.get('certainty'),
                    'headline': properties.get('headline', '').strip(),
                    'description': properties.get('description', '').strip(),
                    'instruction': properties.get('instruction', '').strip(),
                    'effective': properties.get('effective'),
                    'expires': properties.get('expires'),
                    'onset': properties.get('onset'),
                    'ends': properties.get('ends'),
                    'status': properties.get('status'),
                    'message_type': properties.get('messageType'),
                    'category': properties.get('category'),
                    'area_desc': properties.get('areaDesc', '').strip(),
                    'sender_name': properties.get('senderName', '').strip(),
                    'sent': properties.get('sent'),
                    'geometry_type': geometry.get('type'),
                    'coordinates': geometry.get('coordinates'),
                }
                
                key = properties.get('id', '')
                self.send_to_kafka('noaa_weather_alerts', processed_alert, key)
                
            logger.info(f"Scraped {len(alerts)} NOAA weather alerts")
            
        except Exception as e:
            logger.error(f"Error scraping NOAA alerts: {str(e)}")
            raise
    
    async def scrape_observations(self, station_ids: List[str]):
        """Scrape weather observations from specific stations"""
        for station_id in station_ids:
            try:
                endpoint = f"{self.base_url}/stations/{station_id}/observations/latest"
                
                response = requests.get(endpoint, headers=self.headers, timeout=30)
                response.raise_for_status()
                
                data = response.json()
                properties = data.get('properties', {})
                
                processed_observation = {
                    'station_id': station_id,
                    'timestamp': properties.get('timestamp'),
                    'temperature': self._extract_value(properties.get('temperature')),
                    'dewpoint': self._extract_value(properties.get('dewpoint')),
                    'wind_direction': self._extract_value(properties.get('windDirection')),
                    'wind_speed': self._extract_value(properties.get('windSpeed')),
                    'wind_gust': self._extract_value(properties.get('windGust')),
                    'barometric_pressure': self._extract_value(properties.get('barometricPressure')),
                    'sea_level_pressure': self._extract_value(properties.get('seaLevelPressure')),
                    'visibility': self._extract_value(properties.get('visibility')),
                    'max_temperature_last_24_hours': self._extract_value(properties.get('maxTemperatureLast24Hours')),
                    'min_temperature_last_24_hours': self._extract_value(properties.get('minTemperatureLast24Hours')),
                    'precipitation_last_hour': self._extract_value(properties.get('precipitationLastHour')),
                    'precipitation_last_3_hours': self._extract_value(properties.get('precipitationLast3Hours')),
                    'precipitation_last_6_hours': self._extract_value(properties.get('precipitationLast6Hours')),
                    'relative_humidity': self._extract_value(properties.get('relativeHumidity')),
                    'wind_chill': self._extract_value(properties.get('windChill')),
                    'heat_index': self._extract_value(properties.get('heatIndex')),
                }
                
                key = f"{station_id}_{properties.get('timestamp', '')}"
                self.send_to_kafka('noaa_weather_observations', processed_observation, key)
                
            except Exception as e:
                logger.warning(f"Error scraping observations for station {station_id}: {str(e)}")
                continue
    
    def _extract_value(self, measurement_obj):
        """Extract value from NOAA measurement object"""
        if isinstance(measurement_obj, dict):
            return measurement_obj.get('value')
        return measurement_obj


class CoAgMetScraper(EmergencyDataScraper):
    """Colorado Agricultural Meteorological Network scraper"""
    
    def __init__(self, kafka_servers: str, flink_jobmanager: str):
        super().__init__(kafka_servers, flink_jobmanager)
        self.base_url = "https://coagmet.colostate.edu/data_access/web_service"
        
    async def scrape_station_data(self):
        """Scrape CoAgMet weather station data"""
        try:
            # Get list of active stations
            stations_url = f"{self.base_url}/get_stations.php"
            response = requests.get(stations_url, timeout=30)
            response.raise_for_status()
            
            stations = response.json()
            
            # Process each station
            for station in stations[:20]:  # Limit for development
                station_id = station.get('station_id')
                
                # Get recent data for station
                data_url = f"{self.base_url}/get_data.php"
                params = {
                    'station_id': station_id,
                    'start_date': (datetime.now() - timedelta(hours=6)).strftime('%Y-%m-%d %H:00'),
                    'end_date': datetime.now().strftime('%Y-%m-%d %H:00'),
                    'format': 'json'
                }
                
                data_response = requests.get(data_url, params=params, timeout=30)
                if data_response.status_code == 200:
                    station_data = data_response.json()
                    
                    for reading in station_data.get('data', []):
                        processed_reading = {
                            'station_id': station_id,
                            'station_name': station.get('station_name', '').strip(),
                            'latitude': station.get('latitude'),
                            'longitude': station.get('longitude'),
                            'elevation': station.get('elevation'),
                            'timestamp': reading.get('timestamp'),
                            'air_temp': reading.get('air_temp'),
                            'relative_humidity': reading.get('relative_humidity'),
                            'wind_speed': reading.get('wind_speed'),
                            'wind_direction': reading.get('wind_direction'),
                            'wind_gust': reading.get('wind_gust'),
                            'solar_radiation': reading.get('solar_radiation'),
                            'precipitation': reading.get('precipitation'),
                            'soil_temp_2in': reading.get('soil_temp_2in'),
                            'soil_temp_8in': reading.get('soil_temp_8in'),
                            'vapor_pressure': reading.get('vapor_pressure'),
                            'reference_et': reading.get('reference_et'),
                        }
                        
                        key = f"{station_id}_{reading.get('timestamp', '')}"
                        self.send_to_kafka('coagmet_weather_data', processed_reading, key)
            
            logger.info(f"Scraped data from {len(stations)} CoAgMet stations")
            
        except Exception as e:
            logger.error(f"Error scraping CoAgMet data: {str(e)}")
            raise


class ScraperOrchestrator:
    """Orchestrates all scrapers with scheduling and error handling"""
    
    def __init__(self):
        self.kafka_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:29092')
        self.flink_jobmanager = os.getenv('FLINK_JOBMANAGER_HOST', 'flink-jobmanager')
        
        # Initialize scrapers
        self.scrapers = {
            'fema': FEMAScraper(self.kafka_servers, self.flink_jobmanager),
            'noaa': NOAAScraper(self.kafka_servers, self.flink_jobmanager),
            'coagmet': CoAgMetScraper(self.kafka_servers, self.flink_jobmanager),
        }
        
        # NOAA weather stations for Colorado
        self.noaa_stations = [
            'KDEN',  # Denver International
            'KCOS',  # Colorado Springs
            'KGJT',  # Grand Junction
            'KPUB',  # Pueblo
            'KFNL',  # Fort Collins
        ]
    
    async def run_scraping_cycle(self):
        """Run one complete scraping cycle"""
        tasks = []
        
        # FEMA scraping tasks
        tasks.append(self.scrapers['fema'].scrape_disaster_declarations())
        tasks.append(self.scrapers['fema'].scrape_public_assistance_applicants())
        
        # NOAA scraping tasks
        tasks.append(self.scrapers['noaa'].scrape_active_alerts())
        tasks.append(self.scrapers['noaa'].scrape_observations(self.noaa_stations))
        
        # CoAgMet scraping tasks
        tasks.append(self.scrapers['coagmet'].scrape_station_data())
        
        # Execute all tasks concurrently
        try:
            await asyncio.gather(*tasks, return_exceptions=True)
            logger.info("Completed scraping cycle")
        except Exception as e:
            logger.error(f"Error in scraping cycle: {str(e)}")
    
    async def run_continuously(self, interval_minutes: int = 15):
        """Run scraping continuously with specified interval"""
        while True:
            try:
                start_time = datetime.now()
                await self.run_scraping_cycle()
                
                # Calculate sleep time
                elapsed = (datetime.now() - start_time).total_seconds()
                sleep_time = max(0, (interval_minutes * 60) - elapsed)
                
                logger.info(f"Scraping cycle completed in {elapsed:.2f}s. Sleeping for {sleep_time:.2f}s")
                await asyncio.sleep(sleep_time)
                
            except KeyboardInterrupt:
                logger.info("Received interrupt signal, shutting down...")
                break
            except Exception as e:
                logger.error(f"Unexpected error in continuous scraping: {str(e)}")
                await asyncio.sleep(60)  # Wait 1 minute before retrying


async def main():
    """Main entry point"""
    logger.info("Starting Emergency Management Data Scrapers")
    
    orchestrator = ScraperOrchestrator()
    
    # Run initial scraping cycle
    await orchestrator.run_scraping_cycle()
    
    # Start continuous scraping
    await orchestrator.run_continuously(interval_minutes=15)


if __name__ == "__main__":
    asyncio.run(main())