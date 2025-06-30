"""
API Client Utilities for Emergency Management Data Sources
Standardized clients for FEMA, NOAA, CoAgMet, USDA, and other emergency data APIs
"""

import requests
import time
import json
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional, Iterator
from abc import ABC, abstractmethod
import logging

logger = logging.getLogger(__name__)


class APIRateLimiter:
    """Rate limiting utility for API calls"""
    
    def __init__(self, calls_per_minute: int = 60):
        self.calls_per_minute = calls_per_minute
        self.min_interval = 60.0 / calls_per_minute
        self.last_call_time = 0
    
    def wait_if_needed(self):
        """Wait if necessary to respect rate limits"""
        elapsed = time.time() - self.last_call_time
        if elapsed < self.min_interval:
            sleep_time = self.min_interval - elapsed
            time.sleep(sleep_time)
        self.last_call_time = time.time()


class EmergencyAPIClient(ABC):
    """Abstract base class for emergency management API clients"""
    
    def __init__(self, api_key: str = "", rate_limit: int = 60, timeout: int = 30):
        self.api_key = api_key
        self.timeout = timeout
        self.rate_limiter = APIRateLimiter(rate_limit)
        self.session = requests.Session()
        self.session.headers.update(self._get_default_headers())
    
    @abstractmethod
    def _get_default_headers(self) -> Dict[str, str]:
        """Get default headers for API requests"""
        pass
    
    @abstractmethod
    def _get_base_url(self) -> str:
        """Get base URL for the API"""
        pass
    
    def _make_request(self, endpoint: str, params: Dict[str, Any] = None) -> Dict[str, Any]:
        """Make rate-limited API request"""
        self.rate_limiter.wait_if_needed()
        
        url = f"{self._get_base_url()}{endpoint}"
        
        try:
            response = self.session.get(url, params=params, timeout=self.timeout)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"API request failed for {url}: {str(e)}")
            raise
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse JSON response from {url}: {str(e)}")
            raise
    
    def health_check(self) -> bool:
        """Check if the API is accessible"""
        try:
            # Most APIs should respond to a simple request
            self._make_request("", params={"$limit": 1})
            return True
        except Exception:
            return False


class FEMAClient(EmergencyAPIClient):
    """Client for FEMA OpenFEMA API"""
    
    def _get_default_headers(self) -> Dict[str, str]:
        headers = {"User-Agent": "EmergencyManagement/1.0"}
        if self.api_key:
            headers["X-API-Key"] = self.api_key
        return headers
    
    def _get_base_url(self) -> str:
        return "https://www.fema.gov/api/open/v2/"
    
    def get_disaster_declarations(
        self, 
        state: Optional[str] = None,
        start_date: Optional[datetime] = None,
        limit: int = 1000
    ) -> List[Dict[str, Any]]:
        """Get disaster declarations from FEMA"""
        
        params = {"$limit": limit}
        
        filters = []
        if state:
            filters.append(f"state eq '{state.upper()}'")
        if start_date:
            filters.append(f"declarationDate ge '{start_date.isoformat()}'")
        
        if filters:
            params["$filter"] = " and ".join(filters)
        
        try:
            response = self._make_request("DisasterDeclarationsSummaries", params)
            return response.get("DisasterDeclarationsSummaries", [])
        except Exception as e:
            logger.error(f"Error fetching FEMA disaster declarations: {str(e)}")
            return []
    
    def get_public_assistance_applicants(
        self,
        disaster_number: Optional[str] = None,
        state: Optional[str] = None,
        limit: int = 1000
    ) -> List[Dict[str, Any]]:
        """Get public assistance applicant data"""
        
        params = {"$limit": limit}
        
        filters = []
        if disaster_number:
            filters.append(f"disasterNumber eq '{disaster_number}'")
        if state:
            filters.append(f"state eq '{state.upper()}'")
        
        if filters:
            params["$filter"] = " and ".join(filters)
        
        try:
            response = self._make_request("PublicAssistanceApplicants", params)
            return response.get("PublicAssistanceApplicants", [])
        except Exception as e:
            logger.error(f"Error fetching FEMA public assistance data: {str(e)}")
            return []
    
    def get_hazard_mitigation_assistance(
        self,
        state: Optional[str] = None,
        limit: int = 1000
    ) -> List[Dict[str, Any]]:
        """Get hazard mitigation assistance data"""
        
        params = {"$limit": limit}
        
        if state:
            params["$filter"] = f"state eq '{state.upper()}'"
        
        try:
            response = self._make_request("HazardMitigationAssistance", params)
            return response.get("HazardMitigationAssistance", [])
        except Exception as e:
            logger.error(f"Error fetching FEMA hazard mitigation data: {str(e)}")
            return []


class NOAAClient(EmergencyAPIClient):
    """Client for NOAA Weather API"""
    
    def _get_default_headers(self) -> Dict[str, str]:
        user_agent = "EmergencyManagement/1.0"
        if self.api_key:
            user_agent += f" (+{self.api_key})"
        return {"User-Agent": user_agent}
    
    def _get_base_url(self) -> str:
        return "https://api.weather.gov/"
    
    def get_active_alerts(
        self,
        state: Optional[str] = None,
        severity: Optional[str] = None,
        urgency: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """Get active weather alerts"""
        
        params = {}
        if state:
            params["area"] = state.upper()
        if severity:
            params["severity"] = severity
        if urgency:
            params["urgency"] = urgency
        
        try:
            response = self._make_request("alerts/active", params)
            return response.get("features", [])
        except Exception as e:
            logger.error(f"Error fetching NOAA weather alerts: {str(e)}")
            return []
    
    def get_forecast_zones(self, state: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get forecast zones"""
        
        params = {}
        if state:
            params["area"] = state.upper()
        
        try:
            response = self._make_request("zones/forecast", params)
            return response.get("features", [])
        except Exception as e:
            logger.error(f"Error fetching NOAA forecast zones: {str(e)}")
            return []
    
    def get_stations(
        self,
        state: Optional[str] = None,
        limit: int = 500
    ) -> List[Dict[str, Any]]:
        """Get weather stations"""
        
        params = {"limit": limit}
        if state:
            params["state"] = state.upper()
        
        try:
            response = self._make_request("stations", params)
            return response.get("features", [])
        except Exception as e:
            logger.error(f"Error fetching NOAA stations: {str(e)}")
            return []
    
    def get_observations(
        self,
        station_id: str,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None
    ) -> List[Dict[str, Any]]:
        """Get weather observations for a station"""
        
        params = {}
        if start_time:
            params["start"] = start_time.isoformat()
        if end_time:
            params["end"] = end_time.isoformat()
        
        try:
            response = self._make_request(f"stations/{station_id}/observations", params)
            return response.get("features", [])
        except Exception as e:
            logger.error(f"Error fetching NOAA observations for {station_id}: {str(e)}")
            return []


class CoAgMetClient(EmergencyAPIClient):
    """Client for Colorado Agricultural Meteorological Network"""
    
    def _get_default_headers(self) -> Dict[str, str]:
        return {"User-Agent": "EmergencyManagement/1.0"}
    
    def _get_base_url(self) -> str:
        return "https://coagmet.colostate.edu/data_access/web_service/"
    
    def get_stations(self) -> List[Dict[str, Any]]:
        """Get list of CoAgMet stations"""
        
        try:
            response = self._make_request("get_stations.php")
            # CoAgMet returns different format, adapt as needed
            if isinstance(response, list):
                return response
            elif isinstance(response, dict) and 'stations' in response:
                return response['stations']
            else:
                return []
        except Exception as e:
            logger.error(f"Error fetching CoAgMet stations: {str(e)}")
            return []
    
    def get_station_data(
        self,
        station_id: str,
        start_date: datetime,
        end_date: datetime,
        data_format: str = "json"
    ) -> List[Dict[str, Any]]:
        """Get data for a specific station"""
        
        params = {
            "station_id": station_id,
            "start_date": start_date.strftime('%Y-%m-%d'),
            "end_date": end_date.strftime('%Y-%m-%d'),
            "format": data_format
        }
        
        try:
            response = self._make_request("get_data.php", params)
            if isinstance(response, dict) and 'data' in response:
                return response['data']
            elif isinstance(response, list):
                return response
            else:
                return []
        except Exception as e:
            logger.error(f"Error fetching CoAgMet data for station {station_id}: {str(e)}")
            return []
    
    def get_latest_readings(self, hours: int = 24) -> List[Dict[str, Any]]:
        """Get latest readings from all stations"""
        
        end_date = datetime.now()
        start_date = end_date - timedelta(hours=hours)
        
        all_data = []
        stations = self.get_stations()
        
        for station in stations[:10]:  # Limit to avoid rate limits
            station_id = station.get('station_id')
            if station_id:
                station_data = self.get_station_data(station_id, start_date, end_date)
                
                # Enrich with station metadata
                for reading in station_data:
                    reading.update({
                        'station_name': station.get('station_name'),
                        'latitude': station.get('latitude'),
                        'longitude': station.get('longitude'),
                        'elevation': station.get('elevation')
                    })
                
                all_data.extend(station_data)
        
        return all_data


class USDAClient(EmergencyAPIClient):
    """Client for USDA NASS QuickStats API"""
    
    def _get_default_headers(self) -> Dict[str, str]:
        return {"User-Agent": "EmergencyManagement/1.0"}
    
    def _get_base_url(self) -> str:
        return "https://quickstats.nass.usda.gov/api/"
    
    def get_crop_data(
        self,
        state: Optional[str] = None,
        commodity: Optional[str] = None,
        year: Optional[int] = None,
        statisticcat_desc: str = "AREA HARVESTED"
    ) -> List[Dict[str, Any]]:
        """Get crop production data"""
        
        params = {
            "key": self.api_key,
            "source_desc": "SURVEY",
            "sector_desc": "CROPS",
            "statisticcat_desc": statisticcat_desc,
            "format": "JSON"
        }
        
        if state:
            params["state_alpha"] = state.upper()
        if commodity:
            params["commodity_desc"] = commodity.upper()
        if year:
            params["year"] = str(year)
        
        try:
            response = self._make_request("api_GET/", params)
            return response.get("data", [])
        except Exception as e:
            logger.error(f"Error fetching USDA crop data: {str(e)}")
            return []
    
    def get_livestock_data(
        self,
        state: Optional[str] = None,
        year: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """Get livestock data"""
        
        params = {
            "key": self.api_key,
            "source_desc": "SURVEY",
            "sector_desc": "ANIMALS & PRODUCTS",
            "format": "JSON"
        }
        
        if state:
            params["state_alpha"] = state.upper()
        if year:
            params["year"] = str(year)
        
        try:
            response = self._make_request("api_GET/", params)
            return response.get("data", [])
        except Exception as e:
            logger.error(f"Error fetching USDA livestock data: {str(e)}")
            return []
    
    def get_disaster_assistance_data(
        self,
        state: Optional[str] = None,
        year: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """Get agricultural disaster assistance data"""
        
        # Note: This is a simplified example - actual USDA disaster assistance
        # data might require different parameters or endpoints
        
        params = {
            "key": self.api_key,
            "source_desc": "SURVEY",
            "sector_desc": "CROPS",
            "group_desc": "FIELD CROPS",
            "statisticcat_desc": "AREA AFFECTED",
            "format": "JSON"
        }
        
        if state:
            params["state_alpha"] = state.upper()
        if year:
            params["year"] = str(year)
        
        try:
            response = self._make_request("api_GET/", params)
            return response.get("data", [])
        except Exception as e:
            logger.error(f"Error fetching USDA disaster assistance data: {str(e)}")
            return []


class APIClientFactory:
    """Factory for creating API clients"""
    
    @staticmethod
    def create_fema_client(api_key: str = "", rate_limit: int = 60) -> FEMAClient:
        """Create FEMA API client"""
        return FEMAClient(api_key=api_key, rate_limit=rate_limit)
    
    @staticmethod
    def create_noaa_client(contact_info: str = "", rate_limit: int = 60) -> NOAAClient:
        """Create NOAA API client"""
        return NOAAClient(api_key=contact_info, rate_limit=rate_limit)
    
    @staticmethod
    def create_coagmet_client(rate_limit: int = 30) -> CoAgMetClient:
        """Create CoAgMet API client"""
        return CoAgMetClient(rate_limit=rate_limit)
    
    @staticmethod
    def create_usda_client(api_key: str, rate_limit: int = 30) -> USDAClient:
        """Create USDA API client"""
        if not api_key:
            raise ValueError("USDA API requires an API key")
        return USDAClient(api_key=api_key, rate_limit=rate_limit)


class MultiSourceDataFetcher:
    """Utility for fetching data from multiple sources concurrently"""
    
    def __init__(self, clients: Dict[str, EmergencyAPIClient]):
        self.clients = clients
    
    def fetch_all_disaster_data(self, state: Optional[str] = None) -> Dict[str, List[Dict[str, Any]]]:
        """Fetch disaster-related data from all configured sources"""
        
        results = {}
        
        # FEMA disaster declarations
        if "fema" in self.clients:
            try:
                results["fema_disasters"] = self.clients["fema"].get_disaster_declarations(state=state)
            except Exception as e:
                logger.error(f"Error fetching FEMA data: {str(e)}")
                results["fema_disasters"] = []
        
        # NOAA weather alerts
        if "noaa" in self.clients:
            try:
                results["noaa_alerts"] = self.clients["noaa"].get_active_alerts(state=state)
            except Exception as e:
                logger.error(f"Error fetching NOAA data: {str(e)}")
                results["noaa_alerts"] = []
        
        # CoAgMet data (Colorado specific)
        if "coagmet" in self.clients and (state is None or state.upper() == "CO"):
            try:
                results["coagmet_weather"] = self.clients["coagmet"].get_latest_readings()
            except Exception as e:
                logger.error(f"Error fetching CoAgMet data: {str(e)}")
                results["coagmet_weather"] = []
        
        # USDA agricultural data
        if "usda" in self.clients:
            try:
                current_year = datetime.now().year
                results["usda_crops"] = self.clients["usda"].get_crop_data(state=state, year=current_year)
            except Exception as e:
                logger.error(f"Error fetching USDA data: {str(e)}")
                results["usda_crops"] = []
        
        return results
    
    def health_check_all(self) -> Dict[str, bool]:
        """Check health of all configured API clients"""
        
        health_status = {}
        
        for name, client in self.clients.items():
            try:
                health_status[name] = client.health_check()
            except Exception as e:
                logger.error(f"Health check failed for {name}: {str(e)}")
                health_status[name] = False
        
        return health_status