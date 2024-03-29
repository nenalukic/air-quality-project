import io
import pandas as pd
import requests
if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test
import openmeteo_requests
import requests_cache
import requests
import pandas as pd
from retry_requests import retry
from datetime import datetime


def fetch_openmeteo_data(url, params):
    """
    Function to fetch data from the Open-Meteo API.

    Parameters:
        url (str): The base URL for different the Open-Meteo APIs: weather, air quality etc. 
        params (list): A list of dictionaries, each containing parameters for a specific location.

    Returns:
        list: A list of responses containing data for each location.
    """
    # Setup the Open-Meteo API client with cache and retry on error
    # Cache session to avoid redundant requests
    cache_session = requests_cache.CachedSession('.cache', expire_after=3600)       
    
    # Retry failed requests
    retry_session = retry(cache_session, retries=5, backoff_factor=0.2)             
    
    # Initialize the Open-Meteo API client
    openmeteo = openmeteo_requests.Client(session=retry_session)                    

    # Fetch weather forecast data for each location
    responses = [openmeteo.weather_api(url, params=p) for p in params]
    
    return responses

def get_city_name(latitude, longitude):
    """
    Function to retrieve the city name based on latitude and longitude using reverse geocoding.

    Parameters:
        latitude (float): The latitude coordinate of the location.
        longitude (float): The longitude coordinate of the location.

    Returns:
        str: The name of the city corresponding to the provided coordinates.
    """
    # Reverse geocoding with OpenStreetMap Nominatim
    # Base URL for reverse geocoding service
    url = "https://nominatim.openstreetmap.org/reverse"         
    params = {
        "format": "json",
        "lat": latitude,
        "lon": longitude
    }
    
    # Send a GET request to the reverse geocoding service
    response = requests.get(url, params=params)                 
    if response.status_code == 200:
        data = response.json()
        
        # Extract city name from the response data
        city_name = data.get('address', {}).get('city', 'Unknown')      
        return city_name
    else:
        print(f"Failed to retrieve city name for coordinates ({latitude}, {longitude})")
        return "Unknown"

def process_air_quality_data(responses):
    """
    Function to process air quality data responses and format them for further analysis.

    Parameters:
        responses (list): A list of air quality data responses for each location.

    Returns:
        list: A list of dictionaries containing processed air quality data for each location.
    """
    all_data = []
    for response in responses:
        # Process air quality data for each location
        response = response[0]
        city_name = get_city_name(response.Latitude(), response.Longitude())
        city_data = {
            "city": city_name,
            "elevation": response.Elevation(),
            "timezone": f"{response.Timezone()} {response.TimezoneAbbreviation()}",
            "utc_offset": response.UtcOffsetSeconds(),
            "hourly_data": {
                "date": pd.date_range(
                    start=pd.to_datetime(response.Hourly().Time(), unit="s", utc=True),
                    end=pd.to_datetime(response.Hourly().TimeEnd(), unit="s", utc=True),
                    freq=pd.Timedelta(seconds=response.Hourly().Interval()),
                    inclusive="left"
                ),
                "pm10": response.Hourly().Variables(0).ValuesAsNumpy().tolist(),
                "pm2_5": response.Hourly().Variables(1).ValuesAsNumpy().tolist(),
                "dust": response.Hourly().Variables(2).ValuesAsNumpy().tolist(),
                "uv_index": response.Hourly().Variables(3).ValuesAsNumpy().tolist(),
                "uv_index_clear_sky": response.Hourly().Variables(4).ValuesAsNumpy().tolist(),
                "ammonia": response.Hourly().Variables(5).ValuesAsNumpy().tolist(),
                "alder_pollen": response.Hourly().Variables(6).ValuesAsNumpy().tolist(),
                "birch_pollen": response.Hourly().Variables(7).ValuesAsNumpy().tolist(),
                "grass_pollen": response.Hourly().Variables(8).ValuesAsNumpy().tolist(),
                "mugwort_pollen": response.Hourly().Variables(9).ValuesAsNumpy().tolist(),
                "olive_pollen": response.Hourly().Variables(10).ValuesAsNumpy().tolist(),
                "ragweed_pollen": response.Hourly().Variables(11).ValuesAsNumpy().tolist(),
                "european_aqi": response.Hourly().Variables(12).ValuesAsNumpy().tolist()
            }
        }
        all_data.append(city_data)
    
    return all_data

def write_to_files(data):
    """
    Function to write processed weather forecast data to CSV files.

    Parameters:
        data (list): Processed weather forecast data for all locations.
        csv_filename (str): Filename for the CSV output file.
    """

    # Combine data for all cities into a single DataFrame
    combined_data = []
    for city_data in data:
        city_hourly_data = city_data.pop("hourly_data")
        city_df = pd.DataFrame(data=city_hourly_data)
        
        # Add city name to DataFrame
        city_df['city'] = city_data['city']             
        combined_data.append(city_df)
    combined_df = pd.concat(combined_data, ignore_index=True)

    # Get current date and time for file naming
    #current_datetime = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    current_datetime = pd.Timestamp.now()
    combined_df["current_datetime"] = current_datetime
    event_date = combined_df['date'].dt.date
    combined_df["event_date"] = pd.to_datetime(event_date) 

    print(combined_df.dtypes)

    # Define schema for the DataFrame
    schema = {
        "date": "datetime64[s]",
        "pm10": "float64",
        "pm2_5": "float64",
        "dust": "float64",
        "uv_index": "float64",
        "uv_index_clear_sky": "float64",
        "ammonia": "float64",
        "alder_pollen": "float64",
        "birch_pollen": "float64",
        "grass_pollen": "float64",
        "mugwort_pollen": "float64",
        "olive_pollen": "float64",
        "ragweed_pollen": "float64",
        "european_aqi": "float64",
        "city": "object",
        "current_datetime": "datetime64[s]",
        "event_date": "datetime64[s]"
    }

    print(combined_df.info())

    return combined_df

@data_loader
def load_data_from_api(*args, **kwargs):

    # Base URL for air quality API
    url = "https://air-quality-api.open-meteo.com/v1/air-quality"

    # Selecting cities from different parts of Spain to observe potential variations 
    # in weather patterns
    # Additional cities from different regions can be added for further analysis
    cities = [
        {"latitude": 36.7202, "longitude": -4.4203},  # Malaga (Southern Spain)
        {"latitude": 40.4165, "longitude": -3.7026},  # Madrid (Central Spain)
        {"latitude": 43.2627, "longitude": -2.9253},  # Bilbao (Northern Spain)
        {"latitude": 41.3888, "longitude": 2.159}     # Barcelona (Eastern Spain)
        #{"latitude": 42.6, "longitude": -5.5703,}     # León (Northwestern Spain)
        
    ]

    # Parameters for fetching air quality data
    params = [{"latitude": city["latitude"], "longitude": city["longitude"],
            "hourly": ["pm10", "pm2_5", "dust", "uv_index", "uv_index_clear_sky", "ammonia", "alder_pollen",
                        "birch_pollen", "grass_pollen", "mugwort_pollen", "olive_pollen", "ragweed_pollen",
                        "european_aqi"],
            "timezone": "Europe/Berlin",
            "start_date": "2023-12-01",
	        "end_date": "2024-03-31"}
            for city in cities]         

    # Fetch air quality data
    responses = fetch_openmeteo_data(url, params)         

    # Process air quality data
    all_data = process_air_quality_data(responses)          

    # Write data to files
    result_df = write_to_files(all_data)

    print(result_df.head())

    return result_df

@test
def test_output(output) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
