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

def process_weather_data(responses):
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
                "temperature_2m": response.Hourly().Variables(0).ValuesAsNumpy().tolist(),
                "relative_humidity_2m": response.Hourly().Variables(1).ValuesAsNumpy().tolist(),
                "precipitation": response.Hourly().Variables(2).ValuesAsNumpy().tolist(),
                "rain": response.Hourly().Variables(3).ValuesAsNumpy().tolist(),
                "showers": response.Hourly().Variables(4).ValuesAsNumpy().tolist(),
                "surface_pressure": response.Hourly().Variables(5).ValuesAsNumpy().tolist(),
                "cloud_cover": response.Hourly().Variables(6).ValuesAsNumpy().tolist(),
                "visibility": response.Hourly().Variables(7).ValuesAsNumpy().tolist(),
                "wind_speed_10m": response.Hourly().Variables(8).ValuesAsNumpy().tolist(),
                "wind_gusts_10m": response.Hourly().Variables(9).ValuesAsNumpy().tolist()
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

    # Define schema for the DataFrame
    schema = {
        "date": "datetime64[s]",
        "temperature_2m": "float64",
        "relative_humidity_2m": "float64",
        "precipitation": "float64",
        "rain": "float64",
        "showers": "float64",
        "surface_pressure": "float64",
        "cloud_cover": "float64",
        "visibility": "float64",
        "wind_speed_10m": "float64",
        "wind_gusts_10m": "float64",
        "city": "object",
        "current_datetime": "datetime64[s]",
        "event_date": "datetime64[s]"
    }

    print(combined_df.info())

    return combined_df

@data_loader
def load_data_from_api(*args, **kwargs):
    # Base URL for weather API
    url = "https://api.open-meteo.com/v1/forecast"

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

    # Parameters for fetching weather forecast data
    params = [{"latitude": city["latitude"], "longitude": city["longitude"],
            "hourly": ["temperature_2m", "relative_humidity_2m", "precipitation", "rain", "showers",
                        "surface_pressure", "cloud_cover", "visibility", "wind_speed_10m", "wind_gusts_10m"],
            "start_date": "2023-12-01",
	        "end_date": "2024-03-31"}
            for city in cities]         

    # Fetch weather data
    responses = fetch_openmeteo_data(url, params)         

    # Process weather data
    all_data = process_weather_data(responses)          

    # Write data to files
    weather_result_df = write_to_files(all_data)

    print(weather_result_df.head())

    return weather_result_df

@test
def test_output(output) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
