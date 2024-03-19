import openmeteo_requests
import requests_cache
import requests
import pandas as pd
from retry_requests import retry
import json

def fetch_weather_data(url, params):
    """
    Function to fetch weather forecast data from the Open-Meteo API.

    Parameters:
        url (str): The base URL for the Open-Meteo API.
        params (list): A list of dictionaries, each containing parameters for a specific location.

    Returns:
        list: A list of responses containing weather forecast data for each location.
    """
    # Setup the Open-Meteo API client with cache and retry on error
    cache_session = requests_cache.CachedSession('.cache', expire_after=3600)       # Cache session to avoid redundant requests
    retry_session = retry(cache_session, retries=5, backoff_factor=0.2)             # Retry failed requests
    openmeteo = openmeteo_requests.Client(session=retry_session)                    # Initialize the Open-Meteo API client

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
    url = "https://nominatim.openstreetmap.org/reverse"         # Base URL for reverse geocoding service
    params = {
        "format": "json",
        "lat": latitude,
        "lon": longitude
    }
    response = requests.get(url, params=params)                 # Send a GET request to the reverse geocoding service
    if response.status_code == 200:
        data = response.json()
        city_name = data.get('address', {}).get('city', 'Unknown')      # Extract city name from the response data
        return city_name
    else:
        print(f"Failed to retrieve city name for coordinates ({latitude}, {longitude})")
        return "Unknown"

def process_weather_data(responses):
    """
    Function to process weather forecast data responses and format them for further analysis.

    Parameters:
        responses (list): A list of weather forecast data responses for each location.

    Returns:
        list: A list of dictionaries containing processed weather forecast data for each location.
    """
    all_data = []
    for response in responses:
        # Process weather forecast data for each location
        response = response[0]          # Select the first location from the response list
        city_name = get_city_name(response.Latitude(), response.Longitude())        # Retrieve city name
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
                ).strftime('%Y-%m-%d %H:%M:%S').tolist(),  # Convert to string for JSON compatibility
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
        all_data.append(city_data)          # Append processed data for each location to the list
    
    return all_data

def write_to_files(data, json_filename, csv_filename):
    """
    Function to write processed weather forecast data to JSON and CSV files.

    Parameters:
        data (list): Processed weather forecast data for all locations.
        json_filename (str): Filename for the JSON output file.
        csv_filename (str): Filename for the CSV output file.
    """
    # Write to JSON file
    with open(json_filename, 'w') as json_file:
        json.dump(data, json_file)

    # Combine data for all cities into a single DataFrame
    combined_data = []
    for city_data in data:
        city_hourly_data = city_data.pop("hourly_data")
        city_df = pd.DataFrame(data=city_hourly_data)
        city_df['city'] = city_data['city']             # Add city name to DataFrame
        combined_data.append(city_df)
    combined_df = pd.concat(combined_data, ignore_index=True)

    # Write combined DataFrame to a CSV file
    combined_df.to_csv(csv_filename, index=False)

    print(f"Data written to {json_filename} and {csv_filename}")

# Selecting cities from different parts of Spain to observe potential variations in weather patterns
cities = [
    {"latitude": 36.7202, "longitude": -4.4203},  # Malaga (Southern Spain)
    {"latitude": 40.4165, "longitude": -3.7026},  # Madrid (Central Spain)
    {"latitude": 43.2627, "longitude": -2.9253},  # Bilbao (Northern Spain)
    {"latitude": 41.3888, "longitude": 2.159},  # Barcelona (Eastern Spain)
    {"latitude": 42.6, "longitude": -5.5703,}  # León (Northwestern Spain)
    # Additional cities from different regions can be added for further analysis
]

url = "https://api.open-meteo.com/v1/forecast"          # Base URL for weather forecast API
params = [{"latitude": city["latitude"], "longitude": city["longitude"],
        "hourly": ["temperature_2m", "relative_humidity_2m", "precipitation", "rain", "showers",
                    "surface_pressure", "cloud_cover", "visibility", "wind_speed_10m", "wind_gusts_10m"]}
        for city in cities]         # Parameters for fetching weather forecast data

responses = fetch_weather_data(url, params)     # Fetch weather forecast data
all_data = process_weather_data(responses)      # Process weather forecast data
write_to_files(all_data, 'multi_city_weather_forecast.json', 'multi_city_weather_forecast.csv')     # Write data to files
