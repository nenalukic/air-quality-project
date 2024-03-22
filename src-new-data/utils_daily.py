import openmeteo_requests
import requests_cache
import requests
import pandas as pd
from retry_requests import retry
from datetime import datetime
#import json

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

def write_to_files(data, csv_filename):
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
    current_datetime = datetime.now().strftime('%Y-%m-%d')

    # Create unique CSV file name with current date and time
    csv_filename = f"{csv_filename}-{current_datetime}.csv"

    # Write combined DataFrame to a CSV file
    combined_df.to_csv(csv_filename, index=False)

    print(f"Data written to {csv_filename}")
    print(combined_df.info())            