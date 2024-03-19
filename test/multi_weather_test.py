import openmeteo_requests
import requests_cache
import requests
import pandas as pd
from retry_requests import retry
import json

def fetch_weather_data(url, params):
    # Setup the Open-Meteo API client with cache and retry on error
    cache_session = requests_cache.CachedSession('.cache', expire_after=3600)
    retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
    openmeteo = openmeteo_requests.Client(session=retry_session)

    # Fetch weather data
    responses = [openmeteo.weather_api(url, params=p) for p in params]
    
    return responses

def get_city_name(latitude, longitude):
    # Reverse geocoding with OpenStreetMap Nominatim
    url = "https://nominatim.openstreetmap.org/reverse"
    params = {
        "format": "json",
        "lat": latitude,
        "lon": longitude
    }
    response = requests.get(url, params=params)
    if response.status_code == 200:
        data = response.json()
        city_name = data.get('address', {}).get('city', 'Unknown')
        return city_name
    else:
        print(f"Failed to retrieve city name for coordinates ({latitude}, {longitude})")
        return "Unknown"

def process_weather_data(responses):
    all_data = []
    for response in responses:
        # Process first location. Add a for-loop for multiple locations or weather models
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
                ).strftime('%Y-%m-%d %H:%M:%S').tolist(),  # Convert to string for JSON compatibility
                "temperature_2m": response.Hourly().Variables(0).ValuesAsNumpy().tolist()
            }
        }
        all_data.append(city_data)
    
    return all_data

def write_to_files(data, json_filename, csv_filename):
    # Write to JSON file
    with open(json_filename, 'w') as json_file:
        json.dump(data, json_file)

    # Combine data for all cities into a single DataFrame
    combined_data = []
    for city_data in data:
        city_hourly_data = city_data.pop("hourly_data")
        city_df = pd.DataFrame(data=city_hourly_data)
        city_df['city'] = city_data['city']  # Add city name to DataFrame
        combined_data.append(city_df)
    combined_df = pd.concat(combined_data, ignore_index=True)

    # Write to CSV file
    combined_df.to_csv(csv_filename, index=False)

    print(f"Data written to {json_filename} and {csv_filename}")

# Example usage
cities = [
    {"latitude": 36.7202, "longitude": -4.4203},  # Malaga
    {"latitude": 40.4165, "longitude": -3.7026},  # Madrid
    {"latitude": 43.2627, "longitude": -2.9253},  # Bilbao
    {"latitude": 41.3888, "longitude": 2.159},  # Barcelona
    {"latitude": 42.6, "longitude": -5.5703,}  # Castille de Leon
    # Add more cities as needed
]

url = "https://api.open-meteo.com/v1/forecast"
params = [{"latitude": city["latitude"], "longitude": city["longitude"],
        "hourly": ["temperature_2m"],
        "forecast_days": 1}
        for city in cities]

responses = fetch_weather_data(url, params)
all_data = process_weather_data(responses)
write_to_files(all_data, 'multi_city_weather_forecast_test.json', 'multi_city_weather_forecast_test.csv')
