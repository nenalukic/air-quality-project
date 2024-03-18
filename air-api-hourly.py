import openmeteo_requests
import requests_cache
import pandas as pd
from retry_requests import retry
import json

def fetch_weather_data(url, params):
    # Setup the Open-Meteo API client with cache and retry on error
    cache_session = requests_cache.CachedSession('.cache', expire_after=3600)
    retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
    openmeteo = openmeteo_requests.Client(session=retry_session)

    # Fetch weather data
    responses = openmeteo.weather_api(url, params=params)
    
    return responses

def process_weather_data(response):
    # Process first location. Add a for-loop for multiple locations or weather models
    response = response[0]
    print(f"Coordinates {response.Latitude()}°N {response.Longitude()}°E")
    print(f"Elevation {response.Elevation()} m asl")
    print(f"Timezone {response.Timezone()} {response.TimezoneAbbreviation()}")
    print(f"Timezone difference to GMT+0 {response.UtcOffsetSeconds()} s")

    # Process hourly data. The order of variables needs to be the same as requested.
    hourly = response.Hourly()
    hourly_data = {
        "date": pd.date_range(
            start=pd.to_datetime(hourly.Time(), unit="s", utc=True),
            end=pd.to_datetime(hourly.TimeEnd(), unit="s", utc=True),
            freq=pd.Timedelta(seconds=hourly.Interval()),
            inclusive="left"
        ).strftime('%Y-%m-%d %H:%M:%S').tolist(),  # Convert to string for JSON compatibility
        "pm10": hourly.Variables(0).ValuesAsNumpy().tolist(),
        "pm2_5": hourly.Variables(1).ValuesAsNumpy().tolist(),
        "dust": hourly.Variables(2).ValuesAsNumpy().tolist(),
        "uv_index": hourly.Variables(3).ValuesAsNumpy().tolist(),
        "uv_index_clear_sky": hourly.Variables(4).ValuesAsNumpy().tolist(),
        "ammonia": hourly.Variables(5).ValuesAsNumpy().tolist(),
        "alder_pollen": hourly.Variables(6).ValuesAsNumpy().tolist(),
        "birch_pollen": hourly.Variables(7).ValuesAsNumpy().tolist(),
        "grass_pollen": hourly.Variables(8).ValuesAsNumpy().tolist(),
        "mugwort_pollen": hourly.Variables(9).ValuesAsNumpy().tolist(),
        "olive_pollen": hourly.Variables(10).ValuesAsNumpy().tolist(),
        "ragweed_pollen": hourly.Variables(11).ValuesAsNumpy().tolist(),
        "european_aqi": hourly.Variables(12).ValuesAsNumpy().tolist()
    }
    
    return hourly_data

def write_to_files(data, json_filename, csv_filename):
    # Write to JSON file
    with open(json_filename, 'w') as json_file:
        json.dump(data, json_file)

    # Create DataFrame
    hourly_dataframe = pd.DataFrame(data=data)

    # Write to CSV file
    hourly_dataframe.to_csv(csv_filename, index=False)

    print(f"Data written to {json_filename} and {csv_filename}")

# Example usage
url = "https://air-quality-api.open-meteo.com/v1/air-quality"
params = {
    "latitude": 36.7202,
    "longitude": -4.4203,
    "hourly": ["pm10", "pm2_5", "dust", "uv_index", "uv_index_clear_sky", "ammonia", "alder_pollen", "birch_pollen",
               "grass_pollen", "mugwort_pollen", "olive_pollen", "ragweed_pollen", "european_aqi"],
    "timezone": "Europe/Berlin",
    "past_days": 31,
    "forecast_days": 7
}

responses = fetch_weather_data(url, params)
hourly_data = process_weather_data(responses[0])
write_to_files(hourly_data, 'air_quality_data.json', 'air_quality_data.csv')
