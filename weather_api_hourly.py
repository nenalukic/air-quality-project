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
        "temperature_2m": hourly.Variables(0).ValuesAsNumpy().tolist(),
        "relative_humidity_2m": hourly.Variables(1).ValuesAsNumpy().tolist(),
        "precipitation": hourly.Variables(2).ValuesAsNumpy().tolist(),
        "rain": hourly.Variables(3).ValuesAsNumpy().tolist(),
        "showers": hourly.Variables(4).ValuesAsNumpy().tolist(),
        "surface_pressure": hourly.Variables(5).ValuesAsNumpy().tolist(),
        "cloud_cover": hourly.Variables(6).ValuesAsNumpy().tolist(),
        "visibility": hourly.Variables(7).ValuesAsNumpy().tolist(),
        "wind_speed_10m": hourly.Variables(8).ValuesAsNumpy().tolist(),
        "wind_gusts_10m": hourly.Variables(9).ValuesAsNumpy().tolist()
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
url = "https://api.open-meteo.com/v1/forecast"
params = {
    "latitude": 36.7202,
    "longitude": -4.4203,
    "hourly": ["temperature_2m", "relative_humidity_2m", "precipitation", "rain", "showers", "surface_pressure",
               "cloud_cover", "visibility", "wind_speed_10m", "wind_gusts_10m"]
}

responses = fetch_weather_data(url, params)
hourly_data = process_weather_data(responses[0])
write_to_files(hourly_data, 'weather_forecast_data.json', 'weather_forecast_data.csv')
