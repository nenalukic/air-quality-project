import openmeteo_requests
import requests_cache
import pandas as pd
from retry_requests import retry
import json

# Setup the Open-Meteo API client with cache and retry on error
cache_session = requests_cache.CachedSession('.cache', expire_after=3600)
retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
openmeteo = openmeteo_requests.Client(session=retry_session)

# Make sure all required weather variables are listed here
# The order of variables in hourly or daily is important to assign them correctly below
url = "https://api.open-meteo.com/v1/forecast"
params = {
    "latitude": 36.7202,
    "longitude": -4.4203,
    "hourly": ["temperature_2m", "relative_humidity_2m"],
    "timezone": "Europe/Berlin"
}
responses = openmeteo.weather_api(url, params=params)

# Process first location. Add a for-loop for multiple locations or weather models
response = responses[0]

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
    "relative_humidity_2m": hourly.Variables(1).ValuesAsNumpy().tolist()
}

# Write to JSON file
with open('weather_data_test.json', 'w') as json_file:
    json.dump(hourly_data, json_file)

# Create DataFrame
hourly_dataframe = pd.DataFrame(data=hourly_data)

# Write to CSV file
hourly_dataframe.to_csv('weather_data_test.csv', index=False)

print("Data written to weather_data.json and weather_data.csv")
