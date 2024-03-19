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

# Write to JSON file
with open('air_quality_data.json', 'w') as json_file:
    json.dump(hourly_data, json_file)

# Create DataFrame
hourly_dataframe = pd.DataFrame(data=hourly_data)

# Write to CSV file
hourly_dataframe.to_csv('air_quality_data.csv', index=False)

print("Data written to air_quality_data.json and air_quality_data.csv")
