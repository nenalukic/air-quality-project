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
    responses = [openmeteo.weather_api(url, params=p) for p in params]
    
    return responses

def process_weather_data(responses):
    all_data = []
    for response in responses:
        # Process first location. Add a for-loop for multiple locations or weather models
        response = response[0]
        city_data = {
            "city": f"{response.Latitude()}°N {response.Longitude()}°E",
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
        all_data.append(city_data)
    
    return all_data

def write_to_files(data, json_filename, csv_filename):
    # Write to JSON file
    with open(json_filename, 'w') as json_file:
        json.dump(data, json_file)

    # Create DataFrame
    for city_data in data:
        city_hourly_data = city_data.pop("hourly_data")
        city_dataframe = pd.DataFrame(data=city_hourly_data)
        city_dataframe.to_csv(f"{city_data['city']}_weather_forecast.csv", index=False)

    print(f"Data written to {json_filename} and CSV files for each city")

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
           "hourly": ["temperature_2m", "relative_humidity_2m", "precipitation", "rain", "showers",
                      "surface_pressure", "cloud_cover", "visibility", "wind_speed_10m", "wind_gusts_10m"]}
          for city in cities]

responses = fetch_weather_data(url, params)
all_data = process_weather_data(responses)
write_to_files(all_data, 'multi_city_weather_forecast.json', 'multi_city_weather_forecast.csv')
