import openmeteo_requests
import requests_cache
import pandas as pd
from retry_requests import retry
import json

def fetch_air_quality_data(url, params):
    # Setup the Open-Meteo API client with cache and retry on error
    cache_session = requests_cache.CachedSession('.cache', expire_after=3600)
    retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
    openmeteo = openmeteo_requests.Client(session=retry_session)

    # Fetch air quality data
    responses = [openmeteo.weather_api(url, params=p) for p in params]
    
    return responses

def process_air_quality_data(responses):
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

def write_to_files(data, json_filename, csv_filename):
    # Write to JSON file
    with open(json_filename, 'w') as json_file:
        json.dump(data, json_file)

    # Create DataFrame
    for city_data in data:
        city_hourly_data = city_data.pop("hourly_data")
        city_dataframe = pd.DataFrame(data=city_hourly_data)
        city_dataframe.to_csv(f"{city_data['city']}_air_quality.csv", index=False)

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

url = "https://air-quality-api.open-meteo.com/v1/air-quality"
params = [{"latitude": city["latitude"], "longitude": city["longitude"],
           "hourly": ["pm10", "pm2_5", "dust", "uv_index", "uv_index_clear_sky", "ammonia", "alder_pollen",
                      "birch_pollen", "grass_pollen", "mugwort_pollen", "olive_pollen", "ragweed_pollen",
                      "european_aqi"],
           "timezone": "Europe/Berlin",
           "past_days": 31,
           "forecast_days": 7}
          for city in cities]

responses = fetch_air_quality_data(url, params)
all_data = process_air_quality_data(responses)
write_to_files(all_data, 'multi_city_air_quality.json', 'multi_city_air_quality.csv')
