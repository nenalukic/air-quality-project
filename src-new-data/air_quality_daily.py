import pandas as pd
from utils import *


def process_air_quality_data(responses):
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

# Base URL for air quality API
url = "https://air-quality-api.open-meteo.com/v1/air-quality"

# Parameters for fetching air quality data
params = [{"latitude": city["latitude"], "longitude": city["longitude"],
        "hourly": ["pm10", "pm2_5", "dust", "uv_index", "uv_index_clear_sky", "ammonia", "alder_pollen",
                    "birch_pollen", "grass_pollen", "mugwort_pollen", "olive_pollen", "ragweed_pollen",
                    "european_aqi"],
        "timezone": "Europe/Berlin",
	    "forecast_days": 1}
        for city in cities]         

# Fetch air quality data
responses = fetch_openmeteo_data(url, params)         

# Process air quality data
all_data = process_air_quality_data(responses)          

# Write data to files
write_to_files(all_data, 'air-quality')       
