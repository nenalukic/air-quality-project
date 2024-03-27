import pandas as pd
from utils import *

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



# Base URL for weather forecast API
url = "https://api.open-meteo.com/v1/forecast"          

# Parameters for fetching weather forecast data
params = [{"latitude": city["latitude"], "longitude": city["longitude"],
        "hourly": ["temperature_2m", "relative_humidity_2m", "precipitation", "rain", "showers",
                    "surface_pressure", "cloud_cover", "visibility", "wind_speed_10m", "wind_gusts_10m"],
        "start_date": "2023-12-01",
	    "end_date": "2024-03-31"}
        for city in cities]         

# Fetch weather forecast data
responses = fetch_openmeteo_data(url, params)     

# Process weather forecast data
all_data = process_weather_data(responses)      

# Write data to files 
write_to_files(all_data, 'weather')     
