-- Docs: https://docs.mage.ai/guides/sql-blocks
CREATE OR REPLACE TABLE air-quality-project-417718.air_quality.weather_aggregated_data AS
SELECT 
    event_date,
    city,
    ROUND(AVG(temperature_2m), 2) AS avg_temperature_2m,
    ROUND(AVG(relative_humidity_2m), 2) AS avg_relative_humidity_2m,
    ROUND(AVG(precipitation), 2) AS avg_precipitation,
    ROUND(AVG(rain), 2) AS avg_rain,
    ROUND(AVG(showers), 2) AS avg_showers,
    ROUND(AVG(surface_pressure), 2) AS avg_surface_pressure,
    ROUND(AVG(cloud_cover), 2) AS avg_cloud_cover,
    ROUND(AVG(visibility), 2) AS avg_visibility,
    ROUND(AVG(wind_speed_10m), 2) AS avg_wind_speed_10m,
    ROUND(AVG(wind_gusts_10m), 2) AS avg_wind_gusts_10m,
    ROUND(MIN(temperature_2m), 2) AS min_temperature_2m,
    ROUND(MIN(relative_humidity_2m), 2) AS min_relative_humidity_2m,
    ROUND(MIN(precipitation), 2) AS min_precipitation,
    ROUND(MIN(rain), 2) AS min_rain,
    ROUND(MIN(showers), 2) AS min_showers,
    ROUND(MIN(surface_pressure), 2) AS min_surface_pressure,
    ROUND(MIN(cloud_cover), 2) AS min_cloud_cover,
    ROUND(MIN(visibility), 2) AS min_visibility,
    ROUND(MIN(wind_speed_10m), 2) AS min_wind_speed_10m,
    ROUND(MIN(wind_gusts_10m), 2) AS min_wind_gusts_10m,
    ROUND(MAX(temperature_2m), 2) AS max_temperature_2m,
    ROUND(MAX(relative_humidity_2m), 2) AS max_relative_humidity_2m,
    ROUND(MAX(precipitation), 2) AS max_precipitation,
    ROUND(MAX(rain), 2) AS max_rain,
    ROUND(MAX(showers), 2) AS max_showers,
    ROUND(MAX(surface_pressure), 2) AS max_surface_pressure,
    ROUND(MAX(cloud_cover), 2) AS max_cloud_cover,
    ROUND(MAX(visibility), 2) AS max_visibility,
    ROUND(MAX(wind_speed_10m), 2) AS max_wind_speed_10m,
    ROUND(MAX(wind_gusts_10m), 2) AS max_wind_gusts_10m
FROM 
    air-quality-project-417718.air_quality.weather_partitioned
GROUP BY
    event_date, city;