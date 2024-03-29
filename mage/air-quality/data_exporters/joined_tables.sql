-- Docs: https://docs.mage.ai/guides/sql-blocks
CREATE TABLE air-quality-project-417718.air_quality.combined_data AS
SELECT 
    w.event_date AS date,
    w.city,
    w.temperature_2m,
    w.relative_humidity_2m,
    w.precipitation,
    w.rain,
    w.showers,
    w.surface_pressure,
    w.cloud_cover,
    w.visibility,
    w.wind_speed_10m,
    w.wind_gusts_10m,
    a.pm10,
    a.pm2_5,
    a.dust,
    a.uv_index,
    a.uv_index_clear_sky,
    a.ammonia,
    a.alder_pollen,
    a.birch_pollen,
    a.grass_pollen,
    a.mugwort_pollen,
    a.olive_pollen,
    a.ragweed_pollen,
    a.european_aqi
FROM 
    air-quality-project-417718.air_quality.weather_partitioned w
JOIN 
    air-quality-project-417718.air_quality.airquality_partitioned a ON w.event_date = a.event_date AND w.city = a.city;
