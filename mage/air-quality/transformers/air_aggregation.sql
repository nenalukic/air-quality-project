-- Docs: https://docs.mage.ai/guides/sql-blocks
CREATE TABLE air-quality-project-417718.air_quality.air_aggregated_data AS
SELECT 
    event_date,
    city,
    ROUND(AVG(pm10), 2) AS avg_pm10,
    ROUND(AVG(pm2_5), 2) AS avg_pm2_5,
    ROUND(AVG(dust), 2) AS avg_dust,
    ROUND(AVG(uv_index), 2) AS avg_uv_index,
    ROUND(AVG(uv_index_clear_sky), 2) AS avg_uv_index_clear_sky,
    ROUND(AVG(ammonia), 2) AS avg_ammonia,
    ROUND(AVG(alder_pollen), 2) AS avg_alder_pollen,
    ROUND(AVG(birch_pollen), 2) AS avg_birch_pollen,
    ROUND(AVG(grass_pollen), 2) AS avg_grass_pollen,
    ROUND(AVG(mugwort_pollen), 2) AS avg_mugwort_pollen,
    ROUND(AVG(olive_pollen), 2) AS avg_olive_pollen,
    ROUND(AVG(ragweed_pollen), 2) AS avg_ragweed_pollen,
    ROUND(AVG(european_aqi), 2) AS avg_european_aqi,
    ROUND(MIN(pm10), 2) AS min_pm10,
    ROUND(MIN(pm2_5), 2) AS min_pm2_5,
    ROUND(MIN(dust), 2) AS min_dust,
    ROUND(MIN(uv_index), 2) AS min_uv_index,
    ROUND(MIN(uv_index_clear_sky), 2) AS min_uv_index_clear_sky,
    ROUND(MIN(ammonia), 2) AS min_ammonia,
    ROUND(MIN(alder_pollen), 2) AS min_alder_pollen,
    ROUND(MIN(birch_pollen), 2) AS min_birch_pollen,
    ROUND(MIN(grass_pollen), 2) AS min_grass_pollen,
    ROUND(MIN(mugwort_pollen), 2) AS min_mugwort_pollen,
    ROUND(MIN(olive_pollen), 2) AS min_olive_pollen,
    ROUND(MIN(ragweed_pollen), 2) AS min_ragweed_pollen,
    ROUND(MIN(european_aqi), 2) AS min_european_aqi,
    ROUND(MAX(pm10), 2) AS max_pm10,
    ROUND(MAX(pm2_5), 2) AS max_pm2_5,
    ROUND(MAX(dust), 2) AS max_dust,
    ROUND(MAX(uv_index), 2) AS max_uv_index,
    ROUND(MAX(uv_index_clear_sky), 2) AS max_uv_index_clear_sky,
    ROUND(MAX(ammonia), 2) AS max_ammonia,
    ROUND(MAX(alder_pollen), 2) AS max_alder_pollen,
    ROUND(MAX(birch_pollen), 2) AS max_birch_pollen,
    ROUND(MAX(grass_pollen), 2) AS max_grass_pollen,
    ROUND(MAX(mugwort_pollen), 2) AS max_mugwort_pollen,
    ROUND(MAX(olive_pollen), 2) AS max_olive_pollen,
    ROUND(MAX(ragweed_pollen), 2) AS max_ragweed_pollen,
    ROUND(MAX(european_aqi), 2) AS max_european_aqi

FROM 
    air-quality-project-417718.air_quality.airquality_partitioned
GROUP BY
    event_date, city;
