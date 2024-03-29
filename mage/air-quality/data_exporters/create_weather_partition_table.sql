-- Docs: https://docs.mage.ai/guides/sql-blocks
CREATE OR REPLACE TABLE `air-quality-project-417718.air_quality.weather_partitioned`
    PARTITION BY DATE(event_date)
    CLUSTER BY city AS (
    SELECT * FROM `air-quality-project-417718.air_quality.weather`
    );