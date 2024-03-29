from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.bigquery import BigQuery
from os import path
from pandas import DataFrame

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform_in_bigquery(*args, **kwargs) -> DataFrame:
    """
    Performs a transformation in BigQuery
    """
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'

    # Specify your SQL transformation query
    query = CREATE OR REPLACE TABLE `air-quality-project-417718.air_quality.airquality_partitioned`
    PARTITION BY DATE(event_date)
    CLUSTER BY city AS (
    SELECT * FROM `terraform-de-411309.ny_taxi_dataset.greentaxi_2022`
    );



    with BigQuery.with_config(ConfigFileLoader(config_path, config_profile)) as loader:
        # Write queries to transform your dataset with
        loader.execute(query)
        return 


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
