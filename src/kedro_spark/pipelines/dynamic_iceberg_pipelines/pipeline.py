import logging
from kedro.pipeline import Pipeline, node, pipeline

from .nodes import load_and_sleep, load_and_sleep_test

logger = logging.getLogger(__name__)

def create_dynamic_iceberg_pipeline(**kwargs) -> Pipeline:
    return pipeline(
        [
            node(
                func=load_and_sleep,
                inputs="raw_csv",
                outputs="intermediate_parquet_s3.5",
                name="raw_to_intermediate_in_s3",
            ),
            node(
                func=load_and_sleep,
                inputs="intermediate_parquet_s3.5",
                outputs="primary_icebreg.5",
                name="intermediate_to_primary_iceberg",
            ),
            node(
                func=load_and_sleep,
                inputs="primary_icebreg.5",
                outputs="feature_icebreg.5",
                name="primary_to_feature_iceberg",
            )
        ]
    )