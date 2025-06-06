import logging
from kedro.pipeline import Pipeline, node, pipeline

from .nodes import load_and_sleep

logger = logging.getLogger(__name__)

def create_dynamic_filter_pipeline(**kwargs) -> Pipeline:
    return pipeline(
        [
            node(
                func=load_and_sleep,
                inputs="raw_csv",
                outputs="intermediate_parquet_local.5",
                name="raw_to_intermediate_in_local",
            ),
            node(
                func=load_and_sleep,
                inputs="intermediate_parquet_local.5",
                outputs="reporting_parquet_local.5",
                name="intermediate_to_reporting_in_local",
            )
        ]
    )
