import logging
from kedro.pipeline import Pipeline, node, pipeline

from .nodes import load_and_sleep

logger = logging.getLogger(__name__)

def create_filter_pipeline(**kwargs) -> Pipeline:
    return pipeline(
        [
            node(
                func=load_and_sleep,
                inputs="filtered_data_raw",
                outputs="filtered_data_target",
                name="filter_pipeline",
            )
        ]
    )