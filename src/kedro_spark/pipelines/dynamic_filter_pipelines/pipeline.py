import logging
from kedro.pipeline import Pipeline, node, pipeline

from .nodes import load_and_sleep

logger = logging.getLogger(__name__)

def create_dynamic_filter_pipeline(**kwargs) -> Pipeline:
    return pipeline(
        [
            node(
                func=load_and_sleep,
                inputs="dynamic_filter_raw.3",
                outputs="filtered_data_target",
                name="dynamic_filter_pipeline",
            )
        ]
    )


def create_dynamic_filter_pipeline(**kwargs) -> Pipeline:
    return pipeline(
        [
            node(
                func=load_and_sleep,
                inputs="vdsvhdvhs.30",
                outputs="filtered_data_target",
                name="dynamic_filter_pipeline",
            )
        ]
    )