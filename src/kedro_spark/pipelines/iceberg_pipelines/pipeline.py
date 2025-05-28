import logging
from kedro.pipeline import Pipeline, node, pipeline
from omegaconf import OmegaConf

from ...helper_functions import *

from .nodes import transform

logger = logging.getLogger(__name__)

params = OmegaConf.load("./conf/base/globals.yml")

def create_iceberg_pipeline(**kwargs) -> Pipeline:
    return pipeline(
        [
            node(
                func=transform,
                inputs="reviews_iceberg_raw",
                outputs="reviews_iceberg",
                name="iceberg_pipeline",
            )
        ]
    )
