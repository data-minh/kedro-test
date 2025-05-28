"""Project pipelines."""

from kedro.framework.project import find_pipelines
from kedro.pipeline import Pipeline
from .pipelines.dynamic_pipelines.pipeline import create_dynamic_pipeline
from .pipelines.iceberg_pipelines.pipeline import create_iceberg_pipeline
from .pipelines.filter_pipelines.pipeline import create_filter_pipeline
from .pipelines.dynamic_filter_pipelines.pipeline import create_dynamic_filter_pipeline

def register_pipelines() -> dict[str, Pipeline]:
    """Register the project's pipelines.

    Returns:
        A mapping from pipeline names to ``Pipeline`` objects.
    """
    pipelines = find_pipelines()
    # pipelines["dynamic_filter_pipelines"] = create_dynamic_filter_pipeline()
    pipelines["iceberg_pipelines"] = create_iceberg_pipeline()
    # pipelines["filter_pipelines"] = create_filter_pipeline()
    # pipelines["dynamic_pipelines"] = create_dynamic_pipeline()
    pipelines["__default__"] = sum(pipelines.values())

    return pipelines
