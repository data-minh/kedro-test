import datetime as dt
import logging
from pathlib import Path
from dateutil.relativedelta import relativedelta
from kedro.framework.session import KedroSession
from kedro.pipeline import Pipeline, node, pipeline
from omegaconf import OmegaConf

from ...helper_functions import *

from .nodes import *

logger = logging.getLogger(__name__)

params = OmegaConf.load("./conf/base/globals.yml")
# session =  KedroSession.create(project_path=Path.cwd())
# context = session.load_context()
# catalog = context.catalog    

def create_dynamic_pipeline(**kwargs) -> Pipeline:
    pipelines = []
    pipelines += create_iter_pipeline(
        period="daily",
        inputs=[
            "01_raw.{}.foo_raw_df", 
            ],
        outputs="02_intermediate.{}.foo_intermediate_df",
        func=foo,
    )
    return Pipeline(pipelines)

# def create_dynamic_pipeline(**kwargs) -> Pipeline:
#     pipelines = [
#         node(
#             func=foo, 
#             inputs=['01_raw.{2025-01-01}.foo_raw_df', 'params:2025-01-01'], 
#             outputs='02_intermediate.2025-01-01.foo_intermediate_df', 
#             name='foo.2025-01-01')
#     ]

#     return pipeline(pipelines)

# test_date_str = '2025-01-01' 

# def create_dynamic_pipeline(**kwargs) -> Pipeline:
#     pipelines = [
#         node(
#             foo, 
#             [f"01_raw.{test_date_str}.foo_raw_df", f"params:{test_date_str}"], 
#             f"02_intermediate.{test_date_str}.foo_intermediate_df"
#         )
#     ]
#     return pipeline(pipelines)