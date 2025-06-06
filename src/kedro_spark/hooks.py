from kedro.framework.hooks import hook_impl
from pyspark import SparkConf
from pyspark.sql import SparkSession
from kedro.io import DataCatalog
from .partition_helper_functions import check_data_completeness_auto
import logging
from omegaconf import OmegaConf
import re 
from typing import Any, Dict

logger = logging.getLogger(__name__)

try:
    params = OmegaConf.load("./conf/base/globals.yml")
except Exception as e:
    logger.error(f"Failed to load globals.yml: {e}")
    params = {}

def _get_catalog_data(dataset_name: str, catalog_configs: Dict):

    found_config_key = None
    extracted_wildcard_value = None
    placeholder_name = None 

    for config_key in catalog_configs.keys():
        if '{' in config_key and '}' in config_key:
            placeholder_match_in_key = re.search(r"\{(.*?)\}", config_key)
            if placeholder_match_in_key:
                placeholder_name = placeholder_match_in_key.group(1)
                pattern_str = re.escape(config_key).replace(re.escape(f"{{{placeholder_name}}}"), r"(.+)")

                match = re.fullmatch(pattern_str, dataset_name)
                if match:
                    found_config_key = config_key 
                    if match.groups():
                        extracted_wildcard_value = match.group(1)
                    break 
        elif config_key == dataset_name: 
            found_config_key = config_key
            break

    dataset_config = catalog_configs.get(found_config_key)

    return extracted_wildcard_value, dataset_config

class SparkHooks:
    _config_loader = None

    @hook_impl
    def after_context_created(self, context) -> None:
        """Initialises a SparkSession using the config defined in spark.yaml."""
        parameters = context.config_loader["spark"]
        spark_conf = SparkConf().setAll(parameters.items())
        spark_session_conf = (
            SparkSession.builder
            .appName(context.project_path.name)
            .config(conf=spark_conf)
        )
        _spark_session = spark_session_conf.getOrCreate()
        _spark_session.sparkContext.setLogLevel("WARN")

        self._config_loader = context.config_loader

    @hook_impl
    def before_dataset_loaded(self, dataset_name: str) -> None:
        """Hook for validating datasets before they are loaded."""

        logger.debug(f"🔍 Validating dataset '{dataset_name}' before loading...")

        try:
            catalog_configs = self._config_loader["catalog"]
        except Exception as e:
            logger.error(f"Failed to load catalog configuration: {e}")
            raise RuntimeError(f"Could not load catalog configuration: {e}")
        
        # Get information for dataset
        extracted_wildcard_value, dataset_config = _get_catalog_data(dataset_name, catalog_configs)
        
        # Check to see the dataset needed data validation
        data_validation = dataset_config.get("data_validation", False)
        if not data_validation:
            logger.debug(f"🔍 Passing dataset '{dataset_name}")
            return

        try:
            extracted_wildcard_value = int(extracted_wildcard_value)
        except Exception as e:
            raise RuntimeError(f"Failed to covert date range to int: {e}")
        

        try:
            load_args = dataset_config.get("load_args", {})
            file_format = str(dataset_config.get("file_format", "..."))

            if file_format.lower() == 'iceberg':
                catalog_name = load_args.get("catalog", "default")
                namespace = load_args.get("namespace", "default")
                table_name = load_args.get("table_name", "default")
                path = f"{catalog_name}.{'.'.join([namespace] if isinstance(namespace, str) else namespace)}.{table_name}"
            else:
                path = dataset_config.get("filepath")

            partition_column = str(load_args.get("partition_column"))
            processing_date = str(params["PROCESSING_DATE"])

            logger.info(f"✅ Validating dataset '{dataset_name}' at path '{path}'...")

            table_name, missing_dates = check_data_completeness_auto(
                path=path,
                processing_date=processing_date,
                range_date=extracted_wildcard_value,
                partitionBy=partition_column
            )

            if not missing_dates:
                logger.info(
                    f"✅ All expected data is available for table '{table_name}' at path: {path}"
                )
            else:
                error_message = (
                    f"❌ Missing partitions for table '{table_name}' at path: {path}. "
                    f"Missing dates: {missing_dates}"
                )
                logger.error(error_message)
                raise ValueError(error_message)

        except Exception as e:
            logger.error(f"⚠️ Failed to validate dataset '{dataset_name}': {e}")
            raise
