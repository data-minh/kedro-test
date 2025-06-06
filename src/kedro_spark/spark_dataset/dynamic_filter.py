from typing import Any, Optional
from kedro_datasets.spark import SparkDataset
from pyspark.sql import DataFrame
from kedro_datasets._utils.spark_utils import get_spark
from omegaconf import OmegaConf
import inspect
from datetime import datetime, timedelta
import logging

from ..partition_helper_functions import check_data_completeness_auto

logger = logging.getLogger(__name__)

try:
    params = OmegaConf.load("./conf/base/globals.yml")
except Exception as e:
    logger.error(f"Failed to load globals.yml: {e}")
    params = {}

def _strip_dbfs_prefix(path: str, prefix: str = "/dbfs") -> str:
    return path[len(prefix):] if path.startswith(prefix) else path

class DynamicFilterDataset(SparkDataset):
    """
    ``DynamicFilteredSparkDataset`` extends Kedro's SparkDataset to provide powerful filtering
    capabilities applied directly during the load process, optimizing for partition pruning.
    It checks for data completeness within a specified date range and constructs SQL filters
    based on a partition column (assumed to be a date-like column).
    """
    def __init__(
        self,
        data_validation: Optional[bool] = None,
        **kwargs: Any
    ) -> None:

        super().__init__(**kwargs)
        self._filters = str(None)

        # Get parameters for dynamic filter
        self._range_date = self._load_args.get("range_date", "*")
        self._partition_column = self._load_args.get("partition_column", "**")

        if self._range_date != "*":
            self._range_date = int(self._range_date)

        if self._load_args.get("filters", "1=1") != "1=1":
            self._filters = self._load_args.get("filters", "1=1")

        # Get the load path (handled by the parent class)
        self.load_path = _strip_dbfs_prefix(self._fs_prefix + str(self._get_load_path()))

        try:
            self._processing_date = str(params["PROCESSING_DATE"])
        except KeyError:
            function_name = inspect.currentframe().f_code.co_name
            raise ValueError(f"[{function_name}] Missing required parameter: 'PROCESSING_DATE'")

        
    def _get_date_range_for_filter(self) -> tuple[str, str]:
        """
        Calculates the start and end dates for the dynamic filter based on
        _processing_date and _range_date.
        """
        end_date = datetime.strptime(self._processing_date, "%Y-%m-%d").date()
        start_date = end_date - timedelta(days=self._range_date - 1)
        
        return start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d")


    def _load(self) -> DataFrame:
        spark = get_spark()

        read_obj = spark.read.format(self._file_format)

        # Apply schema if defined
        if self._schema:
             read_obj = read_obj.schema(self._schema)
        
        df = read_obj.load(self.load_path, **self._load_args)

        # NOW, apply the filter on the DataFrame.
        # Spark's optimizer will attempt to push this down.
        # Determine the dynamic date filter
        if ((self._range_date == "*" and self._partition_column == "**") or self._range_date == "*" or self._partition_column == "**") and self._load_args.get("filters", "1=1") == "1=1":
            return df
        else:
        
            if self._load_args.get("filters", "1=1") != "1=1":
                if self._filters:
                    if isinstance(self._filters, str):
                        df = df.filter(self._filters)
                    elif isinstance(self._filters, list):
                        for f in self._filters:
                            df = df.filter(f)

            else:
                start_date, end_date = self._get_date_range_for_filter()
                
                if start_date == end_date:
                    self._filters = f"{self._partition_column} = DATE('{end_date}')"
                else:
                    self._filters = (
                        f"{self._partition_column} >= DATE('{start_date}') AND "
                        f"{self._partition_column} <= DATE('{end_date}')"
                    )
                if self._filters:
                    if isinstance(self._filters, str):
                        df = df.filter(self._filters)
                    elif isinstance(self._filters, list):
                        for f in self._filters:
                            df = df.filter(f)

            return df


