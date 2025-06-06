from typing import Any, Dict, List, Union
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

class DynamicFilteredSparkDataset(SparkDataset):
    """
    ``DynamicFilteredSparkDataset`` extends Kedro's SparkDataset to provide powerful filtering
    capabilities applied directly during the load process, optimizing for partition pruning.
    It checks for data completeness within a specified date range and constructs SQL filters
    based on a partition column (assumed to be a date-like column).
    """
    def __init__(
        self,
        *,
        range_date: Union[str, int] = None,
        partition_column: str = None,
        **kwargs: Any
    ) -> None:
        """Creates a new instance of ``DynamicFilteredSparkDataset``.

        Args:
            processing_date (str): The end date (inclusive) for the data range in 'YYYY-MM-DD' format.
            range_date (int): The number of days to include in the range, ending on `processing_date`.
                              Must be a non-negative integer.
            partition_column (str): The name of the partition column (e.g., 'event_date', 'date').
                                    This column is assumed to contain date-like values for filtering.
            filters (Union[str, List[str]], optional): Additional static SQL-like filter string(s)
                                                        to apply. Defaults to None.
            kwargs: All other arguments typically passed to Kedro's `SparkDataset`,
                    e.g., `filepath`, `file_format`, `load_args`, `save_args`, `spark_session`, `schema`, `fs_prefix`.
        """
        super().__init__(**kwargs)
        self._range_date = int(range_date)
        self._filters = str(None)
        self._partition_column = partition_column

        # Get the load path (handled by the parent class)
        self.load_path = _strip_dbfs_prefix(self._fs_prefix + str(self._get_load_path()))

        try:
            self._processing_date = str(params["PROCESSING_DATE"])
        except KeyError:
            function_name = inspect.currentframe().f_code.co_name
            raise ValueError(f"[{function_name}] Missing required parameter: 'PROCESSING_DATE'")
        
        # Validate data completeness immediately on initialization
        self._data_validation()
        
    def _data_validation(self) -> None:
        """
        This function checks the completeness of data in the table for the desired date range.
        It raises a ValueError if any expected partitions are missing.
        """
        table_name, missing_dates = check_data_completeness_auto(
            path=self.load_path,
            processing_date=self._processing_date,
            range_date=self._range_date,
            partitionBy=self._partition_column
        )

        if not missing_dates:
            logger.info(
                f"All expected data is available for table '{table_name}' at path: {self.load_path}"
            )
        else:
            error_message = (
                f"Missing partitions for table '{table_name}' at path: {self.load_path}. "
                f"Missing dates: {missing_dates}"
            )
            logger.error(error_message)
            raise ValueError(error_message)
        
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
        # Determine the dynamic date filter
        start_date, end_date = self._get_date_range_for_filter()
        
        if start_date == end_date:
            self._filters = f"{self._partition_column} = DATE('{end_date}')"
        else:
            self._filters = (
                f"{self._partition_column} >= DATE('{start_date}') AND "
                f"{self._partition_column} <= DATE('{end_date}')"
            )

        read_obj = spark.read.format(self._file_format)

        # Apply schema if defined
        if self._schema:
             read_obj = read_obj.schema(self._schema)
        
        df = read_obj.load(self.load_path, **self._load_args)

        # NOW, apply the filter on the DataFrame.
        # Spark's optimizer will attempt to push this down.
        if self._filters:
            if isinstance(self._filters, str):
                df = df.filter(self._filters)
            elif isinstance(self._filters, list):
                for f in self._filters:
                    df = df.filter(f)

        return df


