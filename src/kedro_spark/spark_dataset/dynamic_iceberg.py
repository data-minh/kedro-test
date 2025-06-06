from typing import Any, Dict, List, Optional, Union
from kedro_datasets.spark import SparkDataset
from pyspark.sql import DataFrame, SparkSession
from kedro_datasets._utils.spark_utils import get_spark
from pyspark.sql.types import StructType
import logging
import inspect
from omegaconf import OmegaConf
from datetime import datetime, timedelta


logger = logging.getLogger(__name__)

try:
    params = OmegaConf.load("./conf/base/globals.yml")
except Exception as e:
    logger.error(f"Failed to load globals.yml: {e}")
    params = {}

class DynamicIcebergDataset(SparkDataset):
    """
    ``SparkIcebergDataset`` provides a Kedro interface to Iceberg tables,
    leveraging Apache Spark's Iceberg connector for all read and write operations.
    It supports all Iceberg catalog types configurable via SparkSession.
    """

    def __init__(
        self,
        *,
        data_validation: Optional[bool] = None,
        **kwargs: Any
    ):
        
        # Ensure 'file_format' is set to 'iceberg'
        if 'file_format' not in kwargs:
            kwargs['file_format'] = 'iceberg'
        elif kwargs['file_format'] != 'iceberg':
            self._logger.warning(
                f"Ignored 'file_format'='{kwargs['file_format']}'. "
                "SparkIcebergDataset explicitly uses 'iceberg' format."
            )
            kwargs['file_format'] = 'iceberg'

        # Since Iceberg tables are identified by catalog.namespace.table_name,
        # the 'filepath' parameter of SparkDataset is somewhat redundant here.
        # We can construct a placeholder or ensure it's provided by the user.
        # For simplicity, we'll ensure it's set to the fully qualified name for consistency.
        self._catalog_name = kwargs.get("load_args", {}).get("catalog", "default")
        self._namespace = kwargs.get("load_args", {}).get("namespace", "default")
        self._table_name = kwargs.get("load_args", {}).get("table_name", "default")

        if 'filepath' not in kwargs:
            if isinstance(self._namespace, list):
                full_table_path = f"{self._catalog_name}.{'.'.join(self._namespace)}.{self._table_name}"
            else:
                full_table_path = f"{self._catalog_name}.{self._namespace}.{self._table_name}"
            kwargs['filepath'] = full_table_path
        elif 'filepath' in kwargs and kwargs['filepath'] != f"{self._catalog_name}.{'.'.join(self._namespace) if isinstance(self._namespace, list) else self._namespace}.{self._table_name}":
            self._logger.warning(
                f"Ignored 'filepath'='{kwargs['filepath']}'. "
                f"SparkIcebergDataset explicitly uses '{kwargs['filepath']}' from catalog/namespace/table_name settings."
            )
            if isinstance(self._namespace, list):
                kwargs['filepath'] = f"{self._catalog_name}.{'.'.join(self._namespace)}.{self._table_name}"
            else:
                kwargs['filepath'] = f"{self._catalog_name}.{self._namespace}.{self._table_name}"
        
        # Pass all remaining arguments to the base SparkDataset constructor.
        # This handles common settings like spark_session, save_args (if not explicitly handled by write_options), etc.
        super().__init__(**kwargs)

        # Get parameters for dynamic filter
        self._range_date = self._load_args.get("range_date", "*")
        self._partition_column = self._load_args.get("partition_column", "**")
        if self._load_args.get("filters", "1=1") != "1=1":
            self._filters = self._load_args.get("filters", "1=1")

        # Get information for saving table
        self._partitionedBy = list()
        try:
            self._partitionedBy = self._save_args.get("partitionBy")
        except:
            self._partitionedBy = []

        if self._range_date != "*":
            self._range_date = int(self._range_date)

        self._filters = str(None)

        try:
            self._processing_date = str(params["PROCESSING_DATE"])
        except KeyError:
            function_name = inspect.currentframe().f_code.co_name
            raise ValueError(f"[{function_name}] Missing required parameter: 'PROCESSING_DATE'")

        # Construct the fully qualified table name for Spark SQL operations
        self._full_iceberg_table_name = (
            f"{self._catalog_name}.{self._namespace}.{self._table_name}"
        )

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

        # Start with the DataFrameReader, specifying "iceberg" format explicitly
        read_obj = spark.read.format("iceberg")

        # Load the Iceberg table. This returns a DataFrame.
        df = read_obj.load(self._full_iceberg_table_name, **self._load_args)

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


    def _save(self, df: DataFrame) -> None:
        if not isinstance(df, DataFrame):
            raise TypeError(
                f"Expected PySpark DataFrame, but got {type(df)}. "
                "SparkIcebergDataset only supports Spark DataFrames for saving."
            )

        writer = df.writeTo(self._full_iceberg_table_name)

        mode = self._save_args.get("mode", "overwrite")  # "append", "overwrite", etc.

        # Optional: handle partitioning when creating table
        if not self._exists():
            if self._partitionedBy:
                for col in self._partitionedBy:
                    writer = writer.partitionedBy(col)
            if mode == "append":
                writer.createOrReplace()
            else:
                writer.createOrReplace()
        else:
            if mode == "append":
                writer.append()
            elif mode == "overwrite":
                writer.overwritePartitions()
            else:
                raise ValueError(f"Unsupported mode '{mode}' for Iceberg write.")

    def _exists(self) -> bool:
        spark = get_spark()
        
        try:
            full_table_name_with_catalog = f"{self._catalog_name}.{'.'.join(self._namespace) if isinstance(self._namespace, list) else self._namespace}.{self._table_name}"
            spark.sql(f"DESCRIBE TABLE {full_table_name_with_catalog}").collect()
            return True
        except Exception as e:
            error_message = str(e).lower()
            if "table or view not found" in error_message or "cannot be found" in error_message:
                self._logger.debug(f"Iceberg table '{self._full_iceberg_table_name}' does not exist.")
            else:
                self._logger.warning(
                    f"Could not reliably check existence of Iceberg table "
                    f"'{self._full_iceberg_table_name}': {e}"
                )
            return False
