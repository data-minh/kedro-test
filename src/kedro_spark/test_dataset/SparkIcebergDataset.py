from typing import Any, Dict, List, Optional, Union
from kedro_datasets.spark import SparkDataset
from pyspark.sql import DataFrame, SparkSession
from kedro_datasets._utils.spark_utils import get_spark
from pyspark.sql.types import StructType

class SparkIcebergDataset(SparkDataset):
    """
    ``SparkIcebergDataset`` provides a Kedro interface to Iceberg tables,
    leveraging Apache Spark's Iceberg connector for all read and write operations.
    It supports all Iceberg catalog types configurable via SparkSession.
    """

    def __init__(
        self,
        *,
        catalog_name: str,
        namespace: Union[str, List[str]],
        table_name: str,
        read_options: Dict[str, Any] = None,
        filters: Union[str, List[str]] = None,
        write_options: Dict[str, Any] = None,
        **kwargs: Any
    ):
        """Creates a new instance of ``SparkIcebergDataset``.

        This dataset uses Spark's built-in Iceberg support, requiring the
        SparkSession to be configured correctly with Iceberg extensions and catalogs.

        Args:
            catalog_name: The name of the Spark SQL catalog configured for Iceberg
                          (e.g., "spark_catalog", "my_hive_catalog").
            namespace: The namespace (database) of the Iceberg table. Can be a string or list of strings.
                       Example: "my_db" or ["my_company", "sales_data"].
            table_name: The name of the Iceberg table.
            read_options: A dictionary of options to pass to Spark's ``DataFrameReader``
                          when reading the Iceberg table.
                          Example: {"snapshot-id": "12345", "as-of-timestamp": "2024-01-01T00:00:00Z"}.
            filters: A single SQL-like filter string or a list of filter strings to apply
                     during read. Spark's optimizer will push down these filters
                     for partition pruning and predicate pushdown.
                     Example: "event_date >= '2024-01-01'" or ["category = 'books'", "price > 50"].
            write_options: A dictionary of options to pass to Spark's ``DataFrameWriter``
                           when writing to the Iceberg table.
                           Example: {"mode": "overwrite", "partitionBy": ["date_col", "country_col"]}.
            spark_session: Optional name of the SparkSession to use from the Kedro config.
                           This is typically required for Spark-based datasets.
            kwargs: Any additional arguments to pass to the base SparkDataset,
                    though most options for Iceberg interaction are handled by
                    `read_options`, `filters`, and `write_options`.
        """
        # SparkDataset requires filepath and file_format even if not directly used for Iceberg.
        # We can set dummy values or assume they are passed via kwargs.
        # However, it's cleaner to handle them explicitly or through kwargs.
        # For Iceberg, the "filepath" is essentially the fully qualified table name.
        # Let's set file_format to "iceberg" for clarity and ensure it's handled.
        
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
        if 'filepath' not in kwargs:
             # Construct a canonical filepath for internal SparkDataset use
            full_table_path = f"{catalog_name}.{'.'.join([namespace] if isinstance(namespace, str) else namespace)}.{table_name}"
            kwargs['filepath'] = full_table_path
        
        # Pass all remaining arguments to the base SparkDataset constructor.
        # This handles common settings like spark_session, save_args (if not explicitly handled by write_options), etc.
        super().__init__(**kwargs)

        self._catalog_name = catalog_name
        self._namespace = [namespace] if isinstance(namespace, str) else list(namespace)
        self._table_name = table_name
        self._read_options = read_options if read_options is not None else {}
        self._filters = filters
        self._write_options = write_options if write_options is not None else {}

        # Construct the fully qualified table name for Spark SQL operations
        self._full_spark_table_name = (
            f"{self._catalog_name}.{'.'.join(self._namespace)}.{self._table_name}"
        )

    def _load(self) -> DataFrame:
        spark = get_spark()

        # Start with the DataFrameReader, specifying "iceberg" format explicitly
        df_reader = spark.read.format("iceberg")

        # Apply read options provided by the user (e.g., snapshot-id, as-of-timestamp)
        for key, value in self._read_options.items():
            df_reader = df_reader.option(key, str(value)) # Options usually require string values

        # Load the Iceberg table. This returns a DataFrame.
        df = df_reader.load(self._full_spark_table_name)

        # Apply filters to the DataFrame. Spark's optimizer will push these down
        # for partition pruning and predicate pushdown based on Iceberg's capabilities.
        if self._filters:
            if isinstance(self._filters, str):
                df = df.filter(self._filters)
            elif isinstance(self._filters, list):
                for f in self._filters:
                    df = df.filter(f)

        return df

    # def _save(self, df: DataFrame) -> None:
    #     if not isinstance(df, DataFrame):
    #         raise TypeError(
    #             f"Expected PySpark DataFrame, but got {type(df)}. "
    #             "SparkIcebergDataset only supports Spark DataFrames for saving."
    #         )

    #     # Start with the DataFrameWriter
    #     df_writer = df.write.format("iceberg")

    #     # Apply write options (mode, partitionBy, etc.)
    #     # If 'mode' is in self._write_options, use it. Otherwise, default to 'overwrite' (or append)
    #     mode = self._write_options.get("mode", "overwrite")
    #     df_writer = df_writer.mode(mode)

    #     # Apply other write options
    #     for key, value in self._write_options.items():
    #         if key.lower() != "mode": # Don't apply mode again as an option
    #             # For partitionBy, if it's a list, it should be passed via .partitionBy() method
    #             if key.lower() == "partitionby" and isinstance(value, list):
    #                 df_writer = df_writer.partitionBy(*value)
    #             else:
    #                 df_writer = df_writer.option(key, str(value))

    #     if self._exists():
    #         df_writer.save(self._full_spark_table_name)
    #     else:
    #         df_writer.saveAsTable(self._full_spark_table_name)

    def _save(self, df: DataFrame) -> None:
        if not isinstance(df, DataFrame):
            raise TypeError(
                f"Expected PySpark DataFrame, but got {type(df)}. "
                "SparkIcebergDataset only supports Spark DataFrames for saving."
            )

        writer = df.writeTo(self._full_spark_table_name)

        mode = self._write_options.get("mode", "overwrite")  # "append", "overwrite", "overwrite_partitions", etc.

        # Optional: handle partitioning when creating table
        if not self._exists():
            partition_cols = self._write_options.get("partitionBy", [])
            for col in partition_cols:
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
                self._logger.debug(f"Iceberg table '{self._full_spark_table_name}' does not exist.")
            else:
                self._logger.warning(
                    f"Could not reliably check existence of Iceberg table "
                    f"'{self._full_spark_table_name}': {e}"
                )
            return False

    def _describe(self) -> Dict[str, Any]:
        """Returns a dictionary that describes the dataset."""
        return {
            "catalog_name": self._catalog_name,
            "namespace": self._namespace,
            "table_name": self._table_name,
            "read_options": self._read_options,
            "filters": self._filters,
            "write_options": self._write_options,
            "full_spark_table_name": self._full_spark_table_name,
        }