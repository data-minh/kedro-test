from typing import Any, Dict, List, Union
from kedro_datasets.spark import SparkDataset
from pyspark.sql import DataFrame
from kedro_datasets._utils.spark_utils import get_spark


def _strip_dbfs_prefix(path: str, prefix: str = "/dbfs") -> str:
    return path[len(prefix):] if path.startswith(prefix) else path

class FilteredSparkDataset(SparkDataset):
    """
    ``SparkSourceDataset`` extends Kedro's SparkDataset to provide powerful filtering
    capabilities applied directly during the load process, optimizing for partition pruning.
    """
    def __init__(
        self,
        *,
        filters: Union[str, List[str]] = None,
        **kwargs: Any
    ) -> None:
        """Creates a new instance of ``SparkSourceDataset``.

        Args:
            filters: A single SQL-like filter string or a list of filter strings to apply.
                     For partitioned data (e.g., Parquet, Delta), these filters are pushed down
                     for partition pruning where possible.
                     Example: "event_date = '2023-01-01'" or ["year = 2023", "month = 1"].
            kwargs: All other arguments typically passed to Kedro's `SparkDataset`,
                    e.g., `filepath`, `file_format`, `load_args`, `save_args`, `spark_session`, `schema`, `fs_prefix`.
        """
        super().__init__(**kwargs)
        self._filters = filters

    def _load(self) -> DataFrame:
        spark = get_spark()
        read_obj = spark.read.format(self._file_format)

        # Apply schema if defined
        if self._schema:
             read_obj = read_obj.schema(self._schema)

        # Get the load path (handled by the parent class)
        load_path = _strip_dbfs_prefix(self._fs_prefix + str(self._get_load_path()))
        
        df = read_obj.load(load_path, **self._load_args)

        # NOW, apply the filter on the DataFrame.
        # Spark's optimizer will attempt to push this down.
        if self._filters:
            if isinstance(self._filters, str):
                df = df.filter(self._filters)
            elif isinstance(self._filters, list):
                for f in self._filters:
                    df = df.filter(f)

        return df


