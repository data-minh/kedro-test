import os
import logging
from datetime import datetime, timedelta
from typing import List, Set
from urllib.parse import urlparse
from kedro_datasets._utils.spark_utils import get_spark
from pyspark.sql.functions import col
from minio import Minio
from minio.error import S3Error
from dotenv import load_dotenv


logger = logging.getLogger(__name__)

try:
    import pyarrow.fs
except ImportError:
    pyarrow = None
    logger.warning("pyarrow is not installed. HDFS functionality will be unavailable.")



def get_date_range(processing_date: str, range_date: int) -> List[str]:
    """
    Generate a list of date strings (yyyy-mm-dd) from (processing_date - range_date + 1) to processing_date.

    Args:
        processing_date (str): Date in 'yyyy-mm-dd' format.
        range_date (int): Number of days to look back including processing_date.

    Returns:
        List[str]: List of date strings.
    """
    try:
        end_date = datetime.strptime(processing_date, '%Y-%m-%d')
    except ValueError:
        raise ValueError(f"Invalid processing_date format: '{processing_date}'. Expected 'YYYY-MM-DD'.")

    if not isinstance(range_date, int) or range_date <= 0:
        raise ValueError(f"Invalid range_date: {range_date}. It must be a positive integer.")

    start_date = end_date - timedelta(days=range_date - 1)
    return [
        (start_date + timedelta(days=i)).strftime('%Y-%m-%d')
        for i in range(range_date)
    ]


def get_partitioned_dates_local(table_path: str, partition_column: str) -> List[str]:
    """
    List all partition folders in a local directory in the format partition_column=yyyy-mm-dd.

    Args:
        table_path (str): Local path to the partitioned table.
        partition_column (str): The name of the partition column (e.g., 'event_date').

    Returns:
        List[str]: List of date strings found in the partitions.
    """
    partition_dates = set()
    if not os.path.isdir(table_path):
        logger.warning(f"Local path does not exist: {table_path}")
        return []

    for entry in os.listdir(table_path):
        full_path = os.path.join(table_path, entry)
        if os.path.isdir(full_path) and entry.startswith(f"{partition_column}="):
            try:
                date_str = entry.split('=')[1]
                datetime.strptime(date_str, '%Y-%m-%d')
                partition_dates.add(date_str)
            except (IndexError, ValueError) as e:
                logger.warning(f"Skipping malformed partition folder '{entry}' in '{table_path}': {e}")
    return sorted(list(partition_dates))


def get_partitioned_dates_s3(bucket: str, prefix: str, partition_column: str) -> List[str]:
    """
    List all partition folders in an S3 prefix in the format partition_column=yyyy-mm-dd.

    Args:
        bucket (str): S3 bucket name.
        prefix (str): S3 prefix (folder path inside the bucket).
        partition_column (str): The name of the partition column (e.g., 'event_date').

    Returns:
        List[str]: List of date strings found in the partitions.
    """
    env_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../.env'))
    load_dotenv(dotenv_path=env_path)

    access_key = os.getenv("AWS_ACCESS_KEY_ID")
    secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
    endpoint = os.getenv("AWS_S3_ENDPOINT").replace("http://", "").replace("https://", "")
    secure = os.getenv("AWS_S3_ENDPOINT").startswith("https://")

    client = Minio(
        endpoint=endpoint,
        access_key=access_key,
        secret_key=secret_key,
        secure=secure
    )

    try:
        objects = client.list_objects(bucket, prefix=prefix, recursive=False)
        partition_dates = set()
        entry = str()
        for obj in objects:
            remaining = obj.object_name[len(prefix):]

            if "/" in remaining:
                entry = remaining.split("/")[0]
            if entry.startswith(f"{partition_column}="):
                date_str = entry.split('=')[1]
                datetime.strptime(date_str, '%Y-%m-%d')
                partition_dates.add(date_str)

    except S3Error as e:
        print("Error:", e)

    return sorted(list(partition_dates))


def get_partitioned_dates_hdfs(hdfs_path: str, partition_column: str) -> List[str]:
    """
    List all partition folders in an HDFS path in the format partition_column=yyyy-mm-dd.

    Args:
        hdfs_path (str): Full HDFS path.
        partition_column (str): The name of the partition column (e.g., 'event_date').

    Returns:
        List[str]: List of date strings found in the partitions.
    """
    if pyarrow is None or pyarrow.fs is None:
        logger.error("pyarrow is not installed or pyarrow.fs is not available. Cannot list HDFS partitions.")
        return []
    
    try:
        fs = pyarrow.fs.HadoopFileSystem()
        selector = pyarrow.fs.FileSelector(hdfs_path, recursive=False)
        file_info = fs.get_file_info(selector)
    except Exception as e:
        logger.error(f"Error connecting to HDFS or listing path '{hdfs_path}': {e}")
        return []

    partition_dates = set()
    for entry in file_info:
        name = entry.base_name
        if entry.is_dir and name.startswith(f"{partition_column}="):
            try:
                date = name.split('=')[1]
                datetime.strptime(date, '%Y-%m-%d') # Validate date format
                partition_dates.add(date)
            except (IndexError, ValueError) as e:
                logger.warning(f"Skipping malformed HDFS partition folder '{name}': {e}")
    return sorted(list(partition_dates))

def get_partitioned_dates_iceberg(path: str, partitionBy: str) -> Set[str]:
    spark = get_spark()

    query = f"""
    SELECT file
    FROM {path}.metadata_log_entries
    ORDER BY timestamp DESC
    LIMIT 1
    """

    result = spark.sql(query).collect()

    if result:
        current_metadata_file_location = result[0]["file"]
    else:
        print(f"No metadata file found for table '{path}'")

    metadata_content_df = spark.read.json(current_metadata_file_location)
    manifest_list_path = metadata_content_df.select(col("snapshots.manifest-list")).collect()[0][0][0]
    manifest_list_data = spark.read.format("avro").load(manifest_list_path)

    manifest_paths = set(
        manifest_list_data.select("manifest_path")
        .rdd.flatMap(lambda x: x)
        .collect()
    )

    partition_set = set()

    for manifest_path in manifest_paths:
        try:
            df = spark.read.format("avro").load(manifest_path)

            partitions = (
                df.select("data_file.partition")
                .rdd
                .map(lambda row: str(row[0][0]))
                .collect()
            )

            partition_set.update(partitions)
        except Exception as e:
            print(f"[ERROR] Failed to process manifest '{manifest_path}': {e}")
    
    return partition_set


def check_data_completeness_auto(path: str, processing_date: str, range_date: int, partitionBy: str) -> tuple[str, List[str]]:
    """
    Automatically determine the storage type and check for completeness of partitioned data
    for a given table path and date range.

    Args:
        path (str): Table path, can be local, s3://bucket/prefix, hdfs://path or iceberg table with "catalog.schema.table_name"
        processing_date (str): The latest date to be included (yyyy-mm-dd).
        range_date (int): Number of days to go back, including processing_date.
        partitionBy (str): The name of the partition column (e.g., 'event_date').

    Returns:
        tuple[str, List[str]]: A tuple containing the table name and a list of missing date strings.
    """
    parsed = urlparse(path)
    table_name = os.path.basename(parsed.path.rstrip('/')) if parsed.path else parsed.netloc

    actual_dates: List[str] = []

    # Determine storage type and get actual dates
    if parsed.scheme == 's3' or parsed.scheme == 's3a':
        bucket = parsed.netloc
        prefix = parsed.path.lstrip('/') + '/'
        actual_dates = get_partitioned_dates_s3(bucket, prefix, partitionBy)
    elif parsed.scheme == 'hdfs':
        actual_dates = get_partitioned_dates_hdfs(path, partitionBy)
    elif (parsed.scheme == '' and '/' in path) or parsed.scheme == 'file': 
        actual_dates = get_partitioned_dates_local(parsed.path, partitionBy)
    elif parsed.scheme == '' and len(path.split('.')) == 3:
        actual_dates = get_partitioned_dates_iceberg(parsed.path, partitionBy)
    else:
        logger.error(f"Unsupported path scheme: '{parsed.scheme}' for path: {path}")
        return table_name, get_date_range(processing_date, range_date)

    # Generate expected date range
    expected_dates = get_date_range(processing_date, range_date)
    missing_dates = [d for d in expected_dates if d not in actual_dates]

    return table_name, missing_dates