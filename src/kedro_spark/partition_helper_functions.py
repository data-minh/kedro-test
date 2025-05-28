import os
import logging
from datetime import datetime, timedelta
from typing import List
from urllib.parse import urlparse

# Chỉ import boto3 và pyarrow.fs nếu cần.
# Nếu môi trường không có, việc import này sẽ gây lỗi.
# Có thể dùng try-except hoặc chỉ import khi hàm tương ứng được gọi.
logger = logging.getLogger(__name__)

try:
    import boto3
except ImportError:
    boto3 = None
    logger.warning("boto3 is not installed. S3 functionality will be unavailable.")

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
    partition_dates = []
    if not os.path.isdir(table_path):
        logger.warning(f"Local path does not exist: {table_path}")
        return []

    for entry in os.listdir(table_path):
        full_path = os.path.join(table_path, entry)
        if os.path.isdir(full_path) and entry.startswith(f"{partition_column}="):
            try:
                date_str = entry.split('=')[1]
                datetime.strptime(date_str, '%Y-%m-%d')
                partition_dates.append(date_str)
            except (IndexError, ValueError) as e:
                logger.warning(f"Skipping malformed partition folder '{entry}' in '{table_path}': {e}")
    return sorted(list(set(partition_dates)))


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
    if boto3 is None:
        logger.error("boto3 is not installed. Cannot list S3 partitions.")
        return []

    s3 = boto3.client('s3')
    paginator = s3.get_paginator('list_objects_v2')
    
    if prefix and not prefix.endswith('/') and not prefix == '/':
        prefix += '/'

    dates = set()
    try:
        result = paginator.paginate(Bucket=bucket, Prefix=prefix, Delimiter='/')

        for page in result:
            for common_prefix in page.get('CommonPrefixes', []):
                folder_name = os.path.basename(common_prefix['Prefix'].rstrip('/'))
                if folder_name.startswith(f"{partition_column}="):
                    try:
                        date_str = folder_name.split('=')[1]
                        datetime.strptime(date_str, '%Y-%m-%d')
                        dates.add(date_str)
                    except (IndexError, ValueError) as e:
                        logger.warning(f"Skipping malformed S3 partition folder '{folder_name}': {e}")
    except Exception as e:
        logger.error(f"Error listing S3 partitions for s3://{bucket}/{prefix}: {e}")
        return []

    return sorted(list(dates))


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


def check_data_completeness_auto(path: str, processing_date: str, range_date: int, partitionBy: str) -> tuple[str, List[str]]:
    """
    Automatically determine the storage type and check for completeness of partitioned data
    for a given table path and date range.

    Args:
        path (str): Table path, can be local, s3://bucket/prefix, or hdfs://path.
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
    if parsed.scheme == 's3':
        bucket = parsed.netloc
        prefix = parsed.path.lstrip('/')
        actual_dates = get_partitioned_dates_s3(bucket, prefix, partitionBy)
    elif parsed.scheme == 'hdfs':
        actual_dates = get_partitioned_dates_hdfs(path, partitionBy)
    elif parsed.scheme == '' or parsed.scheme == 'file': 
        actual_dates = get_partitioned_dates_local(parsed.path, partitionBy)
    else:
        logger.error(f"Unsupported path scheme: '{parsed.scheme}' for path: {path}")
        return table_name, get_date_range(processing_date, range_date)

    # Generate expected date range
    expected_dates = get_date_range(processing_date, range_date)
    missing_dates = [d for d in expected_dates if d not in actual_dates]

    return table_name, missing_dates