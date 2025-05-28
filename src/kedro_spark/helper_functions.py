import logging
import random
import re
import sys

import pandas as pd
import pyspark.sql.functions as f
from hdfs import InsecureClient
from kedro.pipeline import node
from pyspark.sql import DataFrame, SparkSession, Window
from sklearn.model_selection import train_test_split
import pyarrow as pa
import pyarrow.fs
from omegaconf import OmegaConf
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

spark = SparkSession.builder.appName("utils").getOrCreate()
params = OmegaConf.load("./conf/base/globals.yml")

def pandas_train_test_split(
    df, test_size=None, train_size=None, random_state=None, shuffle=True,
    stratify=None, subset_col_name="subset", subset_name=["train", "test"]
):
    if stratify:
        stratify = df[stratify]

    train, test = train_test_split(
        df, test_size=test_size, train_size=train_size,
        random_state=random_state, shuffle=shuffle, stratify=stratify
    )

    train[subset_col_name] = subset_name[0]
    test[subset_col_name] = subset_name[1]
    return pd.concat([train, test])

def to_julian_str(date_str):
    """Convert YYYY-MM-DD to YYYYDDD format"""
    return datetime.strptime(date_str, '%Y-%m-%d').strftime('%Y%j')

def pipe(self, func, *args, **kwargs):
    """
    Functionality:
    - Implementation of Pandas .pipe() function for better visibility
    Applying chainable functions that expect Pyspark.DataFrame
    --------------------------------------------------
    Args:
    - self: use self to add directly to the pyspark.DataFrame object
    as part of attributes of the class.
    - func: the function to be piped
    - *args: any argument of the piped function
    - *kwargs: any keywords argument of the piped function
    --------------------------------------------------
    Returns:
    - func()
    The return type of func
    """
    return func(self, *args, **kwargs)


DataFrame.pipe = pipe


def rename_columns(df, rename_dict):
    """
    Functionality:
    - Renames selected columns in pyspark.DataFrame based on _prefix or _suffix
    --------------------------------------------------
    Args:
    - df: pyspark.DataFrame containings columns to rename.
    - rename_dict: dictionary specified what columns to rename and how
    with prefix_ and _suffix.
     Ex: {
     'prefix_' : {'ftr' : ['age','gender']},
      '_suffix': {'ratio' : ['avg_spending_3m','max_spending_3m']}
     }
    --------------------------------------------------
    Returns:
    - pyspark.DataFrame
    """

    assert (
        "prefix_" and "_suffix"
    ) in rename_dict, (
        "None of the eligible keywords ('prefix_','_suffix') is in dictionary"
    )

    # Create a list to hold all column renaming expr
    rename_expr = []

    prefix_ = rename_dict["prefix_"]
    _suffix = rename_dict["_suffix"]

    # Done list
    done_list = []

    # Iterate over the dictionary
    if isinstance(prefix_, dict):
        for prefix, columns in prefix_.items():
            for col_name in columns:
                rename_expr.append(
                    f.col(col_name)
                    .alias(f"{prefix}_{col_name}")
                    )
                done_list.append(col_name)

    if isinstance(_suffix, dict):
        for suffix, columns in _suffix.items():
            for col_name in columns:
                rename_expr.append(
                    f.col(col_name)
                    .alias(f"{col_name}_{suffix}")
                    )
                done_list.append(col_name)

    # Add the rest of the columns that are not renamed (if any)
    remaining_col_expr = [f.col(c) for c in df.columns if c not in done_list]

    return df.select(*rename_expr, *remaining_col_expr)


def backfill(spk_df: DataFrame) -> DataFrame:
    """
    Functionality:
    - Backfill limit by the pervious limit
    --------------------------------------------------
    Args:
    - spk_df: DataFrame containing limit
    --------------------------------------------------
    Returns:
    - pyspark.DataFrame
    """
    return spk_df.withColumn(
        "current_limit",
        f.first(f.col("current_limit"), ignorenulls=True).over(
            Window
            .partitionBy("card_nbr")
            .orderBy("date")
            .rowsBetween(0, sys.maxsize)
        ),
    )


def get_folder_date(df):
    date = (
        df
            .withColumn(
                "date",
                f.regexp_extract(
                    f.input_file_name(),
                    "\\d{4}-\\d{2}-\\d{2}", 0
                )
            )
            .first()
            ["date"]
    )
    return date

def read_bucket(
    spark,
    num_buckets,
    bucket_cols,
    path,
    schema=None,
    check_num_buckets=True
):
    """
    A helper function to read bucket if the system doesn't
    implement Metadata Catalog
    Author: ThanhNM3
    """
    table_name = f"table_{random.randint(0, sys.maxsize)}"
    FileSystem = \
        spark.sparkContext._gateway.jvm.org.apache.hadoop.fs.FileSystem
    URI = spark.sparkContext._gateway.jvm.java.net.URI
    Configuration = \
        spark.sparkContext._gateway.jvm.org.apache.hadoop.conf.Configuration
    Path = spark.sparkContext._gateway.jvm.org.apache.hadoop.fs.Path

    def get_file_paths(path):
        fs = FileSystem.get(
            URI(re.sub("[?|^|\\[|\\]|\\{|\\}]", "", path)), Configuration()
        )
        file_status = fs.globStatus(Path(path))

        deepest_folder = (map(
            lambda x: x.getPath(),
            file_status if any(x.isDirectory() for x in file_status) else
                fs.globStatus(Path("/".join(path.split("/")[:-1])))
        ))

        folder = next((
            x for x in deepest_folder if
            len(list(
                filter(
                    lambda x: not x.getName().startswith("_"),
                    map(lambda x: x.getPath(), fs.listStatus(x))
                )
            )) > 0
        ), None)

        file_paths = list(map(
            lambda x: x.toString(),
            filter(
                lambda x: not x.getName().startswith("_"),
                map(lambda x: x.getPath(), fs.listStatus(folder))
            )
        ))

        return file_paths

    def get_schema(df):
        return ", ".join(map(lambda x: f"{x[0]} {x[1]}", df.dtypes))

    if schema is None or check_num_buckets:
        file_paths = get_file_paths(path)
        if check_num_buckets:
            bucket_pattern = "_\d*\.c"
            num_discovered_buckets = len(set(
                match.group(0) for match in
                map(lambda x: re.search(bucket_pattern, x), file_paths)
                if match
            ))
            assert num_discovered_buckets == num_buckets, \
                f"Number of specified buckets [{num_buckets}] doesn't match" \
                "with number of discovered buckets [{num_discovered_buckets}]."
        if schema is None:
            schema_ddl = get_schema(spark.read.parquet(file_paths[0]))
        else:
            schema_ddl = schema
    else:
        schema_ddl = schema

    bucket_cols = ", ".join(bucket_cols)
    spark.sql(f"DROP TABLE IF EXISTS {table_name}")
    spark.sql(f"""
        CREATE TABLE {table_name} ( {schema_ddl} )
        USING PARQUET
        CLUSTERED BY ( {bucket_cols} )
        SORTED BY ( {bucket_cols} ) INTO {num_buckets} BUCKETS
        LOCATION "{path}"
    """)
    return spark.table(table_name)


def write_bucket(
    spark,
    df,
    num_buckets,
    bucket_cols,
    path,
    repartition_before_write=True,
    mode="overwrite"
):
    table_name = f"table_{random.randint(0, sys.maxsize)}"
    FileSystem = spark.sparkContext._gateway.jvm.org.apache.hadoop.fs.FileSystem
    URI = spark.sparkContext._gateway.jvm.java.net.URI
    Configuration = spark.sparkContext._gateway.jvm.org.apache.hadoop.conf.Configuration
    Path = spark.sparkContext._gateway.jvm.org.apache.hadoop.fs.Path

    if mode == "ignore":
        path_SUCCESS = path + "/_SUCCESS"
        fs = FileSystem.get(URI(path_SUCCESS), Configuration())
        if fs.exists(Path(path_SUCCESS)):
            logger.warning(f"Path: {path_SUCCESS} already exists. Skip.")
            return

    df = (
        df.repartition(num_buckets, bucket_cols)
        if repartition_before_write else df
    )
    (
        df
            .write
            .format("parquet")
            .bucketBy(num_buckets, bucket_cols)
            .sortBy(bucket_cols)
            .option("path", path)
            .saveAsTable(table_name)
    )

def correct_path(dataset):
    fs_prefix = str(dataset._fs_prefix) if hasattr(dataset, "_fs_prefix") else ""
    file_path = str(dataset._filepath)
    url = ""
    path = ""
    if fs_prefix == "":
        path = file_path
    else:
        url = fs_prefix + file_path.split("/")[0]
        path = "/" + "/".join(file_path.split("/")[1:])
    return url, path

def get_flat_paths(path):
    """
    Expand path patterns with wildcards into all matching paths and also return month-end dates.
    
    Args:
        path: Path with pattern like /tmp/{2024-12-??}
        
    Returns:
        tuple: (all_paths, month_end_paths) where month_end_paths contains only month-end dates
    """
    import re
    import calendar
    from datetime import datetime
    
    full_month_paths = []
    end_month_paths = []
    
    # Handle path with no pattern
    if "{" not in path:
        return [path],[path]
    
    # Extract base path and pattern part
    parent_path = path.split("{")[0]
    remainder = path.split("}")[-1] if "}" in path else ""
    match = re.findall(r'\{(.*?)\}', path)
    
    if not match:
        return [path], end_month_paths
    
    date_patterns = match[0].split(',')
    
    for pattern in date_patterns:
        if "??" in pattern:
            # Handle patterns like 2024-12-??
            year_month_match = re.match(r'(\d{4})-(\d{2})-\?\?', pattern)
            if year_month_match:
                year, month = year_month_match.groups()
                year_int, month_int = int(year), int(month)
                
                # Get number of days in the month
                num_days = calendar.monthrange(year_int, month_int)[1]
                
                # Generate path for each day
                for day in range(1, num_days + 1):
                    date_str = f"{year}-{month}-{day:02d}"
                    full_path = parent_path + date_str + remainder
                    full_month_paths.append(full_path)
                    
                    # Check if it's the last day of the month
                    if day == num_days:
                        end_month_paths.append(full_path)
            else:
                # If not a recognized pattern, keep as is
                full_path = parent_path + pattern + remainder
                full_month_paths.append(full_path)
        else:
            # No wildcard, handle exact date
            full_path = parent_path + pattern + remainder
            full_month_paths.append(full_path)
            
            # Check if this is a month-end date
            date_match = re.match(r'(\d{4})-(\d{2})-(\d{2})', pattern)
            if date_match:
                year, month, day = map(int, date_match.groups())
                last_day = calendar.monthrange(year, month)[1]
                if int(day) == last_day:
                    end_month_paths.append(full_path)
    return full_month_paths, end_month_paths

def check_hdfs_exist(url, path):
    if url == "":
        url = "hdfs://hadoop-ttqtdl:8020"
    fs = pa.fs.HadoopFileSystem.from_uri(url)
    fileSelector = pa.fs.FileSelector(path, allow_not_found=True)
    try:
        path_info = fs.get_file_info(fileSelector)
    except:
        return False
    if len(path_info) == 0:
        return False
    return True

def generate_lookback_pattern(date, period, lookback):
    """
    Generate a pattern string with lookback periods in the format {YYYY-MM-DD,YYYY-MM-DD,...}.
    
    Args:
        date: Reference date in 'YYYY-MM-DD' format
        period: Data period ('latest', 'daily', 'monthly', 'quarterly', 'yearly')
        lookback: Number of periods to look back
    
    Returns:
        String with pattern in format {period1,period2,...}
    """
    import datetime as dt
    from dateutil.relativedelta import relativedelta
    
    d = dt.datetime.strptime(date, "%Y-%m-%d")
    date_range = []
    
    for i in range(lookback):
        if period == 'latest':
            # Just use the reference date
            date_str = (d - relativedelta(days=i)).strftime("%Y-%m-%d")
            
        elif period == 'daily':
            # Look back by days
            date_str = (d - relativedelta(days=i)).strftime("%Y-%m-%d")
            
        elif period == 'monthly':
            # Look back by months with wildcard for days
            date_str = (d - relativedelta(months=i)).strftime("%Y-%m-??")
            
        elif period == 'quarterly':
            # Look back by quarters with wildcard for month and day
            quarter_date = d - relativedelta(months=i*3)
            quarter = ((quarter_date.month - 1) // 3) + 1
            date_str = f"{quarter_date.year}-Q{quarter}"
            
        elif period == 'yearly':
            # Look back by years with wildcard
            date_str = (d - relativedelta(years=i)).strftime("%Y-??-??")
            
        else:
            # Default to monthly if period is not recognized
            date_str = (d - relativedelta(months=i)).strftime("%Y-%m-??")
            
        date_range.append(date_str)
    
    return "{" + ",".join(date_range) + "}"

def create_iter_pipeline(
    catalog,
    func,
    inputs,
    outputs,
    period = 'latest',  # 'latest', 'daily', 'monthly', 'yearly', 'quarterly',
    lookback=1,
):
    """
    Create pipeline by looping through each period in the time range
    and preprocess the data accordingly.
    
    Args:
        catalog: Data catalog
        period: Data period ('latest', 'daily', 'monthly', 'yearly', 'quarterly')
        func: Processing function
        inputs: Input dataset names (string or list)
        outputs: Output dataset names (string or list)
    """
    logger.info("="*20)
    logger.info(f"Check function {func.__name__} with period = {period} and lookback month = {lookback}")
    pipeline = []
    if isinstance(inputs, str):
        inputs = [inputs]
    if isinstance(outputs, str):
        outputs = [outputs]
    
    date_list = []

    end_date = pd.Timestamp(datetime.strptime(params["END_DATE"], '%Y-%m-%d'))
    start_date = pd.Timestamp(datetime.strptime(params["START_DATE"], '%Y-%m-%d'))
    
    if period == 'latest':
        date_list = [end_date]
    
    elif period == 'daily':
        date_list = pd.date_range(start=start_date, end=end_date, freq='D')
    
    elif period == 'monthly':
        date_list = pd.date_range(start=start_date, end=end_date, freq='M')
    
    elif period == 'quarterly':
        date_list = pd.date_range(start=start_date, end=end_date, freq='Q')

    elif period == 'yearly':
        date_list = pd.date_range(start=start_date, end=end_date, freq='Y')
    else:
        raise ValueError(f"Invalid period: {period}. Must be one of 'latest', 'daily', 'monthly', 'quarterly', 'yearly'")
    for date in date_list:
        formatted_date = date.strftime('%Y-%m-%d')
        pattern = generate_lookback_pattern(formatted_date, period, lookback)
        formatted_inputs =  [x.format(pattern) if "{" in x else x for x in inputs]
        formatted_outputs =  [x.format(formatted_date) if "{" in x else x for x in outputs]
        formatted_name = f"{func.__name__}.{formatted_date}"
        
        formatted_inputs.append(f"params:{formatted_date}")
        
        all_input_exist = True
        missing_input = []
        for x in formatted_inputs:
            if "params" in x:
                continue        

            url, path = correct_path(catalog._get_dataset(x))
            
            # Glob Path Processing
            if "{" in path:
                full_month_input_exist = True
                end_month_input_exist = True
                full_month_missing_input = []
                end_month_missing_input = []
                full_month_paths, end_month_paths = get_flat_paths(path)
                
                for flat_path in full_month_paths:
                    if not check_hdfs_exist(url, flat_path):
                        full_month_input_exist = False
                        full_month_missing_input.append(flat_path)

                for flat_path in end_month_paths:
                    if not check_hdfs_exist(url, flat_path):
                        end_month_input_exist = False
                        end_month_missing_input.append(flat_path)

                if (full_month_input_exist == False) & (end_month_input_exist == False):
                    all_input_exist = False
                    missing_input.append(full_month_missing_input)

            else:
                if not check_hdfs_exist(url, path):
                    all_input_exist = False
                    missing_input.append(path)
        output_not_exist = True
        for x in formatted_outputs: 
            url, path = correct_path(catalog._get_dataset(x))
            mode = catalog._get_dataset(x)._save_args.get("mode", "overwrite")
            if mode == 'ignore':
                FileSystem = spark.sparkContext._gateway.jvm.org.apache.hadoop.fs.FileSystem
                URI = spark.sparkContext._gateway.jvm.java.net.URI
                Configuration = spark.sparkContext._gateway.jvm.org.apache.hadoop.conf.Configuration
                Path = spark.sparkContext._gateway.jvm.org.apache.hadoop.fs.Path
                path_SUCCESS = url + path + "/_SUCCESS"
                fs = FileSystem.get(URI(path_SUCCESS), Configuration())
                if fs.exists(Path(path_SUCCESS)):
                    logger.warning(
                        f"Existed output path {path_SUCCESS}. Skip node {formatted_name}."
                        )
                    output_not_exist = False

        if all_input_exist & output_not_exist:
            n = node(
                func=func,
                inputs=formatted_inputs if len(formatted_inputs) > 1 else formatted_inputs[0],
                outputs=formatted_outputs if len(formatted_outputs) > 1 else formatted_outputs[0],
                name=formatted_name
            )
            pipeline.append(n)
        else:
            logger.warning(
                f"Missing input path {missing_input}. Skip node {formatted_name}."
            )

    return pipeline

def create_iter_pipeline(
    func,
    inputs,
    outputs,
    period = 'latest',  # 'latest', 'daily', 'monthly', 'yearly', 'quarterly',
    lookback=1,
):
    """
    Create pipeline by looping through each period in the time range
    and preprocess the data accordingly.
    
    Args:
        catalog: Data catalog
        period: Data period ('latest', 'daily', 'monthly', 'yearly', 'quarterly')
        func: Processing function
        inputs: Input dataset names (string or list)
        outputs: Output dataset names (string or list)
    """
    logger.info("="*20)
    logger.info(f"Check function {func.__name__} with period = {period} and lookback month = {lookback}")
    pipeline = []
    if isinstance(inputs, str):
        inputs = [inputs]
    if isinstance(outputs, str):
        outputs = [outputs]
    
    date_list = []

    end_date = pd.Timestamp(datetime.strptime(params["END_DATE"], '%Y-%m-%d'))
    start_date = pd.Timestamp(datetime.strptime(params["START_DATE"], '%Y-%m-%d'))
    
    if period == 'latest':
        date_list = [end_date]
    
    elif period == 'daily':
        date_list = pd.date_range(start=start_date, end=end_date, freq='D')
    
    elif period == 'monthly':
        date_list = pd.date_range(start=start_date, end=end_date, freq='M')
    
    elif period == 'quarterly':
        date_list = pd.date_range(start=start_date, end=end_date, freq='Q')

    elif period == 'yearly':
        date_list = pd.date_range(start=start_date, end=end_date, freq='Y')
    else:
        raise ValueError(f"Invalid period: {period}. Must be one of 'latest', 'daily', 'monthly', 'quarterly', 'yearly'")
    for date in date_list:
        formatted_date = date.strftime('%Y-%m-%d')
        pattern = generate_lookback_pattern(formatted_date, period, lookback)
        formatted_inputs =  [x.format(pattern) if "{" in x else x for x in inputs]
        formatted_outputs =  [x.format(formatted_date) if "{" in x else x for x in outputs]
        formatted_name = f"{func.__name__}.{formatted_date}"
        
        formatted_inputs.append(f"params:{formatted_date}")
        
        all_input_exist = True
        output_not_exist = True

        if all_input_exist & output_not_exist:
            n = node(
                func=func,
                inputs=formatted_inputs if len(formatted_inputs) > 1 else formatted_inputs[0],
                outputs=formatted_outputs if len(formatted_outputs) > 1 else formatted_outputs[0],
                name=formatted_name
            )
            pipeline.append(n)
        else:
            logger.warning(
                f"Missing input path. Skip node {formatted_name}."
            )

    return pipeline
