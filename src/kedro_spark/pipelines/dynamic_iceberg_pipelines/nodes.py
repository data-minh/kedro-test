import time
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

def load_and_sleep(input_df: DataFrame, sleep_duration_seconds: int = 1) -> DataFrame:
    """
    Loads data, pauses the Spark session for observation, and then returns the DataFrame.
    """
    print(f"\n--- Loading and sleeping for {sleep_duration_seconds} seconds ---")
    print(f"DataFrame schema:\n{input_df.printSchema()}")
    print(f"DataFrame count (will trigger action and Spark UI updates): {input_df.count()}") # Trigger an action

    print(f"Spark UI URL: {input_df.sparkSession.sparkContext.uiWebUrl}")

    print(f"Pausing Spark execution for {sleep_duration_seconds} seconds. Check Spark UI!")
    time.sleep(sleep_duration_seconds) # Dừng thực thi tại đây

    print("Resuming Spark execution.")
    
    # Bạn có thể thêm một số thao tác nhỏ sau đó để chứng minh DataFrame vẫn hoạt động
    return input_df

def load_and_sleep_test(input_df: DataFrame, _dummy_input: DataFrame, sleep_duration_seconds: int = 120) -> DataFrame:
    """
    Loads data, pauses the Spark session for observation, and then returns the DataFrame.
    """
    print(f"\n--- Loading and sleeping for {sleep_duration_seconds} seconds ---")
    print(f"DataFrame schema:\n{input_df.printSchema()}")
    print(f"DataFrame count (will trigger action and Spark UI updates): {input_df.count()}") # Trigger an action

    print(f"Spark UI URL: {input_df.sparkSession.sparkContext.uiWebUrl}")

    print(f"Pausing Spark execution for {sleep_duration_seconds} seconds. Check Spark UI!")
    time.sleep(sleep_duration_seconds) # Dừng thực thi tại đây

    print("Resuming Spark execution.")
    
    # Bạn có thể thêm một số thao tác nhỏ sau đó để chứng minh DataFrame vẫn hoạt động
    return input_df