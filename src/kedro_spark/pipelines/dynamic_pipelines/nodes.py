import pyspark.sql.functions as f
from pyspark.sql import DataFrame
import time
from omegaconf import OmegaConf

params = OmegaConf.load("./conf/base/globals.yml")

def foo(df: DataFrame, date_str: str, sleep_duration_seconds: int = 5):
    print(f"\n--- Loading and sleeping for {sleep_duration_seconds} seconds ---")
    print(f"DataFrame schema:\n{df.printSchema()}")
    print(f"DataFrame count (will trigger action and Spark UI updates): {df.count()}") # Trigger an action

    print(f"Spark UI URL: {df.sparkSession.sparkContext.uiWebUrl}")

    print(f"Pausing Spark execution for {sleep_duration_seconds} seconds. Check Spark UI!")
    time.sleep(sleep_duration_seconds) # Dừng thực thi tại đây

    print("Resuming Spark execution.")
    return df