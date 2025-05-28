from pyspark.sql import SparkSession, DataFrame, Column

from omegaconf import OmegaConf

params = OmegaConf.load("./conf/base/globals.yml")

def transform(df: DataFrame):
    return df