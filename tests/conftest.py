import builtins
from pyspark.sql import SparkSession

builtins.spark = SparkSession.builder.getOrCreate()
builtins.CATALOG_NAME = "tfnsw"
builtins.SCHEMA_NAME = "tfnsw_transit_platform"