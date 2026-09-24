import builtins
from pyspark.sql import SparkSession

builtins.spark = SparkSession.builder.getOrCreate()
builtins.CATALOG_NAME = "dev"
builtins.SCHEMA_NAME = "tfnsw_transit_platform"