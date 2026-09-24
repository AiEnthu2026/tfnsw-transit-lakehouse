import builtins
import os
from pyspark.sql import SparkSession

builtins.spark = SparkSession.builder.getOrCreate()
builtins.CATALOG_NAME = os.environ.get("CATALOG_NAME", "dev")
builtins.SCHEMA_NAME = os.environ.get("SCHEMA_NAME", "tfnsw_transit_platform")