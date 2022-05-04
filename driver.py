from pyspark.sql import SparkSession

from config import get_config
from iomete_mongodb_sync.main import start_job

spark = SparkSession.builder \
    .appName("iomete_mongodb_sync") \
    .getOrCreate()

production_config = get_config("/etc/configs/application.conf")

start_job(spark, production_config)
