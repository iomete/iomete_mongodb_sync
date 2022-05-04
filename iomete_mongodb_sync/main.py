"""Main module."""

from pyspark.sql import SparkSession

from config import ApplicationConfig
from iomete_mongodb_sync.mono_db_sync import MonoDbSync


def start_job(spark: SparkSession, config: ApplicationConfig):
    mongodb_sync = MonoDbSync(spark, config)
    mongodb_sync.sync_table_to_mongodb()
