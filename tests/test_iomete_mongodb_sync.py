#!/usr/bin/env python

"""Tests for `iomete_mongodb_sync` package."""
from iomete_mongodb_sync.config import get_config
from iomete_mongodb_sync.main import start_job
from tests._spark_session import get_spark_session


def test_mongodb_sync():
    # create test spark instance
    test_config = get_config("application.conf")
    # spark = get_spark_session()
    spark = get_spark_session()

    spark.sql("CREATE DATABASE IF NOT EXISTS docdb_raw")

    # run target
    start_job(spark, test_config)

    # check
    df = spark.sql(f"select * from docdb_raw.restaurants")
    df.printSchema()
    assert df.count() == 2


if __name__ == "__main__":
    test_mongodb_sync()