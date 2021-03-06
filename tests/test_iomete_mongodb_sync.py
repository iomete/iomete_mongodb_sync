#!/usr/bin/env python

"""Tests for `iomete_mongodb_sync` package."""
import re
from iomete_mongodb_sync.config import get_config
from iomete_mongodb_sync.main import start_job
from tests._spark_session import get_spark_session


def test_mongodb_sync():
    # create test spark instance
    test_config = get_config("application.conf")
    spark = get_spark_session()

    # if re.match(r".*(image|photo|signature)$", "test_image", re.IGNORECASE):
    #     print("MATCH")
    # else:
    #     print("NO MATCH")
    spark.sql("CREATE DATABASE IF NOT EXISTS docdb_test")

    # run target
    start_job(spark, test_config)

    # check
    df = spark.sql(f"select * from docdb_test.restaurants")
    assert df.count() == 2


if __name__ == "__main__":
    test_mongodb_sync()