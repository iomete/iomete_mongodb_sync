import logging
import re
import time
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql import functions as f

from config import ApplicationConfig, SyncConfig

logger = logging.getLogger(__name__)


class MonoDbSync:
    def __init__(self, spark: SparkSession, config: ApplicationConfig):
        self.spark = spark
        self.config = config
        self.connection_string = self.config.connection.build_connection_string()

    def run(self):
        timer("MongoDB Sync")(self.sync_tables)()

    def sync_tables(self):
        for sync in self.config.syncs:
            # create database manually, creating it automatically is not supported by spark 3.1
            max_table_name_length = max([len(collection) for collection in sync.source_collections])

            for collection in sync.source_collections:
                message = f"[{collection: <{max_table_name_length}}]: collection sync"
                table_timer(message)(self.__sync_table)(collection, sync)

    def __sync_table(self, collection: str, cnf: SyncConfig):
        df = self.spark.read.format("mongo") \
                .option("uri", self.connection_string) \
                .option("database", cnf.source_database) \
                .option("collection", collection) \
                .option("samplePoolSize", "10000") \
                .option("sampleSize", "10000") \
                .load()


        # df.printSchema()
        flatten_df = self.flatten_structs(df)
        # flatten_df.printSchema()

        if cnf.column_exclude_pattern is not None:
            logger.info("Excluding columns matching pattern: %s", cnf.column_exclude_pattern)
            flatten_df = self.exlude_columns(flatten_df, cnf.column_exclude_pattern)

        # flatten_df.printSchema()
        tmp_table_name = "tmp_" + collection
        flatten_df.createTempView(tmp_table_name)

        self.spark.sql(
            f"""create or replace table {cnf.destination_schema}.{collection}
                    as select * from {tmp_table_name}""")
        
        current_rows_count = self.query_single_value(f"select count(1) from {cnf.destination_schema}.{collection}")
        return current_rows_count


    def query_single_value(self, query):
        result = self.spark.sql(query).collect()
        if result and len(result) > 0:
            return result[0][0]
        return None

    def flatten_structs(self, nested_df):
        stack = [((), nested_df)]
        columns = []

        while len(stack) > 0:            
            parents, df = stack.pop()

            flat_cols = [
                f.col(".".join(parents + (c[0],))).alias("_".join(parents + (c[0],)))
                for c in df.dtypes
                if c[1][:6] != "struct" and c[1][:4] != "null"
            ]

            nested_cols = [
                c[0]
                for c in df.dtypes
                if c[1][:6] == "struct"
            ]
            
            columns.extend(flat_cols)

            for nested_col in nested_cols:
                projected_df = df.select(nested_col + ".*")
                stack.append((parents + (nested_col,), projected_df))
            
        return nested_df.select(columns)

    def exlude_columns(self, df, exclude_pattern):
        return df.select([c for c in df.columns if not re.match(exclude_pattern, c, re.IGNORECASE)])

def timer(message: str):
    def timer_decorator(method):
        def timer_func(*args, **kw):
            logger.info(f"{message} started")
            start_time = time.time()
            result = method(*args, **kw)
            duration = (time.time() - start_time)
            logger.info(f"{message} completed in {duration:0.2f} seconds")
            return result

        return timer_func

    return timer_decorator


def table_timer(message: str):
    def timer_decorator(method):
        def timer_func(*args, **kw):
            logger.info(f"{message} started")
            start_time = time.time()
            total_rows = method(*args, **kw)
            duration = (time.time() - start_time)
            logger.info(f"{message} completed in {duration:0.2f} seconds. Total rows: {total_rows}")
            return total_rows

        return timer_func

    return timer_decorator