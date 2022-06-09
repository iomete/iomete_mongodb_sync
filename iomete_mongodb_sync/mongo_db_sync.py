import logging
import time
from pyspark.sql import SparkSession

from config import ApplicationConfig

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
                table_timer(message)(self.__sync_table)(collection, sync.source_database, sync.destination_schema)

    def __sync_table(self, collection: str, source_database: str, destination_schema: str):
        df = self.spark.read.format("mongo") \
                .option("uri", self.connection_string) \
                .option("database", source_database) \
                .option("collection", collection) \
                .load()

        tmp_table_name = "tmp_" + collection
        df.createTempView(tmp_table_name)

        self.spark.sql(
            f"""create or replace table {destination_schema}.{collection}
                    as select * from {tmp_table_name}""")
        
        current_rows_count = self.query_single_value(f"select count(1) from {destination_schema}.{collection}")
        return current_rows_count


    def query_single_value(self, query):
        result = self.spark.sql(query).collect()
        if result and len(result) > 0:
            return result[0][0]
        return None


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