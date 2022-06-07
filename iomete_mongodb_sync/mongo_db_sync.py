import logging
from pyspark.sql import SparkSession
from pymongo import MongoClient

from config import ApplicationConfig

logger = logging.getLogger(__name__)


class MonoDbSync:
    def __init__(self, spark: SparkSession, config: ApplicationConfig):
        self.spark = spark
        self.config = config
        self.client = MongoClient(config.connection.host,
                                  username=config.connection.username,
                                  password=config.connection.password)

    def sync_table_to_mongodb(self):
        logger.info("sync_table_to_mongodb started")

        connection_string = self.config.connection.build_connection_string()

        self.spark.sql("show tables").show()

        for sync in self.config.syncs:
            logger.info("Sync {}".format(sync))

            source_collections = sync.source_collections
            if sync.is_all_collections:
                source_collections = self.__get_collections_of_source_database(sync.source_database)

            # create database manually, creating it automatically is not supported by spark 3.1
            for collection in source_collections:
                df = self.spark.read.format("mongo") \
                    .option("uri", connection_string) \
                    .option("database", sync.source_database) \
                    .option("collection", collection) \
                    .load()

                tmp_table_name = "tmp_" + collection
                df.createTempView(tmp_table_name)

                self.spark.sql(
                    f"""create or replace table {sync.destination_schema}.{collection}
                            as select * from {tmp_table_name}""")

        logger.info("sync_table_to_mongodb finished!")

    def __get_collections_of_source_database(self, source_database):
        return self.client[source_database].list_collections(include_system_collections=False)
