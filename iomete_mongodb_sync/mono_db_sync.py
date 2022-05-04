from pyspark.sql import SparkSession

from config import ApplicationConfig
from iomete_mongodb_sync.utils import PySparkLogger


class MonoDbSync:
    def __init__(self, spark: SparkSession, config: ApplicationConfig):
        self.spark = spark
        self.config = config

        # self.spark.sparkContext.setLogLevel("INFO")
        self.logger = PySparkLogger(spark).get_logger(__name__)

        self.logger.info("pyspark script logger initialized")

    def sync_table_to_mongodb(self):
        self.logger.info("sync_table_to_mongodb started")

        connection_string = self.config.connection.build_connection_string()

        self.spark.sql("show tables").show()

        for sync in self.config.syncs:
            self.logger.info("Sync {}".format(sync))

            # create database manually, creating it automatically is not supported by spark 3.1
            for collection in sync.source_collections:
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

        self.logger.info("sync_table_to_mongodb finished!")
