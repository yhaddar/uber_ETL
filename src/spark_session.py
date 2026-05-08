from pyspark.sql import SparkSession
from src.logs import Logs




class Spark_Session:
    def __init__(self):
        self.csv_path = "./data/extracted/uber.csv"
        self.header = True
        self.inferSchema = True
        self.appName = "uber_transform"
        self.logger = Logs.get_logger("spark session")
        self.spark = None

    def connect_in_spark(self):
        self.logger.info("Start to connecting to Spark...")
        self.spark = SparkSession.builder.appName(self.appName).config("spark.jars.packages", "net.snowflake:spark-snowflake_2.13:2.12.0-spark_3.4").getOrCreate()
        df = self.spark.read.csv(
            self.csv_path,
            header=self.header,
            inferSchema=self.inferSchema
        )
        self.logger.info("Connected To Spark")
        return df
    def return_spark(self):
        return self.spark