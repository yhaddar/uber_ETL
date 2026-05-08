from pyspark.sql import SparkSession
from src.logs import Logs

# spark = SparkSession.builder.appName("").getOrCreate()
#
# df.createOrReplaceTempView("uber")
# df.printSchema()
# df.show(5)
# print(f'total of rows : {df.count()}') # 150000
# print(df.groupby('Booking ID').count().filter("COUNT > 1").show(150000))
# df = df.dropDuplicates(['Booking ID'])
# print(f'total of rows after remove duplicates : {df.count()}') # 150000


class Spark_Session:
    def __init__(self):
        self.csv_path = "./data/extracted/uber.csv"
        self.header = True
        self.inferSchema = True
        self.appName = "uber_transform"
        self.logger = Logs.get_logger("spark session")

    def connect_in_spark(self):
        self.logger.info("Start to connecting to Spark...")
        spark = SparkSession.builder.appName(self.appName).getOrCreate()
        df = spark.read.csv(
            self.csv_path,
            header=self.header,
            inferSchema=self.inferSchema
        )
        self.logger.info("Connected To Spark")
        return df