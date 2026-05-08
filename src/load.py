from src.logs import Logs
from src.config import sfOptions

class Load:
    def __init__(self, data_frame, spark_session):
        self.dataFrame = data_frame
        self.logger = Logs.get_logger("load")
        self.path = "file:/Volumes/NVME/projects/uber_dashboard/data/cleaned/uber-cleaned.csv"
        self.spark = spark_session

    def save_as_csv(self):
        self.logger.info("saving in csv file in progress...")

        self.dataFrame.write.mode("overwrite").csv(self.path)
        self.logger.info("data frame uber was saving")

    def save_to_data_warehouse(self):
        self.logger.info("creating data warehouse using Hive in progress...")


        save_to_snowflake = self.spark.read.csv(self.path, inferSchema=True, header=True)
        save_to_snowflake.write.format("snowflake").options(**sfOptions).option("dbtable", "uber").mode("append").save()
        self.logger.info("data was saved")

    def verify_data_warehouse(self):
        self.logger.info("verifying if data exist in data warehouse...")

        data = self.spark.table("uber_dw.uber")
        path_of_data = self.spark.conf.get("spark.sql.warehouse.dir")
        self.logger.info(f"exist {data.count()} in data warehouse")
        self.logger.info(f"data warehouse exist in {path_of_data}")

    def download_data_as_parquet(self):
        self.logger.info("downloading data from data warehouse...")
        data_from_warehouse = self.spark.sql("SELECT * FROM uber_dw.uber")
        data_from_warehouse.write.mode("overwrite").parquet("file:/Volumes/NVME/projects/uber_dashboard/data/loaded")
        self.logger.info("download data from data warehouse in success in path file:/Volumes/NVME/projects/uber_dashboard/data/loaded")