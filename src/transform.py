from pyspark.sql.functions import col, when
from src.logs import Logs

class Transform:
    def __init__(self, data_frame, spark_session):
        self.dataFrame = data_frame
        self.logger = Logs.get_logger("transform")
        self.sparkSession = spark_session

    def info(self):
        self.logger.info("get information about dataset...")
        count = self.dataFrame.count()
        types = self.dataFrame.dtypes

        self.logger.info(f"count of rows before cleaning and duplicating : {count}")
        self.logger.info(f"type of columns {types}")

    def remove_duplicates(self):
        self.logger.info("remove duplicate rows...")
        count_duplicated = self.dataFrame.groupBy('Booking ID').count().filter("COUNT > 1").count()
        self.dataFrame = self.dataFrame.dropDuplicates(['Booking ID'])

        self.logger.info(f"{count_duplicated} row was removed because is duplicated")
        self.logger.info(f"{self.dataFrame.count()} unique rows")

    def update_null(self):
        self.logger.info("update null values...")
        self.logger.info("updating Cancelled Rides by Customer row...")
        count_of_empty_cancelled_rides_by_customer_before_cleaning = self.dataFrame.filter(col("Cancelled Rides by Customer").isNull()).count()
        self.dataFrame = self.dataFrame.withColumn(
            "Cancelled Rides by Customer",
            when(col("Cancelled Rides by Customer").isNull(), 0)
            .otherwise(col("Cancelled Rides by Customer"))
        )
        count_of_empty_cancelled_rides_by_customer_after_cleaning = self.dataFrame.filter(col("Cancelled Rides by Customer").isNull()).count()

        self.logger.info(f"{count_of_empty_cancelled_rides_by_customer_before_cleaning} row have null in Cancelled Rides by Customer before cleaning")
        self.logger.info(f"{count_of_empty_cancelled_rides_by_customer_after_cleaning} row have null in Cancelled Rides by Customer after cleaning")
        self.logger.info("terminate updating Cancelled Rides by Customer row")
