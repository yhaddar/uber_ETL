from pyspark.sql.functions import col, when
from src.logs import Logs

# Booking Value, Ride Distance, Driver Ratings, Customer Rating, Payment Method

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

    def update_canceled_by_customer(self):
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

    def update_reason_for_cancelling_by_customer(self):
        self.logger.info("updating Reason for cancelling by Customer...")

        count_of_empty_null_value_before_cleaning = self.dataFrame.filter(col("Reason for cancelling by Customer").isNull()).count()
        self.dataFrame = self.dataFrame.withColumn(
            "Reason for cancelling by Customer",
            when(col("Reason for cancelling by Customer").isNull(), "other")
            .otherwise(col("Reason for cancelling by Customer"))
        )
        count_of_empty_null_value_after_cleaning = self.dataFrame.filter(col("Reason for cancelling by Customer").isNull()).count()

        self.logger.info(f"{count_of_empty_null_value_before_cleaning} row have null value in Reason for cancelling by Customer before cleaning")
        self.logger.info(f"{count_of_empty_null_value_after_cleaning} row have null value in Reason for cancelling by Customer have updated to other")
        self.logger.info("terminate updating Reason for cancelling by Customer row")

    def update_canceled_by_driver(self):
        self.logger.info("updating Cancelled Rides by Driver row...")
        count_of_empty_cancelled_rides_by_customer_before_cleaning = self.dataFrame.filter(col("Cancelled Rides by Driver").isNull()).count()
        self.dataFrame = self.dataFrame.withColumn(
            "Cancelled Rides by Driver",
            when(col("Cancelled Rides by Driver").isNull(), 0)
            .otherwise(col("Cancelled Rides by Driver"))
        )
        count_of_empty_cancelled_rides_by_customer_after_cleaning = self.dataFrame.filter(col("Cancelled Rides by Driver").isNull()).count()

        self.logger.info(f"{count_of_empty_cancelled_rides_by_customer_before_cleaning} row have null in Cancelled Rides by Driver before cleaning")
        self.logger.info(f"{count_of_empty_cancelled_rides_by_customer_after_cleaning} row have null in Cancelled Rides by Driver after cleaning")
        self.logger.info("terminate updating Cancelled Rides by Driver row")

    def update_incomplete_rides(self):
        self.logger.info("updating Incomplete Rides row...")
        count_of_empty_cancelled_rides_by_customer_before_cleaning = self.dataFrame.filter(col("Incomplete Rides").isNull()).count()
        self.dataFrame = self.dataFrame.withColumn(
            "Incomplete Rides",
            when(col("Incomplete Rides").isNull(), 0)
            .otherwise(col("Incomplete Rides"))
        )
        count_of_empty_cancelled_rides_by_customer_after_cleaning = self.dataFrame.filter(col("Incomplete Rides").isNull()).count()

        self.logger.info(f"{count_of_empty_cancelled_rides_by_customer_before_cleaning} row have null in Incomplete Rides before cleaning")
        self.logger.info(f"{count_of_empty_cancelled_rides_by_customer_after_cleaning} row have null in Incomplete Rides after cleaning")
        self.logger.info("terminate updating Incomplete Rides row")

    def update_driver_cancellation_reason(self):
        self.logger.info("updating Driver Cancellation Reason...")

        count_of_empty_null_value_before_cleaning = self.dataFrame.filter(col("Driver Cancellation Reason").isNull()).count()
        self.dataFrame = self.dataFrame.withColumn(
            "Driver Cancellation Reason",
            when(col("Driver Cancellation Reason").isNull(), "unknown")
            .otherwise(col("Driver Cancellation Reason"))
        )
        count_of_empty_null_value_after_cleaning = self.dataFrame.filter(col("Driver Cancellation Reason").isNull()).count()

        self.logger.info(f"{count_of_empty_null_value_before_cleaning} row have null value in Driver Cancellation Reason before cleaning")
        self.logger.info(f"{count_of_empty_null_value_after_cleaning} row have null value in Driver Cancellation Reason have updated to other")
        self.logger.info("terminate updating Driver Cancellation Reason row")

    def update_incomplete_rides_reason(self):
        self.logger.info("updating Incomplete Rides Reason row...")
        count_of_empty_cancelled_rides_by_customer_before_cleaning = self.dataFrame.filter(col("Incomplete Rides Reason").isNull()).count()
        self.dataFrame = self.dataFrame.withColumn(
            "Incomplete Rides Reason",
            when(col("Incomplete Rides Reason").isNull(), "unknown")
            .otherwise(col("Incomplete Rides Reason"))
        )
        count_of_empty_cancelled_rides_by_customer_after_cleaning = self.dataFrame.filter(col("Incomplete Rides Reason").isNull()).count()

        self.logger.info(f"{count_of_empty_cancelled_rides_by_customer_before_cleaning} row have null in Incomplete Rides Reason before cleaning")
        self.logger.info(f"{count_of_empty_cancelled_rides_by_customer_after_cleaning} row have null in Incomplete Rides Reason after cleaning")
        self.logger.info("terminate updating Incomplete Rides Reason row")

