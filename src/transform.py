from pyspark.sql.functions import col, when, avg, round, try_to_date, to_timestamp, sum
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

    def update_book_value(self):
        self.logger.info("updating Book value row...")
        count_of_empty_booking_value_before_cleaning = self.dataFrame.filter(col("Booking Value").isNull()).count()

        self.dataFrame = self.dataFrame.withColumn(
            "Booking Value",
            when(col("Booking Value").isNull(), 0)
            .otherwise(col("Booking Value"))
        )
        count_of_empty_booking_value_after_cleaning = self.dataFrame.filter(col("Booking Value").isNull()).count()

        self.logger.info(f"{count_of_empty_booking_value_before_cleaning} row have null value in Booking Value before cleaning")
        self.logger.info(f"{count_of_empty_booking_value_after_cleaning} row have null value in Booking Value before cleaning")
        self.logger.info("terminate updating book value row")

    def update_ride_distance(self):
        self.logger.info("updating Ride Distance row...")
        count_of_empty_ride_distance_before_cleaning = self.dataFrame.filter(col("Ride Distance").isNull()).count()

        self.dataFrame = self.dataFrame.withColumn(
            "Ride Distance",
            when(col("Ride Distance").isNull(), 0)
            .otherwise(col("Ride Distance"))
        )
        count_of_empty_ride_distance_after_cleaning = self.dataFrame.filter(col("Ride Distance").isNull()).count()

        self.logger.info(f"{count_of_empty_ride_distance_before_cleaning} row have null value in Ride Distance before cleaning")
        self.logger.info(f"{count_of_empty_ride_distance_after_cleaning} row have null value in Ride Distance before cleaning")
        self.logger.info("terminate updating book value row")

    def update_driver_rating(self):
        self.logger.info("updating Driver Ratings row...")
        count_of_empty_driver_rating_before_cleaning = self.dataFrame.filter(col("Driver Ratings").isNull()).count()

        avg_rating = self.dataFrame.filter("`Driver Ratings` IS NOT NULL").distinct().select(avg("Driver Ratings"))
        avg_rating_collected = avg_rating.collect()[0][0]

        self.dataFrame = self.dataFrame.withColumn(
            "Driver Ratings",
            when(col("Driver Ratings").isNull(), avg_rating_collected)
            .otherwise(col("Driver Ratings"))
        )

        self.logger.info("round the Driver Ratings value to 2 number after the comma...")

        self.dataFrame = self.dataFrame.withColumn(
            "Driver Ratings",
            round(col("Driver Ratings"), 1)
        )

        count_of_empty_driver_rating_after_cleaning = self.dataFrame.filter(col("Driver Ratings").isNull()).count()

        self.logger.info(f"{count_of_empty_driver_rating_before_cleaning} row have null value in Driver Ratings before cleaning")
        self.logger.info(f"{count_of_empty_driver_rating_after_cleaning} row have null value in Driver Ratings after cleaning")
        self.logger.info("terminate updating Driver Ratings row")

    def update_customer_rating(self):
        self.logger.info("updating Customer Rating row...")
        count_of_empty_customer_rating_before_cleaning = self.dataFrame.filter(col("Customer Rating").isNull()).count()

        avg_rating = self.dataFrame.filter("`Customer Rating` IS NOT NULL").distinct().select(avg("Customer Rating"))
        avg_rating_collected = avg_rating.collect()[0][0]

        self.dataFrame = self.dataFrame.withColumn(
            "Customer Rating",
            when(col("Customer Rating").isNull(), avg_rating_collected)
            .otherwise(col("Customer Rating"))
        )

        self.logger.info("round the Customer Rating value to 2 number after the comma...")

        self.dataFrame = self.dataFrame.withColumn(
            "Customer Rating",
            round(col("Customer Rating"), 1)
        )

        count_of_empty_customer_rating_after_cleaning = self.dataFrame.filter(col("Customer Rating").isNull()).count()

        self.logger.info(f"{count_of_empty_customer_rating_before_cleaning} row have null value in Customer Rating before cleaning")
        self.logger.info(f"{count_of_empty_customer_rating_after_cleaning} row have null value in Customer Rating after cleaning")
        self.logger.info("terminate updating Customer Rating row")

    def update_payment_method(self):
        self.logger.info("updating Payment Method row...")
        count_of_empty_payment_method_before_cleaning = self.dataFrame.filter(col("Payment Method").isNull()).count()

        self.dataFrame = self.dataFrame.withColumn(
            "Payment Method",
            when(col("Payment Method").isNull(), "unknown")
            .otherwise(col("Payment Method"))
        )

        count_of_empty_payment_method_after_cleaning = self.dataFrame.filter(col("Payment Method").isNull()).count()

        self.logger.info(f"{count_of_empty_payment_method_before_cleaning} row have null value in Payment Method before cleaning")
        self.logger.info(f"{count_of_empty_payment_method_after_cleaning} row have null value in Payment Method after cleaning")
        self.logger.info("terminate updating Payment Method row")

    def update_type_of_date(self):
        self.logger.info("updating Type of Date row...")

        self.dataFrame = self.dataFrame.withColumn(
            "Date",
            try_to_date(col("Date"), "dd-MMM-yy")
        )

        self.logger.info("date was updated")

    def update_type_of_time(self):
        self.logger.info("updating Type of Time row...")

        self.dataFrame = self.dataFrame.withColumn(
            "Time",
            to_timestamp(col("Time"), "H:mm:ss")
        )

        self.logger.info("Time was updated")

    def remove_empty_date(self):
        self.logger.info("removing empty date row...")
        self.logger.info(f"{self.dataFrame.count()} rows have null value in Date before cleaning")
        self.dataFrame = self.dataFrame.dropna(subset=["Date"])
        self.logger.info(f"{self.dataFrame.count()} rows have null value in Date after cleaning")

    def verification(self):
        self.logger.info("verifying data...")

        self.logger.info("change the attribute name with add _ and lowercase in every name")
        self.dataFrame = self.dataFrame.toDF(*[
            c.strip().replace(" ", "_").lower()
            for c in self.dataFrame.columns
        ])

        verify_nulls = self.dataFrame.select([
            sum(when(col(c).isNull(), 1).otherwise(0)).alias(c)
            for c in self.dataFrame.columns
        ])

        print(verify_nulls.show())
        print(self.dataFrame.dtypes)

        self.logger.info(f"{self.dataFrame.count()} after remove duplicates data")
        self.logger.info(f"{verify_nulls.collect()[0]} after update null values")

        return self.dataFrame