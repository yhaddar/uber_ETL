from src.spark_session import Spark_Session
from src.transform import Transform
from src.load import Load
import os
os.environ["JAVA_HOME"] = "/opt/homebrew/opt/openjdk@17"
class Main:
    def __init__(self):
        self.sparkSession = Spark_Session()
        self.data = self.sparkSession.connect_in_spark()
        self.final_data = None

    def transform(self):
        t = Transform(self.data, self.sparkSession)
        t.remove_duplicates()
        t.update_canceled_by_customer()
        t.update_reason_for_cancelling_by_customer()
        t.update_canceled_by_driver()
        t.update_incomplete_rides()
        t.update_driver_cancellation_reason()
        t.update_incomplete_rides_reason()
        t.update_book_value()
        t.update_ride_distance()
        t.update_driver_rating()
        t.update_customer_rating()
        t.update_payment_method()
        t.update_type_of_date()
        t.update_type_of_time()
        t.remove_empty_date()

        self.final_data = t.verification()

    def load(self):
        loader = Load(self.final_data, self.sparkSession.spark)
        loader.save_to_data_warehouse()

if __name__ == "__main__":
    main = Main()
    main.transform()
    main.load()