from src.spark_session import Spark_Session
from src.transform import Transform
from src.load import Load

sparkSession = Spark_Session()
df = sparkSession.connect_in_spark()

transform_data = Transform(df, sparkSession)
transform_data.info()
transform_data.remove_duplicates()
transform_data.update_canceled_by_customer()
transform_data.update_reason_for_cancelling_by_customer()
transform_data.update_canceled_by_driver()
transform_data.update_incomplete_rides()
transform_data.update_driver_cancellation_reason()
transform_data.update_incomplete_rides_reason()
transform_data.update_book_value()
transform_data.update_ride_distance()
transform_data.update_driver_rating()
transform_data.update_customer_rating()
transform_data.update_payment_method()
transform_data.update_type_of_date()
transform_data.update_type_of_time()
transform_data.remove_empty_date()
final_data_frame = transform_data.verification()

load_data = Load(final_data_frame, sparkSession.return_spark())
load_data.save_as_csv()
load_data.save_to_data_warehouse()
load_data.verify_data_warehouse()
load_data.download_data_as_parquet()
