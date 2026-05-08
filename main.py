from src.spark_session import Spark_Session
from src.transform import Transform

sparkSession = Spark_Session()
df = sparkSession.connect_in_spark()

transform_data = Transform(df, sparkSession)
transform_data.info()
transform_data.remove_duplicates()
transform_data.update_canceled_by_customer()
transform_data.update_reason_for_cancelling_by_customer()
