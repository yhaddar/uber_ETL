from src.spark_session import Spark_Session

sparkSession = Spark_Session()
df = sparkSession.connect_in_spark()

print(df)