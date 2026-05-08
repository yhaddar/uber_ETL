from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta
from src.spark_session import Spark_Session
from src.transform import Transform
from src.load import Load
from src.logs import Logs

# logger = Logs.get_logger("pipeline")
def extract():
    spark = Spark_Session()
    df = spark.connect_in_spark()
    df.write.mode("overwrite").parquet("/tmp/uber_raw")


def transform():
    spark = Spark_Session()
    df = spark.spark.read.parquet("/tmp/uber_raw")

    t = Transform(df, spark)

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

    final_df = t.verification()
    final_df.write.mode("overwrite").parquet("/tmp/uber_clean")


def load():
    spark = Spark_Session()
    df = spark.spark.read.parquet("/tmp/uber_clean")

    loader = Load(df, spark.spark)
    loader.save_to_data_warehouse()


# logger.info("start pipeline...")


default_args = {
    "owner": "yhaddar",
    "start_date": datetime(2026, 5, 8),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
        dag_id="uber_pipeline",
        default_args=default_args,
        catchup=False
) as dag:
    task_1 = PythonOperator(
        task_id="extracting_data",
        python_callable=extract,
    )

    task_2 = PythonOperator(
        task_id="transforming_data",
        python_callable=transform,
    )

    task_3 = PythonOperator(
        task_id="loading_data",
        python_callable=load,
    )

    task_1 >> task_2 >> task_3

# logger.info("end pipeline")
