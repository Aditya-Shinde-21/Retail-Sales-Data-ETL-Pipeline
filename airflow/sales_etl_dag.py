from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.exceptions import AirflowFailException

import pymysql


default_args = {
    "owner": "Aditya",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=10)
}


def validate_sales_data():
    connection = pymysql.connect(
        host="host.docker.internal",
        user="root",
        password="Aditya@2025",
        database="sales_data"
    )

    cursor = connection.cursor()
    cursor.execute("SELECT COUNT(*) FROM customers_data_mart")
    row_count = cursor.fetchone()[0]

    if row_count == 0:
        raise AirflowFailException("customers_data_mart table is empty")

    cursor.close()
    connection.close()


with DAG(
    dag_id="retail_sales_batch_etl_local",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
) as dag:

    wait_for_sales_data = S3KeySensor(
        task_id="wait_for_sales_data",
        bucket_name="customer-sales-data-project",
        bucket_key="sales_data/*.csv",
        wildcard_match=True,
        aws_conn_id="aws_default",
        poke_interval=180,
        timeout=1800,
    )

    run_spark_etl = BashOperator(
        task_id="run_spark_etl",
        bash_command="""
        export PYTHONPATH=/mnt/c/Users/Aditya/PycharmProjects/de_project1:$PYTHONPATH
        
        spark-submit \
        --master local[2] \
        --conf "spark.driver.extraJavaOptions=-Dlog4j.configuration=file:/mnt/c/Users/Aditya/PycharmProjects/de_project1/resources/dev/log4j.properties" \
        --conf "spark.executor.extraJavaOptions=-Dlog4j.configuration=file:/mnt/c/Users/Aditya/PycharmProjects/de_project1/resources/dev/log4j.properties" \
        /mnt/c/Users/Aditya/PycharmProjects/de_project1/scripts/main/transformations/jobs/main.py
        """
    )

    validate_data = PythonOperator(
        task_id="validate_data",
        python_callable=validate_sales_data
    )

    wait_for_sales_data >> run_spark_etl >> validate_data
