from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.exceptions import AirflowFailException
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.providers.mysql.hooks.mysql import MySqlHook


def spark_bash_command():
    #AWS credentials
    aws = AwsBaseHook(aws_conn_id="aws_default", client_type="sts")
    creds = aws.get_credentials()

    # MySQL credentials
    mysql = MySqlHook(mysql_conn_id="mysql_default")
    mysql_conn = mysql.get_connection("mysql_default")

    return f"""
    # Create environment variables for connection credentials
    # ----------------- AWS ------------------------
    export AWS_ACCESS_KEY_ID={creds.access_key}
    export AWS_SECRET_ACCESS_KEY={creds.secret_key}
    export AWS_DEFAULT_REGION=us-east-1
    
    # ---------- MySQL ----------
    export MYSQL_HOST={mysql_conn.host}
    export MYSQL_PORT={mysql_conn.port}
    export MYSQL_DATABASE={mysql_conn.schema}
    export MYSQL_USER={mysql_conn.login}
    export MYSQL_PASSWORD='{mysql_conn.password}'
    
    # ------------------------ PYTHON --------------------------
    export PYTHONPATH=/mnt/c/Users/Aditya/PycharmProjects/de_project1:$PYTHONPATH
    
    # ------------------------- Spark --------------------------
    spark-submit \
    --master local[3] \
    --driver-memory 4g \
    --conf spark.driver.memoryOverhead=512m \
    --conf spark.hadoop.fs.s3a.fast.upload=true \
    --conf "spark.driver.extraJavaOptions=-Dlog4j.configuration=file:/mnt/c/Users/Aditya/PycharmProjects/de_project1/resources/dev/log4j.properties" \
    --conf "spark.executor.extraJavaOptions=-Dlog4j.configuration=file:/mnt/c/Users/Aditya/PycharmProjects/de_project1/resources/dev/log4j.properties" \
    /mnt/c/Users/Aditya/PycharmProjects/de_project1/scripts/main/transformations/jobs/main.py
    """


def validate_sales_data():
    hook = MySqlHook(mysql_conn_id="mysql_default")

    with hook.get_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) FROM customers_monthly_sales")
            row_count = cursor.fetchone()[0]

    if row_count == 0:
        raise AirflowFailException("customers_monthly_sales table is empty")


default_args = {
    "owner": "Aditya",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=10)
}

with DAG(
    dag_id="retail_sales_batch_etl_local",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule="@Monthly",
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
        bash_command=spark_bash_command()
    )

    validate_data = PythonOperator(
        task_id="validate_data",
        python_callable=validate_sales_data
    )

    wait_for_sales_data >> run_spark_etl >> validate_data