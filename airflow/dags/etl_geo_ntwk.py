from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

HDFS = "hdfs://namenode:9000"
JAR  = "/opt/jobs/etl-job-0.1.jar"
CONN = "spark_default"

SPARK_CONF = {
    "spark.hadoop.fs.defaultFS": HDFS,
    "spark.hadoop.hadoop.user.name": "root",
    "spark.driver.memory": "1g",
    "spark.executor.memory": "1g",
    "spark.executor.instances": "1",
    "spark.executor.cores": "1",
    "spark.cores.max": "1",
}

default_args = {
    "owner": "local",
    "retries": 1,
    "retry_delay": timedelta(seconds=30),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="mock_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    default_args=default_args,
    tags=["scala", "spark", "hdfs", "cell-tower"],
) as dag:

    check_reference = BashOperator(
        task_id="check_reference",
        bash_command="""
            set -e
            for t in msisdns towers registers; do
                if ! hdfs dfs -test -e hdfs://namenode:9000/data/ref/$t/_SUCCESS; then
                    echo "Missing reference table: $t"
                    echo "Run 00_generate_reference.ipynb in Jupyter first."
                    exit 1
                fi
            done
            echo "All reference tables present."
        """,
    )

    generate = SparkSubmitOperator(
        task_id="generate_mock",
        application=JAR,
        java_class="com.etl.GenerateMockData",
        conn_id=CONN,
        application_args=[
            f"{HDFS}/data/mock/pings",
            f"{HDFS}/data/mock/cashless",
            "{{ ds }}",
            "200000",
            "50000",
        ],
        conf=SPARK_CONF,
        verbose=True,
    )

    process = SparkSubmitOperator(
        task_id="process_data",
        application=JAR,
        java_class="com.etl.Processing",
        conn_id=CONN,
        application_args=[
            f"{HDFS}/data/mock/pings",
            f"{HDFS}/data/mock/cashless",
            f"{HDFS}/data/processed",
            "{{ ds }}",
        ],
        conf=SPARK_CONF,
        verbose=True,
    )

    check_reference >> generate >> process