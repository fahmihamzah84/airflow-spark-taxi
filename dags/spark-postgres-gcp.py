from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from datetime import datetime, timedelta
from google.cloud import bigquery, storage


###############################################
# Parameters
###############################################
spark_master = "spark://spark:7077"
postgres_driver_jar = "/usr/local/spark/resources/jars/postgresql-9.4.1207.jar"

taxi_file = "/usr/local/spark/resources/data/fhv_taxi.parquet"
postgres_db = "jdbc:postgresql://postgres/test"
postgres_user = "test"
postgres_pwd = "postgres"

os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="/home/fahmihamzah84/google_credentials.json"
PROJECT_ID = "iykra-370008"
BUCKET = "bucket_trip"
list_data = ['question1', 'question2', 'question3', 'question4']

###############################################
# DAG Definition
###############################################
now = datetime.now()

def upload_to_gcs(bucket, object_name, local_file):
    client = storage.Client()
    bucket = storage_client.create_bucket(bucket)
    bucket = client.bucket(bucket)
    for i in range(4):
        blob = bucket.blob(object_name[i])
        blob.upload_from_filename(local_file[i])

def upload_to_bigquery(project_id,dataset , file):
    client = bigquery.Client()
    dataset_id = "{}.your_dataset".format(project_id)
    for i in range (4)
        table_id = f"iykra-370008.{dataset}.{file[i]}"
        job_config = bigquery.LoadJobConfig(autodetect=True,
        skip_leading_rows=1,
        source_format=bigquery.SourceFormat.CSV)
        uri = f"gs://bucket_trip/{file[i]}.csv"

        load_job = client.load_table_from_uri(
            uri, table_id, job_config=job_config
        )

        load_job.result()

        destination_table = client.get_table(table_id)

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(now.year, now.month, now.day),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1)
}

dag = DAG(
        dag_id="spark-postgres", 
        description="This DAG is a sample of integration between Spark and DB. It reads CSV files, load them into a Postgres DB and then read them from the same Postgres DB.",
        default_args=default_args, 
        schedule_interval=timedelta(1)
    )

start = DummyOperator(task_id="start", dag=dag)

spark_job_load_postgres = SparkSubmitOperator(
    task_id="spark_job_load_postgres",
    application="/usr/local/spark/app/load-postgres.py", # Spark application path created in airflow and spark cluster
    name="load-postgres",
    conn_id="spark_default",
    verbose=1,
    conf={"spark.master":spark_master},
    application_args=[taxi_file,postgres_db,postgres_user,postgres_pwd],
    jars=postgres_driver_jar,
    driver_class_path=postgres_driver_jar,
    dag=dag)

spark_job_read_postgres = SparkSubmitOperator(
    task_id="spark_job_read_postgres",
    application="/usr/local/spark/app/read-postgres.py", # Spark application path created in airflow and spark cluster
    name="read-postgres",
    conn_id="spark_default",
    verbose=1,
    conf={"spark.master":spark_master},
    application_args=[postgres_db,postgres_user,postgres_pwd],
    jars=postgres_driver_jar,
    driver_class_path=postgres_driver_jar,
    dag=dag)

local_to_gcs_task = PythonOperator(
    task_id="local_to_gcs_task",
    python_callable=upload_to_gcs,
    op_kwargs={
        "bucket": BUCKET,
        "object_name": list_data + ".csv",
        "local_file": list_data + ".csv"
    }
    dag=dag)

gcs_to_bigquery_task = PythonOperator(
    task_id="gcs_to_bigquery_task",
    python_callable=upload_to_bigquery,
    op_kwargs={
        "project_id": PROJECT_ID,
        "dataset": "fhv_trip",
        "file": list_data 
    }
    dag=dag)


end = DummyOperator(task_id="end", dag=dag)

start >> spark_job_load_postgres >> spark_job_read_postgres >> local_to_gcs_task >> gcs_to_bigquery_task >> end