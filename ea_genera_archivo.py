import pendulum
from airflow import DAG
from airflow.providers.google.cloud.transfers.postgres_to_gcs import PostgresToGCSOperator
from airflow.operators.dummy_operator    import DummyOperator
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from datetime import timedelta
import os


GCS_BUCKET = 'muchik-csv'
GCS_OBJECT_PATH = 'postgres-test'
SOURCE_TABLE_NAME = 'agenda'
FECHA_HOY = pendulum.now()
FECHA_AYER = days_ago(1)

def my_func():
    archivo=f"cartera_{FECHA_HOY.format('YYYYMMDD')}.csv"
    print(archivo)

with DAG(
    dag_id="ea_pg_to_gcs",
    start_date=pendulum.datetime(2022, 11, 20, tz="UTC"),
    schedule="@once",
    catchup=False,
) as dag:

    start_task  = DummyOperator(  task_id= "start" )
    stop_task   = DummyOperator(  task_id= "stop"  )

    python_task = PythonOperator(task_id='python_task', python_callable=my_func, dag=dag)


    postgres_to_gcs_task = PostgresToGCSOperator(
        task_id="postgres_to_gcs",
        postgres_conn_id='postgres',
        sql=f"SELECT * FROM cartera where fecha_desembolso = '{FECHA_HOY}';",
        bucket='muchik-csv',
        filename=f"cartera_{FECHA_HOY.format('YYYYMMDD')}.csv",
        export_format='csv',
        gzip=False,
        use_server_side_cursor=True,
        gcp_conn_id='google_cloud_muchik')


    start_task >> python_task >> postgres_to_gcs_task >> stop_task

