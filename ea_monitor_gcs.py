import pendulum
from airflow import DAG
from airflow.operators.dummy_operator    import DummyOperator
from airflow.contrib.sensors.gcs_sensor  import GoogleCloudStorageObjectSensor
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.utils.trigger_rule import TriggerRule

FECHA_HOY = pendulum.now()
file_name = f"cartera_{FECHA_HOY.format('YYYYMMDD')}.csv" 

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['edaries@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 0,
}

with DAG(
    dag_id="ea_monitor_gcs",
    start_date=pendulum.datetime(2022, 11, 20, tz="UTC"),
    schedule="@once",
    catchup=False,
    default_args=default_args
) as dag:
    start_task  = DummyOperator(  task_id= "start" )
    stop_task   = DummyOperator(  task_id= "stop" , trigger_rule = TriggerRule.ALL_SUCCESS )
    error_task  = DummyOperator(  task_id= "error" , trigger_rule = TriggerRule.ONE_FAILED )

    gcs_file_sensor = GoogleCloudStorageObjectSensor(
        task_id='gcs_file_sensor_task',
        bucket='muchik-csv',
        object=file_name,
        google_cloud_conn_id='google_cloud_muchik')

    load_dataset_cartera = GCSToBigQueryOperator(

        task_id = 'load_dataset_agenda',
        bucket = 'muchik-csv',
        source_objects = [f'{file_name}'],
        destination_project_dataset_table = 'muchik-cix.muchik_dt.cartera',
        write_disposition='WRITE_TRUNCATE',
        source_format = 'csv',
        allow_quoted_newlines = 'true',
        skip_leading_rows = 1,
        gcp_conn_id='google_cloud_muchik',
        schema_fields=[
            {'name': 'fecha_desembolso', 'type': 'DATE', 'mode': 'NULLABLE'},
            {'name': 'expediente', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'importe', 'type': 'NUMERIC', 'mode': 'NULLABLE'},
            {'name': 'tasa', 'type': 'NUMERIC', 'mode': 'NULLABLE'},
            {'name': 'producto', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'tipo_credito', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'agencia', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'destino', 'type': 'STRING', 'mode': 'NULLABLE'},
            ]
        )

    start_task >> gcs_file_sensor >> load_dataset_cartera >> (error_task,stop_task)