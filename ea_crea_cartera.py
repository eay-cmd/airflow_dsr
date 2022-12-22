import pendulum
from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.dummy_operator    import DummyOperator


with DAG(
    dag_id="ea_crea_cartera_postgres",
    start_date=pendulum.datetime(2022, 11, 20, tz="UTC"),
    schedule="@once",
    catchup=False,
) as dag:

    start_task  = DummyOperator(  task_id= "start" )
    stop_task   = DummyOperator(  task_id= "stop"  )
    crear_tabla = PostgresOperator(
        task_id="crear_tabla",
        sql="""
            CREATE TABLE IF NOT EXISTS cartera (
                fecha_desembolso  date,
                expdiente   integer,
                importe     decimal(10,2),
                tasa        decimal(5,2),
                producto    varchar(30),
                tipo_credito    varchar(30),
                agencia     varchar(40),
                destino     varchar(40)
            );
          """,
        postgres_conn_id = "postgres",
        dag=dag
    )

    limpiar_tabla = PostgresOperator(
        task_id="limpiar_tabla",
        sql="""
            DELETE FROM cartera WHERE 1=1;
          """,
        postgres_conn_id = "postgres",
        dag=dag
    )

    poblar_tabla = PostgresOperator(
        task_id="poblar_tabla",
        sql="""
            INSERT INTO cartera VALUES('2022-12-18',1244,500,65,'CONSUMO','CONSUMO','CHICLAYO','LIBRE DISPONIBILIDAD');
          """,
        postgres_conn_id = "postgres",
        dag=dag
    )

    start_task >> crear_tabla >> limpiar_tabla >> poblar_tabla >> stop_task
