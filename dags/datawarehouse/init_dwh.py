from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from datawarehouse.data_utils import create_schema, create_table

with DAG(
    dag_id="init_dwh",
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,  # вручную
    catchup=False,
) as dag:

    create_staging_schema = PythonOperator(
        task_id="create_staging_schema",
        python_callable=create_schema,
        op_args=["staging"],
    )

    create_staging_table = PythonOperator(
        task_id="create_staging_table",
        python_callable=create_table,
        op_args=["staging"],
    )

    create_core_schema = PythonOperator(
        task_id="create_core_schema",
        python_callable=create_schema,
        op_args=["core"],
    )

    create_core_table = PythonOperator(
        task_id="create_core_table",
        python_callable=create_table,
        op_args=["core"],
    )

    (
        create_staging_schema
        >> create_staging_table
        >> create_core_schema
        >> create_core_table
    )
