from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from config import db_name, ds_schema_name, ds_sql_filename, raw_files_info, ds_tables_pkeys
from etl.stages import sync_ds_tables


default_args = {
    'owner': 'maxp',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
    "retry_delay": timedelta(minutes=11)
}

dag = DAG(
    dag_id = "sync_ds",
    default_args=default_args,
    schedule="@hourly",
    catchup=False,
    tags=['etl'],
)

def run_sync_ds_tables(**kwargs):
    sync_ds_tables(
        db_name=kwargs['db_name'],
        schema_name=kwargs['schema_name'],
        sql_filename=kwargs['sql_filename'],
        raw_files_info=kwargs['raw_files_info'],
        tables_pkeys=kwargs['tables_pkeys']
    )

sync_task = PythonOperator(
    task_id='sync_ds',
    python_callable=run_sync_ds_tables,
    op_kwargs={
        "db_name": db_name,
        "schema_name": ds_schema_name,
        "sql_filename": ds_sql_filename,
        "raw_files_info": raw_files_info,
        "tables_pkeys": ds_tables_pkeys
    },
    dag=dag
)