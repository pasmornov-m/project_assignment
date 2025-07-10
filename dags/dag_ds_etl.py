from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from db_utils.postgres_tools import run_sql_file
from db_utils.check_postges import prepare_db
from etl.stages import sync_ds_tables, dm_f101_round_f_to_csv, reload_dm_f101_round_f_csv
from config import (
    db_name,
    ds_schema_name,
    ds_sql_filename,
    dm_sql_filename,
    raw_files_info,
    ds_tables_pkeys,
    fill_account_turnover_f_filename,
    fill_account_balance_f_filename,
    fill_f101_round_f_filename,
    dm_schema_name,
    dm_f101_round_f_filename,
)


default_args = {
    'owner': 'maxp',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
    "retry_delay": timedelta(minutes=11)
}

dag = DAG(
    dag_id = "project_etl",
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

def prepare_dm_layer(**kwargs):
    prepare_db(
        db_name=kwargs['db_name'],
        schema_name=kwargs['schema_name'],
        sql_filename=kwargs['sql_filename']
    )

def run_procedure_fill_account_turnover_f(**kwargs):
    run_sql_file(
        db_name=kwargs['db_name'],
        sql_filename=kwargs['sql_filename']
    )

def run_procedure_fill_account_balance_f(**kwargs):
    run_sql_file(
        db_name=kwargs['db_name'],
        sql_filename=kwargs['sql_filename']
    )

def run_procedure_fill_f101_round_f(**kwargs):
    run_sql_file(
        db_name=kwargs['db_name'],
        sql_filename=kwargs['sql_filename']
    )

def run_dm_f101_round_f_to_csv(**kwargs):
    dm_f101_round_f_to_csv(
        db_name=kwargs['db_name'],
        schema_name=kwargs['schema_name'],
        table_name=kwargs['table_name'],
        filename=kwargs['filename']
    )

def run_reload_dm_f101_round_f_csv(**kwargs):
    reload_dm_f101_round_f_csv(
        db_name=kwargs['db_name'], 
        schema_name=kwargs['schema_name'], 
        table_name=kwargs['table_name'], 
        filename=kwargs['filename'])

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

prepare_dm_layer_task = PythonOperator(
    task_id='prepare_dm_layer',
    python_callable=prepare_dm_layer,
    op_kwargs={
        "db_name": db_name,
        "schema_name": dm_schema_name,
        "sql_filename": dm_sql_filename,
    },
    dag=dag
)

procedure_fill_account_turnover_f_task = PythonOperator(
    task_id='procedure_fill_account_turnover_f',
    python_callable=run_procedure_fill_account_turnover_f,
    op_kwargs={
        "db_name": db_name,
        "sql_filename": fill_account_turnover_f_filename,
    },
    dag=dag
)

procedure_fill_account_balance_f_task = PythonOperator(
    task_id='procedure_fill_account_balance_f',
    python_callable=run_procedure_fill_account_balance_f,
    op_kwargs={
        "db_name": db_name,
        "sql_filename": fill_account_balance_f_filename,
    },
    dag=dag
)

procedure_fill_f101_round_f_task = PythonOperator(
    task_id='procedure_fill_f101_round_f',
    python_callable=run_procedure_fill_f101_round_f,
    op_kwargs={
        "db_name": db_name,
        "sql_filename": fill_f101_round_f_filename,
    },
    dag=dag
)

run_dm_f101_round_f_to_csv_task = PythonOperator(
    task_id='dm_f101_round_f_to_csv',
    python_callable=run_dm_f101_round_f_to_csv,
    op_kwargs={
        "db_name": db_name,
        "schema_name": dm_schema_name, 
        "table_name": 'dm_f101_round_f',
        "filename": dm_f101_round_f_filename
    },
    dag=dag
)

reload_dm_f101_round_f_csv_task = PythonOperator(
    task_id='reload_dm_f101_round_f_csv',
    python_callable=run_reload_dm_f101_round_f_csv,
    op_kwargs={
        "db_name": db_name,
        "schema_name": dm_schema_name, 
        "table_name": 'dm_f101_round_f_v2',
        "filename": dm_f101_round_f_filename,
    },
    dag=dag
)


sync_task >> \
prepare_dm_layer_task >> \
procedure_fill_account_turnover_f_task >> \
procedure_fill_account_balance_f_task >> \
procedure_fill_f101_round_f_task >> \
run_dm_f101_round_f_to_csv_task >> \
reload_dm_f101_round_f_csv_task