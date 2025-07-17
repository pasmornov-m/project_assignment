from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from db_utils.postgres_tools import run_sql_file
from etl.stages import sync_rd_tables
from config import (
    dwh_db_name,
    rd_schema_name,
    rd_sql_filename,
    task_2_files_info,
    dm_client_drop_duplicates_filename,
    dm_fill_loan_holiday_info_filename,
    correct_account_balance_turnover_filename,
)


default_args = {
    'owner': 'maxp',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
    "retry_delay": timedelta(minutes=1)
}

dag = DAG(
    dag_id = "task_2_etl",
    default_args=default_args,
    schedule="@hourly",
    catchup=False,
    tags=['etl'],
)


def run_procedure_dm_client_drop_duplicates(**kwargs):
    run_sql_file(
        db_name=kwargs['db_name'],
        sql_filename=kwargs['sql_filename']
    )

def run_sync_rd_tables(**kwargs):
    sync_rd_tables(
        db_name=kwargs['db_name'],
        schema_name=kwargs['schema_name'],
        sql_filename=kwargs['sql_filename'],
        raw_files_info=kwargs['raw_files_info'],
    )

def run_procedure_dm_fill_loan_holiday_info(**kwargs):
    run_sql_file(
        db_name=kwargs['db_name'],
        sql_filename=kwargs['sql_filename']
    )

def run_procedure_correct_account_balance_turnover(**kwargs):
    run_sql_file(
        db_name=kwargs['db_name'],
        sql_filename=kwargs['sql_filename']
    )

procedure_dm_client_drop_duplicates_task = PythonOperator(
    task_id='procedure_dm_client_drop_duplicates',
    python_callable=run_procedure_dm_client_drop_duplicates,
    op_kwargs={
        "db_name": dwh_db_name,
        "sql_filename": dm_client_drop_duplicates_filename
    },
    dag=dag
)

sync_task = PythonOperator(
    task_id='sync_rd',
    python_callable=run_sync_rd_tables,
    op_kwargs={
        "db_name": dwh_db_name,
        "schema_name": rd_schema_name,
        "sql_filename": rd_sql_filename,
        "raw_files_info": task_2_files_info,
    },
    dag=dag
)

procedure_dm_fill_loan_holiday_info_task = PythonOperator(
    task_id='procedure_dm_fill_loan_holiday_info',
    python_callable=run_procedure_dm_fill_loan_holiday_info,
    op_kwargs={
        "db_name": dwh_db_name,
        "sql_filename": dm_fill_loan_holiday_info_filename
    },
    dag=dag
)

procedure_correct_account_balance_turnover_task = PythonOperator(
    task_id='procedure_correct_account_balance_turnover',
    python_callable=run_procedure_correct_account_balance_turnover,
    op_kwargs={
        "db_name": dwh_db_name,
        "sql_filename": correct_account_balance_turnover_filename
    },
    dag=dag
)

procedure_dm_client_drop_duplicates_task >> \
sync_task >> \
procedure_dm_fill_loan_holiday_info_task >> \
procedure_correct_account_balance_turnover_task