from db_utils.check_postges import create_database, create_schema, create_table
from utils.readers import read_csv_to_pd
from utils.writers import write_df_to_postgres
from utils.logger import get_logger
from utils.tools import clean_ds_dfs

import time
from datetime import datetime


logger = get_logger(__name__)

def create_and_load(db_name, schema_name, sql_filename, raw_files_info):
    logger.info("--- Начало процесса создания БД и загрузки данных ---")

    create_database(db_name)
    create_schema(db_name, schema_name)
    create_table(db_name, sql_filename)

    logger.info("Чтение CSV файлов в DataFrame")
    dfs_from_csv = read_csv_to_pd(path=raw_files_info['raw_path'], files=raw_files_info['raw_files'])
    clean_dfs = clean_ds_dfs(dfs_from_csv)

    logger.info("Начало загрузки данных в БД")
    start_time = datetime.now()
    logger.info(f"Время начала загрузки: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    time.sleep(5)

    # ft_balance_f
    logger.info("Загрузка таблицы 'ft_balance_f'")
    write_df_to_postgres(db_name=db_name, schema_name=schema_name, table_name='ft_balance_f', df=clean_dfs['ft_balance_f'])

    # ft_posting_f
    logger.info("Загрузка таблицы 'ft_posting_f'")
    write_df_to_postgres(db_name=db_name, schema_name=schema_name, table_name='ft_posting_f', df=clean_dfs['ft_posting_f'])

    # md_account_d
    logger.info("Загрузка таблицы 'md_account_d'")
    write_df_to_postgres(db_name=db_name, schema_name=schema_name, table_name='md_account_d', df=clean_dfs['md_account_d'])

    # md_currency_d
    logger.info("Загрузка таблицы 'md_currency_d'")
    write_df_to_postgres(db_name=db_name, schema_name=schema_name, table_name='md_currency_d', df=clean_dfs['md_currency_d'])

    # md_exchange_rate_d
    logger.info("Загрузка таблицы 'md_exchange_rate_d'")
    write_df_to_postgres(db_name=db_name, schema_name=schema_name, table_name='md_exchange_rate_d', df=clean_dfs['md_exchange_rate_d'])

    # md_ledger_account_s
    logger.info("Загрузка таблицы 'md_ledger_account_s'")
    write_df_to_postgres(db_name=db_name, schema_name=schema_name, table_name='md_ledger_account_s', df=clean_dfs['md_ledger_account_s'])

    end_time = datetime.now()
    logger.info(f"Время окончания загрузки: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    duration = end_time - start_time
    logger.info(f"Время выполнения загрузки: {duration}")

    logger.info("--- Завершение процесса ---")