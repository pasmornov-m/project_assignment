from db_utils.check_postges import create_database, create_schema, create_table
from utils.readers import read_csv_to_pd
from utils.writers import write_df_to_postgres, update_from_df_to_postgres
from utils.logger import get_logger
from utils.tools import clean_ds_dfs
from db_utils.postgres_client import get_postgres_properties
from db_utils.check_postges import prepare_db
from db_utils.postgres_tools import log_to_postgres

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


def update_tables(db_name, schema_name, raw_files_info, tables_pkeys):
    logger.info(f"Начало обновления таблиц в схеме '{schema_name}' БД '{db_name}'")
    start_time = time.time()

    dfs_from_csv = read_csv_to_pd(path=raw_files_info['raw_path'], files=raw_files_info['raw_files'])
    clean_dfs = clean_ds_dfs(dfs_from_csv)
    logger.info(f"Данные из csv успешно загружены: {list(clean_dfs.keys())}")

    # Обработка FT_POSTING_F (truncate + insert)
    if 'ft_posting_f' in clean_dfs:
        logger.info(f"Truncate таблицы {schema_name}.ft_posting_f перед полной загрузкой")
        conn = get_postgres_properties(db_name)
        cursor = conn.cursor()
        try:
            cursor.execute(f"TRUNCATE TABLE {schema_name}.ft_posting_f")
            conn.commit()
            logger.info("Таблица успешно очищена.")
        except Exception as e:
            conn.rollback()
            logger.error(f"Ошибка при очистке таблицы: {e}")
            raise
        finally:
            cursor.close()
            conn.close()

        logger.info("Вставка данных в ft_posting_f")
        write_df_to_postgres(db_name, schema_name, table_name="ft_posting_f", df=clean_dfs['ft_posting_f'])
        logger.info("Загрузка данных в ft_posting_f завершена.")
        clean_dfs.pop('ft_posting_f', None)

    # Обработка остальных таблиц с upsert
    for table_name in clean_dfs.keys():
        if table_name not in tables_pkeys:
            logger.warning(f"Для таблицы {table_name} не указаны первичные ключи. Пропуск.")
            continue

        logger.info(f"Обновление данных в {schema_name}.{table_name} с помощью upsert")
        try:
            update_from_df_to_postgres(
                db_name=db_name,
                schema_name=schema_name,
                df=clean_dfs[table_name],
                table_name=table_name,
                pkeys_cols=tables_pkeys[table_name]
            )
            logger.info(f"Данные успешно обновлены в {schema_name}.{table_name}")
        except Exception as e:
            logger.error(f"Ошибка при обновлении {schema_name}.{table_name}: {e}")
            raise

    end_time = time.time()
    duration = round(end_time - start_time, 2)
    logger.info(f"Обновление всех таблиц завершено за {duration} секунд.")


def sync_ds_tables(db_name, schema_name, sql_filename, raw_files_info, tables_pkeys):
    logger.info("=== Начало процесса синхронизации DS ===")
    start_time = datetime.now()
    logger.info(f"Начало: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    time.sleep(5)

    prepare_db(db_name, schema_name, sql_filename)
    update_tables(db_name, schema_name, raw_files_info, tables_pkeys)

    end_time = datetime.now()
    logger.info(f"Окончание: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"Длительность: {end_time - start_time}")

    log_to_postgres(start_time, end_time)

    logger.info("=== Конец процесса ===")