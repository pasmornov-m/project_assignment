from utils.readers import read_csv_to_spark
from utils.writers import write_to_postgres, upsert_spark_df_to_postgres, save_csv_from_pg
from utils.logger import get_logger
from utils.tools import transform_ds_dfs, transform_dm_f101_round_f, transform_rd_dfs
from clients.postgres_client import get_pg_props_psycopg2
from clients.spark_client import create_spark_session
from db_utils.check_postges import prepare_db
from db_utils.postgres_tools import log_to_postgres
from config import raw_files_path

import time
from datetime import datetime
from zoneinfo import ZoneInfo


logger = get_logger(__name__)


def update_ds_tables(spark, db_name, schema_name, raw_files_info, tables_pkeys):
    logger.info(f"Начало обновления таблиц в схеме '{schema_name}' БД '{db_name}'")
    start_time = time.time()
    dfs_from_csv = read_csv_to_spark(spark=spark, path=raw_files_info['raw_path'], files=raw_files_info['raw_files'])
    clean_dfs = transform_ds_dfs(dfs_from_csv)
    logger.info(f"Данные из csv успешно загружены: {list(clean_dfs.keys())}")

    # Обработка ft_posting_f (truncate + insert)
    if 'ft_posting_f' in clean_dfs:
        logger.info(f"Truncate таблицы {schema_name}.ft_posting_f перед полной загрузкой")
        conn = get_pg_props_psycopg2(db_name)
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
        write_to_postgres(df=clean_dfs['ft_posting_f'], table="ft_posting_f", db_name=db_name, schema_name=schema_name)
        logger.info("Загрузка данных в ft_posting_f завершена.")
        clean_dfs.pop('ft_posting_f', None)

    # Обработка остальных таблиц с upsert
    for table_name in clean_dfs.keys():
        if table_name not in tables_pkeys:
            logger.warning(f"Для таблицы {table_name} не указаны первичные ключи. Пропуск.")
            continue

        logger.info(f"Обновление данных в {schema_name}.{table_name} с помощью upsert")
        try:
            upsert_spark_df_to_postgres(
                df=clean_dfs[table_name],
                db_name=db_name,
                schema_name=schema_name,
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
    logger.info("=== Начало процесса синхронизации DS таблиц ===")
    start_time = datetime.now(ZoneInfo("Europe/Moscow")).replace(tzinfo=None)
    logger.info(f"Начало: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    time.sleep(5)

    spark = create_spark_session()
    prepare_db(db_name, schema_name, sql_filename)
    update_ds_tables(spark, db_name, schema_name, raw_files_info, tables_pkeys)

    end_time = datetime.now(ZoneInfo("Europe/Moscow")).replace(tzinfo=None)
    logger.info(f"Окончание: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"Длительность: {end_time - start_time}")

    log_to_postgres(spark, 'run_sync_ds_tables', start_time, end_time)

    logger.info("=== Конец процесса ===")

def dm_f101_round_f_to_csv(db_name, schema_name, table_name, filename):
    logger.info("=== Начало процесса сохранения dm_f101_round_f в csv===")
    start_time = datetime.now(ZoneInfo("Europe/Moscow")).replace(tzinfo=None)
    logger.info(f"Начало: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")

    spark = create_spark_session()
    save_csv_from_pg(spark=spark, db_name=db_name, schema_name=schema_name, table_name=table_name, filename=filename)

    end_time = datetime.now(ZoneInfo("Europe/Moscow")).replace(tzinfo=None)
    logger.info(f"Окончание: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"Длительность: {end_time - start_time}")

    log_to_postgres(spark, 'run_dm_f101_round_f_to_csv', start_time, end_time)

    logger.info("=== Конец процесса ===")

def reload_dm_f101_round_f_csv(db_name, schema_name, table_name, filename):
    logger.info("=== Начало процесса загрузки dm_f101_round_f из csv в таблицу===")
    start_time = datetime.now(ZoneInfo("Europe/Moscow")).replace(tzinfo=None)
    logger.info(f"Начало: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")

    spark = create_spark_session()
    df_dict = read_csv_to_spark(spark, raw_files_path, filename)
    df = transform_dm_f101_round_f(df_dict[filename])
    write_to_postgres(df, db_name, schema_name, table_name, mode='overwrite')

    end_time = datetime.now(ZoneInfo("Europe/Moscow")).replace(tzinfo=None)
    logger.info(f"Окончание: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"Длительность: {end_time - start_time}")

    log_to_postgres(spark, 'reload_dm_f101_round_f_csv', start_time, end_time)

    logger.info("=== Конец процесса ===")


def sync_rd_tables(db_name, schema_name, sql_filename, raw_files_info):
    logger.info("=== Начало процесса синхронизации таблиц ===")
    start_time = datetime.now(ZoneInfo("Europe/Moscow")).replace(tzinfo=None)
    logger.info(f"Начало: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    time.sleep(5)

    spark = create_spark_session()
    prepare_db(db_name, schema_name, sql_filename)
    update_rd_tables(spark, db_name, schema_name, raw_files_info)

    end_time = datetime.now(ZoneInfo("Europe/Moscow")).replace(tzinfo=None)
    logger.info(f"Окончание: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"Длительность: {end_time - start_time}")

    log_to_postgres(spark, 'run_sync_rd_tables', start_time, end_time, db_name=db_name)

    logger.info("=== Конец процесса ===")


def update_rd_tables(spark, db_name, schema_name, raw_files_info):
    logger.info(f"Начало обновления таблиц в схеме '{schema_name}' БД '{db_name}'")
    start_time = time.time()
    dfs_from_csv = read_csv_to_spark(spark=spark, path=raw_files_info['raw_path'], files=raw_files_info['raw_files'], delim=',', enc='utf-8')
    clean_dfs = transform_rd_dfs(dfs_from_csv)
    logger.info(f"Данные из csv успешно загружены: {list(clean_dfs.keys())}")


    for table_name in clean_dfs:
        logger.info(f"Truncate таблицы {schema_name}.{table_name} перед полной загрузкой")
        conn = get_pg_props_psycopg2(db_name)
        cursor = conn.cursor()
        try:
            cursor.execute(f"TRUNCATE TABLE {schema_name}.{table_name}")
            conn.commit()
            logger.info("Таблица успешно очищена.")
        except Exception as e:
            conn.rollback()
            logger.error(f"Ошибка при очистке таблицы: {e}")
            raise
        finally:
            cursor.close()
            conn.close()

        logger.info(f"Вставка данных в {table_name}")
        write_to_postgres(df=clean_dfs[table_name], table=table_name, db_name=db_name, schema_name=schema_name)
        logger.info("Загрузка данных в {table_name} завершена.")

    end_time = time.time()
    duration = round(end_time - start_time, 2)
    logger.info(f"Обновление всех таблиц завершено за {duration} секунд.")