from db_utils.postgres_client import get_postgres_properties
from utils.logger import get_logger
from utils.readers import read_csv_to_pd
from utils.writers import write_df_to_postgres, upsert_df_to_postgres
from utils.tools import clean_ds_dfs
import time


logger = get_logger(__name__)

def update_tables(db_name, schema_name, raw_files_info, tables_pkeys):  
    logger.info(f"---Начало обновления таблиц в схеме '{schema_name}' БД '{db_name}'---")
    start_time = time.time()

    dfs_from_csv = read_csv_to_pd(path=raw_files_info['raw_path'], files=raw_files_info['raw_files'])
    clean_dfs = clean_ds_dfs(dfs_from_csv)
    logger.info(f"CSV-файлы успешно загружены в память: {list(clean_dfs.keys())}")

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

        logger.info("Вставка данных в ft_posting_f...")
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
            upsert_df_to_postgres(
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

    logger.info("--- Завершение процесса ---")