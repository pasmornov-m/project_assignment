from db_utils.check_postges import create_database, create_schema, create_table
from db_utils.postgres_client import get_postgres_properties
from utils.logger import get_logger
from utils.readers import read_csv_to_pd
from utils.writers import write_df_to_postgres, upsert_df_to_postgres
from utils.tools import clean_ds_dfs
import time
from datetime import datetime


logger = get_logger(__name__)

def sync_ds_tables(db_name, schema_name, sql_filename, raw_files_info, tables_pkeys):
    logger.info("=== Начало процесса синхронизации DS ===")
    start_time = datetime.now()
    logger.info(f"Начало: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    time.sleep(5)

    prepare_database(db_name, schema_name, sql_filename)
    clean_dfs = load_csv_and_clean(raw_files_info)
    load_table_data(db_name, schema_name, clean_dfs, tables_pkeys)

    end_time = datetime.now()
    logger.info(f"Окончание: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"Длительность: {end_time - start_time}")
    logger.info("=== Конец процесса ===")