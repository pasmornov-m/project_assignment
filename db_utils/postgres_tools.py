from db_utils.check_postges import prepare_db
from utils.writers import write_to_postgres
from clients.postgres_client import get_pg_props_psycopg2
from utils.logger import get_logger
from utils.readers import read_sql_file
from config import db_name, log_schema, log_table, log_sql_filename
from db_utils.spark_schemas import LOG_SCHEMA

logger = get_logger(__name__)


def run_sql_file(db_name, sql_filename):
    conn = get_pg_props_psycopg2(db_name)
    cursor = conn.cursor()

    logger.info(f"Читаю SQL из файла '{sql_filename}'")
    sql_code = read_sql_file(sql_filename)
    logger.debug(f"SQL-код для работы:\n{sql_code}")

    logger.info("Выполняю SQL")
    try:
        cursor.execute(sql_code)
        logger.info("Код успешно выполнен")
    except Exception as e:
        logger.error(f"Ошибка при выполнении: {e}")
        raise

    cursor.close()
    conn.commit()
    conn.close()


def log_to_postgres(spark, operation_name, start_time, end_time, db_name=None):
    duration = int((end_time - start_time).total_seconds())
    log_data = [{
        'operation_name': operation_name,
        'start_time': start_time,
        'end_time': end_time,
        'duration_sec': duration
    }]
    log_df = spark.createDataFrame(log_data, schema=LOG_SCHEMA)
    prepare_db(db_name=db_name, schema_name=log_schema, sql_filename=log_sql_filename)
    write_to_postgres(df=log_df, table=log_table, db_name=db_name, schema_name=log_schema)