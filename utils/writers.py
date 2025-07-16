from clients.postgres_client import get_pg_props_psycopg2, get_pg_props_spark
from utils.logger import get_logger
from config import raw_files_path
import os
import shutil

logger = get_logger(__name__)


def write_to_postgres(df, db_name, schema_name, table, mode='append'):
    db_props = get_pg_props_spark(db_name)
    df.write.jdbc(
        url=db_props['url'],
        table=f"{schema_name}.{table}",
        mode=mode,
        properties={
            "user": db_props['user'],
            "password": db_props['password'],
            "driver": db_props['driver']
        }
    )

def upsert_spark_df_to_postgres(df, db_name, schema_name, table_name, pkeys_cols, temp_table_name="temp_upsert_table"):

    write_to_postgres(df=df, db_name=db_name, schema_name=schema_name, table=temp_table_name, mode="overwrite")

    conn = get_pg_props_psycopg2(db_name)
    cursor = conn.cursor()

    columns = df.columns
    cols_str = ', '.join(columns)
    updates = ', '.join([f"{col} = EXCLUDED.{col}" for col in columns if col not in pkeys_cols])
    conflict_keys = ', '.join(pkeys_cols)

    upsert_query = f"""
        INSERT INTO {schema_name}.{table_name} ({cols_str})
        SELECT {cols_str} FROM {schema_name}.{temp_table_name}
        ON CONFLICT ({conflict_keys})
        DO UPDATE SET {updates};
    """

    drop_temp_query = f"DROP TABLE IF EXISTS {schema_name}.{temp_table_name};"

    try:
        cursor.execute(upsert_query)
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        try:
            cursor.execute(drop_temp_query)
            conn.commit()
        except Exception as e:
            conn.rollback()
            print(f"Ошибка при удалении временной таблицы: {e}")
        cursor.close()
        conn.close()


def save_csv_from_pg(spark, db_name, schema_name, table_name, filename, overwrite=None):
    output_path = f"/opt/airflow/{raw_files_path}/{filename}"
    temp_dir = "/tmp/spark_csv_export"

    if os.path.exists(output_path) and overwrite is None:
        logger.info(f"Файл {output_path} уже существует. Запись не выполнена.")
        return
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    db_props = get_pg_props_spark(db_name)
    df = spark.read.jdbc(
        url=db_props['url'],
        table=f"{schema_name}.{table_name}",
        properties={
            "user": db_props['user'],
            "password": db_props['password'],
            "driver": db_props['driver']
        }
    )

    df.coalesce(1).write.option("header", True).option("delimiter", ";").mode("overwrite").csv(temp_dir)

    for file in os.listdir(temp_dir):
        if file.startswith("part-") and file.endswith(".csv"):
            src = os.path.join(temp_dir, file)
            shutil.move(src, output_path)
            break
    shutil.rmtree(temp_dir)
    logger.info(f"Таблица dm_f101_round_f сохранена как {output_path}")
