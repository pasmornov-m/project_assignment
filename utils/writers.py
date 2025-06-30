from clients.postgres_client import get_pg_props_psycopg2, get_pg_props_spark
from utils.logger import get_logger


logger = get_logger(__name__)


def write_to_postgres(df, table, db_name, schema_name, mode='append'):
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

    write_to_postgres(df=df, table=temp_table_name, db_name=db_name, schema_name=schema_name, mode="overwrite")

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
