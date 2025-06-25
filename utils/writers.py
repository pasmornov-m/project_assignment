from db_utils.postgres_client import get_postgres_properties
from utils.logger import get_logger
from psycopg2.extras import execute_values


logger = get_logger(__name__)

def write_df_to_postgres(db_name, schema_name, table_name, df):
    conn = get_postgres_properties(db_name)
    cursor = conn.cursor()

    cols = list(df.columns)
    cols_str = ', '.join(cols)

    placeholders = ', '.join(['%s'] * len(cols))

    insert_query = f"INSERT INTO {schema_name}.{table_name} ({cols_str}) VALUES ({placeholders})"

    data_tuples = [tuple(x) for x in df.to_numpy()]

    try:
        cursor.executemany(insert_query, data_tuples)
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        cursor.close()


def update_from_df_to_postgres(db_name, schema_name, df, table_name, pkeys_cols):
    conn = get_postgres_properties(db_name)
    cursor = conn.cursor()

    columns = list(df.columns)
    values = [tuple(x) for x in df.to_numpy()]
    cols_str = ', '.join(columns)

    updates = ', '.join([f"{col} = EXCLUDED.{col}" for col in columns if col not in pkeys_cols])
    conflict_keys = ', '.join(pkeys_cols)

    query = f"""
        INSERT INTO {schema_name}.{table_name} ({cols_str})
        VALUES %s
        ON CONFLICT ({conflict_keys})
        DO UPDATE SET {updates}
    """

    try:
        logger.info(f"Загрузка/обновление данных в {schema_name}.{table_name}")
        execute_values(cursor, query, values)
        conn.commit()
        logger.info(f"Данные успешно обновлены в {schema_name}.{table_name}")
    except Exception as e:
        conn.rollback()
        logger.error(f"Ошибка при upsert в {schema_name}.{table_name}: {e}")
        raise
    finally:
        cursor.close()
        conn.close()
