from db_utils.postgres_client import get_postgres_properties


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

