from config import POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_HOST, POSTGRES_PORT
import psycopg2


def get_postgres_properties(db_name):
    return psycopg2.connect(
            host=POSTGRES_HOST,
            database=db_name,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD,
            port=POSTGRES_PORT
        )