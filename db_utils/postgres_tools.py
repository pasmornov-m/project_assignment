from db_utils.check_postges import prepare_db
from utils.writers import write_df_to_postgres
from config import db_name, log_schema, log_table, log_sql_filename
import pandas as pd


def log_to_postgres(start_time, end_time):
    duration = int((end_time - start_time).total_seconds())
    log_df = pd.DataFrame([{
        'start_time': start_time,
        'end_time': end_time,
        'duration_seconds': duration
    }])
    prepare_db(db_name=db_name, schema_name=log_schema, sql_filename=log_sql_filename)
    write_df_to_postgres(db_name, schema_name=log_schema, table_name=log_table, df=log_df)