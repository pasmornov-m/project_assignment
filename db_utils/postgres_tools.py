from db_utils.check_postges import prepare_db
from utils.writers import write_to_postgres
from config import db_name, log_schema, log_table, log_sql_filename
from db_utils.spark_schemas import LOG_SCHEMA


def log_to_postgres(spark, start_time, end_time):
    duration = int((end_time - start_time).total_seconds())
    log_data = [{
        'start_time': start_time,
        'end_time': end_time,
        'duration_seconds': duration
    }]
    log_df = spark.createDataFrame(log_data, schema=LOG_SCHEMA)
    prepare_db(db_name=db_name, schema_name=log_schema, sql_filename=log_sql_filename)
    write_to_postgres(df=log_df, table=log_table, db_name=db_name, schema_name=log_schema)