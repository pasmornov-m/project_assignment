from dotenv import load_dotenv
import os

load_dotenv()


SPARK_APP_NAME = "Project_ETL"
SPARK_MASTER = "local[*]"
SPARK_JARS_DIR = "/opt/spark/spark_jars/postgresql-42.7.5.jar"
# SPARK_JARS_DIR = "/home/maxp/Work/project_assignment/spark_jars/postgresql-42.7.5.jar"

POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
# POSTGRES_HOST = "localhost"
POSTGRES_HOST = "db"
POSTGRES_PORT = 5432
POSTGRES_URL = f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/"


# Logs settings
log_schema = 'logs'
log_table = 'etl_log'
log_sql_filename = 'db_utils/log_table.sql'


####################################################################
# TASK 1
####################################################################

# db settings
db_name = 'project'
ds_schema_name = 'ds'
dm_schema_name = 'dm'

ds_sql_filename = 'db_utils/ds_tables.sql'
dm_sql_filename = 'db_utils/dm_tables.sql'

# procedures
fill_account_turnover_f_filename = 'db_utils/fill_account_turnover_f.sql'
fill_account_balance_f_filename = 'db_utils/fill_account_balance_f.sql'
fill_f101_round_f_filename = 'db_utils/fill_f101_round_f.sql'

dm_f101_round_f_filename = "dm_f101_round_f.csv"


raw_files_path = "raw_data"
raw_files_info = {
    "raw_path": raw_files_path,
    "raw_files": ["ft_balance_f", "ft_posting_f", 
                  "md_account_d", "md_currency_d",
                  "md_exchange_rate_d", "md_ledger_account_s"]
}

ds_tables_pkeys = {
    "ft_balance_f": ["on_date", "account_rk"],
    "md_account_d": ["data_actual_date", "account_rk"],
    "md_currency_d": ["currency_rk", "data_actual_date"],
    "md_exchange_rate_d": ["data_actual_date", "currency_rk"],
    "md_ledger_account_s": ["ledger_account", "start_date"]
}


####################################################################
# TASK 2
####################################################################

dwh_db_name = 'dwh'
rd_schema_name = "rd"

rd_sql_filename = "db_utils/rd_tables.sql"
dm_client_drop_duplicates_filename = "db_utils/dm_client_drop_duplicates.sql"
dm_fill_loan_holiday_info_filename = "db_utils/dm_fill_loan_holiday_info.sql"
correct_account_balance_turnover_filename = "db_utils/correct_account_balance_turnover.sql"

task_2_files_info = {
    "raw_path": raw_files_path,
    "raw_files": ["deal_info", 
                  "product"]
}
