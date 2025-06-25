from dotenv import load_dotenv
import os

load_dotenv()

POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
# POSTGRES_HOST = "localhost"
POSTGRES_HOST = "db"
POSTGRES_PORT = 5432


# DS settings
db_name = 'project'
ds_schema_name='ds'
ds_sql_filename='db_utils/ds_tables.sql'

raw_files_info = {
    "raw_path": "raw_data",
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