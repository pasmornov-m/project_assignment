from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, FloatType, TimestampType

FT_BALANCE_F = StructType([
    StructField("on_date", DateType(), False),
    StructField("account_rk", IntegerType(), False),
    StructField("currency_rk", IntegerType(), True),
    StructField("balance_out", FloatType(), True),
])
FT_POSTING_F = StructType([
    StructField("oper_date", DateType(), False),
    StructField("credit_account_rk", IntegerType(), False),
    StructField("debet_account_rk", IntegerType(), False),
    StructField("credit_amount", FloatType(), True),
    StructField("debet_amount", FloatType(), True),
])
MD_ACCOUNT_D = StructType([
    StructField("data_actual_date", DateType(), False),
    StructField("data_actual_end_date", DateType(), False),
    StructField("account_rk", IntegerType(), False),
    StructField("account_number", StringType(), False),
    StructField("char_type", StringType(), False),
    StructField("currency_rk", IntegerType(), False),
    StructField("currency_code", StringType(), False),
])
MD_CURRENCY_D = StructType([
    StructField("currency_rk", IntegerType(), False),
    StructField("data_actual_date", DateType(), False),
    StructField("data_actual_end_date", DateType(), True),
    StructField("currency_code", StringType(), True),
    StructField("code_iso_char", StringType(), True),
])
MD_EXCHANGE_RATE_D = StructType([
    StructField("data_actual_date", DateType(), False),
    StructField("data_actual_end_date", DateType(), True),
    StructField("currency_rk", IntegerType(), False),
    StructField("reduced_cource", FloatType(), True),
    StructField("code_iso_num", StringType(), True),
])
MD_LEDGER_ACCOUNT_S = StructType([
    StructField("chapter", StringType(), True),
    StructField("chapter_name", StringType(), True),
    StructField("section_number", IntegerType(), True),
    StructField("section_name", StringType(), True),
    StructField("subsection_name", StringType(), True),
    StructField("ledger1_account", IntegerType(), True),
    StructField("ledger1_account_name", StringType(), True),
    StructField("ledger_account", IntegerType(), False),
    StructField("ledger_account_name", StringType(), True),
    StructField("characteristic", StringType(), True),
    StructField("is_resident", IntegerType(), True),
    StructField("is_reserve", IntegerType(), True),
    StructField("is_reserved", IntegerType(), True),
    StructField("is_loan", IntegerType(), True),
    StructField("is_reserved_assets", IntegerType(), True),
    StructField("is_overdue", IntegerType(), True),
    StructField("is_interest", IntegerType(), True),
    StructField("pair_account", StringType(), True),
    StructField("start_date", DateType(), False),
    StructField("end_date", DateType(), True),
    StructField("is_rub_only", IntegerType(), True),
    StructField("min_term", StringType(), True),
    StructField("min_term_measure", StringType(), True),
    StructField("max_term", StringType(), True),
    StructField("max_term_measure", StringType(), True),
    StructField("ledger_acc_full_name_translit", StringType(), True),
    StructField("is_revaluation", StringType(), True),
    StructField("is_correct", StringType(), True),
])

LOG_SCHEMA = StructType([
    StructField("operation_name", StringType(), False),
    StructField("start_time", TimestampType(), False),
    StructField("end_time", TimestampType(), False),
    StructField("duration_sec", IntegerType(), False)
])