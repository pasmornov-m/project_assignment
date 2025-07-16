import chardet
import pyspark.sql.functions as F
from pyspark.sql.types import DateType, StringType, DecimalType, LongType


def detect_encoding(file_path, n_bytes=1000):
    with open(file_path, 'rb') as f:
        rawdata = f.read(n_bytes)
    result = chardet.detect(rawdata)
    return result['encoding']

def transform_ds_dfs(dfs):

    # ft_balance_f
    raw_ft_balance_ft = dfs['ft_balance_f']
    raw_ft_balance_ft = raw_ft_balance_ft.dropDuplicates()
    dfs['ft_balance_f'] = raw_ft_balance_ft \
    .withColumn("on_date", F.to_date("ON_DATE", "dd.MM.yyyy")) \
    .withColumn("account_rk", raw_ft_balance_ft["ACCOUNT_RK"].cast("int")) \
    .withColumn("currency_rk", raw_ft_balance_ft["CURRENCY_RK"].cast("int")) \
    .withColumn("balance_out", raw_ft_balance_ft["BALANCE_OUT"].cast("float"))

    # ft_posting_f
    raw_ft_posting_f = dfs["ft_posting_f"]
    raw_ft_posting_f = raw_ft_posting_f.dropDuplicates()
    dfs["ft_posting_f"] = raw_ft_posting_f \
        .withColumn("oper_date", F.to_date("oper_date", "dd-MM-yyyy").cast("date")) \
        .withColumn("credit_account_rk", raw_ft_posting_f["credit_account_rk"].cast("int")) \
        .withColumn("debet_account_rk", raw_ft_posting_f["debet_account_rk"].cast("int")) \
        .withColumn("credit_amount", raw_ft_posting_f["credit_amount"].cast("float")) \
        .withColumn("debet_amount", raw_ft_posting_f["debet_amount"].cast("float"))

    # md_account_d
    raw_md_account_d = dfs["md_account_d"]
    raw_md_account_d = raw_md_account_d.dropDuplicates()
    dfs["md_account_d"] = raw_md_account_d \
        .withColumn("data_actual_date", F.to_date("data_actual_date", "yyyy-MM-dd").cast("date")) \
        .withColumn("data_actual_end_date", F.to_date("data_actual_end_date", "yyyy-MM-dd").cast("date")) \
        .withColumn("account_rk", raw_md_account_d["account_rk"].cast("int")) \
        .withColumn("account_number", raw_md_account_d["account_number"].cast("string")) \
        .withColumn("char_type", raw_md_account_d["char_type"].cast("string")) \
        .withColumn("currency_rk", raw_md_account_d["currency_rk"].cast("int")) \
        .withColumn("currency_code", raw_md_account_d["currency_code"].cast("string"))

    # md_currency_d
    raw_md_currency_d = dfs["md_currency_d"]
    raw_md_currency_d = raw_md_currency_d.dropDuplicates()
    dfs["md_currency_d"] = raw_md_currency_d \
        .withColumn("currency_rk", raw_md_currency_d["currency_rk"].cast("int")) \
        .withColumn("data_actual_date", F.to_date("data_actual_date", "yyyy-MM-dd").cast("date")) \
        .withColumn("data_actual_end_date", F.to_date("data_actual_end_date", "yyyy-MM-dd").cast("date")) \
        .withColumn("currency_code", raw_md_currency_d["currency_code"].cast("string")) \
        .withColumn("code_iso_char", raw_md_currency_d["code_iso_char"].cast("string"))

    # md_exchange_rate_d
    raw_md_exchange_rate_d = dfs["md_exchange_rate_d"]
    raw_md_exchange_rate_d = raw_md_exchange_rate_d.dropDuplicates()
    dfs["md_exchange_rate_d"] = raw_md_exchange_rate_d \
        .withColumn("data_actual_date", F.to_date("data_actual_date", "yyyy-MM-dd").cast("date")) \
        .withColumn("data_actual_end_date", F.to_date("data_actual_end_date", "yyyy-MM-dd").cast("date")) \
        .withColumn("currency_rk", raw_md_exchange_rate_d["currency_rk"].cast("int")) \
        .withColumn("reduced_cource", raw_md_exchange_rate_d["reduced_cource"].cast("float")) \
        .withColumn("code_iso_num", raw_md_exchange_rate_d["code_iso_num"].cast("string"))

    # md_ledger_account_s
    raw_md_ledger_account_s = dfs["md_ledger_account_s"]
    raw_md_ledger_account_s = raw_md_ledger_account_s.dropDuplicates()
    dfs["md_ledger_account_s"] = raw_md_ledger_account_s \
        .withColumn("chapter", raw_md_ledger_account_s["chapter"].cast("string")) \
        .withColumn("chapter_name", raw_md_ledger_account_s["chapter_name"].cast("string")) \
        .withColumn("section_number", raw_md_ledger_account_s["section_number"].cast("int")) \
        .withColumn("section_name", raw_md_ledger_account_s["section_name"].cast("string")) \
        .withColumn("subsection_name", raw_md_ledger_account_s["subsection_name"].cast("string")) \
        .withColumn("ledger1_account", raw_md_ledger_account_s["ledger1_account"].cast("int")) \
        .withColumn("ledger1_account_name", raw_md_ledger_account_s["ledger1_account_name"].cast("string")) \
        .withColumn("ledger_account", raw_md_ledger_account_s["ledger_account"].cast("int")) \
        .withColumn("ledger_account_name", raw_md_ledger_account_s["ledger_account_name"].cast("string")) \
        .withColumn("characteristic", raw_md_ledger_account_s["characteristic"].cast("string")) \
        .withColumn("start_date", F.to_date("start_date", "yyyy-MM-dd").cast("date")) \
        .withColumn("end_date", F.to_date("end_date", "yyyy-MM-dd").cast("date"))

    return dfs


def transform_dm_f101_round_f(df):
    df = df \
    .withColumn("from_date", F.to_date("from_date", "yyyy-MM-dd").cast(DateType())) \
    .withColumn("to_date", F.to_date("to_date", "yyyy-MM-dd").cast(DateType())) \
    .withColumn("chapter", df["chapter"].cast(StringType())) \
    .withColumn("ledger_account", df["ledger_account"].cast(StringType())) \
    .withColumn("characteristic", df["characteristic"].cast(StringType())) \
    .withColumn("balance_in_rub", df["balance_in_rub"].cast(DecimalType(23, 8))) \
    .withColumn("balance_in_val", df["balance_in_val"].cast(DecimalType(23, 8))) \
    .withColumn("balance_in_total", df["balance_in_total"].cast(DecimalType(23, 8))) \
    .withColumn("turn_deb_rub", df["turn_deb_rub"].cast(DecimalType(23, 8))) \
    .withColumn("turn_deb_val", df["turn_deb_val"].cast(DecimalType(23, 8))) \
    .withColumn("turn_deb_total", df["turn_deb_total"].cast(DecimalType(23, 8))) \
    .withColumn("turn_cre_rub", df["turn_cre_rub"].cast(DecimalType(23, 8))) \
    .withColumn("turn_cre_val", df["turn_cre_val"].cast(DecimalType(23, 8))) \
    .withColumn("turn_cre_total", df["turn_cre_total"].cast(DecimalType(23, 8))) \
    .withColumn("balance_out_rub", df["balance_out_rub"].cast(DecimalType(23, 8))) \
    .withColumn("balance_out_val", df["balance_out_val"].cast(DecimalType(23, 8))) \
    .withColumn("balance_out_total", df["balance_out_total"].cast(DecimalType(23, 8)))

    return df
    
    
def transform_rd_dfs(dfs):

    # deal_info
    dfs_deal_info = dfs['deal_info']
    dfs_deal_info = dfs_deal_info.dropDuplicates()
    dfs['deal_info'] = dfs_deal_info \
        .withColumn("deal_rk", F.col("deal_rk").cast(LongType())) \
        .withColumn("deal_num", F.col("deal_num").cast("string")) \
        .withColumn("deal_name", F.col("deal_name").cast("string")) \
        .withColumn("deal_sum", F.col("deal_sum").cast("double")) \
        .withColumn("client_rk", F.col("client_rk").cast(LongType())) \
        .withColumn("account_rk", F.col("account_rk").cast(LongType())) \
        .withColumn("agreement_rk", F.col("agreement_rk").cast(LongType())) \
        .withColumn("deal_start_date", F.to_date(F.col("deal_start_date"), "yyyy-MM-dd")) \
        .withColumn("department_rk", F.col("department_rk").cast(LongType())) \
        .withColumn("product_rk", F.col("product_rk").cast(LongType())) \
        .withColumn("deal_type_cd", F.col("deal_type_cd").cast("string")) \
        .withColumn("effective_from_date", F.to_date(F.col("effective_from_date"), "yyyy-MM-dd")) \
        .withColumn("effective_to_date", F.to_date(F.col("effective_to_date"), "yyyy-MM-dd"))
    
    # product
    dfs_product = dfs['product']
    dfs_product = dfs_product.dropDuplicates()
    dfs['product'] = dfs_product \
        .withColumn("product_rk", F.col("product_rk").cast(LongType())) \
        .withColumn("product_name", F.col("product_name").cast("string")) \
        .withColumn("effective_from_date", F.to_date(F.col("effective_from_date"), "yyyy-MM-dd")) \
        .withColumn("effective_to_date", F.to_date(F.col("effective_to_date"), "yyyy-MM-dd"))
    
    return dfs