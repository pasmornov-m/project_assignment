import chardet
import pandas as pd


def detect_encoding(file_path, n_bytes=10000):
    with open(file_path, 'rb') as f:
        rawdata = f.read(n_bytes)
    result = chardet.detect(rawdata)
    return result['encoding']

def clean_ds_dfs(dfs):

    # ft_balance_f
    dfs['ft_balance_f'] = dfs['ft_balance_f'].drop_duplicates()
    dfs['ft_balance_f']['ON_DATE'] = pd.to_datetime(dfs['ft_balance_f']['ON_DATE'], format='%d.%m.%Y')

    # ft_posting_f
    dfs['ft_posting_f'] = dfs['ft_posting_f'].drop_duplicates()
    dfs['ft_posting_f']['OPER_DATE'] = pd.to_datetime(dfs['ft_posting_f']['OPER_DATE'], format='%d-%m-%Y')

    # md_account_d
    dfs['md_account_d'] = dfs['md_account_d'].drop_duplicates()
    dfs['md_account_d']['DATA_ACTUAL_DATE'] = pd.to_datetime(dfs['md_account_d']['DATA_ACTUAL_DATE'], format='%Y-%m-%d')
    dfs['md_account_d']['DATA_ACTUAL_END_DATE'] = pd.to_datetime(dfs['md_account_d']['DATA_ACTUAL_END_DATE'], format='%Y-%m-%d')

    # md_currency_d
    dfs['md_currency_d'] = dfs['md_currency_d'].drop_duplicates()
    dfs['md_currency_d']['DATA_ACTUAL_DATE'] = pd.to_datetime(dfs['md_currency_d']['DATA_ACTUAL_DATE'], format='%Y-%m-%d')
    dfs['md_currency_d']['DATA_ACTUAL_END_DATE'] = pd.to_datetime(dfs['md_currency_d']['DATA_ACTUAL_END_DATE'], format='%Y-%m-%d')
    dfs['md_currency_d']['CURRENCY_CODE'] = pd.to_numeric(dfs['md_currency_d']['CURRENCY_CODE'], errors='coerce')
    dfs['md_currency_d']['CURRENCY_CODE'] = dfs['md_currency_d']['CURRENCY_CODE'].fillna(-1).astype(int).astype(str)

    # md_exchange_rate_d
    dfs['md_exchange_rate_d'] = dfs['md_exchange_rate_d'].drop_duplicates()
    dfs['md_exchange_rate_d']['DATA_ACTUAL_DATE'] = pd.to_datetime(dfs['md_exchange_rate_d']['DATA_ACTUAL_DATE'], format='%Y-%m-%d')
    dfs['md_exchange_rate_d']['DATA_ACTUAL_END_DATE'] = pd.to_datetime(dfs['md_exchange_rate_d']['DATA_ACTUAL_END_DATE'], format='%Y-%m-%d')
    dfs['md_exchange_rate_d']['CODE_ISO_NUM'] = dfs['md_exchange_rate_d']['CODE_ISO_NUM'].astype(str)

    # md_ledger_account_s
    dfs['md_ledger_account_s'] = dfs['md_ledger_account_s'].drop_duplicates()
    dfs['md_ledger_account_s']['START_DATE'] = pd.to_datetime(dfs['md_ledger_account_s']['START_DATE'], format='%Y-%m-%d')
    dfs['md_ledger_account_s']['END_DATE'] = pd.to_datetime(dfs['md_ledger_account_s']['END_DATE'], format='%Y-%m-%d')

    return dfs