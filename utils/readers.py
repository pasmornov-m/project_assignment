import pandas as pd
import os
from utils.tools import detect_encoding


def read_sql_file(path):
    with open(path, 'r', encoding='utf-8') as file:
        return file.read()


def read_csv_to_pd(path, files):
    dfs = {}
    for f in files:
        f_with_ext = f+".csv"
        full_path = os.path.join(path, f_with_ext)
        enc = detect_encoding(full_path)
        try:
            dfs[f] = pd.read_csv(full_path, encoding=enc, sep=';')
        except Exception as e:
            print(f"Error: {e}")
    return dfs