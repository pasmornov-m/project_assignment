import os
from utils.tools import detect_encoding


def read_sql_file(path):
    with open(path, 'r', encoding='utf-8') as file:
        return file.read()

def read_csv_to_spark(spark, path, files):
    dfs = {}
    for f in files:
        f_with_ext = f+".csv"
        full_path = os.path.join(path, f_with_ext)
        enc = detect_encoding(full_path)
        try:
            df = spark.read.option("encoding", enc).option("header", "true").option("delimiter", ";").csv(full_path)
            df = df.toDF(*[c.lower() for c in df.columns])
            dfs[f] = df
        except Exception as e:
            print(f"Error: {e}")
    return dfs