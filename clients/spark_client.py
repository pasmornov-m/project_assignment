from pyspark.sql import SparkSession
from config import SPARK_APP_NAME, SPARK_MASTER, SPARK_JARS_DIR


def create_spark_session(app_name=SPARK_APP_NAME):
    return SparkSession.builder \
    .appName(app_name) \
    .master(SPARK_MASTER) \
    .config("spark.jars", SPARK_JARS_DIR) \
    .getOrCreate()

    
