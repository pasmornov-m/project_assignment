from pyspark.sql import SparkSession
from config import SPARK_APP_NAME, SPARK_MASTER


def create_spark_session(app_name=SPARK_APP_NAME):
    return SparkSession.builder \
    .appName(app_name) \
    .master(SPARK_MASTER) \
    .config("spark.jars", "/opt/spark/spark_jars/postgresql-42.7.5.jar") \
    .getOrCreate()

    
