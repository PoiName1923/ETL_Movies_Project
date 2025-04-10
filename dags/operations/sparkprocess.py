from pyspark.sql import SparkSession
from contextlib import contextmanager
import pandas as pd

# Create Session
@contextmanager
def spark_session(master: str, appName: str, jars: list = None, config: dict = None):
    spark = None
    try:
        builder = SparkSession.builder.master(master).appName(appName)
        if jars:
            builder = builder.config("spark.jars", ",".join(jars))
        if config:
            for key, value in config.items():
                builder = builder.config(key, value)

        print(f"SparkSession | {appName} | was created")
        spark = builder.getOrCreate()
        yield spark

    except Exception as e:
        raise TypeError(f"Error: {e}")
    
    finally:
        if spark:
            spark.stop()
            print("SparkSession stopped.")

class Spark_Operation:
    '''Initialize the Postgres operation class'''
    def __init__(self, spark):
        self.spark = spark

    '''Read data from HDFS'''
    def read_hdfs(self, path, format="parquet", **options):
        return self.spark.read.format(format).options(**options).load(path)
    
    '''Write data from HDFS'''
    def write_hdfs(self, df, path, format="parquet", mode="overwrite", **options):
        if isinstance(df, pd.DataFrame):
            df = self.spark.createDataFrame(df)
        df.write.format(format).mode(mode).options(**options).save(path)

    '''Write To Postgres'''
    def write_to_postgres(self, df, table, mode, \
                          postgres_user,postgres_pass,postgres_host, postgres_port, postgres_db):

        url = f"jdbc:postgresql://{postgres_host}:{postgres_port}/{postgres_db}"

        properties = {
            'user': postgres_user,
            'password': postgres_pass,
            'driver': "org.postgresql.Driver"
        }
        df.write.jdbc(url=url, table=table, mode=mode, properties=properties)