import sys
import os

# Thêm đường dẫn gốc của dags vào PYTHONPATH
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))


from operations.sparkprocess import *
from pyspark.sql.functions import explode

def gold_process():
    with spark_session(master="spark://spark-master:7077",
                    appName="Gold Process",
                    config={
                        "spark.hadoop.fs.defaultFS":"hdfs://namenode:8020",
                        "spark.jars":"/opt/airflow/dags/jars/postgresql-42.7.5.jar"
                        }
                    ) as spark:
        
        spark_op = Spark_Operation(spark=spark)
        df = spark_op.read_hdfs(path="hdfs://namenode:8020/movies_data/silver_layer",
                        format="parquet")
        
        movies_df = df.select(
            "id",
            "adult",
            "backdrop_path",
            "title",
            "original_language",
            "original_title",
            "overview",
            "poster_path",
            "media_type",
            "popularity",
            "release_date",
            "video",
            "vote_average",
            "vote_count"
        )
        spark_op.write_to_postgres(df=movies_df,
                                   table="movies",
                                   mode='append',
                                   postgres_user='ndtien_postgres',
                                   postgres_pass='ndtien_postgres',
                                   postgres_host='postgres',
                                   postgres_db='movies_db',
                                   postgres_port='5432')
        
if __name__ == "__main__":
    print("================== Gold Process ==================")
    gold_process()
    print("================== Gold Process ==================")