import sys
import os

# Thêm đường dẫn gốc của dags vào PYTHONPATH
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))


from datetime import datetime

from gettingdata.getting_movies_daily import *
from gettingdata.getting_name_daily import *

from operations.sparkprocess import *

def bronze_process():

    cur_date = datetime.now().strftime("%d-%m-%Y")
    
    with spark_session(master="spark://spark-master:7077",
                    appName="Bronze Process",
                    config={
                        "spark.hadoop.fs.defaultFS":"hdfs://namenode:8020",
                        "spark.sql.execution.arrow.pyspark.enabled": "true",
                        "spark.jars":"/opt/airflow/dags/jars/mongo-spark-connector_2.12-10.4.1.jar"
                        }
                    ) as spark:
        
        spark_op = Spark_Operation(spark=spark)

        # Lấy dữ liệu mới vào ngày hôm nay
        loading_movies_name(crawl_date=cur_date)
        new_movies_df = loading_movies_data(crawl_date=cur_date)

        new_movies_spark_df = spark.createDataFrame(new_movies_df)

        # Thêm dữ liệu vào lớp Bronze
        spark_op.write_hdfs(df=new_movies_spark_df,
                            path="hdfs://namenode:8020/movies_data/bronze_layer",
                            format="parquet",
                            mode="overwrite")
        
if __name__ == "__main__":
    print("================== Bronze Process ==================")
    bronze_process()
    print("================== Bronze Process ==================")

