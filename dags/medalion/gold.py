from operations.sparkprocess import *

def gold_process():
    with spark_session(master="spark://spark-master:7077",
                    appName="Gold Process",
                    jars=["org.mongodb.spark:mongo-spark-connector_2.12:10.4.1"],
                    config={
                        "spark.hadoop.fs.defaultFS","hdfs://namenode:9870"
                        }
                    ) as spark:
        
        spark_op = Spark_Operation(spark=spark)
        df = spark_op.read_hdfs(path="hdfs://namenode:8020/movies_data/silver_layer",
                        format="parquet")
        spark_op.write_to_postgres(df=df,
                                   table="movies_data")
        
if __name__ == "__main__":
    print("================== Gold Process ==================")
    gold_process()
    print("================== Gold Process ==================")