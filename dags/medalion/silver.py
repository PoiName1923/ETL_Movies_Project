import sys
import os

# Thêm đường dẫn gốc của dags vào PYTHONPATH
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from pyspark.sql.functions import col, to_date, year, month
from operations.sparkprocess import *
from pyspark.sql.functions import explode
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType, FloatType, ArrayType

schema = StructType([
    StructField("movie_results", ArrayType(StructType([
        StructField("adult", BooleanType(), True),
        StructField("backdrop_path", StringType(), True),
        StructField("id", IntegerType(), True),
        StructField("title", StringType(), True),
        StructField("original_language", StringType(), True),
        StructField("original_title", StringType(), True),
        StructField("overview", StringType(), True),
        StructField("poster_path", StringType(), True),
        StructField("media_type", StringType(), True),
        StructField("genre_ids", ArrayType(IntegerType(), True), True),
        StructField("popularity", FloatType(), True),
        StructField("release_date", StringType(), True),
        StructField("video", BooleanType(), True),
        StructField("vote_average", FloatType(), True),
        StructField("vote_count", IntegerType(), True)
    ]), True)),
    StructField("person_results", ArrayType(StructType([]), True), True),
    StructField("tv_results", ArrayType(StructType([]), True), True),
    StructField("tv_episode_results", ArrayType(StructType([]), True), True),
    StructField("tv_season_results", ArrayType(StructType([]), True), True)
])

def silver_process():
    with spark_session(master="spark://spark-master:7077",
                        appName="Silver Process",
                        config={
                            "spark.hadoop.fs.defaultFS":"hdfs://namenode:8020"
                            }
                        ) as spark:
        
        spark_op = Spark_Operation(spark=spark)

        df =spark_op.read_hdfs(path="hdfs://namenode:8020/movies_data/bronze_layer",
                            format="parquet",schema=schema)
        
        
        """ Xử lý dữ liệu nested"""
        df = df.withColumn("movie", explode("movie_results")).select("movie.*")

        """Loại bỏ các giá trị trùng lặp"""
        df = df.dropDuplicates()

        """Chuẩn hoá lại tên cột"""
        for col_name in df.columns:
            df = df.withColumnRenamed(col_name, col_name.strip().lower().replace(" ", "_"))

        """Xoá các cột có giá trị null"""
        df = df.dropna(subset=["title", "release_date"])

        """Điền các vị trí null"""
        for col_name, dtype in df.dtypes:
            if dtype == 'string':
                df = df.fillna({col_name: ""})
            elif dtype in ('float', 'double'):
                df = df.fillna({col_name: 0.0})
            elif dtype in ('int', 'bigint'):
                df = df.fillna({col_name: 0})

        """Chuẩn hoá ngày"""
        df = df.withColumn("release_date", to_date(col("release_date"), "yyyy-MM-dd"))

        # Ghi xuống HDFS Silver Layer theo partition
        spark_op.write_hdfs(
            df=df,
            path="hdfs://namenode:8020/movies_data/silver_layer",
            format="parquet",
            mode="overwrite",
            partitionBy=["release_year", "release_month"]
        )

if __name__ == "__main__":
    print("================== Sliver Process ==================")
    silver_process()
    print("================== Sliver Process ==================")

