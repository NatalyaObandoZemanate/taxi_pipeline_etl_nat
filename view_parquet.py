import os
from pyspark.sql import SparkSession

os.environ["HADOOP_HOME"] = "C:/Users/Natalya Obando/Desktop/taxi_pipeline_etl/hadoop"
os.environ["PATH"] += os.pathsep + os.path.join(os.environ["HADOOP_HOME"], "bin")

spark = SparkSession.builder.getOrCreate()

df = spark.read.parquet("./output/raw_data") 

df.show(10)
