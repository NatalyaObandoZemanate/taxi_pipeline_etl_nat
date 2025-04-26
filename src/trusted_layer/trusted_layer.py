from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def create_spark_session(app_name="NYC Taxi Data - Trusted Layer"):
    return SparkSession.builder.appName(app_name).getOrCreate()

def clean_and_enrich_data(df, lookup_table_path):
    print("Verificando uso de col() dentro de la función:", col)  
    df = df.withColumnRenamed("tpep_pickup_datetime", "pickup_datetime") \
           .withColumnRenamed("tpep_dropoff_datetime", "dropoff_datetime")
    df = df.filter(col("pickup_datetime") < col("dropoff_datetime")) \
           .filter(col("trip_distance") > 0) \
           .filter(col("fare_amount") > 0)

    spark = SparkSession.builder.getOrCreate()
    taxi_zones = spark.read.option("header", "true").csv(lookup_table_path)
    df = df.join(taxi_zones, df.PULocationID == taxi_zones.LocationID, "left") \
           .drop("LocationID")

    return df

def save_trusted_data(df, output_path):
    df.write.mode("overwrite").parquet(output_path)
