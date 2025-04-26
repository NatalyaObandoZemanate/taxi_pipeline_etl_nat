from pyspark.sql import SparkSession
from pyspark.sql.functions import hour, count, avg
from pyspark.sql.functions import col

def create_spark_session(app_name="NYC Taxi Data - Refined Layer"):
    return SparkSession.builder.appName(app_name).getOrCreate()

def compute_kpis(df):
    demand_pattern = df.groupBy(hour(col("pickup_datetime")).alias("hour")) \
                       .agg(count("*").alias("total_trips")) \
                       .orderBy("hour")

    economic_efficiency = df.groupBy("zone") \
                            .agg(avg("fare_amount").alias("avg_fare"),
                                 avg("trip_distance").alias("avg_distance")) \
                            .orderBy(col("avg_fare").desc())

    return demand_pattern, economic_efficiency

def save_kpi_results(df_dict, output_path):
    for name, df in df_dict.items():
        df.write.mode("overwrite").parquet(f"{output_path}/{name}.parquet")
