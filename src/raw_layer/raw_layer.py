import os
import logging
import urllib.request
from pyspark.sql import SparkSession

def create_spark_session(app_name="NYC Taxi Data - Raw Layer"):
    return SparkSession.builder.appName(app_name).getOrCreate()

def download_parquet_by_url(year: int, months: list, local_dir: str):
    base_url = "https://d37ci6vzurychx.cloudfront.net/trip-data"
    os.makedirs(local_dir, exist_ok=True)

    for month in months:
        file_name = f"yellow_tripdata_{year}-{month:02d}.parquet"
        file_url = f"{base_url}/{file_name}"
        local_path = os.path.join(local_dir, file_name)

        if not os.path.exists(local_path):
            try:
                logging.info(f"📥 Descargando: {file_name}")
                urllib.request.urlretrieve(file_url, local_path)
            except Exception as e:
                logging.error(f"Error al descargar {file_name}: {e}")
        else:
            logging.info(f"{file_name} ya existe. Se omite descarga.")

def load_parquet_to_spark(spark, local_dir: str):
    files = [os.path.join(local_dir, f) for f in os.listdir(local_dir) if f.endswith(".parquet")]
    if not files:
        raise FileNotFoundError("NO se encontraron archivos Parquet en el directorio.")

    df = spark.read.parquet(*files)
    logging.info(f"✅ Datos cargados con {df.count()} registros.")
    return df

def save_raw_data(df, output_path: str):
    df.write.mode("overwrite").parquet(output_path)
    logging.info(f"💾 Datos guardados en: {output_path}")
