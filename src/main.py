import os
import logging
import time
from pyspark.sql import SparkSession
from src.raw_layer.raw_layer import create_spark_session, download_parquet_by_url, load_parquet_to_spark, save_raw_data
from src.trusted_layer.trusted_layer import clean_and_enrich_data, save_trusted_data
from src.refined_layer.refined_layer import compute_kpis, save_kpi_results

os.environ["HADOOP_HOME"] = "C:/Users/Natalya Obando/Desktop/taxi_pipeline_etl/hadoop"
os.environ["PATH"] += os.pathsep + os.path.join(os.environ["HADOOP_HOME"], "bin")

os.environ["SPARK_HOME"] = "C:/Python311/Lib/site-packages/pyspark"
os.environ["JAVA_HOME"] = "C:/Program Files/Java/jdk-21"
os.environ["PYSPARK_PYTHON"] = "C:/Python311/python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = "C:/Python311/python.exe"
os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"

spark = SparkSession.builder \
    .appName("Taxi Pipeline") \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.memory", "4g") \
    .config("spark.network.timeout", "600s") \
    .config("spark.executor.heartbeatInterval", "200s") \
    .getOrCreate()

spark = SparkSession.builder.appName("TaxiPipeline").getOrCreate()
file_path = "data/raw/sample_data.parquet"
df = spark.read.parquet(file_path)
df.show(5) 

def create_spark_session(app_name="NYC Taxi Data Pipeline"):
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.parquet.binaryAsString", "true") \
        .config("spark.hadoop.fs.permissions.umask-mode", "022") \
        .config("spark.hadoop.fs.permissions.mask-mode", "000") \
        .getOrCreate()


logging.basicConfig(filename='execution.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def main():
    start_time = time.time()
    logging.info("Iniciando ejecución del pipeline...")

    YEAR = 2022
    MONTHS = list(range(1, 13)) 
    LOCAL_DIR = "./data/raw"
    RAW_OUTPUT = "./output/raw_data"
    TRUSTED_OUTPUT = "./output/trusted_data"
    REFINED_OUTPUT = "./output/refined_data"
    LOOKUP_PATH = "./data/taxi_zone_lookup.csv"

    try:
        logging.info("Iniciando sesión de Spark...")
        spark = create_spark_session()

        logging.info("📥 Descargando archivos Parquet desde la web pública...")
        download_parquet_by_url(YEAR, MONTHS, LOCAL_DIR)

        logging.info("📂 Cargando archivos Parquet en Spark...")
        df_raw = load_parquet_to_spark(spark, LOCAL_DIR)
        logging.info(f"Registros cargados en Raw Layer: {df_raw.count()}")

        logging.info("💾 Guardando datos RAW...")
        save_raw_data(df_raw, RAW_OUTPUT)

        logging.info("🛠 Procesando capa Trusted...")
        df_trusted = clean_and_enrich_data(df_raw, LOOKUP_PATH)
        logging.info(f"Registros después de limpieza en Trusted Layer: {df_trusted.count()}")
        save_trusted_data(df_trusted, TRUSTED_OUTPUT)

        logging.info("📊 Calculando KPIs en capa Refined...")
        kpis = {
            "demand_pattern": compute_kpis(df_trusted)[0],
            "economic_efficiency": compute_kpis(df_trusted)[1]
        }
        save_kpi_results(kpis, REFINED_OUTPUT)

        end_time = time.time()
        total_time = round(end_time - start_time, 2)
        logging.info(f"🚀 Proceso completado en {total_time} segundos.")

        with open("execution_report.txt", "w") as report:
            report.write(f"Registros en Raw Layer: {df_raw.count()}\n")
            report.write(f"Registros en Trusted Layer: {df_trusted.count()}\n")
            report.write(f"Tiempo total de ejecución: {total_time} segundos\n")
            report.write("Ejecución exitosa.")

    except Exception as e:
        logging.error(f"❌ ERROR en la ejecución: {e}")

if __name__ == "__main__":
    main()
