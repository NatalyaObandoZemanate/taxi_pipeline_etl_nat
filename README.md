# NYC Taxi Data Pipeline  
Este proyecto es un pipeline ETL (Extract, Transform, Load) para procesar datos históricos de viajes del taxi amarillo en Nueva York, usando PySpark y un patrón de arquitectura de medallón (capas Raw, Trusted y Refined) y  calculando KPIs clave. 

# Objetivo
procesar, limpiar y estructurar los datos de viajes en taxi de NYC para que puedan ser analizados.

# Tecnologías Utilizadas
 PySpark: Procesamiento distribuido de datos  
 Pandas: Conversión de datos  
 Hadoop: Gestión de almacenamiento en formato Parquet  
 Python: Orquestación del pipeline  

# Formato de Datos
 Entrada: Archivos Parquet con datos de viajes en taxi  
 Salida: Archivos CSV con datos limpios y KPIs  

# Estructura del Proyecto  
📂 taxi_pipeline_etl  
 ├── 📂 src  
 │   ├── 🗂 raw_layer.py - Carga datos Parquet  
 │   ├── 🗂 trusted_layer.py - Limpieza y validación  
 │   ├── 🗂 refined_layer.py - Cálculo de KPIs  
 │   ├── 🗂 main.py - Orquestador del ETL  
 ├── 📂 data  
 │   ├── 📄 taxi_zone_lookup.csv - Información de zonas  
 │   ├── 📄 raw_parquet_files/ - Datos originales  
 ├── 📂 output    https://drive.google.com/drive/folders/1S71piY6dPTA2D1sFERIjDGXx6wFBLAFk?usp=drive_link
 │   ├── 🗂 raw_data/ - Parquet inicial  
 │   ├── 🗂 trusted_data/ - Datos limpios  
 │   ├── 🗂 refined_data/ - KPIs finales  
 │   ├── 📄 raw_data.csv - Conversión a CSV  
 ├── 📄 README.md - Documentación  

# Descripción de Pipeline
Este pipeline procesa datos de NYC Taxi en tres capas principales:  
- Raw Layer: Carga archivos Parquet y almacena datos originales.  
- Trusted Layer: Limpieza y enriquecimiento con tabla de zonas de taxi.  
- Refined Layer: Cálculo de KPIs sobre demanda y eficiencia económica.

# ¿Cómo Ejecutar el Pipeline?

# Requisito Inicial
- Python 3.11+
- PySpark instalado (`pip install pyspark`)
- `winutils.exe` y `hadoop.dll` configurados correctamente 

# Configuración
Antes de ejecutar, definir estas variables:  
```powershell
setx SPARK_HOME "C:\Python311\Lib\site-packages\pyspark"
setx JAVA_HOME "C:\Program Files\Java\jdk-21"
setx PYSPARK_PYTHON "C:\Python311\python.exe"
setx PYSPARK_DRIVER_PYTHON "C:\Python311\python.exe" 

# Ejecutar Pipeline
python -m src.main (Esto procesará los datos y generará los archivos en la carpeta output/)

# Descripción Técnica**
#main.py
Este archivo orquesta el pipeline, asegurando que cada capa se ejecute correctamente.  

#trusted_layer.py
- Renombra columnas para estandarizar nombres (`tpep_pickup_datetime` → `pickup_datetime`)  
- Aplica filtros para eliminar registros inválidos (`trip_distance > 0`, `fare_amount > 0`)  
- Une datos con la tabla `taxi_zone_lookup.csv` para enriquecer la información  

Ejemplo de código:  
```python
df = df.filter(col("pickup_datetime") < col("dropoff_datetime")) \
       .filter(col("trip_distance") > 0) \
       .filter(col("fare_amount") > 0)

#refined_layer.py
Calcula KPIs clave para evaluar la demanda y eficiencia económica de los taxis.

#KPIs Calculados
Patrón de demanda:Número de viajes agrupados por hora  
Eficiencia económica:Comparación entre tarifas y distancia recorrida  
Tiempo promedio de viaje:Duración entre `pickup_datetime` y `dropoff_datetime`  
Distribución de tarifas:Rango de precios de los viajes en distintos horarios  

Ejemplo de análisis de eficiencia económica:  
```python
df.withColumn("revenue_per_km", col("fare_amount") / col("trip_distance"))

Ejemplo:

trip_distance | fare_amount | revenue_per_km  
--------------|------------|----------------  
3.5          | 12.5       | 3.57  
5.0          | 20.0       | 4.00  


#Solución de Problemas Comunes
# Problema: `winutils.exe` no encontrado  
Verificar que `winutils.exe` este en `hadoop/bin/` 
Problema: Error NativeIO$Windows.access0
Solución: Asegurar que hadoop.dll está presente y configurar HADOOP_HOME

#Observabilidad y Logging  
La ejecución del pipeline, se generan automáticamente archivos de registro (`logs`) y reportes de ejecución.  
#execution.log
Este archivo registra eventos importantes del pipeline, como:  
Inicio y finalización de cada etapa (`Raw Layer`, `Trusted Layer`, `Refined Layer`).  
Cantidad de registros procesados en cada capa.  
Errores detectados durante la ejecución.  

Ejemplo:
2025-04-25 21:39:14,945 - INFO - Iniciando ejecuci�n del pipeline...
2025-04-25 21:39:14,945 - INFO - Iniciando sesi�n de Spark...
2025-04-25 21:39:21,484 - INFO - \U0001f4e5 Descargando archivos Parquet desde la web p�blica...
2025-04-25 21:39:21,486 - INFO - yellow_tripdata_2022-01.parquet ya existe. Se omite descarga.
2025-04-25 21:39:21,487 - INFO - yellow_tripdata_2022-02.parquet ya existe. Se omite descarga.
2025-04-25 21:39:21,488 - INFO - yellow_tripdata_2022-03.parquet ya existe. Se omite descarga.
2025-04-25 21:39:21,488 - INFO - yellow_tripdata_2022-04.parquet ya existe. Se omite descarga.

#Pruebas
#Estructura
taxi_pipeline_etl/
│── data/
│   ├── raw/   
│── tests/
│   ├── test_parquet.py  - Prueba unitaria de lectura de Parquet
│   ├── test_conversion.py  - Prueba unitaria de conversión a CSV
│   ├── test_all_months.py - Prueba de integración 


# Prueba Unitaria
tests/test_parquet.py
#Ejecutar la prueba 
python tests/test_parquet.py

Las pruebas unitarias nos ayudan a asegurarnos de que cada parte funcione bien por separado antes de juntarlas en el pipeline, evitando problemas.

# Prueba de integracion
tests/test_all_months.py
#Ejecutar la prueba
python tests/test_all_months.py

Con las pruebas de integración nos aseguramos de que todos funcione bien en conjunto


#   t a x i _ p i p e l i n e _ e t l _ n a t 
 
 
