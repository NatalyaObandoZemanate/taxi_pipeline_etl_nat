import pandas as pd

df = pd.read_parquet("./output/raw_data")

df.to_csv("./output/raw_data.csv", index=False)

print("Conversión completada: Archivo guardado en ./output/raw_data.csv")
