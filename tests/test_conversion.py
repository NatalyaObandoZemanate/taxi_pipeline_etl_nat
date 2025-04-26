import pandas as pd

def convertir_parquet_a_csv(parquet_path, csv_path):
    df = pd.read_parquet(parquet_path)
    df.to_csv(csv_path, index=False)
    return df.shape[0]  

def test_conversion():
    filas = convertir_parquet_a_csv("test.parquet", "test.csv")
    assert filas > 0, "Error: La conversión no generó filas"

test_conversion()
print("✅ Prueba unitaria completada con éxito!")
