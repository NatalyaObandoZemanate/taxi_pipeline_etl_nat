import pandas as pd
import os

def test_multiple_files():
    files = ["data/raw/sample_data_01.parquet", "data/raw/sample_data_02.parquet", "data/raw/sample_data_03.parquet"]
    
    for file in files:
        assert os.path.exists(file), f"Error: {file} no existe."
        df = pd.read_parquet(file)
        assert not df.empty, f"Error: {file} está vacío."
    
    print("Prueba de integración con múltiples archivos completada.")
