import pandas as pd

def test_data_loading():
    file_path = "data/raw/sample_data.parquet"
    df = pd.read_parquet(file_path)
    
    assert not df.empty, "Error: El DataFrame está vacío."
    print(" Prueba de carga de datos con Pandas completada.")
