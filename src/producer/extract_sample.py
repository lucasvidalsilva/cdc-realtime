import pandas as pd
import os

def download_sample():
    url = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2026-01.parquet"
    
    output_dir = "data"
    output_file = f"{output_dir}/yellow_tripdata_sample.parquet"

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    print(f"Baixando dados de: {url}...")
    
    try:
        df = pd.read_parquet(url)
        sample = df.head(5000)
        
        sample.to_parquet(output_file)
        print(f"Amostra salva com sucesso em: {output_file}")
        print(f"Colunas encontradas: {list(sample.columns)}")
        
    except Exception as e:
        print(f"Erro ao extrair: {e}")

if __name__ == "__main__":
    download_sample()