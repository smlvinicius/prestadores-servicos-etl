import pandas as pd
from pathlib import Path

#Caminhos

bronze_path = Path("data/bronze/PRESTADORES_SERVICOS.parquet")
silver_path = Path("data/silver/PRESTADORES_SERVICOS.parquet")

def main():
  print(f"Carregando dados da camada bronze...")
  df = pd.read_parquet(bronze_path)

  print("Tratando os dados na camada Silver...")
  
  
 

