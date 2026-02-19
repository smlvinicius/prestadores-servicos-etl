import pandas as pd
from pathlib import Path

#Caminhos

raw_path = Path("data/raw/PRESTADORES_SERVICOS_2012_2017.csv")
bronze_path = Path("data/bronze/PRESTADORES_SERVICOS.parquet")

#Leitura do arquivo CSV
def main():
  print(f"Carregando dados do arquivo: {raw_path}")

  df = pd.read_csv(
    raw_path,
    sep=",", #pode ser necessário ajustar o separador dependendo do formato do CSV
    encoding="latin-1", 
    low_memory=False 
  )

  print("Colunas originais:")
  print(df.columns)

  #Padronizar nomes das colunas
  df.columns = (
    df.columns
    .str.strip() # Remove espaços em branco
    .str.lower() # Converte para minúsculas
    .str.replace(" ", "_") # Substitui espaços por underscores
    .str.replace("-", "_") # Substitui hífens por underscores)
  )

  print("Salvando na camada bronze...")
  bronze_path.parent.mkdir(parents=True, exist_ok=True) # Cria diretório se não existir
  df.to_parquet(bronze_path, index=False)

  print("Bronze criada com sucesso!")

if __name__ == "__main__":
  main()