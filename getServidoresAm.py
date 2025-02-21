import os
import requests
import pandas as pd
from tqdm import tqdm

# Criação da pasta de destino
base_dir = os.path.dirname(os.path.abspath(__file__))
dest_dir = os.path.join(base_dir, "downloads")
os.makedirs(dest_dir, exist_ok=True)

# Imputs Validação

while True:
    ano_consulta = input("Digite o ano da consulta (ex: 2024): ").strip()
    if ano_consulta.isdigit() and 2000 <= int(ano_consulta) <= 2100:
        break
    print("Erro: Digite um ano válido entre 2000 e 2100.")

while True:
    mes_consulta = input("Digite o mês da consulta (ex: 01 para Janeiro): ").strip()
    if mes_consulta.isdigit() and 1 <= int(mes_consulta) <= 12:
        mes_consulta = mes_consulta.zfill(2)
        break
    print("Erro: Digite um mês válido (01 a 12).")



# Loop para baixar os arquivos de 1 a 610
with tqdm(range(1, 601), desc="Baixando arquivos", unit="arquivo", leave=False) as pbar: #ajustar range conforme necessário
    for i in pbar:
        url = f"https://www.transparencia.am.gov.br/arquivos/{ano_consulta}/{i}_{ano_consulta}{mes_consulta}.csv"
        file_path = os.path.join(dest_dir, f"{i}_{mes_consulta}{ano_consulta}.csv")
        
        pbar.set_description(f"Baixando arquivo {i}/{len(pbar)}")
        
        try:
            response = requests.get(url, stream=True, timeout=10)
            response.raise_for_status()
            
            with open(file_path, "wb") as file:
                for chunk in response.iter_content(chunk_size=8192):
                    file.write(chunk)
        except requests.exceptions.RequestException as e:
            tqdm.write(f"Erro ao baixar {url}: {e}")
            #continue
print("Todos os downloads foram concluídos!")

# Listar arquivos CSV na pasta
csv_files = [file for file in os.listdir(dest_dir) if file.endswith('.csv')]

if not csv_files:
    print("Nenhum arquivo CSV encontrado! Verifique se os downloads foram concluídos.")
    exit()

# Juntar arquivos em um DataFrame único
frames = []
erros_csv = "erros_csv.txt"

for csv_file in tqdm(csv_files, desc="Consolidando Arquivos", unit="arquivo"):
    file_path = os.path.join(dest_dir, csv_file)
    
    try:
        df = pd.read_csv(file_path, sep=';', encoding='latin-1')  # Ajuste o encoding se necessário
        frames.append(df)
    except Exception as e:
        tqdm.write(f"⚠ Erro ao processar {csv_file}: {e}")
        with open(erros_csv, "a") as log:
            log.write(f"{csv_file} - {e}\n")  # Salva erro no log
            
            
# Salvar o arquivo consolidado
arquivo_final = f'AM_{mes_consulta}{ano_consulta}.csv'
resultado = pd.concat(frames, ignore_index=True)
resultado.to_csv(arquivo_final, index=False, sep=';', encoding='utf-8')

# **Só apagar os arquivos individuais se o arquivo final for criado com sucesso**
if os.path.exists(arquivo_final):
    print("Arquivo consolidado criado com sucesso! Removendo os arquivos individuais...")
    
    try:
        for csv_file in csv_files:
            file_path = os.path.join(dest_dir, csv_file)
            os.remove(file_path)
        print("Arquivos individuais removidos!")
    except Exception as e:
        print(f"Erro ao remover arquivos individuais: {e}")

else:
    print("Erro na consolidação! Arquivos individuais foram mantidos.")
