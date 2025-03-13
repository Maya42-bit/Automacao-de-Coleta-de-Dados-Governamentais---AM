import os
import shutil
import requests
import pandas as pd
from tqdm import tqdm

# Criar diretórios para organização
base_dir = os.path.dirname(os.path.abspath(__file__))
downloads_dir = os.path.join(base_dir, "downloads")
verificacao_dir = os.path.join(base_dir, "arq_estrutura_verificacao")

os.makedirs(downloads_dir, exist_ok=True)
os.makedirs(verificacao_dir, exist_ok=True)

# Entrada do usuário com validação
while True:
    ano_consulta = input("Digite o ano da consulta (ex: 2024): ").strip()
    if ano_consulta.isdigit() and 2000 <= int(ano_consulta) <= 2100:
        break
    print("❌ Erro: Digite um ano válido entre 2000 e 2100.")

while True:
    mes_consulta = input("Digite o mês da consulta (ex: 01 para Janeiro): ").strip()
    if mes_consulta.isdigit() and 1 <= int(mes_consulta) <= 12:
        mes_consulta = mes_consulta.zfill(2)
        break
    print("❌ Erro: Digite um mês válido (01 a 12).")

# Download dos arquivos
with tqdm(range(1, 601), desc="Baixando arquivos", unit="arquivo", leave=False) as pbar:
    for i in pbar:
        url = f"https://www.transparencia.am.gov.br/arquivos/{ano_consulta}/{i}_{ano_consulta}{mes_consulta}.csv"
        file_path = os.path.join(downloads_dir, f"{i}_{mes_consulta}{ano_consulta}.csv")

        pbar.set_description(f"Baixando arquivo {i}/{len(pbar)}")

        try:
            response = requests.get(url, stream=True, timeout=10)
            response.raise_for_status()

            with open(file_path, "wb") as file:
                for chunk in response.iter_content(chunk_size=8192):
                    file.write(chunk)

        except requests.exceptions.RequestException as e:
            tqdm.write(f"❌ Erro ao baixar ou Base  Não disponível em {url}: {e}")

print("✅ Todos os downloads foram concluídos!")

# Listar arquivos CSV na pasta downloads
csv_files = [file for file in os.listdir(downloads_dir) if file.endswith('.csv')]

if not csv_files:
    print("❌ Nenhum arquivo CSV encontrado! Verifique se os downloads foram concluídos.")
    exit()

# Definição do padrão de colunas
colunas_padrao = ['NOME','LOTACAO','CARGO','CLASSE / PADRÃO','FUNCAO','CARGA HR SEM','DT DE ADMISSAO','VINCULO','REMUNERACAO LEGAL TOTAL(R$)','DESC.TETO(R$)','REMUNERACAO LEGAL DEVIDA(R$)','DESCONTOS LEGAIS(R$)','LIQUIDO DISPONIVEL(R$)','']

# Criar um log para arquivos com estrutura errada
erros_csv = "erros_csv.txt"
arquivos_errados = []

# Verificação dos arquivos baixados
for csv_file in csv_files:
    file_path = os.path.join(downloads_dir, csv_file)

    try:
        # Tenta ler as primeiras linhas para verificar a estrutura
        df = pd.read_csv(file_path, sep=';', encoding='latin-1', nrows=5)

        # Se o número de colunas for menor que o esperado, mover para verificação
        if len(df.columns) < len(colunas_padrao)-1:  
            arquivos_errados.append(csv_file)
            shutil.move(file_path, os.path.join(verificacao_dir, csv_file))
            print(f"⚠ Arquivo movido para verificação manual: {csv_file}")

    except Exception as e:
        print(f"❌ Erro ao processar {csv_file}: {e} -> Etapa Verificação dos arquivos baixados")
        with open(erros_csv, "a") as log:
            log.write(f"{csv_file} - {e}-> Etapa Verificação dos arquivos baixados\n")

# Exibir resumo dos arquivos com problema
if arquivos_errados:
    print("\n🚨 Arquivos com estrutura suspeita foram movidos para 'arq_estrutura_verificacao/' 🚨")
    for arquivo in arquivos_errados:
        print(f"  → {arquivo}")
else:
    print("✅ Nenhum arquivo apresentou problemas de estrutura!")


# Juntar arquivos em um DataFrame único
frames = []
erros_csv = "erros_csv.txt"

for csv_file in tqdm(csv_files, desc="Consolidando Arquivos", unit="arquivo"):
    file_path = os.path.join(downloads_dir, csv_file)
    
    try:
        df = pd.read_csv(file_path, sep=';', encoding='latin-1')  # Ajuste o encoding se necessário
        frames.append(df)
    except Exception as e:
        tqdm.write(f"⚠ Erro ao processar {csv_file}: {e} -> Etapa Consolidação dos arquivos")
        with open(erros_csv, "a") as log:
            log.write(f"{csv_file} - {e}-> Etapa Consolidação dos arquivos\n")  # Salva erro no log
            
            
# Salvar o arquivo consolidado
arquivo_final = f'AM_{mes_consulta}{ano_consulta}.csv'
resultado = pd.concat(frames, ignore_index=True)
resultado.to_csv(arquivo_final, index=False, sep=';', encoding='utf-8')

# **Só apagar os arquivos individuais se o arquivo final for criado com sucesso**
if os.path.exists(arquivo_final):
    print("Arquivo consolidado criado com sucesso! Removendo os arquivos individuais...")
    
for csv_file in os.listdir(downloads_dir):  # Verifica a pasta no momento da remoção
    file_path = os.path.join(downloads_dir, csv_file)
    try:
        os.remove(file_path)
        print(f"✅ Arquivo removido: {file_path}")
    except Exception as e:
        print(f"❌ Erro ao remover {csv_file}: {e} -> Delete dos CSVs individuais")
        with open(erros_csv, "a") as log:
            log.write(f"{csv_file} - {e} -> Delete dos CSVs individuais\n")  # Salva erro no log
