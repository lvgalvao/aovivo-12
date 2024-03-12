import os
import pandas as pd
import gdown
import time

def baixar_pasta_google_drive(url_pasta, diretorio_local):
    os.makedirs(diretorio_local, exist_ok=True)
    gdown.download_folder(url_pasta, output=diretorio_local, quiet=False, use_cookies=False)

def listar_arquivos_csv(diretorio):
    arquivos_csv = []
    for raiz, dirs, arquivos in os.walk(diretorio):
        for arquivo in arquivos:
            if arquivo.endswith('.csv'):
                arquivos_csv.append(os.path.join(raiz, arquivo))
    return arquivos_csv

def ler_csv_para_dataframe(caminho_do_arquivo):
    return pd.read_csv(caminho_do_arquivo)

def processar_arquivos_novos(diretorio, arquivos_ja_processados):
    arquivos_csv_atuais = listar_arquivos_csv(diretorio)
    arquivos_novos = [arq for arq in arquivos_csv_atuais if arq not in arquivos_ja_processados]
    dataframes_novos = []

    for arquivo_csv in arquivos_novos:
        df = ler_csv_para_dataframe(arquivo_csv)
        dataframes_novos.append(df)
        print(f"DataFrame criado para novo arquivo {arquivo_csv}:")
        print(df.head())
        arquivos_ja_processados.add(arquivo_csv)

    return dataframes_novos, arquivos_ja_processados

if __name__ == "__main__":
    url_pasta = 'https://drive.google.com/drive/folders/1maqV7E3NRlHp12CsI4dvrCFYwYi7BAAf'
    diretorio_local = './pasta_gdown'
    arquivos_ja_processados = set()

    while True:
        print("Verificando novos arquivos...")
        baixar_pasta_google_drive(url_pasta, diretorio_local)
        dataframes_novos, arquivos_ja_processados = processar_arquivos_novos(diretorio_local, arquivos_ja_processados)
        
        if not dataframes_novos:
            print("Nenhum arquivo novo encontrado.")
        else:
            print("Existe arquivo novo")# Aqui você pode adicionar lógica adicional para processar os novos dataframes
            
        time.sleep(30)  # Espera por 30 segundos antes de verificar novamente
