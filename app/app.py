import time
import os
import pandas as pd
from extract import baixar_pasta_google_drive, listar_arquivos_csv
from transformation import aplicar_transformacoes
from load import salvar_para_parquet

def ler_csv_para_dataframe(caminho_do_arquivo):
    """
    Lê um arquivo CSV e o converte em um DataFrame do pandas.
    """
    return pd.read_csv(caminho_do_arquivo)

def processar_arquivos_novos(diretorio, arquivos_ja_processados):
    """
    Identifica novos arquivos CSV no diretório, aplica transformações e salva como Parquet na mesma pasta.
    
    Args:
    - diretorio: Diretório onde os arquivos CSV estão armazenados.
    - arquivos_ja_processados: Conjunto de arquivos já processados para evitar repetição.
    
    Returns:
    - Conjunto atualizado de arquivos já processados.
    """
    arquivos_csv_atuais = listar_arquivos_csv(diretorio)
    arquivos_novos = [arq for arq in arquivos_csv_atuais if arq not in arquivos_ja_processados]

    for arquivo_csv in arquivos_novos:
        df = ler_csv_para_dataframe(arquivo_csv)
        df_transformado = aplicar_transformacoes(df)
        nome_arquivo_parquet = os.path.splitext(os.path.basename(arquivo_csv))[0] + '.parquet'
        salvar_para_parquet(df_transformado, nome_arquivo_parquet, diretorio)
        arquivos_ja_processados.add(arquivo_csv)
        print(f"Processado e salvo: {os.path.join(diretorio, nome_arquivo_parquet)}")

    return arquivos_ja_processados

if __name__ == "__main__":
    url_pasta = 'https://drive.google.com/drive/folders/1maqV7E3NRlHp12CsI4dvrCFYwYi7BAAf'
    diretorio_local = './pasta_gdown'

    arquivos_ja_processados = set()

    while True:
        print("Verificando novos arquivos...")
        baixar_pasta_google_drive(url_pasta, diretorio_local)
        arquivos_ja_processados = processar_arquivos_novos(diretorio_local, arquivos_ja_processados)

        print("Espera de 30 segundos antes da próxima verificação...")
        time.sleep(30)  # Espera por 30 segundos antes de verificar novamente
