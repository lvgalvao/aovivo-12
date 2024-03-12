import os

def salvar_para_parquet(df, nome_arquivo, diretorio_destino):
    """
    Salva o DataFrame em um arquivo Parquet.

    Parâmetros:
    - df (pandas.DataFrame): DataFrame a ser salvo.
    - nome_arquivo (str): Nome do arquivo de destino.
    - diretorio_destino (str): Diretório onde o arquivo Parquet será salvo.
    """
    caminho_completo = os.path.join(diretorio_destino, nome_arquivo)
    df.to_parquet(caminho_completo, index=False)
    print(f"DataFrame salvo com sucesso em: {caminho_completo}")
