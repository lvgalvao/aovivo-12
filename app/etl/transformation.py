
def aplicar_transformacoes(df):
    """
    Aplica transformações em um DataFrame.
    
    Parâmetros:
    - df (pandas.DataFrame): DataFrame original com as colunas 'nome', 'salario', 'idade'.
    
    Retorna:
    - pandas.DataFrame: DataFrame transformado com colunas adicionais 'nome_upper' e 'salario x2'.
    """
    # Verifica se as colunas necessárias estão no DataFrame
    if not {'nome', 'salario', 'idade'}.issubset(df.columns):
        raise ValueError("DataFrame deve conter as colunas 'nome', 'salario' e 'idade'.")
    
    # Criação das novas colunas
    df['nome_upper'] = df['nome'].str.upper()
    df['salario x2'] = df['salario'] * 2
    
    return df
