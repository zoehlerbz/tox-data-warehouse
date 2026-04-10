import pandas as pd
from pathlib import Path
from pandas import json_normalize

class TransformerBase:

    """
    Classe base para transformação de dados na camada silver.

    Fornece métodos utilitários reutilizáveis para:
    - Leitura de dados (JSON / Parquet)
    - Limpeza de dados (remoção de nulos e colunas)
    - Padronização de tipos
    - Tratamento de strings

    Deve ser estendida por transformers específicos que implementam
    regras de negócio para cada domínio (ex: identificação, toxicidade).
    """

    def __init__(self, file_path: str) -> None:

        """
        Inicializa o transformer base.

        Args:
            file_path (str): Caminho do arquivo de entrada (camada bronze).
        """

        self.file_path = file_path
        self.file_format = Path(file_path).suffix

    def _get_data(self):
        
        """
        Realiza a leitura do arquivo de entrada com base no formato.

        Suporta:
        - JSON
        - Parquet

        Returns:
            pd.DataFrame: DataFrame contendo os dados carregados.
        """
 
        if self.file_format == '.json':
            dataframe = pd.read_json(self.file_path)
        elif self.file_format == '.parquet':
            dataframe = pd.read_parquet(self.file_path)
        else:
            raise ValueError(f"Unsupported file format: {self.file_format}")
            
        return dataframe
    
    def _remove_null(self, dataframe: pd.DataFrame, columns: list):
        
        """
        Remove registros com valores nulos em colunas específicas.

        Args:
            dataframe (pd.DataFrame): DataFrame de entrada.
            columns (list): Lista de colunas obrigatórias.

        Returns:
            pd.DataFrame: DataFrame sem registros nulos nas colunas especificadas.
        """
 
        dataframe.dropna(subset=columns, inplace=True)

        return dataframe
    
    def _drop_columns(self, dataframe: pd.DataFrame, columns: list):

        """
        Remove colunas desnecessárias do DataFrame.

        Args:
            dataframe (pd.DataFrame): DataFrame de entrada.
            columns (list): Lista de colunas a serem removidas.

        Returns:
            pd.DataFrame: DataFrame sem as colunas especificadas.
        """

        dataframe.drop(columns=columns, inplace=True)

        return dataframe
    
    def _convert_types(self, dataframe: pd.DataFrame, schema: dict):

        """
        Converte os tipos das colunas com base em um schema definido.

        Regras:
        - Converte a coluna 'cid' explicitamente para numérico
        - Aplica o schema informado para padronização

        Args:
            dataframe (pd.DataFrame): DataFrame de entrada.
            schema (dict): Mapeamento de colunas e tipos esperados.

        Returns:
            pd.DataFrame: DataFrame com tipos padronizados.
        """

        # Conversão explícita para evitar inconsistências de tipo
        if 'cid' in dataframe.columns:
            dataframe['cid'] = pd.to_numeric(dataframe['cid'], errors='coerce')
    
        dataframe = dataframe.astype(schema)

        return dataframe
    
    def _trim_text(self, dataframe: pd.DataFrame):
        
        """
        Remove espaços em branco no início e fim de colunas do tipo string.

        Considera apenas colunas com dtype 'string', conforme definido
        previamente no schema de transformação.

        Args:
            dataframe (pd.DataFrame): DataFrame de entrada.

        Returns:
            pd.DataFrame: DataFrame com valores de texto normalizados.
        """

        for col in dataframe.columns:
            if dataframe[col].dtype == 'string':  # tipo definido em .config X_SCHEMA
                dataframe[col] = dataframe[col].str.strip()

        return dataframe