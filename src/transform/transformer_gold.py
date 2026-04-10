import pandas as pd

from settings.config import KAGGLE_SCHEMA
from src.transform.transformer_base import TransformerBase

class TransformerGold(TransformerBase):

    """
    Responsável pela transformação final dos dados para a camada gold.

    Consolida dados das entidades:
    - Identificação de compostos
    - Toxicidade

    O resultado é um dataset único, estruturado e pronto para consumo externo.
    """

    def __init__(self, id_file_path: str, tox_file_path: str) -> None:
        
        """
        Inicializa o transformer gold.

        Args:
            id_file_path (str): Caminho dos dados de identificação (silver)
            tox_file_path (str): Caminho dos dados de toxicidade (silver)
        """

        self.id_file_path = id_file_path
        self.tox_file_path = tox_file_path

    def _get_data(self, file_path: str):

        """
        Realiza a leitura de arquivos no formato parquet.

        Args:
            file_path (str): Caminho do arquivo.

        Returns:
            pd.DataFrame: DataFrame carregado.
        """

        dataframe = pd.read_parquet(file_path)

        return dataframe
    
    def kaggle_transformation(self):

        """
        Constrói o dataset final para publicação no Kaggle.

        Etapas:
        1. Seleciona e renomeia colunas de identificação
        2. Seleciona e renomeia colunas de toxicidade
        3. Realiza join entre as duas entidades via 'cid'

        Estrutura final inclui:
        - Informações químicas (nome, SMILES, InChI, etc.)
        - Informações toxicológicas (organismo, dose, unidade, etc.)

        Returns:
            pd.DataFrame: Dataset final consolidado.
        """

        # Etapa 1: Identificação
        id_data = self._get_data(self.id_file_path)[
            [
                'cid',
                'cmpdname',
                'iupacname',
                'inchi_standardized',
                'inchikey_standardized',
                'smiles_standardized'
            ]
        ]

        # Padroniza nomes de colunas para consumo externo
        id_data.rename(columns={
            'cmpdname':'name',
            'iupacname':'iupac_name',
            'inchi_standardized':'inchi',
            'inchikey_standardized':'inchi_key',
            'smiles_standardized':'smiles'
        }, inplace=True)

        # Etapa 2: Toxicidade
        tox_data = self._get_data(self.tox_file_path)[
            [
                'cid',
                'organism',
                'testtype',
                'route',
                'operator',
                'standard_value',
                'standard_unit',
                'analyte',
                'complement'
            ]
        ]
        
        # Padroniza nomes de colunas para consumo externo
        tox_data.rename(columns={
            'testtype':'test_type',
            'standard_value':'dose_value',
            'standard_unit':'unit'
        }, inplace=True)

        # Etapa 3: Consolidação
        # Realiza join entre identificação e toxicidade via CID
        kaggle_dataframe = pd.merge(id_data, tox_data, on='cid', how='inner')
        kaggle_dataframe.drop_duplicates(inplace=True)
        kaggle_dataframe.sort_values(by='cid', inplace=True)

        # Etapa 4: Schema final explícito
        kaggle_dataframe = self._convert_types(kaggle_dataframe, KAGGLE_SCHEMA)

        return kaggle_dataframe