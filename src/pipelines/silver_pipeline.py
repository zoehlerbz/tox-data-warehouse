from src.transform.transformer_silver_identification import TransformerSilverIdentification
from src.transform.transformer_silver_toxicity import TransformerSilverToxicity
from src.load.load_silver import LoadSilver

class SilverPipeline:

    """
    Pipeline responsável pelo processamento da camada silver do Data Warehouse.

    Nesta camada, os dados brutos (bronze) passam por transformações como:
    - Limpeza e padronização
    - Tratamento de valores inconsistentes
    - Estruturação dos dados para análise

    O resultado são dados mais confiáveis e organizados, prontos para consumo
    em análises ou posterior modelagem na camada gold.
    """

    def __init__(self, version: str) -> None:

        """
        Inicializa o pipeline silver.

        Args:
            version (str): Versão da carga de dados (controle de versionamento/partição).
        """

        self.version = version

    def run(self, id_path: str, tox_path: str):

        """
        Executa o pipeline da camada silver.

        Fluxo:
        1. Carrega e transforma dados de identificação (bronze → silver)
        2. Carrega e transforma dados de toxicidade (bronze → silver)
        3. Armazena os dados transformados

        Args:
            id_path (str): Caminho do arquivo de identificação na camada bronze.
            tox_path (str): Caminho do arquivo de toxicidade na camada bronze.

        Returns:
            tuple:
                - str: Caminho do arquivo de identificação (silver)
                - str: Caminho do arquivo de toxicidade (silver)
        """

        # Validação dos caminhos de entrada da camada bronze
        # Garante que os arquivos necessários para transformação existem e foram informados corretamente
        if not isinstance(id_path, str) or not id_path.strip():
            # Levanta erro caso o caminho de identificação não seja fornecido
            raise ValueError("Parameter 'id_path' must be a non-empty string")

        if not isinstance(tox_path, str) or not tox_path.strip():
            # Levanta erro caso o caminho de toxicidade não seja fornecido
            raise ValueError("Parameter 'tox_path' must be a non-empty string")
        
        # Etapa 1: Transformação - Identificação
        id_transformer = TransformerSilverIdentification(id_path)
        id_data = id_transformer.transform_data()

        # Etapa 2: Transformação - Toxicidade
        tox_transformer = TransformerSilverToxicity(tox_path)
        tox_data = tox_transformer.transform_data()

        # Etapa 3: Carga - Identificação
        id_loader = LoadSilver(
            entity='id', 
            version=self.version
        )
        id_loader.load_silver(id_data)

        # Etapa 4: Carga - Toxicidade
        tox_loader = LoadSilver(
            entity='tox', 
            version=self.version
        )
        tox_loader.load_silver(tox_data)

        return id_loader.get_file_path(), tox_loader.get_file_path()