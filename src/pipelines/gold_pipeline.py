from src.transform.transformer_gold import TransformerGold
from src.load.load_gold import LoadGold
from src.load.load_kaggle import LoadKaggleDataset

class GoldPipeline:

    """
    Pipeline responsável pela camada gold do Data Warehouse.

    Nesta camada, os dados da silver são refinados e estruturados
    para consumo final, com foco em análise e distribuição externa.

    Neste pipeline:
    - Os dados são transformados para um formato adequado ao Kaggle
    - O dataset final é gerado e persistido
    - O dataset é publicado via API do Kaggle
    """

    def __init__(self, version):

        """
        Inicializa o pipeline gold.

        Args:
            version (str): Versão do dataset (controle de versionamento).
        """
 
        self.version = version

    def run(self, id_path, tox_path, extract_start, extract_end):

        """
        Executa o pipeline da camada gold.

        Fluxo:
        1. Transforma dados da camada silver em dataset final
        2. Armazena dataset no formato definido (CSV)
        3. Publica dataset no Kaggle

        Args:
            id_path (str): Caminho dos dados de identificação (silver)
            tox_path (str): Caminho dos dados de toxicidade (silver)
            extract_start (str): Data/hora de início da extração (metadado)
            extract_end (str): Data/hora de fim da extração (metadado)
        """

        # Validação dos caminhos de entrada da camada silver
        # Garante que os arquivos necessários existem e foram informados corretamente
        if not isinstance(id_path, str) or not id_path.strip():
            # Levanta erro caso o caminho de identificação não seja fornecido
            raise ValueError("Parameter 'id_path' must be a non-empty string")

        if not isinstance(tox_path, str) or not tox_path.strip():
            # Levanta erro caso o caminho de toxicidade não seja fornecido
            raise ValueError("Parameter 'tox_path' must be a non-empty string")
        
        # Etapa 1: Transformação (silver → gold)
        transformer = TransformerGold(id_path, tox_path)
        kaggle_df = transformer.kaggle_transformation()

        # Etapa 2: Persistência do dataset
        loader = LoadGold(
            entity='kaggle',
            version=self.version,
            subfolder_gold='kaggle',
            file_format='csv'
        )
        loader.load_gold(kaggle_df)

        # Etapa 3: Publicação no Kaggle
        kaggle_api = LoadKaggleDataset(
            loader.get_file_path(),
            self.version,
            extract_start,
            extract_end
        )
        kaggle_api.load_kaggle()