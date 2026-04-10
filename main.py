from datetime import datetime

from src.pipelines.bronze_pipeline import BronzePipeline
from src.pipelines.silver_pipeline import SilverPipeline
from src.pipelines.gold_pipeline import GoldPipeline

from src.utils import newest_file, next_version
from settings.config import BRONZE_PATH

def main():

    """
    Função principal responsável por orquestrar o pipeline completo de dados.

    Fluxo:
    1. Define versão da execução com base nos arquivos existentes
    2. Executa ingestão (bronze - web scraping)
    3. Executa transformação (silver - limpeza e padronização)
    4. Executa consolidação e publicação (gold - dataset final)

    Também registra o intervalo de tempo da extração para controle
    e rastreabilidade dos dados.
    """

    # Etapa 0: Definição de versão
    # Obtém o arquivo mais recente da camada bronze
    file_str = newest_file(BRONZE_PATH, 'json')

    # Define próxima versão com base no histórico
    version = next_version(file_str) if file_str else '1_0_1'

    # Etapa 1: Extração (Bronze)
    # Marca início do processo de scraping
    extract_start = datetime.now()

    bronze = BronzePipeline(version)
    id_bronze, tox_bronze = bronze.run()

    # Marca fim da extração
    extract_end = datetime.now()

    # Etapa 2: Transformação (Silver)
    silver = SilverPipeline(version)
    id_silver, tox_silver = silver.run(id_bronze, tox_bronze)

    # Etapa 3: Consolidação (Gold)
    gold = GoldPipeline(version)
    gold.run(id_silver, tox_silver, extract_start, extract_end)

if __name__ == "__main__":
    main()