from src.extract.scraper_identification import ScraperIdentification
from src.extract.scraper_toxicity import ScraperToxicity
from src.load.load_bronze import LoadBronze

class BronzePipeline:

    """
    Pipeline responsável pela ingestão de dados na camada bronze do Data Warehouse.

    Este pipeline executa duas etapas principais:
    1. Coleta e armazenamento dos identificadores de compostos (CIDs)
    2. Coleta e armazenamento dos dados de toxicidade (Acute Effects)

    Os dados são extraídos de fontes externas (PubChem/ChemIDplus) e armazenados
    em formato bruto, preservando a estrutura original para processamento posterior
    nas camadas silver/gold.
    """
    
    def __init__(self, version: str, run_status: bool) -> None:

        """
        Inicializa o pipeline bronze.

        Args:
            version (str): Versão da carga de dados (controle de versionamento/partição).
            run_status (bool): Define se a execução é para teste. Se 'True', limita os dados em 20 cids.
        """

        self.version = version
        self.run_status = run_status

    def run(self):

        """
        Executa o pipeline completo da camada bronze.

        Fluxo:
        1. Extrai identificadores de compostos
        2. Armazena dados de identificação
        3. Extrai dados de toxicidade com base nos CIDs
        4. Armazena dados de toxicidade

        Returns:
            tuple:
                - str: Caminho do arquivo de identificação
                - str: Caminho do arquivo de toxicidade
        """

        # Etapa 1: Identificação
        with ScraperIdentification() as scraper:
            # Coleta dados de identificação dos compostos
            id_data = scraper.get_compounds_id()

            # Limita o número de dados em caso de teste
            if self.run_status:
                id_data = id_data[:20]

            # Armazena dados brutos de identificação
            id_loader = LoadBronze(
                entity='id', 
                version=self.version
            )
            id_loader.load_bronze(id_data)

        # Etapa 2: Toxicidade
        with ScraperToxicity() as scraper:
            # Obtém lista de CIDs a partir dos dados coletados anteriormente
            cids = [row['cid'] for row in id_data]

            # Coleta dados de toxicidade
            errors, tox_data = scraper.get_compounds_tox(cids)

            # Armazena dados brutos de toxicidade
            tox_loader = LoadBronze(
                entity='tox', 
                version=self.version
            )
            tox_loader.load_bronze(tox_data)  # toxicidade dos compostos
            tox_loader.load_errors(errors)  # erros de extração
        
        return id_loader.get_file_path(), tox_loader.get_file_path()