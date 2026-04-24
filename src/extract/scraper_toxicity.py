import io
import time
import json
import urllib.parse
import requests

from src.extract.scraper_base import ScraperBase
from settings.config import ACUTE_EFFECTS_BASE_URL
from src.utils import jsonl_to_json

class ScraperToxicity(ScraperBase):

    """
    Responsável por realizar o scraping de dados de toxicidade (Acute Effects)
    de compostos químicos via PubChem/ChemIDplus.

    Para cada CID informado:
    - Realiza uma requisição à API
    - Converte os dados retornados (JSONL → JSON)
    - Acumula resultados válidos e erros separadamente

    Ideal para uso na camada bronze do Data Warehouse.
    """

    def get_compounds_tox(self, cids):

        """
        Recupera dados de toxicidade para uma lista de compostos (CID).

        O processo é feito de forma iterativa, com controle de taxa (rate limit)
        para evitar bloqueios pela API.

        Args:
            cids (list[int] | list[str]): Lista de identificadores de compostos.

        Returns:
            tuple:
                - list[dict]: Lista de erros ocorridos durante o scraping
                - list[dict]: Lista de dados de toxicidade obtidos com sucesso
        """

        toxicity_data = []   # Armazena dados válidos de toxicidade
        errors = []  # Armazena erros por CID

        for i, cid in enumerate(cids, start=1):

            # Pequeno delay para evitar rate limiting / bloqueio da API
            time.sleep(0.35)

            # Monta query para busca de dados completos de toxicidade
            query = json.dumps({
                "download": "*",
                "collection": "chemidplus",
                "start": 1,
                "limit": 5000,
                "where": {"ands": [{"cid": cid}]}
            })

            url = ACUTE_EFFECTS_BASE_URL + urllib.parse.quote(query)

            try:
                response = self.get(url=url)
                response.raise_for_status()

                # Converte resposta de JSONL para JSON estruturado
                data = jsonl_to_json(response.text)
                
                # Validação básica de retorno
                if not data:
                    raise ValueError(f"Empty response (data is None) for cid={cid}")

                # Acumula resultados válidos
                toxicity_data.extend(data)
                     
            except Exception as exc:
                # Registra erro sem interromper o processamento dos demais CIDs
                data = {
                    'cid': cid,
                    'url': url,
                    'erro': type(exc).__name__,
                    'msg': str(exc)
                }
                errors.append(data)

        return errors, toxicity_data