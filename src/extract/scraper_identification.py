import json
import urllib.parse
import requests

from src.extract.scraper_base import ScraperBase
from settings.config import CLASSIFICATION_BASE_URL, ACUTE_EFFECTS_BASE_URL
from src.utils import jsonl_to_json

class ScraperIdentification(ScraperBase):

    """
    Responsável por realizar o scraping de identificadores de compostos químicos
    a partir da fonte PubChem (Acute Effects).

    ├── PubChem
    │   └── Classification Browser
    │      └── Toxicity
    │         └── Toxicological Information
    │            └── Acute Effects

    Esta classe:
    - Obtém uma chave de cache necessária para consulta dos dados
    - Realiza a requisição dos compostos químicos
    - Retorna os dados em formato estruturado (JSON)

    Herda de ScraperBase para reutilização de métodos HTTP e controle de sessão.
    """

    def _get_cache_key(self):
        
        """
        Obtém a chave de cache (cache_key) necessária para realizar consultas
        na API de classificação de compostos.

        Essa chave é utilizada como parâmetro na busca de compostos relacionados
        a efeitos agudos (Acute Effects) no PubChem.

        Returns:
            str: Chave de cache utilizada nas requisições subsequentes.

        Raises:
            RuntimeError: Caso ocorra falha na requisição HTTP ou parsing da resposta.
        """        

        params = {
            "hid": "72",
            "hnid": "1857234",
            "cache_uid_type": "Compound",
            "alias": "PubChem Compound TOC: Acute Effects",
            "format": "json"
        }

        try:
            response = self.get(
                url=CLASSIFICATION_BASE_URL, 
                params=params, 
            )

            response.raise_for_status()
            data = response.json()
            key = data['Hierarchies']['CacheKey']

            return key

        except requests.RequestException as exc:
            raise RuntimeError(f"Failed to retrieve cache key: {exc}") from exc
        except Exception as exc:
            raise RuntimeError(f"Failed to execute scraping script (_get_cache_key): {exc}") from exc
        
    def get_compounds_id(self):
        
        """
        Recupera os identificadores e informações básicas dos compostos químicos
        associados a efeitos agudos no PubChem.

        O processo consiste em:
        1. Obter a cache_key via API de classificação
        2. Montar a query de busca com os campos desejados
        3. Realizar a requisição dos dados
        4. Converter o retorno de JSONL para JSON estruturado

        Campos retornados incluem:
        - cid (Compound ID)
        - cmpdname (Nome do composto)
        - iupacname
        - inchikey
        - inchi
        - smiles

        Returns:
            list[dict]: Lista de compostos com seus respectivos metadados.

        Raises:
            RuntimeError: Caso ocorra falha na requisição ou processamento dos dados.
        """
        
        cache_key = self._get_cache_key()

        query = json.dumps({
            "download": "cid,cmpdname,iupacname,inchikey,inchi,smiles",
            "collection":"compound",
            "order":["cid,asc"],
            "start":1,
            "limit":150000,
            "where":{
                "ands":[{
                    "input":{
                        "type":"netcachekey",
                        "idtype":"cid",
                        "key":cache_key
                    }
                }]
            }
        })

        url = ACUTE_EFFECTS_BASE_URL + urllib.parse.quote(query)

        try:
            response = self.get(url=url)
            response.raise_for_status()
            
            data = jsonl_to_json(response.text)

            return data
        
        except requests.RequestException as exc:
            raise RuntimeError(f"Failed to retrieve acute effects CIDs: {exc}") from exc
        except Exception as exc:
            raise RuntimeError(f"Failed to execute scraping script (get_compounds_id): {exc}") from exc