import requests
from urllib3.util import Retry
from requests.adapters import HTTPAdapter

class ScraperBase:

    """
    Classe base para operações de web scraping.

    Fornece:
    - Gerenciamento de sessão HTTP reutilizável
    - Configuração de retries automáticos para falhas transitórias
    - Definição de headers padrão (User-Agent)
    - Suporte a context manager (with statement)

    Essa classe deve ser estendida por scrapers específicos que implementam
    a lógica de coleta de dados.
    """
    
    def __init__(self, timeout=10) -> None:

        """
        Inicializa o scraper base.

        Args:
            timeout (int, opcional): Tempo máximo (em segundos) para requisições HTTP.
        """

        self.session = None
        self.timeout = timeout

        # Header padrão para simular navegador real e evitar bloqueios básicos
        self.headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            )
        }

    def _create_session(self):

        """
        Cria e configura uma sessão HTTP com política de retry.

        Configura:
        - Tentativas automáticas para erros transitórios
        - Backoff exponencial entre tentativas
        - Suporte apenas a método GET (foco em scraping)

        Returns:
            requests.Session: Sessão configurada para uso nas requisições.
        """

        session = requests.Session()
        retry = Retry(
            total=5,
            backoff_factor=2,
            status_forcelist=[408, 429, 500, 502, 503, 504],
            allowed_methods=frozenset(["GET"]),
            respect_retry_after_header=True
        )
        adapter = HTTPAdapter(max_retries=retry)

        # Aplica a estratégia de retry para HTTP e HTTPS
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        return session

    def get(self, url, params=None):

        """
        Realiza uma requisição HTTP GET com retry automático.

        Caso a sessão ainda não exista, ela é criada automaticamente.

        Args:
            url (str): URL da requisição.
            params (dict, opcional): Parâmetros de query string.

        Returns:
            requests.Response: Objeto de resposta da requisição.

        Raises:
            RuntimeError: Caso a requisição falhe após as tentativas de retry.
        """

        if self.session is None:
            self.session = self._create_session()
        try:
            response = self.session.get(
                url=url,
                params=params,
                headers=self.headers,
                timeout=self.timeout
            )
            response.raise_for_status()
            return response
        except requests.RequestException as exc:
            raise RuntimeError(f"Request failed for {url}: {exc}") from exc
        
    def __enter__(self):

        """
        Inicializa a sessão ao entrar no contexto (with statement).

        Returns:
            ScraperBase: Instância do scraper com sessão ativa.
        """

        if self.session is None:
            self.session = self._create_session()
        return self
    
    def __exit__(self, exc_type, exc, tb):

        """
        Encerra a sessão HTTP ao sair do contexto.

        Garante liberação de recursos de rede independentemente de erro.

        Args:
            exc_type: Tipo da exceção (se houver)
            exc: Instância da exceção (se houver)
            tb: Traceback da exceção (se houver)
        """

        if self.session:
            self.session.close()
            self.session = None