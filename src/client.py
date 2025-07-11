# src/client.py
import requests
import logging
from rich.console import Console
from .config import HEADERS

console = Console()

class ElasticsearchClient:
    """Gestiona la conexión y las peticiones a la API de Elasticsearch."""
    def __init__(self, host, user, password, verify_ssl=False):
        self.base_url = host
        self.auth = (user, password) if user else None
        self.verify_ssl = verify_ssl
        self.cluster_info = self._check_connection()

    def _check_connection(self):
        if not self.base_url:
            logging.error("La variable de entorno ES_HOST no está configurada.")
            console.print("[bold red]❌ Error: La variable de entorno ES_HOST no está configurada.[/bold red]")
            return None
        try:
            info = self.get("/")
            if info:
                logging.info(f"Conectado a Elasticsearch. Cluster: {info.get('cluster_name')}, Versión: {info.get('version', {}).get('number')}")
                console.print(f"[bold green]✔ Conectado a Elasticsearch[/bold green] | Cluster: [cyan]{info.get('cluster_name')}[/cyan] | Versión: [cyan]{info.get('version', {}).get('number')}[/cyan]")
                return info
            return None
        except Exception as e:
            logging.error(f"Error de Conexión: {e}", exc_info=True)
            console.rule("[bold red]Error de Conexión")
            console.print(f"[bold red]❌ No se pudo conectar a Elasticsearch:[/bold red] {e}")
            return None

    def get(self, path, params=None):
        url = f"{self.base_url}/{path}"
        try:
            response = requests.get(url, auth=self.auth, verify=self.verify_ssl, headers=HEADERS, params=params)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logging.warning(f"Fallo en petición GET a {url}: {e}")
            return None