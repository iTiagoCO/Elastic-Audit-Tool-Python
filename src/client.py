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
            info = self.get("/", filter_path=["cluster_name", "version.number"])
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

    def get(self, path, params=None, filter_path=None):
        """
        Método GET refactorizado para aceptar filter_path y optimizar respuestas.
        """
        url = f"{self.base_url}/{path}"
        
        query_params = params.copy() if params else {}
        if filter_path:
            query_params['filter_path'] = ",".join(filter_path)
            
        try:
            response = requests.get(url, auth=self.auth, verify=self.verify_ssl, headers=HEADERS, params=query_params)
            response.raise_for_status()
            # Si la respuesta está vacía (posible con filter_path), devuelve un diccionario vacío
            if not response.text:
                return {}
            return response.json()
        except requests.exceptions.RequestException as e:
            logging.warning(f"Fallo en petición GET a {url}: {e}")
            return None
        except ValueError: # Captura errores de JSON si la respuesta no es un JSON válido
            logging.warning(f"Respuesta no es JSON válido desde {url}")
            return None