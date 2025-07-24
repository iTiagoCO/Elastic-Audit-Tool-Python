# src/client.py
import requests
from requests.auth import HTTPBasicAuth
from rich.console import Console
from rich.text import Text
from rich.panel import Panel

console = Console()

class ElasticsearchClient:
    """Cliente para interactuar con la API de Elasticsearch."""

    def __init__(self, host: str, user: str = None, password: str = None, verify_ssl: bool = True):
        self.base_url = host
        self.auth = HTTPBasicAuth(user, password) if user and password else None
        self.verify = verify_ssl
        self.cluster_info = self._get_cluster_info()

        if not self.cluster_info:
            console.print(f"[bold red]Error: No se pudo conectar a Elasticsearch en {self.base_url}.[/bold red]")
            console.print("[yellow]Por favor, verifica la URL, las credenciales y la conectividad de red.[/yellow]")

    def _get_cluster_info(self):
        """Obtiene información básica del clúster para verificar la conexión."""
        try:
            # Llamamos al endpoint raíz para verificar la conexión
            response = self.get("")
            if response:
                return {
                    "name": response.get("cluster_name"),
                    "version": response.get("version", {}).get("number")
                }
            return None
        except Exception as e:
            console.print(f"[bold red]Excepción al conectar: {e}[/bold red]")
            return None

    def get(self, endpoint: str, params: dict = None):
        """
        Realiza una petición GET a un endpoint de Elasticsearch, manejando correctamente
        la construcción de la URL y los errores.
        """
        # Asegurarnos de que el endpoint no tenga una barra inicial si ya está en base_url
        if endpoint.startswith('/'):
            endpoint = endpoint[1:]
            
        url = f"{self.base_url}/{endpoint}"
        
        try:
            response = requests.get(url, auth=self.auth, params=params, verify=self.verify, timeout=30)
            response.raise_for_status()
            
            # Manejar correctamente la respuesta de texto plano para hot_threads
            if 'text/plain' in response.headers.get('content-type', ''):
                return response.text
            return response.json()
        except requests.exceptions.HTTPError as errh:
            # CORRECCIÓN: Escapar el mensaje de error para evitar el MarkupError de rich
            error_text = Text(errh.response.text)
            console.print(f"[red]Error HTTP: {errh.response.status_code}[/red] - Respuesta del servidor:")
            console.print(Panel(error_text, border_style="yellow"))
        except requests.exceptions.ConnectionError as errc:
            console.print(f"[red]Error de Conexión: No se pudo establecer conexión con {url}.[/red]")
        except requests.exceptions.Timeout as errt:
            console.print(f"[red]Error de Timeout: La petición a {url} tardó demasiado en responder.[/red]")
        except requests.exceptions.RequestException as err:
            console.print(f"[red]Error inesperado en la petición: {err}[/red]")
        return None

    # --- Métodos de API específicos ---

    def get_nodes_stats(self):
        return self.get("_nodes/stats/jvm,fs,os,process,thread_pool")

    def get_cluster_stats(self):
        return self.get("_cluster/stats")

    def get_cluster_health(self):
        return self.get("_cluster/health")

    def get_cat_shards(self):
        # CORRECCIÓN: Usar el endpoint correcto `_cat` y pasar los parámetros de forma segura
        return self.get("_cat/shards", params={'format': 'json', 'bytes': 'b'})

    def get_cat_nodes(self):
        # CORRECCIÓN: Usar el endpoint correcto `_cat` y pasar los parámetros de forma segura
        params = {
            'format': 'json',
            'h': 'name,ip,heap.percent,heap.current,heap.max,ram.percent,ram.current,ram.max,cpu,load_1m,load_5m,load_15m,node.role,master'
        }
        return self.get("_cat/nodes", params=params)

    def get_cat_indices(self):
        # CORRECCIÓN: Usar el endpoint correcto `_cat` y pasar los parámetros de forma segura
        params = {
            'format': 'json',
            'bytes': 'b',
            'h': 'index,health,status,pri,rep,docs.count,store.size,indexing.index_total,search.query_total'
        }
        return self.get("_cat/indices", params=params)

    def get_cluster_settings(self):
        return self.get("_cluster/settings", params={'include_defaults': 'true'})
        
    def get_index_templates(self):
        return self.get("_index_template")

    def get_tasks(self, actions='*search*'):
        return self.get("_tasks", params={'actions': actions, 'detailed': 'true'})

    def get_mapping_for_index(self, index_name):
        return self.get(f"{index_name}/_mapping")

    def get_hot_threads(self):
        """Obtiene el análisis de hot threads como texto plano."""
        return self.get("_nodes/hot_threads", params={'threads': 10, 'type': 'cpu'})