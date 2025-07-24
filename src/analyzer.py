# src/analyzer.py
import pandas as pd
import os
import glob
from datetime import datetime
from rich.console import Console

from .client import ElasticsearchClient

console = Console()

class ClusterAnalyzer:
    """Analiza el estado del clúster recolectando y procesando métricas."""

    def __init__(self, client: ElasticsearchClient):
        self.client = client
        self.cluster_name = client.cluster_info.get("name", "unknown_cluster")
        # Directorio de snapshots específico para este clúster
        self.snapshots_dir = os.path.join("snapshots", self.cluster_name)
        os.makedirs(self.snapshots_dir, exist_ok=True)

        # DataFrames actuales
        self.nodes_df = pd.DataFrame()
        self.shards_df = pd.DataFrame()
        self.indices_df = pd.DataFrame()
        self.cluster_health = {}
        self.cluster_stats = {}
        self.node_stats_raw = {}
        self.top_heap_indices = pd.DataFrame()

        # DataFrames de la recolección anterior para calcular deltas
        self.previous_nodes_df = pd.DataFrame()
        self.previous_indices_df = pd.DataFrame()
        self.previous_node_stats_raw = {}
        self.last_fetch_time = None
        self.previous_fetch_time = None

    def fetch_all_data(self, for_deep_dive=False):
        """
        Método centralizado para recolectar todos los datos, guardando el estado
        anterior para calcular deltas y tasas.
        """
        # Guardar estado previo
        self.previous_nodes_df = self.nodes_df.copy()
        self.previous_indices_df = self.indices_df.copy()
        self.previous_node_stats_raw = self.node_stats_raw.copy()
        self.previous_fetch_time = self.last_fetch_time

        # Recolectar nuevos datos
        with console.status("[bold cyan]Recolectando datos del clúster...[/bold cyan]"):
            nodes_stats_raw = self.client.get_nodes_stats()
            cat_nodes = self.client.get_cat_nodes()
            cat_indices = self.client.get_cat_indices()
            cat_shards = self.client.get_cat_shards()
            self.cluster_health = self.client.get_cluster_health()
            self.cluster_stats = self.client.get_cluster_stats()
            self.node_stats_raw = nodes_stats_raw

            if not all([nodes_stats_raw, cat_nodes, cat_indices, cat_shards]):
                console.print("[red]Fallo al recolectar datos esenciales. Algunas vistas pueden no funcionar.[/red]")
                return False

            # Procesar nodos
            nodes_data = []
            if 'nodes' in nodes_stats_raw:
                for node_id, stats in nodes_stats_raw["nodes"].items():
                    gc_info = stats.get("jvm", {}).get("gc", {})
                    collectors = gc_info.get("collectors", {})
                    total_gc_time = sum(c.get("collection_time_in_millis", 0) for c in collectors.values())
                    total_gc_count = sum(c.get("collection_count", 0) for c in collectors.values())
                    rejections_write = stats.get("thread_pool", {}).get("write", {}).get("rejected", 0)
                    rejections_search = stats.get("thread_pool", {}).get("search", {}).get("rejected", 0)

                    node_entry = {
                        "node_id": node_id,
                        "node_name": stats.get("name"),
                        "cpu_percent": stats.get("os", {}).get("cpu", {}).get("percent", 0),
                        "load_1m": stats.get("os", {}).get("cpu", {}).get("load_average", {}).get("1m", 0.0),
                        "heap_percent": stats.get("jvm", {}).get("mem", {}).get("heap_used_percent", 0.0),
                        "heap_old_gen_percent": stats.get("jvm", {}).get("mem", {}).get("pools", {}).get("old", {}).get("used_percent", 0.0),
                        "gc_count": total_gc_count,
                        "gc_time_ms": total_gc_time,
                        "rejections": rejections_write + rejections_search,
                        "breakers_tripped": sum(b.get('tripped', 0) for b in stats.get('breakers', {}).values())
                    }
                    nodes_data.append(node_entry)
            self.nodes_df = pd.DataFrame(nodes_data)

            # Enriquecer con roles de `_cat/nodes`
            if cat_nodes and not self.nodes_df.empty:
                cat_nodes_df = pd.DataFrame(cat_nodes)
                self.nodes_df = pd.merge(self.nodes_df, cat_nodes_df[['name', 'node.role']], left_on='node_name', right_on='name', how='left')
                
                # CORRECCIÓN: Separar `rename` y `drop` en dos operaciones distintas.
                self.nodes_df.rename(columns={'node.role': 'tier'}, inplace=True)
                self.nodes_df.drop(columns=['name'], inplace=True)
                
                self.nodes_df['tier'] = self.nodes_df['tier'].fillna('undefined')


            # Procesar índices y shards
            self.shards_df = pd.DataFrame(cat_shards)
            self.shards_df['store'] = pd.to_numeric(self.shards_df['store'], errors='coerce').fillna(0) / 1024**2 # a MB
            self.indices_df = pd.DataFrame(cat_indices)
            for col in ['docs.count', 'store.size', 'indexing.index_total', 'search.query_total']:
                self.indices_df[col] = pd.to_numeric(self.indices_df[col], errors='coerce').fillna(0)

            self.last_fetch_time = datetime.now()
            # Añadir la hora de la captura como un atributo para cálculos de delta
            self.nodes_df.attrs['fetch_time'] = self.last_fetch_time
            self.indices_df.attrs['fetch_time'] = self.last_fetch_time

            self.save_snapshot()
            return True

    def save_snapshot(self):
        """Guarda un snapshot de las métricas de nodos en un archivo CSV en la carpeta del clúster."""
        if not self.nodes_df.empty:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            file_path = os.path.join(self.snapshots_dir, f"nodes_snapshot_{timestamp}.csv")
            snapshot_df = self.nodes_df[['node_name', 'cpu_percent', 'heap_percent', 'heap_old_gen_percent', 'load_1m', 'gc_count', 'gc_time_ms']]
            snapshot_df.to_csv(file_path, index=False)

    def load_historical_snapshots(self, window_hours: int = 24) -> pd.DataFrame:
        """Carga y consolida snapshots del clúster actual para análisis histórico."""
        now = datetime.now()
        historical_data = []
        # CORRECCIÓN: Usar `os.path.join` para compatibilidad entre sistemas operativos.
        snapshot_files = sorted(glob.glob(os.path.join(self.snapshots_dir, "nodes_snapshot_*.csv")))

        for f in snapshot_files:
            try:
                timestamp_str = os.path.basename(f).replace("nodes_snapshot_", "").replace(".csv", "")
                file_time = datetime.strptime(timestamp_str, "%Y%m%d_%H%M%S")
                if (now - file_time).total_seconds() / 3600 <= window_hours:
                    df = pd.read_csv(f)
                    df['timestamp'] = file_time
                    historical_data.append(df)
            except (ValueError, FileNotFoundError):
                continue
        
        if not historical_data:
            return pd.DataFrame()
        
        return pd.concat(historical_data, ignore_index=True)