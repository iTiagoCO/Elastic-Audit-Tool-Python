# elastic_pro_audit_tool.py
# ----------------------------------------
# Herramienta de diagnóstico y auditoría de nivel experto para clústeres Elasticsearch.
# Versión con Análisis Estadístico, Predictivo y de Causa Raíz.
# Autor: Santiago Poveda + Asistente Gemini
# Requiere: requests, rich, pandas, python-dotenv

import requests
import os
import re
import time
import logging
import fnmatch
import argparse
from datetime import datetime, timedelta
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.columns import Columns
from rich.rule import Rule
from rich.live import Live
from rich.layout import Layout
from rich.prompt import Prompt
from rich.markdown import Markdown
from rich.text import Text
import pandas as pd
from dotenv import load_dotenv
import urllib3

# --- Configuración Inicial ---
load_dotenv()
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename='audit_debug.log',
    filemode='w'
)

ES_HOST = os.getenv("ES_HOST")
ES_USER = os.getenv("ES_USER")
ES_PASS = os.getenv("ES_PASS")
VERIFY_SSL = False
HEADERS = {'Content-Type': 'application/json'}
REFRESH_INTERVAL = 5
SNAPSHOT_DIR = "snapshots"
SNAPSHOT_INTERVAL_S = 300
SNAPSHOT_RETENTION_DAYS = 7

# --- Umbrales ---
HEAP_USAGE_THRESHOLD = 85
HEAP_OLD_GEN_THRESHOLD = 75
CPU_USAGE_THRESHOLD = 90
GC_COUNT_SPIKE_THRESHOLD = 2
GC_TIME_SPIKE_THRESHOLD = 500
GC_TIME_THRESHOLD = 200 
REJECTIONS_THRESHOLD = 0
SHARD_SIZE_GB_TARGET = 30
SHARD_SKEW_WARN_THRESHOLD = 60
DUSTY_SHARD_MB_THRESHOLD = 50
LONG_RUNNING_TASK_MINUTES = 5
HIGH_SHARD_COUNT_TEMPLATE_THRESHOLD = 5

console = Console(record=True)

# --- Clase para la Conexión con Elasticsearch ---
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

# --- Clase Principal de Análisis del Clúster ---
class ClusterAnalyzer:
    """Orquesta la recolección, análisis y visualización de datos del clúster."""
    def __init__(self, client: ElasticsearchClient):
        self.client = client
        self.nodes_df = pd.DataFrame()
        self.indices_df = pd.DataFrame()
        self.shards_df = pd.DataFrame()
        self.previous_nodes_df = pd.DataFrame()
        self.previous_indices_df = pd.DataFrame()
        self.node_stats_raw = {}
        self.previous_node_stats_raw = {}
        self.cluster_stats = {}
        self.cluster_health = {}
        self.pending_tasks = {}
        self.last_fetch_time = None
        self.last_snapshot_time = 0
        self.top_heap_indices = pd.DataFrame()

    def _manage_snapshots(self, current_time):
        os.makedirs(SNAPSHOT_DIR, exist_ok=True)
        if (current_time - self.last_snapshot_time) > SNAPSHOT_INTERVAL_S:
            timestamp_str = int(current_time)
            if not self.nodes_df.empty:
                self.nodes_df.to_json(f"{SNAPSHOT_DIR}/nodes_{timestamp_str}.json", orient='split')
            if not self.indices_df.empty:
                self.indices_df.to_json(f"{SNAPSHOT_DIR}/indices_{timestamp_str}.json", orient='split')
            self.last_snapshot_time = current_time
            logging.info(f"Snapshot guardado en t={timestamp_str}")

        retention_limit = current_time - (SNAPSHOT_RETENTION_DAYS * 24 * 60 * 60)
        for filename in os.listdir(SNAPSHOT_DIR):
            try:
                timestamp = int(re.search(r'_(\d+)\.json', filename).group(1))
                if timestamp < retention_limit:
                    os.remove(os.path.join(SNAPSHOT_DIR, filename))
                    logging.info(f"Snapshot antiguo purgado: {filename}")
            except (AttributeError, ValueError):
                continue

    def fetch_all_data(self, for_deep_dive=False):
        current_time = time.time()
        self.last_fetch_time = current_time

        if not self.nodes_df.empty:
            self.previous_nodes_df = self.nodes_df.copy()
        if not self.indices_df.empty:
            self.previous_indices_df = self.indices_df.copy()
        if self.node_stats_raw:
            self.previous_node_stats_raw = self.node_stats_raw.copy()

        self.node_stats_raw = self.client.get("_nodes/stats/jvm,fs,os,process,thread_pool,transport,breaker") or {}
        nodes_info = self.client.get("_nodes/_all/info/name,roles,attributes") or {}
        
        if not for_deep_dive:
            index_stats_raw = self.client.get("_stats/indexing,search,segments,query_cache,fielddata") or {}
            cat_indices_raw = self.client.get("_cat/indices?format=json&bytes=mb&h=health,status,index,uuid,pri,rep,docs.count,store.size") or []
            self.shards_df = pd.DataFrame(self.client.get("_cat/shards?format=json&bytes=mb&h=index,shard,prirep,state,docs,store,ip,node") or [])
            self.cluster_stats = self.client.get("_cluster/stats") or {}
            self.cluster_health = self.client.get("_cluster/health") or {}
            self.pending_tasks = self.client.get("_cluster/pending_tasks") or {}
            
            cat_df = pd.DataFrame([i for i in cat_indices_raw if i.get('status') == 'open'])
            stats_list = []
            if 'indices' in index_stats_raw:
                for index_name, stats in index_stats_raw.get('indices', {}).items():
                    stats_list.append({
                        'index': index_name,
                        'indexing_total': stats.get('total', {}).get('indexing', {}).get('index_total', 0),
                        'search_total': stats.get('total', {}).get('search', {}).get('query_total', 0),
                        'segments_count': stats.get('total', {}).get('segments', {}).get('count', 0),
                        'memory_segments_mb': stats.get('total', {}).get('segments', {}).get('memory_in_bytes', 0) / 1e6,
                        'memory_cache_mb': stats.get('total', {}).get('query_cache', {}).get('memory_size_in_bytes', 0) / 1e6,
                        'memory_fielddata_mb': stats.get('total', {}).get('fielddata', {}).get('memory_size_in_bytes', 0) / 1e6,
                    })
            stats_df = pd.DataFrame(stats_list)

            if not cat_df.empty and not stats_df.empty:
                self.indices_df = pd.merge(cat_df, stats_df, on='index', how='inner')
                self.indices_df['heap_usage_mb'] = self.indices_df['memory_segments_mb'] + self.indices_df['memory_cache_mb'] + self.indices_df['memory_fielddata_mb']
                self.top_heap_indices = self.indices_df.sort_values('heap_usage_mb', ascending=False).head(5)
            else:
                self.indices_df = pd.DataFrame()
                self.top_heap_indices = pd.DataFrame()

        node_list = []
        if 'nodes' in self.node_stats_raw:
            for node_id, data in self.node_stats_raw.get('nodes', {}).items():
                jvm_mem = data.get('jvm', {}).get('mem', {})
                old_gen = jvm_mem.get('pools', {}).get('old', {})
                heap_old_gen_percent = (old_gen.get('used_in_bytes', 0) / old_gen.get('max_in_bytes', 1) * 100)
                gc_info = data.get('jvm', {}).get('gc', {}).get('collectors', {}).get('old', {})
                
                node_info = nodes_info.get('nodes', {}).get(node_id, {})
                node_attributes = node_info.get('attributes', {})
                tier = next((v for k, v in node_attributes.items() if 'tier' in k), 'undefined')
                
                # Sumar rechazos de todos los pools
                rejections = sum(pool.get('rejected', 0) for pool in data.get('thread_pool', {}).values())

                node_list.append({
                    'node_id': node_id,
                    'node_name': data.get('name', 'N/A'),
                    'tier': tier,
                    'cpu_percent': data.get('os', {}).get('cpu', {}).get('percent', 0),
                    'heap_percent': jvm_mem.get('heap_used_percent', 0),
                    'heap_old_gen_percent': heap_old_gen_percent,
                    'gc_count': gc_info.get('collection_count', 0),
                    'gc_time_ms': gc_info.get('collection_time_in_millis', 0),
                    'breakers_tripped': sum(b.get('tripped', 0) for b in data.get('breaker', {}).values()),
                    'rejections': rejections
                })
        self.nodes_df = pd.DataFrame(node_list)
        
        if not for_deep_dive:
            self._manage_snapshots(current_time)

    def _format_metric(self, current_val, prev_val, spike_threshold, higher_is_worse=True):
        spike_icon = ""
        prev_val_for_delta = prev_val if pd.notna(prev_val) else current_val
        delta = current_val - prev_val_for_delta

        if abs(delta) > spike_threshold:
            spike_icon = "🔥"

        if pd.isna(prev_val) or current_val == prev_val:
            arrow, color = " ", "white"
        elif current_val > prev_val:
            arrow, color = "🔼", "red" if higher_is_worse else "green"
        else:
            arrow, color = "🔽", "green" if higher_is_worse else "red"
        
        val_str = f"{int(current_val)}" if isinstance(current_val, (int, float)) and current_val == int(current_val) else f"{current_val:.1f}"
        
        return f"[{color}]{spike_icon}{arrow} {val_str}[/{color}]"

    def _render_dashboard_layout(self, prev_nodes_df=None, prev_indices_df=None, time_delta=None) -> Layout:
        layout = Layout(name="root")
        layout.split(
            Layout(name="header", size=4),
            Layout(ratio=1, name="main"),
            Layout(size=8, name="footer"),
        )
        layout["main"].split_row(Layout(name="side", ratio=2), Layout(name="body", ratio=3))
        
        layout["header"].update(self._render_header())
        layout["side"].update(self._render_node_health_table(prev_nodes_df))
        layout["body"].update(self._render_top_n_rankings(prev_indices_df, time_delta))
        layout["footer"].update(self._render_actionable_suggestions())
        
        return layout

    def _render_header(self) -> Panel:
        health = self.cluster_health
        status = health.get('status', 'N/A').upper()
        status_color = {"GREEN": "green", "YELLOW": "yellow", "RED": "red"}.get(status, "white")
        
        heap_pct = (self.cluster_stats.get('nodes', {}).get('jvm', {}).get('mem', {}).get('heap_used_in_bytes', 0) / self.cluster_stats.get('nodes', {}).get('jvm', {}).get('mem', {}).get('heap_max_in_bytes', 1) * 100)
        
        shard_status_str = (f"Initializing: [yellow]{health.get('initializing_shards', 0)}[/yellow] | "
                            f"Relocating: [yellow]{health.get('relocating_shards', 0)}[/yellow] | "
                            f"Unassigned: [bold red]{health.get('unassigned_shards', 0)}[/bold red]")

        summary_text = (
            f"Cluster: [b]{self.cluster_stats.get('cluster_name', 'N/A')}[/b] | Status: [b {status_color}]{status}[/b {status_color}] | "
            f"Última Actualización: {datetime.now().strftime('%H:%M:%S')}\n"
            f"Heap Total: {heap_pct:.1f}% | Tareas Pendientes: {len(self.pending_tasks.get('tasks', []))} | {shard_status_str}"
        )
        return Panel(summary_text, title="[b cyan]Dashboard de Salud Elasticsearch[/b cyan]", border_style="cyan")

    def _render_node_health_table(self, previous_df=None) -> Panel:
        if self.nodes_df.empty:
            return Panel("[yellow]Esperando datos de nodos...[/yellow]", border_style="yellow")
            
        table = Table(title="[b]Salud de Nodos por Tier[/b]", expand=True)
        table.add_column("Tier", style="magenta")
        table.add_column("Nodo", style="cyan", no_wrap=True)
        table.add_column("CPU%", justify="right")
        table.add_column("Heap%", justify="right")
        table.add_column("Heap Old%", justify="right")
        table.add_column("GC (c/t ms)", justify="right")
        table.add_column("Rechazos", justify="right")

        if previous_df is None or previous_df.empty:
            merged_df = self.nodes_df.copy()
        else:
            merged_df = self.nodes_df.merge(previous_df, on="node_name", how="left", suffixes=("", "_prev"))

        for tier, group in merged_df.groupby('tier'):
            table.add_section()
            for _, row in group.iterrows():
                cpu_str = self._format_metric(row['cpu_percent'], row.get('cpu_percent_prev'), spike_threshold=20)
                heap_str = self._format_metric(row['heap_percent'], row.get('heap_percent_prev'), spike_threshold=10)
                heap_old_str = self._format_metric(row['heap_old_gen_percent'], row.get('heap_old_gen_percent_prev'), spike_threshold=15)
                
                gc_count_str = self._format_metric(row['gc_count'], row.get('gc_count_prev'), spike_threshold=GC_COUNT_SPIKE_THRESHOLD)
                gc_time_str = self._format_metric(row['gc_time_ms'], row.get('gc_time_ms_prev'), spike_threshold=GC_TIME_SPIKE_THRESHOLD)
                gc_str = f"{gc_count_str}/{gc_time_str}"
                
                rejections_str = self._format_metric(row['rejections'], row.get('rejections_prev'), spike_threshold=0)

                table.add_row(f"[{'yellow' if 'hot' in tier else 'blue'}]{tier}[/]", row['node_name'], cpu_str, heap_str, heap_old_str, gc_str, rejections_str)
            
        return Panel(table, border_style="green")
        
    def _render_top_n_rankings(self, previous_df=None, time_delta=None) -> Panel:
        if self.indices_df.empty:
            return Panel("[yellow]No hay datos de índices disponibles.[/yellow]", title="[b cyan]Rankings de Rendimiento de Índices[/b cyan]", border_style="yellow")
        
        current_indices = self.indices_df.copy()
        if previous_df is not None and not previous_df.empty:
            time_delta = time_delta or REFRESH_INTERVAL
            merged_df = pd.merge(current_indices, previous_df[['index', 'indexing_total', 'search_total']], on='index', how='left', suffixes=('', '_prev'))
            merged_df['indexing_total_prev'] = merged_df['indexing_total_prev'].fillna(merged_df['indexing_total'])
            merged_df['search_total_prev'] = merged_df['search_total_prev'].fillna(merged_df['search_total'])
            current_indices['write_rate'] = (merged_df['indexing_total'] - merged_df['indexing_total_prev']) / time_delta
            current_indices['search_rate'] = (merged_df['search_total'] - merged_df['search_total_prev']) / time_delta
        else:
            current_indices['write_rate'] = 0.0
            current_indices['search_rate'] = 0.0
        
        top_writers = current_indices.sort_values('write_rate', ascending=False).head(5)
        writers_table = Table(title="[b]Top 5 - Tasa Escritura[/b]", expand=True)
        writers_table.add_column("Índice")
        writers_table.add_column("docs/s", justify="right")
        for _, r in top_writers.iterrows(): writers_table.add_row(r['index'], f"{r.get('write_rate', 0):.1f}")

        top_searchers = current_indices.sort_values('search_rate', ascending=False).head(5)
        searchers_table = Table(title="[b]Top 5 - Tasa Búsqueda[/b]", expand=True)
        searchers_table.add_column("Índice")
        searchers_table.add_column("req/s", justify="right")
        for _, r in top_searchers.iterrows(): searchers_table.add_row(r['index'], f"{r.get('search_rate', 0):.1f}")
        
        heap_table = Table(title="[b]Top 5 - Uso de Heap por Índice[/b]", expand=True)
        heap_table.add_column("Índice")
        heap_table.add_column("Total (MB)", justify="right")
        heap_table.add_column("Seg/Cache/Field", justify="right")
        for _, r in self.top_heap_indices.iterrows():
            breakdown = f"{r['memory_segments_mb']:.1f}/{r['memory_cache_mb']:.1f}/{r['memory_fielddata_mb']:.1f}"
            heap_table.add_row(r['index'], f"{r['heap_usage_mb']:.1f}", breakdown)

        return Panel(Columns([writers_table, searchers_table, heap_table]), title="[b cyan]Rankings de Rendimiento de Índices[/b cyan]", border_style="cyan")

    def _render_actionable_suggestions(self) -> Panel:
        suggestions = []
        if self.nodes_df.empty:
            return Panel("[yellow]Esperando datos para generar sugerencias...[/yellow]", border_style="yellow")

        for _, node in self.nodes_df.iterrows():
            if node['heap_old_gen_percent'] > HEAP_OLD_GEN_THRESHOLD:
                suggestion = f"🚨 [bold]Heap Old Gen Alto en '{node['node_name']}'[/bold]: Riesgo de pausas largas de GC."
                if not self.top_heap_indices.empty:
                    top_consumer = self.top_heap_indices.iloc[0]
                    suggestion += f" El índice [cyan]'{top_consumer['index']}'[/cyan] es el que más memoria consume ({top_consumer['heap_usage_mb']:.1f} MB)."
                suggestions.append(suggestion)

            if node['cpu_percent'] > CPU_USAGE_THRESHOLD:
                suggestions.append(f"🔥 [bold]CPU Alta en '{node['node_name']}'[/bold]: Revisa consultas costosas o picos de ingesta. Usa el análisis de tareas lentas.")
            
            if node['gc_time_ms'] > GC_TIME_THRESHOLD:
                suggestions.append(f"🗑️ [bold]GC Excesivo en '{node['node_name']}'[/bold]: El nodo está pausando para limpiar memoria. Revisa el uso de heap.")
            
            if node['rejections'] > 0:
                suggestion = f"🚦 [bold]Rechazos de Escritura en '{node['node_name']}'[/bold]: El nodo no puede procesar la carga de ingesta."
                if not self.indices_df.empty:
                    top_writer = self.indices_df.sort_values('write_rate', ascending=False).iloc[0]
                    if top_writer['write_rate'] > 0:
                        suggestion += f" La alta tasa de [cyan]'{top_writer['index']}'[/cyan] podría ser la causa. Considera escalar nodos o revisar shards."
                suggestions.append(suggestion)

            if node['breakers_tripped'] > 0:
                suggestions.append(f"🛑 [bold red]¡CIRCUIT BREAKER ACTIVADO en '{node['node_name']}'![/bold red] Operación rechazada por exceso de memoria. ¡CRÍTICO!")
        
        if self.cluster_health.get('unassigned_shards', 0) > 0:
            suggestions.append(f"💔 [bold]Shards No Asignados Detectados[/bold]: Usa la API `_cluster/allocation/explain` para diagnosticar la causa.")

        if not suggestions:
            return Panel("[bold green]✅ ¡Todo en orden! No se detectaron problemas críticos.[/bold green]", title="[bold cyan]Acciones Recomendadas (Motor Inteligente)[/bold cyan]", border_style="green")
        
        return Panel("\n".join(f"- {s}" for s in suggestions), title="[bold red]Acciones Recomendadas (Motor Inteligente)[/bold red]", border_style="red")

# --- Funciones de Análisis Adicional ---
def analyze_node_deep_dive(analyzer: ClusterAnalyzer):
    """Ejecuta un dashboard en vivo para todos los nodos, mostrando un desglose detallado de su estado interno."""
    # (El código de esta función no se modifica)
    try:
        with Live(console=console, screen=True, auto_refresh=False) as live:
            while True:
                analyzer.fetch_all_data(for_deep_dive=True)
                layout = Layout(name="deep_dive_root")
                node_panels = []
                sorted_nodes = analyzer.nodes_df.sort_values(by='cpu_percent', ascending=False)
                for _, node_row in sorted_nodes.iterrows():
                    node_name = node_row['node_name']
                    node_id = node_row['node_id']
                    node_stats = analyzer.node_stats_raw.get('nodes', {}).get(node_id, {})
                    prev_node_stats = analyzer.previous_node_stats_raw.get('nodes', {}).get(node_id, {})
                    tp_panel = render_thread_pool_panel(node_stats, prev_node_stats)
                    cb_panel = render_breaker_panel(node_stats, prev_node_stats)
                    node_layout = Layout(name=node_name)
                    node_layout.split_row(tp_panel, cb_panel)
                    node_panels.append(Panel(node_layout, title=f"[b cyan]Nodo: {node_name}[/b cyan]", border_style="magenta"))
                layout.split_column(*node_panels)
                live.update(layout, refresh=True)
                time.sleep(REFRESH_INTERVAL)
    except KeyboardInterrupt:
        console.print(f"\n[bold]Finalizando diagnóstico profundo...[/bold]")

# (Las funciones render_thread_pool_panel, render_breaker_panel, format_delta no se modifican)
def render_thread_pool_panel(node_stats, prev_node_stats):
    tp_table = Table(title="[b]🏊 Thread Pools[/b]", expand=True)
    tp_table.add_column("Pool", style="cyan")
    tp_table.add_column("Activas", justify="right")
    tp_table.add_column("En Cola", justify="right")
    tp_table.add_column("Rechazadas", justify="right")
    current_pools = node_stats.get('thread_pool', {})
    prev_pools = prev_node_stats.get('thread_pool', {}) if prev_node_stats else {}
    for name, stats in sorted(current_pools.items()):
        if stats.get('rejected', 0) > 0 or stats.get('queue', 0) > 0 or stats.get('active', 0) > 0:
            prev_stats = prev_pools.get(name, {})
            active_str = format_delta(stats.get('active', 0), prev_stats.get('active', 0))
            queue_str = format_delta(stats.get('queue', 0), prev_stats.get('queue', 0))
            rejected_str = format_delta(stats.get('rejected', 0), prev_stats.get('rejected', 0))
            tp_table.add_row(name, active_str, queue_str, f"[red]{rejected_str}[/red]")
    return Panel(tp_table)

def render_breaker_panel(node_stats, prev_node_stats):
    cb_table = Table(title="[b]🛑 Circuit Breakers[/b]", expand=True)
    cb_table.add_column("Breaker", style="cyan")
    cb_table.add_column("Límite (MB)", justify="right")
    cb_table.add_column("Usado (MB)", justify="right")
    cb_table.add_column("Tripped", justify="right")
    current_breakers = node_stats.get('breaker', {})
    prev_breakers = prev_node_stats.get('breaker', {}) if prev_node_stats else {}
    for name, stats in sorted(current_breakers.items()):
        limit_mb = stats.get('limit_size_in_bytes', 0) / 1e6
        used_mb = stats.get('estimated_size_in_bytes', 0) / 1e6
        tripped = stats.get('tripped', 0)
        prev_stats = prev_breakers.get(name, {})
        used_mb_str = format_delta(used_mb, prev_stats.get('estimated_size_in_bytes', 0) / 1e6)
        tripped_str = format_delta(tripped, prev_stats.get('tripped', 0))
        cb_table.add_row(name, f"{limit_mb:.1f}", used_mb_str, f"[red]{tripped_str}[/red]" if tripped > 0 else tripped_str)
    return Panel(cb_table)

def format_delta(current, previous):
    if pd.isna(previous):
        return f"{current:.1f}" if isinstance(current, float) else str(current)
    if current > previous:
        return f"[red]🔼 {current:.1f}[/red]" if isinstance(current, float) else f"[red]🔼 {current}[/red]"
    elif current < previous:
        return f"[green]🔽 {current:.1f}[/green]" if isinstance(current, float) else f"[green]🔽 {current}[/green]"
    return f"{current:.1f}" if isinstance(current, float) else str(current)

def analyze_shard_distribution_interactive(analyzer: ClusterAnalyzer):
    # (El código de esta función no se modifica)
    if analyzer.shards_df.empty:
        console.print("[red]No hay datos de shards disponibles para el análisis.[/red]")
        return
    analysis_type = Prompt.ask("¿Analizar por [1] Patrón de Índice o [2] Índice Individual?", choices=["1", "2"], default="1")
    sort_choices = {"1": ("Total Shards", "total_shards"), "2": ("Tamaño Total (GB)", "total_gb"), "3": ("Primarios", "primaries"), "4": ("Nodos Involucrados", "nodes_involved")}
    console.print("\nElige un criterio para ordenar:")
    for key, (desc, _) in sort_choices.items(): console.print(f"  [bold]{key}[/bold]: {desc}")
    sort_option = Prompt.ask("Opción de ordenamiento", choices=list(sort_choices.keys()), default="1")
    sort_by_column = sort_choices[sort_option][1]
    group_by_col = 'pattern' if analysis_type == '1' else 'index'
    try:
        with Live(console=console, screen=True, auto_refresh=False) as live:
            while True:
                analyzer.fetch_all_data()
                shards_df = analyzer.shards_df.copy()
                shards_df['pattern'] = shards_df['index'].apply(lambda x: re.sub(r'\d{4}[-.]\d{2}[-.]\d{2}|-\d{6}', '-*', x))
                shards_df.loc[:, 'store'] = pd.to_numeric(shards_df['store'], errors='coerce').fillna(0)
                summary_df = shards_df.groupby(group_by_col).agg(total_shards=('shard', 'count'), primaries=('prirep', lambda x: (x == 'p').sum()), replicas=('prirep', lambda x: (x == 'r').sum()), total_gb=('store', lambda x: x.sum() / 1024), nodes_involved=('node', 'nunique')).reset_index()
                sorted_df = summary_df.sort_values(by=sort_by_column, ascending=False)
                table = Table(title=f"Distribución de Shards por {'Patrón' if analysis_type == '1' else 'Índice'} (ordenado por {sort_choices[sort_option][0]})")
                table.add_column(group_by_col.capitalize(), style="cyan", max_width=50)
                table.add_column("Total Shards", justify="right")
                table.add_column("Primarios", justify="right")
                table.add_column("Réplicas", justify="right")
                table.add_column("Tamaño (GB)", justify="right")
                table.add_column("Nodos", justify="right")
                for _, row in sorted_df.head(20).iterrows():
                    table.add_row(row[group_by_col], str(row['total_shards']), str(row['primaries']), str(row['replicas']), f"{row['total_gb']:.2f}", str(row['nodes_involved']))
                live.update(Panel(table), refresh=True)
                time.sleep(REFRESH_INTERVAL)
    except KeyboardInterrupt:
        console.print("\n[bold]Volviendo al menú de análisis...[/bold]")

# --- NUEVAS FUNCIONES DE ANÁLISIS EXPERTO ---

def analyze_node_load_correlation(analyzer: ClusterAnalyzer):
    """Correlaciona la carga de CPU y memoria de un nodo con la carga de escritura/lectura generada por sus shards."""
    console.print(Rule("[bold]Análisis de Carga de Nodos por Actividad de Shards[/bold]"))

    # Doble captura para calcular tasas de actividad
    console.print("[yellow]Capturando métricas para calcular tasas de actividad...[/yellow]")
    analyzer.fetch_all_data()
    time.sleep(REFRESH_INTERVAL)
    analyzer.fetch_all_data()

    nodes_df = analyzer.nodes_df.copy()
    shards_df = analyzer.shards_df.copy()
    indices_df = analyzer.indices_df.copy()
    previous_indices_df = analyzer.previous_indices_df.copy()

    if any(df.empty for df in [nodes_df, shards_df, indices_df, previous_indices_df]):
        console.print("[red]No se pudieron obtener datos completos para el análisis de carga.[/red]")
        return

    # Calcular tasas de actividad por índice
    merged_indices = pd.merge(indices_df, previous_indices_df[['index', 'indexing_total', 'search_total']], on='index', how='left', suffixes=('', '_prev'))
    merged_indices['indexing_total_prev'] = merged_indices['indexing_total_prev'].fillna(merged_indices['indexing_total'])
    merged_indices['search_total_prev'] = merged_indices['search_total_prev'].fillna(merged_indices['search_total'])
    time_delta = REFRESH_INTERVAL if REFRESH_INTERVAL > 0 else 1
    indices_df['write_rate'] = (merged_indices['indexing_total'] - merged_indices['indexing_total_prev']) / time_delta
    indices_df['search_rate'] = (merged_indices['search_total'] - merged_indices['search_total_prev']) / time_delta

    # Preparar datos para la agregación
    # Unir shards con tasas de actividad de sus índices
    shard_activity_df = pd.merge(shards_df, indices_df[['index', 'write_rate', 'search_rate']], on='index', how='left').fillna(0)

    # Calcular carga por nodo
    node_loads = []
    for _, node_row in nodes_df.iterrows():
        node_name = node_row['node_name']
        shards_on_node = shard_activity_df[shard_activity_df['node'] == node_name]
        
        primary_shards = shards_on_node[shards_on_node['prirep'] == 'p']
        
        # La carga de escritura solo la reciben los shards primarios
        write_load = primary_shards['write_rate'].sum()
        # La carga de búsqueda la pueden recibir todos los shards
        search_load = shards_on_node['search_rate'].sum()
        
        node_loads.append({
            'Nodo': node_name,
            'CPU %': node_row['cpu_percent'],
            'Heap %': node_row['heap_percent'],
            'Primarios': len(primary_shards),
            'Total Shards': len(shards_on_node),
            'Carga Escritura (docs/s)': write_load,
            'Carga Búsqueda (req/s)': search_load
        })
    
    load_df = pd.DataFrame(node_loads).sort_values(by='CPU %', ascending=False)

    # Renderizar la tabla
    table = Table(title="Correlación de Carga de Nodos y Actividad de Shards")
    for col in load_df.columns:
        table.add_column(col, justify="right", style="cyan" if col == 'Nodo' else "white")
    
    for _, row in load_df.iterrows():
        table.add_row(
            row['Nodo'],
            f"{row['CPU %']:.0f}",
            f"{row['Heap %']:.0f}",
            str(row['Primarios']),
            str(row['Total Shards']),
            f"[green]{row['Carga Escritura (docs/s)']:.1f}[/green]",
            f"[yellow]{row['Carga Búsqueda (req/s)']:.1f}[/yellow]"
        )
    
    console.print(table)
    console.print("\n[italic]Esta tabla te ayuda a ver si los nodos con alta CPU/Heap son los que realmente procesan más escrituras o búsquedas.[/italic]")
    Prompt.ask("\n[bold]Presiona Enter para volver al menú...[/bold]")
    

def analyze_node_index_correlation(analyzer: ClusterAnalyzer):
    """Analiza y muestra el desbalance de shards primarios, enriquecido con métricas de actividad."""
    console.print(Rule("[bold]Análisis de Desbalance y Actividad de Shards (Vista Agrupada)[/bold]"))
    
    # Se necesita una doble captura de datos para calcular las tasas de escritura/búsqueda
    console.print("[yellow]Capturando métricas iniciales...[/yellow]")
    analyzer.fetch_all_data()
    time.sleep(REFRESH_INTERVAL) # Esperar para tener un delta de tiempo
    console.print("[yellow]Capturando métricas finales para calcular tasas de actividad...[/yellow]")
    analyzer.fetch_all_data()

    shards_df = analyzer.shards_df.copy()
    indices_df = analyzer.indices_df.copy()
    previous_indices_df = analyzer.previous_indices_df.copy()

    if shards_df.empty or indices_df.empty or previous_indices_df.empty:
        console.print("[red]No se pudieron obtener suficientes datos para el análisis de actividad.[/red]")
        return
    
    # --- Lógica de cálculo de tasas ---
    merged_df = pd.merge(indices_df, previous_indices_df[['index', 'indexing_total', 'search_total']], on='index', how='left', suffixes=('', '_prev'))
    merged_df['indexing_total_prev'] = merged_df['indexing_total_prev'].fillna(merged_df['indexing_total'])
    merged_df['search_total_prev'] = merged_df['search_total_prev'].fillna(merged_df['search_total'])
    # Evitar división por cero si el intervalo es muy corto
    time_delta = REFRESH_INTERVAL if REFRESH_INTERVAL > 0 else 1
    indices_df['write_rate'] = (merged_df['indexing_total'] - merged_df['indexing_total_prev']) / time_delta
    indices_df['search_rate'] = (merged_df['search_total'] - merged_df['search_total_prev']) / time_delta
    
    # --- Lógica de cálculo de desbalance ---
    primary_shards = shards_df[shards_df['prirep'] == 'p'].copy()
    primary_shards['pattern'] = primary_shards['index'].apply(lambda x: re.sub(r'\d{4}[-.]\d{2}[-.]\d{2}|-\d{6}', '-*', x))
    shard_counts = primary_shards.groupby(['pattern', 'node']).size().reset_index(name='shard_count')
    imbalance_stats = shard_counts.groupby('pattern')['shard_count'].agg(std_dev='std', node_count='count').fillna(0)
    
    # --- Enriquecer con tasas de actividad ---
    indices_df['pattern'] = indices_df['index'].apply(lambda x: re.sub(r'\d{4}[-.]\d{2}[-.]\d{2}|-\d{6}', '-*', x))
    pattern_activity = indices_df.groupby('pattern')[['write_rate', 'search_rate']].sum().reset_index()

    # Combinar estadísticas de desbalance con estadísticas de actividad
    imbalanced_patterns = pd.merge(
        imbalance_stats[imbalance_stats['node_count'] > 1].reset_index(),
        pattern_activity,
        on='pattern',
        how='left'
    ).fillna(0) # Rellenar NaNs en tasas para patrones sin actividad
    
    imbalanced_patterns = imbalanced_patterns[imbalanced_patterns['std_dev'] > 0].sort_values(by='std_dev', ascending=False)
    
    if imbalanced_patterns.empty:
        console.print("[green]✅ No se detectaron desbalances significativos de shards primarios entre nodos.[/green]")
        Prompt.ask("\n[bold]Presiona Enter para volver al menú...[/bold]")
        return

    console.print(f"\nSe encontraron [bold cyan]{len(imbalanced_patterns)}[/bold cyan] patrones de índice con desbalance. Se muestran ordenados por el más crítico.\n")

    # --- Renderizado de la tabla mejorada ---
    table = Table(title="Distribución y Actividad de Shards Primarios por Patrón y Nodo")
    table.add_column("Patrón de Índice", style="bold cyan", no_wrap=True, max_width=50)
    table.add_column("Desbalance (StdDev)", style="bold red", justify="right")
    table.add_column("Escrituras/s", style="green", justify="right")
    table.add_column("Búsquedas/s", style="yellow", justify="right")
    table.add_column("Nodo Afectado", style="magenta", no_wrap=True)
    table.add_column("N° Shards", style="white", justify="right")

    for _, pattern_row in imbalanced_patterns.iterrows():
        pattern, std_dev = pattern_row['pattern'], pattern_row['std_dev']
        write_rate, search_rate = pattern_row.get('write_rate', 0), pattern_row.get('search_rate', 0)
        
        nodes_for_pattern = shard_counts[shard_counts['pattern'] == pattern].sort_values(by='shard_count', ascending=False)
        table.add_section()

        is_first_row = True
        for _, node_row in nodes_for_pattern.iterrows():
            node_name, shard_count = node_row['node'], node_row['shard_count']
            style = "on red" if shard_count == nodes_for_pattern['shard_count'].max() and len(nodes_for_pattern) > 1 else ""
            
            if is_first_row:
                table.add_row(
                    pattern, f"{std_dev:.2f}", f"{write_rate:.1f}", f"{search_rate:.1f}",
                    Text(node_name, style=style), Text(str(shard_count), style=style)
                )
                is_first_row = False
            else:
                table.add_row("", "", "", "", Text(node_name, style=style), Text(str(shard_count), style=style))
    
    console.print(table)
    
    # El panel informativo se mantiene al final
    info_text = """
## 🤔 ¿Qué Significa Este Desbalance?

Has observado que para ciertos patrones de índice, la cantidad de **shards primarios** no está distribuida de manera uniforme entre los nodos.

* **Impacto Principal**: El nodo con más shards (resaltado en rojo) se convierte en un **"hotspot" de indexación**. Recibe una porción desproporcionada de las escrituras para esos datos, lo que puede causar:
    * **CPU Alta**: El nodo se sobrecarga procesando la ingesta.
    * **Presión de Memoria (Heap)**: Más shards activos consumen más memoria.
    * **I/O de Disco Elevado**: El nodo escribe en disco más que sus pares.
* **Consecuencia**: Un solo nodo puede volverse inestable y ralentizar el rendimiento de todo el clúster, mientras otros nodos están subutilizados.

## 🕵️ Posibles Causas Raíz y Pasos a Seguir

El desbalance de shards no es aleatorio. Generalmente, es un síntoma de un problema de configuración. Aquí tienes una guía para encontrar la causa:

1.  **💡 Causa: N° de Shards no es múltiplo del N° de Nodos.**
    * **Explicación**: Es la causa más común. Si un índice tiene **5** shards primarios y tu clúster tiene **3** nodos de datos, es matemáticamente imposible distribuirlos equitativamente (un nodo tendrá 1 shard y dos nodos tendrán 2).
    * **Acción**: Al crear plantillas o índices, asegúrate de que el número de shards primarios sea un múltiplo del número de nodos en el tier de datos correspondiente (ej., 6 shards para 3 nodos, 12 shards para 4 nodos, etc.).

2.  **💡 Causa: Ausencia de `Allocation Awareness`.**
    * **Explicación**: Elasticsearch no sabe cómo están distribuidos tus nodos físicamente (ej., en diferentes racks o zonas de disponibilidad). Sin esta configuración, podría agrupar shards en nodos que comparten el mismo hardware físico.
    * **Acción**: Configura `cluster.routing.allocation.awareness.attributes` en tu clúster y define los atributos correspondientes en el `elasticsearch.yml` de cada nodo (ej., `node.attr.zone: 'us-east-1a'`).

3.  **💡 Causa: Nodos cerca del Límite de Disco (`Disk Watermark`).**
    * **Explicación**: Si un nodo está casi lleno (superando el "low disk watermark"), Elasticsearch evitará activamente asignarle nuevos shards. Esto fuerza a que los shards se concentren en los nodos con más espacio libre.
    * **Acción**: Revisa el uso de disco con la API `_cat/allocation?v=true`. Si hay nodos con poco espacio, considera añadir más almacenamiento o eliminar datos antiguos.

4.  **💡 Causa: Rebalanceo del Clúster Desactivado.**
    * **Explicación**: Es posible que el rebalanceo automático del clúster esté desactivado o limitado, impidiendo que Elasticsearch mueva shards para corregir el desbalance por sí mismo.
    * **Acción**: Revisa la configuración del clúster con `GET /_cluster/settings` y busca la clave `cluster.routing.rebalance.enable`. Debería estar en `all` (por defecto).
"""
    console.print(Panel(Markdown(info_text), title="[bold cyan]Guía de Diagnóstico de Desbalance[/bold cyan]", border_style="cyan"))
    Prompt.ask("\n[bold]Presiona Enter para volver al menú...[/bold]")

def analyze_slow_tasks(analyzer: ClusterAnalyzer):
    """Identifica tareas de búsqueda lentas que se están ejecutando en el clúster."""
    console.print(Rule("[bold]Identificación de Tareas de Búsqueda Lentas[/bold]"))
    
    tasks_data = analyzer.client.get("_tasks", params={'actions': '*search*', 'detailed': 'true'})
    if not tasks_data or 'nodes' not in tasks_data:
        console.print("[red]No se pudo obtener información de tareas.[/red]")
        return

    slow_tasks = []
    for node_id, node_info in tasks_data['nodes'].items():
        node_name = node_info.get('name')
        for task_id, task_info in node_info['tasks'].items():
            running_time_ns = task_info.get('running_time_in_nanos', 0)
            running_time_min = running_time_ns / 60e9 
            if running_time_min > LONG_RUNNING_TASK_MINUTES:
                slow_tasks.append({
                    'node': node_name,
                    'time_min': running_time_min,
                    'description': task_info.get('description', 'N/A')
                })
    
    if not slow_tasks:
        console.print("[green]✅ No se detectaron tareas de búsqueda lentas por encima del umbral.[/green]")
        Prompt.ask("\n[bold]Presiona Enter para volver al menú...[/bold]")
        return

    table = Table(title=f"Tareas de Búsqueda Lentas (Más de {LONG_RUNNING_TASK_MINUTES} minutos)")
    table.add_column("Nodo", style="cyan")
    table.add_column("Tiempo (min)", justify="right", style="yellow")
    table.add_column("Descripción de la Consulta", style="white")

    for task in sorted(slow_tasks, key=lambda x: x['time_min'], reverse=True):
        table.add_row(task['node'], f"{task['time_min']:.2f}", task['description'])

    console.print(table)
    Prompt.ask("\n[bold]Presiona Enter para volver al menú...[/bold]")

def analyze_index_templates(analyzer: ClusterAnalyzer):
    """Evalúa las plantillas de índice en busca de problemas y muestra su impacto."""
    console.print(Rule("[bold]Diagnóstico y Relevancia de Plantillas de Índice[/bold]"))
    analyzer.fetch_all_data() # Asegurarse de tener los datos de índices

    templates_data = analyzer.client.get("_index_template")
    indices_df = analyzer.indices_df
    
    if not templates_data or 'index_templates' not in templates_data:
        console.print("[red]No se pudieron obtener plantillas de índice.[/red]")
        return
        
    if indices_df.empty:
        console.print("[yellow]No hay datos de índices para correlacionar con las plantillas.[/yellow]")
        return

    # Convertir columnas a numérico para poder sumar
    indices_df['docs.count'] = pd.to_numeric(indices_df['docs.count'], errors='coerce').fillna(0)
    indices_df['store.size'] = pd.to_numeric(indices_df['store.size'], errors='coerce').fillna(0)

    table = Table(title="Análisis de Plantillas de Índice y su Impacto")
    table.add_column("Plantilla", style="cyan")
    table.add_column("Índices", justify="right", style="magenta")
    table.add_column("Docs Totales", justify="right", style="green")
    table.add_column("Tamaño Total", justify="right", style="yellow")
    table.add_column("Diagnóstico", style="white")

    for template_info in templates_data['index_templates']:
        name = template_info['name']
        template = template_info['index_template']
        patterns = template.get('index_patterns', [])
        
        # Calcular impacto de la plantilla
        matching_indices = indices_df[indices_df['index'].apply(
            lambda idx: any(fnmatch.fnmatch(idx, pattern) for pattern in patterns)
        )]
        
        index_count = len(matching_indices)
        total_docs = matching_indices['docs.count'].sum()
        total_size_mb = matching_indices['store.size'].sum()
        
        # Formatear tamaño para legibilidad
        size_str = f"{total_size_mb / 1024:.2f} GB" if total_size_mb > 1024 else f"{total_size_mb:.1f} MB"

        # Diagnósticos
        diagnostics = []
        if 'ilm' not in template.get('settings', {}).get('index', {}):
            diagnostics.append("[yellow]Sin política ILM[/yellow]")
        
        num_shards = template.get('settings', {}).get('index', {}).get('number_of_shards')
        if num_shards and int(num_shards) > HIGH_SHARD_COUNT_TEMPLATE_THRESHOLD:
            diagnostics.append(f"[orange3]Alto N° de Shards ({num_shards})[/orange3]")

        for p in patterns:
            if p == "*" or p == "*-*":
                diagnostics.append(f"[red]Comodín Genérico ('{p}')[/red]")
        
        diagnostics_str = ", ".join(diagnostics) if diagnostics else "[green]OK[/green]"

        table.add_row(
            name,
            str(index_count),
            f"{total_docs:,}", # Formato de miles
            size_str,
            diagnostics_str
        )
        
    console.print(table)
    Prompt.ask("\n[bold]Presiona Enter para volver al menú...[/bold]")

def analyze_dusty_shards(analyzer: ClusterAnalyzer):
    """Identifica shards vacíos o extremadamente pequeños ('polvo de shards')."""
    console.print(Rule("[bold]Detección de Shards Vacíos y 'Polvo de Shards'[/bold]"))
    analyzer.fetch_all_data()
    shards_df = analyzer.shards_df.copy()

    if shards_df.empty:
        console.print("[yellow]No se pudieron obtener datos de shards para el análisis.[/yellow]")
        return
    
    shards_df['docs'] = pd.to_numeric(shards_df['docs'], errors='coerce').fillna(0)
    shards_df['store'] = pd.to_numeric(shards_df['store'], errors='coerce').fillna(0)
    
    empty_shards = shards_df[(shards_df['docs'] == 0) & (shards_df['state'] == 'STARTED')]
    dusty_shards = shards_df[(shards_df['docs'] > 0) & (shards_df['store'] < DUSTY_SHARD_MB_THRESHOLD) & (shards_df['state'] == 'STARTED')]

    if empty_shards.empty and dusty_shards.empty:
        console.print("[green]✅ No se detectaron shards vacíos ni 'polvo de shards' problemáticos.[/green]")
        Prompt.ask("\n[bold]Presiona Enter para volver al menú...[/bold]")
        return

    empty_table = Table(title=f"Shards Vacíos (docs=0)")
    empty_table.add_column("Índice", style="cyan")
    empty_table.add_column("Shard", justify="right")
    empty_table.add_column("Nodo", style="magenta")
    for _, row in empty_shards.head(10).iterrows():
        empty_table.add_row(row['index'], row['shard'], row['node'])

    dusty_table = Table(title=f"'Polvo de Shards' (< {DUSTY_SHARD_MB_THRESHOLD} MB)")
    dusty_table.add_column("Índice", style="cyan")
    dusty_table.add_column("Tamaño (MB)", justify="right")
    dusty_table.add_column("Docs", justify="right")
    dusty_table.add_column("Nodo", style="magenta")
    for _, row in dusty_shards.sort_values(by='store').head(10).iterrows():
        dusty_table.add_row(row['index'], f"{row['store']:.1f}", str(int(row['docs'])), row['node'])

    console.print(Columns([Panel(empty_table), Panel(dusty_table)]))
    console.print("\n[italic]Los shards vacíos y el 'polvo de shards' consumen memoria heap de forma ineficiente. Considera usar la API `_shrink` o ajustar las políticas de `rollover` e ILM.[/italic]")
    Prompt.ask("\n[bold]Presiona Enter para volver al menú...[/bold]")

# --- Funciones de Control ---
def run_live_dashboard(analyzer: ClusterAnalyzer):
    """Ejecuta el dashboard en modo de actualización en vivo."""
    try:
        with Live(console=console, screen=True, auto_refresh=False) as live:
            while True:
                analyzer.fetch_all_data()
                dashboard = analyzer._render_dashboard_layout(analyzer.previous_nodes_df, analyzer.previous_indices_df)
                live.update(dashboard, refresh=True)
                time.sleep(REFRESH_INTERVAL)
    except KeyboardInterrupt:
        console.print("\n[bold]Volviendo al menú principal...[/bold]")

def generate_historical_report(analyzer: ClusterAnalyzer, window_seconds: int, window_str: str):
    """Genera un reporte único comparando con un snapshot histórico."""
    console.print(f"\n[yellow]Esta función de reporte histórico aún no está implementada.[/yellow]")
    # Lógica futura para leer snapshots...

# --- Menú Principal y Ejecución ---
def main():
    """Función principal que muestra el menú y controla el flujo."""
    console.print(Rule("[bold]Herramienta de Auditoría Profesional para Elasticsearch[/bold]"))
    client = ElasticsearchClient(ES_HOST, ES_USER, ES_PASS, VERIFY_SSL)
    if not client.cluster_info:
        return

    analyzer = ClusterAnalyzer(client)
    
    # Menú principal actualizado con las nuevas opciones de análisis experto
    menu_options = {
        "1": ("📈 Dashboard General en Vivo", "live"),
        "2": ("🔬 Dashboard de Causa Raíz (Nodos)", "deep_dive"),
        "3": ("📊 Dashboard de Distribución de Shards", "shard_dist"),
        "4": ("🔀 Análisis de Desbalance de Shards", "correlation"),
        "5": ("⚡ Análisis de Carga de Nodos por Shards", "node_load"),
        "6": ("⌛ Identificar Tareas de Búsqueda Lentas", "slow_tasks"),
        "7": ("📝 Diagnóstico de Plantillas de Índice", "templates"),
        "8": ("🧹 Detección de Shards Vacíos / Polvo", "dusty"),
        "salir": ("🚪 Salir", "exit")
    }
    
    while True:
        console.rule("[bold cyan]Menú Principal de Análisis Experto[/bold cyan]")
        for key, (desc, _) in menu_options.items():
            console.print(f"[bold]{key}[/bold]: {desc}")
        
        main_option = Prompt.ask("\n[bold]Elige una opción[/bold]", choices=list(menu_options.keys()), default="1")
        action = menu_options[main_option][1]

        if action == "live":
            run_live_dashboard(analyzer)
        elif action == "deep_dive":
            analyze_node_deep_dive(analyzer)
        elif action == "shard_dist":
            analyze_shard_distribution_interactive(analyzer)
        elif action == "correlation":
            analyze_node_index_correlation(analyzer)
        elif action == "node_load":
            analyze_node_load_correlation(analyzer)
        elif action == "slow_tasks":
            analyze_slow_tasks(analyzer)
        elif action == "templates":
            analyze_index_templates(analyzer)
        elif action == "dusty":
            analyze_dusty_shards(analyzer)
        elif action == "exit":
            console.print("[bold red]Saliendo del sistema...[/bold red]")
            break
        
        console.print("\n[green]Operación completada. Volviendo al menú principal...[/green]")
        time.sleep(1)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Herramienta de Auditoría Profesional para Elasticsearch.")
    parser.add_argument('--report', action='store_true', help='Genera un reporte en formato Markdown y sale.')
    args = parser.parse_args()

    # El modo no interactivo ya está implementado y funciona.
    if args.report:
        # Modo no interactivo para bots
        client = ElasticsearchClient(ES_HOST, ES_USER, ES_PASS, VERIFY_SSL)
        if client.cluster_info:
            analyzer = ClusterAnalyzer(client)
            # Fetch de datos con un pequeño delay para asegurar tasas
            analyzer.fetch_all_data()
            time.sleep(2) 
            analyzer.fetch_all_data()

            # Usar el nuevo motor de sugerencias contextuales
            suggestions_panel = analyzer._render_actionable_suggestions()
            
            console.print(f"# Reporte de Salud del Cluster: {analyzer.cluster_stats.get('cluster_name', 'N/A')}")
            console.print(f"**Fecha:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            console.print(f"**Estado:** {analyzer.cluster_health.get('status', 'N/A')}")
            console.print("## 💡 Sugerencias y Alertas (Motor Inteligente)")
            
            # Convertir el panel de rich a Markdown simple
            text_content = console.export_text(clear=False)
            # Limpieza básica para formato Markdown
            suggestions_text = suggestions_panel.renderable.renderables[0].text
            for line in suggestions_text.split('\n'):
                 console.print(f"* {line.lstrip('- ')}")

    else:
        # Modo interactivo normal
        try:
            main()
        except Exception as e:
            logging.error("Ocurrió un error fatal.", exc_info=True)
            console.print(f"[bold red]❌ Ocurrió un error fatal:[/bold red]")
            console.print_exception(show_locals=True)