# src/renderer.py
import pandas as pd
from datetime import datetime
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.columns import Columns
from rich.layout import Layout
from rich.markdown import Markdown
from rich.text import Text
from rich.rule import Rule 

# Importación completa de las constantes desde el archivo de configuración
from .config import (
    HEAP_OLD_GEN_THRESHOLD,
    CPU_USAGE_THRESHOLD,
    GC_TIME_THRESHOLD,
    GC_COUNT_SPIKE_THRESHOLD,
    GC_TIME_SPIKE_THRESHOLD,
    REJECTIONS_THRESHOLD,
    REFRESH_INTERVAL,
    ANOMALY_STD_DEV_FACTOR
)

console = Console()

# --- Funciones de formato de métricas ---
def _format_metric(current_val, prev_val, spike_threshold, higher_is_worse=True):
    """Formatea una métrica con un indicador de cambio y un ícono de pico si supera un umbral."""
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

def format_delta(current, previous, higher_is_worse=True):
    """Formatea un valor mostrando un indicador de cambio (arriba/abajo) respecto a un valor previo."""
    if pd.isna(previous):
        return f"{current:.1f}" if isinstance(current, float) else str(current)
    
    if current > previous:
        color = "red" if higher_is_worse else "green"
        return f"[{color}]🔼 {current:.1f}[/{color}]" if isinstance(current, float) else f"[{color}]🔼 {current}[/{color}]"
    elif current < previous:
        color = "green" if higher_is_worse else "red"
        return f"[{color}]🔽 {current:.1f}[/{color}]" if isinstance(current, float) else f"[{color}]🔽 {current}[/{color}]"
    
    return f"{current:.1f}" if isinstance(current, float) else str(current)


# --- Funciones de renderizado de componentes de UI ---
def _render_header(analyzer) -> Panel:
    """Renderiza el panel de cabecera con el estado general del clúster."""
    health = analyzer.cluster_health
    status = health.get('status', 'N/A').upper()
    status_color = {"GREEN": "green", "YELLOW": "yellow", "RED": "red"}.get(status, "white")
    
    heap_stats = analyzer.cluster_stats.get('nodes', {}).get('jvm', {}).get('mem', {})
    heap_used = heap_stats.get('heap_used_in_bytes', 0)
    heap_max = heap_stats.get('heap_max_in_bytes', 1)
    heap_pct = (heap_used / heap_max * 100) if heap_max > 0 else 0
    
    pending_tasks_data = analyzer.client.get("_cluster/pending_tasks")
    pending_tasks_count = len(pending_tasks_data.get('tasks', [])) if pending_tasks_data else 0

    shard_status_str = (f"Initializing: [yellow]{health.get('initializing_shards', 0)}[/yellow] | "
                        f"Relocating: [yellow]{health.get('relocating_shards', 0)}[/yellow] | "
                        f"Unassigned: [bold red]{health.get('unassigned_shards', 0)}[/bold red]")

    summary_text = (
        f"Cluster: [b]{analyzer.cluster_name}[/b] | Status: [b {status_color}]{status}[/b {status_color}] | "
        f"Última Actualización: {datetime.now().strftime('%H:%M:%S')}\n"
        f"Heap Total: {heap_pct:.1f}% | Tareas Pendientes: {pending_tasks_count} | {shard_status_str}"
    )
    return Panel(summary_text, title="[b cyan]Dashboard de Salud Elasticsearch[/b cyan]", border_style="cyan")

def _render_node_health_table(analyzer) -> Panel:
    """Renderiza la tabla de salud de nodos, agrupada por tier y con indicadores de cambio."""
    if analyzer.nodes_df.empty:
        return Panel("[yellow]Esperando datos de nodos...[/yellow]", border_style="yellow")
        
    table = Table(title="[b]Salud de Nodos por Tier[/b]", expand=True)
    table.add_column("Tier", style="magenta")
    table.add_column("Nodo", style="cyan", no_wrap=True)
    table.add_column("CPU%", justify="right")
    table.add_column("Heap%", justify="right")
    table.add_column("Heap Old%", justify="right")
    table.add_column("GC (c/t ms)", justify="right")
    table.add_column("Rechazos", justify="right")

    # CORRECCIÓN: Manejar el caso inicial donde el DataFrame previo está vacío.
    if analyzer.previous_nodes_df.empty:
        merged_df = analyzer.nodes_df.copy()
    else:
        merged_df = analyzer.nodes_df.merge(analyzer.previous_nodes_df, on="node_name", how="left", suffixes=("", "_prev"))

    for tier, group in merged_df.groupby('tier'):
        table.add_section()
        for _, row in group.sort_values(by='cpu_percent', ascending=False).iterrows():
            cpu_str = _format_metric(row['cpu_percent'], row.get('cpu_percent_prev'), spike_threshold=20)
            heap_str = _format_metric(row['heap_percent'], row.get('heap_percent_prev'), spike_threshold=10)
            heap_old_str = _format_metric(row['heap_old_gen_percent'], row.get('heap_old_gen_percent_prev'), spike_threshold=15)
            
            gc_count_str = _format_metric(row['gc_count'], row.get('gc_count_prev'), spike_threshold=GC_COUNT_SPIKE_THRESHOLD)
            gc_time_str = _format_metric(row['gc_time_ms'], row.get('gc_time_ms_prev'), spike_threshold=GC_TIME_SPIKE_THRESHOLD)
            gc_str = f"{gc_count_str}/{gc_time_str}"
            
            rejections_str = _format_metric(row.get('rejections', 0), row.get('rejections_prev', 0), spike_threshold=REJECTIONS_THRESHOLD)

            tier_style = 'yellow' if 'hot' in tier else 'blue' if 'warm' in tier else 'white'
            table.add_row(f"[{tier_style}]{tier}[/]", row['node_name'], cpu_str, heap_str, heap_old_str, gc_str, rejections_str)
            
    return Panel(table, border_style="green")
    
def _render_top_n_rankings(analyzer) -> Panel:
    """Renderiza los rankings de índices por tasa de escritura y búsqueda."""
    if analyzer.indices_df.empty:
        return Panel("[yellow]No hay datos de índices disponibles.[/yellow]", title="[b cyan]Rankings de Rendimiento de Índices[/b cyan]", border_style="yellow")
    
    current_indices = analyzer.indices_df.copy()
    
    # CORRECCIÓN: Calcular tasas solo si hay datos previos para comparar.
    if not analyzer.previous_indices_df.empty and analyzer.previous_fetch_time:
        time_delta = (analyzer.last_fetch_time - analyzer.previous_fetch_time).total_seconds()
        if time_delta <= 0: time_delta = REFRESH_INTERVAL

        merged_df = pd.merge(current_indices, analyzer.previous_indices_df[['index', 'indexing.index_total', 'search.query_total']], on='index', how='left', suffixes=('', '_prev'))
        merged_df.fillna({'indexing.index_total_prev': merged_df['indexing.index_total'], 'search.query_total_prev': merged_df['search.query_total']}, inplace=True)
        
        current_indices['write_rate'] = (merged_df['indexing.index_total'] - merged_df['indexing.index_total_prev']) / time_delta
        current_indices['search_rate'] = (merged_df['search.query_total'] - merged_df['search.query_total_prev']) / time_delta
    else:
        # En la primera ejecución, las tasas son 0.
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
    
    return Panel(Columns([writers_table, searchers_table]), title="[b cyan]Rankings de Rendimiento de Índices[/b cyan]", border_style="cyan")

def _render_actionable_suggestions(analyzer) -> Panel:
    """Renderiza el panel de sugerencias accionables basado en los umbrales definidos."""
    suggestions = []
    if analyzer.nodes_df.empty:
        return Panel("[yellow]Esperando datos para generar sugerencias...[/yellow]", border_style="yellow")

    for _, node in analyzer.nodes_df.iterrows():
        if node['heap_old_gen_percent'] > HEAP_OLD_GEN_THRESHOLD:
            suggestions.append(f"🚨 [bold]Heap Old Gen Alto en '{node['node_name']}' ({node['heap_old_gen_percent']:.0f}%):[/bold] Riesgo de pausas largas de GC. Investigar consumo de memoria.")
        if node['cpu_percent'] > CPU_USAGE_THRESHOLD:
            suggestions.append(f"🔥 [bold]CPU Alta en '{node['node_name']}' ({node['cpu_percent']:.0f}%):[/bold] Revisa `hot_threads` o el análisis de toxicidad de shards.")
        if node['gc_time_ms'] > GC_TIME_THRESHOLD:
            suggestions.append(f"🗑️ [bold]GC Excesivo en '{node['node_name']}':[/bold] El nodo está empleando mucho tiempo en limpiar memoria, lo cual indica presión.")
        if node.get('rejections', 0) > REJECTIONS_THRESHOLD:
            suggestions.append(f"🚦 [bold]Rechazos de Escritura en '{node['node_name']}':[/bold] El nodo no puede procesar la carga de ingesta. Considera escalar o revisar shards.")
        if node.get('breakers_tripped', 0) > 0:
            suggestions.append(f"🛑 [bold red]¡CIRCUIT BREAKER ACTIVADO en '{node['node_name']}'![/bold red] Operación rechazada por exceso de memoria. ¡ACCIÓN CRÍTICA REQUERIDA!")
    
    if analyzer.cluster_health.get('unassigned_shards', 0) > 0:
        suggestions.append(f"💔 [bold]Shards No Asignados Detectados ({analyzer.cluster_health['unassigned_shards']}):[/bold] Usa la API `_cluster/allocation/explain` para diagnosticar la causa raíz.")

    if not suggestions:
        return Panel("[bold green]✅ ¡Todo en orden! No se detectaron problemas críticos según los umbrales configurados.[/bold green]", title="[bold cyan]Acciones Recomendadas (Motor Inteligente)[/bold cyan]", border_style="green")
    
    return Panel("\n".join(f"- {s}" for s in suggestions), title="[bold red]Acciones Recomendadas (Motor Inteligente)[/bold red]", border_style="red")

def render_dashboard_layout(analyzer) -> Layout:
    """Construye y devuelve el layout principal del dashboard."""
    layout = Layout(name="root")
    layout.split(
        Layout(name="header", size=4),
        Layout(ratio=1, name="main"),
        Layout(size=10, name="footer"),
    )
    layout["main"].split_row(Layout(name="side", ratio=2), Layout(name="body", ratio=3))
    
    layout["header"].update(_render_header(analyzer))
    layout["side"].update(_render_node_health_table(analyzer))
    layout["body"].update(_render_top_n_rankings(analyzer))
    layout["footer"].update(_render_actionable_suggestions(analyzer))
    
    return layout

def render_thread_pool_panel(node_stats, prev_node_stats):
    """Renderiza el panel de estado de los Thread Pools para un nodo."""
    tp_table = Table(title="[b]🏊 Thread Pools[/b]", expand=True, show_header=True)
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
            rejected_str = format_delta(stats.get('rejected', 0), prev_stats.get('rejected', 0), higher_is_worse=True)
            tp_table.add_row(name, active_str, queue_str, rejected_str)
    return Panel(tp_table)

def render_breaker_panel(node_stats, prev_node_stats):
    """Renderiza el panel de estado de los Circuit Breakers para un nodo."""
    cb_table = Table(title="[b]🛑 Circuit Breakers[/b]", expand=True, show_header=True)
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
        tripped_str = format_delta(tripped, prev_stats.get('tripped', 0), higher_is_worse=True)
        tripped_style = "bold red" if tripped > (prev_stats.get('tripped',0)) else "white"
        cb_table.add_row(name, f"{limit_mb:.1f}", used_mb_str, f"[{tripped_style}]{tripped_str}[/{tripped_style}]")
    return Panel(cb_table)

def render_actionable_suggestions_markdown(analyzer):
    """Genera y muestra las sugerencias en formato Markdown para el modo --report."""
    console.print(f"# Reporte de Salud del Cluster: {analyzer.cluster_name}")
    console.print(f"**Fecha:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    console.print(f"**Estado:** {analyzer.cluster_health.get('status', 'N/A')}")
    console.print("\n## 💡 Sugerencias y Alertas (Motor Inteligente)\n")
    
    suggestions_panel = _render_actionable_suggestions(analyzer)
    
    if hasattr(suggestions_panel.renderable, 'renderables'):
        suggestions_text = suggestions_panel.renderable.renderables[0].text
        plain_text = Text.from_markup(suggestions_text).plain
        for line in plain_text.split('\n'):
            console.print(f"* {line.lstrip('- ')}")
    else:
        console.print(f"* {suggestions_panel.renderable.plain}")

def render_historical_report(analyzer):
    """Genera un reporte de anomalías basado en datos históricos."""
    console.print(Rule("[bold]📈 Reporte de Detección de Anomalías Históricas[/bold]"))
    
    with console.status("[yellow]Cargando y analizando datos históricos...[/yellow]"):
        historical_df = analyzer.load_historical_snapshots(window_hours=24)
    
    if historical_df.empty or len(historical_df) < 10:
        console.print("[yellow]No hay suficientes datos históricos para un reporte de anomalías.[/yellow]")
        return
        
    if analyzer.nodes_df.empty:
        analyzer.fetch_all_data()
    current_df = analyzer.nodes_df
    
    metrics_to_check = ['cpu_percent', 'heap_percent', 'load_1m']
    anomalies_found = []

    for _, current_node in current_df.iterrows():
        node_name = current_node['node_name']
        node_historical_df = historical_df[historical_df['name'] == node_name]
        if node_historical_df.empty: continue
            
        for metric in metrics_to_check:
            mean = node_historical_df[metric].mean()
            std = node_historical_df[metric].std()
            current_value = current_node[metric]
            
            if std > 0 and abs(current_value - mean) > ANOMALY_STD_DEV_FACTOR * std:
                anomalies_found.append({
                    "Nodo": node_name, "Métrica": metric,
                    "Valor Actual": f"{current_value:.2f}",
                    "Media Histórica": f"{mean:.2f}",
                    "Desviación": f"{abs(current_value - mean) / std:.1f}σ"
                })

    if not anomalies_found:
        console.print(Panel("[green]✅ No se detectaron anomalías significativas en el estado actual del clúster comparado con su historial reciente.[/green]"))
        return

    table = Table(title=f"Anomalías Detectadas (Umbral > {ANOMALY_STD_DEV_FACTOR}σ)")
    table.add_column("Nodo", style="cyan")
    table.add_column("Métrica", style="yellow")
    table.add_column("Valor Actual", justify="right", style="green")
    table.add_column("Media Histórica", justify="right", style="blue")
    table.add_column("Desviación", justify="right", style="red")

    for anomaly in anomalies_found:
        table.add_row(anomaly["Nodo"], anomaly["Métrica"], anomaly["Valor Actual"], anomaly["Media Histórica"], anomaly["Desviación"])
    
    console.print(table)