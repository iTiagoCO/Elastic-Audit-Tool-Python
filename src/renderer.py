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

from .config import (
    HEAP_OLD_GEN_THRESHOLD, CPU_USAGE_THRESHOLD, GC_TIME_THRESHOLD,
    GC_COUNT_SPIKE_THRESHOLD, GC_TIME_SPIKE_THRESHOLD, REFRESH_INTERVAL
)

console = Console()

# --- Funciones de formato de métricas ---
def _format_metric(current_val, prev_val, spike_threshold, higher_is_worse=True):
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

def format_delta(current, previous):
    if pd.isna(previous):
        return f"{current:.1f}" if isinstance(current, float) else str(current)
    if current > previous:
        return f"[red]🔼 {current:.1f}[/red]" if isinstance(current, float) else f"[red]🔼 {current}[/red]"
    elif current < previous:
        return f"[green]🔽 {current:.1f}[/green]" if isinstance(current, float) else f"[green]🔽 {current}[/green]"
    return f"{current:.1f}" if isinstance(current, float) else str(current)


# --- Funciones de renderizado de componentes de UI ---
def _render_header(analyzer) -> Panel:
    health = analyzer.cluster_health
    status = health.get('status', 'N/A').upper()
    status_color = {"GREEN": "green", "YELLOW": "yellow", "RED": "red"}.get(status, "white")
    
    heap_pct = (analyzer.cluster_stats.get('nodes', {}).get('jvm', {}).get('mem', {}).get('heap_used_in_bytes', 0) / analyzer.cluster_stats.get('nodes', {}).get('jvm', {}).get('mem', {}).get('heap_max_in_bytes', 1) * 100)
    
    shard_status_str = (f"Initializing: [yellow]{health.get('initializing_shards', 0)}[/yellow] | "
                        f"Relocating: [yellow]{health.get('relocating_shards', 0)}[/yellow] | "
                        f"Unassigned: [bold red]{health.get('unassigned_shards', 0)}[/bold red]")

    summary_text = (
        f"Cluster: [b]{analyzer.cluster_stats.get('cluster_name', 'N/A')}[/b] | Status: [b {status_color}]{status}[/b {status_color}] | "
        f"Última Actualización: {datetime.now().strftime('%H:%M:%S')}\n"
        f"Heap Total: {heap_pct:.1f}% | Tareas Pendientes: {len(analyzer.pending_tasks.get('tasks', []))} | {shard_status_str}"
    )
    return Panel(summary_text, title="[b cyan]Dashboard de Salud Elasticsearch[/b cyan]", border_style="cyan")

def _render_node_health_table(analyzer, previous_df=None) -> Panel:
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

    if previous_df is None or previous_df.empty:
        merged_df = analyzer.nodes_df.copy()
    else:
        merged_df = analyzer.nodes_df.merge(previous_df, on="node_name", how="left", suffixes=("", "_prev"))

    for tier, group in merged_df.groupby('tier'):
        table.add_section()
        for _, row in group.iterrows():
            cpu_str = _format_metric(row['cpu_percent'], row.get('cpu_percent_prev'), spike_threshold=20)
            heap_str = _format_metric(row['heap_percent'], row.get('heap_percent_prev'), spike_threshold=10)
            heap_old_str = _format_metric(row['heap_old_gen_percent'], row.get('heap_old_gen_percent_prev'), spike_threshold=15)
            
            gc_count_str = _format_metric(row['gc_count'], row.get('gc_count_prev'), spike_threshold=GC_COUNT_SPIKE_THRESHOLD)
            gc_time_str = _format_metric(row['gc_time_ms'], row.get('gc_time_ms_prev'), spike_threshold=GC_TIME_SPIKE_THRESHOLD)
            gc_str = f"{gc_count_str}/{gc_time_str}"
            
            rejections_str = _format_metric(row['rejections'], row.get('rejections_prev'), spike_threshold=0)

            table.add_row(f"[{'yellow' if 'hot' in tier else 'blue'}]{tier}[/]", row['node_name'], cpu_str, heap_str, heap_old_str, gc_str, rejections_str)
        
    return Panel(table, border_style="green")
    
def _render_top_n_rankings(analyzer, previous_df=None, time_delta=None) -> Panel:
    if analyzer.indices_df.empty:
        return Panel("[yellow]No hay datos de índices disponibles.[/yellow]", title="[b cyan]Rankings de Rendimiento de Índices[/b cyan]", border_style="yellow")
    
    current_indices = analyzer.indices_df.copy()
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
    for _, r in analyzer.top_heap_indices.iterrows():
        breakdown = f"{r['memory_segments_mb']:.1f}/{r['memory_cache_mb']:.1f}/{r['memory_fielddata_mb']:.1f}"
        heap_table.add_row(r['index'], f"{r['heap_usage_mb']:.1f}", breakdown)

    return Panel(Columns([writers_table, searchers_table, heap_table]), title="[b cyan]Rankings de Rendimiento de Índices[/b cyan]", border_style="cyan")

def _render_actionable_suggestions(analyzer) -> Panel:
    suggestions = []
    if analyzer.nodes_df.empty:
        return Panel("[yellow]Esperando datos para generar sugerencias...[/yellow]", border_style="yellow")

    for _, node in analyzer.nodes_df.iterrows():
        if node['heap_old_gen_percent'] > HEAP_OLD_GEN_THRESHOLD:
            suggestion = f"🚨 [bold]Heap Old Gen Alto en '{node['node_name']}'[/bold]: Riesgo de pausas largas de GC."
            if not analyzer.top_heap_indices.empty:
                top_consumer = analyzer.top_heap_indices.iloc[0]
                suggestion += f" El índice [cyan]'{top_consumer['index']}'[/cyan] es el que más memoria consume ({top_consumer['heap_usage_mb']:.1f} MB)."
            suggestions.append(suggestion)

        if node['cpu_percent'] > CPU_USAGE_THRESHOLD:
            suggestions.append(f"🔥 [bold]CPU Alta en '{node['node_name']}'[/bold]: Revisa consultas costosas o picos de ingesta. Usa el análisis de tareas lentas.")
        
        if node['gc_time_ms'] > GC_TIME_THRESHOLD:
            suggestions.append(f"🗑️ [bold]GC Excesivo en '{node['node_name']}'[/bold]: El nodo está pausando para limpiar memoria. Revisa el uso de heap.")
        
        if node['rejections'] > 0:
            suggestion = f"🚦 [bold]Rechazos de Escritura en '{node['node_name']}'[/bold]: El nodo no puede procesar la carga de ingesta."
            if not analyzer.indices_df.empty and 'write_rate' in analyzer.indices_df.columns:
                top_writer = analyzer.indices_df.sort_values('write_rate', ascending=False).iloc[0]
                if top_writer['write_rate'] > 0:
                    suggestion += f" La alta tasa de [cyan]'{top_writer['index']}'[/cyan] podría ser la causa. Considera escalar nodos o revisar shards."
            suggestions.append(suggestion)

        if node['breakers_tripped'] > 0:
            suggestions.append(f"🛑 [bold red]¡CIRCUIT BREAKER ACTIVADO en '{node['node_name']}'![/bold red] Operación rechazada por exceso de memoria. ¡CRÍTICO!")
    
    if analyzer.cluster_health.get('unassigned_shards', 0) > 0:
        suggestions.append(f"💔 [bold]Shards No Asignados Detectados[/bold]: Usa la API `_cluster/allocation/explain` para diagnosticar la causa.")

    if not suggestions:
        return Panel("[bold green]✅ ¡Todo en orden! No se detectaron problemas críticos.[/bold green]", title="[bold cyan]Acciones Recomendadas (Motor Inteligente)[/bold cyan]", border_style="green")
    
    return Panel("\n".join(f"- {s}" for s in suggestions), title="[bold red]Acciones Recomendadas (Motor Inteligente)[/bold red]", border_style="red")

def render_dashboard_layout(analyzer) -> Layout:
    layout = Layout(name="root")
    layout.split(
        Layout(name="header", size=4),
        Layout(ratio=1, name="main"),
        Layout(size=8, name="footer"),
    )
    layout["main"].split_row(Layout(name="side", ratio=2), Layout(name="body", ratio=3))
    
    layout["header"].update(_render_header(analyzer))
    layout["side"].update(_render_node_health_table(analyzer, analyzer.previous_nodes_df))
    layout["body"].update(_render_top_n_rankings(analyzer, analyzer.previous_indices_df, analyzer.last_fetch_time))
    layout["footer"].update(_render_actionable_suggestions(analyzer))
    
    return layout

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

def render_actionable_suggestions_markdown(analyzer):
    """Genera y muestra las sugerencias en formato Markdown para el modo --report."""
    console.print(f"# Reporte de Salud del Cluster: {analyzer.cluster_stats.get('cluster_name', 'N/A')}")
    console.print(f"**Fecha:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    console.print(f"**Estado:** {analyzer.cluster_health.get('status', 'N/A')}")
    console.print("## 💡 Sugerencias y Alertas (Motor Inteligente)")
    
    suggestions_panel = _render_actionable_suggestions(analyzer)
    
    # Extraer el texto del panel y formatearlo como una lista Markdown
    if hasattr(suggestions_panel.renderable, 'renderables'):
        suggestions_text = suggestions_panel.renderable.renderables[0].text
        # Usar rich para eliminar las etiquetas de formato
        plain_text = Text.from_markup(suggestions_text).plain
        for line in plain_text.split('\n'):
            console.print(f"* {line.lstrip('- ')}")
    else: # Fallback para el caso de "Todo en orden"
        console.print(f"* {suggestions_panel.renderable.plain}")

def render_historical_report(analyzer, window_seconds: int, window_str: str):
    """Genera un reporte único comparando con un snapshot histórico."""
    console.print(f"\n[yellow]Esta función de reporte histórico aún no está implementada.[/yellow]")