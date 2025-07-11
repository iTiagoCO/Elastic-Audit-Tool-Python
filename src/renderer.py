# src/renderer.py
import pandas as pd
from datetime import datetime
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.columns import Columns
from rich.layout import Layout
from rich.markdown import Markdown
from datetime import datetime
from rich.console import Console
from rich.table import Table
from rich.text import Text

from .config import (
    HEAP_OLD_GEN_THRESHOLD, CPU_USAGE_THRESHOLD, GC_TIME_THRESHOLD,
    GC_COUNT_SPIKE_THRESHOLD, GC_TIME_SPIKE_THRESHOLD, REFRESH_INTERVAL
)

console = Console()

# --- Funciones de formato de métricas (sin cambios) ---
def _format_metric(current_val, prev_val, spike_threshold, higher_is_worse=True):
    if pd.isna(current_val): current_val = 0
    if pd.isna(prev_val): prev_val = current_val
    
    delta = current_val - prev_val
    spike_icon = "🔥" if abs(delta) > spike_threshold else ""
    
    if delta == 0: arrow, color = " ", "white"
    elif delta > 0: arrow, color = "🔼", "red" if higher_is_worse else "green"
    else: arrow, color = "🔽", "green" if higher_is_worse else "red"
    
    val_str = f"{int(current_val)}" if current_val == int(current_val) else f"{current_val:.1f}"
    return f"[{color}]{spike_icon}{arrow} {val_str}[/{color}]"

def format_delta(current, previous):
    if pd.isna(previous): return f"{current:.1f}" if isinstance(current, float) else str(current)
    if current > previous: return f"[red]🔼 {current:.1f}[/red]" if isinstance(current, float) else f"[red]🔼 {current}[/red]"
    elif current < previous: return f"[green]🔽 {current:.1f}[/green]" if isinstance(current, float) else f"[green]🔽 {current}[/green]"
    return f"{current:.1f}" if isinstance(current, float) else str(current)

# --- Componentes Internos de Renderizado ---

def _render_header(data: dict) -> Panel:
    health = data.get('cluster_health', {}); stats = data.get('cluster_stats', {}); pending = data.get('pending_tasks', {})
    status = health.get('status', 'N/A').upper()
    status_color = {"GREEN": "green", "YELLOW": "yellow", "RED": "red"}.get(status, "white")
    heap_max = stats.get('nodes', {}).get('jvm', {}).get('mem', {}).get('heap_max_in_bytes', 1)
    heap_used = stats.get('nodes', {}).get('jvm', {}).get('mem', {}).get('heap_used_in_bytes', 0)
    heap_pct = (heap_used / heap_max * 100) if heap_max > 0 else 0
    shards_status = f"Initializing: [yellow]{health.get('initializing_shards', 0)}[/yellow] | Relocating: [yellow]{health.get('relocating_shards', 0)}[/yellow] | Unassigned: [bold red]{health.get('unassigned_shards', 0)}[/bold red]"
    summary = (f"Cluster: [b]{stats.get('cluster_name', 'N/A')}[/b] | Status: [b {status_color}]{status}[/b {status_color}] | "
               f"Última Actualización: {datetime.now().strftime('%H:%M:%S')}\n"
               f"Heap Total: {heap_pct:.1f}% | Tareas Pendientes: {len(pending.get('tasks', []))} | {shards_status}")
    return Panel(summary, title="[b cyan]Dashboard de Salud Elasticsearch[/b cyan]", border_style="cyan")

def _render_node_health_table(data: dict) -> Panel:
    nodes_df = pd.DataFrame(data.get('nodes_df', [])); prev_df = pd.DataFrame(data.get('previous_nodes_df', []))
    if nodes_df.empty: return Panel("[yellow]Esperando datos de nodos...[/yellow]", border_style="yellow")
    
    table = Table(title="[b]Salud de Nodos por Tier[/b]", expand=True)
    cols = ["Tier", "Nodo", "CPU%", "Heap%", "Heap Old%", "GC (c/t ms)", "Rechazos"]
    styles = ["magenta", "cyan", "white", "white", "white", "white", "white"]
    justifies = ["left", "left", "right", "right", "right", "right", "right"]
    for col, style, justify in zip(cols, styles, justifies): table.add_column(col, style=style, justify=justify)

    merged = nodes_df.merge(prev_df, on="node_name", how="left", suffixes=("", "_prev")) if not prev_df.empty else nodes_df
    for tier, group in merged.groupby('tier'):
        table.add_section()
        for _, row in group.sort_values(by="node_name").iterrows():
            gc_str = f"{_format_metric(row.get('gc_count', 0), row.get('gc_count_prev'), GC_COUNT_SPIKE_THRESHOLD)}/{_format_metric(row.get('gc_time_ms', 0), row.get('gc_time_ms_prev'), GC_TIME_SPIKE_THRESHOLD)}"
            table.add_row(f"[{'yellow' if 'hot' in tier else 'blue'}]{tier}[/]", row['node_name'],
                          _format_metric(row['cpu_percent'], row.get('cpu_percent_prev'), 20),
                          _format_metric(row['heap_percent'], row.get('heap_percent_prev'), 10),
                          _format_metric(row['heap_old_gen_percent'], row.get('heap_old_gen_percent_prev'), 15),
                          gc_str, _format_metric(row.get('rejections', 0), row.get('rejections_prev'), 0))
    return Panel(table, border_style="green")

def _render_top_n_rankings(data: dict) -> Panel:
    indices_df = pd.DataFrame(data.get('indices_df', [])); prev_df = pd.DataFrame(data.get('previous_indices_df', []))
    top_heap = data.get('top_heap_indices', [])
    if indices_df.empty: return Panel("[yellow]No hay datos de índices.[/yellow]", title="[b cyan]Rankings[/b cyan]", border_style="yellow")
    
    current = indices_df.copy()
    if not prev_df.empty and data.get('last_fetch_time'):
        delta = REFRESH_INTERVAL
        merged = pd.merge(current, prev_df[['index', 'indexing_total', 'search_total']], on='index', how='left', suffixes=('', '_prev'))
        merged.fillna({'indexing_total_prev': merged['indexing_total'], 'search_total_prev': merged['search_total']}, inplace=True)
        current['write_rate'] = (merged['indexing_total'] - merged['indexing_total_prev']) / delta
        current['search_rate'] = (merged['search_total'] - merged['search_total_prev']) / delta
    else: current['write_rate'], current['search_rate'] = 0.0, 0.0

    writers_table = Table(title="[b]Top 5 - Escritura[/b]", expand=True); writers_table.add_column("Índice"); writers_table.add_column("docs/s", justify="right")
    for _, r in current.sort_values('write_rate', ascending=False).head(5).iterrows(): writers_table.add_row(r['index'], f"{r.get('write_rate', 0):.1f}")
    searchers_table = Table(title="[b]Top 5 - Búsqueda[/b]", expand=True); searchers_table.add_column("Índice"); searchers_table.add_column("req/s", justify="right")
    for _, r in current.sort_values('search_rate', ascending=False).head(5).iterrows(): searchers_table.add_row(r['index'], f"{r.get('search_rate', 0):.1f}")
    heap_table = Table(title="[b]Top 5 - Uso de Heap[/b]", expand=True); heap_table.add_column("Índice"); heap_table.add_column("Total (MB)", justify="right"); heap_table.add_column("Seg/Cache/Field", justify="right")
    for r in top_heap: heap_table.add_row(r['index'], f"{r['heap_usage_mb']:.1f}", f"{r['memory_segments_mb']:.1f}/{r['memory_cache_mb']:.1f}/{r['memory_fielddata_mb']:.1f}")
    return Panel(Columns([writers_table, searchers_table, heap_table]), title="[b cyan]Rankings de Rendimiento de Índices[/b cyan]", border_style="cyan")

def _render_actionable_suggestions(data: dict) -> Panel:
    nodes_df, indices_df, top_heap, health = pd.DataFrame(data.get('nodes_df', [])), pd.DataFrame(data.get('indices_df', [])), data.get('top_heap_indices', []), data.get('cluster_health', {})
    if nodes_df.empty: return Panel("[yellow]Esperando datos...[/yellow]", border_style="yellow")
    suggestions = []
    for _, node in nodes_df.iterrows():
        if node['heap_old_gen_percent'] > HEAP_OLD_GEN_THRESHOLD:
            s = f"🚨 [bold]Heap Old Gen Alto en '{node['node_name']}'[/bold]: Riesgo de pausas largas de GC."
            if top_heap: s += f" El índice [cyan]'{top_heap[0]['index']}'[/cyan] es el que más consume ({top_heap[0]['heap_usage_mb']:.1f} MB)."
            suggestions.append(s)
        if node['cpu_percent'] > CPU_USAGE_THRESHOLD: suggestions.append(f"🔥 [bold]CPU Alta en '{node['node_name']}'[/bold]: Revisa consultas costosas o picos de ingesta.")
        if node['gc_time_ms'] > GC_TIME_THRESHOLD: suggestions.append(f"🗑️ [bold]GC Excesivo en '{node['node_name']}'[/bold]: El nodo está pausando para limpiar memoria.")
        if node['rejections'] > 0:
            s = f"🚦 [bold]Rechazos en '{node['node_name']}'[/bold]: El nodo no puede procesar la carga."
            if not indices_df.empty and 'write_rate' in indices_df.columns:
                top_writer = indices_df.sort_values('write_rate', ascending=False).iloc[0]
                if top_writer['write_rate'] > 0: s += f" La tasa de [cyan]'{top_writer['index']}'[/cyan] podría ser la causa."
            suggestions.append(s)
        if node['breakers_tripped'] > 0: suggestions.append(f"🛑 [bold red]¡CIRCUIT BREAKER ACTIVADO en '{node['node_name']}'![/bold red] CRÍTICO.")
    if health.get('unassigned_shards', 0) > 0: suggestions.append("💔 [bold]Shards No Asignados Detectados[/bold]: Usa `_cluster/allocation/explain`.")
    if not suggestions: return Panel("[bold green]✅ ¡Todo en orden! No se detectaron problemas críticos.[/bold green]", title="[bold cyan]Acciones Recomendadas[/bold cyan]", border_style="green")
    return Panel("\n".join(f"- {s}" for s in suggestions), title="[bold red]Acciones Recomendadas[/bold red]", border_style="red")

def _render_thread_pool_panel(node_stats: dict, prev_node_stats: dict) -> Panel:
    # (Código sin cambios, ya era correcto)
    tp_table = Table(title="[b]🏊 Thread Pools[/b]", expand=True)
    tp_table.add_column("Pool", style="cyan"); tp_table.add_column("Activas", justify="right"); tp_table.add_column("En Cola", justify="right"); tp_table.add_column("Rechazadas", justify="right")
    for name, stats in sorted(node_stats.get('thread_pool', {}).items()):
        if stats.get('rejected', 0) > 0 or stats.get('queue', 0) > 0 or stats.get('active', 0) > 0:
            prev = prev_node_stats.get('thread_pool', {}).get(name, {})
            tp_table.add_row(name, format_delta(stats.get('active', 0), prev.get('active', 0)),
                             format_delta(stats.get('queue', 0), prev.get('queue', 0)),
                             f"[red]{format_delta(stats.get('rejected', 0), prev.get('rejected', 0))}[/red]")
    return Panel(tp_table)

def _render_breaker_panel(node_stats: dict, prev_node_stats: dict) -> Panel:
    # (Código sin cambios, ya era correcto)
    cb_table = Table(title="[b]🛑 Circuit Breakers[/b]", expand=True)
    cb_table.add_column("Breaker", style="cyan"); cb_table.add_column("Límite (MB)", justify="right"); cb_table.add_column("Usado (MB)", justify="right"); cb_table.add_column("Tripped", justify="right")
    for name, stats in sorted(node_stats.get('breaker', {}).items()):
        tripped = stats.get('tripped', 0); prev = prev_node_stats.get('breaker', {}).get(name, {})
        cb_table.add_row(name, f"{stats.get('limit_size_in_bytes', 0) / 1e6:.1f}",
                         format_delta(stats.get('estimated_size_in_bytes', 0) / 1e6, prev.get('estimated_size_in_bytes', 0) / 1e6),
                         f"[red]{format_delta(tripped, prev.get('tripped', 0))}[/red]" if tripped > 0 else format_delta(tripped, prev.get('tripped', 0)))
    return Panel(cb_table)

# --- Renderers Públicos ---

def render_live_dashboard(data: dict) -> Layout:
    layout = Layout(name="root"); layout.split(Layout(name="header", size=4), Layout(ratio=1, name="main"), Layout(size=8, name="footer"))
    layout["main"].split_row(Layout(name="side", ratio=2), Layout(name="body", ratio=3))
    layout["header"].update(_render_header(data)); layout["side"].update(_render_node_health_table(data))
    layout["body"].update(_render_top_n_rankings(data)); layout["footer"].update(_render_actionable_suggestions(data))
    return layout

def render_deep_dive(data: dict) -> Layout:
    layout = Layout(name="deep_dive_root"); node_panels = []
    nodes_df = pd.DataFrame(data.get('nodes_df', []))
    if nodes_df.empty: return Panel("[yellow]Esperando datos de nodos...[/yellow]")
    for _, row in nodes_df.iterrows():
        node_id = row['node_id']; node_name = row['node_name']
        node_stats = data.get('node_stats_raw', {}).get('nodes', {}).get(node_id, {})
        prev_stats = data.get('previous_node_stats_raw', {}).get('nodes', {}).get(node_id, {})
        node_layout = Layout(name=node_name); node_layout.split_row(_render_thread_pool_panel(node_stats, prev_stats), _render_breaker_panel(node_stats, prev_stats))
        node_panels.append(Panel(node_layout, title=f"[b cyan]Nodo: {node_name}[/b cyan]", border_style="magenta"))
    layout.split_column(*node_panels); return layout

def render_shard_distribution(data: dict, group_by: str, sort_by: str) -> Panel:
    summary = data.get('summary', [])
    if not summary: return Panel("[red]No hay datos de shards disponibles.[/red]")
    table = Table(title=f"Distribución de Shards por {group_by.capitalize()} (ordenado por {sort_by})")
    cols = [group_by.capitalize(), "Total Shards", "Primarios", "Réplicas", "Tamaño (GB)", "Nodos"]
    for col in cols: table.add_column(col)
    for row in summary: table.add_row(row[group_by], str(row['total_shards']), str(row['primaries']), str(row['replicas']), f"{row['total_gb']:.2f}", str(row['nodes_involved']))
    return Panel(table)

# --- Renderers para Análisis Estáticos (Añadidos para corregir el error) ---

def render_node_load_correlation(data: dict):
    node_loads = data.get('node_loads')
    if not node_loads: console.print(f"[yellow]No hay datos de carga de nodos. Causa: {data.get('error', 'desconocida')}[/yellow]"); return
    table = Table(title="Correlación de Carga de Nodos y Actividad de Shards")
    for col in node_loads[0].keys(): table.add_column(col, justify="right", style="cyan" if col == 'Nodo' else "white")
    for row in node_loads: table.add_row(row['Nodo'], f"{row['CPU %']:.0f}", f"{row['Heap %']:.0f}", str(row['Primarios']), str(row['Total Shards']), f"[green]{row['Carga Escritura (docs/s)']:.1f}[/green]", f"[yellow]{row['Carga Búsqueda (req/s)']:.1f}[/yellow]")
    console.print(table)

def render_shard_imbalance(data: dict):
    patterns = data.get('imbalanced_patterns', [])
    if not patterns: console.print("[green]✅ No se detectaron desbalances significativos de shards primarios.[/green]"); return
    console.print(f"\nSe encontraron [bold cyan]{len(patterns)}[/bold cyan] patrones de índice con desbalance.\n")
    table = Table(title="Distribución y Actividad de Shards Primarios por Patrón y Nodo")
    cols = ["Patrón", "Desbalance (StdDev)", "Escrituras/s", "Búsquedas/s", "Nodo Afectado", "N° Shards"]
    for col in cols: table.add_column(col)
    for p_data in patterns:
        p_info, nodes = p_data['pattern_info'], p_data['nodes']
        table.add_section()
        for i, node in enumerate(nodes):
            style = "on red" if node['shard_count'] == max(n['shard_count'] for n in nodes) and len(nodes) > 1 else ""
            if i == 0: table.add_row(p_info['pattern'], f"{p_info['std_dev']:.2f}", f"{p_info['write_rate']:.1f}", f"{p_info['search_rate']:.1f}", Text(node['node'], style=style), Text(str(node['shard_count']), style=style))
            else: table.add_row("", "", "", "", Text(node['node'], style=style), Text(str(node['shard_count']), style=style))
    console.print(table)

def render_slow_tasks(data: dict):
    if not data.get('tasks'): console.print(f"[green]✅ No se detectaron tareas lentas por encima de {data.get('threshold_minutes', 5)} minutos.[/green]"); return
    table = Table(title=f"Tareas de Búsqueda Lentas (Más de {data['threshold_minutes']} minutos)")
    table.add_column("Nodo", style="cyan"); table.add_column("Tiempo (min)", justify="right", style="yellow"); table.add_column("Descripción", style="white", max_width=80, overflow="fold")
    for task in data['tasks']: table.add_row(task['node'], f"{task['time_min']:.2f}", task['description'])
    console.print(table)

def render_index_templates(data: dict):
    if not data.get('templates'): console.print("[yellow]No se encontraron plantillas de índice.[/yellow]"); return
    table = Table(title="Análisis de Plantillas de Índice y su Impacto")
    cols = ["Plantilla", "Índices", "Docs Totales", "Tamaño Total", "Diagnóstico"]; 
    for col in cols: table.add_column(col)
    for t in data['templates']: table.add_row(t['name'], str(t['index_count']), f"{t['total_docs']:,}", t['total_size_str'], t['diagnostics_str'].replace("[", "\\["))
    console.print(table)

def render_mapping_explosion(data: dict):
    indices = data.get('indices', [])
    if not indices: console.print("[green]✅ No se detectaron índices con riesgo de explosión de mapeo.[/green]"); return
    table = Table(title="Resultados del Análisis de Mapeo de Campos")
    table.add_column("Índice", style="cyan"); table.add_column("N° de Campos", justify="right"); table.add_column("Diagnóstico")
    for i in indices:
        style = "bold red" if "RIESGO" in i['diagnostic'] else "yellow"
        table.add_row(i['index_name'], f"[{style}]{i['field_count']}[/{style}]", i['diagnostic'])
    console.print(table)

def render_dusty_shards(data: dict):
    if not data.get('empty_shards') and not data.get('dusty_shards'): console.print("[green]✅ No se detectaron shards vacíos ni 'polvo de shards'.[/green]"); return
    if data['empty_shards']:
        empty_table = Table(title="Shards Vacíos (docs=0)"); empty_table.add_column("Índice"); empty_table.add_column("Shard"); empty_table.add_column("Nodo")
        for row in data['empty_shards']: empty_table.add_row(row['index'], str(row['shard']), row['node'])
        console.print(empty_table)
    if data['dusty_shards']:
        dusty_table = Table(title=f"'Polvo de Shards' (< {data['threshold_mb']} MB)"); dusty_table.add_column("Índice"); dusty_table.add_column("Tamaño (MB)"); dusty_table.add_column("Docs"); dusty_table.add_column("Nodo")
        for row in data['dusty_shards']: dusty_table.add_row(row['index'], f"{row['store']:.1f}", str(int(row['docs'])), row['node'])
        console.print(dusty_table)

def render_configuration_drift(data: dict):
    drifts = data.get('drifts', [])
    if not drifts: console.print("[green]✅ No se detectaron derivas en la configuración crítica.[/green]"); return
    formatted = [f"- [bold red]{d}[/bold red]" if "Crítica" in d else f"- [yellow]{d}[/yellow]" for d in drifts]
    console.print(Panel("\n".join(formatted), title="[yellow]Resultados del Análisis de Deriva[/yellow]", border_style="yellow"))

def render_causality_chain(data: dict):
    reports = data.get('reports', [])
    if not reports: console.print("[green]✅ No se detectaron nodos con presión de memoria crítica (Old Gen).[/green]"); return
    console.print(f"Se detectaron [bold red]{len(reports)}[/bold red] nodos con alta presión de memoria.\n")
    for report in reports:
        console.print(Panel("\n".join(report['report_lines']), title=f"[yellow]Cadena de Causalidad - {report['node_name']}[/yellow]", border_style="yellow"))

def render_shard_toxicity(data: dict):
    tenants = data.get('toxic_tenants', [])
    if not tenants: console.print(f"[green]✅ {data.get('message', 'No se encontraron inquilinos tóxicos.')}[/green]"); return
    table = Table(title="Resultados del Análisis de Inquilinos Tóxicos")
    cols = ["Nodo Afectado", "CPU%", "Inquilino (ID Extraído)", "Tiempo Tarea (s)", "Descripción de la Consulta"]
    for col in cols: table.add_column(col)
    for t in tenants: table.add_row(t['node_name'], f"{t['cpu']:.0f}%", t['tenant_id'], f"{t['running_time_s']:.1f}", t['description'])
    console.print(table)

def render_markdown_report(data: dict):
    """
    Genera y muestra el reporte en formato Markdown en la consola.
    """
    console.print(f"# Reporte de Salud del Cluster: {data.get('cluster_name', 'N/A')}")
    console.print(f"**Fecha:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    console.print(f"**Estado:** {data.get('cluster_status', 'N/A').upper()}")
    console.print("\n## 💡 Sugerencias y Alertas\n")
    
    suggestions = data.get('suggestions', [])
    if not suggestions:
        console.print("* ✅ ¡Todo en orden! No se detectaron problemas críticos.")
    else:
        for suggestion in suggestions:
            # Añadimos emojis para mejorar la legibilidad del reporte
            if "Heap" in suggestion: suggestion = f"🚨 {suggestion}"
            elif "CPU" in suggestion: suggestion = f"🔥 {suggestion}"
            elif "GC" in suggestion: suggestion = f"🗑️ {suggestion}"
            elif "Rechazos" in suggestion: suggestion = f"🚦 {suggestion}"
            elif "CIRCUIT BREAKER" in suggestion: suggestion = f"🛑 {suggestion}"
            elif "Shards No Asignados" in suggestion: suggestion = f"💔 {suggestion}"
            console.print(f"* {suggestion}")