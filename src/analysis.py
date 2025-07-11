# src/analysis.py
import time
import re
import pandas as pd
import json
import fnmatch
from rich.console import Console
from rich.live import Live
from rich.layout import Layout
from rich.panel import Panel
from rich.columns import Columns
from rich.rule import Rule
from rich.table import Table
from rich.prompt import Prompt
from rich.markdown import Markdown
from rich.text import Text

from .analyzer import ClusterAnalyzer
from .renderer import (
    render_dashboard_layout, render_thread_pool_panel, render_breaker_panel,
    format_delta
)

from .config import (
    REFRESH_INTERVAL, LONG_RUNNING_TASK_MINUTES,
    HIGH_SHARD_COUNT_TEMPLATE_THRESHOLD, DUSTY_SHARD_MB_THRESHOLD,
    HEAP_OLD_GEN_THRESHOLD, GC_TIME_THRESHOLD, CPU_USAGE_THRESHOLD
)

console = Console()

# --- Funciones de Control y Flujo de Análisis ---

def run_live_dashboard(analyzer: ClusterAnalyzer):
    """Ejecuta el dashboard principal en modo de actualización en vivo."""
    try:
        with Live(console=console, screen=True, auto_refresh=False) as live:
            while True:
                analyzer.fetch_all_data()
                dashboard = render_dashboard_layout(analyzer)
                live.update(dashboard, refresh=True)
                time.sleep(REFRESH_INTERVAL)
    except KeyboardInterrupt:
        console.print("\n[bold]Volviendo al menú principal...[/bold]")

def analyze_node_deep_dive(analyzer: ClusterAnalyzer):
    """Ejecuta un dashboard en vivo para todos los nodos, mostrando un desglose detallado."""
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

def analyze_shard_distribution_interactive(analyzer: ClusterAnalyzer):
    """Muestra un dashboard interactivo de distribución de shards."""
    if analyzer.shards_df.empty:
        analyzer.fetch_all_data()
    
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

# --- Funciones de Análisis Experto ---

def analyze_node_load_correlation(analyzer: ClusterAnalyzer):
    """Correlaciona la carga de CPU y memoria de un nodo con la carga de escritura/lectura generada por sus shards."""
    console.print(Rule("[bold]Análisis de Carga de Nodos por Actividad de Shards[/bold]"))
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

    merged_indices = pd.merge(indices_df, previous_indices_df[['index', 'indexing_total', 'search_total']], on='index', how='left', suffixes=('', '_prev'))
    merged_indices['indexing_total_prev'] = merged_indices['indexing_total_prev'].fillna(merged_indices['indexing_total'])
    merged_indices['search_total_prev'] = merged_indices['search_total_prev'].fillna(merged_indices['search_total'])
    time_delta = REFRESH_INTERVAL if REFRESH_INTERVAL > 0 else 1
    indices_df['write_rate'] = (merged_indices['indexing_total'] - merged_indices['indexing_total_prev']) / time_delta
    indices_df['search_rate'] = (merged_indices['search_total'] - merged_indices['search_total_prev']) / time_delta

    shard_activity_df = pd.merge(shards_df, indices_df[['index', 'write_rate', 'search_rate']], on='index', how='left').fillna(0)
    
    node_loads = []
    for _, node_row in nodes_df.iterrows():
        node_name = node_row['node_name']
        shards_on_node = shard_activity_df[shard_activity_df['node'] == node_name]
        primary_shards = shards_on_node[shards_on_node['prirep'] == 'p']
        write_load = primary_shards['write_rate'].sum()
        search_load = shards_on_node['search_rate'].sum()
        node_loads.append({
            'Nodo': node_name, 'CPU %': node_row['cpu_percent'], 'Heap %': node_row['heap_percent'],
            'Primarios': len(primary_shards), 'Total Shards': len(shards_on_node),
            'Carga Escritura (docs/s)': write_load, 'Carga Búsqueda (req/s)': search_load
        })
    
    load_df = pd.DataFrame(node_loads).sort_values(by='CPU %', ascending=False)
    table = Table(title="Correlación de Carga de Nodos y Actividad de Shards")
    for col in load_df.columns:
        table.add_column(col, justify="right", style="cyan" if col == 'Nodo' else "white")
    
    for _, row in load_df.iterrows():
        table.add_row(
            row['Nodo'], f"{row['CPU %']:.0f}", f"{row['Heap %']:.0f}", str(row['Primarios']),
            str(row['Total Shards']), f"[green]{row['Carga Escritura (docs/s)']:.1f}[/green]",
            f"[yellow]{row['Carga Búsqueda (req/s)']:.1f}[/yellow]"
        )
    
    console.print(table)
    console.print("\n[italic]Esta tabla te ayuda a ver si los nodos con alta CPU/Heap son los que realmente procesan más escrituras o búsquedas.[/italic]")
    Prompt.ask("\n[bold]Presiona Enter para volver al menú...[/bold]")

def analyze_node_index_correlation(analyzer: ClusterAnalyzer):
    """Analiza y muestra el desbalance de shards primarios, enriquecido con métricas de actividad."""
    console.print(Rule("[bold]Análisis de Desbalance y Actividad de Shards (Vista Agrupada)[/bold]"))
    console.print("[yellow]Capturando métricas para calcular tasas de actividad...[/yellow]")
    analyzer.fetch_all_data()
    time.sleep(REFRESH_INTERVAL)
    analyzer.fetch_all_data()

    shards_df, indices_df, previous_indices_df = analyzer.shards_df.copy(), analyzer.indices_df.copy(), analyzer.previous_indices_df.copy()

    if any(df.empty for df in [shards_df, indices_df, previous_indices_df]):
        console.print("[red]No se pudieron obtener suficientes datos para el análisis de actividad.[/red]")
        return
    
    merged_df = pd.merge(indices_df, previous_indices_df[['index', 'indexing_total', 'search_total']], on='index', how='left', suffixes=('', '_prev'))
    merged_df['indexing_total_prev'] = merged_df['indexing_total_prev'].fillna(merged_df['indexing_total'])
    merged_df['search_total_prev'] = merged_df['search_total_prev'].fillna(merged_df['search_total'])
    time_delta = REFRESH_INTERVAL if REFRESH_INTERVAL > 0 else 1
    indices_df['write_rate'] = (merged_df['indexing_total'] - merged_df['indexing_total_prev']) / time_delta
    indices_df['search_rate'] = (merged_df['search_total'] - merged_df['search_total_prev']) / time_delta
    
    primary_shards = shards_df[shards_df['prirep'] == 'p'].copy()
    primary_shards['pattern'] = primary_shards['index'].apply(lambda x: re.sub(r'\d{4}[-.]\d{2}[-.]\d{2}|-\d{6}', '-*', x))
    shard_counts = primary_shards.groupby(['pattern', 'node']).size().reset_index(name='shard_count')
    imbalance_stats = shard_counts.groupby('pattern')['shard_count'].agg(std_dev='std', node_count='count').fillna(0)
    
    indices_df['pattern'] = indices_df['index'].apply(lambda x: re.sub(r'\d{4}[-.]\d{2}[-.]\d{2}|-\d{6}', '-*', x))
    pattern_activity = indices_df.groupby('pattern')[['write_rate', 'search_rate']].sum().reset_index()

    imbalanced_patterns = pd.merge(imbalance_stats[imbalance_stats['node_count'] > 1].reset_index(), pattern_activity, on='pattern', how='left').fillna(0)
    imbalanced_patterns = imbalanced_patterns[imbalanced_patterns['std_dev'] > 0].sort_values(by='std_dev', ascending=False)
    
    if imbalanced_patterns.empty:
        console.print("[green]✅ No se detectaron desbalances significativos de shards primarios.[/green]")
        Prompt.ask("\n[bold]Presiona Enter para volver al menú...[/bold]")
        return

    console.print(f"\nSe encontraron [bold cyan]{len(imbalanced_patterns)}[/bold cyan] patrones de índice con desbalance.\n")
    table = Table(title="Distribución y Actividad de Shards Primarios por Patrón y Nodo")
    table.add_column("Patrón", style="bold cyan", no_wrap=True, max_width=50)
    table.add_column("Desbalance (StdDev)", style="bold red", justify="right")
    table.add_column("Escrituras/s", style="green", justify="right")
    table.add_column("Búsquedas/s", style="yellow", justify="right")
    table.add_column("Nodo Afectado", style="magenta", no_wrap=True)
    table.add_column("N° Shards", style="white", justify="right")

    for _, pattern_row in imbalanced_patterns.iterrows():
        pattern, std_dev, write_rate, search_rate = pattern_row['pattern'], pattern_row['std_dev'], pattern_row.get('write_rate', 0), pattern_row.get('search_rate', 0)
        nodes_for_pattern = shard_counts[shard_counts['pattern'] == pattern].sort_values(by='shard_count', ascending=False)
        table.add_section()
        for i, node_row in enumerate(nodes_for_pattern.iterrows()):
            node_name, shard_count = node_row[1]['node'], node_row[1]['shard_count']
            style = "on red" if shard_count == nodes_for_pattern['shard_count'].max() and len(nodes_for_pattern) > 1 else ""
            if i == 0:
                table.add_row(pattern, f"{std_dev:.2f}", f"{write_rate:.1f}", f"{search_rate:.1f}", Text(node_name, style=style), Text(str(shard_count), style=style))
            else:
                table.add_row("", "", "", "", Text(node_name, style=style), Text(str(shard_count), style=style))
    console.print(table)
    
    info_text = "..." # El texto de la guía de diagnóstico se puede mantener aquí.
    console.print(Panel(Markdown(info_text), title="[bold cyan]Guía de Diagnóstico de Desbalance[/bold cyan]", border_style="cyan"))
    Prompt.ask("\n[bold]Presiona Enter para volver al menú...[/bold]")

def analyze_slow_tasks(analyzer: ClusterAnalyzer):
    """Identifica tareas de búsqueda lentas que se están ejecutando en el clúster."""
    console.print(Rule("[bold]Identificación de Tareas de Búsqueda Lentas[/bold]"))
    
    tasks_data = analyzer.client.get("_tasks", params={'actions': '*search*', 'detailed': 'true'})
    if not tasks_data or 'nodes' not in tasks_data:
        console.print("[red]No se pudo obtener información de tareas.[/red]")
        return

    slow_tasks = [
        {'node': node_info.get('name'), 'time_min': task_info.get('running_time_in_nanos', 0) / 60e9, 'description': task_info.get('description', 'N/A')}
        for node_id, node_info in tasks_data['nodes'].items()
        for task_id, task_info in node_info['tasks'].items()
        if task_info.get('running_time_in_nanos', 0) / 60e9 > LONG_RUNNING_TASK_MINUTES
    ]
    
    if not slow_tasks:
        console.print(f"[green]✅ No se detectaron tareas de búsqueda lentas por encima de {LONG_RUNNING_TASK_MINUTES} minutos.[/green]")
        Prompt.ask("\n[bold]Presiona Enter para volver al menú...[/bold]")
        return

    table = Table(title=f"Tareas de Búsqueda Lentas (Más de {LONG_RUNNING_TASK_MINUTES} minutos)")
    table.add_column("Nodo", style="cyan")
    table.add_column("Tiempo (min)", justify="right", style="yellow")
    table.add_column("Descripción", style="white")

    for task in sorted(slow_tasks, key=lambda x: x['time_min'], reverse=True):
        table.add_row(task['node'], f"{task['time_min']:.2f}", task['description'])

    console.print(table)
    Prompt.ask("\n[bold]Presiona Enter para volver al menú...[/bold]")

def analyze_index_templates(analyzer: ClusterAnalyzer):
    """Evalúa las plantillas de índice en busca de problemas y muestra su impacto."""
    console.print(Rule("[bold]Diagnóstico y Relevancia de Plantillas de Índice[/bold]"))
    analyzer.fetch_all_data()

    templates_data = analyzer.client.get("_index_template")
    indices_df = analyzer.indices_df
    
    if not templates_data or 'index_templates' not in templates_data:
        console.print("[red]No se pudieron obtener plantillas de índice.[/red]")
        return
        
    if indices_df.empty:
        console.print("[yellow]No hay datos de índices para correlacionar con las plantillas.[/yellow]")
        return

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
        
        matching_indices = indices_df[indices_df['index'].apply(
            lambda idx: any(fnmatch.fnmatch(idx, pattern) for pattern in patterns)
        )]
        
        index_count = len(matching_indices)
        total_docs = matching_indices['docs.count'].sum()
        total_size_mb = matching_indices['store.size'].sum()
        
        size_str = f"{total_size_mb / 1024:.2f} GB" if total_size_mb > 1024 else f"{total_size_mb:.1f} MB"

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
            f"{total_docs:,}",
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

def analyze_configuration_drift(analyzer: ClusterAnalyzer):
    """
    Compara la configuración actual del clúster con los valores por defecto de Elasticsearch
    para detectar cambios o "derivas" que puedan ser problemáticas.
    """
    console.print(Rule("[bold]🕵️ Detección de Deriva de Configuración (Drift)[/bold]"))
    
    # Valores por defecto para configuraciones críticas. Un valor de 'None' significa que no se espera que exista.
    # Esto se podría externalizar a un archivo de configuración YAML/JSON.
    DEFAULT_SETTINGS = {
        "persistent": {
            "cluster.routing.rebalance.enable": "all",
            "indices.breaker.total.limit": "70%", # Ejemplo, el default real es más complejo
            "cluster.routing.allocation.enable": "all"
        },
        "transient": {
            # Los transient NUNCA deberían existir en un clúster estable. Su presencia es una alerta.
        }
    }
    
    settings_data = analyzer.client.get("_cluster/settings")
    if not settings_data:
        console.print("[red]No se pudo obtener la configuración del clúster.[/red]")
        return
        
    drifts = []
    
    # 1. Chequear configuraciones persistentes contra los defaults esperados
    persistent_settings = settings_data.get("persistent", {})
    for key, default_value in DEFAULT_SETTINGS["persistent"].items():
        current_value = persistent_settings.get(key)
        if current_value is not None and current_value != default_value:
            drifts.append(
                f"-[red]Deriva Persistente Detectada[/red]: "
                f"El parámetro [cyan]'{key}'[/cyan] tiene el valor [yellow]'{current_value}'[/yellow], "
                f"pero el valor esperado o por defecto es [green]'{default_value}'[/green]."
            )
            
    # 2. Chequear si existen configuraciones transitorias (lo cual ya es una alerta)
    transient_settings = settings_data.get("transient", {})
    if transient_settings:
        for key, value in transient_settings.items():
            drifts.append(
                f"-[bold red]¡Alerta Crítica![/bold red] "
                f"Se encontró una configuración transitoria para [cyan]'{key}'[/cyan] ([yellow]'{value}'[/yellow]). "
                f"Esta configuración se perderá al reiniciar el clúster y suele ser una solución temporal olvidada."
            )
            
    if not drifts:
        console.print("[green]✅ No se detectaron derivas en la configuración crítica del clúster. Todo en orden.[/green]")
    else:
        console.print(Panel(
            "\n".join(drifts),
            title="[yellow]Resultados del Análisis de Deriva[/yellow]",
            border_style="yellow",
            subtitle="Las derivas pueden indicar configuraciones temporales olvidadas que afectan el rendimiento."
        ))
        
    Prompt.ask("\n[bold]Presiona Enter para volver al menú...[/bold]")


def analyze_mapping_explosion(analyzer: ClusterAnalyzer):
    """
    Analiza los mapeos de los índices para detectar una cantidad excesiva de campos,
    lo que puede llevar a inestabilidad en el clúster.
    """
    console.print(Rule("[bold]💥 Análisis de Explosión de Mapeo[/bold]"))
    
    # Umbral de campos. Más de 1000 campos es una advertencia oficial de Elastic.
    FIELD_COUNT_THRESHOLD = 1000
    
    # Obtenemos solo los índices más grandes para no analizar todo el clúster
    analyzer.fetch_all_data()
    indices_df = analyzer.indices_df.copy()
    
    if indices_df.empty:
        console.print("[yellow]No hay datos de índices para analizar.[/yellow]")
        return
    
    # Analizamos los 20 índices con más documentos
    top_indices = indices_df.sort_values(by='docs.count', ascending=False).head(20)
    
    table = Table(title="Resultados del Análisis de Mapeo de Campos")
    table.add_column("Índice", style="cyan")
    table.add_column("N° de Campos", justify="right", style="magenta")
    table.add_column("Diagnóstico", style="white")

    with console.status("[yellow]Analizando mapeos...[/yellow]", spinner="dots"):
        for _, index_row in top_indices.iterrows():
            index_name = index_row['index']
            mapping_data = analyzer.client.get(f"{index_name}/_mapping")
            
            field_count = 0
            # Función recursiva para contar los campos anidados
            def count_fields(mapping):
                count = 0
                if 'properties' in mapping:
                    props = mapping['properties']
                    count += len(props)
                    for field, value in props.items():
                        count += count_fields(value)
                return count

            if mapping_data and index_name in mapping_data:
                field_count = count_fields(mapping_data[index_name]['mappings'])
            
            if field_count > FIELD_COUNT_THRESHOLD:
                style = "bold red"
                diagnostic = f"¡RIESGO ALTO! ({field_count}/{FIELD_COUNT_THRESHOLD})"
            elif field_count > FIELD_COUNT_THRESHOLD * 0.75:
                style = "yellow"
                diagnostic = f"Advertencia ({field_count}/{FIELD_COUNT_THRESHOLD})"
            else:
                style = "green"
                diagnostic = "Saludable"

            table.add_row(index_name, f"[{style}]{field_count}[/{style}]", diagnostic)
            
    console.print(table)
    console.print("\n[italic]Un número excesivo de campos (más de 1000) consume una gran cantidad de memoria en el nodo Máster y puede desestabilizar el clúster.[/italic]")
    Prompt.ask("\n[bold]Presiona Enter para volver al menú...[/bold]")


def run_causality_chain_analysis(analyzer: ClusterAnalyzer):
    """
    Motor de diagnóstico que intenta conectar síntomas con causas probables,
    guiando al usuario a través de una cadena de razonamiento.
    """
    console.print(Rule("[bold]🔗 Diagnóstico por Cadenas de Causalidad[/bold]"))
    
    with console.status("[yellow]Ejecutando análisis profundo...[/yellow]", spinner="earth"):
        analyzer.fetch_all_data()
        time.sleep(2) # Espera para calcular tasas
        analyzer.fetch_all_data()

    # 1. Punto de partida: ¿Hay algún nodo con uso de HEAP OLD GEN muy alto?
    nodes_df = analyzer.nodes_df.copy()
    high_heap_nodes = nodes_df[nodes_df['heap_old_gen_percent'] > HEAP_OLD_GEN_THRESHOLD]
    
    if high_heap_nodes.empty:
        console.print("[green]✅ No se detectaron nodos con presión de memoria crítica (Old Gen). El clúster parece estable en este aspecto.[/green]")
        Prompt.ask("\n[bold]Presiona Enter para volver al menú...[/bold]")
        return
        
    console.print(f"Se detectaron [bold red]{len(high_heap_nodes)}[/bold red] nodos con alta presión de memoria. Iniciando análisis de causa raíz...\n")

    for _, node in high_heap_nodes.iterrows():
        node_name = node['node_name']
        report = [
            f"Diagnóstico para el nodo: [bold magenta]{node_name}[/bold magenta]",
            f"  [1] SÍNTOMA: El uso de memoria Heap Old Gen es del [bold red]{node['heap_old_gen_percent']:.1f}%[/bold red], superando el umbral del {HEAP_OLD_GEN_THRESHOLD}%."
        ]
        
        # 2. Investigación: ¿Coincide con actividad alta de Garbage Collection?
        gc_time = node['gc_time_ms']
        if gc_time > GC_TIME_THRESHOLD:
            report.append(f"  [2] CORRELACIÓN: Se observa un tiempo de GC elevado ({gc_time} ms), lo que confirma que el nodo está luchando por liberar memoria.")
        else:
            report.append(f"  [2] CORRELACIÓN: El tiempo de GC no es anormalmente alto, la presión puede ser reciente o constante.")
            
        # 3. Investigación: ¿Qué shards primarios están en este nodo y qué carga tienen?
        shards_on_node = analyzer.shards_df[analyzer.shards_df['node'] == node_name]
        primary_shards = shards_on_node[shards_on_node['prirep'] == 'p']
        
        if primary_shards.empty:
            report.append("  [3] ANÁLISIS DE CARGA: Este nodo no tiene shards primarios. La presión de memoria podría venir de réplicas, búsquedas pesadas o tareas internas.")
        else:
            indices_with_rates = analyzer.indices_df[['index', 'write_rate', 'search_rate']]
            primary_shards_activity = pd.merge(primary_shards, indices_with_rates, on='index', how='left').fillna(0)
            
            top_writing_shard = primary_shards_activity.sort_values(by='write_rate', ascending=False).iloc[0]
            top_searching_shard = primary_shards_activity.sort_values(by='search_rate', ascending=False).iloc[0]

            report.append(f"  [3] ANÁLISIS DE CARGA: El nodo aloja {len(primary_shards)} shards primarios.")
            if top_writing_shard['write_rate'] > 0:
                 report.append(f"    - El principal contribuyente a la carga de ESCRITURA es el índice [cyan]{top_writing_shard['index']}[/cyan].")
            if top_searching_shard['search_rate'] > 0:
                report.append(f"    - El principal contribuyente a la carga de BÚSQUEDA es el índice [cyan]{top_searching_shard['index']}[/cyan].")

        # 4. Investigación: ¿Qué índices en este nodo están consumiendo más memoria Heap?
        # (Esto es una simplificación, una implementación real requeriría una API más detallada)
        top_heap_consumer = analyzer.top_heap_indices.iloc[0] if not analyzer.top_heap_indices.empty else None
        if top_heap_consumer is not None:
             report.append(f"  [4] ANÁLISIS DE MEMORIA: A nivel de clúster, el índice [cyan]{top_heap_consumer['index']}[/cyan] es el que más memoria consume ({top_heap_consumer['heap_usage_mb']:.1f} MB). Es probable que sea un factor contribuyente.")

        # 5. Conclusión / Hipótesis
        conclusion = (
            "HIPÓTESIS: La alta presión de memoria en este nodo es probablemente causada por una combinación de "
            "una alta carga de ingesta/búsqueda en los shards primarios que aloja y el alto consumo de memoria de "
            f"índices como [cyan]{top_heap_consumer['index']}[/cyan]." if top_heap_consumer else "una alta carga de ingesta/búsqueda en los shards primarios que aloja."
        )
        report.append(f"\n  [bold green]CONCLUSIÓN PRELIMINAR:[/] {conclusion}")

        console.print(Panel("\n".join(report), title=f"[yellow]Cadena de Causalidad - {node_name}[/yellow]", border_style="yellow"))

    Prompt.ask("\n[bold]Presiona Enter para volver al menú...[/bold]")


def analyze_shard_toxicity(analyzer: ClusterAnalyzer):
    """
    Identifica "inquilinos tóxicos" correlacionando nodos con alta CPU
    con las consultas lentas que se ejecutan en ellos.
    """
    console.print(Rule("[bold]☣️ Análisis de Toxicidad de Shards e Inquilinos[/bold]"))

    # Paso 1: Realizar el trabajo pesado DENTRO del bloque de estado
    with console.status("[yellow]Identificando nodos sobrecargados y tareas lentas...[/yellow]"):
        analyzer.fetch_all_data()
        nodes_df = analyzer.nodes_df.copy()
        high_cpu_nodes = nodes_df[nodes_df['cpu_percent'] > CPU_USAGE_THRESHOLD]

        # Solo obtenemos las tareas si hay nodos con CPU alta
        tasks_data = None
        if not high_cpu_nodes.empty:
            tasks_data = analyzer.client.get("_tasks", params={'actions': '*search*', 'detailed': 'true'})

    # Paso 2: Ahora que el spinner desapareció, mostramos los resultados
    if high_cpu_nodes.empty:
        console.print(f"[green]✅ No se detectaron nodos con CPU por encima del umbral ({CPU_USAGE_THRESHOLD}%).[/green]")
        Prompt.ask("\n[bold]Presiona Enter para volver al menú...[/bold]")
        return

    if not tasks_data or 'nodes' not in tasks_data:
        console.print("[red]Se detectaron nodos con CPU alta, pero no se pudo obtener la información de las tareas.[/red]")
        return

    toxic_tenants = []

    # Paso 3: Correlacionar los datos obtenidos
    for _, node in high_cpu_nodes.iterrows():
        node_name = node['node_name']
        node_id = node['node_id']

        if node_id not in tasks_data.get('nodes', {}):
            continue

        for task_id, task_info in tasks_data['nodes'][node_id]['tasks'].items():
            description = task_info.get('description', '')
            tenant_id = "No Extraído"
            try:
                if 'body:' in description:
                    query_body_str = description.split('body:')[1].strip()
                    query_body = json.loads(query_body_str)
                    if 'term' in query_body.get('query', {}).get('bool', {}).get('filter', [{}])[0]:
                       term_filter = query_body['query']['bool']['filter'][0]['term']
                       for key, value in term_filter.items():
                           if 'customer_id' in key or 'tenant_id' in key:
                               tenant_id = str(value)
                               break
            except (json.JSONDecodeError, IndexError, KeyError):
                pass

            toxic_tenants.append({
                "node_name": node_name,
                "cpu": node['cpu_percent'],
                "running_time_s": task_info.get('running_time_in_nanos', 0) / 1e9,
                "tenant_id": tenant_id,
                "description": description
            })

    if not toxic_tenants:
        console.print("[green]✅ Se detectaron nodos con CPU alta, pero no hay tareas de búsqueda lenta asociadas que puedan ser la causa.[/green]")
        Prompt.ask("\n[bold]Presiona Enter para volver al menú...[/bold]")
        return

    table = Table(title="Resultados del Análisis de Inquilinos Tóxicos")
    table.add_column("Nodo Afectado", style="magenta")
    table.add_column("CPU%", justify="right", style="red")
    table.add_column("Inquilino (ID Extraído)", style="yellow")
    table.add_column("Tiempo Tarea (s)", justify="right")
    table.add_column("Descripción de la Consulta", max_width=80)

    for tenant in sorted(toxic_tenants, key=lambda x: x['running_time_s'], reverse=True):
        table.add_row(
            tenant['node_name'],
            f"{tenant['cpu']:.0f}%",
            tenant['tenant_id'],
            f"{tenant['running_time_s']:.1f}",
            tenant['description']
        )

    console.print(table)
    console.print("\n[italic]Esta tabla muestra consultas lentas en nodos sobrecargados. El 'inquilino' es una extracción heurística de la consulta.[/italic]")
    Prompt.ask("\n[bold]Presiona Enter para volver al menú...[/bold]")