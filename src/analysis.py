# src/analysis.py
import time
import re
import pandas as pd
import json
import fnmatch

from .analyzer import ClusterAnalyzer
from .config import (
    REFRESH_INTERVAL, LONG_RUNNING_TASK_MINUTES,
    HIGH_SHARD_COUNT_TEMPLATE_THRESHOLD, DUSTY_SHARD_MB_THRESHOLD,
    HEAP_OLD_GEN_THRESHOLD, GC_TIME_THRESHOLD, CPU_USAGE_THRESHOLD
)

# --- Funciones de Datos para Dashboards en Vivo ---

def get_live_dashboard_data(analyzer: ClusterAnalyzer) -> dict:
    analyzer.fetch_all_data()
    return {
        "cluster_health": analyzer.cluster_health, "cluster_stats": analyzer.cluster_stats,
        "pending_tasks": analyzer.pending_tasks, "nodes_df": analyzer.nodes_df.to_dict('records'),
        "previous_nodes_df": analyzer.previous_nodes_df.to_dict('records'),
        "indices_df": analyzer.indices_df.to_dict('records'),
        "previous_indices_df": analyzer.previous_indices_df.to_dict('records'),
        "top_heap_indices": analyzer.top_heap_indices.to_dict('records'),
        "last_fetch_time": analyzer.last_fetch_time
    }


def get_deep_dive_data(analyzer: ClusterAnalyzer) -> dict:
    analyzer.fetch_all_data(for_deep_dive=True)
    return {
        "nodes_df": analyzer.nodes_df.sort_values(by='cpu_percent', ascending=False).to_dict('records'),
        "node_stats_raw": analyzer.node_stats_raw, "previous_node_stats_raw": analyzer.previous_node_stats_raw
    }

def get_shard_distribution_data(analyzer: ClusterAnalyzer, group_by_col: str, sort_by_column: str) -> dict:
    analyzer.fetch_all_data()
    shards_df = analyzer.shards_df.copy()
    if shards_df.empty: return {"summary": []}
    shards_df['pattern'] = shards_df['index'].apply(lambda x: re.sub(r'\d{4}[-.]\d{2}[-.]\d{2}|-\d{6}', '-*', x))
    shards_df['store'] = pd.to_numeric(shards_df['store'], errors='coerce').fillna(0)
    summary_df = shards_df.groupby(group_by_col).agg(
        total_shards=('shard', 'count'), primaries=('prirep', lambda x: (x == 'p').sum()),
        replicas=('prirep', lambda x: (x == 'r').sum()), total_gb=('store', lambda x: x.sum() / 1024),
        nodes_involved=('node', 'nunique')).reset_index()
    sorted_df = summary_df.sort_values(by=sort_by_column, ascending=False)
    return {"summary": sorted_df.head(20).to_dict('records')}

# --- Funciones de Análisis Experto (Devuelven datos, no imprimen) ---

def analyze_node_load_correlation(analyzer: ClusterAnalyzer) -> dict:
    analyzer.fetch_all_data(); time.sleep(REFRESH_INTERVAL); analyzer.fetch_all_data()
    nodes_df, shards_df, indices_df, prev_indices_df = analyzer.nodes_df.copy(), analyzer.shards_df.copy(), analyzer.indices_df.copy(), analyzer.previous_indices_df.copy()
    if any(df.empty for df in [nodes_df, shards_df, indices_df, prev_indices_df]): return {"error": "Datos incompletos."}
    
    merged = pd.merge(indices_df, prev_indices_df[['index', 'indexing_total', 'search_total']], on='index', how='left', suffixes=('', '_prev'))
    merged['indexing_total_prev'] = merged['indexing_total_prev'].fillna(merged['indexing_total'])
    merged['search_total_prev'] = merged['search_total_prev'].fillna(merged['search_total'])
    delta = REFRESH_INTERVAL if REFRESH_INTERVAL > 0 else 1
    indices_df['write_rate'] = (merged['indexing_total'] - merged['indexing_total_prev']) / delta
    indices_df['search_rate'] = (merged['search_total'] - merged['search_total_prev']) / delta

    activity_df = pd.merge(shards_df, indices_df[['index', 'write_rate', 'search_rate']], on='index', how='left').fillna(0)
    loads = [{'Nodo': n['node_name'], 'CPU %': n['cpu_percent'], 'Heap %': n['heap_percent'],
              'Primarios': len(activity_df[(activity_df['node'] == n['node_name']) & (activity_df['prirep'] == 'p')]),
              'Total Shards': len(activity_df[activity_df['node'] == n['node_name']]),
              'Carga Escritura (docs/s)': activity_df[(activity_df['node'] == n['node_name']) & (activity_df['prirep'] == 'p')]['write_rate'].sum(),
              'Carga Búsqueda (req/s)': activity_df[activity_df['node'] == n['node_name']]['search_rate'].sum()
             } for _, n in nodes_df.iterrows()]
    return {"node_loads": pd.DataFrame(loads).sort_values(by='CPU %', ascending=False).to_dict('records')}

def analyze_node_index_correlation(analyzer: ClusterAnalyzer) -> dict:
    analyzer.fetch_all_data(); time.sleep(REFRESH_INTERVAL); analyzer.fetch_all_data()
    shards_df, indices_df, prev_indices_df = analyzer.shards_df.copy(), analyzer.indices_df.copy(), analyzer.previous_indices_df.copy()
    if any(df.empty for df in [shards_df, indices_df, prev_indices_df]): return {"error": "Datos incompletos."}

    merged = pd.merge(indices_df, prev_indices_df[['index', 'indexing_total', 'search_total']], on='index', how='left', suffixes=('', '_prev'))
    merged['indexing_total_prev'] = merged['indexing_total_prev'].fillna(merged['indexing_total'])
    merged['search_total_prev'] = merged['search_total_prev'].fillna(merged['search_total'])
    delta = REFRESH_INTERVAL if REFRESH_INTERVAL > 0 else 1
    indices_df['write_rate'] = (merged['indexing_total'] - merged['indexing_total_prev']) / delta
    indices_df['search_rate'] = (merged['search_total'] - merged['search_total_prev']) / delta
    
    primaries = shards_df[shards_df['prirep'] == 'p'].copy()
    primaries['pattern'] = primaries['index'].apply(lambda x: re.sub(r'\d{4}[-.]\d{2}[-.]\d{2}|-\d{6}', '-*', x))
    counts = primaries.groupby(['pattern', 'node']).size().reset_index(name='shard_count')
    imbalance = counts.groupby('pattern')['shard_count'].agg(std_dev='std', node_count='count').fillna(0)
    
    indices_df['pattern'] = indices_df['index'].apply(lambda x: re.sub(r'\d{4}[-.]\d{2}[-.]\d{2}|-\d{6}', '-*', x))
    activity = indices_df.groupby('pattern')[['write_rate', 'search_rate']].sum().reset_index()

    imbalanced_patterns = pd.merge(imbalance[imbalance['node_count'] > 1].reset_index(), activity, on='pattern', how='left').fillna(0)
    imbalanced_patterns = imbalanced_patterns[imbalanced_patterns['std_dev'] > 0].sort_values(by='std_dev', ascending=False)

    results = []
    for _, p_row in imbalanced_patterns.iterrows():
        nodes = counts[counts['pattern'] == p_row['pattern']].sort_values(by='shard_count', ascending=False).to_dict('records')
        results.append({"pattern_info": p_row.to_dict(), "nodes": nodes})
    return {"imbalanced_patterns": results}

def analyze_slow_tasks(analyzer: ClusterAnalyzer) -> dict:
    # (Código sin cambios, ya era correcto)
    tasks_data = analyzer.client.get("_tasks", params={'actions': '*search*', 'detailed': 'true'})
    if not tasks_data or 'nodes' not in tasks_data: return {"threshold_minutes": LONG_RUNNING_TASK_MINUTES, "tasks": []}
    slow_tasks = [{'node': n.get('name'), 'time_min': t.get('running_time_in_nanos', 0) / 60e9, 'description': t.get('description', 'N/A')}
                  for _, n in tasks_data['nodes'].items() for _, t in n['tasks'].items()
                  if t.get('running_time_in_nanos', 0) / 60e9 > LONG_RUNNING_TASK_MINUTES]
    return {"threshold_minutes": LONG_RUNNING_TASK_MINUTES, "tasks": sorted(slow_tasks, key=lambda x: x['time_min'], reverse=True)}


def generate_report_data(analyzer: ClusterAnalyzer) -> dict:
    """
    Recolecta datos y genera una lista de sugerencias accionables para el reporte.
    """
    analyzer.fetch_all_data()
    time.sleep(2) # Pequeña espera para asegurar el cálculo de tasas si fuera necesario
    analyzer.fetch_all_data()

    nodes_df = analyzer.nodes_df
    indices_df = analyzer.indices_df
    top_heap = analyzer.top_heap_indices
    health = analyzer.cluster_health
    
    suggestions = []
    if nodes_df.empty:
        suggestions.append("No se pudieron obtener datos de los nodos para generar sugerencias.")
    else:
        for _, node in nodes_df.iterrows():
            if node['heap_old_gen_percent'] > HEAP_OLD_GEN_THRESHOLD:
                suggestion = f"Heap Old Gen Alto en '{node['node_name']}': Riesgo de pausas largas de GC."
                if not top_heap.empty:
                    top_consumer = top_heap.iloc[0]
                    suggestion += f" El índice '{top_consumer['index']}' es el que más memoria consume ({top_consumer['heap_usage_mb']:.1f} MB)."
                suggestions.append(suggestion)

            if node['cpu_percent'] > CPU_USAGE_THRESHOLD:
                suggestions.append(f"CPU Alta en '{node['node_name']}': Considera revisar consultas costosas o picos de ingesta.")
            
            if node['gc_time_ms'] > GC_TIME_THRESHOLD:
                suggestions.append(f"GC Excesivo en '{node['node_name']}': El nodo está pausando para limpiar memoria. Revisa el uso de heap.")
            
            if node['rejections'] > 0:
                suggestion = f"Rechazos de peticiones en '{node['node_name']}': El nodo no puede procesar la carga actual."
                if not indices_df.empty and 'write_rate' in indices_df.columns:
                    top_writer = indices_df.sort_values('write_rate', ascending=False).iloc[0]
                    if top_writer['write_rate'] > 0:
                        suggestion += f" La alta tasa de escrituras del índice '{top_writer['index']}' podría ser la causa."
                suggestions.append(suggestion)

            if node['breakers_tripped'] > 0:
                suggestions.append(f"¡CIRCUIT BREAKER ACTIVADO en '{node['node_name']}'! Una operación fue rechazada por exceso de memoria. ¡CRÍTICO!")
    
    if health.get('unassigned_shards', 0) > 0:
        suggestions.append(f"Shards No Asignados Detectados: El clúster no está completamente operativo. Usa la API `_cluster/allocation/explain` para diagnosticar la causa.")

    return {
        "cluster_name": analyzer.cluster_stats.get('cluster_name', 'N/A'),
        "cluster_status": health.get('status', 'N/A'),
        "suggestions": suggestions
    }



def analyze_index_templates(analyzer: ClusterAnalyzer) -> dict:
    # (Código sin cambios, ya era correcto)
    analyzer.fetch_all_data()
    templates_data = analyzer.client.get("_index_template"); indices_df = analyzer.indices_df
    if not templates_data or 'index_templates' not in templates_data or indices_df.empty: return {"templates": []}
    indices_df['docs.count'] = pd.to_numeric(indices_df['docs.count'], errors='coerce').fillna(0)
    indices_df['store.size'] = pd.to_numeric(indices_df['store.size'], errors='coerce').fillna(0)
    results = []
    for t_info in templates_data['index_templates']:
        name = t_info['name']; template = t_info['index_template']; patterns = template.get('index_patterns', [])
        matching = indices_df[indices_df['index'].apply(lambda idx: any(fnmatch.fnmatch(idx, p) for p in patterns))]
        docs = matching['docs.count'].sum(); size_mb = matching['store.size'].sum()
        size_str = f"{size_mb / 1024:.2f} GB" if size_mb > 1024 else f"{size_mb:.1f} MB"
        diags = [f"Alto N° de Shards ({s})" for s in [template.get('settings', {}).get('index', {}).get('number_of_shards')] if s and int(s) > HIGH_SHARD_COUNT_TEMPLATE_THRESHOLD]
        if 'ilm' not in template.get('settings', {}).get('index', {}): diags.append("Sin política ILM")
        for p in patterns:
            if p in ["*", "*-*"]: diags.append(f"Comodín Genérico ('{p}')")
        results.append({"name": name, "index_count": len(matching), "total_docs": int(docs), "total_size_str": size_str, "diagnostics_str": ", ".join(diags) or "OK"})
    return {"templates": results}

def analyze_mapping_explosion(analyzer: ClusterAnalyzer) -> dict:
    # (Código sin cambios, ya era correcto)
    FIELD_COUNT_THRESHOLD = 1000; analyzer.fetch_all_data(); indices_df = analyzer.indices_df
    if indices_df.empty: return {"indices": []}
    top_indices = indices_df.sort_values(by='docs.count', ascending=False).head(20)
    results = []
    def count_fields(mapping):
        c = 0
        if 'properties' in mapping: c += len(mapping['properties']); c += sum(count_fields(v) for v in mapping['properties'].values())
        return c
    for _, row in top_indices.iterrows():
        name = row['index']; data = analyzer.client.get(f"{name}/_mapping")
        count = count_fields(data[name]['mappings']) if data and name in data else 0
        if count > FIELD_COUNT_THRESHOLD * 0.75:
            diag = f"¡RIESGO ALTO! ({count}/{FIELD_COUNT_THRESHOLD})" if count > FIELD_COUNT_THRESHOLD else f"Advertencia ({count}/{FIELD_COUNT_THRESHOLD})"
            results.append({"index_name": name, "field_count": count, "diagnostic": diag})
    return {"indices": results}

def analyze_dusty_shards(analyzer: ClusterAnalyzer) -> dict:
    # (Código sin cambios, ya era correcto)
    analyzer.fetch_all_data(); shards_df = analyzer.shards_df.copy()
    if shards_df.empty: return {"threshold_mb": DUSTY_SHARD_MB_THRESHOLD, "empty_shards": [], "dusty_shards": []}
    shards_df['docs'] = pd.to_numeric(shards_df['docs'], errors='coerce').fillna(0)
    shards_df['store'] = pd.to_numeric(shards_df['store'], errors='coerce').fillna(0)
    empty = shards_df[(shards_df['docs'] == 0) & (shards_df['state'] == 'STARTED')]
    dusty = shards_df[(shards_df['docs'] > 0) & (shards_df['store'] < DUSTY_SHARD_MB_THRESHOLD) & (shards_df['state'] == 'STARTED')]
    return {"threshold_mb": DUSTY_SHARD_MB_THRESHOLD, "empty_shards": empty.head(10).to_dict('records'), "dusty_shards": dusty.sort_values(by='store').head(10).to_dict('records')}

def analyze_configuration_drift(analyzer: ClusterAnalyzer) -> dict:
    # (Código sin cambios, ya era correcto)
    DEFAULTS = {"persistent": {"cluster.routing.rebalance.enable": "all", "cluster.routing.allocation.enable": "all"}}
    settings = analyzer.client.get("_cluster/settings")
    if not settings: return {"drifts": ["No se pudo obtener la configuración del clúster."]}
    drifts = [f"Deriva Persistente: '{k}' es '{v}', se esperaba '{d}'." for k, d in DEFAULTS["persistent"].items() if (v := settings.get("persistent", {}).get(k)) is not None and v != d]
    drifts.extend(f"¡Alerta Crítica! Configuración transitoria encontrada: '{k}':'{v}'. Se perderá al reiniciar." for k, v in settings.get("transient", {}).items())
    return {"drifts": drifts}

def run_causality_chain_analysis(analyzer: ClusterAnalyzer) -> dict:
    analyzer.fetch_all_data(); time.sleep(2); analyzer.fetch_all_data()
    nodes_df = analyzer.nodes_df.copy()
    high_heap_nodes = nodes_df[nodes_df['heap_old_gen_percent'] > HEAP_OLD_GEN_THRESHOLD]
    if high_heap_nodes.empty: return {"reports": []}
    
    reports = []
    for _, node in high_heap_nodes.iterrows():
        node_name = node['node_name']
        report = [f"Diagnóstico para el nodo: [bold magenta]{node_name}[/bold magenta]",
                  f"  [1] SÍNTOMA: Uso de Heap Old Gen del [bold red]{node['heap_old_gen_percent']:.1f}%[/bold red] (Umbral: {HEAP_OLD_GEN_THRESHOLD}%)."]
        
        gc_time = node['gc_time_ms']
        report.append(f"  [2] CORRELACIÓN: Tiempo de GC de {gc_time} ms. {'Confirma presión de memoria.' if gc_time > GC_TIME_THRESHOLD else 'No parece anormal.'}")
        
        shards_on_node = analyzer.shards_df[analyzer.shards_df['node'] == node_name]
        primary_shards = shards_on_node[shards_on_node['prirep'] == 'p']
        
        if primary_shards.empty:
            report.append("  [3] ANÁLISIS DE CARGA: El nodo no tiene shards primarios.")
        else:
            rates = analyzer.indices_df[['index', 'write_rate', 'search_rate']]
            activity = pd.merge(primary_shards, rates, on='index', how='left').fillna(0)
            top_w = activity.sort_values(by='write_rate', ascending=False).iloc[0]
            top_s = activity.sort_values(by='search_rate', ascending=False).iloc[0]
            report.append(f"  [3] ANÁLISIS DE CARGA: {len(primary_shards)} shards primarios.")
            if top_w['write_rate'] > 0: report.append(f"    - Principal carga de ESCRITURA: índice [cyan]{top_w['index']}[/cyan].")
            if top_s['search_rate'] > 0: report.append(f"    - Principal carga de BÚSQUEDA: índice [cyan]{top_s['index']}[/cyan].")

        top_heap = analyzer.top_heap_indices.iloc[0] if not analyzer.top_heap_indices.empty else None
        if top_heap is not None:
            report.append(f"  [4] ANÁLISIS DE MEMORIA: Índice que más consume en el clúster: [cyan]{top_heap['index']}[/cyan] ({top_heap['heap_usage_mb']:.1f} MB).")
        
        conclusion = f"La alta presión de memoria en este nodo es probablemente causada por una combinación de la carga de sus shards primarios y el consumo de memoria de índices como [cyan]{top_heap['index']}[/cyan]." if top_heap else "La alta presión de memoria es probablemente causada por la carga de sus shards primarios."
        report.append(f"\n  [bold green]HIPÓTESIS:[/] {conclusion}")
        reports.append({"node_name": node_name, "report_lines": report})
    return {"reports": reports}

def analyze_shard_toxicity(analyzer: ClusterAnalyzer) -> dict:
    analyzer.fetch_all_data()
    nodes_df = analyzer.nodes_df.copy(); high_cpu_nodes = nodes_df[nodes_df['cpu_percent'] > CPU_USAGE_THRESHOLD]
    if high_cpu_nodes.empty: return {"toxic_tenants": [], "message": f"No hay nodos con CPU > {CPU_USAGE_THRESHOLD}%."}

    tasks_data = analyzer.client.get("_tasks", params={'actions': '*search*', 'detailed': 'true'})
    if not tasks_data or 'nodes' not in tasks_data: return {"toxic_tenants": [], "message": "CPU alta detectada, pero no se pudieron obtener las tareas."}

    tenants = []
    for _, node in high_cpu_nodes.iterrows():
        if node['node_id'] not in tasks_data.get('nodes', {}): continue
        for _, task in tasks_data['nodes'][node['node_id']]['tasks'].items():
            desc = task.get('description', ''); tenant_id = "N/A"
            try:
                if 'body:' in desc:
                    body = json.loads(desc.split('body:')[1].strip())
                    term = body.get('query', {}).get('bool', {}).get('filter', [{}])[0].get('term', {})
                    for k, v in term.items():
                        if 'customer_id' in k or 'tenant_id' in k: tenant_id = str(v); break
            except (json.JSONDecodeError, IndexError, KeyError): pass
            tenants.append({"node_name": node['node_name'], "cpu": node['cpu_percent'], "running_time_s": task.get('running_time_in_nanos', 0) / 1e9, "tenant_id": tenant_id, "description": desc})
    
    if not tenants: return {"toxic_tenants": [], "message": "CPU alta detectada, pero sin tareas de búsqueda lenta asociadas."}
    return {"toxic_tenants": sorted(tenants, key=lambda x: x['running_time_s'], reverse=True)}