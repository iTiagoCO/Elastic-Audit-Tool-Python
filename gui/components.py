# gui/components.py
from dash import dcc, html
import dash_bootstrap_components as dbc
import plotly.graph_objects as go
import pandas as pd
import json

# --- Helpers de UI ---
def create_kpi_card(title, value, color="light", subtitle=None):
    """Crea una tarjeta para un Indicador Clave de Rendimiento (KPI)."""
    return dbc.Card(
        dbc.CardBody(
            [
                html.H5(title, className="card-title text-muted"),
                html.H2(value, className=f"card-text text-{color}"),
                html.P(subtitle, className="card-text small text-muted") if subtitle else None,
            ]
        ),
        className="text-center m-2 shadow-sm",
    )

def df_to_dbc_table(df, title):
    """Convierte un DataFrame de Pandas en una tabla estilizada de Dash Bootstrap."""
    if df.empty:
        return dbc.Alert(f"No hay datos para mostrar en '{title}'.", color="info")
    
    table_header = [html.Thead(html.Tr([html.Th(col) for col in df.columns]))]
    table_body = [html.Tbody([
        html.Tr([html.Td(str(data)) for data in row]) for i, row in df.iterrows()
    ])]
    
    # La corrección está en esta línea: se eliminó 'dark=True'
    return html.Div([
        html.H4(title),
        dbc.Table(table_header + table_body, bordered=True, striped=True, hover=True, responsive=True)
    ])

# --- Vistas de Página Completa ---

def render_dashboard_general(analyzer):
    """Crea el layout para el Dashboard General con KPIs y cálculo de tasas."""
    import time

    # --- Lógica de Cálculo de Tasas ---
    # 1. Primera captura de datos
    analyzer.fetch_all_data()
    indices_df_before = analyzer.indices_df.copy()

    # 2. Esperamos un breve intervalo para tener un delta
    time.sleep(2)

    # 3. Segunda captura de datos
    analyzer.fetch_all_data()
    indices_df_after = analyzer.indices_df.copy()

    # 4. Calculamos las tasas
    if not indices_df_before.empty and not indices_df_after.empty:
        merged_df = pd.merge(
            indices_df_after,
            indices_df_before[['index', 'indexing_total']],
            on='index',
            how='left',
            suffixes=('', '_prev')
        )
        merged_df['indexing_total_prev'] = merged_df['indexing_total_prev'].fillna(merged_df['indexing_total'])
        # El delta de tiempo es el intervalo de 'sleep'
        indices_df_after['write_rate'] = (merged_df['indexing_total'] - merged_df['indexing_total_prev']) / 2.0
    else:
        indices_df_after['write_rate'] = 0.0

    # --- Lógica de Renderizado ---
    health = analyzer.cluster_health
    nodes_df = analyzer.nodes_df

    unassigned_shards = health.get('unassigned_shards', 0)
    unassigned_color = "danger" if unassigned_shards > 0 else "light"

    avg_cpu = nodes_df['cpu_percent'].mean() if not nodes_df.empty else 0
    cpu_color = "warning" if avg_cpu > 75 else "light"

    avg_heap = nodes_df['heap_percent'].mean() if not nodes_df.empty else 0
    heap_color = "warning" if avg_heap > 75 else "light"

    # Ahora que 'write_rate' existe, podemos obtener el top 5
    top_writers_df = pd.DataFrame()
    if 'write_rate' in indices_df_after.columns:
        top_writers_df = indices_df_after.sort_values(by='write_rate', ascending=False).head(5)
        # Nos aseguramos de mostrar solo las columnas relevantes y con formato
        top_writers_df['write_rate'] = top_writers_df['write_rate'].round(1)
        top_writers_df = top_writers_df[['index', 'write_rate']]

    return html.Div([
        html.H3("📈 Dashboard General"),
        dbc.Row([
            dbc.Col(create_kpi_card("Nodos Totales", health.get('number_of_nodes', 0))),
            dbc.Col(create_kpi_card("Shards No Asignados", unassigned_shards, unassigned_color)),
            dbc.Col(create_kpi_card("CPU Promedio", f"{avg_cpu:.1f}%", cpu_color)),
            dbc.Col(create_kpi_card("Heap Promedio", f"{avg_heap:.1f}%", heap_color)),
        ]),
        html.Hr(className="my-4"),
        # La tabla ahora se crea de forma segura
        df_to_dbc_table(top_writers_df, "Top 5 Índices por Tasa de Escritura (docs/s)")
    ])

def render_node_health_view(analyzer):
    analyzer.fetch_all_data()
    nodes_df = analyzer.nodes_df.sort_values(by='tier')
    if nodes_df.empty: return dbc.Alert("No se pudieron obtener datos de los nodos.", color="warning")
    
    fig = go.Figure()
    fig.add_trace(go.Bar(x=nodes_df['node_name'], y=nodes_df['cpu_percent'], name='CPU %'))
    fig.add_trace(go.Bar(x=nodes_df['node_name'], y=nodes_df['heap_percent'], name='Heap %'))
    fig.update_layout(title_text='Uso de CPU y Heap por Nodo', barmode='group', template="plotly_dark", legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1))
    
    return html.Div([html.H3("🔬 Salud Detallada de Nodos"), dcc.Graph(figure=fig)])

def render_shard_distribution_view(analyzer):
    analyzer.fetch_all_data()
    shards_df = analyzer.shards_df.copy()
    if shards_df.empty: return dbc.Alert("No hay datos de shards para analizar.", color="warning")
    
    shards_df['pattern'] = shards_df['index'].str.split(r'[-\._]').str[0]
    shards_df['store'] = pd.to_numeric(shards_df['store'], errors='coerce').fillna(0)
    
    fig = go.Figure(go.Sunburst(
        labels=shards_df['pattern'].tolist() + shards_df['node'].tolist(),
        parents=[""] * len(shards_df) + shards_df['pattern'].tolist(),
        values=shards_df['store'],
        branchvalues="total",
    ))
    fig.update_layout(margin=dict(t=25, l=0, r=0, b=0), template="plotly_dark", title_text="Distribución de Shards por Patrón y Nodo (Tamaño en MB)")
    return html.Div([html.H3("📊 Distribución de Shards"), dcc.Graph(figure=fig)])

def render_causality_chain_view(analyzer):
    analyzer.fetch_all_data()
    nodes_df = analyzer.nodes_df.copy()
    high_heap_nodes = nodes_df[nodes_df['heap_old_gen_percent'] > 75] # Umbral
    
    if high_heap_nodes.empty:
        return dbc.Alert("✅ No se detectaron nodos con presión de memoria crítica (Old Gen).", color="success")
        
    alerts = []
    for _, node in high_heap_nodes.iterrows():
        node_name = node['node_name']
        steps = [
            html.H5(f"Diagnóstico para: {node_name}", className="card-title"),
            html.P(f"➡️ SÍNTOMA: Uso de Heap Old Gen es del {node['heap_old_gen_percent']:.1f}%.", className="mb-1"),
            html.P(f"➡️ CORRELACIÓN: Tiempo de GC es de {node['gc_time_ms']} ms.", className="mb-1")
        ]
        alerts.append(dbc.Card(dbc.CardBody(steps), className="mb-3"))
        
    return html.Div([html.H3("🔗 Cadenas de Causalidad"), *alerts])

def render_shard_toxicity_view(analyzer):
    analyzer.fetch_all_data()
    nodes_df = analyzer.nodes_df.copy()
    high_cpu_nodes = nodes_df[nodes_df['cpu_percent'] > 80]
    
    if high_cpu_nodes.empty:
        return dbc.Alert("✅ No se detectaron nodos con CPU por encima del umbral.", color="success")

    tasks_data = analyzer.client.get("_tasks", params={'actions': '*search*', 'detailed': 'true'})
    if not tasks_data or 'nodes' not in tasks_data:
        return dbc.Alert("Se detectó CPU alta, pero no se pudo obtener información de tareas.", color="warning")

    toxic_tenants = []
    for _, node in high_cpu_nodes.iterrows():
        for task_id, task_info in tasks_data.get('nodes', {}).get(node['node_id'], {}).get('tasks', {}).items():
            toxic_tenants.append({
                'Nodo': node['node_name'],
                'Tiempo (s)': f"{task_info.get('running_time_in_nanos', 0) / 1e9:.1f}",
                'Descripción': task_info.get('description', 'N/A')
            })

    if not toxic_tenants:
        return dbc.Alert("Se detectó CPU alta, pero no hay tareas de búsqueda lenta asociadas.", color="info")

    return html.Div([
        html.H3("☣️ Análisis de Toxicidad de Shards e Inquilinos"),
        df_to_dbc_table(pd.DataFrame(toxic_tenants), "Tareas Lentas en Nodos Sobrecargados")
    ])

def render_imbalance_view(analyzer):
    analyzer.fetch_all_data()
    # (Lógica de analyze_node_index_correlation adaptada a un DataFrame)
    return dbc.Alert("Vista 'Desbalance de Shards' en construcción.", color="secondary")

def render_node_load_view(analyzer):
    analyzer.fetch_all_data()
    # (Lógica de analyze_node_load_correlation adaptada a un DataFrame)
    return dbc.Alert("Vista 'Carga de Nodos' en construcción.", color="secondary")

def render_slow_tasks_view(analyzer):
    tasks_data = analyzer.client.get("_tasks", params={'actions': '*search*', 'detailed': 'true'})
    if not tasks_data: return dbc.Alert("No se pudo obtener info de tareas.", color="danger")
    
    slow_tasks = [{'Nodo': n.get('name'), 'Tiempo (min)': f"{t.get('running_time_in_nanos', 0)/6e10:.2f}", 'Descripción': t.get('description')} for _, n in tasks_data['nodes'].items() for _, t in n['tasks'].items() if t.get('running_time_in_nanos', 0)/6e10 > 5]
    
    if not slow_tasks: return dbc.Alert("✅ No se detectaron tareas de búsqueda lentas.", color="success")
    return html.Div([html.H3("⌛ Tareas de Búsqueda Lentas"), df_to_dbc_table(pd.DataFrame(slow_tasks), "Tareas con más de 5 minutos de ejecución")])

def render_templates_view(analyzer):
    templates_data = analyzer.client.get("_index_template")
    if not templates_data: return dbc.Alert("No se pudieron obtener plantillas.", color="danger")
    
    templates = [{'Nombre': t['name'], 'Patrones': ", ".join(t['index_template'].get('index_patterns', [])), 'Prioridad': t['index_template'].get('priority', 'N/A')} for t in templates_data['index_templates']]
    return html.Div([html.H3("📝 Plantillas de Índice"), df_to_dbc_table(pd.DataFrame(templates), "Plantillas de Índice Registradas")])

def render_mapping_explosion_view(analyzer):
    analyzer.fetch_all_data()
    indices_df = analyzer.indices_df.copy().sort_values(by='docs.count', ascending=False).head(10)
    
    results = []
    for index_name in indices_df['index']:
        mapping_data = analyzer.client.get(f"{index_name}/_mapping")
        def count_fields(mapping):
            if 'properties' not in mapping: return 0
            return len(mapping['properties']) + sum(count_fields(v) for v in mapping['properties'].values())
        
        field_count = count_fields(mapping_data[index_name]['mappings']) if mapping_data and index_name in mapping_data else 0
        results.append({'Índice': index_name, 'N° de Campos': field_count, 'Riesgo': 'Alto' if field_count > 1000 else 'Bajo'})
    
    return html.Div([html.H3("💥 Explosión de Mapeo"), df_to_dbc_table(pd.DataFrame(results), "Análisis de Cantidad de Campos por Índice")])

def render_dusty_shards_view(analyzer):
    analyzer.fetch_all_data()
    shards_df = analyzer.shards_df.copy()
    shards_df['docs'] = pd.to_numeric(shards_df['docs'], errors='coerce').fillna(0)
    shards_df['store'] = pd.to_numeric(shards_df['store'], errors='coerce').fillna(0)
    
    dusty_shards = shards_df[(shards_df['docs'] > 0) & (shards_df['store'] < 50)] # Umbral de 50MB
    if dusty_shards.empty: return dbc.Alert("✅ No se detectaron 'shards de polvo'.", color="success")
    
    return html.Div([html.H3("🧹 Shards Vacíos / Polvo"), df_to_dbc_table(dusty_shards[['index', 'shard', 'node', 'docs', 'store']], "Shards con Menos de 50MB")])

def render_config_drift_view(analyzer):
    settings_data = analyzer.client.get("_cluster/settings")
    if not settings_data: return dbc.Alert("No se pudo obtener la configuración del clúster.", color="danger")
    
    drifts = []
    if settings_data.get("transient"):
        for k, v in settings_data["transient"].items():
            drifts.append(dbc.Alert(f"Configuración Transitoria Detectada: {k}={v}. ¡Esto es una mala práctica!", color="danger"))
            
    if not drifts: return dbc.Alert("✅ No se detectó deriva de configuración (transient).", color="success")
    return html.Div([html.H3("🕵️ Deriva de Configuración"), *drifts])