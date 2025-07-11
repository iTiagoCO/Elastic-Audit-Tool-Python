# gui/components.py
# gui/components.py
from dash import dcc, html
import dash_bootstrap_components as dbc
import plotly.graph_objects as go
import pandas as pd

def create_kpi_panel(title, value, color="white"):
    return html.Div([html.P(title, className="kpi-title mb-1"), html.H2(value, className=f"kpi-value text-{color}")], className="kpi-card text-center")

def df_to_dbc_table(df):
    if df.empty: return dbc.Alert("No hay datos para mostrar.", color="secondary")
    return dbc.Table.from_dataframe(df.astype(str), striped=True, bordered=False, hover=True, responsive=True)

def create_view_panel(header_text, children):
    if not isinstance(children, list): children = [children]
    return html.Div([html.Div(header_text, className="view-panel-header"), html.Div(children, className="view-panel-body")], className="view-panel")

def render_dashboard_general(analyzer):
    analyzer.fetch_all_data()
    health, nodes_df = analyzer.cluster_health, analyzer.nodes_df
    unassigned = health.get('unassigned_shards', 0)
    avg_cpu = nodes_df['cpu_percent'].mean() if not nodes_df.empty else 0
    return create_view_panel("Dashboard General", [dbc.Row([
        dbc.Col(create_view_panel("Nodos Totales", [create_kpi_panel(None, health.get('number_of_nodes', 0))]), width=4),
        dbc.Col(create_view_panel("Shards No Asignados", [create_kpi_panel(None, unassigned, "danger" if unassigned > 0 else "success")]), width=4),
        dbc.Col(create_view_panel("CPU Promedio", [create_kpi_panel(None, f"{avg_cpu:.1f}%", "warning" if avg_cpu > 75 else "white")]), width=4),
    ])])

def render_node_health_view(analyzer):
    analyzer.fetch_all_data()
    nodes_df = analyzer.nodes_df.sort_values(by='cpu_percent', ascending=False)
    if nodes_df.empty: return create_view_panel("Salud de Nodos", [dbc.Alert("No data.", color="warning")])
    fig = go.Figure(data=[go.Bar(x=nodes_df['node_name'], y=nodes_df['cpu_percent'], name='CPU %', marker_color='#3e92cc')])
    fig.update_layout(template="plotly_dark", paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0.2)', font_color='#a0a8b9')
    return create_view_panel("Uso de CPU por Nodo", [dcc.Graph(figure=fig), html.Hr(), df_to_dbc_table(nodes_df[['node_name', 'tier', 'cpu_percent', 'heap_percent']].round(1))])

def render_shard_distribution_view(analyzer):
    analyzer.fetch_all_data()
    shards_df = analyzer.shards_df.copy()
    if not shards_df.empty:
        shards_df['pattern'] = shards_df['index'].str.extract(r'(^\.?[a-zA-Z_.-]+)')[0].fillna('otros')
        shards_df['datastream'] = shards_df['index'].str.extract(r'^\.ds-([a-zA-Z_.-]+?)-')[0].fillna('No Datastream')
        shards_df['store'] = pd.to_numeric(shards_df['store'], errors='coerce').fillna(0)
        shards_df['docs'] = pd.to_numeric(shards_df['docs'], errors='coerce').fillna(0)

    controls = dbc.Card(dbc.CardBody(dbc.Row([
        dbc.Col([html.Label("Métrica:", className="fw-bold"), dcc.Dropdown(id='treemap-metric-selector', options=[{'label': 'Tamaño (MB)', 'value': 'store'}, {'label': 'Documentos', 'value': 'docs'}], value='store', clearable=False)], width=6),
        dbc.Col([html.Label("Jerarquía:", className="fw-bold"), dcc.Dropdown(id='treemap-hierarchy-selector', options=[{'label': 'Patrón > Nodo', 'value': 'pattern,node'}, {'label': 'Datastream > Nodo', 'value': 'datastream,node'}], value='pattern,node', clearable=False)], width=6),
    ])), className="mb-4")
    return create_view_panel("Distribución de Shards (Treemap Interactivo)", [dcc.Store(id='shard-data-store', data=shards_df.to_dict('records')), controls, dbc.Spinner(dcc.Graph(id='shard-treemap-graph', style={'height': '70vh'}))])

def render_slow_tasks_view(analyzer):
    tasks_data = analyzer.client.get("_tasks", params={'actions': '*search*', 'detailed': 'true'})
    if not tasks_data: return create_view_panel("Tareas Lentas", [dbc.Alert("No se pudo obtener info de tareas.", color="danger")])
    slow_tasks = [{'Nodo': n.get('name'), 'Tiempo (min)': f"{t.get('running_time_in_nanos', 0)/6e10:.2f}", 'Descripción': t.get('description')} for n in tasks_data.get('nodes', {}).values() for t in n.get('tasks', {}).values() if t.get('running_time_in_nanos', 0)/6e10 > 1]
    if not slow_tasks: return create_view_panel("Tareas Lentas", [dbc.Alert("✅ No se detectaron tareas lentas.", color="success")])
    return create_view_panel(f"{len(slow_tasks)} Tareas Lentas Encontradas", [df_to_dbc_table(pd.DataFrame(slow_tasks))])