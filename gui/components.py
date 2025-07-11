# gui/components.py
from dash import dcc, html
import dash_bootstrap_components as dbc
import plotly.graph_objects as go

def create_kpi_card(title, value, color):
    """Crea una tarjeta para un Indicador Clave de Rendimiento (KPI)."""
    return dbc.Card(
        dbc.CardBody(
            [
                html.H4(title, className="card-title text-muted"),
                html.H2(value, className=f"card-text text-{color}"),
            ]
        ),
        className="text-center m-2",
    )

def render_dashboard_general(analyzer):
    """Crea el layout para el Dashboard General con KPIs."""
    analyzer.fetch_all_data()
    health = analyzer.cluster_health
    nodes_df = analyzer.nodes_df

    # Preparar los valores para las tarjetas KPI
    unassigned_shards = health.get('unassigned_shards', 0)
    unassigned_color = "danger" if unassigned_shards > 0 else "light"

    avg_cpu = nodes_df['cpu_percent'].mean() if not nodes_df.empty else 0
    cpu_color = "warning" if avg_cpu > 75 else "light"

    avg_heap = nodes_df['heap_percent'].mean() if not nodes_df.empty else 0
    heap_color = "warning" if avg_heap > 75 else "light"

    return html.Div([
        html.H3("Dashboard General"),
        dbc.Row([
            dbc.Col(create_kpi_card("Nodos Totales", health.get('number_of_nodes', 0), "light")),
            dbc.Col(create_kpi_card("Shards No Asignados", unassigned_shards, unassigned_color)),
            dbc.Col(create_kpi_card("CPU Promedio", f"{avg_cpu:.1f}%", cpu_color)),
            dbc.Col(create_kpi_card("Heap Promedio", f"{avg_heap:.1f}%", heap_color)),
        ])
    ])

def render_node_health_view(analyzer):
    """Crea el layout para la vista de Salud de Nodos con gráficos."""
    analyzer.fetch_all_data()
    nodes_df = analyzer.nodes_df.sort_values(by='tier')

    if nodes_df.empty:
        return dbc.Alert("No se pudieron obtener datos de los nodos.", color="warning")

    # Crear el gráfico interactivo
    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=nodes_df['node_name'],
        y=nodes_df['cpu_percent'],
        name='CPU %',
        marker_color='rgb(26, 118, 255)'
    ))
    fig.add_trace(go.Bar(
        x=nodes_df['node_name'],
        y=nodes_df['heap_percent'],
        name='Heap %',
        marker_color='rgb(55, 83, 109)'
    ))

    # Personalizar el estilo del gráfico para el tema oscuro
    fig.update_layout(
        title_text='Uso de CPU y Heap por Nodo',
        barmode='group',
        xaxis_tickangle=-45,
        template="plotly_dark",  # ¡Tema oscuro para el gráfico!
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
    )

    return html.Div([
        html.H3("Salud Detallada de Nodos"),
        dcc.Graph(id='node-health-graph', figure=fig)
    ])