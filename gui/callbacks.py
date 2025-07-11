# gui/callbacks.py
from dash import Input, Output, State, html
import dash_bootstrap_components as dbc
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import traceback
from .app import app
from . import components
from src.client import ElasticsearchClient
from src.analyzer import ClusterAnalyzer
from src.config import ES_HOST, ES_USER, ES_PASS, VERIFY_SSL

try:
    analyzer = ClusterAnalyzer(ElasticsearchClient(ES_HOST, ES_USER, ES_PASS, VERIFY_SSL))
    CLIENT_CONNECTED = True
except Exception as e:
    analyzer = None
    CLIENT_CONNECTED = False
    print(f"CRITICAL STARTUP ERROR: {e}")

@app.callback(Output('header-status', 'children'), Input('url', 'pathname'))
def update_header_status(pathname):
    if not CLIENT_CONNECTED: return dbc.Alert("❌ Connection Failed", color="danger")
    health = analyzer.client.get("_cluster/health") or {}
    status = health.get('status', 'N/A').upper()
    color = "success" if status == "GREEN" else "warning" if status == "YELLOW" else "danger"
    return dbc.Row([
        dbc.Col(html.H5(f"Cluster: {health.get('cluster_name')}", className="mb-0 text-white-50"), width='auto'),
        dbc.Col(dbc.Badge(status, color=color, className="ms-2"), width='auto')
    ], align="center")

@app.callback(Output('page-content', 'children'), Input('url', 'pathname'))
def display_page(pathname):
    if not CLIENT_CONNECTED: return dbc.Alert("Cannot connect to Elasticsearch. Check credentials and restart.", color="danger", className="m-4")

    view_map = {
        '/': components.render_dashboard_general,
        '/nodes': components.render_node_health_view,
        '/shard-distribution': components.render_shard_distribution_view,
        '/slow-tasks': components.render_slow_tasks_view,
    }
    try:
        render_function = view_map.get(pathname, lambda a: components.create_view_panel(f"Página no Encontrada: {pathname}", [dbc.Alert("This view is under construction.", color="info")]))
        return dbc.Spinner(children=[render_function(analyzer)], color="primary")
    except Exception as e:
        return dbc.Alert([html.H4("Error al Renderizar Vista"), html.Pre(traceback.format_exc())], color="danger", className="m-4")

@app.callback(
    Output('shard-treemap-graph', 'figure'),
    Input('treemap-metric-selector', 'value'),
    Input('treemap-hierarchy-selector', 'value'),
    State('shard-data-store', 'data')
)
def update_treemap(metric, hierarchy_str, data):
    if not data: return go.Figure().update_layout(template="plotly_dark", paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)')
    df = pd.DataFrame(data)
    path = hierarchy_str.split(',')
    unit = "MB" if metric == 'store' else "Docs"
    fig = px.treemap(df, path=path, values=metric, color=metric, color_continuous_scale='Blues')
    fig.update_layout(margin=dict(t=25, l=10, r=10, b=10), template="plotly_dark", paper_bgcolor='rgba(0,0,0,0)', font_color='#f0f3f6')
    # This is the corrected line with the closing quote
    fig.update_traces(texttemplate="<b>%{label}</b><br>%{value:,.0f} " + unit, hovertemplate='<b>%{label}</b><br>Total: %{customdata[0]:,.0f} ' + unit + '<extra></extra>', customdata=df[[metric]])
    return fig