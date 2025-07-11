# gui/callbacks.py
from dash import Input, Output, html, dcc
import dash_bootstrap_components as dbc
import traceback

# Importaciones de la aplicación y de los componentes visuales
from .app import app
from .layout import main_layout
from . import components

# Importaciones del backend de análisis
from src.client import ElasticsearchClient
from src.analyzer import ClusterAnalyzer
from src.config import ES_HOST, ES_USER, ES_PASS, VERIFY_SSL

# --- Instancias Globales para la GUI ---
# Se crean una sola vez cuando la aplicación se inicia,
# garantizando que todas las callbacks usen el mismo cliente y analizador.
try:
    es_client = ElasticsearchClient(ES_HOST, ES_USER, ES_PASS, VERIFY_SSL)
    analyzer = ClusterAnalyzer(es_client)
    CLIENT_CONNECTED = True
except Exception as e:
    es_client = None
    analyzer = None
    CLIENT_CONNECTED = False
    print(f"ERROR FATAL AL INICIAR: No se pudo crear el cliente de Elasticsearch. {e}")


# Asigna el layout (los "planos") a la aplicación
app.layout = main_layout

# --- Callbacks de la Aplicación ---

@app.callback(
    Output('header-status', 'children'),
    Input('url', 'pathname') # Se actualiza en cada cambio de página
)
def update_header_status(pathname):
    """Actualiza la cabecera con el estado del clúster."""
    if not CLIENT_CONNECTED or not es_client:
        return dbc.Alert("❌ No se pudo conectar a Elasticsearch.", color="danger", className="m-3")
        
    try:
        health = es_client.get("_cluster/health")
        if not health:
            raise ValueError("La respuesta de la API de salud está vacía.")
            
        status = health.get('status', 'N/A').upper()
        status_color = {"GREEN": "success", "YELLOW": "warning", "RED": "danger"}.get(status, "secondary")
        
        return dbc.Row(
            [
                dbc.Col(html.H2("Elastic Pro Audit Console"), width='auto'),
                dbc.Col(dbc.Badge(status, color=status_color, className="ms-1 fs-5"), width='auto'),
                dbc.Col(
                    html.Div(f"Cluster: {health.get('cluster_name')} | Nodos: {health.get('number_of_nodes')}"),
                    className="text-end text-muted small align-self-end"
                )
            ],
            align="center",
            className="p-3 mb-2 bg-dark text-white rounded shadow"
        )
    except Exception as e:
        return dbc.Alert(f"Error al obtener salud del clúster: {e}", color="danger", className="m-3")


@app.callback(
    Output('page-content', 'children'),
    Input('url', 'pathname')
)
def display_page(pathname):
    """
    Enrutador principal: Renderiza la vista correcta y la envuelve en un spinner de carga.
    """
    if not CLIENT_CONNECTED or not analyzer:
         return dbc.Alert("La aplicación no puede funcionar porque no hay conexión a Elasticsearch. Revisa tus variables de entorno y reinicia.", color="danger", className="m-4")

    # El spinner envuelve el contenido. Se muestra mientras el servidor
    # ejecuta la función get_page_content y renderiza la vista.
    return dbc.Spinner(
        children=[get_page_content(pathname)],
        color="primary",
        type="grow"
    )

def get_page_content(pathname):
    """
    Función auxiliar que contiene la lógica de enrutamiento y manejo de errores.
    Llama a la función de renderizado correspondiente desde components.py.
    """
    try:
        if pathname == '/': return components.render_dashboard_general(analyzer)
        elif pathname == '/nodes': return components.render_node_health_view(analyzer)
        elif pathname == '/shard-distribution': return components.render_shard_distribution_view(analyzer)
        elif pathname == '/causality-chain': return components.render_causality_chain_view(analyzer)
        elif pathname == '/shard-toxicity': return components.render_shard_toxicity_view(analyzer)
        elif pathname == '/imbalance': return components.render_imbalance_view(analyzer)
        elif pathname == '/node-load': return components.render_node_load_view(analyzer)
        elif pathname == '/slow-tasks': return components.render_slow_tasks_view(analyzer)
        elif pathname == '/templates': return components.render_templates_view(analyzer)
        elif pathname == '/mapping-explosion': return components.render_mapping_explosion_view(analyzer)
        elif pathname == '/dusty-shards': return components.render_dusty_shards_view(analyzer)
        elif pathname == '/config-drift': return components.render_config_drift_view(analyzer)
        else:
            return dbc.Alert("Error 404: Página no encontrada.", color="danger", className="m-4")
    except Exception:
        # Si algo falla al generar una vista, mostramos el error completo
        # para facilitar la depuración.
        tb_str = traceback.format_exc()
        error_message = html.Div([
            html.H4("Ocurrió un error al renderizar esta vista", className="alert-heading"),
            html.Hr(),
            html.P("El siguiente error impidió que la página se cargara:"),
            html.Code(tb_str, style={'white-space': 'pre-wrap'})
        ])
        return dbc.Alert(error_message, color="danger", className="m-4")