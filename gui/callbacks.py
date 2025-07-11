# gui/callbacks.py
from dash import Input, Output, html, dcc
import dash_bootstrap_components as dbc

from .app import app
from .layout import main_layout
# Importamos nuestros nuevos componentes visuales
from . import components

# Importa y crea una única instancia del cliente y el analizador
from src.client import ElasticsearchClient
from src.analyzer import ClusterAnalyzer
from src.config import ES_HOST, ES_USER, ES_PASS, VERIFY_SSL

# --- Instancias Globales para la GUI ---
es_client = ElasticsearchClient(ES_HOST, ES_USER, ES_PASS, VERIFY_SSL)
analyzer = ClusterAnalyzer(es_client)

# Asigna el layout a la aplicación
app.layout = main_layout()

# --- Callbacks ---

@app.callback(
    Output('header-status', 'children'),
    Input('interval-component', 'n_intervals')
)
def update_header_status(n):
    """Actualiza la cabecera con el estado del clúster."""
    if not es_client.cluster_info:
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
                dbc.Col(
                    dbc.Badge(status, color=status_color, className="ms-1 fs-5"),
                    width='auto'
                ),
                dbc.Col(
                    html.Div(f"Cluster: {health.get('cluster_name')} | Nodos: {health.get('number_of_nodes')}"),
                    className="text-end text-muted small align-self-end"
                )
            ],
            align="center",
            className="p-3 mb-2 bg-dark text-white rounded"
        )
    except Exception as e:
        return dbc.Alert(f"Error al obtener salud del clúster: {e}", color="danger", className="m-3")


@app.callback(
    Output('page-content', 'children'),
    Input('url', 'pathname')
)
def display_page(pathname):
    """Renderiza el contenido de la página según la URL."""
    # Esta es la lógica principal de enrutamiento
    if pathname == '/nodes':
        # Llama a la función del módulo de componentes para crear la vista de nodos
        return components.render_node_health_view(analyzer)
    else:
        # Página por defecto: Llama a la función para crear el dashboard general
        return components.render_dashboard_general(analyzer)