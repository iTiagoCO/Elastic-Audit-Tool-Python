# gui/layout.py
from dash import dcc, html
import dash_bootstrap_components as dbc

def main_layout():
    """Define la estructura visual completa de la página."""
    sidebar = dbc.Col(
        [
            html.H3("Elastic Pro", className="display-6"),
            html.P("Audit Console", className="lead"),
            html.Hr(),
            dbc.Nav(
                [
                    dbc.NavLink("📈 Dashboard General", href="/", active="exact"),
                    dbc.NavLink("🔬 Salud de Nodos", href="/nodes", active="exact"),
                    dbc.NavLink("📊 Distribución de Shards", href="/shard-distribution", active="exact"),
                    html.Hr(),
                    html.H5("Análisis de Causa Raíz", className="mt-3"),
                    dbc.NavLink("🔗 Cadenas de Causalidad", href="/causality-chain", active="exact"),
                    dbc.NavLink("☣️ Toxicidad de Shards", href="/shard-toxicity", active="exact"),
                    dbc.NavLink("🔀 Desbalance de Shards", href="/imbalance", active="exact"),
                    dbc.NavLink("⚡ Carga de Nodos", href="/node-load", active="exact"),
                    dbc.NavLink("⌛ Tareas Lentas", href="/slow-tasks", active="exact"),
                    html.Hr(),
                    html.H5("Auditoría de Configuración", className="mt-3"),
                    dbc.NavLink("📝 Plantillas de Índice", href="/templates", active="exact"),
                    dbc.NavLink("💥 Explosión de Mapeo", href="/mapping-explosion", active="exact"),
                    dbc.NavLink("🧹 Shards Vacíos / Polvo", href="/dusty-shards", active="exact"),
                    dbc.NavLink("🕵️ Deriva de Configuración", href="/config-drift", active="exact"),
                ],
                vertical=True,
                pills=True,
            ),
        ],
        width=2,
    )

    content = dbc.Col(
        [
            dcc.Location(id='url', refresh=False),
            dcc.Store(id='page-load-store'), # Para disparar la carga de la página
            html.Div(id='page-content')
        ],
        width=10,
    )

    return dbc.Container(
        [
            html.Div(id='header-status'),
            html.Hr(),
            dbc.Row([sidebar, content])
        ],
        fluid=True,
        className="dbc"
    )