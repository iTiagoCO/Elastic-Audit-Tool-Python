# gui/layout.py
from dash import dcc, html
import dash_bootstrap_components as dbc

def main_layout():
    sidebar = dbc.Col(
        [
            html.H3([html.I(className="fas fa-shield-alt me-2"), "Elastic Pro"], className="text-white my-3"),
            html.P("Audit Console", className="text-muted"),
            html.Hr(className="text-muted"),
            dbc.Nav(
                [
                    dbc.NavLink([html.I(className="fas fa-th-large me-2"), "Dashboard"], href="/", active="exact"),
                    dbc.NavLink([html.I(className="fas fa-server me-2"), "Salud de Nodos"], href="/nodes", active="exact"),
                    dbc.NavLink([html.I(className="fas fa-sitemap me-2"), "Distribución"], href="/shard-distribution", active="exact"),
                    html.P("Análisis de Causa Raíz", className="mt-4 text-muted small text-uppercase fw-bold"),
                    dbc.NavLink([html.I(className="fas fa-hourglass-half me-2"), "Tareas Lentas"], href="/slow-tasks", active="exact"),
                ],
                vertical=True, pills=True,
            ),
        ],
        width=2, className="sidebar",
    )
    content = dbc.Col(
        [
            html.Div(id='header-status', className="p-3"),
            dcc.Location(id='url', refresh=False),
            html.Div(id='page-content', className="p-4")
        ],
        width=10,
    )
    return dbc.Container([dbc.Row([sidebar, content], className="g-0")], fluid=True)