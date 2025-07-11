# gui/layout.py
from dash import dcc, html
import dash_bootstrap_components as dbc

# Aquí definiremos la estructura visual de la página
def main_layout():
    return dbc.Container(
        [
            # Intervalo para actualizar componentes de forma automática
            dcc.Interval(
                id='interval-component',
                interval=10*1000, # en milisegundos (ej: 10 segundos)
                n_intervals=0
            ),

            # Header que será actualizado por un callback
            html.Div(id='header-status'),

            html.Hr(), # Una línea divisoria

            # Contenido principal
            dbc.Row(
                [
                    # Sidebar de navegación
                    dbc.Col(
                        [
                            html.H4("Análisis"),
                            dbc.Nav(
                                [
                                    dbc.NavLink("Dashboard General", href="/", active="exact"),
                                    dbc.NavLink("Salud de Nodos", href="/nodes", active="exact"),
                                    # ... aquí irán más links
                                ],
                                vertical=True,
                                pills=True,
                            ),
                        ],
                        width=2,
                    ),

                    # Área de contenido que cambiará según la URL
                    dbc.Col(
                        [
                            dcc.Location(id='url', refresh=False),
                            html.Div(id='page-content')
                        ],
                        width=10,
                    ),
                ]
            )
        ],
        # La línea clave que faltaba: aplica la clase de texto del tema al contenedor
        fluid=True,
        className="dbc" 
    )