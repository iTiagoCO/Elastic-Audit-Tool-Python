# gui/app.py
import dash
import dash_bootstrap_components as dbc
from .layout import main_layout

# Inicializa la aplicación Dash
app = dash.Dash(
    __name__, 
    external_stylesheets=[dbc.themes.VAPOR], 
    suppress_callback_exceptions=True
)
server = app.server

#  Asignar el layout (los "planos") a la aplicación.
#    Ahora esto se hace ANTES de que el servidor se inicie.
app.layout = main_layout()

#  Importa el módulo de callbacks.
#    Simplemente importar este archivo hace que Dash reconozca
#    todas las funciones interactivas que hemos definido allí.
from . import callbacks