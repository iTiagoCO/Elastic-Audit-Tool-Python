# gui/app.py
import dash
import dash_bootstrap_components as dbc

app = dash.Dash(
    __name__,
    external_stylesheets=[dbc.themes.BOOTSTRAP, 'https://use.fontawesome.com/releases/v5.8.1/css/all.css'],
    suppress_callback_exceptions=True
)
server = app.server

from . import layout, callbacks
app.layout = layout.main_layout()