# run.py
import argparse
import webbrowser
from threading import Timer

def run_tui():
    """Lanza la aplicación de terminal."""
    print("Lanzando en modo Terminal (TUI)...")
    # Usamos importlib para evitar importaciones circulares y cargar dinámicamente
    import subprocess
    subprocess.run(["python", "-m", "src.main"])



def run_gui():
    """Lanza la aplicación web gráfica (GUI) con el modo debug activado."""
    print("Lanzando en modo Gráfico (GUI)...")
    
    from gui.app import app
    
    print("Servidor Dash iniciado. Abre tu navegador en http://127.0.0.1:8050")
    print("El modo DEBUG está activado. Verás los errores directamente en el navegador.")
    
    Timer(1, lambda: webbrowser.open("http://127.0.0.1:8050")).start()
    
    # La corrección está aquí:
    app.run(host='0.0.0.0', port=8050, debug=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Elastic Pro Audit Tool - Elige el modo de ejecución.")
    parser.add_argument(
        '--mode', 
        type=str, 
        choices=['tui', 'gui'], 
        default='tui', 
        help="Especifica el modo de interfaz: 'tui' para terminal (default), 'gui' para gráfica."
    )
    args = parser.parse_args()

    if args.mode == 'gui':
        run_gui()
    else:
        run_tui()