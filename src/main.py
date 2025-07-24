# main.py
import argparse
import time
from rich.console import Console
from rich.prompt import Prompt, IntPrompt
from rich.rule import Rule
from rich.panel import Panel

# Importamos los componentes desde nuestra estructura en 'src'
from src.client import ElasticsearchClient
from src.analyzer import ClusterAnalyzer
from src.renderer import render_actionable_suggestions_markdown
import src.analysis as analysis
from src.config import ES_HOST, ES_USER, ES_PASS, VERIFY_SSL

console = Console(record=True)

# --- Menús de la Aplicación (Estructura Consolidada) ---

def show_node_analysis_menu(analyzer: ClusterAnalyzer):
    """Submenú para el análisis detallado de nodos."""
    while True:
        console.print(Rule("[bold cyan]🔬 Menú de Análisis de Nodos[/bold cyan]"))
        console.print("1. Dashboard de Causa Raíz (Thread Pools y Breakers en vivo)")
        console.print("2. 🔥 Análisis de Hot Threads (Diagnóstico de Picos de CPU)")
        console.print("3. ⚡ Correlación de Carga (CPU vs Actividad de Shards)")
        console.print("4. ☣️ Análisis de Toxicidad de Shards (Inquilinos Tóxicos)")
        console.print("5. Volver al menú principal")
        choice = IntPrompt.ask("[bold]Selecciona una opción[/bold]", choices=["1", "2", "3", "4", "5"], default=5)
        
        if choice == 1: analysis.analyze_node_deep_dive(analyzer)
        elif choice == 2: analysis.analyze_hot_threads(analyzer)
        elif choice == 3: analysis.analyze_node_load_correlation(analyzer)
        elif choice == 4: analysis.analyze_shard_toxicity(analyzer)
        elif choice == 5: break

def show_index_analysis_menu(analyzer: ClusterAnalyzer):
    """Submenú para el análisis de índices, shards y plantillas."""
    while True:
        console.print(Rule("[bold green]🗂️ Menú de Análisis de Índices y Shards[/bold green]"))
        console.print("1. Distribución de Shards por Patrón de Índice")
        console.print("2. 🔀 Desbalance y Actividad de Shards (Hotspots)")
        console.print("3. 📝 Diagnóstico de Plantillas de Índice (ILM, Shards, etc.)")
        console.print("4. 💥 Análisis de Explosión de Mapeo")
        console.print("5. 🧹 Detección de Shards Vacíos / Polvo")
        console.print("6. Volver al menú principal")
        choice = IntPrompt.ask("[bold]Selecciona una opción[/bold]", choices=["1", "2", "3", "4", "5", "6"], default=6)

        if choice == 1: analysis.analyze_shard_distribution_interactive(analyzer)
        elif choice == 2: analysis.analyze_node_index_correlation(analyzer)
        elif choice == 3: analysis.analyze_index_templates(analyzer)
        elif choice == 4: analysis.analyze_mapping_explosion(analyzer)
        elif choice == 5: analysis.analyze_dusty_shards(analyzer)
        elif choice == 6: break

def show_advanced_analysis_menu(analyzer: ClusterAnalyzer):
    """Submenú para los análisis avanzados, predictivos y de causa raíz."""
    while True:
        console.print(Rule("[bold yellow]🤖 Menú de Análisis Avanzado y Predictivo[/bold yellow]"))
        console.print("1. 🔗 Diagnóstico por Cadenas de Causalidad")
        console.print("2. 📈 Detección de Anomalías Estadísticas (vs. Historial)")
        console.print("3. ⌛ Identificar Tareas de Búsqueda Lentas en el Clúster")
        console.print("4. 🕵️ Detección de Deriva de Configuración (Drift)")
        console.print("5. Volver al menú principal")
        choice = IntPrompt.ask("[bold]Selecciona una opción[/bold]", choices=["1", "2", "3", "4", "5"], default=5)
        
        if choice == 1: analysis.run_causality_chain_analysis(analyzer)
        elif choice == 2: analysis.run_anomaly_detection_analysis(analyzer) # Usamos la nueva función
        elif choice == 3: analysis.analyze_slow_tasks(analyzer)
        elif choice == 4: analysis.analyze_configuration_drift(analyzer)
        elif choice == 5: break


# --- Flujo Principal ---

def main_interactive(client: ElasticsearchClient):
    """Flujo principal para el modo interactivo con el menú consolidado."""
    analyzer = ClusterAnalyzer(client)
    
    console.print(Panel(
        f"[bold]Conectado a clúster: [green]{client.cluster_info['name']}[/green] (Versión: {client.cluster_info['version']})[/bold]",
        title="Conexión Exitosa",
        border_style="green"
    ))
    
    # Mapeo del menú principal a las funciones o submenús
    menu_options = {
        "1": ("📈 Dashboard General en Vivo", analysis.run_live_dashboard),
        "2": ("🔬 Análisis Profundo de Nodos", show_node_analysis_menu),
        "3": ("🗂️ Análisis de Índices y Shards", show_index_analysis_menu),
        "4": ("🤖 Análisis Avanzado y Predictivo", show_advanced_analysis_menu),
        "5": ("🚪 Salir", lambda analyzer: True) # Lambda para manejar la salida
    }

    while True:
        console.print(Rule("[bold magenta]Menú Principal[/bold magenta]"))
        for key, (desc, _) in menu_options.items():
            console.print(f"[bold]{key}[/bold]: {desc}")
        
        choice = Prompt.ask("\n[bold]Elige una opción[/bold]", choices=list(menu_options.keys()), default="1")
        
        action_func = menu_options[choice][1]
        
        # Si la opción es salir, la función lambda devuelve True y rompemos el bucle
        if action_func(analyzer):
            console.print("[bold red]Saliendo del sistema...[/bold red]")
            break

def main_report(client: ElasticsearchClient):
    """Flujo para generar un reporte en formato Markdown y salir."""
    console.print("[bold]Modo Reporte: Generando informe...[/bold]")
    analyzer = ClusterAnalyzer(client)
    
    with console.status("[yellow]Recolectando datos para el reporte...[/yellow]"):
        analyzer.fetch_all_data()
        # Se requiere una segunda captura para calcular tasas de actividad
        time.sleep(2)
        analyzer.fetch_all_data()

    # Usamos la función de renderizado de reportes que ya existía
    render_actionable_suggestions_markdown(analyzer)
    console.print(f"\n[green]Reporte generado y mostrado en la consola.[/green]")

def run():
    """
    Punto de entrada principal que maneja argumentos de línea de comandos
    y decide si ejecutar el modo interactivo o el modo de reporte.
    """
    parser = argparse.ArgumentParser(
        description="Herramienta de Auditoría Profesional para Elasticsearch.",
        formatter_class=argparse.RawTextHelpFormatter # Para un mejor formato de ayuda
    )
    parser.add_argument('--report', action='store_true', help='Genera un reporte en formato Markdown en la consola y sale.')
    parser.add_argument('--host', type=str, help=f"URL del host de Elasticsearch.\n(sobrescribe el valor del .env: {ES_HOST})")
    parser.add_argument('--user', type=str, help="Usuario para la autenticación básica.\n(sobrescribe el valor del .env)")
    parser.add_argument('--password', type=str, help="Contraseña para la autenticación básica.\n(sobrescribe el valor del .env)")
    
    args = parser.parse_args()

    # Determinar credenciales: argumentos de CLI tienen prioridad sobre .env
    es_host = args.host or ES_HOST
    es_user = args.user or ES_USER
    es_pass = args.password or ES_PASS
    
    if not es_host:
        console.print("[bold red]Error: El host de Elasticsearch no está configurado.[/bold red]")
        console.print("Proporciónalo con el parámetro --host o en el archivo .env como ES_HOST.")
        return

    client = ElasticsearchClient(es_host, es_user, es_pass, VERIFY_SSL)
    
    if not client.cluster_info:
        # El constructor del cliente ya imprime un error detallado si falla la conexión.
        return

    try:
        if args.report:
            main_report(client)
        else:
            main_interactive(client)
    except KeyboardInterrupt:
        console.print("\n\n[bold yellow]Operación cancelada por el usuario. Saliendo.[/bold yellow]")
    except Exception:
        console.print(f"[bold red]\n❌ Ocurrió un error fatal inesperado:[/bold red]")
        # Imprime el traceback completo para facilitar la depuración
        console.print_exception(show_locals=True)

if __name__ == "__main__":
    run()