# main.py
import argparse
import time
from rich.console import Console
from rich.prompt import Prompt
from rich.rule import Rule

# Importamos los componentes desde nuestra nueva estructura en 'src'
from src.client import ElasticsearchClient
from src.analyzer import ClusterAnalyzer
from src.renderer import render_historical_report, render_actionable_suggestions_markdown
import src.analysis as analysis
from src.config import ES_HOST, ES_USER, ES_PASS, VERIFY_SSL

console = Console(record=True)

def main():
    """Función principal que muestra el menú y controla el flujo."""
    console.print(Rule("[bold]Herramienta de Auditoría Profesional para Elasticsearch[/bold]"))
    
    # Inicializa el cliente y el analizador
    client = ElasticsearchClient(ES_HOST, ES_USER, ES_PASS, VERIFY_SSL)
    if not client.cluster_info:
        return

    analyzer = ClusterAnalyzer(client)
    
    menu_options = {
    "1": ("📈 Dashboard General en Vivo", analysis.run_live_dashboard),
    "2": ("🔬 Dashboard de Causa Raíz (Nodos)", analysis.analyze_node_deep_dive),
    "3": ("📊 Dashboard de Distribución de Shards", analysis.analyze_shard_distribution_interactive),
    "4": ("🔀 Análisis de Desbalance de Shards", analysis.analyze_node_index_correlation),
    "5": ("⚡ Análisis de Carga de Nodos por Shards", analysis.analyze_node_load_correlation),
    "6": ("⌛ Identificar Tareas de Búsqueda Lentas", analysis.analyze_slow_tasks),
    "7.1": ("📝 Diagnóstico de Plantillas de Índice", analysis.analyze_index_templates),
    "7.2": ("💥 Análisis de Explosión de Mapeo", analysis.analyze_mapping_explosion),
    "8": ("🧹 Detección de Shards Vacíos / Polvo", analysis.analyze_dusty_shards),
    "9": ("🕵️  Detección de Deriva de Configuración (Drift)", analysis.analyze_configuration_drift),
    "10": ("🔗 Diagnóstico por Cadenas de Causalidad", analysis.run_causality_chain_analysis),
    "11": ("☣️ Análisis de Toxicidad de Shards", analysis.analyze_shard_toxicity),
    "salir": ("🚪 Salir", lambda analyzer: None)
}
    
    while True:
        console.rule("[bold cyan]Menú Principal de Análisis Experto[/bold cyan]")
        for key, (desc, _) in menu_options.items():
            console.print(f"[bold]{key}[/bold]: {desc}")
        
        main_option = Prompt.ask("\n[bold]Elige una opción[/bold]", choices=list(menu_options.keys()), default="1")
        
        if main_option == "salir":
            console.print("[bold red]Saliendo del sistema...[/bold red]")
            break

        # Llama a la función de análisis correspondiente
        action_func = menu_options[main_option][1]
        action_func(analyzer) # Pasamos el objeto analyzer
        
        console.print("\n[green]Operación completada. Volviendo al menú principal...[/green]")
        time.sleep(1)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Herramienta de Auditoría Profesional para Elasticsearch.")
    parser.add_argument('--report', action='store_true', help='Genera un reporte en formato Markdown y sale.')
    args = parser.parse_args()

    if args.report:
        client = ElasticsearchClient(ES_HOST, ES_USER, ES_PASS, VERIFY_SSL)
        if client.cluster_info:
            analyzer = ClusterAnalyzer(client)
            # Fetch de datos con un pequeño delay para asegurar tasas
            analyzer.fetch_all_data()
            time.sleep(2) 
            analyzer.fetch_all_data()

            render_actionable_suggestions_markdown(analyzer)
    else:
        try:
            main()
        except KeyboardInterrupt:
            console.print("\n[bold]Interrupción por teclado. Saliendo...[/bold]")
        except Exception as e:
            console.print(f"[bold red]❌ Ocurrió un error fatal:[/bold red]")
            console.print_exception(show_locals=True)