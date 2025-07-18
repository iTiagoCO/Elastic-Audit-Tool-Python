# src/main.py
import httpx
import time
import argparse
from rich.console import Console
from rich.prompt import Prompt
from rich.rule import Rule
from rich.live import Live
import src.renderer as renderer
from src.config import REFRESH_INTERVAL

console = Console()
API_BASE_URL = "http://127.0.0.1:8000"

def check_api_health():
    """Verifica si el servidor de la API está en ejecución."""
    try:
        with console.status("[yellow]Verificando conexión con la API...[/yellow]"):
            response = httpx.get(f"{API_BASE_URL}/health", timeout=2)
            response.raise_for_status()
        console.print("[bold green]✔ Conexión con la API establecida.[/bold green]")
        return True
    except (httpx.RequestError, httpx.HTTPStatusError):
        console.print("\n[bold red]❌ Error: No se pudo conectar al servidor de la API.[/bold red]")
        console.print("Por favor, inicia el servidor en otro terminal con: [cyan]uvicorn src.api:app --reload[/cyan]\n")
        return False

# --- Handlers para el TUI---
def ui_run_live_dashboard():
    try:
        with Live(console=console, screen=True, auto_refresh=False, vertical_overflow="visible") as live:
            while True:
                
                response = httpx.get(f"{API_BASE_URL}/api/v1/live/dashboard", timeout=50.0)
                response.raise_for_status()
                live.update(renderer.render_live_dashboard(response.json()), refresh=True)
                time.sleep(REFRESH_INTERVAL)
    except KeyboardInterrupt: console.print("\n[bold]Volviendo al menú principal...[/bold]")
    except httpx.RequestError as e: console.print(f"[red]Error de API: {e}[/red]")

def create_api_call_handler(endpoint: str, render_function, description: str):
    def handler():
        try:
            with console.status(f"[yellow]{description}...[/yellow]"):
                
                response = httpx.get(f"{API_BASE_URL}/api/v1{endpoint}", timeout=60.0)
                response.raise_for_status()
            render_function(response.json())
        except httpx.RequestError as e: console.print(f"[red]Error de API: {e}[/red]")
        except httpx.HTTPStatusError as e: console.print(f"[red]Error en la respuesta de la API ({e.response.status_code}): {e.response.text}[/red]")
    return handler

def ui_run_deep_dive():
    try:
        with Live(console=console, screen=True, auto_refresh=False) as live:
            while True:
               
                response = httpx.get(f"{API_BASE_URL}/api/v1/live/deep-dive", timeout=10.0)
                response.raise_for_status()
                live.update(renderer.render_deep_dive(response.json()), refresh=True)
                time.sleep(REFRESH_INTERVAL)
    except KeyboardInterrupt: console.print(f"\n[bold]Finalizando diagnóstico profundo...[/bold]")


def ui_run_shard_distribution():
    analysis_type = Prompt.ask("¿Analizar por [1] Patrón de Índice o [2] Índice Individual?", choices=["1", "2"], default="1")
    sort_choices = {"1": ("Total Shards", "total_shards"), "2": ("Tamaño Total (GB)", "total_gb"), "3": ("Primarios", "primaries"), "4": ("Nodos Involucrados", "nodes_involved")}
    console.print("\nElige un criterio para ordenar:"); [console.print(f"  [bold]{k}[/bold]: {v[0]}") for k, v in sort_choices.items()]
    sort_option = Prompt.ask("Opción", choices=list(sort_choices.keys()), default="1")
    group_by, sort_by, sort_desc = ('pattern' if analysis_type == '1' else 'index'), sort_choices[sort_option][1], sort_choices[sort_option][0]
    try:
        with Live(console=console, screen=True, auto_refresh=False) as live:
            while True:
                params = {"group_by": group_by, "sort_by": sort_by}
               
                response = httpx.get(f"{API_BASE_URL}/api/v1/live/shard-distribution", params=params, timeout=10.0)
                response.raise_for_status()
                live.update(renderer.render_shard_distribution(response.json(), group_by, sort_desc), refresh=True)
                time.sleep(REFRESH_INTERVAL)
    except KeyboardInterrupt: console.print("\n[bold]Volviendo al menú...[/bold]")



# --- FUNCIÓN PARA EL REPORTE DETALLADO (JSON) ---
def run_detailed_report_mode(duration: int, analyze: bool):
    """Llama a la API para el reporte técnico con monitoreo."""
    if not check_api_health(): return
    
    console.print(f"[bold]Iniciando modo de reporte detallado (JSON/IA)...[/bold]")
    console.print(f"  - [cyan]Duración:[/cyan] {duration}s. [cyan]Análisis IA:[/cyan] {'Sí' if analyze else 'No'}")
    
    try:
        timeout = float(duration + 120)
        with console.status(f"[yellow]Ejecutando suite con monitoreo durante {duration}s...[/yellow]"):
            params = {"duration": duration, "analyze": analyze}
            response = httpx.post(f"{API_BASE_URL}/api/v1/report/detailed-diagnostic", params=params, timeout=timeout)
            response.raise_for_status()
        
        console.print("\n[bold green]✔ Reporte detallado completado.[/bold green]")
        data = response.json()
        console.print(f"- [green]Detalle:[/green] {data.get('detail')}")
        if 'ai_analysis' in data: console.print(f"- [green]Análisis IA:[/green] {data.get('ai_analysis')}")
        if 'ai_analysis_error' in data: console.print(f"- [bold red]Error IA:[/bold red] {data.get('ai_analysis_error')}")

    except Exception as e:
        console.print_exception(show_locals=True)


# --- FUNCIÓN PARA LA AUDITORÍA COMPLETA (MARKDOWN) ---
def run_full_audit_report_mode():
    """Llama a la API para generar el informe de auditoría completo en Markdown."""
    if not check_api_health(): return
    
    try:
        with console.status("[yellow]Ejecutando auditoría completa... (Esto puede tardar varios segundos)[/yellow]"):
            # Usamos un timeout largo porque ejecuta muchas tareas
            response = httpx.post(f"{API_BASE_URL}/api/v1/report/full-audit", timeout=300.0)
            response.raise_for_status()
        
        console.print("\n[bold green]✔ Auditoría completa finalizada.[/bold green]")
        data = response.json()
        console.print(f"- [green]Detalle:[/green] {data.get('detail')}")
        if 'error' in data:
            console.print(f"- [bold red]Error:[/bold red] {data.get('error')}")

    except Exception as e:
        console.print_exception(show_locals=True)

def main():
    """Función principal que muestra el menú TUI y controla el flujo."""
    console.print(Rule("[bold]Herramienta de Auditoría Profesional para Elasticsearch (Cliente TUI)[/bold]"))
    if not check_api_health(): return

    menu_options = {
        "1": ("📈 Dashboard General en Vivo", ui_run_live_dashboard),
        "2": ("🔬 Dashboard de Causa Raíz (Nodos)", ui_run_deep_dive),
        "3": ("📊 Dashboard de Distribución de Shards", ui_run_shard_distribution),
        "4": ("🔀 Análisis de Desbalance de Shards", create_api_call_handler("/audit/shard-imbalance", renderer.render_shard_imbalance, "Analizando desbalance")),
        "5": ("⚡ Análisis de Carga de Nodos por Shards", create_api_call_handler("/audit/node-load-correlation", renderer.render_node_load_correlation, "Analizando carga de nodos")),
        "6": ("⌛ Identificar Tareas de Búsqueda Lentas", create_api_call_handler("/audit/slow-tasks", renderer.render_slow_tasks, "Consultando tareas lentas")),
        "7": ("📝 Diagnóstico de Plantillas de Índice", create_api_call_handler("/audit/index-templates", renderer.render_index_templates, "Analizando plantillas")),
        "8": ("💥 Análisis de Explosión de Mapeo", create_api_call_handler("/audit/mapping-explosion", renderer.render_mapping_explosion, "Revisando mapeos")),
        "9": ("🧹 Detección de Shards Vacíos / Polvo", create_api_call_handler("/audit/dusty-shards", renderer.render_dusty_shards, "Buscando 'polvo de shards'")),
        "10": ("🕵️ Detección de Deriva de Configuración", create_api_call_handler("/audit/configuration-drift", renderer.render_configuration_drift, "Detectando derivas")),
        "11": ("🔗 Diagnóstico por Cadenas de Causalidad", create_api_call_handler("/audit/causality-chain", renderer.render_causality_chain, "Ejecutando diagnóstico causal")),
        "12": ("☣️ Análisis de Toxicidad de Shards", create_api_call_handler("/audit/shard-toxicity", renderer.render_shard_toxicity, "Buscando inquilinos tóxicos")),
        "salir": ("🚪 Salir", lambda: "exit")
    }
    
    while True:
        console.rule("[bold cyan]Menú Principal de Análisis Experto[/bold cyan]")
        for key, (desc, _) in menu_options.items():
            num_key = key.split('.')[0]
            console.print(f"[bold]{num_key}[/bold]: {desc}")
        
        main_option = Prompt.ask("\n[bold]Elige una opción[/bold]", choices=[k.split('.')[0] for k in menu_options.keys()])
        
        chosen_key = next((k for k in menu_options if k.startswith(main_option)), None)
        if not chosen_key: continue

        action_function = menu_options[chosen_key][1]
        result = action_function()
        
        if result == "exit":
            console.print("[bold red]Saliendo del sistema...[/bold red]"); break

        Prompt.ask("\n[bold]Análisis completado. Presiona Enter para volver al menú...[/bold]")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Herramienta de Auditoría para Elasticsearch.",
        formatter_class=argparse.RawTextHelpFormatter
    )
   
    parser.add_argument(
        '--report', 
        action='store_true', 
        help="Genera el informe de auditoría profesional en Markdown."
    )
    parser.add_argument(
        '--detailed-report', 
        action='store_true', 
        help="Genera un reporte técnico detallado en JSON con monitoreo."
    )
    parser.add_argument(
        '--duration', 
        type=int,
        help="[Opcional] Duración del monitoreo para --detailed-report. Default: 30s."
    )
    parser.add_argument(
        '--analyze', 
        action='store_true', 
        help="[Opcional] Activa el análisis con IA para --detailed-report."
    )
    args = parser.parse_args()

    # --- LÓGICA DE DECISIÓN---
    if args.detailed_report:
        
        duration = args.duration if args.duration else 30
        run_detailed_report_mode(duration=duration, analyze=args.analyze)
    elif args.report:
        
        run_full_audit_report_mode()
    else:
        # Si no hay argumentos de reporte, se ejecuta el menú interactivo
        try:
            main() 
        except KeyboardInterrupt:
            console.print("\n[bold]Saliendo...[/bold]")