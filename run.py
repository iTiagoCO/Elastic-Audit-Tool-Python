# run.py
import sys
import subprocess

def run_tui():
    """Lanza la aplicación de terminal."""
    print("Lanzando en modo Terminal (TUI)...")
    try:
       
        subprocess.run([sys.executable, "-m", "src.main"], check=True)
    except KeyboardInterrupt:
        print("\n[bold red]TUI interrumpido por el usuario.[/bold red]")
    except subprocess.CalledProcessError as e:
        print(f"\n[bold red]❌ Error en ejecución del módulo TUI:[/bold red] {e}")

if __name__ == "__main__":
    
    run_tui()