import argparse
import os
import glob
from markdown_pdf import MarkdownPdf, Section


CSS_STYLE = """
@page {
    size: tabloid;
    margin: 1.5cm;
}

body {
    font-family: "Helvetica Neue", Arial, "sans-serif";
    line-height: 1.7;
    color: #212529; /* Texto principal casi negro para máximo contraste */
    background-color: #fff;
    max-width: 1200px;
    margin: 0 auto;
}

h1, h2, h3, h4 {
    color: #000;
    font-weight: 600;
    margin-top: 1.8em;
    margin-bottom: 1em;
    line-height: 1.3;
}

h1 {
    font-size: 2.8em;
    text-align: center;
    border-bottom: none;
    margin-bottom: 1.5em;
}

h2 {
    font-size: 2.2em;
    border-bottom: 2px solid #343a40; /* Línea inferior más gruesa y oscura */
    padding-bottom: 10px;
}

h3 {
    font-size: 1.6em;
    border-bottom: 1px solid #6c757d; /* Línea inferior más sutil */
    padding-bottom: 8px;
}

p {
    margin-bottom: 1.2em;
}

/* --- ESTILOS DE TABLA MINIMALISTAS --- */
table {
    width: 100%;
    border-collapse: collapse;
    margin: 2em 0;
    font-size: 0.95em;
    border: 1px solid #dee2e6; /* Borde sutil alrededor de toda la tabla */
}

th, td {
    padding: 12px 15px;
    text-align: left;
    border: 1px solid #dee2e6; /* Bordes para todas las celdas, creando una grilla clara */
}

thead th {
    background-color: #f8f9fa; /* Fondo gris muy claro para la cabecera */
    font-weight: bold;
    color: #212529; /* Texto oscuro en la cabecera */
    font-size: 1em;
    border-bottom: 2px solid #dee2e6; /* Borde inferior más grueso para la cabecera */
}

/* Se elimina el 'zebra-striping' para un look más limpio, pero se mantiene un hover */
tbody tr:hover {
    background-color: #f1f3f5;
}

code {
    background-color: #e9ecef;
    padding: 3px 6px;
    border-radius: 4px;
    font-family: "Courier New", Courier, monospace;
    color: #343a40;
}
"""

def find_latest_report(pattern="Informe_Auditoria_Completa_*.md"):
    """Encuentra el archivo de reporte más reciente que coincida con el patrón."""
    root_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
    search_path = os.path.join(root_dir, pattern)
    list_of_files = glob.glob(search_path)
    if not list_of_files:
        return None
    return max(list_of_files, key=os.path.getctime)

def convert_md_to_pdf(input_file, output_file):
    """
    Lee un archivo Markdown y lo convierte a PDF usando markdown-pdf.
    """
    print(f"📄 Leyendo el archivo de entrada: {input_file}")
    
    try:
        with open(input_file, 'r', encoding='utf-8') as f:
            md_content = f.read()
    except FileNotFoundError:
        print(f"❌ Error: No se encontró el archivo de entrada '{input_file}'.")
        print("Asegúrate de haber generado primero el reporte con 'python -m src.main --report'")
        return

    print("🔄 Creando instancia de PDF...")
    
    pdf = MarkdownPdf(toc_level=2)

    section = Section(
        md_content,
        toc=False
    )
    
    pdf.add_section(section, user_css=CSS_STYLE)

    print(f"🎨 Guardando el PDF en: {output_file}")
    pdf.save(output_file)
    
    print(f"✅ ¡Éxito! El reporte ha sido guardado como: {output_file}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Convierte un informe de auditoría de Markdown a PDF con un estilo profesional."
    )
    
    parser.add_argument(
        '--input',
        type=str,
        default=None,
        help="Ruta al archivo Markdown de entrada. Si no se especifica, busca el último reporte generado."
    )
    parser.add_argument(
        '--output',
        type=str,
        default=None,
        help="Ruta para el archivo PDF de salida. Si no se especifica, se basa en el nombre del archivo de entrada."
    )
    
    args = parser.parse_args()
    
    input_md = args.input
    if not input_md:
        print("🔍 No se especificó un archivo de entrada. Buscando el último reporte generado...")
        input_md = find_latest_report()
        if not input_md:
            print("❌ No se encontraron reportes de auditoría. Por favor, genera uno primero.")
            exit()
        print(f"👍 Reporte encontrado: {input_md}")

    output_pdf = args.output
    if not output_pdf:
        base_name = os.path.splitext(os.path.basename(input_md))[0]
        root_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
        output_pdf = os.path.join(root_dir, f"{base_name}.pdf")
        
    convert_md_to_pdf(input_md, output_pdf)