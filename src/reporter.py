# src/reporter.py
import time
import json
import uuid
import logging
from openai import AzureOpenAI
import json


from src.analyzer import ClusterAnalyzer
from src.analysis import (
    get_live_dashboard_data, get_deep_dive_data, get_shard_distribution_data,
    analyze_node_index_correlation, analyze_node_load_correlation, analyze_slow_tasks,
    analyze_index_templates, analyze_mapping_explosion, analyze_dusty_shards,
    analyze_configuration_drift, run_causality_chain_analysis, analyze_shard_toxicity
)
from src.config import (
    REFRESH_INTERVAL, AZURE_OPENAI_KEY, AZURE_OPENAI_ENDPOINT, AZURE_OPENAI_DEPLOYMENT_NAME
)



# --- SUITE PARA EL INFORME DE AUDITORÍA COMPLETA (.md) ---

FULL_AUDIT_SUITE = [
    ("1. Resumen del Clúster", get_live_dashboard_data, lambda data: _format_dashboard(data)),
    ("2. Correlación de Carga en Nodos", analyze_node_load_correlation, lambda data: _format_dataframe(data, 'node_loads', "Carga de Nodos")),
    ("3. Desbalance de Shards por Patrón", analyze_node_index_correlation, lambda data: _format_imbalance(data)),
    ("4. Tareas de Búsqueda Lentas", analyze_slow_tasks, lambda data: _format_dataframe(data, 'tasks', "Tareas Lentas")),
    ("5. Diagnóstico de Plantillas de Índice", analyze_index_templates, lambda data: _format_dataframe(data, 'templates', "Plantillas de Índice")),
    ("6. Análisis de Explosión de Mapeo", analyze_mapping_explosion, lambda data: _format_dataframe(data, 'indices', "Riesgo de Explosión de Mapeo")),
    ("7. Detección de Shards 'Polvorientos'", analyze_dusty_shards, lambda data: _format_dusty_shards(data)),
    ("8. Deriva de Configuración", analyze_configuration_drift, lambda data: _format_list(data, 'drifts', "Derivas de Configuración Encontradas")),
    ("9. Análisis de Cadenas de Causalidad", run_causality_chain_analysis, lambda data: _format_causality(data)),
    ("10. Análisis de 'Inquilinos Tóxicos'", analyze_shard_toxicity, lambda data: _format_dataframe(data, 'toxic_tenants', "Inquilinos con Búsquedas Costosas")),
]

# --- SUITE PARA EL REPORTE DETALLADO (JSON) ---
ANALYSIS_SUITE = [
    {
        "name": "Live Dashboard Data",
        "function": get_live_dashboard_data,
        "description": "Salud general, estadísticas, y estado de nodos e índices."
    },
    {
        "name": "Node Load Correlation",
        "function": analyze_node_load_correlation,
        "description": "Correlación entre CPU, Heap y carga de trabajo en los nodos."
    },
    {
        "name": "Slow Task Analysis",
        "function": analyze_slow_tasks,
        "description": "Detección de tareas de búsqueda lentas en ejecución."
    }
]


# --- SECCIÓN DE FUNCIONES DE FORMATEO---

def _format_dashboard(data):
    health = data.get('cluster_health', {})
    stats = data.get('cluster_stats', {})
    status = health.get('status', 'N/A').upper()
    return [
        f"| Métrica | Valor |",
        f"|---|---|",
        f"| **Estado del Clúster** | **{status}** |",
        f"| Nodos Totales | {stats.get('nodes', {}).get('count', {}).get('total', 'N/A')} |",
        f"| Índices Totales | {stats.get('indices', {}).get('count', 'N/A')} |",
        f"| Shards Totales | {stats.get('indices', {}).get('shards', {}).get('total', 'N/A')} |",
        f"| Documentos | {stats.get('indices', {}).get('docs', {}).get('count', 'N/A')} |",
        f"| Tamaño en Disco | {stats.get('indices', {}).get('store', {}).get('size_in_bytes', 'N/A')} |",
        f"| Shards No Asignados | {health.get('unassigned_shards', 0)} |"
    ]

def _format_dataframe(data, key, title):
    items = data.get(key)
    if not isinstance(items, list) or not items:
        return ["✅ No se encontraron problemas o datos para este análisis."]
    
    headers = list(items[0].keys())
    table = [f"| {' | '.join(h.replace('_', ' ').title() for h in headers)} |"]
    table.append(f"|{'---|' * len(headers)}")
    for item in items:
        row = ' | '.join(str(item.get(h, '')) for h in headers)
        table.append(f"| {row} |")
    return table

def _format_list(data, key, title):
    items = data.get(key)
    if not items:
        return ["✅ No se encontraron problemas para este análisis."]
    return [f"- `{item}`" for item in items]

def _format_imbalance(data):
    patterns = data.get('imbalanced_patterns', [])
    if not patterns:
        return ["✅ No se detectó desbalance significativo de shards."]
    
    lines = []
    for p in patterns:
        info = p.get('pattern_info', {})
        lines.append(f"#### Patrón: `{info.get('pattern')}` (Desviación Estándar: {info.get('std_dev', 0):.2f})")
        lines.extend(_format_dataframe(p, 'nodes', ''))
        lines.append("")
    return lines

def _format_dusty_shards(data):
    empty = data.get('empty_shards', [])
    dusty = data.get('dusty_shards', [])
    if not empty and not dusty:
        return ["✅ No se encontraron shards vacíos o 'polvorientos'."]
    
    lines = []
    if empty:
        lines.append("#### Shards Vacíos Encontrados")
        lines.extend(_format_dataframe({'shards': empty}, 'shards', ''))
    if dusty:
        lines.append("#### Shards 'Polvorientos' Encontrados (Pequeños)")
        lines.extend(_format_dataframe({'shards': dusty}, 'shards', ''))
    return lines

def _format_causality(data):
    reports = data.get('reports', [])
    if not reports:
        return ["✅ No se encontraron cadenas de causalidad para nodos con alta presión de memoria."]
    
    lines = []
    for r in reports:
        lines.extend(r.get('report_lines', []))
        lines.append("")
    return lines


# --- SECCIÓN DE FUNCIONES DE REPORTE ---

def generate_full_audit_report(analyzer: ClusterAnalyzer) -> dict:
    """Ejecuta cada análisis de la suite y crea un informe profesional en Markdown."""
    try:
        report_parts = [f"# Informe de Auditoría de Elasticsearch", f"**Fecha:** {time.strftime('%Y-%m-%d %H:%M:%S')}"]
        logging.info("Iniciando auditoría completa para reporte Markdown...")

        for name, analysis_func, format_func in FULL_AUDIT_SUITE:
            report_parts.append(f"\n---\n\n## {name}\n")
            try:
                logging.info(f"Ejecutando análisis: {name}...")
                data = analysis_func(analyzer)
                formatted_lines = format_func(data)
                report_parts.extend(formatted_lines)
            except Exception as e:
                logging.error(f"Error en análisis '{name}': {e}", exc_info=True)
                report_parts.append(f"**❌ Error durante este análisis:** `{str(e)}`")

        final_report_str = "\n".join(report_parts)
        report_filename = f"Informe_Auditoria_Completa_{time.strftime('%Y%m%d_%H%M%S')}.md"
        
        with open(report_filename, 'w', encoding='utf-8') as f:
            f.write(final_report_str)
            
        return {"detail": f"Informe de auditoría completo guardado en '{report_filename}'."}
    except Exception as e:
        logging.error(f"Error generando informe de auditoría: {e}", exc_info=True)
        return {"error": str(e)}


def generate_detailed_report(analyzer: ClusterAnalyzer, duration: int, analyze_ia: bool) -> dict:
    """Orquesta la ejecución de la suite de análisis detallado (JSON/IA)."""
    full_report_data = {
        item["name"]: {"description": item["description"], "observations": []}
        for item in ANALYSIS_SUITE
    }

    end_time = time.time() + duration
    logging.info(f"Iniciando ciclo de recolección para reporte detallado durante {duration}s...")

    while time.time() < end_time:
        timestamp = time.strftime('%Y-%m-%d %H:%M:%S')
        for analysis_item in ANALYSIS_SUITE:
            name = analysis_item["name"]
            func = analysis_item["function"]
            try:
                result = func(analyzer)
                full_report_data[name]["observations"].append({"timestamp": timestamp, "result": result})
            except Exception as e:
                logging.error(f"Error en análisis detallado '{name}': {e}", exc_info=True)
                full_report_data[name]["observations"].append({"timestamp": timestamp, "error": str(e)})
        time.sleep(REFRESH_INTERVAL)

    reporte_final = {
        "metadata": {"report_id": str(uuid.uuid4()), "fecha_generacion": time.strftime('%Y-%m-%d %H:%M:%S'), "duracion_monitoreo_seg": duration},
        "diagnostic_suite_results": full_report_data
    }
    
    report_filename = "reporte_diagnostico_detallado.json"
    with open(report_filename, 'w', encoding='utf-8') as f:
        json.dump(reporte_final, f, indent=2, ensure_ascii=False)
        
    response = {"detail": f"Reporte detallado guardado en '{report_filename}'."}

    if analyze_ia:
        try:
            ai_analysis = _analyze_with_ai(json.dumps(reporte_final))
            ai_filename = "analisis_experto_ia.md"
            with open(ai_filename, 'w', encoding='utf-8') as f:
                f.write(ai_analysis)
            response["ai_analysis"] = f"Análisis de IA guardado en '{ai_filename}'."
        except Exception as e:
            response["ai_analysis_error"] = str(e)
            
    return response

def _analyze_with_ai(report_content: str) -> str:
    """Envía el contenido del reporte a Azure OpenAI para análisis."""
    if not all([AZURE_OPENAI_KEY, AZURE_OPENAI_ENDPOINT, AZURE_OPENAI_DEPLOYMENT_NAME]):
        raise ValueError("Credenciales de Azure OpenAI no configuradas en el archivo .env.")

    client = AzureOpenAI(api_key=AZURE_OPENAI_KEY, api_version="2024-02-01", azure_endpoint=AZURE_OPENAI_ENDPOINT)
    prompt_sistema = """
    Actúa como un Ingeniero de Confiabilidad de Sitios (SRE) experto en Elastic. He generado un reporte con observaciones de un clúster a lo largo del tiempo. 
    Analiza los datos de TODAS las secciones, busca correlaciones y patrones preocupantes, y proporciona un análisis profesional con dos secciones en Markdown:
    ## 1. Análisis y Correlación de Hallazgos
    - Resume el estado general.
    - Correlaciona eventos (ej. 'un pico de CPU en el nodo X coincidió con un aumento en la tasa de indexación del índice Y').
    - Identifica métricas anómalas o tendencias preocupantes.
    ## 2. Sugerencias y Pasos a Seguir
    - Proporciona una lista de acciones recomendadas y priorizadas.
    - Sugiere comandos de diagnóstico específicos.
    - Ofrece recomendaciones de configuración para mejorar la resiliencia.
    """
    response = client.chat.completions.create(
        model=AZURE_OPENAI_DEPLOYMENT_NAME,
        messages=[
            {"role": "system", "content": prompt_sistema},
            {"role": "user", "content": f"Analiza el siguiente reporte de diagnóstico:\n\n{report_content}"}
        ]
    )
    return response.choices[0].message.content