# src/api.py
from fastapi import FastAPI, Depends, HTTPException, Query
import src.analysis as analysis_funcs
from .client import ElasticsearchClient
from .analyzer import ClusterAnalyzer
from .config import ES_HOST, ES_USER, ES_PASS, VERIFY_SSL

app = FastAPI(title="Elastic Pro Audit Tool API")

def get_analyzer():
    client = ElasticsearchClient(ES_HOST, ES_USER, ES_PASS, VERIFY_SSL)
    if not client.cluster_info: raise HTTPException(status_code=503, detail="No se pudo conectar a Elasticsearch.")
    yield ClusterAnalyzer(client)

# --- Endpoints para Dashboards en Vivo ---
@app.get("/api/v1/live/dashboard", tags=["Dashboards en Vivo"])
def ep_get_dashboard_data(analyzer: ClusterAnalyzer = Depends(get_analyzer)):
    return analysis_funcs.get_live_dashboard_data(analyzer)

@app.get("/api/v1/live/deep-dive", tags=["Dashboards en Vivo"])
def ep_get_deep_dive_data(analyzer: ClusterAnalyzer = Depends(get_analyzer)):
    return analysis_funcs.get_deep_dive_data(analyzer)

@app.get("/api/v1/live/shard-distribution", tags=["Dashboards en Vivo"])
def ep_get_shard_distribution_data(
    group_by: str = Query("pattern", enum=["pattern", "index"]),
    sort_by: str = Query("total_shards", enum=["total_shards", "total_gb", "primaries", "nodes_involved"]),
    analyzer: ClusterAnalyzer = Depends(get_analyzer)):
    return analysis_funcs.get_shard_distribution_data(analyzer, group_by, sort_by)


# --- Endpoints para Análisis Estáticos ---
@app.get("/api/v1/audit/node-load-correlation", tags=["Análisis de Carga"])
def ep_node_load(analyzer: ClusterAnalyzer = Depends(get_analyzer)):
    return analysis_funcs.analyze_node_load_correlation(analyzer)

@app.get("/api/v1/audit/shard-imbalance", tags=["Análisis de Carga"])
def ep_shard_imbalance(analyzer: ClusterAnalyzer = Depends(get_analyzer)):
    return analysis_funcs.analyze_node_index_correlation(analyzer)
    
@app.get("/api/v1/audit/shard-toxicity", tags=["Análisis de Carga"])
def ep_shard_toxicity(analyzer: ClusterAnalyzer = Depends(get_analyzer)):
    return analysis_funcs.analyze_shard_toxicity(analyzer)

@app.get("/api/v1/audit/slow-tasks", tags=["Análisis de Rendimiento"])
def ep_slow_tasks(analyzer: ClusterAnalyzer = Depends(get_analyzer)):
    return analysis_funcs.analyze_slow_tasks(analyzer)

@app.get("/api/v1/audit/causality-chain", tags=["Análisis de Rendimiento"])
def ep_causality_chain(analyzer: ClusterAnalyzer = Depends(get_analyzer)):
    return analysis_funcs.run_causality_chain_analysis(analyzer)

@app.get("/api/v1/audit/index-templates", tags=["Análisis de Configuración"])
def ep_templates(analyzer: ClusterAnalyzer = Depends(get_analyzer)):
    return analysis_funcs.analyze_index_templates(analyzer)

@app.get("/api/v1/audit/mapping-explosion", tags=["Análisis de Configuración"])
def ep_mapping(analyzer: ClusterAnalyzer = Depends(get_analyzer)):
    return analysis_funcs.analyze_mapping_explosion(analyzer)

@app.get("/api/v1/audit/dusty-shards", tags=["Análisis de Configuración"])
def ep_dusty(analyzer: ClusterAnalyzer = Depends(get_analyzer)):
    return analysis_funcs.analyze_dusty_shards(analyzer)

@app.get("/api/v1/audit/configuration-drift", tags=["Análisis de Configuración"])
def ep_drift(analyzer: ClusterAnalyzer = Depends(get_analyzer)):
    return analysis_funcs.analyze_configuration_drift(analyzer)

@app.get("/health", tags=["Sistema"])
def health_check():
    return {"status": "ok"}

@app.get("/api/v1/report/suggestions", tags=["Reportes"])
def ep_get_report_suggestions(analyzer: ClusterAnalyzer = Depends(get_analyzer)):
    """
    Genera una lista de sugerencias accionables para un reporte estático.
    """
    return analysis_funcs.generate_report_data(analyzer)