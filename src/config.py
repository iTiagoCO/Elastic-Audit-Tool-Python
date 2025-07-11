# src/config.py
import os
import logging
from dotenv import load_dotenv
import urllib3

# --- Configuración Inicial ---
load_dotenv()
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Configuración del logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename='audit_debug.log',
    filemode='w'
)

# --- Conexión a Elasticsearch ---
ES_HOST = os.getenv("ES_HOST")
ES_USER = os.getenv("ES_USER")
ES_PASS = os.getenv("ES_PASS")
VERIFY_SSL = False
HEADERS = {'Content-Type': 'application/json'}

# --- Parámetros de la Herramienta ---
REFRESH_INTERVAL = 5
SNAPSHOT_DIR = "snapshots"
SNAPSHOT_INTERVAL_S = 300
SNAPSHOT_RETENTION_DAYS = 7

# --- Umbrales de Diagnóstico ---
HEAP_USAGE_THRESHOLD = 85
HEAP_OLD_GEN_THRESHOLD = 75
CPU_USAGE_THRESHOLD = 50
GC_COUNT_SPIKE_THRESHOLD = 2
GC_TIME_SPIKE_THRESHOLD = 500
GC_TIME_THRESHOLD = 200 
REJECTIONS_THRESHOLD = 0
SHARD_SIZE_GB_TARGET = 30
SHARD_SKEW_WARN_THRESHOLD = 60
DUSTY_SHARD_MB_THRESHOLD = 50
LONG_RUNNING_TASK_MINUTES = 5
HIGH_SHARD_COUNT_TEMPLATE_THRESHOLD = 5