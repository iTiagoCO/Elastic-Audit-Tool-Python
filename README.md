# Elastic Pro Audit Tool - Versión Avanzada con API (`api-refactor`)

Esta rama contiene la versión con arquitectura cliente-servidor: un backend FastAPI y un cliente interactivo TUI que lo consume.

🔁 Ideal para generación de reportes automáticos, auditorías avanzadas y análisis inteligente.

---

## Características

- 🔧 Backend API (FastAPI)
- 🧠 Análisis con Azure OpenAI
- 📄 Generación de reportes `.md` automáticos
- 📊 Diagnóstico profundo en `.json`
- 🕵️ Correlación automática de síntomas y causas raíz

---

## Instalación

```bash
git checkout api-refactor
git clone https://github.com/iTiagoCO/Elastic-Audit-Tool-Python.git
cd Elastic-Audit-Tool-Python
python -m venv env
source env/bin/activate  # Windows: env\Scripts\activate
pip install -r requirements.txt
```

Configura el archivo `.env`:

```env
# Elasticsearch
ES_HOST="https://tu-cluster.es.us-east-1.aws.found.io:9243"
ES_USER="usuario"
ES_PASS="clave"

# Azure OpenAI (opcional)
AZURE_OPENAI_KEY="clave"
AZURE_OPENAI_ENDPOINT="https://endpoint.openai.azure.com/"
AZURE_OPENAI_DEPLOYMENT_NAME="nombre_modelo"
```

---

## Uso

### Paso 1 – Lanza el backend

```bash
uvicorn src.api:app --reload
```

### Paso 2 – Cliente interactivo

```bash
python -m src.main
```

### Paso 3 – Generar reportes

```bash
# Markdown:
python -m src.main --report

# Diagnóstico profundo:
python -m src.main --detailed-report --duration 15

# Con IA:
python -m src.main --detailed-report --duration 15 --analyze
```

---

## Arquitectura

```
[ Elasticsearch ] <---> [ FastAPI (motor) ] <---> [ Cliente TUI ]
```

Puedes construir nuevos frontends o consumir los endpoints directamente.

---

## Comparación con `main`

| Característica                 | `main`            | `api-refactor`          |
|-------------------------------|-------------------|--------------------------|
| TUI interactiva               | ✅                | ✅                       |
| Generación de reportes `.md` | ❌                | ✅                       |
| Análisis con IA               | ❌                | ✅                       |
| Requiere backend              | ❌                | ✅                       |

---

## 🧠 Para uso profesional

Esta versión está orientada a analistas de SRE, DevOps, SecOps, y arquitectos de observabilidad.

---
