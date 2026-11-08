# Bre-B · Detección de fraude en tiempo real

Proyecto final de ST1630 Sistemas Intensivos en Datos (EAFIT, 2026-2). Implementación de la arquitectura del Entregable 1 v2.0: Kafka, Spark Structured Streaming, Delta Lake, MongoDB, Airflow, Trino, Metabase y alertas por SNS/SQS, con un modelo XGBoost de Kaggle dentro del Job 2.

- **Plan y avance:** [PLAN.md](PLAN.md)
- **Contratos de integración:** [contratos/README.md](contratos/README.md)

## Estructura
| Carpeta | Contenido |
|---|---|
| `src/breb/contratos/` | Esquemas JSON del evento y de la alerta, validación |
| `src/breb/datos/` | Carga de IBM AML en el mismo orden del entrenamiento |
| `src/breb/features/` | Motor de las 58 variables del modelo (Job 2) |
| `src/breb/modelo/` | Carga del modelo y puntuación |
| `src/breb/estado/` | Estado en MongoDB, idempotente por micro-batch, y estado inicial |
| `src/breb/job2/` | Lógica del micro-batch del Job 2, sin Spark |
| `src/breb/productor/` | Conversión de filas a eventos |
| `modelo/` | Notebook de entrenamiento (versión 1 de Kaggle) y artefactos del modelo |
| `herramientas/referencia/` | Dataset sintético y ejecución del notebook original para la prueba de paridad |
| `herramientas/kaggle/` | Notebooks para Kaggle: verificación de paridad con datos reales y demo del modelo |
| `tests/` | Pruebas (paridad, Job 2 con MongoDB, contratos) |

Pendiente según el plan: `infra/` (docker-compose), `jobs/` (Spark), `dags/` (Airflow), `servicios/` (API de ingesta y servicio de la entidad).

## Entorno de desarrollo
La versión de Python y de las librerías es la de la imagen de Spark: PySpark 3.5 admite hasta Python 3.11 y exige numpy < 2.
```bash
uv venv -p 3.11 .venv
uv pip install -p .venv/bin/python -e ".[dev]"
.venv/bin/python -m pytest -q
```

## Artefactos del modelo
`modelo/aml_xgb/` debe contener `model_meta.json` (incluido), `aml_xgb.json` y `golden_sample.parquet`, descargados de la pestaña Output de la versión 1 de [AML XGBoost Bre-B Fraud](https://www.kaggle.com/code/sebastianmedinacab/aml-xgboost-bre-b-fraud). No se usa `ohe.joblib`.

## Atribución
- Modelo adaptado de [XGBoost + Leiden community detection](https://www.kaggle.com/code/arturmarkosyan2/xgboost-leiden-community-detection-pr-auc-0-39) (Artur Markosyan).
- Datos: [IBM Transactions for Anti Money Laundering](https://www.kaggle.com/datasets/ealtman2019/ibm-transactions-for-anti-money-laundering-aml), licencia CDLA-Sharing-1.0 (Altman et al., NeurIPS 2023).
