"""breb_lakehouse_pipeline (RF7, RF9, ADR 05): Silver → Gold cada 15 minutos.

check_streaming_health ─┐
                        ├─ silver_to_gold_dims → silver_to_gold_facts → data_quality_checks → sync_profiles_to_mongo ─┬─ optimize
check_new_silver_data ──┘                                                                                             └─ vacuum

- check_new_silver_data fija la versión de Silver que procesan las tres etapas del Job 4 (time travel); si no
  hay datos nuevos desde la última ejecución, se salta el resto (optimize y vacuum corren igual).
- data_quality_checks falla si los hechos no cuadran con Silver o pierden integridad referencial; así un dato
  inválido no llega a MongoDB.
- optimize y vacuum usan trigger_rule all_done: el mantenimiento corre aunque falle algo antes.
- Hasta 3 reintentos con retroceso exponencial; catchup=False y max_active_runs=1.
"""
from datetime import datetime, timedelta

from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG, TriggerRule
from airflow.sdk.exceptions import AirflowSkipException

from breb import orquestacion as o


def _check_new_silver_data():
    version = o.version_silver_nueva()
    if version is None:
        raise AirflowSkipException("sin datos nuevos en Silver desde la última ejecución")
    return version


VERSION = "{{ ti.xcom_pull(task_ids='check_new_silver_data') }}"

with DAG(
    dag_id="breb_lakehouse_pipeline",
    description="Silver → Gold en estrella, calidad, perfiles en MongoDB y mantenimiento del lakehouse",
    schedule=timedelta(minutes=15),
    start_date=datetime(2026, 11, 1),
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1,
    tags=["breb", "lakehouse"],
    default_args={"retries": 3, "retry_delay": timedelta(seconds=30), "retry_exponential_backoff": True,
                  "max_retry_delay": timedelta(minutes=5), "execution_timeout": timedelta(minutes=30)},
) as dag:
    salud = BashOperator(task_id="check_streaming_health", bash_command=o.spark("salud", "1g"))
    nuevos = PythonOperator(task_id="check_new_silver_data", python_callable=_check_new_silver_data, retries=0)
    dims = BashOperator(task_id="silver_to_gold_dims", bash_command=o.spark(f"dimensiones --version-silver {VERSION}"))
    hechos = BashOperator(task_id="silver_to_gold_facts", bash_command=o.spark(f"hechos --version-silver {VERSION}"))
    calidad = BashOperator(task_id="data_quality_checks", bash_command=o.spark(f"calidad --version-silver {VERSION}"),
                           retries=0)
    perfiles = BashOperator(task_id="sync_profiles_to_mongo", bash_command=o.spark("perfiles"))
    optimize = BashOperator(task_id="optimize", bash_command=o.spark("optimize"), trigger_rule=TriggerRule.ALL_DONE)
    vacuum = BashOperator(task_id="vacuum", bash_command=o.spark("vacuum"), trigger_rule=TriggerRule.ALL_DONE)

    [salud, nuevos] >> dims >> hechos >> calidad >> perfiles >> [optimize, vacuum]
