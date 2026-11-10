"""reprocess_quarantine (RF12): bajo demanda. Corrige los eventos en cuarentena de Bronze y escribe en Silver
los que quedan válidos, con MERGE por transaction_uuid (o por coordenadas de Kafka si no traen identificador).
No reinyecta nada a Kafka. Al terminar dispara breb_lakehouse_pipeline para que lleguen a Gold.

reprocesar_cuarentena → disparar_pipeline
"""
from datetime import datetime, timedelta

from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk import DAG

from breb import orquestacion as o

with DAG(
    dag_id="reprocess_quarantine",
    description="Cuarentena de Bronze → esquema corregido → MERGE a Silver",
    schedule=None,
    start_date=datetime(2026, 11, 1),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,   # sin horario: solo corre cuando se dispara
    tags=["breb", "lakehouse"],
    default_args={"retries": 2, "retry_delay": timedelta(seconds=30), "retry_exponential_backoff": True,
                  "execution_timeout": timedelta(minutes=30)},
) as dag:
    reprocesar = BashOperator(task_id="reprocesar_cuarentena", bash_command=o.spark("cuarentena"))
    disparar = TriggerDagRunOperator(task_id="disparar_pipeline", trigger_dag_id="breb_lakehouse_pipeline",
                                     wait_for_completion=False)
    reprocesar >> disparar
