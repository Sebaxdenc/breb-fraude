"""breb_lakehouse_mantenimiento (ADR 03): OPTIMIZE y VACUUM del lakehouse una vez al día.

optimize → vacuum

- Salió de breb_lakehouse_pipeline: en la primera corrida en Docker (2026-11-10), optimize tardó 1 min 16 s y
  vacuum 6 min 46 s, de los 11 min 39 s de la corrida. Compactar los archivos chicos de los micro-batches y
  borrar los que ya no usa ninguna versión no hace falta cada 15 minutos.
- vacuum usa trigger_rule all_done: corre aunque optimize falle.
- Corre a las 08:00 UTC (03:00 en Bogotá), fuera del horario de uso. Arranca en pausa para que no compita por
  memoria durante la demo; se activa con `airflow dags unpause breb_lakehouse_mantenimiento`.
- Si coincide con breb_lakehouse_pipeline, OPTIMIZE solo reescribe archivos (dataChange = false): cuando choca
  con un MERGE, Delta rechaza uno de los dos commits y esa tarea se reintenta (breb.batch.mantenimiento y los
  reintentos del pipeline). VACUUM respeta la retención de 7 días, así que no borra archivos que una lectura
  en curso todavía use.
"""
from datetime import datetime, timedelta

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG, TriggerRule

from breb import orquestacion as o

with DAG(
    dag_id="breb_lakehouse_mantenimiento",
    description="OPTIMIZE y VACUUM del lakehouse, una vez al día",
    schedule="0 8 * * *",
    start_date=datetime(2026, 11, 1),
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1,
    is_paused_upon_creation=True,
    tags=["breb", "lakehouse"],
    default_args={"retries": 3, "retry_delay": timedelta(seconds=30), "retry_exponential_backoff": True,
                  "max_retry_delay": timedelta(minutes=5), "execution_timeout": timedelta(minutes=30)},
) as dag:
    optimize = BashOperator(task_id="optimize", bash_command=o.spark("optimize"))
    vacuum = BashOperator(task_id="vacuum", bash_command=o.spark("vacuum"), trigger_rule=TriggerRule.ALL_DONE)

    optimize >> vacuum
