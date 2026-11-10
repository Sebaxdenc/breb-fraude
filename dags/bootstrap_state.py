"""bootstrap_state (RF8): bajo demanda, una vez antes de iniciar el productor. Calcula el estado de cuentas, pares
y bancos con todas las transacciones anteriores al punto de inicio y lo carga en MongoDB.

Parámetro `inicio`:
- "tramo" (por defecto): el tramo de 15.000 transacciones con más alertas (datos/productor/tramo_demo.json).
- "corte": el corte de validación del modelo (2022-09-09 03:13), para reproducir todo el test.

comprobar_streaming_detenido → preparar_eventos → elegir_tramo → cargar_estado → verificar_estado

preparar_eventos y elegir_tramo se saltan si su resultado ya existe en datos/productor/. Necesita unos 5,5 GB
de RAM (decisión D4): los jobs de streaming tienen que estar apagados.
"""
from datetime import datetime, timedelta

from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG, Param, TriggerRule

from breb import orquestacion as o

P = f"{o.DATOS}/productor"

with DAG(
    dag_id="bootstrap_state",
    description="Estado inicial del Job 2 en MongoDB (cuentas, pares y bancos)",
    schedule=None,
    start_date=datetime(2026, 11, 1),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,   # sin horario: solo corre cuando se dispara
    tags=["breb", "estado"],
    params={"inicio": Param("tramo", type="string", enum=["tramo", "corte"],
                            description="tramo: el tramo de la demo; corte: todo el test desde el corte de validación")},
    default_args={"retries": 0, "execution_timeout": timedelta(minutes=45)},
) as dag:
    detenido = PythonOperator(task_id="comprobar_streaming_detenido", python_callable=o.comprobar_streaming_detenido)
    # exit 99: la tarea queda "skipped" (skip_on_exit_code); las siguientes usan none_failed
    preparar = BashOperator(
        task_id="preparar_eventos",
        bash_command=f"if [ -f {P}/eventos_test.parquet ]; then echo 'ya existe {P}/eventos_test.parquet'; exit 99; fi; "
                     "python -m breb.productor preparar")
    tramo = BashOperator(
        task_id="elegir_tramo", trigger_rule=TriggerRule.NONE_FAILED,
        bash_command="if [ '{{ params.inicio }}' != tramo ]; then echo 'inicio en el corte de validación'; exit 99; fi; "
                     f"if [ -f {P}/tramo_demo.json ]; then echo 'ya existe {P}/tramo_demo.json'; exit 99; fi; "
                     "python -m breb.estado tramo")
    cargar = BashOperator(
        task_id="cargar_estado", trigger_rule=TriggerRule.NONE_FAILED,
        bash_command="python -m breb.estado cargar --reemplazar {{ '--tramo' if params.inicio == 'tramo' else '' }}")
    verificar = PythonOperator(task_id="verificar_estado", python_callable=o.verificar_estado)

    detenido >> preparar >> tramo >> cargar >> verificar
