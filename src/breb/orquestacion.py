"""Piezas comunes de los DAG de Airflow (dags/): comandos de Spark y comprobaciones rápidas en Python.

Los procesos de Spark corren dentro del contenedor de Airflow en modo local (BREB_SPARK_MASTER, local[2] por
defecto): en el equipo de la demo, los jobs de streaming ocupan todos los núcleos del worker, y un proceso
batch enviado al clúster esperaría núcleos libres. Con más recursos, BREB_SPARK_MASTER=spark://spark-master:7077.
"""
from __future__ import annotations

import os

SPARK_MASTER = os.environ.get("BREB_SPARK_MASTER", "local[2]")
SPARK_MEMORIA = os.environ.get("BREB_SPARK_MEMORIA", "2g")
DATOS = os.environ.get("BREB_DATOS", "/datos")


def spark(orden: str, memoria: str | None = None) -> str:
    """Comando de BashOperator para una orden de jobs/lakehouse.py (ver breb.batch.cli)."""
    return (f"spark-submit --master {SPARK_MASTER} --driver-memory {memoria or SPARK_MEMORIA} "
            f"--conf spark.ui.enabled=false /opt/breb/jobs/lakehouse.py {orden}")


def version_silver_nueva() -> int | None:
    """check_new_silver_data: la versión actual de Silver si tiene datos que no llegaron a Gold, o None."""
    from breb.batch.version import version_silver_pendiente
    actual, procesada = version_silver_pendiente()
    print(f"Silver: versión actual {actual}, última versión procesada en Gold {procesada}")
    if actual is None or (procesada is not None and actual <= procesada):
        return None
    return actual


def comprobar_streaming_detenido() -> None:
    """bootstrap_state reemplaza el estado de MongoDB y necesita unos 5,5 GB de RAM (decisión D4): los jobs de
    streaming tienen que estar apagados."""
    from breb.batch.salud import aplicaciones_spark, jobs_activos
    if aplicaciones_spark() is None:
        print("el master de Spark no responde: se asume que los jobs de streaming están apagados")
        return
    activos = jobs_activos()
    if activos:
        raise RuntimeError(f"Hay jobs de streaming corriendo ({', '.join(activos)}). Apáguenlos antes de cargar el "
                           "estado: docker compose stop job1-bronze job2-riesgo job2-ventanas job3-silver")
    print("jobs de streaming apagados")


def verificar_estado() -> dict:
    from breb.batch.comun import base_mongo
    db = base_mongo()
    conteos = {c: db[c].estimated_document_count() for c in ("cuentas", "pares", "bancos")}
    print("estado en MongoDB: " + ", ".join(f"{c} {n:,}" for c, n in conteos.items()))
    if not all(conteos.values()):
        raise RuntimeError("alguna colección quedó vacía")
    return conteos
