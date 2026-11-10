"""check_streaming_health: salud de los jobs de streaming desde Airflow (RF9, ADR 05).

Spark no confirma offsets en Kafka, así que la salud se lee de la tabla de métricas que escribe el
StreamingQueryListener de cada job (último micro-batch, duración, maxOffsetsBehindLatest) y de las aplicaciones
registradas en el master de Spark. Falla solo si una consulta activa se está quedando atrás (más de
`--umbral-atraso` offsets): con los jobs detenidos, Gold se puede construir igual con lo que ya está en Silver.
"""
from __future__ import annotations

import json
import os
import urllib.request
from datetime import datetime, timezone

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

from breb.streaming.comun import METRICAS

MASTER_UI = os.environ.get("SPARK_MASTER_UI", "http://spark-master:8080")
JOBS_STREAMING = ("job1-bronze", "job2-riesgo", "job2-ventanas", "job3-silver")


def aplicaciones_spark() -> list[dict] | None:
    """Aplicaciones activas en el master de Spark, o None si no responde."""
    try:
        with urllib.request.urlopen(f"{MASTER_UI}/json/", timeout=5) as r:
            return json.load(r).get("activeapps", [])
    except (OSError, ValueError):
        return None


def jobs_activos() -> list[str]:
    apps = aplicaciones_spark() or []
    return sorted(a["name"] for a in apps if a.get("name") in JOBS_STREAMING)


def revisar(spark: SparkSession, umbral_atraso: int = 50_000, minutos_activo: float = 5.0) -> bool:
    apps = aplicaciones_spark()
    if apps is None:
        print(f"[salud] el master de Spark no responde en {MASTER_UI}")
    else:
        print(f"[salud] aplicaciones activas en Spark: {', '.join(a['name'] for a in apps) or 'ninguna'}")
    try:
        m = spark.read.format("delta").load(METRICAS)
    except Exception:  # noqa: BLE001 - la tabla aparece con el primer micro-batch
        print("[salud] todavía no hay métricas de streaming")
        return True
    ahora = datetime.now(timezone.utc).replace(tzinfo=None)
    ultimo = (m.withColumn("_r", F.row_number().over(Window.partitionBy("job", "consulta").orderBy(F.desc("ts"))))
              .where("_r = 1").select("job", "consulta", F.col("ts").alias("ultimo_micro_batch"),
                                      F.col("max_offsets_atras").alias("atraso_offsets"), "duracion_ms"))
    resumen = (m.groupBy("job", "consulta").agg(F.count(F.lit(1)).alias("micro_batches"), F.sum("filas").alias("filas"))
               .join(ultimo, ["job", "consulta"]).orderBy("job", "consulta").collect())
    problemas = []
    print(f"[salud] {'consulta':<26} {'micro-batches':>13} {'filas':>10} {'último hace':>12} {'atraso':>9}  estado")
    for r in resumen:
        edad_min = (ahora - r["ultimo_micro_batch"]).total_seconds() / 60
        activa = edad_min <= minutos_activo
        atraso = r["atraso_offsets"]
        estado = "activa" if activa else "detenida"
        if activa and atraso is not None and atraso > umbral_atraso:
            estado = "ATRASADA"
            problemas.append(f"{r['consulta']} con {atraso:,} offsets de atraso")
        print(f"[salud] {r['consulta']:<26} {r['micro_batches']:>13,} {r['filas'] or 0:>10,} {edad_min:>9.1f} min "
              f"{'-' if atraso is None else f'{atraso:,}':>9}  {estado}")
    if problemas:
        print("[salud] STREAMING ATRASADO: " + "; ".join(problemas))
        return False
    print("[salud] STREAMING OK (las consultas detenidas no bloquean la construcción de Gold)")
    return True
