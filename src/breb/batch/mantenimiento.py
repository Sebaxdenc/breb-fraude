"""optimize y vacuum del lakehouse (ADR 03): compactación de los archivos chicos que dejan los micro-batches y
borrado de los archivos que ya no usa ninguna versión dentro de la retención (7 días: el Time Travel llega
hasta ahí).

OPTIMIZE solo reescribe archivos (dataChange = false): los jobs de streaming que leen estas tablas no ven
filas nuevas. Si choca con una escritura concurrente (p. ej. un MERGE del Job 3 en Silver), Delta rechaza uno
de los dos commits; aquí se reintenta hasta 3 veces, y el Job 3 se recupera solo desde su checkpoint.
"""
from __future__ import annotations

import os
import time

from delta.tables import DeltaTable
from pyspark.sql import SparkSession

from breb.batch.job4_gold import GOLD, existe
from breb.streaming.comun import BRONZE, METRICAS, RISK_DECISIONS, SILVER

REPROCESOS = os.environ.get("BREB_REPROCESOS", SILVER.rsplit("/", 1)[0] + "/reprocesos_cuarentena")
TABLAS = [("bronze.transacciones", BRONZE), ("silver.transacciones", SILVER), ("silver.reprocesos_cuarentena", REPROCESOS),
          ("gold.risk_decisions", RISK_DECISIONS), ("gold.progreso_streaming", METRICAS)] + [
    (f"gold.{t}", f"{GOLD}/{t}") for t in ("fact_transferencias", "dim_cuenta", "dim_entidad_financiera", "dim_tiempo",
                                           "ejecuciones")]


def _archivos(spark: SparkSession, path: str) -> int:
    return spark.sql(f"DESCRIBE DETAIL delta.`{path}`").first()["numFiles"]


def _con_reintentos(accion, intentos: int = 3):
    for i in range(intentos):
        try:
            return accion()
        except Exception as e:  # noqa: BLE001 - se reintentan solo los conflictos de concurrencia de Delta
            if "Concurrent" not in type(e).__name__ + str(e)[:300] or i == intentos - 1:
                raise
            print(f"[mantenimiento] conflicto con una escritura concurrente, reintento {i + 1}")
            time.sleep(5)


def optimizar(spark: SparkSession) -> dict:
    total = {"antes": 0, "despues": 0}
    for nombre, path in TABLAS:
        if not existe(spark, path):
            continue
        antes = _archivos(spark, path)
        t0 = time.perf_counter()
        _con_reintentos(lambda: DeltaTable.forPath(spark, path).optimize().executeCompaction())
        despues = _archivos(spark, path)
        total["antes"] += antes
        total["despues"] += despues
        print(f"[optimize] {nombre:<32} {antes:>6,} archivos → {despues:>6,} ({time.perf_counter() - t0:.0f} s)")
    print(f"[optimize] total: {total['antes']:,} archivos → {total['despues']:,}")
    return total


def vacuum(spark: SparkSession, horas: int = 168) -> None:
    for nombre, path in TABLAS:
        if not existe(spark, path):
            continue
        t0 = time.perf_counter()
        _con_reintentos(lambda: DeltaTable.forPath(spark, path).vacuum(horas))
        print(f"[vacuum] {nombre:<32} retención {horas} h ({time.perf_counter() - t0:.0f} s)")
