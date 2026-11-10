"""spark-submit /opt/breb/jobs/lakehouse.py <orden> ...: los procesos batch que ejecutan los DAG de Airflow.

  salud                       check_streaming_health
  dimensiones | hechos | calidad  --version-silver N   silver_to_gold_dims, silver_to_gold_facts, data_quality_checks
  todo                        las tres etapas del Job 4 con la versión actual de Silver
  perfiles                    sync_profiles_to_mongo
  optimize | vacuum           mantenimiento del lakehouse
  cuarentena                  reprocess_quarantine
"""
from __future__ import annotations

import argparse
import sys

from breb.batch import job4_gold


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="lakehouse.py")
    ap.add_argument("orden", choices=["salud", "dimensiones", "hechos", "calidad", "todo", "perfiles", "optimize",
                                      "vacuum", "cuarentena"])
    ap.add_argument("--version-silver", type=int)
    ap.add_argument("--umbral-atraso", type=int, default=50_000)
    ap.add_argument("--retencion-horas", type=int, default=168)
    a = ap.parse_args(argv)
    spark = job4_gold.sesion()
    codigo = 0
    if a.orden in ("dimensiones", "hechos", "calidad", "todo"):
        codigo = job4_gold.ejecutar(spark, a.orden, a.version_silver)
    elif a.orden == "salud":
        from breb.batch.salud import revisar
        codigo = 0 if revisar(spark, a.umbral_atraso) else 1
    elif a.orden == "perfiles":
        from breb.batch.perfiles import sincronizar
        sincronizar(spark)
    elif a.orden == "optimize":
        from breb.batch.mantenimiento import optimizar
        optimizar(spark)
    elif a.orden == "vacuum":
        from breb.batch.mantenimiento import vacuum
        vacuum(spark, a.retencion_horas)
    elif a.orden == "cuarentena":
        from breb.batch.cuarentena import reprocesar
        reprocesar(spark)
    spark.stop()
    return codigo


if __name__ == "__main__":
    sys.exit(main())
