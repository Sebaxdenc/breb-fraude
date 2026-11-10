"""Demo en vivo: cuántas alertas del tramo quedan antes y después de cada punto de corte.

Antes de la clase se publica la primera parte del tramo (`publicar --tramo --max K`) y Airflow la lleva a Gold; en
vivo se publica el resto (`publicar --continuar --max N-K`). Este script ayuda a elegir K para que la parte en vivo
tenga suficientes alertas. Usa la puntuación fuera de línea del test (scores_test.parquet), que da el mismo score
que el Job 2.

    docker compose run --rm spark-cliente python /opt/breb/herramientas/streaming/dividir_tramo.py --paso 1000
"""
import argparse
import json
import os
import pathlib

import pandas as pd


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--productor", default=os.path.join(os.environ.get("BREB_DATOS", "/datos"), "productor"),
                    help="carpeta con tramo_demo.json y scores_test.parquet")
    ap.add_argument("--paso", type=int, default=1000, help="separación entre los puntos de corte")
    a = ap.parse_args()

    d = pathlib.Path(a.productor)
    info = json.loads((d / "tramo_demo.json").read_text())
    tramo, umbral = info["tramo"], info["metricas_test"]["umbral"]
    s = pd.read_parquet(d / "scores_test.parquet").sort_values("seq")
    s = s[s["seq"] >= tramo["desde_seq"]].head(tramo["transacciones"]).reset_index(drop=True)
    acumuladas = (s["score"] >= umbral).cumsum()
    total = int(acumuladas.iloc[-1])
    print(f"tramo: {len(s):,} transacciones desde seq {tramo['desde_seq']:,}, {total} alertas (umbral {umbral:.3f})")
    print(f"{'antes de la clase (K)':>22} | {'en vivo':>8} | {'alertas antes':>13} | {'alertas en vivo':>15}")
    for k in range(a.paso, len(s), a.paso):
        antes = int(acumuladas.iloc[k - 1])
        print(f"{k:>22,} | {len(s) - k:>8,} | {antes:>13} | {total - antes:>15}")


if __name__ == "__main__":
    main()
