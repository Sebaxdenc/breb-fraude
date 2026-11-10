"""python -m breb.estado {tramo|cargar}

- tramo: puntúa todo el test fuera de línea, compara las métricas con las del notebook y elige el tramo de N
  transacciones con más alertas para la demo (datos/productor/tramo_demo.json y scores_test.parquet).
- cargar: calcula el estado inicial y lo carga en MongoDB. Es lo que hará el DAG bootstrap_state (fase 5).
  Con --desde-seq, el estado incluye también el test hasta ese seq.
Cada uno necesita unos 5,5 GB de RAM (decisión D4).
"""
import argparse
import gc
import json
import os
import sys
import time

from pymongo import MongoClient

from breb.estado.almacen import COLECCIONES, AlmacenMongo
from breb.estado import tramo as tr
from breb.estado.inicial import calcular


def main() -> int:
    ap = argparse.ArgumentParser(prog="python -m breb.estado")
    sub = ap.add_subparsers(dest="orden", required=True)
    c = sub.add_parser("cargar", help="calcula el estado inicial y lo carga en MongoDB")
    c.add_argument("--csv", default=os.path.join(os.environ.get("BREB_DATOS", "/datos"), "LI-Small_Trans.csv"))
    c.add_argument("--modelo", default=os.environ.get("BREB_DIR_MODELO", "/opt/breb/modelo/aml_xgb"))
    c.add_argument("--reemplazar", action="store_true", help="borra cuentas, pares y bancos antes de cargar")
    c.add_argument("--desde-seq", type=int, help="estado hasta este seq (tramo de la demo)")
    c.add_argument("--tramo", action="store_true", help="estado hasta el inicio del tramo (productor/tramo_demo.json)")
    t = sub.add_parser("tramo", help="puntúa el test completo y elige el tramo de la demo")
    t.add_argument("--csv", default=os.path.join(os.environ.get("BREB_DATOS", "/datos"), "LI-Small_Trans.csv"))
    t.add_argument("--modelo", default=os.environ.get("BREB_DIR_MODELO", "/opt/breb/modelo/aml_xgb"))
    t.add_argument("--transacciones", type=int, default=15_000, help="tamaño del tramo (50 ev/s durante 5 min)")
    t.add_argument("--salida", default=os.path.join(os.environ.get("BREB_DATOS", "/datos"), "productor"))
    a = ap.parse_args()

    if a.orden == "tramo":
        scores, metricas = tr.puntuar_test(a.csv, a.modelo)
        tramo = tr.elegir_tramo(scores, metricas["umbral"], a.transacciones)
        tr.guardar(scores, metricas, tramo, a.salida)
        print("métricas del test completo con el motor del Job 2:")
        print(json.dumps(metricas, indent=2, ensure_ascii=False))
        print("tramo de la demo:")
        print(json.dumps(tramo, indent=2, ensure_ascii=False))
        igual = metricas["pr_auc_notebook"] is not None and abs(metricas["pr_auc"] - metricas["pr_auc_notebook"]) < 1e-9
        print("PR-AUC igual al del notebook" if igual else "PR-AUC DISTINTO al del notebook: revisar")
        print("siguiente paso: python -m breb.estado cargar --tramo")
        return 0 if igual else 1

    cliente = MongoClient(f"mongodb://{os.environ['MONGO_MIEMBROS']}/?replicaSet=rs0&authSource=admin",
                          username=os.environ["MONGO_APP_USUARIO"], password=os.environ["MONGO_APP_CLAVE"],
                          serverSelectionTimeoutMS=20000)
    db = cliente["breb"]
    ocupadas = [n for n in COLECCIONES if db[n].estimated_document_count()]
    if ocupadas and not a.reemplazar:
        print(f"MongoDB ya tiene estado en {', '.join(ocupadas)}. Para cargarlo de nuevo: --reemplazar "
              "(y vaciar el tópico y los checkpoints con docker compose --profile \"*\" down -v si se va a reproducir "
              "desde cero)")
        return 1
    for n in ocupadas:
        db[n].drop()

    if a.tramo:
        ruta_tramo = os.path.join(os.environ.get("BREB_DATOS", "/datos"), "productor", "tramo_demo.json")
        with open(ruta_tramo) as fh:
            a.desde_seq = json.load(fh)["tramo"]["desde_seq"]
        print(f"estado hasta el inicio del tramo de la demo: seq {a.desde_seq}")
    motor, resumen = calcular(a.csv, a.modelo, a.desde_seq)
    print(json.dumps(resumen, ensure_ascii=False), flush=True)
    gc.collect()
    t0 = time.perf_counter()
    escritos = AlmacenMongo(db).cargar_estado_inicial(motor)
    print(f"documentos cargados: {escritos:,} en {time.perf_counter() - t0:.0f} s "
          f"({', '.join(f'{n} {db[n].estimated_document_count():,}' for n in COLECCIONES)})")
    print("ESTADO INICIAL OK")
    return 0


if __name__ == "__main__":
    sys.exit(main())
