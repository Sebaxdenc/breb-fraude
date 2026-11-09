"""python -m breb.estado cargar: calcula el estado inicial con la historia de LI-Small y lo carga en MongoDB.

Es lo que hará el DAG bootstrap_state (fase 5). Se corre una vez antes de arrancar el Job 2 y de publicar.
Necesita unos 5,5 GB de RAM (decisión D4).
"""
import argparse
import gc
import json
import os
import sys
import time

from pymongo import MongoClient

from breb.estado.almacen import COLECCIONES, AlmacenMongo
from breb.estado.inicial import calcular


def main() -> int:
    ap = argparse.ArgumentParser(prog="python -m breb.estado")
    sub = ap.add_subparsers(dest="orden", required=True)
    c = sub.add_parser("cargar", help="calcula el estado inicial y lo carga en MongoDB")
    c.add_argument("--csv", default=os.path.join(os.environ.get("BREB_DATOS", "/datos"), "LI-Small_Trans.csv"))
    c.add_argument("--modelo", default=os.environ.get("BREB_DIR_MODELO", "/opt/breb/modelo/aml_xgb"))
    c.add_argument("--reemplazar", action="store_true", help="borra cuentas, pares y bancos antes de cargar")
    a = ap.parse_args()

    cliente = MongoClient(f"mongodb://{os.environ['MONGO_MIEMBROS']}/?replicaSet=rs0&authSource=admin",
                          username=os.environ["MONGO_APP_USUARIO"], password=os.environ["MONGO_APP_CLAVE"],
                          serverSelectionTimeoutMS=20000)
    db = cliente["breb"]
    ocupadas = [n for n in COLECCIONES if db[n].estimated_document_count()]
    if ocupadas and not a.reemplazar:
        print(f"MongoDB ya tiene estado en {', '.join(ocupadas)}. Para cargarlo de nuevo: --reemplazar "
              "(y vaciar el tópico y los checkpoints con docker compose down -v si se va a reproducir desde cero)")
        return 1
    for n in ocupadas:
        db[n].drop()

    motor, resumen = calcular(a.csv, a.modelo)
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
