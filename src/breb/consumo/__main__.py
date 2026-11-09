"""python -m breb.consumo {registrar|permisos|linaje|tablero}

- registrar: registra en Trino las tablas de Bronze, Silver y Gold y muestra cuántas filas tiene cada una.
- permisos: comprueba los permisos por rol (analista solo Gold; cumplimiento las tres capas en solo lectura).
- linaje [transaction_uuid]: reconstruye una transacción desde Kafka hasta Gold (RNF-06).
- tablero: crea el tablero de Metabase con las preguntas de la sección 1.3.
"""
import argparse
import sys

from breb.consumo import linaje, registrar, tablero, trino


def main() -> int:
    ap = argparse.ArgumentParser(prog="python -m breb.consumo")
    sub = ap.add_subparsers(dest="orden", required=True)
    sub.add_parser("registrar")
    sub.add_parser("permisos")
    l = sub.add_parser("linaje")
    l.add_argument("transaction_uuid", nargs="?")
    sub.add_parser("tablero")
    a = ap.parse_args()
    if a.orden == "tablero":
        tablero.crear()
        print("TABLERO OK")
        return 0
    trino.esperar()
    if a.orden == "registrar":
        estado = registrar.registrar()
        print("REGISTRO OK" if not estado["pendientes"] else
              "REGISTRO PARCIAL: las pendientes se registran cuando existan (p. ej. Gold después del Job 4)")
        return 0
    if a.orden == "permisos":
        ok = registrar.comprobar_permisos()
        print("PERMISOS OK" if ok else "PERMISOS CON FALLAS")
        return 0 if ok else 1
    linaje.reconstruir(a.transaction_uuid)
    return 0


if __name__ == "__main__":
    sys.exit(main())
