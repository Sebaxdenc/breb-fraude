"""python -m breb.entidad --entidades 70,12   |   python -m breb.entidad --analistas (ver servicio.py)."""
import argparse
import json
import os
import sys

from breb.entidad.servicio import atender_entidades, bandeja_analistas


def main() -> int:
    ap = argparse.ArgumentParser(prog="python -m breb.entidad")
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--entidades", help="identificadores de banco separados por coma, p. ej. 70,12")
    g.add_argument("--analistas", action="store_true", help="bandeja de los analistas (todas las alertas)")
    g.add_argument("--tramo", action="store_true",
                   help="las entidades con más alertas del tramo de la demo (productor/tramo_demo.json)")
    ap.add_argument("--una-vez", action="store_true", help="procesar lo que haya en las colas y terminar")
    a = ap.parse_args()
    if a.analistas:
        print(f"alertas vistas: {bandeja_analistas(a.una_vez)}")
        return 0
    if a.tramo:
        with open(os.path.join(os.environ.get("BREB_DATOS", "/datos"), "productor", "tramo_demo.json")) as fh:
            entidades = json.load(fh)["tramo"]["entidades_con_mas_alertas"]
    else:
        entidades = [int(x) for x in a.entidades.split(",") if x.strip()]
    print(atender_entidades(entidades, a.una_vez))
    return 0


if __name__ == "__main__":
    sys.exit(main())
