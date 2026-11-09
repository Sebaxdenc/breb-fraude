"""python -m breb.entidad --entidades 70,12   |   python -m breb.entidad --analistas (ver servicio.py)."""
import argparse
import os
import sys

from breb.entidad.servicio import atender_entidades, bandeja_analistas, entidades_del_tramo


def main() -> int:
    ap = argparse.ArgumentParser(prog="python -m breb.entidad")
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--entidades", help="identificadores de banco separados por coma, p. ej. 70,12")
    g.add_argument("--analistas", action="store_true", help="bandeja de los analistas (todas las alertas)")
    g.add_argument("--tramo", action="store_true",
                   help="todas las entidades que participan en alertas del tramo de la demo (datos/productor)")
    ap.add_argument("--una-vez", action="store_true", help="procesar lo que haya en las colas y terminar")
    a = ap.parse_args()
    if a.analistas:
        print(f"alertas vistas: {bandeja_analistas(a.una_vez)}")
        return 0
    if a.tramo:
        entidades = entidades_del_tramo(os.path.join(os.environ.get("BREB_DATOS", "/datos"), "productor"),
                                        os.environ.get("BREB_DIR_MODELO", "/opt/breb/modelo/aml_xgb"))
    else:
        entidades = [int(x) for x in a.entidades.split(",") if x.strip()]
    print(atender_entidades(entidades, a.una_vez))
    return 0


if __name__ == "__main__":
    sys.exit(main())
