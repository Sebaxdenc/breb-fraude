"""python -m breb.productor {preparar|publicar} ... (ver breb/productor/publicador.py)."""
import argparse
import json
import os
import pathlib
import sys
from datetime import datetime

from breb.productor.publicador import ESCALONES_RNF01, leer_escalones, preparar, publicar

META = os.environ.get("BREB_MODEL_META", "/opt/breb/modelo/aml_xgb/model_meta.json")
DIR_DATOS = pathlib.Path(os.environ.get("BREB_DATOS", "/datos"))


def main() -> int:
    ap = argparse.ArgumentParser(prog="python -m breb.productor")
    sub = ap.add_subparsers(dest="orden", required=True)

    p = sub.add_parser("preparar", help="lee el CSV de LI-Small y guarda las transacciones a reproducir")
    p.add_argument("--csv", default=str(DIR_DATOS / "LI-Small_Trans.csv"))
    p.add_argument("--meta", default=META, help="model_meta.json (corte de validación y fin del rango)")
    p.add_argument("--salida", default=str(DIR_DATOS / "productor"))

    q = sub.add_parser("publicar", help="publica en Kafka por escalones y mide la latencia fuente → Kafka")
    q.add_argument("--eventos", default=str(DIR_DATOS / "productor" / "eventos_test.parquet"))
    q.add_argument("--bootstrap", default=os.environ.get("KAFKA_BOOTSTRAP", "kafka-1:9092"))
    q.add_argument("--topico", default="breb.transacciones.raw")
    q.add_argument("--escalones", default=ESCALONES_RNF01,
                   help=f"tasa:segundos separados por coma (por defecto {ESCALONES_RNF01})")
    q.add_argument("--invalidos", type=float, default=0.0, help="fracción de eventos inválidos, p. ej. 0.01")
    q.add_argument("--desde-seq", type=int, help="primer seq a publicar")
    q.add_argument("--continuar", action="store_true",
                   help="seguir después del último seq publicado (productor/progreso.json)")
    q.add_argument("--max", type=int, help="máximo de transacciones a publicar")
    q.add_argument("--semilla", type=int, default=7)
    q.add_argument("--informe", help="JSON con las mediciones (por defecto mediciones/productor_<fecha>.json)")
    a = ap.parse_args()

    if a.orden == "preparar":
        print(json.dumps(preparar(a.csv, a.meta, a.salida), indent=2, ensure_ascii=False))
        print("PREPARACIÓN OK")
        return 0

    progreso = pathlib.Path(a.eventos).parent / "progreso.json"
    desde = a.desde_seq
    if a.continuar:
        if not progreso.exists():
            print(f"no existe {progreso}: se publica desde el principio")
        else:
            desde = json.loads(progreso.read_text())["ultimo_seq_valido"] + 1
            print(f"continuando desde seq {desde}")
    resultado = publicar(a.eventos, a.bootstrap, a.topico, leer_escalones(a.escalones), a.invalidos,
                         desde, a.max, a.semilla)
    if resultado["ultimo_seq_valido"] is None:
        print("NO HABÍA TRANSACCIONES POR PUBLICAR: ¿ya se publicaron todas? Quiten --continuar o usen --desde-seq")
        return 1
    if resultado["ultimo_seq_valido"] is not None and resultado["sin_confirmar"] == 0:
        progreso.write_text(json.dumps({"ultimo_seq_valido": resultado["ultimo_seq_valido"]}))
    informe = pathlib.Path(a.informe) if a.informe else (
        DIR_DATOS / "mediciones" / f"productor_{datetime.now():%Y%m%d_%H%M%S}.json")
    informe.parent.mkdir(parents=True, exist_ok=True)
    informe.write_text(json.dumps(resultado, indent=2, ensure_ascii=False))

    print(f"\n{'ev/s objetivo':>13} {'logrados':>9} {'confirmados':>12} {'p50 ms':>8} {'p99 ms':>8} {'errores':>8}  RF1")
    for e in resultado["escalones"]:
        lat = e["latencia_fuente_kafka_ms"] or {}
        print(f"{e['tasa_objetivo']:>13g} {e['tasa_lograda']:>9g} {e['confirmados']:>12,} "
              f"{lat.get('p50', float('nan')):>8.1f} {lat.get('p99', float('nan')):>8.1f} {e['errores']:>8}  "
              f"{'sin datos' if e['cumple_rf1'] is None else 'cumple' if e['cumple_rf1'] else 'NO cumple'}")
    print(f"informe: {informe} | último seq publicado: {resultado['ultimo_seq_valido']}")
    sin_append = sum(e["sin_log_append_time"] for e in resultado["escalones"])
    if sin_append:
        print(f"AVISO: {sin_append} mensajes sin LogAppendTime: ¿el tópico tiene message.timestamp.type=LogAppendTime?")
    ok = all(e["cumple_rf1"] is not False for e in resultado["escalones"]) and resultado["sin_confirmar"] == 0
    print("PUBLICACIÓN OK" if ok else "PUBLICACIÓN CON PROBLEMAS: revisen los escalones que no cumplen")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
