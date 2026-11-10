"""Productor de la demo: reproduce las transacciones de LI-Small en Kafka (fase 3, RF1).

Representa a las entidades que envían las transferencias a Bre-B. Publica directo en el tópico
breb.transacciones.raw (decisión D3), en el orden del entrenamiento (seq), con la clave
source_account del contrato, acks=all e idempotencia.

Dos pasos:
- `preparar`: lee el CSV una sola vez (unos 4 GB de RAM), separa las transacciones posteriores al corte de
  validación y las guarda en Parquet, junto con sus etiquetas para Gold (fase 5).
- `publicar`: envía esas transacciones por escalones de tasa (eventos por segundo) y mide la latencia
  fuente → Kafka: desde `produced_at`, que se fija justo antes de enviar, hasta la hora en que el bróker
  guardó el mensaje (LogAppendTime, configurado en el tópico).
"""
from __future__ import annotations

import json
import pathlib
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone

import numpy as np
import pandas as pd

from breb.contratos import errores_evento, uuid_transaccion
from breb.datos import ibm_aml
from breb.productor.eventos import FUENTE, a_eventos, eventos_invalidos

COLUMNAS = ["seq", "timestamp", "from_bank", "to_bank", "from_node_key", "to_node_key", "amount_paid",
            "payment_currency", "amount_received", "receiving_currency", "payment_format"]
ESCALONES_RNF01 = "100:60,250:60,500:60,750:60,1000:60"
TIMESTAMP_LOG_APPEND_TIME = 2      # confluent_kafka.TIMESTAMP_LOG_APPEND_TIME
OBJETIVO_P99_MS = 300.0            # RF1: fuente → Kafka


def preparar(ruta_csv: str, ruta_meta: str, salida: str) -> dict:
    """Escribe eventos_test.parquet (lo que se publica) y etiquetas_test.parquet (Is Laundering por seq)."""
    meta = json.loads(pathlib.Path(ruta_meta).read_text())
    t0 = time.perf_counter()
    df = ibm_aml.cargar(ruta_csv, meta["primary_end"][:10])
    historia, test = ibm_aml.dividir(df, meta["valid_cutoff"])
    errores = [e for ev in a_eventos(test.head(1000)) for e in errores_evento(ev)]
    if errores:
        raise ValueError(f"los eventos no cumplen el contrato: {errores[:3]}")
    out = pathlib.Path(salida)
    out.mkdir(parents=True, exist_ok=True)
    test[COLUMNAS].to_parquet(out / "eventos_test.parquet", index=False)
    etiquetas = test[["seq", "is_laundering"]].copy()
    etiquetas.insert(1, "transaction_uuid", [uuid_transaccion(FUENTE, int(s)) for s in etiquetas["seq"]])
    etiquetas.to_parquet(out / "etiquetas_test.parquet", index=False)
    resumen = {"filas_csv_en_rango": len(df), "historia": len(historia), "a_reproducir": len(test),
               "lavado_a_reproducir": int(test["is_laundering"].sum()),
               "seq": [int(test["seq"].min()), int(test["seq"].max())],
               "fechas": [str(test["timestamp"].min()), str(test["timestamp"].max())],
               "segundos": round(time.perf_counter() - t0, 1)}
    (out / "preparacion.json").write_text(json.dumps(resumen, indent=2, ensure_ascii=False))
    return resumen


def leer_escalones(texto: str) -> list[tuple[float, float]]:
    """'100:60,250:60' -> [(100.0, 60.0), (250.0, 60.0)]: tasa en eventos por segundo y duración en segundos."""
    escalones = []
    for parte in texto.split(","):
        tasa, duracion = (float(x) for x in parte.strip().split(":"))
        if tasa <= 0 or duracion <= 0:
            raise ValueError(f"escalón inválido: {parte!r}")
        escalones.append((tasa, duracion))
    return escalones


def _ahora_iso() -> tuple[str, int]:
    ahora = datetime.now(timezone.utc)
    return ahora.isoformat(timespec="milliseconds").replace("+00:00", "Z"), int(ahora.timestamp() * 1000)


@dataclass
class Escalon:
    tasa_objetivo: float
    duracion_objetivo: float
    enviados: int = 0
    invalidos: int = 0
    confirmados: int = 0
    errores: int = 0
    segundos: float = 0.0
    lat_append: list = field(default_factory=list)
    lat_ack: list = field(default_factory=list)
    sin_append_time: int = 0

    def resumen(self) -> dict:
        def pct(v):
            if not v:
                return None
            a = np.asarray(v, dtype=np.float64)
            return {"p50": float(np.percentile(a, 50)), "p95": float(np.percentile(a, 95)),
                    "p99": float(np.percentile(a, 99)), "max": float(a.max())}
        app = pct(self.lat_append)
        return {"tasa_objetivo": self.tasa_objetivo, "duracion_s": round(self.segundos, 2),
                "enviados": self.enviados, "invalidos": self.invalidos, "confirmados": self.confirmados,
                "errores": self.errores,
                "tasa_lograda": round(self.confirmados / self.segundos, 1) if self.segundos else 0.0,
                "latencia_fuente_kafka_ms": app, "latencia_ack_ms": pct(self.lat_ack),
                "sin_log_append_time": self.sin_append_time,
                # None si el escalón no alcanzó a publicar nada (se acabaron las transacciones)
                "cumple_rf1": None if not self.confirmados else bool(app and app["p99"] < OBJETIVO_P99_MS
                                                                       and self.errores == 0)}


def _fuente_de_eventos(ruta: str, desde_seq: int | None, maximo: int | None, frac_invalidos: float,
                       semilla: int, bloque: int = 2000):
    """Genera (clave, evento, es_valido, seq) en orden de seq, intercalando inválidos al azar."""
    df = pd.read_parquet(ruta)
    if desde_seq is not None:
        df = df[df["seq"] >= desde_seq]
    if maximo is not None:
        df = df.head(maximo)
    rng = np.random.default_rng(semilla)
    k_invalido = 0
    for ini in range(0, len(df), bloque):
        for ev in a_eventos(df.iloc[ini:ini + bloque]):
            if frac_invalidos and rng.random() < frac_invalidos:
                malo = eventos_invalidos(1, semilla=semilla + k_invalido)[0]
                malo["transaction_uuid"] = uuid_transaccion("invalido", semilla * 10_000_000 + k_invalido)
                k_invalido += 1
                clave = malo.get("source_account") if isinstance(malo.get("source_account"), str) else "invalido"
                yield clave, malo, False, None
            yield ev["source_account"], ev, True, ev["seq"]


def _solo_invalidos(n: int, semilla: int):
    """n eventos inválidos sin transacciones reales: llenan la cuarentena sin mover la reproducción."""
    for k, malo in enumerate(eventos_invalidos(n, semilla=semilla)):
        malo["transaction_uuid"] = uuid_transaccion("invalido", semilla * 10_000_000 + 5_000_000 + k)
        clave = malo.get("source_account") if isinstance(malo.get("source_account"), str) else "invalido"
        yield clave, malo, False, None


def publicar(ruta_eventos: str, bootstrap: str, topico: str, escalones: list[tuple[float, float]],
             frac_invalidos: float = 0.0, desde_seq: int | None = None, maximo: int | None = None,
             semilla: int = 7, config_extra: dict | None = None, registro=print,
             solo_invalidos: int | None = None) -> dict:
    from confluent_kafka import KafkaException, Producer  # import tardío: solo lo necesita el productor

    conf = {"bootstrap.servers": bootstrap, "client.id": "breb-productor", "acks": "all",
            "enable.idempotence": True, "linger.ms": 5, "compression.type": "lz4"}
    conf.update(config_extra or {})
    productor = Producer(conf)
    fuente = (_solo_invalidos(solo_invalidos, semilla) if solo_invalidos
              else _fuente_de_eventos(ruta_eventos, desde_seq, maximo, frac_invalidos, semilla))
    resultados: list[Escalon] = []
    ultimo_seq = None
    agotado = interrumpido = False

    def al_confirmar(esc: Escalon, produced_ms: int):
        def cb(err, msg):
            if err is not None:
                esc.errores += 1
                return
            esc.confirmados += 1
            esc.lat_ack.append(time.time() * 1000 - produced_ms)
            tipo, ts = msg.timestamp()
            if tipo == TIMESTAMP_LOG_APPEND_TIME:
                esc.lat_append.append(ts - produced_ms)
            else:
                esc.sin_append_time += 1
        return cb

    calentamiento = None
    try:
        # Calentamiento: el primer mensaje espera la metadata y el PID del productor idempotente (en un
        # bróker recién levantado, cientos de ms). Se envía y se espera su confirmación antes de medir.
        try:
            clave, ev, valido, seq = next(fuente)
        except StopIteration:
            agotado = True
        else:
            esc0 = Escalon(0.0, 0.0)
            ev["produced_at"], produced_ms = _ahora_iso()
            productor.produce(topico, key=clave.encode(), value=json.dumps(ev, separators=(",", ":")).encode(),
                              on_delivery=al_confirmar(esc0, produced_ms))
            productor.flush(30)
            calentamiento = {"confirmado": esc0.confirmados == 1, "valido": valido,
                             "latencia_ms": esc0.lat_ack[0] if esc0.lat_ack else None}
            if valido:
                ultimo_seq = seq
            registro(f"calentamiento: primer mensaje confirmado en {calentamiento['latencia_ms'] or float('nan'):.0f} ms (no entra en las mediciones)")
        for tasa, duracion in escalones:
            if agotado:
                break
            esc = Escalon(tasa, duracion)
            resultados.append(esc)
            registro(f"escalón {len(resultados)}/{len(escalones)}: {tasa:g} ev/s durante {duracion:g} s")
            t_ini = time.monotonic()
            while not agotado:
                transcurrido = time.monotonic() - t_ini
                if transcurrido >= duracion:
                    break
                debidos = min(int(transcurrido * tasa) + 1, int(duracion * tasa)) - esc.enviados
                for _ in range(max(debidos, 0)):
                    try:
                        clave, ev, valido, seq = next(fuente)
                    except StopIteration:
                        agotado = True
                        break
                    ev["produced_at"], produced_ms = _ahora_iso()
                    valor = json.dumps(ev, separators=(",", ":"), ensure_ascii=False).encode()
                    while True:
                        try:
                            productor.produce(topico, key=clave.encode(), value=valor,
                                              on_delivery=al_confirmar(esc, produced_ms))
                            break
                        except BufferError:
                            productor.poll(0.05)
                    esc.enviados += 1
                    if valido:
                        ultimo_seq = seq
                    else:
                        esc.invalidos += 1
                productor.poll(0)
                siguiente = t_ini + esc.enviados / tasa
                time.sleep(max(0.0, min(siguiente - time.monotonic(), 0.01)))
            esc.segundos = time.monotonic() - t_ini
            productor.flush(30)
            r = esc.resumen()
            lat = r["latencia_fuente_kafka_ms"] or {}
            registro(f"  confirmados {r['confirmados']:,} de {r['enviados']:,} | {r['tasa_lograda']:g} ev/s | "
                     f"fuente→Kafka p50 {lat.get('p50', float('nan')):.1f} ms, p99 {lat.get('p99', float('nan')):.1f} ms | "
                     f"errores {r['errores']}")
            if agotado:
                registro("no quedan transacciones por publicar")
                break
    except KeyboardInterrupt:
        interrumpido = True
        registro("interrumpido: esperando las confirmaciones pendientes")
    except KafkaException as e:
        registro(f"error de Kafka: {e}")
        raise
    finally:
        pendientes = productor.flush(30)
    return {"topico": topico, "bootstrap": bootstrap, "fraccion_invalidos": frac_invalidos,
            "calentamiento": calentamiento,
            "ultimo_seq_valido": ultimo_seq, "interrumpido": interrumpido, "sin_confirmar": pendientes,
            "escalones": [e.resumen() for e in resultados]}
