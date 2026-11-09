"""Linaje de una transacción (RNF-06): el oficial de cumplimiento la reconstruye con SQL desde el evento en Kafka
hasta Gold, su decisión y lo que hizo la entidad, consultando como el usuario `cumplimiento` (solo lectura en las
tres capas). Sin transaction_uuid, toma la alerta con el score más alto.
"""
from __future__ import annotations

import os
import time

from breb.consumo.trino import ErrorTrino, consulta, tabla_texto

USUARIO = "cumplimiento"
PASOS = [
    ("1. Bronze (evento recibido de Kafka)",
     "SELECT kafka_topic, kafka_partition, kafka_offset, kafka_timestamp, ingestion_timestamp, is_valid, "
     "cardinality(validation_errors) AS errores FROM lakehouse.bronze.transacciones WHERE transaction_uuid = '{u}'"),
    ("2. Silver (transacción limpia)",
     "SELECT seq, event_time, source_account, destination_account, amount_paid, payment_currency, payment_format, "
     "silver_timestamp FROM lakehouse.silver.transacciones WHERE transaction_uuid = '{u}'"),
    ("3. Gold · risk_decisions (decisión del Job 2)",
     "SELECT round(risk_score, 4) AS score, round(umbral, 4) AS umbral, decision, model_version, batch_id, decision_ts, "
     "round(latencia_ms) AS latencia_ms FROM lakehouse.gold.risk_decisions WHERE transaction_uuid = '{u}'"),
    ("4. Gold · fact_transferencias y dim_cuenta",
     "SELECT t.fecha, t.hora, c.canal, o.cuenta_id AS ordenante, o.nivel_riesgo AS riesgo_ordenante, "
     "d.cuenta_id AS destino, d.nivel_riesgo AS riesgo_destino, f.es_lavado "
     "FROM lakehouse.gold.fact_transferencias f "
     "JOIN lakehouse.gold.dim_tiempo t ON t.tiempo_key = f.tiempo_key "
     "JOIN lakehouse.gold.dim_canal c ON c.canal_key = f.canal_key "
     "JOIN lakehouse.gold.dim_cuenta o ON o.cuenta_key = f.cuenta_origen_key "
     "JOIN lakehouse.gold.dim_cuenta d ON d.cuenta_key = f.cuenta_destino_key WHERE f.transaction_uuid = '{u}'"),
]


def alerta_principal() -> str:
    _, filas = consulta("SELECT transaction_uuid FROM lakehouse.gold.risk_decisions WHERE decision = 'alerta' "
                        "ORDER BY risk_score DESC LIMIT 1", usuario=USUARIO)
    if not filas:
        raise SystemExit("risk_decisions no tiene alertas todavía")
    return filas[0][0]


def acciones_de_entidades(uuid: str) -> list[list]:
    """Lo que hicieron las entidades con la alerta (MongoDB, colección alertas_recibidas)."""
    if not os.environ.get("MONGO_MIEMBROS"):
        return []
    from pymongo import MongoClient
    db = MongoClient(f"mongodb://{os.environ['MONGO_MIEMBROS']}/?replicaSet=rs0&authSource=admin",
                     username=os.environ["MONGO_APP_USUARIO"], password=os.environ["MONGO_APP_CLAVE"],
                     serverSelectionTimeoutMS=5000)["breb"]
    try:
        return [[d["entidad"], "; ".join(d["acciones"]), d["recibida"]]
                for d in db.alertas_recibidas.find({"transaction_uuid": uuid})]
    except Exception:  # noqa: BLE001 - MongoDB es opcional para el linaje
        return []


def reconstruir(uuid: str | None = None, registro=print) -> float:
    t0 = time.perf_counter()
    uuid = uuid or alerta_principal()
    registro(f"linaje de la transacción {uuid} (usuario de Trino: {USUARIO})")
    for titulo, sql in PASOS:
        try:
            columnas, filas = consulta(sql.format(u=uuid), usuario=USUARIO)
            registro(f"\n{titulo}\n" + (tabla_texto(columnas, filas) if filas else "  (sin registros)"))
        except ErrorTrino as e:
            registro(f"\n{titulo}\n  error: {str(e).splitlines()[0]}")
    acciones = acciones_de_entidades(uuid)
    registro("\n5. Entidades (MongoDB, alertas_recibidas)\n"
             + (tabla_texto(["entidad", "acciones", "recibida"], acciones) if acciones
                else "  (ninguna entidad la procesó: ¿estaba abierto el servicio de la entidad?)"))
    segundos = time.perf_counter() - t0
    registro(f"\nlinaje reconstruido en {segundos:.1f} s (RNF-06: menos de 5 min)")
    return segundos
