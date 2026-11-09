"""Lógica del Job 2 para un micro-batch, independiente de Spark.

El `foreachBatch` de Spark convierte el micro-batch a pandas (cientos de filas) y llama a
`procesar_microbatch`. Así la parte que decide (variables, modelo, estado, alertas) se
prueba sin Kafka ni Spark, y el código de Spark queda reducido a leer, convertir y escribir.

Orden de efectos en el micro-batch B (RNF-03):
1. leer el estado de MongoDB vigente antes de B (si B es un reintento, `streaming_prev`);
2. calcular variables en orden de seq, puntuar y decidir;
3. escribir el estado nuevo con batch_id = B;
4. (Spark) publicar las alertas en SNS y agregar las decisiones a risk_decisions con
   txnAppId/txnVersion = B. Si falla después del paso 3, el reintento recalcula lo mismo;
   las alertas pueden repetirse y los consumidores las descartan por transaction_uuid.
"""
from __future__ import annotations

import time
from dataclasses import dataclass, field
from datetime import datetime, timezone

import numpy as np
import pandas as pd

from breb.features.motor import MotorVariables, clave_par, derivar, preparar
from breb.modelo.puntuador import Puntuador

COLUMNAS_EVENTO = ["transaction_uuid", "seq", "event_ts", "from_bank", "source_account", "to_bank",
                   "destination_account", "amount_paid", "payment_currency", "amount_received",
                   "receiving_currency", "payment_format"]


@dataclass
class ResultadoLote:
    decisiones: pd.DataFrame
    alertas: list[dict] = field(default_factory=list)
    variables: pd.DataFrame | None = None
    documentos_escritos: int = 0
    tiempos: dict = field(default_factory=dict)   # ms por tramo: mongo_lectura, variables, modelo, mongo_escritura


def _ahora_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def a_tabla(eventos: pd.DataFrame) -> pd.DataFrame:
    ev = eventos.drop_duplicates("transaction_uuid").sort_values("seq", kind="stable")
    df = pd.DataFrame({
        "transaction_uuid": ev["transaction_uuid"].to_numpy(),
        "seq": ev["seq"].to_numpy(dtype=np.int64),
        "timestamp": pd.to_datetime(ev["event_ts"].to_numpy(), format="%Y-%m-%dT%H:%M:%S"),
        "from_bank": ev["from_bank"].to_numpy(dtype=np.int64),
        "to_bank": ev["to_bank"].to_numpy(dtype=np.int64),
        "from_node_key": ev["source_account"].to_numpy(),
        "to_node_key": ev["destination_account"].to_numpy(),
        "amount_paid": ev["amount_paid"].to_numpy(dtype=np.float64),
        "amount_received": ev["amount_received"].to_numpy(dtype=np.float64),
        "payment_currency": ev["payment_currency"].to_numpy(),
        "receiving_currency": ev["receiving_currency"].to_numpy(),
        "payment_format": ev["payment_format"].to_numpy(),
    })
    return preparar(df)


def procesar_microbatch(eventos: pd.DataFrame, batch_id: int, almacen, puntuador: Puntuador,
                        decision_ts: str | None = None, limite_escritura: int | None = None) -> ResultadoLote:
    """`eventos`: eventos válidos del micro-batch (columnas del contrato)."""
    if len(eventos) == 0:
        return ResultadoLote(decisiones=pd.DataFrame())
    df = a_tabla(eventos)
    claves = (set(df["from_node_key"]) | set(df["to_node_key"]),
              {clave_par(f, t) for f, t in zip(df["from_node_key"], df["to_node_key"])},
              {str(b) for b in df["from_bank"]} | {str(b) for b in df["to_bank"]})
    t0 = time.perf_counter()
    cargados = almacen.cargar(claves, batch_id)
    t1 = time.perf_counter()
    # Copia del estado previo para streaming_prev. Cada estado es un dict plano de números, así que basta
    # copiar cada dict (con 30.000 cuentas, deepcopy tarda 240 ms y esto 14 ms).
    iniciales = tuple({k: dict(v) for k, v in tabla.items()} for tabla in cargados)
    motor = MotorVariables(*cargados)
    crudas = motor.procesar(df)
    variables = derivar(df, crudas, puntuador.num)
    t2 = time.perf_counter()
    score = puntuador.puntuar(variables, df[puntuador.cat])
    t3 = time.perf_counter()
    escritos = almacen.guardar(motor, iniciales, batch_id, limite=limite_escritura)
    t4 = time.perf_counter()
    tiempos = {"mongo_lectura": 1000 * (t1 - t0), "variables": 1000 * (t2 - t1), "modelo": 1000 * (t3 - t2),
               "mongo_escritura": 1000 * (t4 - t3)}

    decision_ts = decision_ts or _ahora_iso()
    alerta = score >= puntuador.umbral
    decisiones = pd.DataFrame({
        "transaction_uuid": df["transaction_uuid"].to_numpy(),
        "seq": df["seq"].to_numpy(),
        "event_ts": df["timestamp"].to_numpy(),
        "entidad": df["from_bank"].to_numpy(),
        "entidad_destino": df["to_bank"].to_numpy(),
        "canal": df["payment_format"].to_numpy(),
        "risk_score": score.astype(np.float64),
        "umbral": puntuador.umbral,
        "decision": np.where(alerta, "alerta", "sin_alerta"),
        "model_version": puntuador.version,
        "batch_id": batch_id,
        "decision_ts": decision_ts,
    })
    decisiones["variables"] = list(variables.to_numpy(dtype=np.float32).astype(np.float64))

    alertas = []
    for i in np.flatnonzero(alerta):
        r, v = df.iloc[i], variables.iloc[i]
        alertas.append({
            "schema_version": "1.0",
            "transaction_uuid": r["transaction_uuid"],
            "event_ts": r["timestamp"].strftime("%Y-%m-%dT%H:%M:%S"),
            "origen": {"entidad": int(r["from_bank"]), "cuenta": r["from_node_key"]},
            "destino": {"entidad": int(r["to_bank"]), "cuenta": r["to_node_key"]},
            "monto": {"valor": float(r["amount_paid"]), "moneda": r["payment_currency"]},
            "canal": r["payment_format"],
            "score": round(float(score[i]), 6),
            "umbral": round(puntuador.umbral, 6),
            "model_version": puntuador.version,
            "batch_id": int(batch_id),
            "decision_ts": decision_ts,
            "contexto": {
                "transacciones_previas_del_ordenante": int(v["tx_count_from"]) - 1,
                "destinatarios_distintos_del_ordenante": int(v["distinct_recipients_from"]),
                "ordenantes_distintos_del_destino": int(v["recipient_unique_senders"]),
                "segundos_desde_ultima_entrada_al_ordenante":
                    int(v["sender_inflow_gap_sec"]) if v["sender_has_prior_inflow"] else None,
            },
        })
    return ResultadoLote(decisiones=decisiones, alertas=alertas, variables=variables, documentos_escritos=escritos,
                         tiempos=tiempos)
