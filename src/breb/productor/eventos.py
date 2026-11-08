"""Conversión de filas de IBM AML a eventos Bre-B (contrato evento_transferencia 1.0)."""
from __future__ import annotations

from datetime import datetime, timezone

import numpy as np
import pandas as pd

from breb.contratos import uuid_transaccion

FUENTE = "ibm-aml-li-small"


def _ahora() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def a_eventos(df: pd.DataFrame, fuente: str = FUENTE) -> list[dict]:
    """`df` viene de breb.datos.ibm_aml.cargar. La etiqueta is_laundering NO viaja en el evento."""
    ts = df["timestamp"].dt.strftime("%Y-%m-%dT%H:%M:%S").to_numpy()
    eventos = []
    for i, r in enumerate(df.itertuples(index=False)):
        eventos.append({
            "schema_version": "1.0",
            "transaction_uuid": uuid_transaccion(fuente, int(r.seq)),
            "seq": int(r.seq),
            "event_ts": ts[i],
            "produced_at": _ahora(),
            "from_bank": int(r.from_bank),
            "source_account": r.from_node_key,
            "to_bank": int(r.to_bank),
            "destination_account": r.to_node_key,
            "amount_paid": float(r.amount_paid),
            "payment_currency": r.payment_currency,
            "amount_received": float(r.amount_received),
            "receiving_currency": r.receiving_currency,
            "payment_format": r.payment_format,
        })
    return eventos


def eventos_invalidos(n: int, semilla: int = 0) -> list[dict]:
    """Eventos mal formados para demostrar la cuarentena (RF2). No llevan un seq válido,
    así que nunca alteran el estado del Job 2."""
    rng = np.random.default_rng(semilla)
    base = {"schema_version": "1.0", "transaction_uuid": "", "seq": 0, "event_ts": "2022-09-09T03:13:00",
            "produced_at": _ahora(), "from_bank": 1, "source_account": "1_ABC", "to_bank": 2,
            "destination_account": "2_DEF", "amount_paid": 10.0, "payment_currency": "US Dollar",
            "amount_received": 10.0, "receiving_currency": "US Dollar", "payment_format": "ACH"}
    defectos = [
        lambda e: e.pop("amount_paid"),
        lambda e: e.update(amount_paid=-5.0),
        lambda e: e.update(payment_currency="Peso Colombiano"),
        lambda e: e.update(event_ts="09/09/2022 03:13"),
        lambda e: e.update(source_account="sin-banco"),
        lambda e: e.update(payment_format="Transfiya"),
        lambda e: e.update(campo_extra=True),
    ]
    salida = []
    for k in range(n):
        e = dict(base)
        e["transaction_uuid"] = uuid_transaccion("invalido", k)
        e["seq"] = -1
        defectos[int(rng.integers(0, len(defectos)))](e)
        salida.append(e)
    return salida
