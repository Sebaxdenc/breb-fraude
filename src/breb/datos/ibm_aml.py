"""Carga de IBM AML (LI-Small) exactamente como el notebook de entrenamiento.

El orden de las filas importa: muchas variables son acumulados por cuenta, y el notebook
ordena por timestamp con un sort estable (los empates del mismo minuto conservan el orden
del CSV) después de quitar la cola posterior al rango primario. `seq` es la posición de la
fila en ese orden y es lo que el Job 2 usa para ordenar cada micro-batch.
"""
from __future__ import annotations

import pandas as pd

COLUMNAS_CSV = ["Timestamp", "From Bank", "Account", "To Bank", "Account.1", "Amount Received",
                "Receiving Currency", "Amount Paid", "Payment Currency", "Payment Format", "Is Laundering"]
NOMBRES = ["timestamp", "from_bank", "from_account", "to_bank", "to_account", "amount_received",
           "receiving_currency", "amount_paid", "payment_currency", "payment_format", "is_laundering"]


def cargar(ruta_csv: str, primary_end: str = "2022-09-11") -> pd.DataFrame:
    df = pd.read_csv(ruta_csv, usecols=COLUMNAS_CSV)
    df = df[COLUMNAS_CSV]
    df.columns = NOMBRES
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df = df.sort_values("timestamp", kind="stable").reset_index(drop=True)
    df = df.loc[df["timestamp"] < pd.Timestamp(primary_end)].reset_index(drop=True)
    df["from_node_key"] = df["from_bank"].astype(str) + "_" + df["from_account"].astype(str)
    df["to_node_key"] = df["to_bank"].astype(str) + "_" + df["to_account"].astype(str)
    df["seq"] = df.index.astype("int64")
    return df


def dividir(df: pd.DataFrame, valid_cutoff: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Historia previa al corte de validación (estado inicial) y transacciones a reproducir (test)."""
    corte = pd.Timestamp(valid_cutoff)
    historia = df["timestamp"] < corte
    return df.loc[historia], df.loc[~historia]
