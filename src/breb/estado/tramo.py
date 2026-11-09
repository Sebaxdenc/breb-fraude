"""Puntuación fuera de línea de todo el test y elección del tramo de la demo.

Recorre la historia y luego las 1.038.880 transacciones del test con el mismo motor del Job 2, en orden de
seq, y las puntúa con el modelo. Sirve para dos cosas:
- comprobar de punta a punta que el motor reproduce las métricas del notebook en todo el test (PR-AUC,
  verdaderos y falsos positivos al umbral), no solo en las 20.000 filas de golden_sample;
- elegir el tramo de N transacciones consecutivas con más alertas, para que la demo muestre alertas. La demo
  arranca en ese seq con el estado calculado hasta ahí (`python -m breb.estado cargar --desde-seq`).

El test se procesa por bloques: el estado del motor sigue de un bloque al siguiente y las variables de cada
fila no dependen de las demás filas del bloque, así que el resultado es el mismo que de una vez.
"""
from __future__ import annotations

import json
import pathlib
import time

import numpy as np
import pandas as pd

from breb.datos import ibm_aml
from breb.features.motor import MotorVariables, derivar, preparar
from breb.modelo.puntuador import Puntuador

BLOQUE = 200_000


def precision_promedio(y: np.ndarray, s: np.ndarray) -> float:
    """PR-AUC como sklearn.metrics.average_precision_score: suma de (R_n − R_{n−1}) · P_n por umbral distinto."""
    orden = np.argsort(-s, kind="mergesort")
    y, s = y[orden].astype(np.float64), s[orden]
    fin = np.r_[np.flatnonzero(np.diff(s)), len(s) - 1]
    vp = np.cumsum(y)[fin]
    precision = vp / (fin + 1)
    recall = vp / vp[-1]
    return float(np.sum((recall - np.r_[0.0, recall[:-1]]) * precision))


def puntuar_test(ruta_csv: str, dir_modelo: str, registro=print) -> tuple[pd.DataFrame, dict]:
    p = Puntuador(dir_modelo)
    meta = p.meta
    t0 = time.perf_counter()
    df = preparar(ibm_aml.cargar(ruta_csv, meta["primary_end"][:10]))
    historia, test = ibm_aml.dividir(df, meta["valid_cutoff"])
    motor = MotorVariables()
    motor.procesar(historia)
    del historia
    registro(f"estado de la historia listo en {time.perf_counter() - t0:.0f} s; puntuando {len(test):,} transacciones")
    partes = []
    for ini in range(0, len(test), BLOQUE):
        bloque = test.iloc[ini:ini + BLOQUE]
        variables = derivar(bloque, motor.procesar(bloque), p.num)
        partes.append(pd.DataFrame({
            "seq": bloque["seq"].to_numpy(), "event_ts": bloque["timestamp"].to_numpy(),
            "from_bank": bloque["from_bank"].to_numpy(), "to_bank": bloque["to_bank"].to_numpy(),
            "is_laundering": bloque["is_laundering"].to_numpy(),
            "score": p.puntuar(variables, bloque[p.cat])}))
    scores = pd.concat(partes, ignore_index=True)
    y, s = scores["is_laundering"].to_numpy(), scores["score"].to_numpy()
    alerta = s >= p.umbral
    metricas = {
        "transacciones": int(len(scores)), "lavado": int(y.sum()),
        "pr_auc": precision_promedio(y, s), "pr_auc_notebook": meta.get("test_pr_auc"),
        "umbral": p.umbral, "alertas": int(alerta.sum()),
        "verdaderos_positivos": int((alerta & (y == 1)).sum()), "falsos_positivos": int((alerta & (y == 0)).sum()),
        "falsos_negativos": int((~alerta & (y == 1)).sum()), "segundos": round(time.perf_counter() - t0, 1)}
    return scores, metricas


def elegir_tramo(scores: pd.DataFrame, umbral: float, n: int) -> dict:
    """Las n transacciones consecutivas (en orden de seq) con más alertas; a igualdad, con más lavado real
    entre las alertas."""
    n = min(n, len(scores))
    alerta = (scores["score"].to_numpy() >= umbral).astype(np.int64)
    acierto = alerta * scores["is_laundering"].to_numpy().astype(np.int64)
    ca, cv = np.r_[0, np.cumsum(alerta)], np.r_[0, np.cumsum(acierto)]
    por_alerta = ca[n:] - ca[:-n]
    por_acierto = cv[n:] - cv[:-n]
    mejores = np.flatnonzero(por_alerta == por_alerta.max())
    ini = int(mejores[np.argmax(por_acierto[mejores])])
    tramo = scores.iloc[ini:ini + n]
    alertas = tramo[tramo["score"] >= umbral]
    entidades = pd.concat([alertas["from_bank"], alertas["to_bank"]]).value_counts()
    return {"desde_seq": int(tramo["seq"].iloc[0]), "transacciones": int(n),
            "alertas": int(len(alertas)), "alertas_con_lavado_real": int(alertas["is_laundering"].sum()),
            "lavado_en_el_tramo": int(tramo["is_laundering"].sum()),
            "fechas": [str(tramo["event_ts"].iloc[0]), str(tramo["event_ts"].iloc[-1])],
            "entidades_con_mas_alertas": [int(e) for e in entidades.index[:5]],
            "alertas_promedio_por_tramo": round(float(alerta.mean() * n), 1)}


def guardar(scores: pd.DataFrame, metricas: dict, tramo: dict, salida: str) -> None:
    d = pathlib.Path(salida)
    d.mkdir(parents=True, exist_ok=True)
    scores[["seq", "score", "is_laundering"]].to_parquet(d / "scores_test.parquet", index=False)
    (d / "tramo_demo.json").write_text(json.dumps({"metricas_test": metricas, "tramo": tramo}, indent=2,
                                                  ensure_ascii=False))
