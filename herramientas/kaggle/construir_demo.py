# -*- coding: utf-8 -*-
"""Genera demo_modelo_breb.ipynb: demo del modelo XGBoost para la sesión con el profesor."""
import json
import pathlib

OUT = pathlib.Path(__file__).with_name("demo_modelo_breb.ipynb")  # notebook de demo para Kaggle
cells = []
md = lambda s: cells.append({"cell_type": "markdown", "metadata": {}, "source": s.strip() + "\n"})
code = lambda s: cells.append({"cell_type": "code", "metadata": {}, "execution_count": None, "outputs": [], "source": s.strip() + "\n"})

md("""
# Demo del modelo de fraude · Bre-B (EAFIT ST1630)

Esta demo ejecuta el modelo XGBoost tal como lo hará el **Job 2 · riesgo + XGBoost (paso 3 del diagrama)** al final de cada micro-batch:
carga los artefactos, puntúa transacciones que el modelo nunca vio, emite las alertas y guarda cada decisión.

| Paso del diagrama | En esta demo |
|---|---|
| 2 · Amazon MSK (Kafka) | Las transacciones del test se leen en orden cronológico, en lotes de 500 (1 segundo a 500 eventos/s) |
| 3 · Job 2 | Carga del modelo, codificación de variables, score y decisión: **es el código real del Job 2** |
| 4 · MongoDB | **Simulado:** las 58 variables ya vienen calculadas en la muestra. En la arquitectura, el Job 2 las calcula con el estado guardado en MongoDB |
| 5 · SNS / SQS | Se imprime el mensaje de alerta con el formato que se publicaría en el tópico `fraud-alerts-critical` |
| 9 · risk_decisions | Se arma la tabla con una fila por transacción evaluada |

Modelo: adaptado de [XGBoost + Leiden community detection](https://www.kaggle.com/code/arturmarkosyan2/xgboost-leiden-community-detection-pr-auc-0-39) y entrenado en
[AML XGBoost Bre-B Fraud](https://www.kaggle.com/code/sebastianmedinacab/aml-xgboost-bre-b-fraud) con el dataset IBM AML (LI-Small).
""")

md("## 1. Parámetros de la demo")
code("""
TASA_EVENTOS_POR_SEGUNDO = 500   # tamaño del micro-batch: trigger de 1 s a 500 eventos/s
PAUSA_ENTRE_LOTES_S = 0.15       # solo para que la simulación se vea en vivo
MAX_ALERTAS_CON_DETALLE = 5      # cuántas alertas se imprimen con el mensaje completo
UMBRAL = None                    # None = umbral del modelo (model_meta.json); se puede cambiar para probar
""")

md("## 2. Carga de los artefactos del modelo (lo mismo que hace el Job 2 al arrancar)")
code("""
import glob, json, os, time, uuid
import numpy as np
import pandas as pd
import xgboost as xgb
from IPython.display import display

RAIZ = os.environ.get("DEMO_INPUT_ROOT", "/kaggle/input")
def buscar(nombre):
    hallados = glob.glob(f"{RAIZ}/**/{nombre}", recursive=True)
    assert hallados, f"No encuentro {nombre}: agreguen como Input la salida del notebook AML XGBoost Bre-B Fraud"
    return hallados[0]

meta = json.load(open(buscar("model_meta.json")))
modelo = xgb.Booster()                      # sin scikit-learn, igual que en el Job 2
modelo.load_model(buscar("aml_xgb.json"))
ITERACIONES = (0, meta["best_iteration"] + 1)  # árboles hasta la mejor iteración, como en el entrenamiento

def puntuar(X: pd.DataFrame) -> np.ndarray:
    return modelo.predict(xgb.DMatrix(X), iteration_range=ITERACIONES)

muestra = pd.read_parquet(buscar("golden_sample.parquet"))
UMBRAL = float(meta["threshold"]) if UMBRAL is None else float(UMBRAL)
VERSION_MODELO = f"aml_xgb v1 · iteración {meta['best_iteration']}"

def requiere(*nombres):
    faltan = [n for n in nombres if n not in globals()]
    if faltan:
        raise RuntimeError("Falta ejecutar la sección 4 (simulación del Job 2). "
                           "Usen Run → Run all, o ejecuten las celdas en orden de arriba hacia abajo.")

if xgb.__version__ != meta["versions"]["xgboost"]:
    print(f"Nota: este entorno tiene xgboost {xgb.__version__} y el modelo se entrenó con {meta['versions']['xgboost']}. "
          "La sección 5 comprueba que el score sea idéntico al calculado en el entrenamiento.\\n")

print(f"Modelo:            {VERSION_MODELO} (xgboost {xgb.__version__})")
print(f"Variables:         {len(meta['num_features'])} numéricas + {len(meta['cat_features'])} categóricas "
      f"= {len(meta['model_input_columns'])} columnas de entrada")
print(f"Umbral de alerta:  {UMBRAL:.4f}")
print(f"PR-AUC en el test completo: {meta['test_pr_auc']:.3f} "
      f"(un modelo aleatorio sacaría {meta['test_laundering'] / meta['test_rows']:.4f})")
print(f"Muestra a reproducir: {len(muestra):,} transacciones del test, "
      f"de {muestra['timestamp'].min():%Y-%m-%d %H:%M} a {muestra['timestamp'].max():%Y-%m-%d %H:%M} (hora original)")
duracion_original_s = (muestra["timestamp"].max() - muestra["timestamp"].min()).total_seconds()
duracion_demo_s = len(muestra) / TASA_EVENTOS_POR_SEGUNDO
print(f"A {TASA_EVENTOS_POR_SEGUNDO} eventos/s la muestra dura {duracion_demo_s:.0f} s de tráfico; "
      f"en el dataset original abarca {duracion_original_s / 3600:.1f} h "
      f"(reloj comprimido unas {duracion_original_s / max(duracion_demo_s, 1):.0f} veces)")
""")

md("""
## 3. Codificación de variables (código del Job 2)

El one-hot de canal y monedas se reconstruye con la lista `model_input_columns` de `model_meta.json`, sin cargar `ohe.joblib` (RF14).
Una categoría que el modelo no conoce queda en ceros, igual que en el entrenamiento.
""")
code("""
COLUMNAS = meta["model_input_columns"]
ONE_HOT = {c: [col[len(c) + 1:] for col in COLUMNAS if col.startswith(c + "_")] for c in meta["cat_features"]}

def matriz_de_entrada(lote: pd.DataFrame) -> pd.DataFrame:
    X = lote[meta["num_features"]].copy()
    for cat, valores in ONE_HOT.items():
        for v in valores:
            X[f"{cat}_{v}"] = (lote[cat] == v).astype("float32")
    return X[COLUMNAS]

for cat, valores in ONE_HOT.items():
    print(f"{cat:20s} {len(valores):2d} categorías: {', '.join(valores)}")
""")

md("""
## 4. Simulación del Job 2: micro-batches, score y alertas

Cada lote es un micro-batch. El mensaje de cada alerta tiene el formato que se publicaría en SNS (RF10).
El sistema **no conoce la etiqueta real**: se usa solo en la sección 5 para verificar.
""")
code("""
NS = uuid.UUID("6f1c2a52-6a3e-4c55-9d1e-0b5e2b6a1f00")
decisiones, alertas, scores_demo = [], [], []
n_lotes = int(np.ceil(len(muestra) / TASA_EVENTOS_POR_SEGUNDO))

for b in range(n_lotes):
    lote = muestra.iloc[b * TASA_EVENTOS_POR_SEGUNDO:(b + 1) * TASA_EVENTOS_POR_SEGUNDO]
    t0 = time.perf_counter()
    score = puntuar(matriz_de_entrada(lote))
    ms = (time.perf_counter() - t0) * 1000
    scores_demo.extend(score.tolist())
    nuevas = 0
    for (i, fila), s in zip(lote.iterrows(), score):
        tx_id = str(uuid.uuid5(NS, f"{fila.timestamp}|{fila.from_node_key}|{fila.to_node_key}|{i}"))
        alerta = bool(s >= UMBRAL)
        decisiones.append({"transaction_uuid": tx_id, "event_ts": fila.timestamp, "entidad": int(fila.from_bank),
                           "canal": fila.payment_format, "risk_score": round(float(s), 4), "umbral": round(UMBRAL, 4),
                           "decision": "alerta" if alerta else "sin alerta", "model_version": VERSION_MODELO})
        if alerta:
            nuevas += 1
            alertas.append(i)
            mensaje = {
                "transaction_uuid": tx_id,
                "event_ts": fila.timestamp.isoformat(),
                "origen": {"entidad": int(fila.from_bank), "cuenta": fila.from_node_key},
                "destino": {"entidad": int(fila.to_bank), "cuenta": fila.to_node_key},
                "monto": f"{fila.amount_paid:,.2f} {fila.payment_currency}",
                "canal": fila.payment_format,
                "score": round(float(s), 4), "umbral": round(UMBRAL, 4), "model_version": VERSION_MODELO,
                "contexto": {
                    "transacciones_previas_del_ordenante": int(fila.tx_count_from) - 1,
                    "destinatarios_distintos_del_ordenante": int(fila.distinct_recipients_from),
                    "ordenantes_distintos_del_destino": int(fila.recipient_unique_senders),
                    "segundos_desde_ultima_entrada_al_ordenante":
                        int(fila.sender_inflow_gap_sec) if fila.sender_has_prior_inflow else None,
                },
            }
            if len(alertas) <= MAX_ALERTAS_CON_DETALLE:
                print("  >>> ALERTA publicada en SNS fraud-alerts-critical:")
                print("      " + json.dumps(mensaje, ensure_ascii=False, indent=2).replace("\\n", "\\n      "))
            else:
                print(f"  >>> ALERTA {tx_id[:8]}… score {s:.3f} · {fila.payment_format} · {fila.amount_paid:,.0f} {fila.payment_currency}")
    print(f"micro-batch {b + 1:02d}/{n_lotes} | {len(lote)} transacciones | hora original "
          f"{lote.timestamp.min():%H:%M}–{lote.timestamp.max():%H:%M} | score en {ms:5.1f} ms | "
          f"alertas: {nuevas} (acumuladas: {len(alertas)})")
    time.sleep(PAUSA_ENTRE_LOTES_S)

risk_decisions = pd.DataFrame(decisiones)
print(f"\\nTransacciones evaluadas: {len(risk_decisions):,} | alertas: {len(alertas)}")
""")

md("### Tabla `risk_decisions` (paso 9): una fila por transacción evaluada (RF13)")
code("""
requiere("risk_decisions")
display(pd.concat([risk_decisions[risk_decisions.decision == "alerta"].head(5),
                   risk_decisions[risk_decisions.decision == "sin alerta"].head(3)]))
""")

md("""
## 5. Verificación con la etiqueta real

Esta parte no existe en el sistema en producción: la etiqueta `Is Laundering` solo se usa para medir el modelo.
""")
code("""
requiere("risk_decisions", "alertas", "scores_demo")
y = muestra["is_laundering"].to_numpy()
pred = risk_decisions["decision"].eq("alerta").to_numpy()
vp, fp, fn = int((pred & (y == 1)).sum()), int((pred & (y == 0)).sum()), int((~pred & (y == 1)).sum())
print(f"Casos reales de lavado en la muestra: {int(y.sum())}")
print(f"Alertas correctas (lavado detectado):  {vp}")
print(f"Alertas falsas (transacción legítima): {fp}")
print(f"Lavado no detectado:                   {fn}")
if vp + fp:
    print(f"Precisión en la muestra: {vp / (vp + fp):.2f} | recall: {vp / max(y.sum(), 1):.2f}")
print("En el test completo (1.038.880 transacciones): precisión 0,53 y recall 0,23 con el mismo umbral.")

tabla = muestra.loc[alertas, ["timestamp", "from_node_key", "to_node_key", "payment_format",
                               "amount_paid", "payment_currency", "is_laundering"]].copy()
tabla.insert(0, "score", [round(scores_demo[muestra.index.get_loc(i)], 4) for i in alertas])
tabla["¿era lavado?"] = tabla.pop("is_laundering").map({1: "sí", 0: "no"})
display(tabla.head(15))

diferencia = float(np.abs(np.array(scores_demo) - muestra["score"].to_numpy()).max())
print(f"Reproducibilidad: diferencia máxima con el score calculado en Kaggle = {diferencia:.2e} "
      f"({'coinciden' if diferencia < 1e-5 else 'NO coinciden: revisar versiones'})")
""")

md("""
## 6. ¿Por qué el modelo marcó esta transacción?

XGBoost descompone cada score en el aporte de cada variable (`pred_contribs`). Los aportes positivos suben el riesgo y los negativos lo bajan.
Esto es lo que vería un analista o un oficial de cumplimiento al revisar una alerta.
""")
code("""
requiere("alertas")
import matplotlib.pyplot as plt

DESCRIPCION = {
    "hour": "hora del día", "day_of_week": "día de la semana", "hour_sin": "hora del día (cíclica)",
    "hour_cos": "hora del día (cíclica)", "day_sin": "día de la semana (cíclico)", "day_cos": "día de la semana (cíclico)",
    "log_amount_paid": "monto enviado (log)", "log_amount_received": "monto recibido (log)",
    "amount_difference": "diferencia entre monto enviado y recibido", "log_amt_diff": "diferencia de montos (log)",
    "same_bank": "origen y destino en el mismo banco", "same_currency": "misma moneda de pago y recepción",
    "seconds_since_last_tx_from": "segundos desde el envío anterior del ordenante",
    "seconds_since_last_tx_to": "segundos desde la entrada anterior al destino",
    "tx_count_from": "envíos acumulados del ordenante", "tx_count_to": "entradas acumuladas del destino",
    "amt_sum_from": "monto total enviado por el ordenante", "amt_sum_to": "monto total recibido por el destino",
    "net_flow": "recibido por el destino menos enviado por el ordenante",
    "tx_count_ratio": "envíos del ordenante frente a entradas del destino",
    "flow_ratio": "monto enviado por el ordenante frente al recibido por el destino",
    "amt_to_avg_ratio_from": "monto actual frente al promedio del ordenante",
    "is_just_under_10k": "monto entre 9.000 y 10.000", "is_round_1k": "monto múltiplo de 1.000",
    "amount_mismatch": "monto enviado distinto del recibido", "is_cross_currency": "cambio de moneda",
    "from_bank_freq": "transacciones acumuladas del banco de origen", "to_bank_freq": "transacciones acumuladas del banco de destino",
    "distinct_users_on_recipient": "ordenantes distintos del destino (con este)",
    "is_first_sender_pair": "primera transacción entre este ordenante y este destino",
    "sender_avg_amt": "monto promedio previo del ordenante", "sender_std_amt": "variación del monto previo del ordenante",
    "z_score_amount_sender": "qué tan atípico es el monto para el ordenante", "amount_cents_fraction": "centavos del monto",
    "is_round_100": "monto múltiplo de 100", "is_just_under_5k": "monto entre 4.500 y 5.000",
    "is_night_tx": "transacción entre las 0 y las 5 h", "is_weekend": "fin de semana",
    "recipient_prev_tx_count": "entradas previas del destino", "recipient_prev_received": "monto previo recibido por el destino",
    "recipient_hist_mean": "monto promedio recibido por el destino", "recipient_amount_ratio": "monto actual frente al promedio del destino",
    "recipient_unique_senders": "ordenantes distintos previos del destino",
    "chain_turnaround_sec": "segundos desde la última entrada al destino",
    "chain_amount_preservation_ratio": "monto actual frente a la última entrada al destino",
    "is_rapid_forwarding": "última entrada al destino hace menos de 1 h", "pass_through_velocity": "velocidad de paso de fondos",
    "counterparty_revisit_count": "transacciones previas entre este par", "is_reciprocal_loop": "este ordenante ya le había enviado a este destino",
    "distinct_recipients_from": "destinatarios distintos del ordenante", "recipient_churn_from": "proporción de destinatarios nuevos del ordenante",
    "sender_inflow_gap_sec": "segundos desde la última entrada de fondos al ordenante",
    "sender_has_prior_inflow": "el ordenante había recibido fondos antes",
    "passthrough_amount_ratio": "fracción de la última entrada que el ordenante reenvía",
    "passthrough_velocity_true": "reenvío rápido de lo recibido",
    "pair_tx_share_from": "peso de este destino en los envíos del ordenante",
    "pair_tx_share_to": "peso de este ordenante en las entradas del destino",
    "pair_amt_share_from": "peso de este destino en el monto enviado por el ordenante",
}
ETIQUETA_CAT = {"payment_format": "canal", "receiving_currency": "moneda recibida", "payment_currency": "moneda pagada"}

def describir(col):
    for cat, nombre in ETIQUETA_CAT.items():
        if col.startswith(cat + "_"):
            return f"{nombre}: {col[len(cat) + 1:]}"
    return DESCRIPCION.get(col, col)

correctas = [i for i in alertas if muestra.at[i, "is_laundering"] == 1]
if correctas:
    elegida, motivo = max(correctas, key=lambda i: muestra.at[i, "score"]), "alerta correcta con mayor score"
elif alertas:
    elegida, motivo = max(alertas, key=lambda i: muestra.at[i, "score"]), "alerta con mayor score"
else:
    elegida, motivo = muestra["score"].idxmax(), "transacción con mayor score (no hubo alertas con este umbral)"

X1 = matriz_de_entrada(muestra.loc[[elegida]])
aportes = modelo.predict(xgb.DMatrix(X1), pred_contribs=True, iteration_range=ITERACIONES)[0]
score_reconstruido = 1 / (1 + np.exp(-aportes.sum()))
fila = muestra.loc[elegida]
print(f"Transacción elegida ({motivo}): score {fila.score:.4f} | ¿era lavado?: {'sí' if fila.is_laundering else 'no'}")
print(f"{fila.timestamp:%Y-%m-%d %H:%M} · {fila.from_node_key} → {fila.to_node_key} · "
      f"{fila.amount_paid:,.2f} {fila.payment_currency} · canal {fila.payment_format}")
ok = abs(score_reconstruido - fila.score) < 1e-4
print(f"Suma de aportes → score {score_reconstruido:.4f} ({'coincide' if ok else 'NO coincide'} con el score del modelo)")

top = pd.DataFrame({"columna": COLUMNAS, "aporte": aportes[:-1], "valor": X1.iloc[0].to_numpy()})
top = top.reindex(top.aporte.abs().sort_values(ascending=False).index).head(8)
top["variable"] = top.columna.map(describir)
BINARIAS = {c for c in COLUMNAS if c not in meta["num_features"] or c.startswith("is_")} | {
    "same_bank", "same_currency", "amount_mismatch", "sender_has_prior_inflow"}
top["valor"] = [("sí" if v == 1 else "no") if c in BINARIAS else f"{v:,.2f}" for c, v in zip(top.columna, top.valor)]
display(top[["variable", "valor", "aporte"]].round({"aporte": 3}).reset_index(drop=True))

fig, ax = plt.subplots(figsize=(9, 4))
orden = top.iloc[::-1]
ax.barh(orden.variable, orden.aporte, color=["#C00000" if a > 0 else "#2E9E44" for a in orden.aporte])
ax.axvline(0, color="#555", lw=0.8)
ax.set_xlabel("aporte al riesgo (log-odds): rojo sube el riesgo, verde lo baja")
ax.set_title(f"¿Por qué el modelo dio un score de {fila.score:.3f}?")
plt.tight_layout(); plt.show()
""")

md("""
## 7. Calibración del umbral

El umbral es el parámetro que ajustan los analistas de fraude (sección 1.4): más bajo detecta más lavado pero genera más alertas falsas.
""")
code("""
requiere("scores_demo")
y = muestra["is_laundering"].to_numpy()
filas = []
s_all = np.array(scores_demo)
for u in sorted({0.05, 0.10, round(UMBRAL, 4), 0.30, 0.50}):
    p = s_all >= u
    filas.append({"umbral": u, "alertas": int(p.sum()), "correctas": int((p & (y == 1)).sum()),
                  "falsas": int((p & (y == 0)).sum()), "lavado no detectado": int((~p & (y == 1)).sum()),
                  "nota": "umbral usado en la demo" if u == round(UMBRAL, 4) else ""})
display(pd.DataFrame(filas).set_index("umbral"))
""")

md("""
## Qué no muestra esta demo

- **Kafka, Spark, MongoDB y SNS reales:** aquí se simulan con un ciclo en Python. Se integran en el E2 con Docker Compose.
- **Cálculo de las 58 variables con estado:** en la muestra vienen calculadas. En el E2, el Job 2 las calcula con el estado de cuentas, pares y bancos en MongoDB, y se verifica que den lo mismo que esta muestra (riesgo R8).
""")

for k, c in enumerate(cells):
    c["id"] = f"celda-{k:02d}"
nb = {"nbformat": 4, "nbformat_minor": 5, "cells": cells,
      "metadata": {"kernelspec": {"display_name": "Python 3", "language": "python", "name": "python3"},
                   "language_info": {"name": "python"},
                   "kaggle": {"accelerator": "none", "isInternetEnabled": False, "dataSources": []}}}
OUT.write_text(json.dumps(nb, ensure_ascii=False, indent=1), encoding="utf-8")
print("ok", OUT.name, len(cells), "celdas")
