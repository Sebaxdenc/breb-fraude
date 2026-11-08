"""Genera verificar_paridad_kaggle.ipynb: comprueba con los datos REALES de LI-Small que el motor
del Job 2 calcula las mismas 58 variables y el mismo score que el notebook de entrenamiento.

El código del motor se copia tal cual desde src/breb (así el notebook prueba exactamente el
código que corre en el Job 2). Se ejecuta en Kaggle porque allí ya están montados el dataset
(unos 7 millones de filas) y la salida de la versión 1 del entrenamiento, y hay 30 GB de RAM.

Uso: python construir_verificacion.py   (escribe el .ipynb junto a este archivo)
"""
import json
import pathlib

RAIZ = pathlib.Path(__file__).resolve().parents[2]
OUT = pathlib.Path(__file__).with_name("verificar_paridad_kaggle.ipynb")
cells = []
md = lambda s: cells.append({"cell_type": "markdown", "metadata": {}, "source": s.strip() + "\n"})
code = lambda s: cells.append({"cell_type": "code", "metadata": {}, "execution_count": None, "outputs": [],
                               "source": s.strip() + "\n"})

md("""
# Verificación de paridad · motor del Job 2 vs. notebook de entrenamiento

Riesgo R8 del Entregable 1: si el Job 2 calcula las variables distinto al notebook, el modelo pierde las
métricas del test. Este notebook reconstruye el estado con toda la historia anterior al corte de validación,
calcula las variables de las transacciones de `golden_sample.parquet` con el **mismo código del Job 2**
(`src/breb/features/motor.py`) y las compara una por una.

**Inputs necesarios:** el dataset *IBM Transactions for Anti Money Laundering (AML)* y la salida del notebook
*AML XGBoost Bre-B Fraud* (versión 1). Accelerator: None. Tarda unos minutos.
""")
code("""
import glob, json, os, time
import numpy as np
import pandas as pd
import xgboost as xgb

RAIZ = os.environ.get("VERIF_INPUT_ROOT", "/kaggle/input")
def buscar(nombre):
    hallados = sorted(glob.glob(f"{RAIZ}/**/{nombre}", recursive=True))
    assert hallados, f"No encuentro {nombre}: revisen los Inputs del notebook"
    return hallados[0]

RUTA_CSV = buscar("LI-Small_Trans.csv")
meta = json.load(open(buscar("model_meta.json")))
golden = pd.read_parquet(buscar("golden_sample.parquet"))
print("CSV:", RUTA_CSV)
print(f"golden_sample: {len(golden):,} filas | corte de validación: {meta['valid_cutoff']} | xgboost {xgb.__version__}")
""")
md("## Código del Job 2 (copiado de `src/breb/datos/ibm_aml.py`)")
code((RAIZ / "src/breb/datos/ibm_aml.py").read_text())
md("## Código del Job 2 (copiado de `src/breb/features/motor.py`)")
code((RAIZ / "src/breb/features/motor.py").read_text())
md("## Estado inicial con la historia y variables del test")
code("""
t0 = time.perf_counter()
df = preparar(cargar(RUTA_CSV, meta["primary_end"][:10]))
historia, test = dividir(df, meta["valid_cutoff"])
print(f"filas: {len(df):,} | historia: {len(historia):,} | a reproducir: {len(test):,} ({time.perf_counter() - t0:.0f} s)")

t0 = time.perf_counter()
motor = MotorVariables()
motor.procesar(historia)
print(f"estado inicial: {len(motor.cuentas):,} cuentas, {len(motor.pares):,} pares, {len(motor.bancos):,} bancos "
      f"({time.perf_counter() - t0:.0f} s)")

muestra = test.iloc[:len(golden)]
variables = derivar(muestra, motor.procesar(muestra), meta["num_features"])
""")
md("## Comparación")
code("""
iguales_clave = all((muestra[c].to_numpy() == golden[c].to_numpy()).all()
                    for c in ["timestamp", "from_node_key", "to_node_key"])
print("Mismas transacciones y en el mismo orden:", "sí" if iguales_clave else "NO")

filas = []
for f in meta["num_features"]:
    a, b = variables[f].to_numpy(np.float32), golden[f].to_numpy(np.float32)
    distinta = ~((a == b) | (np.isnan(a) & np.isnan(b)))
    filas.append({"variable": f, "filas_distintas": int(distinta.sum()),
                  "max_dif_relativa": float(np.max(np.abs(a[distinta] - b[distinta]) / np.maximum(np.abs(b[distinta]), 1e-12)))
                  if distinta.any() else 0.0})
tabla = pd.DataFrame(filas)
malas = tabla[tabla.filas_distintas > 0]
print(f"Variables idénticas: {len(tabla) - len(malas)} de {len(tabla)}")
if len(malas):
    display(malas)

booster = xgb.Booster()
booster.load_model(buscar("aml_xgb.json"))
columnas = meta["model_input_columns"]
X = variables.copy()
for cat in meta["cat_features"]:
    for col in [c for c in columnas if c.startswith(cat + "_")]:
        X[col] = (muestra[cat].to_numpy() == col[len(cat) + 1:]).astype(np.float32)
score = booster.predict(xgb.DMatrix(X[columnas]), iteration_range=(0, meta["best_iteration"] + 1))
dif = np.abs(score.astype(np.float64) - golden["score"].to_numpy(np.float64))
umbral = meta["threshold"]
mismas_decisiones = int(((score >= umbral) == (golden["score"].to_numpy() >= umbral)).sum())
print(f"Diferencia máxima de score: {dif.max():.2e} | decisiones iguales: {mismas_decisiones:,} de {len(score):,}")
ok = iguales_clave and len(malas) == 0 and dif.max() < 1e-5
print("\\nRESULTADO:", "PARIDAD OK" if ok else "HAY DIFERENCIAS: copien esta salida y revísenla con el equipo")
""")

for k, c in enumerate(cells):
    c["id"] = f"celda-{k:02d}"
nb = {"nbformat": 4, "nbformat_minor": 5, "cells": cells,
      "metadata": {"kernelspec": {"display_name": "Python 3", "language": "python", "name": "python3"},
                   "language_info": {"name": "python"},
                   "kaggle": {"accelerator": "none", "isInternetEnabled": False, "dataSources": []}}}
OUT.write_text(json.dumps(nb, ensure_ascii=False, indent=1), encoding="utf-8")
print("ok", OUT.name, len(cells), "celdas")
