"""Compara las variables del motor del Job 2 contra la referencia del notebook (rápido, sin pytest)."""
import json
import pathlib
import sys
import time

import numpy as np
import pandas as pd

RAIZ = pathlib.Path(__file__).resolve().parents[2]
sys.path.insert(0, str(RAIZ / "src"))
from breb.datos import ibm_aml  # noqa: E402
from breb.features.motor import MotorVariables, derivar, preparar  # noqa: E402

dir_ds, dir_ref = pathlib.Path(sys.argv[1]), pathlib.Path(sys.argv[2])
meta = json.load(open(dir_ref / "model_meta.json"))
ref = pd.read_parquet(dir_ref / "test_completo_referencia.parquet")

df = preparar(ibm_aml.cargar(str(dir_ds / "LI-Small_Trans.csv"), meta["primary_end"][:10]))
hist, test = ibm_aml.dividir(df, meta["valid_cutoff"])
t0 = time.perf_counter()
motor = MotorVariables()
motor.procesar(hist)
t1 = time.perf_counter()
crudas = motor.procesar(test)
var = derivar(test, crudas, meta["num_features"])
t2 = time.perf_counter()
print(f"historia {len(hist):,} filas en {t1 - t0:.1f} s ({(t1 - t0) / len(hist) * 1e6:.1f} µs/fila) | test {len(test):,} filas en {t2 - t1:.1f} s")
assert len(var) == len(ref), (len(var), len(ref))
assert (test["from_node_key"].to_numpy() == ref["from_node_key"].to_numpy()).all(), "orden distinto"

malas = 0
for f in meta["num_features"]:
    a = var[f].to_numpy(dtype=np.float32)
    b = ref[f].to_numpy(dtype=np.float32)
    distintos = ~((a == b) | (np.isnan(a) & np.isnan(b)))
    if distintos.any():
        malas += 1
        i = int(np.flatnonzero(distintos)[0])
        rel = np.max(np.abs(a[distintos] - b[distintos]) / np.maximum(np.abs(b[distintos]), 1e-12))
        print(f"  {f:34s} filas distintas: {distintos.sum():6d} | max dif relativa {rel:.2e} | ej fila {i}: motor={a[i]!r} ref={b[i]!r}")
print(f"variables con diferencias: {malas} de {len(meta['num_features'])}")
