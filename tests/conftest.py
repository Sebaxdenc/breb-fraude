import hashlib
import json
import pathlib
import sys

import numpy as np
import pandas as pd
import pytest

RAIZ = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(RAIZ / "src"))
sys.path.insert(0, str(RAIZ / "herramientas" / "referencia"))

FIXTURE = RAIZ / "tests" / "fixtures" / "referencia_sintetica"
ULP_LOG1P = 2   # ver VARIABLES_LOG1P_F32 en breb.features.motor


def _filas_distintas(nombre, a, b):
    a, b = np.asarray(a, dtype=np.float32), np.asarray(b, dtype=np.float32)
    iguales = (a == b) | (np.isnan(a) & np.isnan(b))
    from breb.features.motor import VARIABLES_LOG1P_F32
    if nombre in VARIABLES_LOG1P_F32:
        ulp = np.abs(a.view(np.int32).astype(np.int64) - b.view(np.int32).astype(np.int64))
        iguales |= (np.sign(a) == np.sign(b)) & (ulp <= ULP_LOG1P)
    return int((~iguales).sum())


@pytest.fixture(scope="session")
def filas_distintas():
    """Cuenta filas distintas entre motor y referencia; exacto salvo VARIABLES_LOG1P_F32 (≤ 2 ULP)."""
    return _filas_distintas


@pytest.fixture(scope="session")
def referencia(tmp_path_factory):
    """Dataset sintético regenerado (determinista) + salida del notebook de entrenamiento sobre él."""
    from generar_dataset_sintetico import generar
    from breb.datos import ibm_aml
    from breb.features.motor import preparar

    origen = json.loads((FIXTURE / "origen.json").read_text())
    d = tmp_path_factory.mktemp("dataset")
    csv = generar(d, origen["filas"], origen["semilla"])
    assert hashlib.sha256(csv.read_bytes()).hexdigest() == origen["sha256_csv"], \
        "el generador cambió: regenere la referencia con ejecutar_notebook_referencia.py"
    meta = json.loads((FIXTURE / "model_meta.json").read_text())
    df = preparar(ibm_aml.cargar(str(csv), meta["primary_end"][:10]))
    historia, test = ibm_aml.dividir(df, meta["valid_cutoff"])
    ref = pd.read_parquet(FIXTURE / "test_completo_referencia.parquet")
    return {"csv": csv, "meta": meta, "historia": historia, "test": test, "ref": ref, "dir_modelo": FIXTURE}
