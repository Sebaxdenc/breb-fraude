import hashlib
import json
import pathlib
import sys

import pandas as pd
import pytest

RAIZ = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(RAIZ / "src"))
sys.path.insert(0, str(RAIZ / "herramientas" / "referencia"))

FIXTURE = RAIZ / "tests" / "fixtures" / "referencia_sintetica"


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
