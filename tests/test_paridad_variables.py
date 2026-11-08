"""Riesgo R8: el Job 2 debe calcular las mismas variables y el mismo score que el notebook."""
import numpy as np

from breb.features.motor import MotorVariables, derivar
from breb.modelo.puntuador import Puntuador


def _variables_test(referencia):
    motor = MotorVariables()
    motor.procesar(referencia["historia"])
    return derivar(referencia["test"], motor.procesar(referencia["test"]), referencia["meta"]["num_features"])


def test_mismo_orden_de_filas(referencia):
    test, ref = referencia["test"], referencia["ref"]
    assert len(test) == len(ref)
    assert (test["from_node_key"].to_numpy() == ref["from_node_key"].to_numpy()).all()
    assert (test["to_node_key"].to_numpy() == ref["to_node_key"].to_numpy()).all()
    assert (test["timestamp"].to_numpy() == ref["timestamp"].to_numpy()).all()


def test_58_variables_identicas(referencia):
    var = _variables_test(referencia)
    ref = referencia["ref"]
    distintas = {}
    for f in referencia["meta"]["num_features"]:
        a, b = var[f].to_numpy(np.float32), ref[f].to_numpy(np.float32)
        n = int((~((a == b) | (np.isnan(a) & np.isnan(b)))).sum())
        if n:
            distintas[f] = n
    assert distintas == {}, f"variables con filas distintas: {distintas}"


def test_score_identico(referencia):
    var = _variables_test(referencia)
    p = Puntuador(referencia["dir_modelo"])
    score = p.puntuar(var, referencia["test"][p.cat])
    dif = np.abs(score.astype(np.float64) - referencia["ref"]["score"].to_numpy())
    assert dif.max() < 1e-6, dif.max()
    assert ((score >= p.umbral) == (referencia["ref"]["score"].to_numpy() >= p.umbral)).all()


def test_categoria_desconocida_queda_en_ceros(referencia):
    p = Puntuador(referencia["dir_modelo"])
    var = _variables_test(referencia).head(3)
    cat = referencia["test"][p.cat].head(3).copy()
    cat["payment_currency"] = "Peso Colombiano"
    X = p.matriz(var, cat)
    assert X[[c for c in X.columns if c.startswith("payment_currency_")]].to_numpy().sum() == 0


def test_explicacion_suma_el_score(referencia):
    p = Puntuador(referencia["dir_modelo"])
    var = _variables_test(referencia).head(50)
    cat = referencia["test"][p.cat].head(50)
    aportes = p.explicar(var, cat)
    reconstruido = 1 / (1 + np.exp(-aportes.sum(axis=1)))
    assert np.abs(reconstruido - p.puntuar(var, cat)).max() < 1e-5
