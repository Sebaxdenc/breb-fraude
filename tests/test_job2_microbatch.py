"""Job 2 de punta a punta sin Spark: estado inicial en MongoDB (mongomock), reproducción en
micro-batches, idempotencia ante reintentos y formato de las alertas."""
import mongomock
import numpy as np
import pandas as pd
import pytest

from breb.contratos import errores_alerta, errores_evento
from breb.estado.almacen import AlmacenMemoria, AlmacenMongo
from breb.features.motor import MotorVariables, clave_par
from breb.job2.microbatch import procesar_microbatch
from breb.modelo.puntuador import Puntuador
from breb.productor.eventos import a_eventos

TAM_LOTE = 500


@pytest.fixture(scope="module")
def puntuador(referencia):
    return Puntuador(referencia["dir_modelo"])


@pytest.fixture(scope="module")
def eventos_test(referencia):
    return pd.DataFrame(a_eventos(referencia["test"]))


def _motor_inicial(referencia, eventos=None):
    motor = MotorVariables()
    motor.procesar(referencia["historia"])
    if eventos is not None:   # solo las claves que van a leerse (mongomock es lento con colecciones grandes)
        cuentas = set(eventos["source_account"]) | set(eventos["destination_account"])
        pares = {clave_par(f, t) for f, t in zip(eventos["source_account"], eventos["destination_account"])}
        bancos = {str(b) for b in eventos["from_bank"]} | {str(b) for b in eventos["to_bank"]}
        motor = MotorVariables({k: v for k, v in motor.cuentas.items() if k in cuentas},
                               {k: v for k, v in motor.pares.items() if k in pares},
                               {k: v for k, v in motor.bancos.items() if k in bancos})
    return motor


def _almacen_con_estado_inicial(referencia, tipo, eventos=None):
    motor = _motor_inicial(referencia, eventos)
    if tipo == "mongo":
        almacen = AlmacenMongo(mongomock.MongoClient().breb, w=1, read_concern="local")
        almacen.cargar_estado_inicial(motor)
    else:
        almacen = AlmacenMemoria()
        motor.tocadas = (set(motor.cuentas), set(motor.pares), set(motor.bancos))
        almacen.guardar(motor, ({}, {}, {}), -1)
    return almacen


def test_eventos_cumplen_contrato(eventos_test):
    for e in eventos_test.head(200).to_dict("records"):
        assert errores_evento(e) == []


def test_almacen_mongo_equivale_a_memoria_con_reintento(referencia, puntuador, eventos_test):
    """Mismo resultado con MongoDB que en memoria, incluido un reintento tras falla parcial."""
    eventos = eventos_test.iloc[:3 * TAM_LOTE]
    mongo = _almacen_con_estado_inicial(referencia, "mongo", eventos)
    memoria = _almacen_con_estado_inicial(referencia, "memoria")
    for b in range(3):
        lote = eventos.iloc[b * TAM_LOTE:(b + 1) * TAM_LOTE]
        esperado = procesar_microbatch(lote, b, memoria, puntuador, decision_ts="t")
        if b == 1:
            with pytest.raises(ConnectionError):
                procesar_microbatch(lote, b, mongo, puntuador, decision_ts="t", limite_escritura=400)
        obtenido = procesar_microbatch(lote, b, mongo, puntuador, decision_ts="t")
        pd.testing.assert_frame_equal(esperado.decisiones, obtenido.decisiones)
        assert esperado.alertas == obtenido.alertas
    col = mongo.cols[0]
    for doc in col.find({}):
        assert doc["streaming"] == memoria.docs[0][doc["_id"]]["streaming"]


def test_reproduccion_completa_en_microbatches(referencia, puntuador, eventos_test):
    almacen = _almacen_con_estado_inicial(referencia, "memoria")
    lotes = []
    for b, inicio in enumerate(range(0, len(eventos_test), TAM_LOTE)):
        lote = eventos_test.iloc[inicio:inicio + TAM_LOTE].sample(frac=1, random_state=b)  # llegan desordenados
        r = procesar_microbatch(lote, b, almacen, puntuador, decision_ts="2026-11-10T00:00:00.000Z")
        for a in r.alertas:
            assert errores_alerta(a) == []
        lotes.append(r.decisiones)
    dec = pd.concat(lotes, ignore_index=True)
    ref = referencia["ref"]
    assert len(dec) == len(ref)
    assert np.abs(dec["risk_score"].to_numpy() - ref["score"].to_numpy()).max() < 1e-6
    var = np.vstack(dec["variables"].to_numpy())
    esperado = ref[referencia["meta"]["num_features"]].to_numpy(np.float32)
    assert np.array_equal(var.astype(np.float32), esperado)


def test_reintento_tras_falla_a_mitad_de_escritura(referencia, puntuador, eventos_test):
    """Un micro-batch que falla después de escribir parte del estado y se reintenta da el mismo
    resultado que si no hubiera fallado (RNF-03)."""
    limpio = _almacen_con_estado_inicial(referencia, "memoria")
    con_falla = _almacen_con_estado_inicial(referencia, "memoria")
    lotes = [eventos_test.iloc[i:i + TAM_LOTE] for i in range(0, 3 * TAM_LOTE, TAM_LOTE)]
    for b, lote in enumerate(lotes):
        esperado = procesar_microbatch(lote, b, limpio, puntuador, decision_ts="t")
        if b == 1:
            with pytest.raises(ConnectionError):
                procesar_microbatch(lote, b, con_falla, puntuador, decision_ts="t", limite_escritura=300)
        obtenido = procesar_microbatch(lote, b, con_falla, puntuador, decision_ts="t")
        pd.testing.assert_frame_equal(esperado.decisiones, obtenido.decisiones)
        assert esperado.alertas == obtenido.alertas
    assert limpio.docs == con_falla.docs


def test_duplicados_del_mismo_evento_no_cuentan_dos_veces(referencia, puntuador, eventos_test):
    a = _almacen_con_estado_inicial(referencia, "memoria")
    b = _almacen_con_estado_inicial(referencia, "memoria")
    lote = eventos_test.iloc[:TAM_LOTE]
    duplicado = pd.concat([lote, lote.iloc[:50]])
    r1 = procesar_microbatch(lote, 0, a, puntuador, decision_ts="t")
    r2 = procesar_microbatch(duplicado, 0, b, puntuador, decision_ts="t")
    pd.testing.assert_frame_equal(r1.decisiones, r2.decisiones)
    assert a.docs == b.docs
