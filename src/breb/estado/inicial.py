"""Estado inicial del Job 2 (DAG bootstrap_state, RF8).

Recorre con el mismo motor del Job 2 todas las transacciones anteriores al corte de
validación del modelo y deja el estado listo para que el productor reproduzca desde ese
corte. Usar el mismo motor (y no una agregación aparte) garantiza que el estado sea el
mismo que el notebook usó para calcular las variables del test.
"""
from __future__ import annotations

import json
import pathlib
import time

from breb.datos import ibm_aml
from breb.features.motor import MotorVariables, preparar


def calcular(ruta_csv: str, dir_modelo: str) -> tuple[MotorVariables, dict]:
    meta = json.loads((pathlib.Path(dir_modelo) / "model_meta.json").read_text())
    t0 = time.perf_counter()
    df = preparar(ibm_aml.cargar(ruta_csv, meta["primary_end"][:10]))
    historia, replay = ibm_aml.dividir(df, meta["valid_cutoff"])
    motor = MotorVariables()
    motor.procesar(historia)
    resumen = {
        "filas_historia": int(len(historia)),
        "filas_a_reproducir": int(len(replay)),
        "primer_seq_a_reproducir": int(replay["seq"].iloc[0]) if len(replay) else None,
        "cuentas": len(motor.cuentas), "pares": len(motor.pares), "bancos": len(motor.bancos),
        "segundos": round(time.perf_counter() - t0, 1),
    }
    return motor, resumen
