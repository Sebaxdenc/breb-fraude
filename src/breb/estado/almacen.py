"""Estado del Job 2 en MongoDB (RF8, RNF-03, RNF-04).

Colecciones `cuentas`, `pares` y `bancos`. El Job 2 escribe solo el subdocumento
`streaming` (y su copia `streaming_prev`); Airflow escribe `historico` y el servicio de
la entidad escribe `estado_retencion`. Ningún escritor toca los campos de otro.

Idempotencia por micro-batch: cada documento guarda en `streaming.batch_id` el último
micro-batch aplicado y en `streaming_prev` el estado anterior a ese micro-batch. Si Spark
reintenta el micro-batch B (mismos offsets de Kafka), los documentos que ya alcanzaron a
escribirse con batch_id = B se leen desde `streaming_prev`, de modo que el reintento parte
del mismo estado y produce exactamente el mismo resultado, aunque la falla haya ocurrido a
mitad de la escritura.
"""
from __future__ import annotations

import copy
from typing import Iterable

from pymongo import UpdateOne
from pymongo.read_concern import ReadConcern
from pymongo.write_concern import WriteConcern

from breb.features.motor import MotorVariables

COLECCIONES = ("cuentas", "pares", "bancos")
LOTE_ESCRITURA = 5000


class AlmacenMongo:
    def __init__(self, db, w: str | int = "majority", read_concern: str = "majority"):
        self.cols = [db.get_collection(n, write_concern=WriteConcern(w=w),
                                       read_concern=ReadConcern(read_concern)) for n in COLECCIONES]

    def cargar(self, claves: tuple[Iterable, Iterable, Iterable], batch_id: int) -> tuple[dict, dict, dict]:
        """Estado vigente ANTES del micro-batch `batch_id` para las claves pedidas."""
        tablas = []
        for col, ks in zip(self.cols, claves):
            ks = list(set(ks))
            tabla = {}
            if ks:
                for doc in col.find({"_id": {"$in": ks}}, {"streaming": 1, "streaming_prev": 1}):
                    actual = doc.get("streaming")
                    if actual is None:
                        continue
                    aplicado = actual.get("batch_id", -1)
                    if aplicado == batch_id:
                        estado = doc.get("streaming_prev")       # reintento del mismo micro-batch
                    elif aplicado < batch_id:
                        estado = actual
                    else:
                        raise RuntimeError(f"{col.name}/{doc['_id']}: estado del micro-batch {aplicado}, "
                                           f"posterior al {batch_id} que se intenta procesar")
                    if estado is not None:
                        estado = {k: v for k, v in estado.items() if k != "batch_id"}
                        tabla[doc["_id"]] = estado
            tablas.append(tabla)
        return tablas[0], tablas[1], tablas[2]

    def guardar(self, motor: MotorVariables, iniciales: tuple[dict, dict, dict], batch_id: int,
                limite: int | None = None) -> int:
        """Escribe el estado de las claves tocadas. `limite` simula una falla a mitad de la escritura (pruebas)."""
        escritos = 0
        for col, tabla, inicial, tocadas in zip(self.cols, (motor.cuentas, motor.pares, motor.bancos), iniciales,
                                                 motor.tocadas):
            ops = []
            for k in tocadas:
                nuevo = dict(tabla[k])
                nuevo["batch_id"] = batch_id
                ops.append(UpdateOne({"_id": k}, {"$set": {"streaming": nuevo, "streaming_prev": inicial.get(k)}},
                                     upsert=True))
            for i in range(0, len(ops), LOTE_ESCRITURA):
                parte = ops[i:i + LOTE_ESCRITURA]
                if limite is not None and escritos + len(parte) > limite:
                    parte = parte[:max(limite - escritos, 0)]
                    if parte:
                        col.bulk_write(parte, ordered=False)
                    raise ConnectionError("falla simulada a mitad de la escritura")
                col.bulk_write(parte, ordered=False)
                escritos += len(parte)
        return escritos

    def cargar_estado_inicial(self, motor: MotorVariables, batch_id: int = -1) -> int:
        """Carga masiva del estado calculado sobre la historia (DAG bootstrap_state).

        Las colecciones deben estar vacías: se insertan los documentos por lotes, sin armar en memoria
        una operación por documento (con LI-Small son unos 2,1 millones)."""
        escritos = 0
        for col, tabla in zip(self.cols, (motor.cuentas, motor.pares, motor.bancos)):
            if col.estimated_document_count():
                raise RuntimeError(f"la colección {col.name} ya tiene documentos: bórrenla antes de cargar el estado")
            lote = []
            for k, estado in tabla.items():
                lote.append({"_id": k, "streaming": dict(estado, batch_id=batch_id), "streaming_prev": None})
                if len(lote) == LOTE_ESCRITURA:
                    col.insert_many(lote, ordered=False)
                    escritos += len(lote)
                    lote = []
            if lote:
                col.insert_many(lote, ordered=False)
                escritos += len(lote)
        return escritos


class AlmacenMemoria:
    """Mismo contrato que AlmacenMongo, en memoria (pruebas y cálculo del estado inicial)."""

    def __init__(self):
        self.docs = ({}, {}, {})

    def cargar(self, claves, batch_id):
        tablas = []
        for docs, ks in zip(self.docs, claves):
            tabla = {}
            for k in set(ks):
                doc = docs.get(k)
                if doc is None:
                    continue
                aplicado = doc["streaming"]["batch_id"]
                estado = doc["streaming_prev"] if aplicado == batch_id else doc["streaming"]
                if estado is not None:
                    tabla[k] = {kk: vv for kk, vv in estado.items() if kk != "batch_id"}
            tablas.append(copy.deepcopy(tabla))
        return tablas[0], tablas[1], tablas[2]

    def guardar(self, motor, iniciales, batch_id, limite=None):
        escritos = 0
        for docs, tabla, inicial, tocadas in zip(self.docs, (motor.cuentas, motor.pares, motor.bancos), iniciales,
                                                  motor.tocadas):
            for k in tocadas:
                if limite is not None and escritos >= limite:
                    raise ConnectionError("falla simulada a mitad de la escritura")
                nuevo = dict(tabla[k])
                nuevo["batch_id"] = batch_id
                docs[k] = {"streaming": copy.deepcopy(nuevo), "streaming_prev": copy.deepcopy(inicial.get(k))}
                escritos += 1
        return escritos
