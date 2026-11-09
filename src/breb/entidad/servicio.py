"""Servicio de retención de una entidad financiera y bandeja de los analistas (RF15, RF8).

Cada entidad tiene una cola SQS suscrita al tópico SNS fraud-alerts-critical con un filtro sobre el atributo
`entidades` (las entidades de origen y de destino de la alerta): recibe las alertas en las que participa. Al recibir una alerta:
- si es la entidad destino, retiene los fondos en la cuenta destino;
- si es la entidad origen, bloquea las transferencias siguientes de la cuenta ordenante;
y lo registra en el subdocumento `estado_retencion` de la cuenta en MongoDB (solo este servicio lo escribe).

SNS entrega al menos una vez, así que el servicio es idempotente por transaction_uuid: guarda cada alerta
procesada en la colección `alertas_recibidas` (_id = entidad:transaction_uuid) y descarta las repetidas.
El orden es: actualizar la cuenta, registrar la alerta, borrar el mensaje. Si se cae en medio, SQS vuelve a
entregar el mensaje y la actualización se repite con los mismos valores.

Los analistas tienen una cola sin filtro (`breb-analistas-fraude`), que en local reemplaza la suscripción por
correo: la bandeja imprime cada alerta.
"""
from __future__ import annotations

import json
import os
import time

import boto3
from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError
from pymongo.write_concern import WriteConcern

from breb.contratos import errores_alerta

TOPICO_SNS = os.environ.get("BREB_TOPICO_SNS", "fraud-alerts-critical")
COLA_ANALISTAS = "breb-analistas-fraude"


def clientes():
    region = os.environ.get("AWS_REGION", "us-east-1")
    sns = boto3.client("sns", endpoint_url=os.environ.get("SNS_ENDPOINT"), region_name=region)
    sqs = boto3.client("sqs", endpoint_url=os.environ.get("SQS_ENDPOINT", os.environ.get("SNS_ENDPOINT")),
                       region_name=region)
    return sns, sqs


def nombre_cola(entidad: int) -> str:
    return f"breb-entidad-{entidad}"


def asegurar_cola(sns, sqs, nombre: str, filtro: dict | None = None) -> str:
    """Crea la cola y su suscripción al tópico (idempotente). SNS admite una sola suscripción por cola."""
    arn_topico = sns.create_topic(Name=TOPICO_SNS)["TopicArn"]
    url = sqs.create_queue(QueueName=nombre)["QueueUrl"]
    arn_cola = sqs.get_queue_attributes(QueueUrl=url, AttributeNames=["QueueArn"])["Attributes"]["QueueArn"]
    sqs.set_queue_attributes(QueueUrl=url, Attributes={"Policy": json.dumps({
        "Version": "2012-10-17", "Statement": [{
            "Effect": "Allow", "Principal": {"Service": "sns.amazonaws.com"}, "Action": "sqs:SendMessage",
            "Resource": arn_cola, "Condition": {"ArnEquals": {"aws:SourceArn": arn_topico}}}]})})
    if not any(s["Endpoint"] == arn_cola for s in sns.list_subscriptions_by_topic(TopicArn=arn_topico)["Subscriptions"]):
        attrs = {"RawMessageDelivery": "true"}
        if filtro:
            attrs["FilterPolicy"] = json.dumps(filtro)
        sns.subscribe(TopicArn=arn_topico, Protocol="sqs", Endpoint=arn_cola, Attributes=attrs)
    return url


def asegurar_entidad(sns, sqs, entidad: int) -> str:
    return asegurar_cola(sns, sqs, nombre_cola(entidad), {"entidades": [str(entidad)]})


def asegurar_analistas(sns, sqs) -> str:
    return asegurar_cola(sns, sqs, COLA_ANALISTAS)


def base_mongo():
    cliente = MongoClient(f"mongodb://{os.environ['MONGO_MIEMBROS']}/?replicaSet=rs0&authSource=admin",
                          username=os.environ["MONGO_APP_USUARIO"], password=os.environ["MONGO_APP_CLAVE"],
                          serverSelectionTimeoutMS=20000)
    return cliente.get_database("breb", write_concern=WriteConcern(w="majority"))


def aplicar_alerta(db, entidad: int, alerta: dict) -> list[str]:
    """Aplica la alerta para `entidad`. Devuelve las acciones tomadas, o [] si ya se había procesado."""
    uuid = alerta["transaction_uuid"]
    if db.alertas_recibidas.find_one({"_id": f"{entidad}:{uuid}"}, {"_id": 1}):
        return []
    base = {"transaction_uuid": uuid, "score": alerta["score"], "umbral": alerta["umbral"], "entidad": entidad,
            "model_version": alerta["model_version"], "decision_ts": alerta["decision_ts"]}
    acciones = []
    if alerta["destino"]["entidad"] == entidad:
        cuenta = alerta["destino"]["cuenta"]
        db.cuentas.update_one({"_id": cuenta}, {"$set": {"estado_retencion": {**base, "estado": "fondos_retenidos"}}},
                              upsert=True)
        acciones.append(f"fondos retenidos en la cuenta destino {cuenta}")
    if alerta["origen"]["entidad"] == entidad:
        cuenta = alerta["origen"]["cuenta"]
        db.cuentas.update_one({"_id": cuenta},
                              {"$set": {"estado_retencion": {**base, "estado": "transferencias_bloqueadas"}}},
                              upsert=True)
        acciones.append(f"transferencias bloqueadas en la cuenta ordenante {cuenta}")
    try:
        db.alertas_recibidas.insert_one({"_id": f"{entidad}:{uuid}", **base, "acciones": acciones,
                                         "recibida": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())})
    except DuplicateKeyError:      # otra copia se procesó al mismo tiempo
        return []
    return acciones


def entidades_del_tramo(dir_productor: str, dir_modelo: str) -> list[int]:
    """Todas las entidades (origen o destino) de las alertas del tramo de la demo, a partir de la puntuación fuera
    de línea (scores_test.parquet), que es idéntica a la del Job 2."""
    import pandas as pd
    tramo = json.loads(open(os.path.join(dir_productor, "tramo_demo.json")).read())["tramo"]
    umbral = json.loads(open(os.path.join(dir_modelo, "model_meta.json")).read())["threshold"]
    desde, hasta = tramo["desde_seq"], tramo["desde_seq"] + tramo["transacciones"]
    s = pd.read_parquet(os.path.join(dir_productor, "scores_test.parquet"), columns=["seq", "score"])
    s = s[(s["seq"] >= desde) & (s["seq"] < hasta) & (s["score"] >= umbral)]
    ev = pd.read_parquet(os.path.join(dir_productor, "eventos_test.parquet"), columns=["seq", "from_bank", "to_bank"])
    ev = ev[ev["seq"].isin(s["seq"])]
    return sorted({int(b) for b in pd.concat([ev["from_bank"], ev["to_bank"]])})


def _recibir(sqs, url: str, espera_s: int):
    return sqs.receive_message(QueueUrl=url, MaxNumberOfMessages=10, WaitTimeSeconds=espera_s).get("Messages", [])


def atender_entidades(entidades: list[int], una_vez: bool = False, registro=print) -> dict:
    sns, sqs = clientes()
    db = base_mongo()
    colas = {e: asegurar_entidad(sns, sqs, e) for e in entidades}
    registro(f"entidades {', '.join(map(str, entidades))}: escuchando sus colas SQS")
    totales = {"procesadas": 0, "duplicadas": 0, "rechazadas": 0}
    espera = 1 if len(colas) <= 3 else 0     # con muchas colas, consulta rápida a cada una y pausa al final
    while True:
        recibidos = 0
        for entidad, url in colas.items():
            for m in _recibir(sqs, url, espera):
                recibidos += 1
                alerta = json.loads(m["Body"])
                errores = errores_alerta(alerta)
                if errores:
                    totales["rechazadas"] += 1
                    registro(f"[entidad {entidad}] mensaje que no cumple el contrato de la alerta: {errores[:2]}")
                else:
                    acciones = aplicar_alerta(db, entidad, alerta)
                    if acciones:
                        totales["procesadas"] += 1
                        registro(f"[entidad {entidad}] alerta {alerta['transaction_uuid'][:8]} score "
                                 f"{alerta['score']:.3f} (umbral {alerta['umbral']:.3f}): {'; '.join(acciones)}")
                    else:
                        totales["duplicadas"] += 1
                sqs.delete_message(QueueUrl=url, ReceiptHandle=m["ReceiptHandle"])
        if una_vez and recibidos == 0:
            return totales
        if recibidos == 0 and espera == 0:
            time.sleep(1)


def bandeja_analistas(una_vez: bool = False, registro=print) -> int:
    sns, sqs = clientes()
    url = asegurar_analistas(sns, sqs)
    registro("bandeja de los analistas de fraude: esperando alertas")
    vistas = set()
    while True:
        mensajes = _recibir(sqs, url, 5)
        for m in mensajes:
            a = json.loads(m["Body"])
            if a["transaction_uuid"] not in vistas:
                vistas.add(a["transaction_uuid"])
                c = a["contexto"]
                registro(f"ALERTA {a['transaction_uuid']} | {a['event_ts']} | {a['canal']} "
                         f"{a['monto']['valor']:,.2f} {a['monto']['moneda']} | {a['origen']['cuenta']} → "
                         f"{a['destino']['cuenta']} | score {a['score']:.3f} | previas del ordenante "
                         f"{c['transacciones_previas_del_ordenante']}, destinatarios distintos "
                         f"{c['destinatarios_distintos_del_ordenante']}, ordenantes del destino "
                         f"{c['ordenantes_distintos_del_destino']}")
            sqs.delete_message(QueueUrl=url, ReceiptHandle=m["ReceiptHandle"])
        if una_vez and not mensajes:
            return len(vistas)
