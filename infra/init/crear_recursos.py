"""Crea los recursos del lakehouse local: los tres buckets en SeaweedFS y la tabla `delta_log` en
DynamoDB Local (commits coordinados de Delta, RNF-05). Es idempotente: se puede correr varias veces.

Variables de entorno: S3_ENDPOINT, DYNAMODB_ENDPOINT, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY,
AWS_REGION (por defecto us-east-1).
"""
import os
import sys
import time

import boto3
from botocore.exceptions import ClientError, EndpointConnectionError

BUCKETS = ["lakehouse-bronze", "lakehouse-silver", "lakehouse-gold"]
TABLA_DELTA = "delta_log"
REGION = os.environ.get("AWS_REGION", "us-east-1")


def esperar(nombre, prueba, intentos=60):
    for i in range(intentos):
        try:
            return prueba()
        except (EndpointConnectionError, ClientError, ConnectionError) as e:
            if i == intentos - 1:
                raise
            print(f"[init] esperando {nombre}: {type(e).__name__}", flush=True)
            time.sleep(2)


def crear_buckets():
    s3 = boto3.client("s3", endpoint_url=os.environ["S3_ENDPOINT"], region_name=REGION)
    existentes = {b["Name"] for b in esperar("SeaweedFS", s3.list_buckets)["Buckets"]}
    for b in BUCKETS:
        if b in existentes:
            print(f"[init] bucket {b}: ya existe")
        else:
            s3.create_bucket(Bucket=b)
            print(f"[init] bucket {b}: creado")


def crear_tabla_delta():
    ddb = boto3.client("dynamodb", endpoint_url=os.environ["DYNAMODB_ENDPOINT"], region_name=REGION)
    tablas = esperar("DynamoDB Local", ddb.list_tables)["TableNames"]
    if TABLA_DELTA in tablas:
        print(f"[init] tabla {TABLA_DELTA}: ya existe")
        return
    # Mismo esquema que crea S3DynamoDBLogStore (clave tablePath + fileName).
    ddb.create_table(
        TableName=TABLA_DELTA,
        AttributeDefinitions=[{"AttributeName": "tablePath", "AttributeType": "S"},
                              {"AttributeName": "fileName", "AttributeType": "S"}],
        KeySchema=[{"AttributeName": "tablePath", "KeyType": "HASH"},
                   {"AttributeName": "fileName", "KeyType": "RANGE"}],
        BillingMode="PAY_PER_REQUEST",
    )
    ddb.get_waiter("table_exists").wait(TableName=TABLA_DELTA)
    # La documentación de Delta recomienda TTL sobre expireTime para limpiar entradas viejas.
    ddb.update_time_to_live(TableName=TABLA_DELTA,
                            TimeToLiveSpecification={"Enabled": True, "AttributeName": "expireTime"})
    print(f"[init] tabla {TABLA_DELTA}: creada")


def crear_cola_analistas():
    """Cola de los analistas suscrita al tópico de alertas (en local reemplaza el correo, RF15). Se crea al
    iniciar para que las alertas se acumulen aunque la bandeja todavía no esté abierta."""
    if not os.environ.get("SNS_ENDPOINT"):
        return
    from breb.entidad.servicio import asegurar_analistas, clientes
    sns, sqs = clientes()
    esperar("moto", lambda: sns.list_topics())
    asegurar_analistas(sns, sqs)
    print("[init] tópico fraud-alerts-critical y cola breb-analistas-fraude listos")


if __name__ == "__main__":
    crear_buckets()
    crear_tabla_delta()
    crear_cola_analistas()
    print("[init] recursos listos")
    sys.exit(0)
