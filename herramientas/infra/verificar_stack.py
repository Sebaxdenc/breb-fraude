"""Verifica que el stack local quedó listo: S3, DynamoDB, MongoDB y Kafka (este último a través de Spark,
con el mismo conector que usarán los Jobs 1 y 2).

Uso: docker compose run --rm spark-cliente python /opt/breb/herramientas/infra/verificar_stack.py
(en el perfil completo, agregar --env-file completo.env antes de run). Termina con "STACK OK".
"""
import os
import sys
import time
import uuid

import boto3
from pymongo import MongoClient

from breb.buckets import CAPAS, bucket

fallas = []


def paso(nombre):
    def deco(f):
        try:
            print(f"[ok] {nombre}: {f()}", flush=True)
        except Exception as e:  # noqa: BLE001 - se reportan todas las fallas al final
            fallas.append(nombre)
            print(f"[FALLA] {nombre}: {type(e).__name__}: {e}", flush=True)
        return f
    return deco


@paso("S3")
def s3():
    s3 = boto3.client("s3", endpoint_url=os.environ["S3_ENDPOINT"])
    buckets = {b["Name"] for b in s3.list_buckets()["Buckets"]}
    esperados = [bucket(c) for c in CAPAS]
    faltan = set(esperados) - buckets
    assert not faltan, f"faltan los buckets {sorted(faltan)}"
    return f"buckets {', '.join(esperados)} en {os.environ['S3_ENDPOINT']}"


@paso("DynamoDB")
def dynamodb():
    ddb = boto3.client("dynamodb", endpoint_url=os.environ["DYNAMODB_ENDPOINT"])
    t = ddb.describe_table(TableName="delta_log")["Table"]
    return f"tabla delta_log {t['TableStatus']}"


@paso("MongoDB")
def mongo():
    miembros = os.environ["MONGO_MIEMBROS"]
    cli = MongoClient(f"mongodb://{miembros}/?replicaSet=rs0&authSource=admin",
                      username=os.environ["MONGO_APP_USUARIO"], password=os.environ["MONGO_APP_CLAVE"],
                      w="majority", readConcernLevel="majority", serverSelectionTimeoutMS=20000)
    hola = cli.admin.command("hello")
    esperados = len(miembros.split(","))
    assert len(hola["hosts"]) == esperados, f"{len(hola['hosts'])} miembros, se esperaban {esperados}"
    col = cli["breb"]["_verificacion"]
    marca = uuid.uuid4().hex
    col.insert_one({"_id": marca, "ts": time.time()})
    assert col.find_one({"_id": marca}) is not None
    col.delete_one({"_id": marca})
    return f"replica set {hola['setName']} con {esperados} miembro(s), primario {hola['primary']}, escritura w:majority"


@paso("Kafka (con Spark)")
def kafka():
    from pyspark.sql import SparkSession
    spark = (SparkSession.builder.master("local[1]").appName("verificar-stack")
             .config("spark.ui.enabled", "false").getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    boot = os.environ["KAFKA_BOOTSTRAP"]
    marca = uuid.uuid4().hex
    (spark.createDataFrame([(marca,)], "value string")
     .write.format("kafka").option("kafka.bootstrap.servers", boot).option("topic", "breb.humo").save())
    leidos = (spark.read.format("kafka").option("kafka.bootstrap.servers", boot).option("subscribe", "breb.humo")
              .option("startingOffsets", "earliest").load()
              .selectExpr("CAST(value AS STRING) AS v").filter(f"v = '{marca}'").count())
    assert leidos == 1, f"publiqué un mensaje en breb.humo y leí {leidos}"
    particiones = (spark.read.format("kafka").option("kafka.bootstrap.servers", boot)
                   .option("subscribe", "breb.transacciones.raw").option("startingOffsets", "earliest").load()
                   .select("partition").distinct().count())
    spark.stop()
    return f"publicación y lectura en breb.humo; breb.transacciones.raw existe ({particiones} particiones con datos)"


if fallas:
    print(f"\nHAY FALLAS: {', '.join(fallas)}")
    sys.exit(1)
print("\nSTACK OK")
