"""Prueba del almacenamiento S3 local con Delta Lake (decisión D1, riesgos R9 y R10).

Sobre s3a:// (SeaweedFS), con los commits coordinados en DynamoDB Local, comprueba:
1. escritura, 2. MERGE, 3. OPTIMIZE, 4. VACUUM,
5. dos escritores en drivers distintos escribiendo a la vez en la misma tabla sin perder commits
   (el caso del Job 3 y el DAG reprocess_quarantine sobre Silver), y
6. que esos commits pasaron por la tabla delta_log de DynamoDB.

Uso dentro del stack:
    docker compose run --rm spark-cliente spark-submit /opt/breb/herramientas/infra/prueba_delta_s3.py
Termina con "PRUEBA DELTA OK" o con el paso que falló.
"""
import argparse
import os
import socket
import subprocess
import sys
import time
import uuid

import boto3
from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

BUCKET = "lakehouse-bronze"
PREFIJO = "_pruebas"


def sesion(nombre: str, master: str) -> SparkSession:
    b = (SparkSession.builder.appName(nombre).master(master)
         .config("spark.databricks.delta.retentionDurationCheck.enabled", "false")
         .config("spark.sql.shuffle.partitions", "4"))
    if not master.startswith("local"):
        # Los ejecutores se conectan de vuelta al driver: se anuncia la IP del contenedor.
        b = (b.config("spark.driver.host", socket.gethostbyname(socket.gethostname()))
             .config("spark.driver.bindAddress", "0.0.0.0")
             .config("spark.cores.max", "2")
             .config("spark.executor.memory", "768m"))
    spark = b.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def num_archivos(spark: SparkSession, ruta: str) -> int:
    return spark.sql(f"DESCRIBE DETAIL delta.`{ruta}`").collect()[0]["numFiles"]


def parquets_en_s3(s3, prefijo: str) -> int:
    n = 0
    for pagina in s3.get_paginator("list_objects_v2").paginate(Bucket=BUCKET, Prefix=prefijo + "/"):
        n += sum(1 for o in pagina.get("Contents", []) if o["Key"].endswith(".parquet"))
    return n


def escritor(args) -> None:
    spark = sesion(f"prueba-delta-escritor-{args.escritor}", "local[1]")
    for k in range(args.commits):
        (spark.range(10).withColumn("valor", F.lit(1000 * args.escritor + k).cast("long"))
         .write.format("delta").mode("append").save(args.ruta))
    spark.stop()


def principal(args) -> None:
    s3 = boto3.client("s3", endpoint_url=os.environ["S3_ENDPOINT"])
    ddb = boto3.client("dynamodb", endpoint_url=os.environ["DYNAMODB_ENDPOINT"])
    clave = f"{PREFIJO}/delta_{uuid.uuid4().hex[:8]}"
    ruta = f"s3a://{BUCKET}/{clave}"
    spark = sesion("prueba-delta-s3", args.master)
    print(f"tabla de prueba: {ruta} | master: {args.master}", flush=True)
    t0 = time.perf_counter()

    def ok(paso: str) -> None:
        print(f"[ok] {paso} ({time.perf_counter() - t0:.1f} s)", flush=True)

    # 1. Escritura en 8 archivos
    spark.range(0, 1000).withColumn("valor", F.col("id") * 10).repartition(8).write.format("delta").save(ruta)
    assert spark.read.format("delta").load(ruta).count() == 1000
    ok("1. escritura: 1.000 filas")

    # 2. MERGE: 100 actualizaciones y 100 inserciones
    tabla = DeltaTable.forPath(spark, ruta)
    cambios = spark.range(900, 1100).withColumn("valor", F.lit(-1).cast("long"))
    (tabla.alias("t").merge(cambios.alias("c"), "t.id = c.id")
     .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute())
    df = spark.read.format("delta").load(ruta)
    assert df.count() == 1100, df.count()
    assert df.filter("valor = -1").count() == 200
    ok("2. MERGE: 1.100 filas, 200 con el valor nuevo")

    # 3. OPTIMIZE compacta archivos chicos (6 appends de 10 filas, como los micro-batches del streaming)
    for k in range(6):
        spark.range(2000 + 10 * k, 2010 + 10 * k).withColumn("valor", F.lit(k).cast("long")).coalesce(1) \
            .write.format("delta").mode("append").save(ruta)
    antes = num_archivos(spark, ruta)
    spark.sql(f"OPTIMIZE delta.`{ruta}`")
    despues = num_archivos(spark, ruta)
    assert despues < antes, (antes, despues)
    assert spark.read.format("delta").load(ruta).count() == 1160
    ok(f"3. OPTIMIZE: {antes} archivos -> {despues}, 1.160 filas")

    # 4. VACUUM borra de S3 los archivos que ya no usa ninguna versión
    en_s3 = parquets_en_s3(s3, clave)
    spark.sql(f"VACUUM delta.`{ruta}` RETAIN 0 HOURS")
    quedan = parquets_en_s3(s3, clave)
    assert quedan == despues, (en_s3, quedan, despues)
    assert spark.read.format("delta").load(ruta).count() == 1160
    ok(f"4. VACUUM: {en_s3} parquet en S3 -> {quedan}, la tabla sigue con 1.160 filas")

    # 5. Dos drivers escribiendo a la vez en la misma tabla
    version_antes = tabla.history(1).collect()[0]["version"]
    submit = os.path.join(os.environ["SPARK_HOME"], "bin", "spark-submit")
    procesos = [subprocess.Popen([submit, "--master", "local[1]", "--driver-memory", "512m", sys.argv[0],
                                  "--escritor", str(i), "--ruta", ruta, "--commits", str(args.commits)],
                                 stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
                for i in (1, 2)]
    salidas = [p.communicate()[0] for p in procesos]
    for p, s in zip(procesos, salidas):
        if p.returncode != 0:
            print(s[-4000:])
    assert [p.returncode for p in procesos] == [0, 0], [p.returncode for p in procesos]
    spark.catalog.clearCache()
    total = spark.read.format("delta").load(ruta).count()
    esperado = 1160 + 2 * args.commits * 10
    assert total == esperado, (total, esperado)
    historia = DeltaTable.forPath(spark, ruta).history().filter(f"version > {version_antes}").collect()
    assert len(historia) == 2 * args.commits, len(historia)
    ok(f"5. dos escritores concurrentes: {2 * args.commits} commits, {total} filas, ninguno perdido")

    # 6. Los commits pasaron por DynamoDB (S3DynamoDBLogStore con ddb.endpoint)
    entradas = ddb.scan(TableName="delta_log", Select="COUNT",
                        FilterExpression="contains(tablePath, :p)",
                        ExpressionAttributeValues={":p": {"S": clave}})["Count"]
    assert entradas >= 2 * args.commits, entradas
    ok(f"6. delta_log en DynamoDB: {entradas} entradas de commit para la tabla")

    if not args.conservar:
        for pagina in s3.get_paginator("list_objects_v2").paginate(Bucket=BUCKET, Prefix=clave + "/"):
            objetos = [{"Key": o["Key"]} for o in pagina.get("Contents", [])]
            if objetos:
                s3.delete_objects(Bucket=BUCKET, Delete={"Objects": objetos})
    spark.stop()
    print(f"\nPRUEBA DELTA OK en {time.perf_counter() - t0:.0f} s")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--master", default=os.environ.get("SPARK_MASTER_URL", "local[2]"))
    ap.add_argument("--commits", type=int, default=5)
    ap.add_argument("--conservar", action="store_true", help="no borrar la tabla de prueba al final")
    ap.add_argument("--escritor", type=int)
    ap.add_argument("--ruta")
    a = ap.parse_args()
    escritor(a) if a.escritor else principal(a)
