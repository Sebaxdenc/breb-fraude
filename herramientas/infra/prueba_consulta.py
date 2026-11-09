"""Prueba de la capa de consulta (RF11, corrección 7 del Entregable 1): Spark escribe tablas Delta en los
tres buckets, se registran en el Hive Metastore a través de Trino y se consultan con SQL.

Comprueba:
1. Spark escribe una tabla Delta en lakehouse-bronze, lakehouse-silver y lakehouse-gold,
2. Trino crea un esquema (el Hive Metastore crea su carpeta en S3),
3. registra las tres tablas con lakehouse.system.register_table,
4. las consulta y los conteos coinciden,
5. después de un append de Spark, Trino ve las filas nuevas sin volver a registrar la tabla, y
6. Metabase responde (/api/health).
Al final borra lo que creó.

Uso: docker compose --profile consumo run --rm spark-cliente spark-submit /opt/breb/herramientas/infra/prueba_consulta.py
Termina con "PRUEBA CONSULTA OK".
"""
import argparse
import json
import os
import time
import urllib.error
import urllib.request
import uuid

import boto3
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

USUARIO = "breb_admin"   # infra/trino/etc/reglas.json: el único que registra tablas


def trino(sql: str) -> list:
    """Ejecuta una sentencia con la API REST de Trino (/v1/statement) y devuelve todas las filas."""
    base = os.environ.get("TRINO_URL", "http://trino:8080")
    cab = {"X-Trino-User": USUARIO, "X-Trino-Source": "prueba_consulta"}
    peticion = urllib.request.Request(f"{base}/v1/statement", data=sql.encode(), headers=cab, method="POST")
    r = json.load(urllib.request.urlopen(peticion, timeout=60))
    filas = []
    while True:
        if r.get("error"):
            raise RuntimeError(f"Trino: {r['error'].get('message')} | SQL: {sql}")
        filas += r.get("data", [])
        if "nextUri" not in r:
            return filas
        r = json.load(urllib.request.urlopen(urllib.request.Request(r["nextUri"], headers=cab), timeout=60))


def esperar_trino(intentos: int = 60) -> None:
    for i in range(intentos):
        try:
            if trino("SELECT 1") == [[1]]:
                return
        except (urllib.error.URLError, RuntimeError, ConnectionError) as e:
            if i == intentos - 1:
                raise
            print(f"esperando a Trino: {e}", flush=True)
        time.sleep(3)


def principal(args) -> None:
    marca = uuid.uuid4().hex[:8]
    esquema = f"pruebas_{marca}"
    tablas = {"bronze": 30, "silver": 20, "gold": 10}
    rutas = {capa: f"s3a://lakehouse-{capa}/_pruebas/consulta_{marca}" for capa in tablas}
    ruta_esquema = f"s3a://lakehouse-gold/_pruebas/esquema_{marca}"
    s3 = boto3.client("s3", endpoint_url=os.environ["S3_ENDPOINT"])
    t0 = time.perf_counter()

    def ok(paso: str) -> None:
        print(f"[ok] {paso} ({time.perf_counter() - t0:.1f} s)", flush=True)

    # Pocos reintentos de S3A: si SeaweedFS falla, la prueba debe fallar en segundos, no en minutos.
    spark = (SparkSession.builder.appName("prueba-consulta").master(args.master)
             .config("spark.sql.shuffle.partitions", "2")
             .config("spark.hadoop.fs.s3a.retry.limit", "2")
             .config("spark.hadoop.fs.s3a.attempts.maximum", "2").getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    try:
        # 1. Una tabla Delta por capa
        for capa, n in tablas.items():
            (spark.range(n).withColumn("capa", F.lit(capa)).withColumn("monto", F.col("id") * 1.5)
             .write.format("delta").save(rutas[capa]))
        ok("1. Spark escribió una tabla Delta en cada bucket: " + ", ".join(f"{c} {n} filas" for c, n in tablas.items()))

        # 2. Esquema en el metastore, con carpeta en S3
        esperar_trino()
        trino(f"CREATE SCHEMA lakehouse.{esquema} WITH (location = '{ruta_esquema}')")
        assert [esquema] in trino("SHOW SCHEMAS FROM lakehouse"), "el esquema no aparece en el metastore"
        ok(f"2. esquema lakehouse.{esquema} creado en el Hive Metastore")

        # 3. Registro de las tres tablas
        for capa in tablas:
            trino(f"CALL lakehouse.system.register_table(schema_name => '{esquema}', table_name => '{capa}', "
                  f"table_location => '{rutas[capa]}')")
        registradas = sorted(f[0] for f in trino(f"SHOW TABLES FROM lakehouse.{esquema}"))
        assert registradas == sorted(tablas), registradas
        ok(f"3. tablas registradas: {', '.join(registradas)}")

        # 4. Consultas: conteo por tabla y una unión de las tres capas
        for capa, n in tablas.items():
            assert trino(f"SELECT count(*) FROM lakehouse.{esquema}.{capa}") == [[n]], capa
        union = trino(" UNION ALL ".join(f"SELECT capa, count(*), sum(monto) FROM lakehouse.{esquema}.{c} GROUP BY capa"
                                         for c in tablas) + " ORDER BY 1")
        ok(f"4. consultas por SQL: {union}")

        # 5. Append desde Spark: Trino lee el log de Delta en cada consulta
        spark.range(100, 105).withColumn("capa", F.lit("bronze")).withColumn("monto", F.col("id") * 1.5) \
            .write.format("delta").mode("append").save(rutas["bronze"])
        assert trino(f"SELECT count(*) FROM lakehouse.{esquema}.bronze") == [[tablas["bronze"] + 5]]
        ok(f"5. después de un append de Spark, Trino ve {tablas['bronze'] + 5} filas en bronze sin volver a registrarla")

        # 6. Metabase
        if args.sin_metabase:
            print("[--] 6. Metabase omitido (--sin-metabase)")
        else:
            url = os.environ.get("METABASE_URL", "http://metabase:3000") + "/api/health"
            for _ in range(40):
                try:
                    if json.load(urllib.request.urlopen(url, timeout=10)).get("status") == "ok":
                        break
                except (urllib.error.URLError, ConnectionError, ValueError):
                    pass
                time.sleep(3)
            else:
                raise AssertionError("Metabase no respondió ok en /api/health en 2 minutos")
            ok("6. Metabase responde ok")
    finally:
        if not args.conservar:
            for capa in tablas:
                try:
                    trino(f"CALL lakehouse.system.unregister_table(schema_name => '{esquema}', table_name => '{capa}')")
                except Exception:  # noqa: BLE001 - limpieza de lo que se alcanzó a crear
                    pass
            try:
                trino(f"DROP SCHEMA IF EXISTS lakehouse.{esquema}")
            except Exception:  # noqa: BLE001
                pass
            for ruta in list(rutas.values()) + [ruta_esquema]:
                bucket, prefijo = ruta[len("s3a://"):].split("/", 1)
                for pagina in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefijo + "/"):
                    objetos = [{"Key": o["Key"]} for o in pagina.get("Contents", [])]
                    if objetos:
                        s3.delete_objects(Bucket=bucket, Delete={"Objects": objetos})
        spark.stop()
    print(f"\nPRUEBA CONSULTA OK en {time.perf_counter() - t0:.0f} s")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    # Spark en modo local: esta prueba es de Trino y el metastore; el clúster ya lo prueba prueba_delta_s3.py.
    ap.add_argument("--master", default="local[2]")
    ap.add_argument("--conservar", action="store_true", help="no borrar las tablas ni el esquema al final")
    ap.add_argument("--sin-metabase", action="store_true", help="no comprobar Metabase")
    principal(ap.parse_args())
