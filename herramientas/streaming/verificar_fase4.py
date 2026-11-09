"""Verificación de la fase 4: compara Kafka, Bronze, Silver, risk_decisions y MongoDB, y mide la paridad en
línea del Job 2 (decisión D2) y la latencia del RNF-02.

Uso, cuando el productor terminó y los jobs alcanzaron a procesar (unos 60 s después):
    docker compose run --rm spark-cliente spark-submit /opt/breb/herramientas/streaming/verificar_fase4.py
Termina con "FASE 4 OK" o con lo que no cuadra.

Paridad en línea: las variables y el score que guardó el Job 2 en risk_decisions se comparan con los que
calculó el notebook de entrenamiento (`golden_sample.parquet`: las primeras 20.000 transacciones del test,
por seq). Si hay diferencias, la causa esperada es el orden entre particiones (D2).
"""
import argparse
import json
import os
import sys
import time

import numpy as np
import pandas as pd
from pymongo import MongoClient
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from breb.streaming.comun import BRONZE, KAFKA, METRICAS, RISK_DECISIONS, SILVER, TOPICO

ap = argparse.ArgumentParser()
ap.add_argument("--referencia", default="/opt/breb/modelo/aml_xgb/golden_sample.parquet",
                help="parquet con seq, las 58 variables y score (golden_sample o la referencia sintética)")
ap.add_argument("--meta", default="/opt/breb/modelo/aml_xgb/model_meta.json")
ap.add_argument("--esperar", type=int, default=120, help="segundos máximos esperando a que los jobs se pongan al día")
a = ap.parse_args()

spark = (SparkSession.builder.appName("verificar-fase4").master("local[2]")
         .config("spark.ui.enabled", "false").getOrCreate())
spark.sparkContext.setLogLevel("ERROR")
fallas = []


def chequear(condicion: bool, texto: str) -> None:
    print(("[ok] " if condicion else "[FALLA] ") + texto, flush=True)
    if not condicion:
        fallas.append(texto)


# Kafka: total de mensajes en el tópico
kafka = (spark.read.format("kafka").option("kafka.bootstrap.servers", KAFKA).option("subscribe", TOPICO)
         .option("startingOffsets", "earliest").option("endingOffsets", "latest").load())
n_kafka = kafka.count()
print(f"Kafka: {n_kafka:,} mensajes en {TOPICO}", flush=True)

# Esperar a que los jobs se pongan al día (triggers de 30, 10 y 1 s)
limite = time.monotonic() + a.esperar


def contar(ruta: str) -> int:
    try:
        return spark.read.format("delta").load(ruta).count()
    except Exception:  # noqa: BLE001 - la tabla aún no existe
        return 0


def validos_bronze() -> int:
    try:
        return spark.read.format("delta").load(BRONZE).where("is_valid").select("transaction_uuid").distinct().count()
    except Exception:  # noqa: BLE001
        return -1


while True:
    conteos = tuple(contar(r) for r in (BRONZE, SILVER, RISK_DECISIONS))
    # Listo cuando Bronze tiene todo Kafka y Silver y risk_decisions tienen todas las válidas de Bronze
    if conteos[0] >= n_kafka and min(conteos[1], conteos[2]) >= validos_bronze() >= 0:
        break
    if time.monotonic() > limite:
        print(f"  dejé de esperar después de {a.esperar} s: el Job 2 o el Job 3 siguen atrasados", flush=True)
        break
    print(f"  esperando: Bronze {conteos[0]:,}, Silver {conteos[1]:,}, risk_decisions {conteos[2]:,}", flush=True)
    time.sleep(15)

bronze = spark.read.format("delta").load(BRONZE)
n_bronze = bronze.count()
n_invalidos = bronze.where("NOT is_valid").count()
n_validos_uuid = bronze.where("is_valid").select("transaction_uuid").distinct().count()
chequear(n_bronze == n_kafka, f"Bronze tiene {n_bronze:,} de {n_kafka:,} mensajes de Kafka (RF5: el 100 %)")
chequear(bronze.select("kafka_partition", "kafka_offset").distinct().count() == n_bronze,
         "Bronze no repite coordenadas de Kafka (partición, offset)")
print(f"     válidos: {n_bronze - n_invalidos:,} | en cuarentena: {n_invalidos:,}")
if n_invalidos:
    ej = bronze.where("NOT is_valid").select("validation_errors").first()[0]
    print(f"     ejemplo de validation_errors: {ej}")

silver = spark.read.format("delta").load(SILVER)
n_silver = silver.count()
chequear(n_silver == n_validos_uuid and silver.select("transaction_uuid").distinct().count() == n_silver,
         f"Silver tiene {n_silver:,} transacciones, sin duplicados, de {n_validos_uuid:,} válidas distintas (RF6)")

risk = spark.read.format("delta").load(RISK_DECISIONS)
n_risk_filas = risk.count()
n_risk = risk.select("transaction_uuid").distinct().count()
chequear(n_risk == n_validos_uuid and n_risk_filas == n_risk,
         f"risk_decisions tiene {n_risk:,} transacciones en {n_risk_filas:,} filas (RF13, una por transacción)")

# Paridad en línea contra la referencia del notebook
meta = json.load(open(a.meta))
num = list(meta["num_features"])
ref = pd.read_parquet(a.referencia)
# La referencia no trae seq: son las primeras filas del test en orden. Se alinea con el primer seq de Silver
# y se comprueba que cuenta ordenante, destino y minuto coincidan fila por fila.
primer = silver.agg(F.min("seq")).first()[0]
ref["seq"] = np.arange(primer, primer + len(ref), dtype=np.int64)
claves = (silver.where(F.col("seq") < primer + len(ref))
          .select("seq", "source_account", "destination_account", "event_time").toPandas())
alineadas = ref[["seq", "from_node_key", "to_node_key", "timestamp"]].merge(claves, on="seq")
coinciden = int(((alineadas["from_node_key"] == alineadas["source_account"])
                 & (alineadas["to_node_key"] == alineadas["destination_account"])
                 & (pd.to_datetime(alineadas["timestamp"]) == pd.to_datetime(alineadas["event_time"]))).sum())
chequear(coinciden == len(alineadas),
         f"referencia alineada por seq desde {primer:,}: {coinciden:,} de {len(alineadas):,} transacciones con las mismas cuentas y minuto")
ref = ref[["seq", "score"] + num]
dec = risk.where(F.col("seq").isin([int(s) for s in ref["seq"]]) if len(ref) <= 50000 else F.lit(True)) \
          .select("seq", "risk_score", "variables").toPandas()
juntos = ref.merge(dec, on="seq", how="inner")
print(f"paridad en línea: {len(juntos):,} de {len(ref):,} transacciones de la referencia ya están en risk_decisions")
if len(juntos):
    obtenidas = np.vstack(juntos["variables"].to_numpy()).astype(np.float32)
    esperadas = juntos[num].to_numpy(np.float32)
    distintas = ~((obtenidas == esperadas) | (np.isnan(obtenidas) & np.isnan(esperadas)))
    filas_mal = int(distintas.any(axis=1).sum())
    dif_score = np.abs(juntos["risk_score"].to_numpy() - juntos["score"].to_numpy(np.float64))
    umbral = float(meta["threshold"])
    decisiones_iguales = int(((juntos["risk_score"] >= umbral) == (juntos["score"] >= umbral)).sum())
    chequear(filas_mal == 0, f"variables idénticas en {len(juntos) - filas_mal:,} de {len(juntos):,} transacciones")
    if filas_mal:
        por_variable = pd.Series(distintas.sum(axis=0), index=num)
        print("     variables con diferencias (filas):", por_variable[por_variable > 0].sort_values(ascending=False).head(8).to_dict())
    print(f"     diferencia máxima de score: {dif_score.max():.2e} | decisiones iguales: {decisiones_iguales:,} de {len(juntos):,}")
    chequear(decisiones_iguales == len(juntos), "mismas decisiones que el notebook (score ≥ umbral)")

# Latencia del RNF-02
lat = risk.select(F.percentile_approx("latencia_ms", [0.5, 0.95, 0.99], 10000).alias("p"),
                  F.max("latencia_ms").alias("max")).first()
p50, p95, p99 = lat["p"]
chequear(p99 < 3000, f"latencia Kafka → decisión: p50 {p50:.0f} ms, p95 {p95:.0f} ms, p99 {p99:.0f} ms, máx. {lat['max']:.0f} ms (RNF-02: p99 < 3 s)")
por_minuto = (risk.groupBy(F.date_trunc("minute", "kafka_timestamp").alias("minuto"))
              .agg(F.count(F.lit(1)).alias("transacciones"),
                   F.expr("percentile_approx(latencia_ms, 0.5)").alias("p50_ms"),
                   F.expr("percentile_approx(latencia_ms, 0.99)").alias("p99_ms"))
              .orderBy("minuto").toPandas())
por_minuto["ev_por_s"] = (por_minuto["transacciones"] / 60).round(1)
print("latencia por minuto de publicación (sirve para ver cada escalón del productor):\n"
      + por_minuto[["minuto", "transacciones", "ev_por_s", "p50_ms", "p99_ms"]].to_string(index=False))
alertas = risk.where("decision = 'alerta'").count()
print(f"alertas publicadas en SNS: {alertas:,} ({alertas / max(n_risk, 1):.2%} de las transacciones)")

# MongoDB: estado y ventanas del RF3
db = MongoClient(f"mongodb://{os.environ['MONGO_MIEMBROS']}/?replicaSet=rs0&authSource=admin",
                 username=os.environ["MONGO_APP_USUARIO"], password=os.environ["MONGO_APP_CLAVE"])["breb"]
con_batch = db.cuentas.count_documents({"streaming.batch_id": {"$gte": 0}})
con_ventana = db.cuentas.count_documents({"ventanas.envio_5m": {"$exists": True}})
con_ventana_r = db.cuentas.count_documents({"ventanas.recepcion_10m": {"$exists": True}})
chequear(con_batch > 0, f"MongoDB: {con_batch:,} cuentas actualizadas por el Job 2")
chequear(con_ventana > 0 and con_ventana_r > 0,
         f"MongoDB: ventanas del RF3 en {con_ventana:,} cuentas ordenantes y {con_ventana_r:,} receptoras")
ej = db.cuentas.find_one({"ventanas.envio_5m": {"$exists": True}}, {"ventanas": 1})
if ej:
    print(f"     ejemplo: {ej['_id']} → {ej['ventanas']}")

# Métricas de StreamingQueryProgress
try:
    m = spark.read.format("delta").load(METRICAS)
    resumen = (m.groupBy("job", "consulta").agg(F.count(F.lit(1)).alias("batches"), F.sum("filas").alias("filas"),
                                                F.expr("percentile_approx(duracion_ms, 0.99)").alias("p99_ms"),
                                                F.max("max_offsets_atras").alias("max_atras"))
               .orderBy("job", "consulta").toPandas())
    print("métricas de los micro-batches:\n" + resumen.to_string(index=False))
    chequear(len(resumen) >= 3, "tabla de métricas con los tres jobs")
except Exception as e:  # noqa: BLE001
    chequear(False, f"tabla de métricas: {e}")

print("\nFASE 4 OK" if not fallas else f"\nHAY FALLAS: {len(fallas)}")
spark.stop()
sys.exit(0 if not fallas else 1)
