"""Job 2 · riesgo + XGBoost (RF3, RF4, RF10, RF13, RF14, RNF-02, RNF-03).

Lee Kafka directamente (sin pasar por el Job 1) y corre tres consultas en la misma aplicación:

- `job2_decisiones` (trigger de 1 s): validación del contrato → dropDuplicatesWithinWatermark por
  transaction_uuid → foreachBatch. En el driver, `breb.job2.microbatch.procesar_microbatch` lee el estado
  de MongoDB, calcula las 58 variables en orden de seq, puntúa y escribe el estado nuevo. Después se
  publican las alertas en SNS y se agrega una fila por transacción a risk_decisions, con txnAppId/txnVersion
  para que un reintento del micro-batch no duplique filas. decision_ts se toma después de publicar en SNS:
  la latencia del RNF-02 es decision_ts − kafka_timestamp (LogAppendTime) y queda en latencia_ms.
- `job2_ventanas_envio` y `job2_ventanas_recepcion` (RF3, output mode update, trigger de 30 s): ventanas
  deslizantes de 5 min por cuenta ordenante y de 10 min por cuenta receptora, con salto de 1 min y
  watermark de 2 min. La ventana de los últimos minutos de cada cuenta se guarda en su perfil de MongoDB
  (subdocumento `ventanas`).

El watermark de la deduplicación (JOB2_WATERMARK_DEDUP, 1 hora de tiempo de evento por defecto) es mayor que
el de las ventanas porque los eventos más viejos que el watermark se descartan, y la reproducción comprime
el tiempo unas 155 veces a 1.000 ev/s: 1 hora de eventos son unos 23 s reales.
"""
from __future__ import annotations

import json
import os
import time
from datetime import datetime, timezone

import boto3
import numpy as np
import pandas as pd
from pymongo import MongoClient, UpdateOne
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

from breb.estado.almacen import AlmacenMongo
from breb.job2.microbatch import COLUMNAS_EVENTO, procesar_microbatch
from breb.modelo.puntuador import Puntuador
from breb.streaming.comun import (CHECKPOINTS, RISK_DECISIONS, con_evento, leer_kafka, registrar_metricas, sesion,
                                  validar)

DIR_MODELO = os.environ.get("BREB_DIR_MODELO", "/opt/breb/modelo/aml_xgb")
TOPICO_SNS = os.environ.get("BREB_TOPICO_SNS", "fraud-alerts-critical")

ESQUEMA_DECISIONES = T.StructType([
    T.StructField("transaction_uuid", T.StringType()), T.StructField("seq", T.LongType()),
    T.StructField("event_ts", T.TimestampType()), T.StructField("entidad", T.LongType()),
    T.StructField("entidad_destino", T.LongType()), T.StructField("canal", T.StringType()),
    T.StructField("risk_score", T.DoubleType()), T.StructField("umbral", T.DoubleType()),
    T.StructField("decision", T.StringType()), T.StructField("model_version", T.StringType()),
    T.StructField("batch_id", T.LongType()), T.StructField("variables", T.ArrayType(T.DoubleType())),
    T.StructField("kafka_partition", T.IntegerType()), T.StructField("kafka_offset", T.LongType()),
    T.StructField("kafka_timestamp", T.TimestampType()), T.StructField("decision_ts", T.TimestampType()),
    T.StructField("latencia_ms", T.DoubleType()), T.StructField("year", T.IntegerType()),
    T.StructField("month", T.IntegerType()), T.StructField("day", T.IntegerType()),
])


def cliente_mongo() -> MongoClient:
    return MongoClient(f"mongodb://{os.environ['MONGO_MIEMBROS']}/?replicaSet=rs0&authSource=admin",
                       username=os.environ["MONGO_APP_USUARIO"], password=os.environ["MONGO_APP_CLAVE"],
                       serverSelectionTimeoutMS=20000)


def publicar_alertas(sns, arn: str, alertas: list[dict]) -> None:
    """SNS entrega al menos una vez; si el micro-batch se reintenta, las alertas se repiten y los
    consumidores las descartan por transaction_uuid (contrato de la alerta)."""
    for i in range(0, len(alertas), 10):
        parte = alertas[i:i + 10]
        r = sns.publish_batch(TopicArn=arn, PublishBatchRequestEntries=[
            {"Id": str(k), "Message": json.dumps(a, ensure_ascii=False),
             "MessageAttributes": {
                 "entidad_origen": {"DataType": "Number", "StringValue": str(a["origen"]["entidad"])},
                 "entidad_destino": {"DataType": "Number", "StringValue": str(a["destino"]["entidad"])}}}
            for k, a in enumerate(parte)])
        if r.get("Failed"):
            raise RuntimeError(f"SNS rechazó {len(r['Failed'])} alertas: {r['Failed'][:2]}")


def consulta_decisiones(spark, eventos: DataFrame):
    puntuador = Puntuador(DIR_MODELO)
    almacen = AlmacenMongo(cliente_mongo()["breb"])
    sns = boto3.client("sns", endpoint_url=os.environ.get("SNS_ENDPOINT"))
    arn = sns.create_topic(Name=TOPICO_SNS)["TopicArn"]          # idempotente
    print(f"modelo {puntuador.version}, umbral {puntuador.umbral:.4f}, SNS {arn}", flush=True)

    def aplicar(lote: DataFrame, batch_id: int) -> None:
        spark.sparkContext.setLocalProperty("spark.scheduler.pool", "decisiones")
        t0 = time.perf_counter()
        pdf = lote.select(*COLUMNAS_EVENTO, "kafka_partition", "kafka_offset", "kafka_timestamp").toPandas()
        if pdf.empty:
            return
        t1 = time.perf_counter()
        r = procesar_microbatch(pdf[COLUMNAS_EVENTO], batch_id, almacen, puntuador)
        t2 = time.perf_counter()
        publicar_alertas(sns, arn, r.alertas)
        t3 = time.perf_counter()
        ahora = pd.Timestamp(datetime.now(timezone.utc).replace(tzinfo=None))
        coords = pdf.drop_duplicates("transaction_uuid").set_index("transaction_uuid")
        dec = r.decisiones.drop(columns=["decision_ts"])
        dec = dec.join(coords[["kafka_partition", "kafka_offset", "kafka_timestamp"]], on="transaction_uuid")
        dec["decision_ts"] = ahora
        dec["latencia_ms"] = (ahora - pd.to_datetime(dec["kafka_timestamp"])).dt.total_seconds() * 1000.0
        ts = pd.to_datetime(dec["event_ts"])
        dec["year"], dec["month"], dec["day"] = ts.dt.year, ts.dt.month, ts.dt.day
        dec["variables"] = [list(map(float, v)) for v in dec["variables"]]
        filas = spark.createDataFrame(dec[[c.name for c in ESQUEMA_DECISIONES]].astype(
            {"seq": np.int64, "entidad": np.int64, "entidad_destino": np.int64, "batch_id": np.int64,
             "kafka_partition": np.int32, "kafka_offset": np.int64, "year": np.int32, "month": np.int32,
             "day": np.int32}), ESQUEMA_DECISIONES)
        # Sin partitionBy: un solo archivo por micro-batch (la tabla tiene una fila por transacción y se
        # compacta con OPTIMIZE en el DAG); year, month y day quedan como columnas para filtrar.
        (filas.write.format("delta").mode("append")
         .option("txnAppId", "job2_risk_decisions").option("txnVersion", batch_id).save(RISK_DECISIONS))
        t4 = time.perf_counter()
        print(f"[job2] batch {batch_id}: {len(dec)} transacciones, {len(r.alertas)} alertas, "
              f"latencia p99 {dec['latencia_ms'].quantile(0.99):.0f} ms | lectura {1000 * (t1 - t0):.0f} ms, "
              f"variables+modelo+MongoDB {1000 * (t2 - t1):.0f} ms, SNS {1000 * (t3 - t2):.0f} ms, "
              f"risk_decisions {1000 * (t4 - t3):.0f} ms", flush=True)

    dedup = (eventos.withWatermark("event_time", os.environ.get("JOB2_WATERMARK_DEDUP", "1 hour"))
             .dropDuplicatesWithinWatermark(["transaction_uuid"]))
    spark.sparkContext.setLocalProperty("spark.scheduler.pool", "decisiones")
    return (dedup.writeStream.queryName("job2_decisiones").foreachBatch(aplicar)
            .option("checkpointLocation", f"{CHECKPOINTS}/job2_decisiones")
            .trigger(processingTime=os.environ.get("JOB2_TRIGGER", "1 second"))
            .start())


def _ventana_actual(df: DataFrame, cuenta: str, tamano_min: int) -> DataFrame:
    """De las ventanas actualizadas de cada cuenta, la de los últimos `tamano_min` minutos: la que empieza
    tamano_min − 1 minutos antes de la ventana más reciente (salto de 1 minuto)."""
    w = Window.partitionBy(cuenta)
    return (df.withColumn("_max_ini", F.max("window.start").over(w))
            .where(F.col("window.start") == F.col("_max_ini") - F.expr(f"INTERVAL {tamano_min - 1} MINUTES"))
            .drop("_max_ini"))


def _guardar_ventanas(campo: str, cuenta: str, tamano_min: int, metricas: list[str]):
    cuentas = cliente_mongo()["breb"]["cuentas"]

    def aplicar(lote: DataFrame, batch_id: int) -> None:
        lote.sparkSession.sparkContext.setLocalProperty("spark.scheduler.pool", "ventanas")
        filas = _ventana_actual(lote, cuenta, tamano_min).collect()
        ops = []
        for f in filas:
            nuevo = {"inicio": f["window"]["start"], "fin": f["window"]["end"], "batch_id": batch_id,
                     **{m: (float(f[m]) if isinstance(f[m], float) else int(f[m])) for m in metricas}}
            ruta = f"ventanas.{campo}"
            # Solo reemplaza si la ventana es igual o más reciente que la guardada.
            ops.append(UpdateOne({"_id": f[cuenta]}, [{"$set": {ruta: {"$cond": [
                {"$gte": [nuevo["fin"], {"$ifNull": [f"${ruta}.fin", datetime(1970, 1, 1)]}]}, {"$literal": nuevo},
                f"${ruta}"]}}}], upsert=True))
        for i in range(0, len(ops), 5000):
            cuentas.bulk_write(ops[i:i + 5000], ordered=False)
    return aplicar


def consultas_ventanas(eventos: DataFrame):
    eventos.sparkSession.sparkContext.setLocalProperty("spark.scheduler.pool", "ventanas")
    base = eventos.withWatermark("event_time", os.environ.get("JOB2_WATERMARK_VENTANAS", "2 minutes"))
    disparo = os.environ.get("JOB2_TRIGGER_VENTANAS", "30 seconds")
    envio = (base.groupBy(F.window("event_time", "5 minutes", "1 minute"), "source_account")
             .agg(F.count(F.lit(1)).alias("transferencias"), F.sum("amount_paid").alias("monto"),
                  F.approx_count_distinct("destination_account").alias("destinatarios_distintos")))
    recepcion = (base.groupBy(F.window("event_time", "10 minutes", "1 minute"), "destination_account")
                 .agg(F.count(F.lit(1)).alias("transferencias"), F.sum("amount_received").alias("monto_recibido"),
                      F.approx_count_distinct("source_account").alias("ordenantes_distintos")))
    q1 = (envio.writeStream.queryName("job2_ventanas_envio").outputMode("update")
          .foreachBatch(_guardar_ventanas("envio_5m", "source_account", 5,
                                          ["transferencias", "monto", "destinatarios_distintos"]))
          .option("checkpointLocation", f"{CHECKPOINTS}/job2_ventanas_envio").trigger(processingTime=disparo).start())
    q2 = (recepcion.writeStream.queryName("job2_ventanas_recepcion").outputMode("update")
          .foreachBatch(_guardar_ventanas("recepcion_10m", "destination_account", 10,
                                          ["transferencias", "monto_recibido", "ordenantes_distintos"]))
          .option("checkpointLocation", f"{CHECKPOINTS}/job2_ventanas_recepcion").trigger(processingTime=disparo).start())
    return q1, q2


def eventos_para_ventanas(spark, maximo: int) -> DataFrame:
    """Para los contadores de ventana basta un filtro barato (JSON legible, campos de la ventana presentes y
    seq válido) en lugar de la validación completa del contrato, que es la parte cara en Python y ya la
    hacen el Job 1 y la consulta de decisiones."""
    crudos = leer_kafka(spark, max_offsets=maximo).select(F.col("value").cast("string").alias("raw_payload"))
    return (con_evento(crudos)
            .where(F.col("seq").isNotNull() & (F.col("seq") >= 0) & F.col("event_time").isNotNull()
                   & F.col("source_account").isNotNull() & F.col("destination_account").isNotNull()
                   & (F.col("amount_paid") >= 0) & (F.col("amount_received") >= 0)))


def main() -> None:
    spark = sesion("job2-riesgo")
    registrar_metricas(spark, "job2")
    # Sin tope por defecto: con maxOffsetsPerTrigger, Spark reparte el tope entre particiones y un micro-batch
    # puede llegar a seq distintos en cada una; los eventos que quedan atrás se procesan después de otros con
    # seq mayor y cambian los acumulados por banco (decisión D2). En la prueba sin Docker, con tope de 5.000,
    # 226 de 9.204 transacciones tuvieron variables distintas; sin tope, 0 de 7.920.
    maximo = int(os.environ.get("JOB2_MAX_OFFSETS", "0")) or None
    eventos = con_evento(validar(leer_kafka(spark, max_offsets=maximo)).where("is_valid"))
    consulta_decisiones(spark, eventos)
    if os.environ.get("JOB2_VENTANAS", "1") == "1":
        consultas_ventanas(eventos_para_ventanas(spark, maximo))
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
