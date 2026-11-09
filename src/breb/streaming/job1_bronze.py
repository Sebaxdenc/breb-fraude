"""Job 1 · validación y Bronze (RF2, RF5).

Kafka → validación del contrato → Bronze append-only, con el 100 % de los eventos: los inválidos quedan en
cuarentena dentro de Bronze (is_valid = false y validation_errors), sin tópico de mensajes muertos.
Particionado por fecha de ingesta (year, month, day). Trigger de 30 s.
"""
from __future__ import annotations

import os

from pyspark.sql import functions as F
from pyspark.sql import types as T

from breb.streaming.comun import BRONZE, CHECKPOINTS, leer_kafka, registrar_metricas, sesion, validar

ESQUEMA_BRONZE = T.StructType([
    T.StructField("raw_payload", T.StringType()),
    T.StructField("kafka_topic", T.StringType()),
    T.StructField("kafka_partition", T.IntegerType()),
    T.StructField("kafka_offset", T.LongType()),
    T.StructField("kafka_timestamp", T.TimestampType()),
    T.StructField("validation_errors", T.ArrayType(T.StringType())),
    T.StructField("is_valid", T.BooleanType()),
    T.StructField("transaction_uuid", T.StringType()),
    T.StructField("ingestion_timestamp", T.TimestampType()),
    T.StructField("year", T.IntegerType()),
    T.StructField("month", T.IntegerType()),
    T.StructField("day", T.IntegerType()),
])


def main() -> None:
    spark = sesion("job1-bronze")
    registrar_metricas(spark, "job1")
    # La tabla se crea vacía al arrancar para que el Job 3 pueda empezar a leerla de inmediato.
    (spark.createDataFrame([], ESQUEMA_BRONZE).write.format("delta").mode("ignore")
     .partitionBy("year", "month", "day").save(BRONZE))

    bronze = (validar(leer_kafka(spark))
              .withColumn("transaction_uuid", F.get_json_object("raw_payload", "$.transaction_uuid"))
              .withColumn("ingestion_timestamp", F.current_timestamp())
              .withColumn("year", F.year("ingestion_timestamp"))
              .withColumn("month", F.month("ingestion_timestamp"))
              .withColumn("day", F.dayofmonth("ingestion_timestamp")))
    (bronze.writeStream.format("delta").outputMode("append").queryName("job1_bronze")
     .option("checkpointLocation", f"{CHECKPOINTS}/job1_bronze")
     .partitionBy("year", "month", "day")
     .trigger(processingTime=os.environ.get("JOB1_TRIGGER", "30 seconds"))
     .start(BRONZE))
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
