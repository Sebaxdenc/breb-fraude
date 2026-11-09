"""Job 3 · limpieza y Silver (RF6).

Bronze (is_valid = true) → Silver tipado, en UTC y sin duplicados de transaction_uuid. Cada micro-batch se
aplica con un MERGE que inserta solo las transacciones que no existen, así que un reintento o un duplicado
tardío no cambian Silver.

Sin dropDuplicatesWithinWatermark: la reproducción comprime el tiempo. Las 1.038.880 transacciones del test
cubren 44,8 horas de eventos; a 1.000 ev/s se publican en 17 minutos, unas 155 veces más rápido. Un watermark
de 10 min de tiempo de evento equivaldría a unos 4 s reales y descartaría como tardíos los eventos de una
partición demorada (p. ej. al caer un bróker). El MERGE deduplica sin descartar nada. Trigger de 30 s.
"""
from __future__ import annotations

import os

from delta.tables import DeltaTable
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from breb.streaming.comun import BRONZE, CHECKPOINTS, SILVER, con_evento, esperar_tabla, registrar_metricas, sesion

COLUMNAS_SILVER = ["transaction_uuid", "seq", "event_time", "produced_at", "from_bank", "source_account", "to_bank",
                   "destination_account", "amount_paid", "payment_currency", "amount_received",
                   "receiving_currency", "payment_format", "kafka_partition", "kafka_offset", "kafka_timestamp",
                   "ingestion_timestamp", "silver_timestamp", "year", "month", "day"]


def a_silver(bronze: DataFrame) -> DataFrame:
    # Bronze ya trae transaction_uuid (leído del JSON); el del evento parseado es el mismo.
    return (con_evento(bronze.drop("transaction_uuid"))
            .withColumn("produced_at", F.to_timestamp("produced_at"))
            .withColumn("silver_timestamp", F.current_timestamp())
            .drop("year", "month", "day")
            .withColumn("year", F.year("event_time"))
            .withColumn("month", F.month("event_time"))
            .withColumn("day", F.dayofmonth("event_time"))
            .select(*COLUMNAS_SILVER))


def main() -> None:
    spark = sesion("job3-silver")
    registrar_metricas(spark, "job3")
    esperar_tabla(spark, BRONZE)
    vacia = a_silver(spark.read.format("delta").load(BRONZE).limit(0))
    vacia.write.format("delta").mode("ignore").partitionBy("year", "month", "day").save(SILVER)

    def aplicar(lote: DataFrame, batch_id: int) -> None:
        nuevos = a_silver(lote).dropDuplicates(["transaction_uuid"]).persist()
        dias = nuevos.select("year", "month", "day").distinct().collect()
        if dias:
            # Poda por partición: solo los días del lote (Delta solo poda con literales en la condición).
            filtro = " OR ".join(f"(t.year = {d.year} AND t.month = {d.month} AND t.day = {d.day})" for d in dias)
            (DeltaTable.forPath(spark, SILVER).alias("t")
             .merge(nuevos.alias("s"), f"t.transaction_uuid = s.transaction_uuid AND ({filtro})")
             .whenNotMatchedInsertAll().execute())
        nuevos.unpersist()

    (spark.readStream.format("delta").load(BRONZE).where("is_valid")
     .writeStream.queryName("job3_silver").foreachBatch(aplicar)
     .option("checkpointLocation", f"{CHECKPOINTS}/job3_silver")
     .trigger(processingTime=os.environ.get("JOB3_TRIGGER", "30 seconds"))
     .start())
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
