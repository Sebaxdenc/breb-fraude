"""Piezas comunes de los Jobs 1, 2 y 3: rutas, lectura de Kafka, validación del contrato (RF2) y métricas
de StreamingQueryProgress (RNF-02, ADR 02).

La validación es la misma función del contrato (`breb.contratos.errores_evento`), aplicada con una pandas
UDF, para que el Job 1 y el Job 2 decidan igual qué evento es válido.
"""
from __future__ import annotations

import json
import os
import threading
import time
from datetime import datetime

import pandas as pd
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.functions import pandas_udf
from pyspark.sql.streaming import StreamingQueryListener

from breb.buckets import ruta

TOPICO = os.environ.get("BREB_TOPICO", "breb.transacciones.raw")
KAFKA = os.environ.get("KAFKA_BOOTSTRAP", "kafka-1:9092")
BRONZE = os.environ.get("BREB_BRONZE", ruta("bronze", "transacciones"))
SILVER = os.environ.get("BREB_SILVER", ruta("silver", "transacciones"))
RISK_DECISIONS = os.environ.get("BREB_RISK_DECISIONS", ruta("gold", "risk_decisions"))
METRICAS = os.environ.get("BREB_METRICAS", ruta("gold", "metricas/progreso_streaming"))
CHECKPOINTS = os.environ.get("BREB_CHECKPOINTS", "file:///checkpoints")

ESQUEMA_EVENTO = T.StructType([
    T.StructField("schema_version", T.StringType()),
    T.StructField("transaction_uuid", T.StringType()),
    T.StructField("seq", T.LongType()),
    T.StructField("event_ts", T.StringType()),
    T.StructField("produced_at", T.StringType()),
    T.StructField("from_bank", T.LongType()),
    T.StructField("source_account", T.StringType()),
    T.StructField("to_bank", T.LongType()),
    T.StructField("destination_account", T.StringType()),
    T.StructField("amount_paid", T.DoubleType()),
    T.StructField("payment_currency", T.StringType()),
    T.StructField("amount_received", T.DoubleType()),
    T.StructField("receiving_currency", T.StringType()),
    T.StructField("payment_format", T.StringType()),
])


def comprobar_servicios(**destinos: str | None) -> None:
    """Falla en segundos si un servicio no responde, en lugar de dejar que S3A o Kafka reintenten durante
    minutos. `destinos`: nombre=URL o host:puerto (se toma el primero si hay varios separados por coma)."""
    import socket
    from urllib.parse import urlparse
    caidos = []
    for nombre, destino in destinos.items():
        if not destino:
            continue
        d = destino.split(",")[0].strip()
        u = urlparse(d if "://" in d else f"tcp://{d}")
        try:
            socket.create_connection((u.hostname, u.port or (443 if u.scheme == "https" else 80)), timeout=5).close()
        except OSError as e:
            caidos.append(f"{nombre} ({u.hostname}:{u.port}): {e}")
    if caidos:
        raise SystemExit("No responden: " + "; ".join(caidos) + ".\nLevanten la infraestructura con "
                         "'docker compose up -d' (dentro de infra) y vuelvan a intentar.")


def sesion(nombre: str) -> SparkSession:
    spark = SparkSession.builder.appName(nombre).getOrCreate()
    spark.sparkContext.setLogLevel(os.environ.get("BREB_LOG", "WARN"))
    return spark


def leer_kafka(spark: SparkSession, max_offsets: int | None = None) -> DataFrame:
    lector = (spark.readStream.format("kafka")
              .option("kafka.bootstrap.servers", KAFKA)
              .option("subscribe", TOPICO)
              .option("startingOffsets", "earliest"))
    if max_offsets:
        lector = lector.option("maxOffsetsPerTrigger", max_offsets)
    return lector.load()


def _errores_de_payload(texto: str) -> list[str]:
    from breb.contratos import errores_evento
    try:
        evento = json.loads(texto)
    except (TypeError, ValueError) as e:
        return [f"(raíz): JSON inválido: {e}"]
    if not isinstance(evento, dict):
        return ["(raíz): el evento no es un objeto JSON"]
    return errores_evento(evento)


@pandas_udf(T.ArrayType(T.StringType()))
def errores_payload(payload: pd.Series) -> pd.Series:
    return payload.map(_errores_de_payload)


def validar(kafka: DataFrame) -> DataFrame:
    """Columnas de Bronze (RF2, RF5): payload original, coordenadas de Kafka y resultado de la validación."""
    return (kafka.select(F.col("value").cast("string").alias("raw_payload"),
                         F.col("topic").alias("kafka_topic"),
                         F.col("partition").alias("kafka_partition"),
                         F.col("offset").alias("kafka_offset"),
                         F.col("timestamp").alias("kafka_timestamp"))
            .withColumn("validation_errors", errores_payload("raw_payload"))
            .withColumn("is_valid", F.size("validation_errors") == 0))


def con_evento(df: DataFrame) -> DataFrame:
    """Agrega los campos del evento (contrato 1.0) y event_time, la marca de tiempo del evento en UTC."""
    return (df.withColumn("_ev", F.from_json("raw_payload", ESQUEMA_EVENTO))
            .select("*", "_ev.*").drop("_ev")
            .withColumn("event_time", F.to_timestamp("event_ts", "yyyy-MM-dd'T'HH:mm:ss")))


def esperar_tabla(spark: SparkSession, ruta: str, minutos: float = 10.0) -> None:
    # No se usa DeltaTable.isDeltaTable: guarda en caché el "no existe" y no vuelve a mirar el log.
    from pyspark.errors import AnalysisException
    limite = time.monotonic() + minutos * 60
    while True:
        try:
            spark.read.format("delta").load(ruta).limit(0).collect()
            return
        except AnalysisException:
            if time.monotonic() > limite:
                raise TimeoutError(f"{ruta} no existe después de {minutos:g} min")
            print(f"esperando a que exista {ruta}", flush=True)
            time.sleep(5)


ESQUEMA_METRICAS = T.StructType([
    T.StructField("job", T.StringType()), T.StructField("consulta", T.StringType()),
    T.StructField("batch_id", T.LongType()), T.StructField("ts", T.TimestampType()),
    T.StructField("filas", T.LongType()), T.StructField("filas_por_s_entrada", T.DoubleType()),
    T.StructField("filas_por_s_procesadas", T.DoubleType()), T.StructField("duracion_ms", T.LongType()),
    T.StructField("add_batch_ms", T.LongType()), T.StructField("max_offsets_atras", T.LongType()),
    T.StructField("estado_filas", T.LongType()), T.StructField("estado_bytes", T.LongType()),
])


class Metricas(StreamingQueryListener):
    """Guarda el progreso de cada micro-batch (StreamingQueryProgress) y lo escribe cada 30 s en la tabla
    Delta de métricas. Spark no confirma offsets en Kafka, así que el atraso se lee de aquí
    (maxOffsetsBehindLatest de la fuente de Kafka)."""

    def __init__(self, spark: SparkSession, job: str, cada_s: float = 30.0):
        super().__init__()
        # `spark` es una propiedad de solo lectura de StreamingQueryListener en PySpark 3.5
        self._sesion, self.job, self.cada_s = spark, job, cada_s
        self.cola: list[dict] = []
        self.candado = threading.Lock()
        threading.Thread(target=self._escribir_periodicamente, daemon=True, name=f"metricas-{job}").start()

    def onQueryStarted(self, event):
        print(f"[{self.job}] consulta {event.name} iniciada", flush=True)

    def onQueryProgress(self, event):
        p = event.progress
        if not p.numInputRows:
            return
        fuente = p.sources[0] if p.sources else None
        m = getattr(fuente, "metrics", None) or {}
        atras = m.get("maxOffsetsBehindLatest")
        estado = p.stateOperators[0] if p.stateOperators else None
        fila = {"job": self.job, "consulta": p.name, "batch_id": int(p.batchId),
                "ts": datetime.fromisoformat(p.timestamp.replace("Z", "+00:00")).replace(tzinfo=None),
                "filas": int(p.numInputRows),
                "filas_por_s_entrada": float(p.inputRowsPerSecond or 0.0),
                "filas_por_s_procesadas": float(p.processedRowsPerSecond or 0.0),
                "duracion_ms": int(p.durationMs.get("triggerExecution", 0)),
                "add_batch_ms": int(p.durationMs.get("addBatch", 0)),
                "max_offsets_atras": int(float(atras)) if atras not in (None, "") else None,
                "estado_filas": int(estado.numRowsTotal) if estado else None,
                "estado_bytes": int(estado.memoryUsedBytes) if estado else None}
        with self.candado:
            self.cola.append(fila)
        print(f"[{self.job}] {p.name} batch {p.batchId}: {p.numInputRows} filas en {fila['duracion_ms']} ms"
              + (f", atraso {fila['max_offsets_atras']} offsets" if fila["max_offsets_atras"] is not None else ""),
              flush=True)

    def onQueryIdle(self, event):
        pass

    def onQueryTerminated(self, event):
        print(f"[{self.job}] consulta terminada: {event.exception or 'sin error'}", flush=True)

    def _escribir_periodicamente(self):
        while True:
            time.sleep(self.cada_s)
            with self.candado:
                filas, self.cola = self.cola, []
            if filas:
                try:
                    (self._sesion.createDataFrame(filas, ESQUEMA_METRICAS)
                     .write.format("delta").mode("append").save(METRICAS))
                except Exception as e:  # noqa: BLE001 - las métricas no deben tumbar el job
                    # Típico al arrancar: los tres jobs crean la tabla a la vez (ProtocolChangedException).
                    # Las filas vuelven a la cola y se reintentan en la próxima vuelta.
                    with self.candado:
                        self.cola = filas + self.cola
                    print(f"[{self.job}] métricas sin escribir, se reintenta en {self.cada_s:g} s: "
                          f"{str(e).splitlines()[0][:160]}", flush=True)


def registrar_metricas(spark: SparkSession, job: str) -> Metricas:
    m = Metricas(spark, job)
    spark.streams.addListener(m)
    return m
