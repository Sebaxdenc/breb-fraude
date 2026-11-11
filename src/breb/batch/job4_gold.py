"""Job 4 · modelo en estrella en Gold (RF7). Batch: lo ejecuta Airflow cada 15 min (DAG breb_lakehouse_pipeline).

Silver + risk_decisions (+ etiquetas) → en s3a://lakehouse-gold/analytics/:
- dim_tiempo: un registro por minuto (tiempo_key = yyyyMMddHHmm), con día de la semana y franja del día.
- dim_canal: los 7 formatos de pago de IBM AML.
- dim_entidad_financiera: un registro por banco.
- dim_cuenta (SCD tipo 2 sobre la entidad y el estado de riesgo): una versión por cambio. Una cuenta empieza
  en nivel_riesgo "normal" y pasa a "con_alerta" desde su primera transacción con alerta; la versión anterior
  se cierra (vigente = false, valido_hasta).
- fact_transferencias: grano por transacción, con transaction_uuid como dimensión degenerada. Las claves de
  cuenta son las de la versión de dim_cuenta válida en el momento del evento.
- etiquetas: Is Laundering de cada transacción del test (para medir precisión y recall en Gold).
- ejecuciones: una fila por ejecución con lo que se cargó y el resultado de los controles de calidad.

Incremental e idempotente: solo procesa las transacciones de Silver que todavía no están en los hechos, todo
se escribe con MERGE, y una ejecución repetida no cambia nada. Si una transacción llegó a Silver antes que su
decisión a risk_decisions, el hecho se inserta sin decisión y una ejecución posterior la completa.

Etapas (tareas del DAG breb_lakehouse_pipeline): dimensiones → hechos → calidad. Las tres leen Silver en la
misma versión de Delta (`--version-silver`, time travel): si el Job 3 agrega filas mientras corren, las nuevas
quedan para la ejecución siguiente y los hechos siempre encuentran sus dimensiones. `todo` corre las tres
con la versión actual de Silver.
"""
from __future__ import annotations

import argparse
import os
import sys
import time

from delta.tables import DeltaTable
from pyspark.errors import AnalysisException
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from breb.buckets import ruta
from breb.streaming.comun import RISK_DECISIONS, SILVER, comprobar_servicios

GOLD = os.environ.get("BREB_GOLD", ruta("gold", "analytics"))
ETIQUETAS_LOCAL = os.environ.get("BREB_ETIQUETAS", "/datos/productor/etiquetas_test.parquet")
CANALES = [("ACH", "transferencia electrónica por lotes"), ("Bitcoin", "criptoactivo"), ("Cash", "efectivo"),
           ("Cheque", "cheque"), ("Credit Card", "tarjeta de crédito"), ("Reinvestment", "reinversión en la misma cuenta"),
           ("Wire", "transferencia electrónica inmediata")]
DIAS = ["domingo", "lunes", "martes", "miércoles", "jueves", "viernes", "sábado"]   # dayofweek: 1 = domingo


def ruta(tabla: str) -> str:
    return f"{GOLD}/{tabla}"


def existe(spark: SparkSession, path: str) -> bool:
    try:
        DeltaTable.forPath(spark, path)
        return True
    except AnalysisException:
        return False


def insertar_nuevos(spark: SparkSession, df: DataFrame, path: str, clave: str) -> None:
    """MERGE que solo inserta las filas cuya clave no existe (crea la tabla si hace falta)."""
    if not existe(spark, path):
        df.write.format("delta").save(path)
        return
    (DeltaTable.forPath(spark, path).alias("t").merge(df.alias("s"), f"t.{clave} = s.{clave}")
     .whenNotMatchedInsertAll().execute())


def cargar_etiquetas(spark: SparkSession) -> bool:
    if existe(spark, ruta("etiquetas")):
        return True
    if not os.path.exists(ETIQUETAS_LOCAL):
        print(f"[job4] sin etiquetas: no existe {ETIQUETAS_LOCAL} (correr python -m breb.productor preparar)")
        return False
    import pandas as pd
    pdf = pd.read_parquet(ETIQUETAS_LOCAL)
    (spark.createDataFrame(pdf).select("transaction_uuid", "seq", F.col("is_laundering").cast("int").alias("es_lavado"))
     .write.format("delta").save(ruta("etiquetas")))
    print(f"[job4] etiquetas cargadas: {len(pdf):,} transacciones, {int(pdf['is_laundering'].sum())} de lavado")
    return True


def clave_tiempo(col):
    return F.date_format(F.date_trunc("minute", col), "yyyyMMddHHmm").cast("long")


def dim_tiempo(spark: SparkSession, nuevos: DataFrame) -> None:
    m = nuevos.select(F.date_trunc("minute", "event_time").alias("minuto")).distinct()
    hora = F.hour("minuto")
    filas = m.select(
        clave_tiempo(F.col("minuto")).alias("tiempo_key"), "minuto", F.to_date("minuto").alias("fecha"),
        F.year("minuto").alias("anio"), F.month("minuto").alias("mes"), F.dayofmonth("minuto").alias("dia"),
        hora.alias("hora"), F.minute("minuto").alias("minuto_de_hora"),
        F.element_at(F.array(*[F.lit(d) for d in DIAS]), F.dayofweek("minuto")).alias("dia_semana"),
        F.dayofweek("minuto").isin(1, 7).alias("es_fin_de_semana"),
        F.when(hora < 6, "madrugada").when(hora < 12, "mañana").when(hora < 18, "tarde").otherwise("noche").alias("franja"))
    insertar_nuevos(spark, filas, ruta("dim_tiempo"), "tiempo_key")


def dim_canal(spark: SparkSession) -> None:
    if not existe(spark, ruta("dim_canal")):
        spark.createDataFrame([(i + 1, c, d) for i, (c, d) in enumerate(CANALES)],
                              "canal_key INT, canal STRING, descripcion STRING").write.format("delta").save(ruta("dim_canal"))


def dim_entidad(spark: SparkSession, nuevos: DataFrame) -> None:
    bancos = (nuevos.select(F.col("from_bank").alias("entidad_id"), "event_time")
              .unionByName(nuevos.select(F.col("to_bank").alias("entidad_id"), "event_time"))
              .groupBy("entidad_id").agg(F.min("event_time").alias("primera_transaccion"))
              .withColumn("nombre", F.concat(F.lit("Entidad "), F.col("entidad_id").cast("string"))))
    insertar_nuevos(spark, bancos.select("entidad_id", "nombre", "primera_transaccion"),
                    ruta("dim_entidad_financiera"), "entidad_id")


def dim_cuenta(spark: SparkSession, nuevos: DataFrame) -> dict:
    """SCD tipo 2. Para cada cuenta del lote: primera transacción y primera alerta del lote.
    - cuenta nueva sin alerta: versión "normal" vigente;
    - cuenta nueva con alerta: "normal" cerrada en la primera alerta (si hubo transacciones antes) y
      "con_alerta" vigente desde la primera alerta;
    - cuenta "normal" con alerta en el lote: se cierra la versión vigente y se abre "con_alerta";
    - en cualquier otro caso, no cambia."""
    mov = (nuevos.select(F.col("source_account").alias("cuenta_id"), F.col("from_bank").alias("entidad_id"),
                         "event_time", "decision")
           .unionByName(nuevos.select(F.col("destination_account").alias("cuenta_id"), F.col("to_bank").alias("entidad_id"),
                                      "event_time", "decision")))
    lote = mov.groupBy("cuenta_id", "entidad_id").agg(
        F.min("event_time").alias("primera"),
        F.min(F.when(F.col("decision") == "alerta", F.col("event_time"))).alias("primera_alerta"))

    def version(df, desde, hasta, nivel, vigente):
        return df.select("cuenta_id", "entidad_id", F.lit(nivel).alias("nivel_riesgo"), F.col(desde).alias("valido_desde"),
                         (F.col(hasta) if hasta else F.lit(None).cast("timestamp")).alias("valido_hasta"),
                         F.lit(vigente).alias("vigente"))

    if existe(spark, ruta("dim_cuenta")):
        actual = (spark.read.format("delta").load(ruta("dim_cuenta")).where("vigente")
                  .select("cuenta_id", F.col("nivel_riesgo").alias("nivel_actual")))
        lote = lote.join(actual, "cuenta_id", "left")
    else:
        lote = lote.withColumn("nivel_actual", F.lit(None).cast("string"))
    nueva = lote.where("nivel_actual IS NULL")
    sube = lote.where("nivel_actual = 'normal' AND primera_alerta IS NOT NULL")
    inserciones = (version(nueva.where("primera_alerta IS NULL"), "primera", None, "normal", True)
                   .unionByName(version(nueva.where("primera_alerta > primera"), "primera", "primera_alerta", "normal", False))
                   .unionByName(version(nueva.where("primera_alerta IS NOT NULL"), "primera_alerta", None, "con_alerta", True))
                   .unionByName(version(sube, "primera_alerta", None, "con_alerta", True)))
    inserciones = inserciones.withColumn(
        "cuenta_key", F.xxhash64("cuenta_id", F.col("valido_desde").cast("string"))).persist()
    cierres = sube.select(F.col("cuenta_id").alias("merge_key"), F.col("primera_alerta").alias("cierre"))
    columnas = ["cuenta_key", "cuenta_id", "entidad_id", "nivel_riesgo", "valido_desde", "valido_hasta", "vigente"]
    resumen = {"cuentas_nuevas": nueva.count(), "cuentas_que_pasan_a_con_alerta": sube.count()}
    if not existe(spark, ruta("dim_cuenta")):
        inserciones.select(*columnas).write.format("delta").save(ruta("dim_cuenta"))
    else:
        # Patrón de SCD 2 con un solo MERGE: las filas con merge_key cierran la versión vigente y las que no
        # tienen merge_key se insertan como versiones nuevas.
        fuente = (cierres.select("merge_key", "cierre",
                                 *[F.lit(None).cast(inserciones.schema[c].dataType).alias(c) for c in columnas])
                  .unionByName(inserciones.select(F.lit(None).cast("string").alias("merge_key"),
                                                  F.lit(None).cast("timestamp").alias("cierre"), *columnas)))
        (DeltaTable.forPath(spark, ruta("dim_cuenta")).alias("t")
         .merge(fuente.alias("s"), "t.cuenta_id = s.merge_key AND t.vigente")
         .whenMatchedUpdate(set={"vigente": "false", "valido_hasta": "s.cierre"})
         .whenNotMatchedInsert(condition="s.merge_key IS NULL", values={c: f"s.{c}" for c in columnas})
         .execute())
    inserciones.unpersist()
    return resumen


def claves_de_cuenta(spark: SparkSession, hechos: DataFrame, col_cuenta: str, alias: str) -> DataFrame:
    """Clave de la versión de dim_cuenta válida en el momento del evento (o la vigente si el evento es anterior
    a la primera versión, p. ej. una transacción tardía)."""
    dim = spark.read.format("delta").load(ruta("dim_cuenta"))
    en_fecha = (hechos.select("transaction_uuid", col_cuenta, "event_time").alias("h")
                .join(dim.alias("d"), (F.col(f"h.{col_cuenta}") == F.col("d.cuenta_id"))
                      & (F.col("h.event_time") >= F.col("d.valido_desde"))
                      & (F.col("d.valido_hasta").isNull() | (F.col("h.event_time") < F.col("d.valido_hasta"))), "left")
                .select("h.transaction_uuid", F.col("d.cuenta_key").alias("k_fecha"), F.col(f"h.{col_cuenta}").alias("_c")))
    vigentes = dim.where("vigente").select(F.col("cuenta_id").alias("_c"), F.col("cuenta_key").alias("k_vigente"))
    return (en_fecha.join(vigentes, "_c", "left")
            .select("transaction_uuid", F.coalesce("k_fecha", "k_vigente").alias(alias))
            .dropDuplicates(["transaction_uuid"]))


def hechos(spark: SparkSession, nuevos: DataFrame, con_etiquetas: bool) -> None:
    canal = spark.read.format("delta").load(ruta("dim_canal")).select(F.col("canal").alias("payment_format"), "canal_key")
    f = (nuevos.join(canal, "payment_format", "left")
         .join(claves_de_cuenta(spark, nuevos, "source_account", "cuenta_origen_key"), "transaction_uuid", "left")
         .join(claves_de_cuenta(spark, nuevos, "destination_account", "cuenta_destino_key"), "transaction_uuid", "left"))
    if con_etiquetas:
        f = f.join(spark.read.format("delta").load(ruta("etiquetas")).select("transaction_uuid", "es_lavado"),
                   "transaction_uuid", "left")
    else:
        f = f.withColumn("es_lavado", F.lit(None).cast("int"))
    f = f.select(
        "transaction_uuid", "seq", clave_tiempo(F.col("event_time")).alias("tiempo_key"), "event_time",
        "cuenta_origen_key", "cuenta_destino_key", F.col("from_bank").alias("entidad_origen_id"),
        F.col("to_bank").alias("entidad_destino_id"), "canal_key",
        F.col("amount_paid").alias("monto_pagado"), F.col("payment_currency").alias("moneda_pago"),
        F.col("amount_received").alias("monto_recibido"), F.col("receiving_currency").alias("moneda_recibida"),
        (F.col("payment_currency") != F.col("receiving_currency")).alias("es_cruce_de_moneda"),
        (F.col("from_bank") == F.col("to_bank")).alias("es_misma_entidad"),
        "risk_score", "umbral", "decision", "model_version", "latencia_ms", "es_lavado",
        "kafka_partition", "kafka_offset", F.current_timestamp().alias("gold_timestamp"))
    insertar_nuevos(spark, f, ruta("fact_transferencias"), "transaction_uuid")


def completar_decisiones(spark: SparkSession, decisiones: DataFrame) -> int:
    """Hechos insertados sin decisión porque risk_decisions iba atrasada."""
    fact = DeltaTable.forPath(spark, ruta("fact_transferencias"))
    pendientes = (fact.toDF().where("risk_score IS NULL").select("transaction_uuid")
                  .join(decisiones, "transaction_uuid"))
    n = pendientes.count()
    if n:
        cols = ["risk_score", "umbral", "decision", "model_version", "latencia_ms"]
        (fact.alias("t").merge(pendientes.alias("s"), "t.transaction_uuid = s.transaction_uuid")
         .whenMatchedUpdate(set={c: f"s.{c}" for c in cols}).execute())
    return n


def calidad(spark: SparkSession, silver: DataFrame) -> tuple[bool, dict]:
    fact = spark.read.format("delta").load(ruta("fact_transferencias"))
    dim = spark.read.format("delta").load(ruta("dim_cuenta"))
    tiempo = spark.read.format("delta").load(ruta("dim_tiempo")).select("tiempo_key")
    r = fact.agg(
        F.count(F.lit(1)).alias("hechos"),
        F.sum(F.col("tiempo_key").isNull().cast("int")).alias("sin_tiempo"),
        F.sum(F.col("canal_key").isNull().cast("int")).alias("sin_canal"),
        F.sum((F.col("cuenta_origen_key").isNull() | F.col("cuenta_destino_key").isNull()).cast("int")).alias("sin_cuenta"),
        F.sum((F.col("decision").isNull() & (F.col("seq") >= 0)).cast("int")).alias("sin_decision"),
        F.sum((F.col("decision").isNull() & (F.col("seq") < 0)).cast("int")).alias("de_cuarentena"),
        F.sum(((F.col("decision") == "alerta") & (F.col("es_lavado") == 1)).cast("int")).alias("vp"),
        F.sum(((F.col("decision") == "alerta") & (F.col("es_lavado") == 0)).cast("int")).alias("fp"),
        F.sum(((F.col("decision") == "sin_alerta") & (F.col("es_lavado") == 1)).cast("int")).alias("fn"),
    ).first().asDict()
    r = {k: int(v or 0) for k, v in r.items()}
    r["silver"] = silver.count()
    r["hechos_sin_dim_tiempo"] = fact.join(tiempo, "tiempo_key", "left_anti").count()
    r["cuentas_con_varias_vigentes"] = dim.where("vigente").groupBy("cuenta_id").count().where("count > 1").count()
    r["precision"] = round(r["vp"] / (r["vp"] + r["fp"]), 4) if r["vp"] + r["fp"] else None
    r["recall"] = round(r["vp"] / (r["vp"] + r["fn"]), 4) if r["vp"] + r["fn"] else None
    controles = {
        "todas las transacciones de Silver (en esta versión) están en los hechos": r["hechos"] == r["silver"],
        "ningún hecho sin clave de tiempo, canal o cuenta": r["sin_tiempo"] + r["sin_canal"] + r["sin_cuenta"] == 0,
        "integridad referencial con dim_tiempo": r["hechos_sin_dim_tiempo"] == 0,
        "una sola versión vigente por cuenta en dim_cuenta": r["cuentas_con_varias_vigentes"] == 0,
    }
    for nombre, ok in controles.items():
        print(f"[job4] {'[ok]' if ok else '[FALLA]'} {nombre}")
    if r["sin_decision"]:
        print(f"[job4] aviso: {r['sin_decision']:,} hechos todavía sin decisión (risk_decisions atrasada)")
    if r["de_cuarentena"]:
        print(f"[job4] {r['de_cuarentena']:,} hechos recuperados de la cuarentena, sin decisión (el Job 2 no los puntúa)")
    return all(controles.values()), r


def version_actual(spark: SparkSession) -> int:
    return int(DeltaTable.forPath(spark, SILVER).history(1).select("version").first()[0])


def leer_silver(spark: SparkSession, version: int) -> DataFrame:
    return spark.read.format("delta").option("versionAsOf", version).load(SILVER)


def leer_decisiones(spark: SparkSession) -> DataFrame:
    return (spark.read.format("delta").load(RISK_DECISIONS)
            .select("transaction_uuid", "risk_score", "umbral", "decision", "model_version", "latencia_ms"))


def pendientes(spark: SparkSession, version: int) -> DataFrame:
    """Transacciones de Silver (en `version`) que todavía no están en los hechos, con su decisión si ya existe."""
    silver = leer_silver(spark, version)
    if existe(spark, ruta("fact_transferencias")):
        ya = spark.read.format("delta").load(ruta("fact_transferencias")).select("transaction_uuid")
        silver = silver.join(ya, "transaction_uuid", "left_anti")
    return silver.join(leer_decisiones(spark), "transaction_uuid", "left")


def etapa_dimensiones(spark: SparkSession, version: int) -> dict:
    cargar_etiquetas(spark)
    dim_canal(spark)
    nuevos = pendientes(spark, version).persist()
    n = nuevos.count()
    print(f"[job4] Silver versión {version}: {n:,} transacciones nuevas para Gold")
    resumen = {"nuevas": n}
    if n:
        dim_tiempo(spark, nuevos)
        dim_entidad(spark, nuevos)
        resumen.update(dim_cuenta(spark, nuevos))
    nuevos.unpersist()
    return resumen


def etapa_hechos(spark: SparkSession, version: int) -> dict:
    nuevos = pendientes(spark, version).persist()
    n = nuevos.count()
    if n:
        hechos(spark, nuevos, existe(spark, ruta("etiquetas")))
    nuevos.unpersist()
    completadas = completar_decisiones(spark, leer_decisiones(spark)) if existe(spark, ruta("fact_transferencias")) else 0
    print(f"[job4] hechos insertados: {n:,}, decisiones completadas: {completadas:,}")
    return {"hechos_insertados": n, "decisiones_completadas": completadas}


def registrar_version_procesada(version: int, hechos_total: int) -> None:
    """Guarda en MongoDB la última versión de Silver que llegó a Gold sin fallas de calidad: el sensor
    check_new_silver_data del DAG la compara con la versión actual."""
    from breb.batch.comun import base_mongo
    db = base_mongo()
    if db is None:
        print("[job4] sin MONGO_MIEMBROS: no se registra la versión procesada")
        return
    from datetime import datetime, timezone
    db.sincronizacion.update_one({"_id": "gold"}, {"$set": {
        "version_silver": version, "hechos": hechos_total, "ts": datetime.now(timezone.utc)}}, upsert=True)


def etapa_calidad(spark: SparkSession, version: int, resumen: dict | None = None) -> bool:
    resumen = dict(resumen or {})
    ok, r = calidad(spark, leer_silver(spark, version))
    resumen.update(r)
    print("[job4] " + ", ".join(f"{k} {v:,}" if isinstance(v, int) else f"{k} {v}" for k, v in resumen.items()))
    fila = spark.createDataFrame([(str(resumen),)], "resumen STRING").select(
        F.current_timestamp().alias("ts"), F.lit(ok).alias("calidad_ok"),
        F.lit(resumen.get("nuevas")).cast("long").alias("nuevas"),
        F.lit(resumen.get("hechos")).cast("long").alias("hechos"),
        F.lit(resumen.get("precision")).cast("double").alias("precision"),
        F.lit(resumen.get("recall")).cast("double").alias("recall"), "resumen",
        F.lit(version).cast("long").alias("version_silver"))
    fila.write.format("delta").mode("append").option("mergeSchema", "true").save(ruta("ejecuciones"))
    if ok:
        registrar_version_procesada(version, resumen.get("hechos", 0))
    return ok


def sesion() -> SparkSession:
    comprobar_servicios(SeaweedFS=os.environ.get("S3_ENDPOINT"), DynamoDB=os.environ.get("DYNAMODB_ENDPOINT"))
    spark = SparkSession.builder.appName("job4-gold").getOrCreate()
    spark.sparkContext.setLogLevel(os.environ.get("BREB_LOG", "WARN"))
    spark.conf.set("spark.sql.shuffle.partitions", os.environ.get("JOB4_PARTICIONES", "4"))
    return spark


def ejecutar(spark: SparkSession, etapa: str = "todo", version: int | None = None, sin_calidad: bool = False) -> int:
    t0 = time.perf_counter()
    if version is None:
        version = version_actual(spark)
    resumen = {}
    if etapa in ("todo", "dimensiones"):
        resumen.update(etapa_dimensiones(spark, version))
    if etapa in ("todo", "hechos"):
        resumen.update(etapa_hechos(spark, version))
    ok = True
    if etapa in ("todo", "calidad") and not sin_calidad:
        ok = etapa_calidad(spark, version, resumen | {"segundos": round(time.perf_counter() - t0, 1)})
        print("JOB 4 OK" if ok else "JOB 4 CON FALLAS DE CALIDAD")
    return 0 if ok else 1


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--etapa", choices=["todo", "dimensiones", "hechos", "calidad"], default="todo")
    ap.add_argument("--version-silver", type=int, help="versión de Silver a procesar (por defecto, la actual)")
    ap.add_argument("--sin-calidad", action="store_true")
    a = ap.parse_args()
    spark = sesion()
    codigo = ejecutar(spark, a.etapa, a.version_silver, a.sin_calidad)
    spark.stop()
    return codigo


if __name__ == "__main__":
    sys.exit(main())
