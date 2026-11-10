"""sync_profiles_to_mongo: perfil histórico de cada cuenta desde Gold hacia MongoDB (RF8).

Escribe solo el subdocumento `historico` de `cuentas` (el Job 2 escribe `streaming` y el servicio de la entidad
`estado_retencion`). Es incremental: recalcula el perfil de las cuentas que aparecen en hechos agregados a Gold
desde la última sincronización (marca en `sincronizacion._id = "perfiles"`), con todos sus hechos.
"""
from __future__ import annotations

import time
from datetime import datetime, timezone

from pymongo import UpdateOne
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from breb.batch.comun import base_mongo
from breb.batch.job4_gold import ruta

LOTE = 5000


def sincronizar(spark: SparkSession) -> dict:
    t0 = time.perf_counter()
    db = base_mongo()
    if db is None:
        raise SystemExit("sync_profiles_to_mongo necesita MONGO_MIEMBROS, MONGO_APP_USUARIO y MONGO_APP_CLAVE")
    fact = spark.read.format("delta").load(ruta("fact_transferencias"))
    dim = spark.read.format("delta").load(ruta("dim_cuenta"))
    hasta = fact.agg(F.max("gold_timestamp")).first()[0]
    if hasta is None:
        print("[perfiles] Gold no tiene hechos todavía")
        return {"cuentas": 0}
    marca = db.sincronizacion.find_one({"_id": "perfiles"})
    # La marca se guarda también como texto ISO: MongoDB guarda las fechas al milisegundo y gold_timestamp tiene
    # microsegundos, así que la fecha sola haría que los últimos hechos parecieran nuevos en cada ejecución.
    desde = datetime.fromisoformat(marca["hasta_iso"]) if marca and marca.get("hasta_iso") else None
    claves = dim.select("cuenta_key", "cuenta_id")
    f = (fact.join(claves.withColumnRenamed("cuenta_key", "cuenta_origen_key").withColumnRenamed("cuenta_id", "origen"),
                   "cuenta_origen_key")
         .join(claves.withColumnRenamed("cuenta_key", "cuenta_destino_key").withColumnRenamed("cuenta_id", "destino"),
               "cuenta_destino_key")
         .withColumn("alerta", (F.col("decision") == "alerta").cast("int")))
    recientes = f if desde is None else f.where(F.col("gold_timestamp") > F.lit(desde))
    tocadas = (recientes.select(F.col("origen").alias("cuenta_id"))
               .union(recientes.select(F.col("destino").alias("cuenta_id"))).distinct())
    enviados = f.groupBy(F.col("origen").alias("cuenta_id")).agg(
        F.count(F.lit(1)).alias("transacciones_enviadas"), F.sum("alerta").alias("alertas_como_ordenante"),
        F.countDistinct("destino").alias("destinatarios_distintos"),
        F.min("event_time").alias("primer_envio"), F.max("event_time").alias("ultimo_envio"))
    recibidos = f.groupBy(F.col("destino").alias("cuenta_id")).agg(
        F.count(F.lit(1)).alias("transacciones_recibidas"), F.sum("alerta").alias("alertas_como_destino"),
        F.countDistinct("origen").alias("ordenantes_distintos"),
        F.min("event_time").alias("primera_recepcion"), F.max("event_time").alias("ultima_recepcion"))
    vigente = dim.where("vigente").select("cuenta_id", "entidad_id", "nivel_riesgo",
                                          F.col("valido_desde").alias("nivel_desde"))
    perfiles = (tocadas.join(vigente, "cuenta_id", "left").join(enviados, "cuenta_id", "left")
                .join(recibidos, "cuenta_id", "left"))
    ahora = datetime.now(timezone.utc)
    ops, n = [], 0
    for r in perfiles.toLocalIterator():
        d = r.asDict()
        env, rec = d["transacciones_enviadas"] or 0, d["transacciones_recibidas"] or 0
        historico = {
            "entidad": d["entidad_id"], "nivel_riesgo": d["nivel_riesgo"], "nivel_desde": d["nivel_desde"],
            "transacciones_enviadas": env, "transacciones_recibidas": rec,
            "alertas_como_ordenante": d["alertas_como_ordenante"] or 0,
            "alertas_como_destino": d["alertas_como_destino"] or 0,
            "destinatarios_distintos": d["destinatarios_distintos"] or 0,
            "ordenantes_distintos": d["ordenantes_distintos"] or 0,
            # Proporción de contrapartes nuevas: la referencia de la pregunta 1 de la sección 1.3
            "proporcion_destinatarios_nuevos": round((d["destinatarios_distintos"] or 0) / env, 4) if env else None,
            "primera_transaccion": min(x for x in (d["primer_envio"], d["primera_recepcion"]) if x),
            "ultima_transaccion": max(x for x in (d["ultimo_envio"], d["ultima_recepcion"]) if x),
            "fuente": "gold.fact_transferencias", "actualizado": ahora}
        ops.append(UpdateOne({"_id": d["cuenta_id"]}, {"$set": {"historico": historico}}, upsert=True))
        if len(ops) == LOTE:
            db.cuentas.bulk_write(ops, ordered=False)
            n, ops = n + len(ops), []
    if ops:
        db.cuentas.bulk_write(ops, ordered=False)
        n += len(ops)
    db.sincronizacion.update_one({"_id": "perfiles"}, {"$set": {"hasta": hasta, "hasta_iso": hasta.isoformat(),
                                                                "cuentas": n, "ts": ahora}}, upsert=True)
    print(f"[perfiles] {n:,} perfiles actualizados en cuentas.historico ({'primera sincronización' if desde is None else f'hechos posteriores a {desde}'}) en {time.perf_counter() - t0:.0f} s")
    return {"cuentas": n}
