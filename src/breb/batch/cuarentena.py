"""reprocess_quarantine (RF12): corrige los eventos en cuarentena de Bronze y escribe en Silver los que quedan
válidos, con un MERGE que no genera duplicados. No se reinyecta nada a Kafka.

"Esquema corregido": el contrato 1.0 más estas correcciones, que no inventan datos:
- quita los campos que no están en el contrato;
- quita espacios y normaliza mayúsculas: cuentas en mayúsculas; moneda y canal con el nombre exacto del
  contrato si solo cambian mayúsculas o espacios ("us dollar" → "US Dollar");
- montos escritos como texto ("10.5") → número;
- event_ts en otros formatos de fecha (dd/MM/yyyy HH:mm, yyyy/MM/dd HH:mm, con espacio en lugar de T) → ISO;
- sin transaction_uuid válido: UUID v5 a partir de las coordenadas en Kafka (tópico, partición, offset), así el
  MERGE deduplica por coordenadas aunque el evento no traiga identificador.
Los eventos de la cuarentena no tienen posición en la reproducción (seq = -1) y no los puntúa el Job 2: llegan a
Gold sin decisión. Se valida todo el contrato excepto seq. Lo que no se puede corregir (montos negativos o
ausentes, moneda o canal desconocidos, cuenta sin banco) sigue en cuarentena con sus errores.

Cada evento en cuarentena queda en `silver/reprocesos_cuarentena` (una fila por coordenada de Kafka) con las
correcciones aplicadas, los errores que quedan y si se recuperó.
"""
from __future__ import annotations

import json
import re
import uuid
from datetime import datetime

import pandas as pd
from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from breb.batch.job4_gold import existe
from breb.batch.mantenimiento import REPROCESOS
from breb.contratos import NAMESPACE_EVENTOS, errores_evento, esquema
from breb.streaming.comun import BRONZE, SILVER

FORMATOS_FECHA = ["%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M", "%Y-%m-%d %H:%M",
                  "%d/%m/%Y %H:%M", "%d/%m/%Y %H:%M:%S", "%Y/%m/%d %H:%M", "%Y/%m/%d %H:%M:%S"]
PATRON_UUID = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")
ESQUEMA_REPROCESOS = T.StructType([
    T.StructField("kafka_topic", T.StringType()), T.StructField("kafka_partition", T.IntegerType()),
    T.StructField("kafka_offset", T.LongType()), T.StructField("transaction_uuid", T.StringType()),
    T.StructField("recuperado", T.BooleanType()), T.StructField("correcciones", T.ArrayType(T.StringType())),
    T.StructField("errores_originales", T.ArrayType(T.StringType())),
    T.StructField("errores_restantes", T.ArrayType(T.StringType())), T.StructField("reprocesado_ts", T.TimestampType()),
])


def corregir(evento: dict, coordenadas: tuple[str, int, int]) -> tuple[dict, list[str]]:
    contrato = esquema("evento_transferencia")
    props = contrato["properties"]
    ev, correcciones = dict(evento), []
    extra = sorted(k for k in ev if k not in props)
    if extra:
        for k in extra:
            ev.pop(k)
        correcciones.append(f"campos fuera del contrato eliminados: {', '.join(extra)}")
    for k, v in list(ev.items()):
        if isinstance(v, str) and v != v.strip():
            ev[k] = v.strip()
            correcciones.append(f"{k}: espacios eliminados")
    for k in ("source_account", "destination_account"):
        if isinstance(ev.get(k), str) and ev[k] != ev[k].upper():
            ev[k] = ev[k].upper()
            correcciones.append(f"{k}: mayúsculas")
    monedas = contrato["$defs"]["moneda"]["enum"]
    for k, opciones in (("payment_currency", monedas), ("receiving_currency", monedas),
                        ("payment_format", props["payment_format"]["enum"])):
        v = ev.get(k)
        if isinstance(v, str) and v not in opciones:
            igual = [o for o in opciones if " ".join(o.lower().split()) == " ".join(v.lower().split())]
            if igual:
                ev[k] = igual[0]
                correcciones.append(f"{k}: {v!r} → {igual[0]!r}")
    for k in ("amount_paid", "amount_received", "from_bank", "to_bank"):
        v = ev.get(k)
        if isinstance(v, str):
            try:
                ev[k] = int(v) if k.endswith("bank") else float(v.replace(",", ""))
                correcciones.append(f"{k}: texto → número")
            except ValueError:
                pass
    ts = ev.get("event_ts")
    if isinstance(ts, str):
        for formato in FORMATOS_FECHA:
            try:
                iso = datetime.strptime(ts, formato).strftime("%Y-%m-%dT%H:%M:%S")
            except ValueError:
                continue
            if iso != ts:
                ev["event_ts"] = iso
                correcciones.append(f"event_ts: {ts!r} → {iso!r}")
            break
    if not (isinstance(ev.get("transaction_uuid"), str) and PATRON_UUID.match(ev["transaction_uuid"])):
        topico, particion, offset = coordenadas
        ev["transaction_uuid"] = str(uuid.uuid5(NAMESPACE_EVENTOS, f"kafka:{topico}:{particion}:{offset}"))
        correcciones.append("transaction_uuid derivado de las coordenadas en Kafka")
    if not isinstance(ev.get("seq"), int) or ev["seq"] < 0:
        ev["seq"] = -1       # sin posición en la reproducción
    return ev, correcciones


def errores_sin_seq(ev: dict) -> list[str]:
    errores = [e for e in errores_evento(ev) if not e.startswith("seq:")]
    if not errores:
        try:
            datetime.strptime(ev["event_ts"], "%Y-%m-%dT%H:%M:%S")
        except ValueError:
            errores.append("event_ts: fecha inexistente")
    return errores


def reprocesar(spark: SparkSession) -> dict:
    if not existe(spark, BRONZE):
        print("[cuarentena] Bronze todavía no existe")
        return {}
    cuarentena = (spark.read.format("delta").load(BRONZE).where("NOT is_valid")
                  .select("raw_payload", "kafka_topic", "kafka_partition", "kafka_offset", "kafka_timestamp",
                          "ingestion_timestamp", "validation_errors").toPandas())
    print(f"[cuarentena] eventos en cuarentena en Bronze: {len(cuarentena):,}")
    if cuarentena.empty:
        return {"en_cuarentena": 0}
    ahora = pd.Timestamp.now(tz="UTC").tz_localize(None)
    ahora_py = ahora.to_pydatetime()       # createDataFrame con tuplas pide datetime, no pandas.Timestamp
    estados, filas = [], []
    for r in cuarentena.itertuples(index=False):
        coords = (r.kafka_topic, int(r.kafka_partition), int(r.kafka_offset))
        try:
            original = json.loads(r.raw_payload)
            if not isinstance(original, dict):
                raise ValueError("no es un objeto")
        except (TypeError, ValueError) as e:
            estados.append((*coords, None, False, [], list(r.validation_errors), [f"(raíz): JSON inválido: {e}"], ahora_py))
            continue
        ev, correcciones = corregir(original, coords)
        errores = errores_sin_seq(ev)
        estados.append((*coords, ev["transaction_uuid"], not errores, correcciones, list(r.validation_errors), errores, ahora_py))
        if not errores:
            t = pd.Timestamp(ev["event_ts"])
            filas.append({
                "transaction_uuid": ev["transaction_uuid"], "seq": -1, "event_time": t,
                "produced_at": pd.to_datetime(ev["produced_at"], utc=True).tz_localize(None),
                "from_bank": ev["from_bank"], "source_account": ev["source_account"], "to_bank": ev["to_bank"],
                "destination_account": ev["destination_account"], "amount_paid": float(ev["amount_paid"]),
                "payment_currency": ev["payment_currency"], "amount_received": float(ev["amount_received"]),
                "receiving_currency": ev["receiving_currency"], "payment_format": ev["payment_format"],
                "kafka_partition": coords[1], "kafka_offset": coords[2], "kafka_timestamp": r.kafka_timestamp,
                "ingestion_timestamp": r.ingestion_timestamp, "silver_timestamp": ahora,
                "year": t.year, "month": t.month, "day": t.day})
    resumen = {"en_cuarentena": len(estados), "recuperables": len(filas), "insertados_en_silver": 0}
    if filas:
        esquema_silver = spark.read.format("delta").load(SILVER).schema
        nuevos = spark.createDataFrame(pd.DataFrame(filas)[esquema_silver.fieldNames()], esquema_silver)
        ya = spark.read.format("delta").load(SILVER).select("transaction_uuid")
        resumen["insertados_en_silver"] = nuevos.join(ya, "transaction_uuid", "left_anti").count()
        dias = nuevos.select("year", "month", "day").distinct().collect()
        filtro = " OR ".join(f"(t.year = {d.year} AND t.month = {d.month} AND t.day = {d.day})" for d in dias)
        (DeltaTable.forPath(spark, SILVER).alias("t")
         .merge(nuevos.alias("s"), f"t.transaction_uuid = s.transaction_uuid AND ({filtro})")
         .whenNotMatchedInsertAll().execute())
    tabla = spark.createDataFrame(estados, ESQUEMA_REPROCESOS)
    if not existe(spark, REPROCESOS):
        tabla.write.format("delta").save(REPROCESOS)
    else:
        (DeltaTable.forPath(spark, REPROCESOS).alias("t")
         .merge(tabla.alias("s"), "t.kafka_topic = s.kafka_topic AND t.kafka_partition = s.kafka_partition "
                                  "AND t.kafka_offset = s.kafka_offset")
         .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute())
    motivos = (tabla.where("NOT recuperado").select(F.explode("errores_restantes").alias("e"))
               .select(F.split("e", ":").getItem(0).alias("campo")).groupBy("campo").count()
               .orderBy(F.desc("count")).collect())
    print(f"[cuarentena] recuperados: {resumen['recuperables']:,} de {resumen['en_cuarentena']:,} "
          f"(insertados en Silver en esta ejecución: {resumen['insertados_en_silver']:,}; el resto ya estaba)")
    if motivos:
        print("[cuarentena] siguen en cuarentena, por campo con error: "
              + ", ".join(f"{m['campo']} {m['count']}" for m in motivos))
    ejemplos = tabla.where("recuperado").select("correcciones").limit(3).collect()
    for e in ejemplos:
        print(f"[cuarentena] ejemplo de corrección: {'; '.join(e['correcciones'])}")
    return resumen
