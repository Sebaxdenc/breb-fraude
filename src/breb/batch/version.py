"""Versión actual de una tabla Delta sin Spark: el número del último commit en `_delta_log` (lo usa el sensor
check_new_silver_data del DAG, que corre en segundos)."""
from __future__ import annotations

import os
import re

from breb.streaming.comun import SILVER

COMMIT = re.compile(r"(\d{20})\.json$")


def version_delta(ruta: str = SILVER) -> int | None:
    """None si la tabla todavía no existe."""
    if ruta.startswith("file://"):
        d = os.path.join(ruta[len("file://"):], "_delta_log")
        if not os.path.isdir(d):
            return None
        versiones = [int(m.group(1)) for f in os.listdir(d) if (m := COMMIT.search(f))]
        return max(versiones) if versiones else None
    import boto3
    bucket, prefijo = ruta.split("://", 1)[1].split("/", 1)
    s3 = boto3.client("s3", endpoint_url=os.environ.get("S3_ENDPOINT"))
    versiones = []
    for pagina in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=f"{prefijo}/_delta_log/"):
        versiones += [int(m.group(1)) for o in pagina.get("Contents", []) if (m := COMMIT.search(o["Key"]))]
    return max(versiones) if versiones else None


def version_silver_pendiente() -> tuple[int | None, int | None]:
    """(versión actual de Silver, última versión que llegó a Gold sin fallas de calidad)."""
    from breb.batch.comun import base_mongo
    actual = version_delta(SILVER)
    db = base_mongo()
    marca = db.sincronizacion.find_one({"_id": "gold"}) if db is not None else None
    return actual, (marca or {}).get("version_silver")
