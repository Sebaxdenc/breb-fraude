"""El lakehouse en Amazon Athena (modo AWS, rama aws/mvp-ec2; guía en infra/aws/README.md, paso 11).

1. Crea el bucket de resultados de Athena (<prefijo>athena-resultados) y lo deja como ubicación de resultados
   del workgroup primary, para que también funcione el editor de consultas de la consola.
2. Registra las tablas Delta de Bronze, Silver y Gold en el AWS Glue Data Catalog, en las bases de datos bronze,
   silver y gold. Athena lee el esquema del _delta_log: CREATE EXTERNAL TABLE ... TBLPROPERTIES
   ('table_type' = 'DELTA'). Son las mismas tablas que registra python -m breb.consumo registrar en Trino.
3. Corre en Athena las consultas del tablero de Metabase (breb.consumo.tablero.preguntas), sin cambiarles una
   letra: Athena (versión 3) usa el mismo SQL de Trino.
4. Con --metabase, agrega Athena como segunda base de datos de Metabase y arma el mismo tablero sobre ella.

Uso, en la EC2:
    docker compose --profile consumo run --rm spark-cliente python /opt/breb/herramientas/aws/athena.py [--metabase]

Credenciales: el rol de la instancia (LabRole), como el resto del modo AWS.
"""
import argparse
import os
import time

import boto3
from botocore.exceptions import ClientError

from breb.buckets import PREFIJO
from breb.consumo import tablero
from breb.consumo.registrar import TABLAS

REGION = os.environ.get("AWS_REGION", "us-east-1")
BUCKET_RESULTADOS = f"{PREFIJO}athena-resultados"
SALIDA = f"s3://{BUCKET_RESULTADOS}/consultas/"
WORKGROUP = "primary"

athena = boto3.client("athena", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)


class ErrorAthena(RuntimeError):
    pass


def consulta(sql: str) -> tuple[list[list[str]], int, float]:
    """Corre una sentencia y devuelve (filas con el encabezado primero, bytes escaneados, segundos)."""
    qid = athena.start_query_execution(QueryString=sql, WorkGroup=WORKGROUP,
                                       ResultConfiguration={"OutputLocation": SALIDA})["QueryExecutionId"]
    while True:
        e = athena.get_query_execution(QueryExecutionId=qid)["QueryExecution"]
        estado = e["Status"]["State"]
        if estado in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break
        time.sleep(1)
    if estado != "SUCCEEDED":
        raise ErrorAthena(e["Status"].get("StateChangeReason", estado))
    filas = []
    for pagina in athena.get_paginator("get_query_results").paginate(QueryExecutionId=qid):
        filas += [[c.get("VarCharValue", "") for c in f["Data"]] for f in pagina["ResultSet"]["Rows"]]
    est = e.get("Statistics", {})
    return filas, est.get("DataScannedInBytes", 0), est.get("EngineExecutionTimeInMillis", 0) / 1000


def texto(filas: list[list[str]], maximo: int = 5) -> str:
    if not filas:
        return "  (sin filas)"
    muestra = [[v[:28] for v in f] for f in filas[:maximo + 1]]
    anchos = [max(len(f[i]) for f in muestra) for i in range(len(muestra[0]))]
    lineas = ["  " + "  ".join(v.ljust(a) for v, a in zip(f, anchos)) for f in muestra]
    if len(filas) - 1 > maximo:
        lineas.append(f"  ... {len(filas) - 1 - maximo} filas más")
    return "\n".join(lineas)


def preparar_resultados() -> None:
    try:
        s3.create_bucket(Bucket=BUCKET_RESULTADOS)
        print(f"[athena] bucket {BUCKET_RESULTADOS}: creado")
    except ClientError as e:
        if e.response["Error"]["Code"] != "BucketAlreadyOwnedByYou":
            raise
        print(f"[athena] bucket {BUCKET_RESULTADOS}: ya existe")
    try:
        athena.update_work_group(WorkGroup=WORKGROUP, ConfigurationUpdates={
            "ResultConfigurationUpdates": {"OutputLocation": SALIDA}})
        print(f"[athena] workgroup {WORKGROUP}: resultados en {SALIDA} (también para la consola)")
    except ClientError as e:
        print(f"[athena] no se pudo configurar el workgroup ({e.response['Error']['Code']}). En la consola: "
              f"Query editor → Settings → Manage → {SALIDA}")


def _tiene_delta_log(ubicacion: str) -> bool:
    bucket, _, clave = ubicacion.removeprefix("s3://").partition("/")
    r = s3.list_objects_v2(Bucket=bucket, Prefix=f"{clave.rstrip('/')}/_delta_log/", MaxKeys=1)
    return r.get("KeyCount", 0) > 0


def registrar(recrear: bool) -> list[tuple[str, str]]:
    for capa in sorted({c for c, _, _ in TABLAS}):
        consulta(f"CREATE DATABASE IF NOT EXISTS {capa}")
    registradas = []
    for capa, nombre, ruta in TABLAS:
        ubicacion = ruta.replace("s3a://", "s3://", 1).rstrip("/") + "/"
        if not _tiene_delta_log(ubicacion):
            print(f"[athena] {capa}.{nombre}: pendiente (todavía no hay tabla Delta en {ubicacion})")
            continue
        if recrear:
            consulta(f"DROP TABLE IF EXISTS {capa}.{nombre}")
        consulta(f"CREATE EXTERNAL TABLE IF NOT EXISTS {capa}.{nombre} LOCATION '{ubicacion}' "
                 "TBLPROPERTIES ('table_type' = 'DELTA')")
        registradas.append((capa, nombre))
    print(f"[athena] registradas en el Glue Data Catalog: {', '.join(f'{c}.{n}' for c, n in registradas)}")
    filas = [["tabla", "filas"]]
    for capa, nombre in registradas:
        r, _, _ = consulta(f"SELECT count(*) FROM {capa}.{nombre}")
        filas.append([f"{capa}.{nombre}", r[1][0]])
    print(texto(filas, maximo=len(filas)))
    return registradas


def preguntas_de_negocio() -> int:
    fallas = 0
    for p in tablero.preguntas():
        try:
            filas, escaneado, segundos = consulta(p["sql"])
        except ErrorAthena as e:
            fallas += 1
            print(f"[falla] {p['nombre']}: {str(e)[:300]}")
            continue
        print(f"[ok] {p['nombre']}: {len(filas) - 1} filas en {segundos:.1f} s, {escaneado / 1e6:.1f} MB escaneados")
        print(texto(filas))
    return fallas


def metabase() -> None:
    """Athena como segunda base de datos de Metabase y el mismo tablero sobre ella (crear() de breb.consumo.tablero)."""
    nombre = "Lakehouse (Athena)"

    def base_athena(sesion: str) -> int:
        bases = tablero._llamar("GET", "/api/database", sesion=sesion)
        bases = bases.get("data", bases) if isinstance(bases, dict) else bases
        for b in bases:
            if b["name"] == nombre:
                return b["id"]
        # Sin llaves: el driver de Athena usa la cadena de credenciales por defecto, es decir, el rol de la EC2
        b = tablero._llamar("POST", "/api/database", {
            "engine": "athena", "name": nombre, "is_full_sync": False,
            "details": {"region": REGION, "workgroup": WORKGROUP, "s3_staging_dir": SALIDA}}, sesion=sesion)
        print(f"base de datos '{nombre}' agregada (Amazon Athena, rol de la instancia)")
        return b["id"]

    tablero.base_de_datos = base_athena
    tablero.COLECCION = "Bre-B (Athena)"
    tablero.TABLERO = "Bre-B · detección de fraude (Athena)"
    tablero.crear()


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--metabase", action="store_true", help="arma también el tablero de Metabase sobre Athena")
    ap.add_argument("--recrear", action="store_true", help="borra y vuelve a crear las tablas del catálogo")
    a = ap.parse_args()
    preparar_resultados()
    registrar(a.recrear)
    fallas = preguntas_de_negocio()
    if a.metabase:
        metabase()
    print("ATHENA OK" if not fallas else f"ATHENA CON FALLAS: {fallas}")
    return 0 if not fallas else 1


if __name__ == "__main__":
    raise SystemExit(main())
