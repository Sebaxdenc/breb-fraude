"""Registro de las tablas Delta de las tres capas en el Hive Metastore, a través de Trino (RF11), y prueba de los
permisos por rol.

Esquemas del catálogo `lakehouse`: bronze, silver y gold. Spark escribe las tablas en S3; aquí solo se
registran (`CALL lakehouse.system.register_table`). Es idempotente: una tabla ya registrada que sigue leyéndose
no se toca; si su ubicación ya no tiene una tabla Delta válida (p. ej. después de `docker compose down -v`), se
vuelve a registrar; si todavía no existe (p. ej. Gold antes del Job 4), queda pendiente.
"""
from __future__ import annotations

import os

from breb.consumo.trino import ErrorTrino, consulta, tabla_texto

GOLD = os.environ.get("BREB_GOLD", "s3a://lakehouse-gold/analytics")
TABLAS = [
    ("bronze", "transacciones", os.environ.get("BREB_BRONZE", "s3a://lakehouse-bronze/transacciones")),
    ("silver", "transacciones", os.environ.get("BREB_SILVER", "s3a://lakehouse-silver/transacciones")),
    ("gold", "risk_decisions", os.environ.get("BREB_RISK_DECISIONS", "s3a://lakehouse-gold/risk_decisions")),
    ("gold", "progreso_streaming", os.environ.get("BREB_METRICAS", "s3a://lakehouse-gold/metricas/progreso_streaming")),
] + [("gold", t, f"{GOLD}/{t}") for t in ("fact_transferencias", "dim_cuenta", "dim_entidad_financiera", "dim_canal",
                                         "dim_tiempo", "etiquetas", "ejecuciones")]


def _registrar(esquema: str, tabla: str, ubicacion: str) -> None:
    consulta(f"CALL lakehouse.system.register_table(schema_name => '{esquema}', table_name => '{tabla}', "
             f"table_location => '{ubicacion}')")


def registrar(registro=print) -> dict:
    estado = {"registradas": [], "ya_estaban": [], "re_registradas": [], "pendientes": []}
    for esquema in ("bronze", "silver", "gold"):
        consulta(f"CREATE SCHEMA IF NOT EXISTS lakehouse.{esquema} "
                 f"WITH (location = 's3a://lakehouse-{esquema}/_esquema_trino')")
    for esquema, tabla, ubicacion in TABLAS:
        nombre = f"{esquema}.{tabla}"
        existentes = {f[0] for f in consulta(f"SHOW TABLES FROM lakehouse.{esquema}")[1]}
        try:
            if tabla in existentes:
                try:
                    consulta(f"SELECT count(*) FROM lakehouse.{nombre} WHERE false")
                    estado["ya_estaban"].append(nombre)
                    continue
                except ErrorTrino:
                    consulta(f"CALL lakehouse.system.unregister_table(schema_name => '{esquema}', table_name => '{tabla}')")
                    _registrar(esquema, tabla, ubicacion)
                    estado["re_registradas"].append(nombre)
            else:
                _registrar(esquema, tabla, ubicacion)
                estado["registradas"].append(nombre)
        except ErrorTrino as e:
            estado["pendientes"].append(f"{nombre} ({str(e).splitlines()[0][:90]})")
    for clave, valores in estado.items():
        if valores:
            registro(f"{clave.replace('_', ' ')}: {', '.join(valores)}")
    filas = []
    for esquema, tabla, _ in TABLAS:
        try:
            filas.append([f"lakehouse.{esquema}.{tabla}", consulta(f"SELECT count(*) FROM lakehouse.{esquema}.{tabla}")[1][0][0]])
        except ErrorTrino:
            filas.append([f"lakehouse.{esquema}.{tabla}", "sin registrar"])
    registro("\n" + tabla_texto(["tabla", "filas"], filas, 50))
    return estado


PRUEBAS_PERMISOS = [
    ("analista", "SELECT count(*) FROM lakehouse.gold.fact_transferencias", True),
    ("analista", "SELECT count(*) FROM lakehouse.silver.transacciones", False),
    ("analista", "SELECT count(*) FROM lakehouse.bronze.transacciones", False),
    ("cumplimiento", "SELECT count(*) FROM lakehouse.bronze.transacciones", True),
    ("cumplimiento", "SELECT count(*) FROM lakehouse.silver.transacciones", True),
    ("cumplimiento", "SELECT count(*) FROM lakehouse.gold.fact_transferencias", True),
    ("cumplimiento", "DELETE FROM lakehouse.gold.etiquetas WHERE false", False),
    ("metabase", "SELECT count(*) FROM lakehouse.gold.risk_decisions", True),
    ("metabase", "SELECT count(*) FROM lakehouse.bronze.transacciones", False),
    ("otro_usuario", "SELECT count(*) FROM lakehouse.gold.fact_transferencias", False),
]


def comprobar_permisos(registro=print) -> bool:
    filas, ok = [], True
    for usuario, sql, permitido in PRUEBAS_PERMISOS:
        try:
            consulta(sql, usuario=usuario)
            resultado = "permitido"
        except ErrorTrino as e:
            resultado = "denegado" if e.tipo == "PERMISSION_DENIED" or "Access Denied" in str(e) else f"error: {e}"
        esperado = "permitido" if permitido else "denegado"
        ok &= resultado == esperado
        filas.append([usuario, sql.replace("lakehouse.", ""), resultado, "ok" if resultado == esperado else "FALLA"])
    registro("\n" + tabla_texto(["usuario", "sentencia", "resultado", ""], filas))
    return ok

