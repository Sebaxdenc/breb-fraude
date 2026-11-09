"""Tablero de Metabase (RF11, sección 1.3), creado por la API de Metabase.

Si Metabase no está configurado, crea el administrador (credenciales/metabase-admin.env). Agrega la conexión a
Trino con el driver Starburst y el usuario `metabase` (solo lectura en Gold) y crea, en la colección "Bre-B", las
preguntas del tablero "Bre-B · detección de fraude": las cuatro de la sección 1.3 más alertas, desempeño del
modelo, latencia y salud del streaming. Es repetible: si la colección ya existe, la archiva y la crea de nuevo.

Las variables del modelo se leen del vector `variables` de risk_decisions; la posición de cada una sale de
model_meta.json (los arreglos de Trino empiezan en 1).
"""
from __future__ import annotations

import json
import os
import time
import urllib.error
import urllib.request

URL = os.environ.get("METABASE_URL", "http://metabase:3000")
NOMBRE_BD = "Lakehouse (Trino)"
COLECCION = "Bre-B"
TABLERO = "Bre-B · detección de fraude"
DIR_MODELO = os.environ.get("BREB_DIR_MODELO", "/opt/breb/modelo/aml_xgb")


class ErrorMetabase(RuntimeError):
    pass


def _llamar(metodo: str, ruta: str, cuerpo=None, sesion: str | None = None, timeout: int = 120):
    cab = {"Content-Type": "application/json"}
    if sesion:
        cab["X-Metabase-Session"] = sesion
    datos = json.dumps(cuerpo).encode() if cuerpo is not None else None
    try:
        with urllib.request.urlopen(urllib.request.Request(URL + ruta, data=datos, headers=cab, method=metodo),
                                    timeout=timeout) as r:
            texto = r.read().decode()
            return json.loads(texto) if texto else None
    except urllib.error.HTTPError as e:
        raise ErrorMetabase(f"{metodo} {ruta}: HTTP {e.code} {e.read().decode()[:300]}") from None


def esperar(segundos: int = 300) -> None:
    limite = time.monotonic() + segundos
    while True:
        try:
            if _llamar("GET", "/api/health", timeout=10).get("status") == "ok":
                return
        except (urllib.error.URLError, ConnectionError, ErrorMetabase, ValueError, AttributeError):
            pass
        if time.monotonic() > limite:
            raise SystemExit(f"Metabase no respondió en {URL} después de {segundos} s")
        time.sleep(5)


def iniciar_sesion() -> str:
    correo, clave = os.environ["MB_ADMIN_CORREO"], os.environ["MB_ADMIN_CLAVE"]
    props = _llamar("GET", "/api/session/properties")
    if not props.get("has-user-setup") and props.get("setup-token"):
        r = _llamar("POST", "/api/setup", {
            "token": props["setup-token"], "prefs": {"site_name": "Bre-B", "site_locale": "es", "allow_tracking": False},
            "user": {"email": correo, "password": clave, "first_name": "Admin", "last_name": "Bre-B"}})
        print(f"Metabase configurado: administrador {correo}")
        return r["id"]
    try:
        return _llamar("POST", "/api/session", {"username": correo, "password": clave})["id"]
    except ErrorMetabase:
        raise SystemExit(f"Metabase ya tiene un administrador distinto de {correo}. Usen sus credenciales con "
                         "MB_ADMIN_CORREO y MB_ADMIN_CLAVE, o borren el volumen de PostgreSQL "
                         "(docker compose --profile consumo down -v).") from None


def base_de_datos(sesion: str) -> int:
    bases = _llamar("GET", "/api/database", sesion=sesion)
    bases = bases.get("data", bases) if isinstance(bases, dict) else bases
    for b in bases:
        if b["name"] == NOMBRE_BD:
            return b["id"]
    b = _llamar("POST", "/api/database", {
        "engine": "starburst", "name": NOMBRE_BD, "is_full_sync": False,
        "details": {"host": os.environ.get("TRINO_HOST", "trino"), "port": int(os.environ.get("TRINO_PUERTO", "8080")),
                    "catalog": "lakehouse", "schema": "gold", "user": "metabase", "ssl": False}}, sesion=sesion)
    print(f"base de datos '{NOMBRE_BD}' agregada (Trino, usuario metabase)")
    return b["id"]


def coleccion(sesion: str) -> int:
    for c in _llamar("GET", "/api/collection", sesion=sesion):
        if c.get("name") == COLECCION and not c.get("archived") and isinstance(c.get("id"), int):
            _llamar("PUT", f"/api/collection/{c['id']}", {"archived": True}, sesion=sesion)
    return _llamar("POST", "/api/collection", {"name": COLECCION, "color": "#509EE3", "parent_id": None}, sesion=sesion)["id"]


def preguntas() -> list[dict]:
    meta = json.loads(open(os.path.join(DIR_MODELO, "model_meta.json")).read())
    i = {f: meta["num_features"].index(f) + 1 for f in
         ("is_first_sender_pair", "sender_inflow_gap_sec", "sender_has_prior_inflow", "passthrough_amount_ratio")}
    base = ("FROM gold.fact_transferencias f JOIN gold.risk_decisions r ON r.transaction_uuid = f.transaction_uuid "
            "JOIN gold.dim_cuenta o ON o.cuenta_key = f.cuenta_origen_key "
            "JOIN gold.dim_cuenta d ON d.cuenta_key = f.cuenta_destino_key")
    alerta = "CASE WHEN f.decision = 'alerta' THEN 1 ELSE 0 END"
    return [
        {"nombre": "Desempeño del modelo frente a las etiquetas reales", "tipo": "table", "ancho": 12, "alto": 4, "sql":
         f"SELECT count(*) AS transacciones, sum({alerta}) AS alertas, "
         "sum(CASE WHEN f.decision = 'alerta' AND f.es_lavado = 1 THEN 1 ELSE 0 END) AS alertas_con_lavado, "
         "sum(f.es_lavado) AS lavado_real, "
         "round(1.0 * sum(CASE WHEN f.decision = 'alerta' AND f.es_lavado = 1 THEN 1 ELSE 0 END) / nullif(sum("
         f"{alerta}), 0), 3) AS precision, "
         "round(1.0 * sum(CASE WHEN f.decision = 'alerta' AND f.es_lavado = 1 THEN 1 ELSE 0 END) / nullif(sum(f.es_lavado), 0), 3) AS recall "
         "FROM gold.fact_transferencias f"},
        {"nombre": "Latencia Kafka → decisión por minuto (RNF-02)", "tipo": "line", "ancho": 12, "alto": 4,
         "visual": {"graph.dimensions": ["minuto"], "graph.metrics": ["p50_s", "p99_s"]}, "sql":
         "SELECT date_trunc('minute', kafka_timestamp) AS minuto, count(*) AS transacciones, "
         "round(approx_percentile(latencia_ms, 0.5) / 1000, 1) AS p50_s, round(approx_percentile(latencia_ms, 0.99) / 1000, 1) AS p99_s "
         "FROM gold.risk_decisions GROUP BY 1 ORDER BY 1"},
        {"nombre": "Alertas emitidas", "tipo": "table", "ancho": 24, "alto": 6, "sql":
         "SELECT f.event_time, o.cuenta_id AS ordenante, f.entidad_origen_id AS entidad_origen, d.cuenta_id AS destino, "
         "f.entidad_destino_id AS entidad_destino, round(f.monto_pagado, 2) AS monto, f.moneda_pago AS moneda, "
         "round(f.risk_score, 3) AS score, CASE f.es_lavado WHEN 1 THEN 'sí' ELSE 'no' END AS lavado_real, "
         f"round(f.latencia_ms / 1000, 1) AS latencia_s {base.replace('JOIN gold.risk_decisions r ON r.transaction_uuid = f.transaction_uuid ', '')} "
         "WHERE f.decision = 'alerta' ORDER BY f.event_time"},
        {"nombre": "1. Ordenantes con muchos destinatarios nuevos (ventanas de 5 min)", "tipo": "table", "ancho": 12, "alto": 6,
         "sql": "SELECT o.cuenta_id AS ordenante, "
         "date_add('minute', -(minute(f.event_time) % 5), date_trunc('minute', f.event_time)) AS ventana_5_min, "
         f"count(*) AS transferencias, CAST(sum(element_at(r.variables, {i['is_first_sender_pair']})) AS integer) AS destinatarios_nuevos, "
         f"round(max(r.risk_score), 3) AS score_maximo, sum({alerta}) AS alertas {base} "
         f"GROUP BY 1, 2 HAVING sum(element_at(r.variables, {i['is_first_sender_pair']})) >= 2 "
         "ORDER BY destinatarios_nuevos DESC, score_maximo DESC LIMIT 25"},
        {"nombre": "2. Concentración hacia posibles cuentas mula (ventanas de 10 min)", "tipo": "table", "ancho": 12, "alto": 6,
         "sql": "SELECT d.cuenta_id AS receptora, "
         "date_add('minute', -(minute(f.event_time) % 10), date_trunc('minute', f.event_time)) AS ventana_10_min, "
         "count(DISTINCT o.cuenta_id) AS ordenantes_distintos, count(*) AS transferencias, "
         f"round(sum(f.monto_recibido), 2) AS monto_recibido, sum({alerta}) AS alertas {base} "
         "GROUP BY 1, 2 HAVING count(DISTINCT o.cuenta_id) >= 2 ORDER BY ordenantes_distintos DESC, monto_recibido DESC LIMIT 25"},
        {"nombre": "3. Reenvío rápido de lo recibido (pass-through)", "tipo": "table", "ancho": 12, "alto": 6, "sql":
         "SELECT o.cuenta_id AS cuenta, f.event_time, round(f.monto_pagado, 2) AS monto_reenviado, f.moneda_pago AS moneda, "
         f"CAST(element_at(r.variables, {i['sender_inflow_gap_sec']}) AS bigint) AS segundos_desde_ultima_entrada, "
         f"round(element_at(r.variables, {i['passthrough_amount_ratio']}), 3) AS fraccion_reenviada, "
         f"round(r.risk_score, 3) AS score, f.decision {base} "
         f"WHERE element_at(r.variables, {i['sender_has_prior_inflow']}) = 1 "
         f"AND element_at(r.variables, {i['sender_inflow_gap_sec']}) <= 3600 "
         f"AND element_at(r.variables, {i['passthrough_amount_ratio']}) BETWEEN 0.8 AND 1.2 "
         "ORDER BY segundos_desde_ultima_entrada, score DESC LIMIT 25"},
        {"nombre": "4. Tasa de alertas por canal", "tipo": "bar", "ancho": 12, "alto": 6,
         "visual": {"graph.dimensions": ["canal"], "graph.metrics": ["porcentaje_alertas"]}, "sql":
         f"SELECT c.canal, count(*) AS transacciones, sum({alerta}) AS alertas, "
         f"round(100.0 * sum({alerta}) / count(*), 3) AS porcentaje_alertas "
         "FROM gold.fact_transferencias f JOIN gold.dim_canal c ON c.canal_key = f.canal_key "
         "GROUP BY 1 ORDER BY porcentaje_alertas DESC"},
        {"nombre": "4. Tasa de alertas por entidad de origen", "tipo": "table", "ancho": 12, "alto": 6, "sql":
         f"SELECT e.nombre AS entidad, count(*) AS transacciones, sum({alerta}) AS alertas, "
         f"round(100.0 * sum({alerta}) / count(*), 3) AS porcentaje_alertas "
         "FROM gold.fact_transferencias f JOIN gold.dim_entidad_financiera e ON e.entidad_id = f.entidad_origen_id "
         f"GROUP BY 1 HAVING sum({alerta}) > 0 ORDER BY alertas DESC, porcentaje_alertas DESC LIMIT 15"},
        {"nombre": "Salud del streaming (StreamingQueryListener)", "tipo": "table", "ancho": 24, "alto": 5, "sql":
         "SELECT job, consulta, count(*) AS micro_batches, sum(filas) AS filas, max(ts) AS ultimo_micro_batch, "
         "approx_percentile(duracion_ms, 0.5) AS duracion_p50_ms, max(duracion_ms) AS duracion_max_ms, "
         "max(max_offsets_atras) AS max_offsets_atras FROM gold.progreso_streaming GROUP BY 1, 2 ORDER BY 1, 2"},
    ]


def crear(registro=print) -> str:
    esperar()
    sesion = iniciar_sesion()
    bd = base_de_datos(sesion)
    col = coleccion(sesion)
    tarjetas, fila, columna, alto_fila = [], 0, 0, 0
    for k, p in enumerate(preguntas()):
        tarjeta = _llamar("POST", "/api/card", {
            "name": p["nombre"], "collection_id": col, "display": p["tipo"],
            "visualization_settings": p.get("visual", {}),
            "dataset_query": {"type": "native", "database": bd, "native": {"query": p["sql"], "template-tags": {}}}},
            sesion=sesion)
        r = _llamar("POST", f"/api/card/{tarjeta['id']}/query", {}, sesion=sesion, timeout=180)
        if r.get("status") != "completed":
            raise ErrorMetabase(f"la pregunta '{p['nombre']}' falló: {str(r.get('error'))[:300]}")
        registro(f"[ok] {p['nombre']}: {r['row_count']} filas")
        if columna + p["ancho"] > 24:
            fila, columna, alto_fila = fila + alto_fila, 0, 0
        tarjetas.append({"id": -(k + 1), "card_id": tarjeta["id"], "row": fila, "col": columna,
                         "size_x": p["ancho"], "size_y": p["alto"], "parameter_mappings": [], "visualization_settings": {}})
        columna += p["ancho"]
        alto_fila = max(alto_fila, p["alto"])
    tablero = _llamar("POST", "/api/dashboard", {"name": TABLERO, "collection_id": col}, sesion=sesion)
    _llamar("PUT", f"/api/dashboard/{tablero['id']}", {"dashcards": tarjetas}, sesion=sesion)
    enlace = f"http://localhost:3000/dashboard/{tablero['id']}"
    registro(f"tablero listo: {enlace} (usuario {os.environ['MB_ADMIN_CORREO']})")
    return enlace
