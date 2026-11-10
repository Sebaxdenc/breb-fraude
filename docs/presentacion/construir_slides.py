"""Genera las láminas de la sustentación: slides.pptx (editable) y slides.pdf (para presentar sin conexión).

    python docs/presentacion/construir_slides.py

Necesita python-pptx, Pillow y playwright con Chromium (`playwright install chromium`). Las notas del orador salen de
guion.md (párrafos que empiezan con "> " bajo cada "### Lámina N").

Cómo funciona:
1. Cada lámina se describe una sola vez abajo (LAMINAS), en pulgadas sobre una página de 13,333 × 7,5.
2. Se arma un HTML con esa geometría y Chromium lo renderiza con Liberation Sans, que tiene las mismas métricas
   que Arial. Así se mide el alto de cada fila de las tablas y se detecta cualquier texto que no quepa.
3. Se exporta slides.pdf y una vista PNG por lámina en vista/ (para revisar).
4. Se arma slides.pptx con Arial y los altos medidos.
Si el sistema no tiene Arial ni Liberation Sans, BREB_FUENTES apunta a una carpeta con LiberationSans-*.ttf.
"""
from __future__ import annotations

import html
import os
import pathlib
import re
import sys

from PIL import Image

AQUI = pathlib.Path(__file__).resolve().parent
DOCS = AQUI.parent
ANCHO, ALTO = 13.333, 7.5
NAVY, GRIS, ROJO, AZUL, VERDE = "232F3E", "5F6B7A", "C00000", "1E6FD9", "2E9E44"
SUAVE, BLANCO, CLARO = "F2F4F7", "FFFFFF", "C9D3E0"
ESTADOS = {"Cumple": ("E3F4E6", "1E7B34"), "No cumple": ("FBE3E3", "A30000"),
           "Parcial": ("FDF0E1", "A85A00"), "Pendiente": ("FDF0E1", "A85A00")}
INTERLINEA = 1.2      # PowerPoint usa cerca de 1,2 en interlineado sencillo con Arial
SANGRIA, BALA = 0.22, 0.16


# ------------------------------------------------------------------ elementos
def texto(x, y, w, h, pars, tam=14, color=NAVY, fondo=None, borde=None, anclaje="t", alin="l", margen=0.05,
          despues=6):
    return dict(tipo="texto", x=x, y=y, w=w, h=h, pars=pars, tam=tam, color=color, fondo=fondo, borde=borde,
                anclaje=anclaje, alin=alin, margen=margen, despues=despues)


def tabla(x, y, anchos, filas, tam=12, max_h=None):
    return dict(tipo="tabla", x=x, y=y, anchos=anchos, filas=filas, tam=tam, max_h=max_h)


def imagen(ruta, x, y, w, h):
    return dict(tipo="img", ruta=str(ruta), x=x, y=y, w=w, h=h)


def rect(x, y, w, h, fondo):
    return dict(tipo="rect", x=x, y=y, w=w, h=h, fondo=fondo)


def estado(t):
    fondo, color = ESTADOS[t]
    return {"t": t, "fondo": fondo, "color": color, "b": True}


def normalizar(p, tam, color, despues):
    d = dict(p) if isinstance(p, dict) else {"t": p}
    t = d["t"]
    if t.startswith("-- "):
        d.update(t=t[3:], nivel=2)
    elif t.startswith("- "):
        d.update(t=t[2:], nivel=1)
    elif t.startswith("# "):
        d.update(t=t[2:], b=True, tam=d.get("tam", tam + 1), despues=d.get("despues", 4))
    d.setdefault("tam", tam)
    d.setdefault("color", color)
    d.setdefault("nivel", 0)
    d.setdefault("b", False)
    d.setdefault("despues", despues)
    return d


def tramos(t):
    """'a **b** c' -> [('a ', False), ('b', True), (' c', False)]"""
    partes = t.split("**")
    return [(s, i % 2 == 1) for i, s in enumerate(partes) if s]


def celda(c):
    return dict(c) if isinstance(c, dict) else {"t": c}


# ------------------------------------------------------------------ láminas
ARQ = DOCS / "arquitectura" / "arquitectura_e2.png"
DAGS = DOCS / "arquitectura" / "dags_airflow.png"
LAT = AQUI / "img" / "latencia.png"


def titulo(t):
    return [rect(0.5, 0.42, 0.08, 0.52, ROJO), texto(0.72, 0.3, 12.1, 0.76, [{"t": t, "b": True}], tam=26, anclaje="m")]


LAMINAS = [
    {"expone": "Sebastian", "pie": True, "el": [
        rect(0.5, 1.42, 0.08, 1.75, ROJO),
        texto(0.72, 0.85, 7.6, 0.42, ["ST1630 Sistemas Intensivos en Datos · Universidad EAFIT · Entregable 2"], tam=13,
              color=GRIS),
        texto(0.72, 1.35, 7.6, 1.9, [{"t": "Bre-B: detección de fraude transaccional en tiempo real", "b": True}],
              tam=36, anclaje="m"),
        texto(0.72, 3.35, 7.6, 0.55, ["Qué construimos, por qué así y qué aprendimos"],
              tam=17, color=AZUL),
        texto(0.72, 4.2, 7.6, 1.9, ["**Samuel Arango** · ingesta y streaming",
                                     "**Mateo Sanz Medina** · almacenamiento",
                                     "**Nathalia Valentina Cardoza Azuaje** · DataOps",
                                     "**Sebastian Andres Medina Cabezas** · consumo y gobernanza"], tam=15, despues=5),
        texto(0.72, 6.25, 7.6, 0.4, ["Profesor Andrés Sacre Alzate · 2026-2 · Medellín"], tam=12, color=GRIS),
        texto(8.75, 1.35, 4.1, 4.9, [{"t": "Pregunta central del proyecto", "tam": 13, "color": CLARO, "despues": 12},
                                     {"t": "Dado el fraude en pagos inmediatos, ¿qué pipeline de datos construiríamos "
                                           "y por qué exactamente así?", "tam": 23, "b": True, "color": BLANCO}],
              fondo=NAVY, anclaje="m", margen=0.32),
    ]},
    {"expone": "Sebastian", "pie": True, "el": titulo("El problema: en Bre-B el fraude se consuma en minutos") + [
        texto(0.5, 1.4, 6.2, 5.45, [
            "# Bre-B en cifras",
            "- Acredita cada transferencia en unos **20 segundos**, 24/7.",
            "- Primer año: **1.700 millones** de transacciones y 36 millones de usuarios con llave.",
            "- Día récord: **10,4 millones** de operaciones, unas **120 por segundo** en promedio.",
            "# El dolor",
            "- Las redes de mulas reparten el dinero entre entidades **en minutos**, y el pago inmediato no se reversa.",
            "- La banca analiza por lotes, con cortes de **12 a 24 horas**: la alerta llega cuando el dinero ya salió.",
            "# Lo que se necesita",
            "- Decidir **en segundos** por transacción y avisar a la entidad para que retenga los fondos.",
            "- Guardar todo con su linaje para la auditoría (SFC, UIAF).",
        ], tam=15),
        tabla(6.95, 1.5, [1.6, 2.05, 2.23], [
            ["Usuario", "Qué recibe", "Qué decide"],
            ["Entidad financiera", "La alerta en su cola SQS", "Retener los fondos en la cuenta destino o bloquear la ordenante"],
            ["Analistas de fraude", "Tablero de Metabase y bandeja de alertas", "Confirmar redes, reportar a la UIAF y calibrar el umbral"],
            ["Cumplimiento", "SQL sobre Bronze, Silver y Gold", "Reconstruir el linaje de una alerta para la SFC"],
        ], tam=14.5, max_h=3.6),
        texto(6.95, 4.35, 5.88, 1.15, ["**Detección asíncrona con notificación:** el sistema no participa en la "
                                      "autorización del pago; decide y avisa en paralelo."],
              tam=14, fondo=SUAVE, anclaje="m", margen=0.16),
    ]},
    {"expone": "Sebastian", "pie": True, "el": titulo("Las cuatro preguntas que responde el sistema") + [
        tabla(0.5, 1.45, [0.45, 4.1, 4.6, 3.18], [
            ["#", "Pregunta (Entregable 1)", "Cómo se calcula", "Dónde se ve"],
            ["1", "¿El ordenante envía a muchos destinatarios nuevos en poco tiempo?",
             "Ventana deslizante de 5 min por ordenante (job2-ventanas) y la variable is_first_sender_pair",
             "cuentas.ventanas en MongoDB · tablero, pregunta 1"],
            ["2", "¿Hay concentración hacia cuentas mula?",
             "Ordenantes distintos y monto recibido por receptora en ventanas de 10 min",
             "cuentas.ventanas · tablero, pregunta 2"],
            ["3", "¿La cuenta reenvía rápido lo que recibe (pass-through)?",
             "Segundos desde la última entrada y fracción reenviada: variables del modelo calculadas con el estado",
             "risk_decisions.variables · tablero, pregunta 3"],
            ["4", "¿Qué tasa de transacciones marca el modelo, por entidad y canal?",
             "Porcentaje de transacciones con score ≥ umbral",
             "Gold: fact_transferencias con dim_canal y dim_entidad_financiera"],
        ], tam=15.5, max_h=3.95),
        texto(0.5, 5.35, 12.33, 1.25, ["**La decisión la toma el modelo, no las preguntas.** XGBoost preentrenado de Kaggle "
                                     "con IBM AML (corrección 2 del profesor): PR-AUC de **0,267** en el test fuera de "
                                     "tiempo, unas 450 veces el de un modelo al azar; umbral 0,172. Las preguntas dan "
                                     "contexto a cada alerta y alimentan los tableros."],
              tam=14, fondo=SUAVE, anclaje="m", margen=0.18),
    ]},
    {"expone": "Samuel", "pie": False, "el": [imagen(ARQ, 0, 0, ANCHO, ALTO)]},
    {"expone": "Nathalia", "pie": True, "el": titulo("Del Entregable 1 al 2: qué se mantuvo, qué cambió y qué falló") + [
        tabla(0.5, 1.35, [1.75, 2.6, 3.0, 4.98], [
            ["Pieza", "Entregable 1", "Entregable 2", "Por qué"],
            ["Ingesta", "API de ingesta en ECS y Kafka 3.6", "El productor publica directo en Kafka 4.3.2",
             "La API no tenía un consumidor real (D3). Kafka 3.6 ya no tiene soporte ni imagen oficial"],
            ["S3 local", "MinIO", "SeaweedFS 4.52",
             "MinIO dejó de publicar imágenes. Delta pasó la prueba con dos escritores concurrentes (D1)"],
            ["Log de Delta", "S3DynamoDBLogStore", "El mismo, con un parche de 5 líneas",
             "Delta 3.3.3 no deja configurar el endpoint de DynamoDB Local"],
            ["risk_decisions", "Escritura con txnAppId dentro del micro-batch",
             "Archivo por micro-batch y commit Delta cada 10 s", "Escribir en Delta costaba de 6 a 19 s por micro-batch"],
            ["Ventanas (RF3)", "Dentro del Job 2", "Aplicación aparte: job2-ventanas", "No quitarle núcleos a la decisión"],
            ["transaction_uuid", "UUID v4", "UUID v5 (determinista)",
             "El mismo evento siempre tiene el mismo identificador, también al reprocesar"],
            ["Alertas a analistas", "Correo", "Cola SQS sin filtro", "No hay servidor de correo en local"],
            ["Tareas de Airflow", "SparkSubmitOperator o DockerOperator", "spark-submit local[2] en la imagen de Spark",
             "Sin exponer el socket de Docker; el streaming ocupa los núcleos del worker"],
        ], tam=13, max_h=4.35),
        texto(0.5, 5.85, 6.05, 1.0, ["**Se mantuvo:** Kafka, Spark Structured Streaming, MongoDB CP, Delta en tres capas, "
                                     "estrella con SCD 2, cuarentena en Bronze, Trino con catálogo, SNS y SQS, Airflow 3."],
              tam=12.5, fondo="E3F4E6", anclaje="m", margen=0.14),
        texto(6.78, 5.85, 6.05, 1.0, ["**Falló:** la latencia de la decisión (RNF-02, p99 < 3 s). Se midió de 285 a 53 s. "
                                      "La lámina 11 explica por qué."],
              tam=12.5, fondo="FBE3E3", anclaje="m", margen=0.14),
    ]},
    {"expone": "Samuel", "pie": True,
     "el": titulo("Kafka y Spark Structured Streaming, con el modelo en el Job 2") + [
        texto(0.5, 1.4, 6.4, 5.45, [
            "# ¿Por qué Spark Structured Streaming y no Flink?",
            "- Un solo motor para los 4 jobs, en streaming y en batch, con integración nativa con Delta.",
            "- El equipo ya lo conocía; Flink obligaba a mantener dos motores.",
            "- Costo: latencia = espera del micro-batch + su duración (trigger de 1 s).",
            "# ¿Por qué Kafka y no Kinesis?",
            "- Spark open source no trae una fuente de Kinesis para Structured Streaming.",
            "- Corre sin internet en Docker; en AWS es MSK, con la misma API.",
            "- Varios consumidores independientes del mismo log (Job 1, Job 2 y sus ventanas), con 7 días de retención.",
            "- Clave source_account: el orden de cada cuenta se conserva en su partición.",
        ], tam=15),
        texto(7.15, 1.4, 5.68, 0.5, ["# Paridad exacta con el notebook (riesgo R8)"], tam=14),
        texto(7.15, 1.95, 5.68, 1.08, [{"t": "58 de 58", "tam": 26, "b": True, "color": AZUL, "despues": 0},
                                      {"t": "variables idénticas al notebook, bit a bit", "tam": 12.5}],
              fondo=SUAVE, anclaje="m", margen=0.14),
        texto(7.15, 3.13, 5.68, 1.08, [{"t": "20.000 de 20.000", "tam": 26, "b": True, "color": AZUL, "despues": 0},
                                      {"t": "decisiones iguales en Kaggle (NumPy 2) y en Docker (NumPy 1.26)", "tam": 12.5}],
              fondo=SUAVE, anclaje="m", margen=0.14),
        texto(7.15, 4.31, 5.68, 1.08, [{"t": "0,26744793", "tam": 26, "b": True, "color": AZUL, "despues": 0},
                                      {"t": "PR-AUC del Job 2 en el test completo, igual al del notebook", "tam": 12.5}],
              fondo=SUAVE, anclaje="m", margen=0.14),
        texto(7.15, 5.5, 5.68, 1.4, ["Para lograrlo se replicaron las sumas de Kahan de pandas, la aritmética de numexpr "
                                      "(float64 y pow) y el orden por seq dentro del micro-batch. Limitar los offsets por "
                                      "micro-batch rompía la paridad, así que se quitó (D2)."], tam=12.5, color=GRIS),
    ]},
    {"expone": "Mateo", "pie": True,
     "el": titulo("¿Cómo evitamos procesar dos veces un evento?") + [
        texto(0.5, 1.3, 12.33, 0.75, ["Kafka entrega al menos una vez. Si Spark falla, repite el micro-batch con los mismos "
                                      "offsets del checkpoint. Por eso **cada efecto es idempotente**."], tam=15),
        tabla(0.5, 2.1, [2.35, 6.0, 3.98], [
            ["Componente", "Mecanismo", "Evidencia"],
            ["Productor → Kafka", "acks=all y enable.idempotence: un reintento no duplica el mensaje",
             "Bronze no repite coordenadas de Kafka"],
            ["Job 1 → Bronze", "Offsets en el checkpoint y sink de Delta: un solo commit por micro-batch",
             "71.910 de 71.910 eventos en Bronze"],
            ["Job 2 → MongoDB", "Cada documento guarda el batch_id que lo escribió y streaming_prev; si el micro-batch "
                                "se repite, se parte de streaming_prev",
             "Prueba: la escritura falla a la mitad y el reintento deja el mismo estado"],
            ["Job 2 → risk_decisions", "Archivo batch_<id>.parquet con nombre fijo; otra consulta lo agrega a Delta con "
                                       "su propio checkpoint", "Una fila por transacción"],
            ["Job 2 → SNS → entidad", "SNS puede repetir; la entidad registra {entidad}:{transaction_uuid} y descarta "
                                      "las repetidas", "Probado con moto: la reentrega no se aplica dos veces"],
            ["Job 3 y Job 4", "MERGE por transaction_uuid que solo inserta; Gold lee Silver en una versión fija",
             "Silver sin duplicados; calidad: hechos = Silver"],
        ], tam=14, max_h=3.95),
        texto(0.5, 6.1, 12.33, 0.72, ["Resultado: **exactly-once en el efecto**, no en el transporte."],
              tam=16, fondo=SUAVE, anclaje="m", margen=0.14),
    ]},
    {"expone": "Mateo", "pie": True,
     "el": titulo("Estado en MongoDB (CP) y lakehouse en Delta Lake") + [
        texto(0.5, 1.4, 6.0, 5.45, [
            "# MongoDB 7.0, replica set",
            "- Leer Silver en S3 tarda de 1 a 4 s: no cabe en un micro-batch de 1 s. MongoDB responde por _id, en lote.",
            "- **CP:** write concern y read concern majority. Una escritura confirmada sobrevive a la elección de "
            "otro primario.",
            "- **Se sacrifica disponibilidad:** durante la elección (electionTimeoutMillis = 2.000) el Job 2 no escribe "
            "y la latencia sube.",
            "- Claves: cuentas._id = cuenta; pares._id = \"origen|destino\" (dirigido); bancos._id = banco. "
            "En producción, sharding por hash de la cuenta.",
            "- Cassandra descartada: sus contadores no son idempotentes ante reintentos.",
        ], tam=15),
        texto(6.83, 1.4, 6.0, 5.45, [
            "# Delta Lake 3.3 en tres capas",
            "- **Bronze:** append-only, el 100 % de los eventos con sus coordenadas de Kafka; la cuarentena es "
            "is_valid = false.",
            "- **Silver:** tipado, en UTC y sin duplicados (MERGE por transaction_uuid).",
            "- **Gold:** estrella con fact_transferencias, dim_cuenta (SCD 2), entidad, canal y tiempo, más "
            "risk_decisions y las etiquetas.",
            "- Varios escritores por tabla (Job 3 y la cuarentena en Silver, OPTIMIZE): el log de commits va en DynamoDB.",
            "- Iceberg descartado: más complejo con Structured Streaming en local; Delta trae MERGE, time travel "
            "y OPTIMIZE.",
        ], tam=15),
    ]},
    {"expone": "Nathalia", "pie": True,
     "el": titulo("Airflow 3 para el batch y Trino con catálogo para consultar") + [
        imagen(DAGS, 0.5, 1.3, 12.33, 3.05),
        texto(0.5, 4.5, 6.0, 2.35, [
            "# Airflow 3.3.2 (ADR 05)",
            "- Las tres etapas del Job 4 leen Silver en la versión que fija el sensor: un reintento no mezcla datos nuevos.",
            "- La calidad corre antes de escribir en MongoDB. OPTIMIZE y VACUUM van en un DAG diario: eran 8 de los 11 min 39 s.",
            "- Cron descartado: sin dependencias, reintentos ni historial.",
        ], tam=12.5, despues=4),
        texto(6.83, 4.5, 6.0, 2.35, [
            "# Trino 484 + Hive Metastore (ADR 08)",
            "- Una sola capa SQL sobre Bronze, Silver y Gold, que lee el _delta_log (corrección 7).",
            "- Reglas por rol: analista y metabase solo ven Gold; cumplimiento lee las tres capas sin modificar. "
            "**10 de 10 casos.**",
            "- Alertas por SNS y una cola SQS por entidad con filtro, en lugar de FastAPI (ADR 09).",
        ], tam=12.5, despues=4),
    ]},
    {"expone": "Sebastian", "pie": True, "el": titulo("Resultados medidos en el equipo de la demo (16 GB, WSL 2)") + [
        tabla(0.5, 1.35, [3.1, 2.35, 5.28, 1.6], [
            ["Requisito", "Objetivo", "Medido", "Estado"],
            ["RF1 · fuente → Kafka", "p99 < 300 ms", "p99 de 49 a 235 ms, de 100 a 1.000 ev/s", estado("Cumple")],
            ["RNF-01 · tasa del productor", "1.000 ev/s nominales", "Los 5 escalones, de 100 a 1.000 ev/s, sin errores",
             estado("Cumple")],
            ["RNF-02 · Kafka → decisión", "p99 < 3 s", "285 s → 99 s → 53 s (a 50 ev/s)", estado("No cumple")],
            ["RF5 y RF6 · completitud", "100 %, sin duplicados", "71.910 de 71.910 en Bronze, Silver y risk_decisions",
             estado("Cumple")],
            ["R8 · paridad con el notebook", "Idéntica", "58 de 58 variables; 20.000 de 20.000 decisiones", estado("Cumple")],
            ["RF7 y RF9 · Gold con Airflow", "Cada 15 min, orquestado",
             "En Docker: hechos en Gold a los 2 min; Gold completo en 3 min 35 s. Precisión y recall 0,52",
             estado("Cumple")],
            ["RNF-06 · linaje", "< 5 min", "6 s, de Kafka a la acción de la entidad", estado("Cumple")],
            ["RF11 · acceso por rol", "Por rol", "10 de 10 casos de permisos", estado("Cumple")],
            ["R5 · memoria", "Caber en 11,7 GiB", "4,0 a 4,6 GiB sin los jobs de streaming; falta medir todo junto",
             estado("Parcial")],
            ["RNF-03, 04 y 05 · fallas", "Bróker, ejecutor, primario", "Diseñadas, no medidas", estado("Pendiente")],
        ], tam=14.5, max_h=5.5),
    ]},
    {"expone": "Samuel", "pie": True, "el": titulo("Qué falló: la latencia de la decisión, y por qué") + [
        imagen(LAT, 0.5, 1.3, 8.1, 3.5),
        texto(8.8, 1.35, 4.03, 3.3, [
            "- Variables y modelo: **< 0,5 s** por micro-batch.",
            "- El tiempo se va en **leer Kafka y validar el contrato en Python** (2,7 a 62 s) y en **MongoDB** "
            "(hasta 12,4 s).",
            "- El equipo está saturado: con los jobs corriendo, el productor vio p99 de 5,5 s hacia Kafka.",
            "- Sacar risk_decisions y las ventanas del camino crítico bajó el p99 de 285 a 99 s.",
        ], tam=12.5),
        texto(0.5, 4.88, 12.33, 0.42, ["# Otros errores encontrados y su arreglo"], tam=13),
        tabla(0.5, 5.3, [6.6, 5.73], [
            ["Error", "Arreglo"],
            ["Limitar los offsets por micro-batch los repartía entre particiones y cambiaba variables (226 de 9.204 "
             "transacciones en la prueba)", "Sin tope: el Job 2 ordena cada micro-batch por seq (D2)"],
            ["SeaweedFS se quedaba sin volúmenes y S3A reintentaba durante minutos",
             "WEED_MASTER_VOLUME_GROWTH_COPY_1=1"],
            ["docker compose --profile streaming stop apagaba también Kafka, MongoDB y S3",
             "Apagar solo los jobs, por nombre"],
        ], tam=12, max_h=1.8),
    ]},
    {"expone": "Mateo", "pie": True, "el": titulo("Qué cambiaríamos con más tiempo, y por qué") + [
        tabla(0.5, 1.4, [0.45, 5.4, 6.48], [
            ["#", "Cambio", "Por qué"],
            ["1", "Validar el contrato con expresiones de Spark (from_json y reglas en SQL) en vez de una pandas UDF "
                  "con jsonschema", "Es el tramo más lento del micro-batch: hasta 62 s"],
            ["2", "Escribir en MongoDB solo los campos que cambian; a escala, estado en el state store de Spark "
                  "particionado por cuenta", "MongoDB suma hasta 12,4 s por micro-batch; acercar el estado al cómputo "
                                             "quita viajes de red"],
            ["3", "Medir en Linux nativo o en la nube con más núcleos y declarar la tasa con 30 % de margen",
             "El portátil está saturado: hasta el productor ve 5,5 s hacia Kafka"],
            ["4", "Correr las pruebas de falla: bróker, ejecutor de Spark y elección del primario de MongoDB",
             "RNF-03, RNF-04 y RNF-05 están diseñados, pero no medidos"],
            ["5", "Gold incremental con el Change Data Feed de Silver",
             "Hoy el Job 4 compara contra todos los hechos, y eso crece con el histórico"],
            ["6", "TLS, secretos en un vault y CI con las pruebas de paridad",
             "RNF-08 solo está cubierto por los roles de Trino y credenciales locales"],
        ], tam=16, max_h=5.4),
    ]},
    {"expone": "todo el equipo", "pie": True, "el": titulo("Demo en vivo: qué vamos a mostrar (8 minutos)") + [
        texto(0.5 + i * 2.08, 1.5, 1.93, 2.75, [{"t": h, "tam": 18, "b": True, "color": ROJO, "despues": 4},
                                                {"t": n, "tam": 15, "b": True, "despues": 6},
                                                {"t": d, "tam": 13}],
              fondo=SUAVE, margen=0.13)
        for i, (h, n, d) in enumerate([
            ("0:00", "Estado de partida", "Estado inicial en MongoDB (bootstrap_state) y la primera parte del tramo ya en Gold"),
            ("0:40", "Productor en vivo", "El resto del tramo, a 50 ev/s y en orden de seq"),
            ("1:00", "Streaming", "Spark UI y log del Job 2 por micro-batch; alertas a la entidad y a los analistas"),
            ("3:45", "Airflow", "El DAG principal: sensores, dimensiones, hechos, calidad y perfiles"),
            ("6:15", "Gold", "Consulta en Trino o Metabase: alertas por canal y desempeño contra las etiquetas"),
            ("7:15", "Linaje", "Una alerta de esta corrida, de Kafka a la acción de la entidad"),
        ])] + [
        texto(0.5, 4.55, 12.33, 2.2, [
            "# En pantalla",
            "- Navegador: Spark (localhost:8080), Airflow (localhost:8085), Metabase (localhost:3000) y el repositorio en GitHub.",
            "- Terminales: productor, log del Job 2, entidad, analistas y Trino.",
            "- Si algo falla, lo explicamos con la evidencia en pantalla: qué falló, por qué y cómo se arregla.",
        ], tam=15, despues=5),
    ]},
    {"expone": "respaldo para la defensa", "pie": True, "el": titulo("Respaldo: si el volumen se multiplicara por 100") + [
        texto(0.5, 1.3, 12.33, 0.7, ["Referencia: el promedio del día récord son 120 ev/s; por 100 son **12.000 ev/s**, unos "
                                     "12 MB/s con eventos de 1 KB. Kafka no es el cuello; el procesamiento con estado sí."],
              tam=14),
        tabla(0.5, 2.05, [1.9, 3.75, 6.68], [
            ["Pieza", "Hoy", "Qué cambiaríamos (en orden)"],
            ["1 · Job 2", "Validación en Python y estado en MongoDB en cada micro-batch: el cuello medido",
             "Validación con expresiones de Spark; estado en el state store (RocksDB) particionado por cuenta; MongoDB "
             "solo para perfiles y consultas"],
            ["2 · Kafka", "6 particiones, 1 o 3 brókers",
             "48 a 96 particiones, definidas antes de producción (cambiarlas reasigna las claves); MSK en 3 zonas"],
            ["3 · MongoDB", "Replica set sin sharding", "Sharding por hash de la cuenta; escribir solo los campos que cambian"],
            ["4 · Spark", "1 worker de 5 núcleos para los 4 jobs",
             "Un clúster por job con autoescalado (EMR o Kubernetes); shuffle.partitions queda fijo en el checkpoint"],
            ["5 · Lakehouse", "risk_decisions y Gold sin particionar",
             "Particionar por fecha o usar liquid clustering; OPTIMIZE por partición"],
            ["6 · Job 4", "Anti-join de Silver contra todos los hechos", "Change Data Feed de Silver: procesar solo lo nuevo"],
            ["7 · Consumo", "Trino de un nodo", "Athena o un clúster de Trino"],
        ], tam=14, max_h=4.8),
    ]},
    {"expone": "respaldo para la defensa", "pie": True, "el": titulo("Respaldo: qué pasa si algo falla") + [
        tabla(0.5, 1.35, [2.35, 6.1, 3.88], [
            ["Falla", "Qué pasa", "En el código"],
            ["Cae un bróker de Kafka", "Perfil completo (RF=3, min.insync.replicas=2): acks=all sigue escribiendo. Con un "
                                       "bróker (dev): el productor reintenta hasta 5 min, los jobs fallan, Docker los "
                                       "reinicia y siguen desde el checkpoint",
             "publicador.py:143 · crear_topicos.sh:10-14 · docker-compose.yml:79"],
            ["Spark falla a mitad de un micro-batch", "Repite el micro-batch con los mismos offsets; MongoDB parte de "
                                                      "streaming_prev; Delta y MERGE no duplican",
             "comun.py:74-81 · almacen.py:34-80 · test_job2_microbatch.py:96"],
            ["Cae el primario de MongoDB", "Se elige otro primario (timeout de 2 s); el Job 2 espera y la latencia sube; "
                                           "no se pierde ninguna escritura confirmada",
             "iniciar_replica.js:34 · almacen.py:30"],
            ["SNS no responde", "El micro-batch falla y se repite; puede haber alertas duplicadas, que la entidad descarta",
             "job2_riesgo.py:112-128 · servicio.py:78-101"],
            ["Falla una tarea de Airflow", "Hasta 3 reintentos con retroceso exponencial, sobre la misma versión de Silver",
             "dags/breb_lakehouse_pipeline.py:43"],
            ["Llegan eventos inválidos", "Quedan en Bronze con is_valid = false; reprocess_quarantine corrige lo que "
                                         "puede y hace MERGE a Silver", "job1_bronze.py · batch/cuarentena.py:49"],
            ["Se acaba la memoria", "Docker reinicia el contenedor (restart: unless-stopped) y el job sigue desde el "
                                    "checkpoint", "topes por servicio en infra/docker-compose.yml"],
        ], tam=13.5, max_h=5.5),
    ]},
]


# ------------------------------------------------------------------ notas desde guion.md
def leer_notas() -> dict[int, str]:
    guion = (AQUI / "guion.md").read_text(encoding="utf-8")
    notas = {}
    for m in re.finditer(r"^### Lámina (\d+) · (.+?)$(.*?)(?=^### |^## |^---|\Z)", guion, re.M | re.S):
        quien = m.group(2).split("—")[-1].strip() if "—" in m.group(2) else ""
        citas = [linea[2:] for linea in m.group(3).splitlines() if linea.startswith("> ")]
        pantalla = re.search(r"\*\*En pantalla:\*\* (.+)", m.group(3))
        partes = [quien] if quien else []
        if pantalla:
            partes.append("En pantalla: " + pantalla.group(1))
        partes += citas
        notas[int(m.group(1))] = "\n\n".join(partes)
    return notas


# ------------------------------------------------------------------ HTML
def css_fuentes() -> str:
    carpeta = os.environ.get("BREB_FUENTES")
    if not carpeta:
        return ""
    reglas = []
    for archivo, peso, estilo in [("LiberationSans-Regular.ttf", 400, "normal"), ("LiberationSans-Bold.ttf", 700, "normal"),
                                  ("LiberationSans-Italic.ttf", 400, "italic")]:
        ruta = pathlib.Path(carpeta) / archivo
        if ruta.exists():
            reglas.append(f"@font-face{{font-family:'Liberation Sans';src:url('{ruta.as_uri()}');"
                          f"font-weight:{peso};font-style:{estilo};}}")
    return "".join(reglas)


def html_runs(d):
    return "".join(f"<b>{html.escape(s)}</b>" if (n or d["b"]) else html.escape(s) for s, n in tramos(d["t"]))


def html_par(d, alin):
    estilo = (f"font-size:{d['tam']}pt;color:#{d['color']};margin:0 0 {d['despues']}pt 0;line-height:{INTERLINEA};"
              f"text-align:{ {'l': 'left', 'c': 'center', 'r': 'right'}[d.get('alin') or alin] };")
    if d["nivel"]:
        sangria = SANGRIA * d["nivel"]
        estilo += f"padding-left:{sangria}in;text-indent:-{BALA}in;"
        bala = "•" if d["nivel"] == 1 else "–"
        return (f"<p style=\"{estilo}\"><span style=\"display:inline-block;width:{BALA}in;text-indent:0\">{bala}</span>"
                f"{html_runs(d)}</p>")
    return f"<p style=\"{estilo}\">{html_runs(d)}</p>"


def html_lamina(i, lam) -> str:
    partes = []
    for j, el in enumerate(lam["el"]):
        eid = f"l{i}e{j}"
        pos = f"left:{el['x']}in;top:{el['y']}in;"
        if el["tipo"] == "rect":
            partes.append(f"<div style=\"position:absolute;{pos}width:{el['w']}in;height:{el['h']}in;"
                          f"background:#{el['fondo']}\"></div>")
        elif el["tipo"] == "img":
            x, y, w, h = ajustar(el)
            partes.append(f"<img src=\"{pathlib.Path(el['ruta']).as_uri()}\" style=\"position:absolute;left:{x}in;"
                          f"top:{y}in;width:{w}in;height:{h}in\">")
        elif el["tipo"] == "texto":
            pars = "".join(html_par(normalizar(p, el["tam"], el["color"], el["despues"]), el["alin"]) for p in el["pars"])
            fondo = f"background:#{el['fondo']};" if el["fondo"] else ""
            borde = f"border:1.33px solid #{el['borde']};" if el["borde"] else ""
            centro = "display:flex;flex-direction:column;justify-content:center;" if el["anclaje"] == "m" else ""
            partes.append(f"<div id=\"{eid}\" class=\"tx\" style=\"{pos}width:{el['w']}in;height:{el['h']}in;"
                          f"padding:{el['margen']}in;{fondo}{borde}{centro}\">{pars}</div>")
        elif el["tipo"] == "tabla":
            cols = "".join(f"<col style=\"width:{a}in\">" for a in el["anchos"])
            filas = []
            for r, fila in enumerate(el["filas"]):
                tds = []
                for c in map(celda, fila):
                    enc = r == 0
                    fondo = c.get("fondo") or (NAVY if enc else (SUAVE if r % 2 == 0 else BLANCO))
                    color = c.get("color") or (BLANCO if enc else NAVY)
                    d = {"t": c["t"], "b": c.get("b", enc)}
                    tds.append(f"<td style=\"background:#{fondo};color:#{color};font-size:{el['tam']}pt\">"
                               f"{html_runs(d)}</td>")
                filas.append("<tr>" + "".join(tds) + "</tr>")
            partes.append(f"<table id=\"{eid}\" style=\"{pos}width:{sum(el['anchos'])}in\"><colgroup>{cols}</colgroup>"
                          + "".join(filas) + "</table>")
    if lam["pie"]:
        partes.append(f"<div class=\"pie\" style=\"left:0.5in\">Bre-B · detección de fraude en tiempo real · ST1630 EAFIT</div>"
                      f"<div class=\"pie\" style=\"right:0.5in;text-align:right\">{i + 1} / {len(LAMINAS)} · "
                      f"{'Expone: ' + lam['expone'] if lam['expone'][0].isupper() else lam['expone'].capitalize()}</div>")
    return f"<section class=\"lam\" id=\"s{i}\">{''.join(partes)}</section>"


def ajustar(el):
    with Image.open(el["ruta"]) as im:
        proporcion = im.width / im.height
    w, h = el["w"], el["w"] / proporcion
    if h > el["h"]:
        h, w = el["h"], el["h"] * proporcion
    return round(el["x"] + (el["w"] - w) / 2, 4), round(el["y"] + (el["h"] - h) / 2, 4), round(w, 4), round(h, 4)


def html_documento() -> str:
    estilo = (css_fuentes() + "@page{size:13.333in 7.5in;margin:0}body{margin:0}"
              ".lam{position:relative;width:13.333in;height:7.5in;overflow:hidden;page-break-after:always;"
              "background:#fff;font-family:'Liberation Sans',Arial,sans-serif;color:#232F3E}"
              ".tx{position:absolute;box-sizing:border-box;overflow:hidden}"
              "table{position:absolute;border-collapse:collapse;table-layout:fixed}"
              f"td{{border:1.33px solid #fff;padding:0.04in 0.08in;vertical-align:middle;line-height:{INTERLINEA}}}"
              ".pie{position:absolute;top:7.03in;width:6in;font-size:10pt;color:#5F6B7A}")
    cuerpo = "".join(html_lamina(i, lam) for i, lam in enumerate(LAMINAS))
    return f"<!doctype html><html lang=\"es\"><head><meta charset=\"utf-8\"><style>{estilo}</style></head><body>{cuerpo}</body></html>"


MEDIR = """() => {
  const r = {desbordes: [], filas: {}};
  document.querySelectorAll('.tx').forEach(e => {
    if (e.scrollHeight > e.clientHeight + 1 || e.scrollWidth > e.clientWidth + 1)
      r.desbordes.push([e.id, e.scrollHeight, e.clientHeight]);
  });
  document.querySelectorAll('table').forEach(t => { r.filas[t.id] = [...t.rows].map(f => f.getBoundingClientRect().height); });
  return r;
}"""


def renderizar() -> dict:
    from playwright.sync_api import sync_playwright
    vista = AQUI / "vista"
    vista.mkdir(exist_ok=True)
    archivo = vista / "slides.html"
    archivo.write_text(html_documento(), encoding="utf-8")
    with sync_playwright() as p:
        nav = p.chromium.launch()
        pag = nav.new_page(viewport={"width": 1280, "height": 720}, device_scale_factor=1.5)
        pag.goto(archivo.as_uri())
        pag.evaluate("document.fonts.ready")
        medidas = pag.evaluate(MEDIR)
        for i in range(len(LAMINAS)):
            pag.locator(f"#s{i}").screenshot(path=str(vista / f"lamina_{i + 1:02d}.png"))
        pag.pdf(path=str(AQUI / "slides.pdf"), width="13.333in", height="7.5in", print_background=True)
        nav.close()
    return medidas


# ------------------------------------------------------------------ PPTX
def construir_pptx(medidas: dict, notas: dict[int, str]) -> None:
    from lxml import etree
    from pptx import Presentation
    from pptx.dml.color import RGBColor
    from pptx.enum.shapes import MSO_SHAPE
    from pptx.enum.text import MSO_ANCHOR, MSO_AUTO_SIZE, PP_ALIGN
    from pptx.oxml.ns import qn
    from pptx.util import Inches, Pt

    alineacion = {"l": PP_ALIGN.LEFT, "c": PP_ALIGN.CENTER, "r": PP_ALIGN.RIGHT}

    def rgb(h):
        return RGBColor.from_string(h)

    def escribir(tf, pars, alin):
        for k, d in enumerate(pars):
            p = tf.paragraphs[0] if k == 0 else tf.add_paragraph()
            p.alignment = alineacion[d.get("alin") or alin]
            p.line_spacing = 1.0
            p.space_before = Pt(0)
            p.space_after = Pt(d["despues"])
            ppr = p._p.get_or_add_pPr()
            if d["nivel"]:
                ppr.set("marL", str(int(Inches(SANGRIA * d["nivel"]))))
                ppr.set("indent", str(-int(Inches(BALA))))
                etree.SubElement(ppr, qn("a:buFont")).set("typeface", "Arial")
                etree.SubElement(ppr, qn("a:buChar")).set("char", "•" if d["nivel"] == 1 else "–")
            else:
                etree.SubElement(ppr, qn("a:buNone"))
            for s, negrita in tramos(d["t"]):
                r = p.add_run()
                r.text = s
                r.font.name = "Arial"
                r.font.size = Pt(d["tam"])
                r.font.bold = negrita or d["b"]
                r.font.color.rgb = rgb(d["color"])

    prs = Presentation()
    prs.slide_width, prs.slide_height = Inches(ANCHO), Inches(ALTO)
    for i, lam in enumerate(LAMINAS):
        s = prs.slides.add_slide(prs.slide_layouts[6])
        for j, el in enumerate(lam["el"]):
            eid = f"l{i}e{j}"
            if el["tipo"] == "rect":
                sh = s.shapes.add_shape(MSO_SHAPE.RECTANGLE, Inches(el["x"]), Inches(el["y"]), Inches(el["w"]),
                                        Inches(el["h"]))
                sh.fill.solid()
                sh.fill.fore_color.rgb = rgb(el["fondo"])
                sh.line.fill.background()
                sh.shadow.inherit = False
            elif el["tipo"] == "img":
                x, y, w, h = ajustar(el)
                s.shapes.add_picture(el["ruta"], Inches(x), Inches(y), Inches(w), Inches(h))
            elif el["tipo"] == "texto":
                caja = (Inches(el["x"]), Inches(el["y"]), Inches(el["w"]), Inches(el["h"]))
                if el["fondo"] or el["borde"]:
                    sh = s.shapes.add_shape(MSO_SHAPE.RECTANGLE, *caja)
                    sh.shadow.inherit = False
                    if el["fondo"]:
                        sh.fill.solid()
                        sh.fill.fore_color.rgb = rgb(el["fondo"])
                    else:
                        sh.fill.background()
                    if el["borde"]:
                        sh.line.color.rgb = rgb(el["borde"])
                        sh.line.width = Pt(1)
                    else:
                        sh.line.fill.background()
                else:
                    sh = s.shapes.add_textbox(*caja)
                tf = sh.text_frame
                tf.word_wrap = True
                tf.auto_size = MSO_AUTO_SIZE.NONE
                tf.margin_left = tf.margin_right = tf.margin_top = tf.margin_bottom = Inches(el["margen"])
                tf.vertical_anchor = MSO_ANCHOR.MIDDLE if el["anclaje"] == "m" else MSO_ANCHOR.TOP
                escribir(tf, [normalizar(p, el["tam"], el["color"], el["despues"]) for p in el["pars"]], el["alin"])
            elif el["tipo"] == "tabla":
                altos = [h / 96 for h in medidas["filas"][eid]]
                nf, nc = len(el["filas"]), len(el["anchos"])
                gf = s.shapes.add_table(nf, nc, Inches(el["x"]), Inches(el["y"]), Inches(sum(el["anchos"])),
                                        Inches(sum(altos)))
                t = gf.table
                t.first_row, t.horz_banding = True, False
                for c, a in enumerate(el["anchos"]):
                    t.columns[c].width = Inches(a)
                for r, fila in enumerate(el["filas"]):
                    t.rows[r].height = Inches(altos[r])
                    for c, contenido in enumerate(map(celda, fila)):
                        enc = r == 0
                        cel = t.cell(r, c)
                        cel.fill.solid()
                        cel.fill.fore_color.rgb = rgb(contenido.get("fondo") or (NAVY if enc else (SUAVE if r % 2 == 0
                                                                                                     else BLANCO)))
                        cel.margin_left = cel.margin_right = Inches(0.08)
                        cel.margin_top = cel.margin_bottom = Inches(0.04)
                        cel.vertical_anchor = MSO_ANCHOR.MIDDLE
                        d = normalizar({"t": contenido["t"], "b": contenido.get("b", enc), "despues": 0,
                                        "color": contenido.get("color") or (BLANCO if enc else NAVY)},
                                       el["tam"], NAVY, 0)
                        escribir(cel.text_frame, [d], "l")
        if lam["pie"]:
            quien = "Expone: " + lam["expone"] if lam["expone"][0].isupper() else lam["expone"].capitalize()
            for x, txt, alin in [(0.5, "Bre-B · detección de fraude en tiempo real · ST1630 EAFIT", "l"),
                                 (ANCHO - 6.5, f"{i + 1} / {len(LAMINAS)} · {quien}", "r")]:
                tb = s.shapes.add_textbox(Inches(x), Inches(7.03), Inches(6), Inches(0.3))
                tf = tb.text_frame
                tf.margin_left = tf.margin_right = tf.margin_top = tf.margin_bottom = 0
                escribir(tf, [normalizar({"t": txt, "despues": 0}, 10, GRIS, 0)], alin)
        if notas.get(i + 1):
            s.notes_slide.notes_text_frame.text = notas[i + 1]
    prs.core_properties.title = "Bre-B · detección de fraude en tiempo real · sustentación del Entregable 2"
    prs.core_properties.author = "Equipo ST1630"
    prs.save(AQUI / "slides.pptx")


def main() -> int:
    if len(LAMINAS) > 15:
        print("más de 15 láminas")
        return 1
    notas = leer_notas()
    medidas = renderizar()
    problemas = [f"texto que no cabe: {e} ({alto} px de {visible})" for e, alto, visible in medidas["desbordes"]]
    for i, lam in enumerate(LAMINAS):
        for j, el in enumerate(lam["el"]):
            if el["tipo"] == "tabla":
                alto = sum(medidas["filas"][f"l{i}e{j}"]) / 96
                if el["max_h"] and alto > el["max_h"] + 0.01:
                    problemas.append(f"tabla l{i}e{j} (lámina {i + 1}): {alto:.2f} in de {el['max_h']} in")
                print(f"lámina {i + 1}: tabla de {alto:.2f} in, de {el['y']:.2f} a {el['y'] + alto:.2f}")
    sin_notas = [n for n in range(1, len(LAMINAS) + 1) if n not in notas]
    construir_pptx(medidas, notas)
    print(f"{len(LAMINAS)} láminas; sin notas: {sin_notas or 'ninguna'}")
    for p in problemas:
        print("PROBLEMA:", p)
    print("slides.pptx y slides.pdf listos" if not problemas else f"{len(problemas)} problemas")
    return 1 if problemas else 0


if __name__ == "__main__":
    sys.exit(main())
