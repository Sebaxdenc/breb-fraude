# Guía de la demo en vivo (8 minutos)

La regla del curso permite tener datos precargados, pero el procesamiento tiene que ocurrir frente al profesor. Por eso la demo se divide así:
- **Antes de la clase:** se carga el estado inicial de MongoDB con `bootstrap_state` y se procesa la **primera parte** del tramo de la demo hasta Gold.
- **En vivo:** el productor publica la **segunda parte** del tramo; Spark la valida, la puntúa y alerta; Airflow la lleva a Gold; al final se consulta Gold y el linaje de una alerta.

Todos los comandos son de PowerShell y se corren **desde la raíz del repositorio** (`breb-fraude`). El `docker-compose.yml` de la raíz incluye `infra/docker-compose.yml`, así que también funcionan desde `infra/`.

Nathalia opera la terminal y el navegador durante toda la demo; los demás narran (ver `guion.md`, bloque 2).

> **Airflow ya corrió en Docker:** la primera corrida de `breb_lakehouse_pipeline` (paso P10) terminó en `success` en **11 min 39 s** (2026-11-10, de 03:21 a 03:32 UTC). Una corrida así no cabe en los 8 minutos de la demo, así que el ensayo tiene que medir cuánto tarda la corrida en vivo hasta `silver_to_gold_facts` (sección 1, "Tiempos de Airflow"). El ensayo del día anterior sigue siendo obligatorio, y sus tiempos reemplazan los estimados de esta guía.

---

## 0. Una vez, antes del primer ensayo
1. Fusionar los PR pendientes y actualizar: `git switch main`, `git pull`.
2. Construir las imágenes (la de Airflow tarda varios minutos la primera vez):
   ```powershell
   docker compose --profile consumo --profile orquestacion --profile herramientas build
   docker compose --profile consumo --profile orquestacion --profile herramientas --profile streaming pull
   ```
3. Comprobar que `datos/productor/` tiene `eventos_test.parquet`, `etiquetas_test.parquet`, `scores_test.parquet` y `tramo_demo.json`. Si falta alguno: `docker compose run --rm productor preparar` y `docker compose run --rm spark-cliente python -m breb.estado tramo` (README, "Demo con alertas").
4. `docker info | Select-String "CPUs|Total Memory"` debe mostrar 12 CPUs y unos 11,7 GiB.

## 1. Ensayo del día anterior: plan A o plan B
Hagan la demo completa, igual que en la clase, y anoten `docker stats --no-stream` en tres momentos: con el productor publicando, con el pipeline de Airflow corriendo y al consultar Gold.

| Momento | Memoria total (suma de `docker stats`) | ¿Algún contenedor se reinició? |
|---|---|---|
| Productor publicando | | |
| Pipeline de Airflow | | |
| Consulta a Gold | | |

- **Plan A**, si el total se mantiene por debajo de unos 10 GiB y nada se reinicia: todo queda arriba durante la demo, incluido Metabase.
- **Plan B**, si no: Metabase se apaga antes de la clase y la consulta a Gold se hace con la terminal de Trino; los jobs de streaming se apagan justo antes de correr el pipeline (paso D5).

### Tiempos de Airflow
La corrida completa es larga porque las 8 tareas van en serie (`max_active_tasks=1`) y cada tarea de Spark es un `spark-submit` nuevo en `local[2]` dentro del contenedor de Airflow. La primera corrida, además, carga las etiquetas de las 1.038.880 transacciones del test y hace OPTIMIZE y VACUUM de todas las tablas. Para la consulta a Gold (D7) basta con que termine `silver_to_gold_facts`: calidad, perfiles y mantenimiento pueden seguir corriendo mientras tanto.

Después de cada corrida, la duración de cada tarea sale con:
```powershell
docker compose exec airflow airflow dags list-runs breb_lakehouse_pipeline     # copiar el run_id
docker compose exec airflow airflow tasks states-for-dag-run breb_lakehouse_pipeline "<run_id>"
```

| Corrida | Desde que arranca hasta `silver_to_gold_facts` en verde | Total |
|---|---|---|
| Primera (P10, primera parte del tramo) | | 11 min 39 s (2026-11-10) |
| En vivo (D6, segunda parte del tramo) | | |

- Si la corrida en vivo tiene `silver_to_gold_facts` en verde en 2 minutos o menos, sirve el orden del guion: D6 en 3:45 y D7 en 6:15.
- Si tarda más, usen el **orden B**: D6 en 3:45, después D8 (linaje) y al final D7 (consulta a Gold), cuando `silver_to_gold_facts` esté en verde. En D8, el paso 4 del linaje (Gold) sale "(sin registros)" hasta que termine `silver_to_gold_facts`; se explica así: "Bronze, Silver y la decisión ya están; el hecho llega a Gold cuando termine esta corrida".
- Si al minuto 7:15 `silver_to_gold_facts` todavía no termina, D7 consulta Gold con la primera parte del tramo y se dice cuánto tardó la corrida en el ensayo. La salida de `docker compose exec airflow airflow tasks states-for-dag-run` es la evidencia.

Las dos salidas de la terminal que confunden, y no son fallas:
- `Could not import graphviz`: es un aviso de la CLI de Airflow; no afecta las corridas.
- `airflow dags unpause` puede imprimir `is_paused | True` aunque sí quitó la pausa. Se comprueba con `airflow dags list-runs breb_lakehouse_pipeline` o en la interfaz: la corrida aparece en segundos.

## 2. Preparación: 60 minutos antes de la clase
| Paso | Qué hacer | Resultado esperado |
|---|---|---|
| P1 | Abrir Docker Desktop. | `docker info` responde. |
| P2 | Empezar de cero: `docker compose down -v` | Borra tópicos, MongoDB, lakehouse, checkpoints y los metastores de Hive, Metabase y Airflow. `datos/` no se toca. |
| P3 | `docker compose up -d` y luego `docker compose --profile orquestacion up -d` | En 2 o 3 minutos, `docker compose ps -a` muestra `kafka-init`, `mongo-init`, `init-recursos` y `postgres-bases` en `Exited (0)`, y `airflow` en `healthy`. |
| P4 | Estado inicial con Airflow: `docker compose exec airflow airflow dags trigger bootstrap_state` | De 5 a 7 minutos. `docker compose exec airflow airflow dags list-runs bootstrap_state` termina en `success`. Queda en el historial de Airflow para mostrarlo en la demo. |
| P5 | Elegir el corte: `docker compose run --rm spark-cliente python /opt/breb/herramientas/streaming/dividir_tramo.py --paso 500` | Una tabla con las alertas antes y después de cada corte. Elijan **K** para que la parte en vivo tenga entre 5.000 y 7.500 transacciones y al menos 8 alertas. Anoten K y 15.000 − K. |
| P6 | Streaming: `docker compose --profile streaming up -d` | En http://localhost:8080 aparecen 4 aplicaciones en RUNNING: job1, job2-riesgo, job2-ventanas y job3. |
| P7 | Consumidores de alertas, cada uno en su propia ventana de PowerShell: `docker compose run --rm entidad --tramo` y `docker compose run --rm entidad --analistas` | "escuchando sus colas SQS" y "esperando alertas". Tienen que estar arriba **antes** de publicar: cada cola se crea al arrancar su servicio. |
| P8 | Primera parte del tramo: `docker compose run --rm productor publicar --tramo --max K --escalones "100:200"` (con K = 7500, unos 75 s) | Termina en `PUBLICACIÓN OK` y deja `datos/productor/progreso.json` con el último seq. |
| P9 | Esperar a que los jobs terminen. Repetir cada 30 s: `docker compose logs --since 40s job2-riesgo job3-silver \| Select-String "batch"` | Cuando no imprime nada, terminaron: los jobs solo escriben una línea `batch` si el micro-batch trajo filas. Unos 2 o 3 minutos. |
| P10 | Primera corrida de Gold. Apagar los jobs para liberar memoria: `docker compose stop job1-bronze job2-riesgo job2-ventanas job3-silver`. Luego `docker compose exec airflow airflow dags unpause breb_lakehouse_pipeline` | Arranca una corrida en segundos. `docker compose exec airflow airflow dags list-runs breb_lakehouse_pipeline` termina en `success` (la primera vez tardó 11 min 39 s). Anoten los tiempos por tarea ("Tiempos de Airflow"). |
| P11 | Pausar el pipeline para que no corra durante la presentación: `docker compose exec airflow airflow dags pause breb_lakehouse_pipeline`. Volver a subir los jobs: `docker compose --profile streaming up -d` | Los 4 jobs siguen desde su checkpoint, sin datos nuevos. |
| P12 | Consumo: `docker compose --profile consumo up -d`, después `docker compose --profile consumo run --rm spark-cliente python -m breb.consumo registrar` y `docker compose --profile consumo run --rm spark-cliente python -m breb.consumo tablero` | `registrar` lista las tablas; `tablero` imprime el enlace del tablero, por ejemplo http://localhost:3000/dashboard/1. |
| P13 | Solo en el plan B: `docker compose stop metabase` | Trino y el Hive Metastore siguen arriba. |
| P14 | Cinco minutos antes: cerrar con Ctrl+C las dos ventanas de consumidores y volver a abrirlas (P7) para que la pantalla quede limpia. | Las colas ya existen; las alertas nuevas aparecen ahí. |

Ventanas listas al empezar la demo:
- **Navegador:** Spark (http://localhost:8080), Airflow (http://localhost:8085, usuario `admin` y la clave `AIRFLOW_ADMIN_CLAVE` de `infra/credenciales/airflow.env`), Metabase con el tablero (plan A) y el repositorio en GitHub.
- **PowerShell 1 (operación):** en la raíz del repositorio, con el comando del productor escrito y sin ejecutar.
- **PowerShell 2:** log del Job 2 con `docker compose logs -f --tail 0 job2-riesgo | Select-String "\[job2\] batch"`.
- **PowerShell 3 y 4:** entidad y analistas (P7).
- Letra de la terminal grande (Ctrl + rueda del ratón) para que se lea en el proyector.

---

## 3. Durante la demo
Los minutos son los del guion. N = 15.000 − K.

### D1 · 0:00 · Estado de partida
- Airflow, lista de DAG: `bootstrap_state` con su corrida en verde y `breb_lakehouse_pipeline` con una corrida en verde y en pausa.
- Spark: las 4 aplicaciones en RUNNING y los núcleos que usa cada una (1, 2, 1 y 1).
- Plan A: el tablero de Metabase muestra los hechos de la primera parte.

### D2 · 0:40 · Productor en vivo
```powershell
docker compose run --rm productor publicar --continuar --max N --escalones "50:200"
```
Salida esperada: `continuando desde seq …`, `escalón 1/1: 50 ev/s durante 200 s` y, al final, `no quedan transacciones por publicar`, la tabla del RF1 y `PUBLICACIÓN OK`. Con N = 7.500 tarda unos 150 s.

**No usar `--tramo` junto con `--continuar` sin `--max`:** el productor publicaría 15.000 transacciones desde la mitad del tramo.

### D3 · 1:00 · Streaming
- PowerShell 2: cada línea `[job2] batch` dice cuántas transacciones y alertas tuvo el micro-batch, su latencia p50 y p99, y cuánto tardó cada tramo (lectura de Kafka, MongoDB, variables, modelo, SNS).
- Spark: los ejecutores trabajando en el worker.
- La latencia de la decisión va a estar en decenas de segundos (D6, lámina 11). No es una falla de la demo: se explica con los tramos del log.

### D4 · 2:00 · Alertas
- PowerShell 3: `[entidad N] alerta 1a2b3c4d score 0.9xx (umbral 0.172): fondos retenidos en la cuenta destino …`.
- PowerShell 4: `ALERTA <transaction_uuid completo> | …`. **Copien uno** para el paso D8.

### D5 · 3:15 · El productor termina y los jobs se ponen al día
Los jobs terminaron cuando este comando no imprime nada (solo escriben una línea `batch` si el micro-batch trajo filas):
```powershell
docker compose logs --since 40s job2-riesgo job3-silver | Select-String "batch"
```
Para mostrar que no se perdió nada, con Trino (las tres cifras deben ser 15.000, o K + N):
```powershell
docker compose --profile consumo exec trino trino --user cumplimiento --catalog lakehouse --execute "SELECT (SELECT count(*) FROM bronze.transacciones) AS bronze, (SELECT count(*) FROM silver.transacciones) AS silver, (SELECT count(*) FROM gold.risk_decisions) AS decisiones"
```

Plan B, después de eso:
```powershell
docker compose stop job1-bronze job2-riesgo job2-ventanas job3-silver
```
Qué decir: "Apagamos el streaming para liberar memoria del portátil de 16 GB; en AWS, el batch corre en su propio clúster".

### D6 · 3:45 · Airflow
En la lista de DAG, quitar la pausa de `breb_lakehouse_pipeline` (interruptor de la izquierda), o:
```powershell
docker compose exec airflow airflow dags unpause breb_lakehouse_pipeline
```
Con `catchup=False`, la corrida arranca en segundos. Si a los 30 s no aparece, botón **Trigger**. Si la terminal imprime `is_paused | True` o el aviso de graphviz, sigan: no son fallas (sección 1, "Tiempos de Airflow").

Abrir la corrida en la vista **Graph** y mostrar:
- el log de `check_new_silver_data` ("Silver: versión actual X, última versión procesada en Gold Y");
- cuando termine, el log de `data_quality_checks`, con los cuatro controles, la precisión y el recall.

Si en el ensayo `silver_to_gold_facts` tardó más de 2 minutos, sigan con el **orden B**: D8 ahora y D7 al final.

### D7 · 6:15 · Consulta a Gold
Plan A: recargar el tablero de Metabase y mostrar "4. Tasa de alertas por canal" y "Desempeño del modelo frente a las etiquetas reales".

Plan A o B, con Trino (pregunta 4 del Entregable 1):
```powershell
docker compose --profile consumo exec trino trino --user analista --catalog lakehouse --schema gold --execute "SELECT c.canal, count(*) AS transacciones, count_if(f.decision = 'alerta') AS alertas, round(100.0 * count_if(f.decision = 'alerta') / count(*), 3) AS pct_alertas FROM fact_transferencias f JOIN dim_canal c ON c.canal_key = f.canal_key GROUP BY c.canal ORDER BY pct_alertas DESC"
```
Desempeño contra las etiquetas reales, que nunca viajaron en el evento:
```powershell
docker compose --profile consumo exec trino trino --user analista --catalog lakehouse --schema gold --execute "SELECT count(*) AS hechos, count_if(decision = 'alerta') AS alertas, count_if(decision = 'alerta' AND es_lavado = 1) AS alertas_correctas, count_if(es_lavado = 1) AS lavado_real FROM fact_transferencias"
```
Si sobra tiempo, mostrar que el analista no puede leer Silver (la respuesta es "Access Denied"):
```powershell
docker compose --profile consumo exec trino trino --user analista --catalog lakehouse --execute "SELECT count(*) FROM silver.transacciones"
```

### D8 · 7:15 · Linaje de una alerta de esta corrida
```powershell
docker compose --profile consumo run --rm spark-cliente python -m breb.consumo linaje <transaction_uuid copiado en D4>
```
Muestra, como cumplimiento: la coordenada en Kafka y la hora de ingesta en Bronze, la fila de Silver, la decisión con score, umbral y versión del modelo, el hecho en Gold con el nivel de riesgo de las cuentas y la acción de la entidad en MongoDB. Sin UUID, toma la alerta con mayor score.

### Al terminar
```powershell
docker compose exec airflow airflow dags pause breb_lakehouse_pipeline
```
Dejen el stack arriba: en la defensa el profesor puede pedir que muestren algo.

---

## 4. Si algo falla
Quien está hablando dice qué falló, por qué y cómo se arregla. Primer diagnóstico para todo: `docker compose ps -a` y `docker compose logs --tail 30 <servicio>`.

| Síntoma | Causa probable | Qué decir y qué hacer |
|---|---|---|
| El productor imprime `error de Kafka` o se queda sin confirmar mensajes | El bróker no responde (contenedor caído o sin memoria) | "Con acks=all, el productor no da un mensaje por enviado hasta que el bróker lo confirma; reintenta hasta 5 minutos y `progreso.json` solo avanza con lo confirmado". `docker compose up -d kafka-1` y repetir D2 con `--continuar`: no se duplica nada, porque el Job 2 deduplica por transaction_uuid y Silver hace MERGE. |
| Un job de streaming aparece como `Restarting` | Se quedó sin memoria o perdió a Kafka o MongoDB | "La consulta falla, Docker la reinicia (`restart: unless-stopped`) y sigue desde el checkpoint; el micro-batch se repite con los mismos offsets y los efectos son idempotentes". Esperar 1 minuto. Si se repite, apagar `job2-ventanas` (no es necesario para la demo) o Metabase. |
| Las alertas llegan con decenas de segundos de atraso | La latencia medida (D6): lectura y validación de Kafka en Python, y MongoDB | Mostrar los tramos en el log `[job2] batch`. "Variables y modelo tardan milisegundos; el resto es acceso a datos en un portátil saturado. La lámina 12 dice qué cambiaríamos". |
| La ventana de la entidad no recibe nada, pero la de analistas sí | La cola de esa entidad se creó después de publicar, o la alerta es de otra entidad | `entidad --tramo` escucha las entidades de las alertas del tramo. "SNS filtra por el atributo `entidades`; la cola de analistas no tiene filtro". |
| `check_streaming_health` falla | Un job activo va más de 50.000 offsets atrasado | "Es el comportamiento diseñado: no se arma Gold sobre un streaming atrasado". Esperar a que se ponga al día, o apagar el streaming (D5) y usar **Clear** en la tarea. |
| `check_new_silver_data` queda en `skipped` | Silver no tiene una versión nueva desde la última corrida: Job 3 apagado antes de terminar, o ya se procesó | `docker compose logs --tail 5 job3-silver`. Si Job 3 no terminó, `docker compose up -d job3-silver`, esperar y volver a disparar. |
| `silver_to_gold_*` falla con código -9 o 137 | Memoria del contenedor de Airflow o de WSL | Apagar el streaming (D5) y usar **Clear** en la tarea: el reintento lee la misma versión de Silver, así que no mezcla datos. |
| `data_quality_checks` falla | Uno de los cuatro controles no se cumple (el log dice cuál) | "Por diseño, MongoDB no se toca si la calidad falla". Leer el control en el log y explicarlo. |
| La corrida queda en `queued` | El DAG está en pausa o hay otra corrida activa (`max_active_runs=1`) | Quitar la pausa o esperar a la corrida anterior. |
| Trino responde `Table … does not exist` | No se registraron las tablas después de `down -v` | `docker compose --profile consumo run --rm spark-cliente python -m breb.consumo registrar` |
| Metabase no carga | Tarda unos 2 minutos en arrancar, o está apagado (plan B) | Usar las consultas de Trino de D7. |
| El productor dice `NO HABÍA TRANSACCIONES POR PUBLICAR` | `progreso.json` ya está al final del tramo | Revisar el `último seq publicado` del informe. Para repetir la parte en vivo hay que rehacer la preparación desde P2. |
| El portátil se congela | WSL se quedó sin memoria (riesgo R5) | "El stack completo pide más RAM que la del portátil; en AWS cada pieza corre en su propio servicio". Apagar Metabase y el streaming, y seguir con Gold y el linaje. |

---

## 5. Si el profesor pide ver algo puntual
| Pide | Comando o lugar |
|---|---|
| Los offsets del tópico | `docker compose exec kafka-1 /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic breb.transacciones.raw` |
| La cuarentena en vivo | Con `job1-bronze` arriba: `docker compose run --rm productor publicar --solo-invalidos 20`. Al minuto: `docker compose --profile consumo exec trino trino --user cumplimiento --catalog lakehouse --execute "SELECT count(*) FROM bronze.transacciones WHERE NOT is_valid"`. Después: `docker compose exec airflow airflow dags trigger reprocess_quarantine`. |
| El documento de una cuenta en MongoDB | `docker compose exec mongo-1 mongosh --quiet -u breb_app -p <MONGO_APP_CLAVE de infra/credenciales/mongo-app.env> --authenticationDatabase admin breb` y dentro: `db.cuentas.findOne({estado_retencion: {$exists: true}}, {streaming_prev: 0})` |
| Los permisos por rol | `docker compose --profile consumo run --rm spark-cliente python -m breb.consumo permisos` (termina en `PERMISOS OK`) |
| El código de algo | Los enlaces de `preguntas_defensa.md`, sección "Dónde está cada cosa". |
| La salud del streaming | Pregunta "Salud del streaming" del tablero, o el log de `check_streaming_health` en Airflow. |

El usuario de MongoDB es el de `MONGO_APP_USUARIO` en `infra/credenciales/mongo-app.env`; si es distinto de `breb_app`, usen ese.
