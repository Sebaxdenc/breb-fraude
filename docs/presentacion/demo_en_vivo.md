# Demo en AWS: cómo prepararla y grabarla

Esta guía lleva de "todo apagado" a un video de 8 minutos que sigue la diapositiva 14: consola de AWS, productor, streaming y alertas, Airflow, Athena y Metabase, y linaje. Lo que se dice en cada parte está en [guion.md, Bloque 2](guion.md#bloque-2--demo-en-aws-8-min).

Para no copiar y pegar comandos hay dos piezas:
- **`herramientas/aws/abrir_demo.ps1`**, en el portátil. Pide la IP de la EC2, abre las pestañas de PowerShell ya conectadas y copia los scripts de la demo en la instancia.
- **Los scripts de `~/demo/` en la EC2.** Cada uno muestra en cian el comando real de `docker compose` antes de correrlo, así que en el video se ve qué se ejecuta, y se corre escribiendo solo `demo/3_productor.sh`.

---

## 1. Estrategia

- **Se graba por escenas, no de corrido.** Hay esperas reales: el Job 2 tarda en ponerse al día y Gold tarda unos 5 minutos. Se pausa la grabación mientras se espera y se une todo al editar. El orden del video no tiene que ser el orden real.
- **Primero la imagen, después la voz.** Graben la pantalla sin hablar y, al editar, cada integrante pone su voz en su parte con el texto del guion. Si algo sale mal, se repite la escena sin regrabar la voz.
- **Todo lo que se pueda repetir, se repite.** El productor siempre sigue desde la última transacción publicada (`--continuar`), así que repetir el streaming solo agrega transacciones nuevas. Athena, Metabase y el linaje se pueden grabar las veces que quieran.
- **Lo que no se muestra:** la salida de `demo/credenciales.sh` y el archivo `labsuser.pem`.
- **Legibilidad:**
  - Grabar en 1920×1080.
  - Letra de la terminal más grande (Ctrl + +).
  - Zoom del navegador al 110 o 125 %.
  - Barra de favoritos oculta.
  - Notificaciones de Windows en "No molestar".
  - Una sola ventana a la vez, maximizada.

### Horario sugerido para hoy

| Hora | Qué |
|---|---|
| 11:30 | Prender el laboratorio y la instancia (parte 2) |
| 11:45 | Pestañas, stack y navegador; ensayo (partes 3 a 6) |
| 12:15 | Grabar las escenas (parte 7) |
| 13:00 | Colchón para repetir escenas; luego apagar todo (parte 10) |
| 13:30 | Editar y poner las voces (parte 8) |
| 16:00 | Exportar y subir el video. Límite: 5 PM |

La sesión del Learner Lab dura unas 4 horas. Si el contador baja de 30 minutos, **Start Lab** otra vez lo reinicia sin apagar nada.

---

## 2. Prender todo, desde cero

1. **Laboratorio.**
   - AWS Academy → el curso → **Modules** → **Launch AWS Academy Learner Lab** → **Start Lab**.
   - Cuando el círculo quede en verde, **AWS** abre la consola.
   - Arriba a la derecha, la región debe ser **N. Virginia (us-east-1)**.
2. **Instancia.**
   - **EC2** → **Instances** → `breb-mvp` → **Instance state** → **Start**.
   - Esperen a que diga *Running* y *2/2 checks passed*, unos 2 minutos.
   - Copien la **Public IPv4 address**: cambia cada vez que se prende la instancia.
3. **Si cambiaron de red** (otra casa, la universidad), actualicen la regla de SSH: la instancia → **Security** → el security group → **Edit inbound rules** → en la regla SSH, **Source: My IP** → **Save rules**.
4. **En el portátil, apaguen el stack local de Docker** si está prendido: usa los mismos puertos que el túnel (8080, 8085, 8090 y 3000).

---

## 3. Las pestañas de PowerShell

### Abrirlas
Guarden `abrir_demo.ps1` en Descargas y, en una ventana de PowerShell (mejor dentro de Windows Terminal):
```powershell
powershell -ExecutionPolicy Bypass -File $HOME\Downloads\abrir_demo.ps1
```
El script:
1. **Pide la IP** de la EC2.
2. **Escribe `C:\breb\ssh.conf`.** Con eso, `ssh -F C:\breb\ssh.conf breb` entra a la instancia sin repetir IP, usuario ni llave.
3. **Prueba la conexión y copia los scripts** a `~/demo/` en la EC2.
4. **Abre las pestañas A-comandos y E-tunel.**
5. **Espera a que corran `demo/0_prender.sh`** en la pestaña A. Cuando termine, presionan Enter en la ventana del script y abre B-job2, C-entidad y D-analistas.

`-ExecutionPolicy Bypass` solo afecta esta ejecución: Windows no deja correr scripts descargados por defecto. Si sin querer cierran B, C o D, ábranlas otra vez con:
```powershell
powershell -ExecutionPolicy Bypass -File $HOME\Downloads\abrir_demo.ps1 -SoloBCD
```
Sin Windows Terminal, el script abre ventanas de PowerShell separadas en lugar de pestañas.

### Qué muestra cada pestaña

| Pestaña | Qué corre | Qué muestra y por qué importa |
|---|---|---|
| **A-comandos** | Una terminal en la EC2 | Donde se corren los scripts de la demo, en orden. Cada script imprime primero el comando de `docker compose` en cian. |
| **B-job2** | `docker compose logs -f job2-riesgo \| grep "[job2] batch"` | Una línea por micro-batch del Job 2, con cuántas transacciones decidió, cuántas alertas, la latencia y cuánto tardó cada tramo. Es la prueba de que el modelo decide en milisegundos y de que el cuello es leer y validar. Espera sola a que el job arranque. |
| **C-entidad** | `docker compose run --rm entidad --tramo` | El servicio de las entidades financieras que tienen alertas en el tramo. Lee sus colas SQS y, por cada alerta, retiene los fondos o bloquea la cuenta. Si SNS repite un mensaje, lo descarta. |
| **D-analistas** | `docker compose run --rm entidad --analistas` | La bandeja de los analistas de fraude (cola `breb-analistas-fraude`): recibe todas las alertas, con monto, cuentas, score y contexto. |
| **E-tunel** | `ssh -N breb-tunel` | No imprime nada y se deja abierta. Lleva los puertos 8080, 8085, 8090 y 3000 del portátil a la EC2, donde Spark, Airflow, Trino y Metabase solo escuchan en `127.0.0.1`. Así no hay que abrir puertos a internet. |

Una línea de la pestaña B, explicada:
```
[job2] batch 812: 480 transacciones, 1 alertas, latencia p50 40213 ms, p99 86020 ms | lectura 29204 ms, MongoDB lectura 610 ms, variables 150 ms, modelo 98 ms, MongoDB escritura 492 ms, SNS 201 ms, archivo 31 ms
```
- **Número del micro-batch:** es el `batchId` de Spark; el estado de MongoDB lo usa para que un reintento no duplique nada.
- **Transacciones y alertas:** cuántas decidió este micro-batch y cuántas superaron el umbral de 0,172.
- **Latencia p50 y p99:** desde que Kafka escribió el evento hasta la decisión (RNF-02).
- **Lectura:** leer de Kafka y validar el contrato con Python. Es el cuello: 29 de 31 s.
- **MongoDB lectura y escritura:** el estado de cuentas, pares y bancos.
- **Variables y modelo:** las 58 variables y el XGBoost: milisegundos.
- **SNS:** publicar las alertas.
- **Archivo:** dejar las decisiones para `risk_decisions`.

### Los scripts de la pestaña A
Todos están en `~/demo/` en la EC2. Para estudiar: la columna del medio es lo que realmente se ejecuta.

| Script | Comando real | Qué hace |
|---|---|---|
| `demo/0_prender.sh` | `sudo swapon /swapfile`; `docker compose up -d`; `docker compose --profile orquestacion up -d`; `docker compose --profile consumo up -d` | Activa los 8 GB de swap (no quedan activos al reiniciar). Levanta los servicios base (Kafka, MongoDB, el master y el worker de Spark, y el contenedor que crea los recursos de AWS si faltan), Airflow con su PostgreSQL, y el consumo (Hive Metastore, Trino y Metabase). Espera a que Airflow, Trino y Metabase queden *healthy* y muestra qué corre. |
| `demo/estado.sh` | `docker compose --profile "*" ps` | Lista los contenedores que corren. No aparece ningún emulador (`seaweedfs`, `dynamodb-local`, `moto`): S3, DynamoDB, SNS y SQS son los de AWS. |
| `demo/credenciales.sh` | `grep` sobre `infra/credenciales/` | Usuario y clave de Airflow y de Metabase. **No se graba.** |
| `demo/1_streaming.sh` | `docker compose --profile consumo stop trino metabase hive-metastore`; `docker compose --profile streaming up -d` | Apaga el consumo para dejarle memoria y CPU al streaming. Con 2 vCPU no corren los dos a la vez. Prende los cuatro jobs: `job1-bronze`, `job2-riesgo`, `job2-ventanas` y `job3-silver`. Cada uno sigue desde su checkpoint, así que no reprocesa lo de ayer. |
| `demo/2_alertas_esperadas.sh [N]` | `docker compose run productor python …` | Con los scores del notebook, dice cuántas alertas van a salir si se publican las siguientes N transacciones. Sirve para elegir un N con al menos 2 alertas. |
| `demo/3_productor.sh [N]` | `docker compose run --rm productor publicar --continuar --max N --escalones "25:S"` | Publica en Kafka las siguientes N transacciones del tramo (por defecto 1.500) a 25 por segundo, en orden de `seq`, con clave la cuenta ordenante, `acks=all` e idempotencia. `--continuar` sigue después de la última publicada (`progreso.json`). Al final imprime la tabla del RF1: confirmados, p50 y p99 de fuente a Kafka, errores y `PUBLICACIÓN OK`. |
| `demo/4_verificar.sh` | `docker compose run --rm spark-cliente spark-submit …/verificar_fase4.py --esperar 900` | Espera a que los jobs se pongan al día y compara Kafka, Bronze, Silver, `risk_decisions` y MongoDB: deben cuadrar sin duplicados. También revisa que el score en línea sea el del notebook y mide la latencia. La única falla esperada es el RNF-02. |
| `demo/5_gold.sh` | `docker compose --profile streaming stop job1-bronze job2-riesgo job2-ventanas job3-silver`; `docker compose exec airflow airflow dags unpause breb_lakehouse_pipeline` | Apaga el streaming y quita la pausa del DAG. Como tiene `catchup=False`, Airflow crea de inmediato una corrida para el último intervalo de 15 minutos. Después muestra las corridas con `airflow dags list-runs`. |
| `demo/6_consumo.sh` | `docker compose --profile consumo up -d` | Prende Hive Metastore, Trino y Metabase cuando Gold ya terminó, y espera a que queden *healthy*. |
| `demo/7_linaje.sh` | `docker compose --profile consumo run --rm spark-cliente python -m breb.consumo linaje` | Reconstruye una alerta como el usuario `cumplimiento`, que solo tiene lectura. Muestra 5 pasos: Bronze (tópico, partición, offset y hora de ingesta), Silver, `risk_decisions` (score, umbral, versión del modelo, latencia), Gold (canal y nivel de riesgo de las cuentas) y lo que hizo cada entidad, desde MongoDB. Termina con los segundos que tardó (RNF-06: menos de 5 minutos). |
| `demo/9_apagar.sh` | `docker compose exec airflow airflow dags pause breb_lakehouse_pipeline`; `docker compose --profile "*" stop` | Pausa el DAG y detiene todos los contenedores, para que no arranquen solos la próxima vez que se prenda la instancia. |

---

## 4. Pestañas del navegador

Ábranlas en este orden antes de grabar; así, en el video, solo se cambia de pestaña. La consola debe estar abierta desde el botón **AWS** del laboratorio. Si un enlace no abre, lleguen por el buscador de la consola.

| # | Pestaña | Dirección | Qué se muestra |
|---|---|---|---|
| 1 | EC2 | https://us-east-1.console.aws.amazon.com/ec2/home?region=us-east-1#Instances: | `breb-mvp` en *Running*, tipo `r5.large`. En la pestaña **Security**: *IAM Role* `LabRole`. |
| 2 | S3 · Bronze | https://us-east-1.console.aws.amazon.com/s3/buckets/breb-414490329576-lakehouse-bronze?region=us-east-1&prefix=transacciones/ | Las carpetas `year=/month=/day=` (Bronze se particiona por fecha de ingesta: hoy aparece una carpeta nueva) y `_delta_log/`, con un JSON por commit de Delta. |
| 3 | S3 · Gold | https://us-east-1.console.aws.amazon.com/s3/buckets/breb-414490329576-lakehouse-gold?region=us-east-1&prefix=risk_decisions/_delta_log/ | Los commits de `risk_decisions`, uno cada 10 s mientras hay decisiones. Ordenen por **Last modified**. |
| 4 | DynamoDB | https://us-east-1.console.aws.amazon.com/dynamodbv2/home?region=us-east-1#item-explorer?table=delta_log | La tabla `delta_log`: una fila por commit de Delta (`tablePath` y `fileName`). Coordina a los procesos que escriben la misma tabla. |
| 5 | SNS | https://us-east-1.console.aws.amazon.com/sns/v3/home?region=us-east-1#/topics | `fraud-alerts-critical` → **Subscriptions**: una suscripción por cola, con su política de filtro por entidad. |
| 6 | SQS | https://us-east-1.console.aws.amazon.com/sqs/v3/home?region=us-east-1#/queues | Las colas `breb-entidad-*` y `breb-analistas-fraude`. En una cola → **Monitoring**: mensajes enviados y recibidos, con unos 5 minutos de retraso. **No usen *Poll for messages*:** se llevaría las alertas antes que las pestañas C y D. |
| 7 | Athena | https://us-east-1.console.aws.amazon.com/athena/home?region=us-east-1#/query-editor | El editor, con las consultas de la parte 5, cada una en su propia pestaña del editor (el botón **+**). Arriba a la izquierda: *Data source* `AwsDataCatalog`. |
| 8 | Glue (opcional) | https://us-east-1.console.aws.amazon.com/glue/home?region=us-east-1#/v2/data-catalog/tables | Las tablas de `bronze`, `silver` y `gold`, con *Table type* `DELTA` y su ubicación en S3. |
| 9 | Spark | http://localhost:8080 | El master de Spark: el worker con 2 núcleos y, en **Running Applications**, los cuatro jobs de streaming. |
| 10 | Airflow | http://localhost:8085 | Usuario `admin` (clave en `demo/credenciales.sh`). DAG `breb_lakehouse_pipeline` → **Graph**: las tareas cambian de color mientras corren. |
| 11 | Metabase (Athena) | http://localhost:3000/dashboard/3 | El tablero "Bre-B · detección de fraude (Athena)". La versión sobre Trino es `/dashboard/2`. Si los números no coinciden, entren a http://localhost:3000 → **Colecciones**. |
| 12 | Trino (opcional) | http://localhost:8090/ui/ | Entren con cualquier nombre de usuario: no tiene clave. Muestra las consultas que corre el linaje como `cumplimiento`. |

Las direcciones `localhost` solo funcionan con la pestaña E-tunel abierta y con el servicio prendido. Spark está arriba con el streaming. Airflow está siempre arriba. Trino y Metabase, después de `demo/0_prender.sh` y de `demo/6_consumo.sh`.

---

## 5. Consultas para Athena

Cada una va en su propia pestaña del editor. Al correrla, debajo del resultado aparecen el tiempo y los datos escaneados (*Data scanned*): Athena cobra 5 USD por TB, con un mínimo de 10 MB por consulta. Las horas salen en UTC, la hora de Bogotá + 5.

**Q1 · Las capas cuadran (RF5 y RF6).** Antes de Gold, `fact_transferencias` tiene lo de ayer; después, lo mismo que las demás.
```sql
SELECT 'bronze' AS tabla, count(*) AS filas, count(DISTINCT transaction_uuid) AS distintas FROM bronze.transacciones
UNION ALL SELECT 'silver', count(*), count(DISTINCT transaction_uuid) FROM silver.transacciones
UNION ALL SELECT 'risk_decisions', count(*), count(DISTINCT transaction_uuid) FROM gold.risk_decisions
UNION ALL SELECT 'fact_transferencias', count(*), count(DISTINCT transaction_uuid) FROM gold.fact_transferencias
```

**Q2 · Pregunta 4: tasa de alertas por canal.** La misma consulta del tablero.
```sql
SELECT c.canal, count(*) AS transacciones,
       sum(CASE WHEN f.decision = 'alerta' THEN 1 ELSE 0 END) AS alertas,
       round(100.0 * sum(CASE WHEN f.decision = 'alerta' THEN 1 ELSE 0 END) / count(*), 3) AS porcentaje_alertas
FROM gold.fact_transferencias f
JOIN gold.dim_canal c ON c.canal_key = f.canal_key
GROUP BY 1 ORDER BY porcentaje_alertas DESC
```

**Q3 · Las alertas emitidas, con la etiqueta real.**
```sql
SELECT f.event_time, o.cuenta_id AS ordenante, d.cuenta_id AS destino,
       round(f.monto_pagado, 2) AS monto, f.moneda_pago AS moneda, round(f.risk_score, 3) AS score,
       CASE f.es_lavado WHEN 1 THEN 'sí' ELSE 'no' END AS lavado_real, round(f.latencia_ms / 1000, 1) AS latencia_s
FROM gold.fact_transferencias f
JOIN gold.dim_cuenta o ON o.cuenta_key = f.cuenta_origen_key
JOIN gold.dim_cuenta d ON d.cuenta_key = f.cuenta_destino_key
WHERE f.decision = 'alerta'
ORDER BY f.event_time
```

**Q4 · Desempeño del modelo frente a las etiquetas.** Precisión: de las alertas, cuántas eran lavado. Recall: de los casos de lavado, cuántos se alertaron.
```sql
SELECT count(*) AS transacciones,
       sum(CASE WHEN decision = 'alerta' THEN 1 ELSE 0 END) AS alertas,
       sum(CASE WHEN decision = 'alerta' AND es_lavado = 1 THEN 1 ELSE 0 END) AS alertas_con_lavado,
       sum(es_lavado) AS casos_de_lavado,
       round(1.0 * sum(CASE WHEN decision = 'alerta' AND es_lavado = 1 THEN 1 ELSE 0 END)
             / nullif(sum(CASE WHEN decision = 'alerta' THEN 1 ELSE 0 END), 0), 2) AS precision,
       round(1.0 * sum(CASE WHEN decision = 'alerta' AND es_lavado = 1 THEN 1 ELSE 0 END)
             / nullif(sum(es_lavado), 0), 2) AS recall
FROM gold.fact_transferencias
```

**Q5 · Latencia Kafka → decisión por minuto (RNF-02).** Se ven los minutos de ayer y los de hoy.
```sql
SELECT date_trunc('minute', kafka_timestamp) AS minuto, count(*) AS transacciones,
       round(approx_percentile(latencia_ms, 0.5) / 1000, 1) AS p50_s,
       round(approx_percentile(latencia_ms, 0.99) / 1000, 1) AS p99_s
FROM gold.risk_decisions
GROUP BY 1 ORDER BY 1
```

**Q6 · Linaje de las alertas en las tres capas.** El mismo `transaction_uuid` une el evento de Kafka en Bronze, la transacción limpia en Silver y la decisión.
```sql
SELECT r.transaction_uuid, b.kafka_partition, b.kafka_offset, b.ingestion_timestamp,
       s.source_account, s.destination_account, s.amount_paid,
       round(r.risk_score, 3) AS score, r.decision_ts, round(r.latencia_ms / 1000, 1) AS latencia_s
FROM gold.risk_decisions r
JOIN bronze.transacciones b ON b.transaction_uuid = r.transaction_uuid
JOIN silver.transacciones s ON s.transaction_uuid = r.transaction_uuid
WHERE r.decision = 'alerta'
ORDER BY r.decision_ts DESC
LIMIT 10
```

**Q7 · Cuarentena.** Cuántos eventos llegaron válidos y cuántos quedaron marcados en Bronze.
```sql
SELECT is_valid, count(*) AS eventos FROM bronze.transacciones GROUP BY is_valid
```

**Q8 · Salud del streaming.** Lo que registró el `StreamingQueryListener` por cada micro-batch.
```sql
SELECT job, consulta, count(*) AS micro_batches, sum(filas) AS filas, max(ts) AS ultimo_micro_batch,
       approx_percentile(duracion_ms, 0.5) AS duracion_p50_ms, max(max_offsets_atras) AS max_offsets_atras
FROM gold.progreso_streaming
GROUP BY 1, 2 ORDER BY 1, 2
```

---

## 6. Ensayo antes de grabar

1. Corran `abrir_demo.ps1` (parte 3). En **A-comandos**: `demo/0_prender.sh`.
2. Presionen Enter en la ventana del script para abrir B, C y D. B dice "esperando a que job2-riesgo esté corriendo"; es normal.
3. En A, `demo/credenciales.sh` (sin grabar). Entren a Airflow (http://localhost:8085) y a Metabase (http://localhost:3000), y revisen que abran `/dashboard/2` y `/dashboard/3`.
4. En Athena, peguen las 8 consultas y córranlas una vez: deben responder con los datos de ayer.
5. En A, `demo/1_streaming.sh`. En 1 o 2 minutos, http://localhost:8080 muestra cuatro aplicaciones en *Running Applications* y B empieza a esperar micro-batches.
6. En A, `demo/2_alertas_esperadas.sh`. Elijan el N más pequeño con al menos 2 alertas (por ejemplo, `demo/3_productor.sh 2000`). Con más transacciones, el Job 2 tarda más en ponerse al día: unas 15 por segundo con 2 vCPU.
7. Preparen el grabador. Con OBS Studio:
   - **Fuente:** *Display Capture* de la pantalla completa.
   - **Video:** 1920×1080 a 30 fps.
   - **Atajos:** uno para empezar y detener la grabación y otro para pausarla (**Settings → Hotkeys**).
   - **Prueba:** graben 10 segundos para revisar que se lee bien.
   - Sin OBS, sirve el grabador de pantalla de Clipchamp.

---

## 7. Grabación

### Lo que se ve en el video final (8:00)

| Tiempo | Pantalla | Qué se ve | Quién narra y qué dice (resumen del guion) |
|---|---|---|---|
| 0:00–0:08 | EC2 | `breb-mvp` en *Running*, `r5.large` | Samuel: "Todo esto corre en AWS Academy, en us-east-1." |
| 0:08–0:14 | EC2 → Security | *IAM Role* `LabRole` | "La instancia toma sus credenciales del rol, sin llaves en archivos." |
| 0:14–0:20 | A · `demo/estado.sh` | Kafka, MongoDB, Spark y Airflow; ningún emulador | "En la EC2 solo corre el cómputo." |
| 0:20–0:28 | S3 · Bronze | `transacciones/_delta_log/`, un JSON por commit | "El lakehouse está en S3, en Delta." |
| 0:28–0:35 | DynamoDB · `delta_log` | Una fila por commit | "Cada commit de Delta se coordina en DynamoDB." |
| 0:35–0:40 | SNS · Subscriptions | Una suscripción por cola, con filtro | "Las alertas salen por SNS a una cola SQS por entidad." |
| 0:40–0:52 | A · `demo/3_productor.sh N` | El comando en cian, "continuando desde seq …" | Samuel: "El productor publica en Kafka el siguiente pedazo del tramo, en orden, con acks=all." |
| 0:52–1:00 | A | "calentamiento: primer mensaje confirmado en … ms" | "El primer mensaje espera la conexión; desde ahí se mide." |
| 1:00–1:15 | Spark · localhost:8080 | Cuatro *Running Applications*, el worker con 2 núcleos | "Cuatro aplicaciones de Spark leen el mismo tópico, cada una con sus offsets." |
| 1:15–1:45 | B-job2 | Llegan las líneas `[job2] batch` | "Cada línea es un micro-batch: transacciones, alertas, latencia y el tiempo de cada tramo." |
| 1:45–2:00 | B-job2, acercando una línea | La lectura, decenas de miles de ms, frente a variables y modelo, cientos de ms | "Variables y modelo, milisegundos; leer y validar, segundos. Ese es el cuello." |
| 2:00–2:20 | C-entidad | `[entidad X] alerta … score … : retiene …` | Sebastian: "Esta alerta salió por SNS y llegó por SQS a la cola de la entidad, que retuvo los fondos." |
| 2:20–2:35 | D-analistas | `ALERTA … \| canal \| monto \| cuentas \| score` | "Los analistas reciben todas las alertas con su contexto. Si SNS repite una, se descarta." |
| 2:35–2:55 | S3 · Bronze | La carpeta de hoy en `year=…/month=…/day=…` y Parquet recién escritos | "Bronze guarda el 100 % de lo que llega, particionado por fecha de ingesta." |
| 2:55–3:15 | S3 · Gold, por *Last modified* | Commits de `risk_decisions` cada 10 s | "Las decisiones se guardan en Delta cada 10 segundos, fuera del micro-batch." |
| 3:15–3:35 | A | La tabla del RF1 y `PUBLICACIÓN OK` | Samuel: "Confirmados todos, cero errores; el p99 hacia Kafka, en milisegundos." |
| 3:35–3:45 | A · final de `demo/4_verificar.sh` | Kafka, Bronze, Silver y `risk_decisions` con el mismo número | Samuel: "Nada se perdió ni se duplicó." |
| 3:45–4:00 | A · `demo/5_gold.sh` | Se detienen los 4 jobs y se quita la pausa del DAG | Mateo: "Con 2 vCPU apagamos el streaming para que el batch tenga procesador." |
| 4:00–4:20 | Airflow → Graph | `check_streaming_health` y `check_new_silver_data` en verde | "check_new_silver_data fija la versión de Silver; todo lo demás lee esa versión." |
| 4:20–5:30 | Airflow → Graph, acelerado | Dims, facts, `data_quality_checks` y `sync_profiles_to_mongo` en verde | "Dimensiones con SCD 2, hechos con MERGE de solo inserción, y la calidad antes de escribir en MongoDB." |
| 5:30–6:00 | Airflow → la corrida | *success* y su duración, unos 5 minutos | "La corrida completa tarda unos 5 minutos en AWS." |
| 6:00–6:15 | Airflow → `data_quality_checks` → Logs | Los controles en OK | "Si un control falla, no se sincronizan perfiles." |
| 6:15–6:35 | Athena · Q1 | Las cuatro tablas con el mismo número | Sebastian: "Athena lee el mismo Delta de S3 por el catálogo de Glue, sin servidor." |
| 6:35–6:55 | Athena · Q2 | Alertas por canal y el *Data scanned* | "Pregunta 4 del E1, respondida escaneando unos pocos MB." |
| 6:55–7:15 | Metabase · `/dashboard/3` | Desempeño, latencia, alertas y preguntas 1 a 4 | "El tablero de los analistas, sobre Athena." |
| 7:15–7:25 | A · `demo/7_linaje.sh` | El comando | "Como cumplimiento, que solo puede leer:" |
| 7:25–7:55 | A | Los 5 pasos: Bronze, Silver, `risk_decisions`, Gold y la entidad | "de la partición y el offset en Kafka hasta la acción de cada entidad…" |
| 7:55–8:00 | A | "reconstruido en … s" | "…en segundos; el requisito es menos de 5 minutos." |

### Cómo se graba, en el orden real

**Escena 1 · Consola (0:00–0:40).** Se puede grabar en cualquier momento después del ensayo. Recorran las pestañas 1, A (`demo/estado.sh`), 2, 4 y 5 con calma: al editar se recorta.

**Escena 2 · Streaming en vivo (0:40–3:35).** Una sola grabación, pausándola en las esperas:
1. Empiecen a grabar con la pestaña A visible y corran `demo/3_productor.sh N`, con el N del ensayo.
2. Cambien a Spark (pestaña 9) unos 10 segundos y luego a B-job2. Esperen las primeras líneas, que pueden tardar de 30 a 60 segundos.
3. Cuando B muestre un micro-batch con alertas, pasen a C-entidad y a D-analistas.
4. Vayan a S3 · Bronze y actualicen la vista: debe aparecer la carpeta de hoy. Luego S3 · Gold, ordenado por *Last modified*.
5. Vuelvan a A para el final del productor: la tabla del RF1 y `PUBLICACIÓN OK`. El productor termina antes que el Job 2; al editar se reordena.

**Escena 3 · Verificación (3:35–3:45).** Sin grabar, corran `demo/4_verificar.sh`: espera hasta que los jobs se pongan al día, unos minutos. Graben solo el resumen final. Esto también confirma que Silver ya tiene todo antes de armar Gold.

**Escena 4 · Gold (3:45–6:15).**
1. Graben `demo/5_gold.sh` en la pestaña A.
2. En Airflow, abran `breb_lakehouse_pipeline` → **Graph**, con la corrida nueva seleccionada.
3. Pausen la grabación y reanúdenla cada vez que una tarea se ponga verde. Al editar, ese tramo se acelera.
4. Al final, la corrida en *success* y el log de `data_quality_checks`.

**Entre escenas (sin grabar).** En A, `demo/6_consumo.sh`, unos 3 minutos. En Athena, vuelvan a correr Q1 para revisar que ya aparezcan las transacciones de hoy.

**Escena 5 · Athena y Metabase (6:15–7:15).** Q1, luego Q2 (muestren el *Data scanned*) y después Metabase `/dashboard/3`, bajando despacio por el tablero.

**Escena 6 · Linaje (7:15–8:00).** En A, `demo/7_linaje.sh`. Dejen la salida completa en pantalla unos segundos.

**Extra opcional:** unos 5 minutos después de la escena 2, SQS → una cola → **Monitoring** muestra el pico de mensajes. Sirve como imagen de apoyo entre 2:00 y 2:35.

---

## 8. Edición

Con Clipchamp, que viene con Windows 11:
1. Importen los videos de las escenas y pónganlos en el orden de la tabla de la parte 7.
2. Corten las esperas. Lo que no se pueda cortar, como Airflow, acelérenlo de 4× a 8× y pongan un texto pequeño: "acelerado 8×".
3. Agreguen textos cortos donde ayuden: "Kafka → decisión", "SNS → SQS", "Gold en 5 min".
4. Voces: cada integrante graba su parte con el guion (Samuel, de 0:00 a 2:00 y de 3:15 a 3:45; Sebastian, de 2:00 a 3:15 y de 6:15 a 8:00; Mateo, de 3:45 a 6:15). Puede ser con la grabadora de Clipchamp o con el celular, e importando el audio.
5. Exporten en 1080p y revisen el video completo antes de subirlo.

---

## 9. Si algo falla

| Síntoma | Causa probable | Qué hacer |
|---|---|---|
| `abrir_demo.ps1`: "No hubo conexión" | La instancia todavía arranca, o cambió su IP pública | Esperen *2/2 checks*. Actualicen la regla de SSH con **My IP** (parte 2, paso 3) y corran el script otra vez con la IP nueva. |
| E-tunel se cierra con "Address already in use" | Algo del portátil usa 8080, 8085, 8090 o 3000 (por ejemplo, el stack local de Docker) | Apaguen eso y vuelvan a abrir el túnel: `ssh -F C:\breb\ssh.conf -N breb-tunel`. |
| `localhost:…` no carga | El túnel está cerrado o el servicio está apagado | Revisen E-tunel. Spark solo está arriba con el streaming, y Metabase y Trino después de `demo/6_consumo.sh`. |
| Spark muestra menos de 4 aplicaciones | Los jobs todavía arrancan | Esperen 2 minutos. En A, `demo/estado.sh`; si un job se reinicia en bucle: `docker compose --profile streaming logs --tail 50 <job>`. |
| B no muestra nada | No llegan transacciones nuevas | Es normal hasta correr `demo/3_productor.sh`. Las primeras líneas tardan de 30 a 60 s. |
| No llega ninguna alerta | El N elegido no tenía alertas | `demo/2_alertas_esperadas.sh 3000 5000` y otra vez `demo/3_productor.sh N`. Como alternativa, muestren las 8 alertas de ayer en Athena (Q3). |
| El productor dice "NO HABÍA TRANSACCIONES POR PUBLICAR" | Se acabó el tramo, que es de 15.000; ayer se publicaron 5.000, así que no debería pasar | Publiquen con un seq explícito: `docker compose run --rm productor publicar --desde-seq <seq> --max N --escalones "25:90"`. |
| No aparece una corrida en Airflow | El scheduler todavía no la creó | En A: `docker compose exec airflow airflow dags trigger breb_lakehouse_pipeline`. |
| `check_new_silver_data` sale *skipped* | Silver no tenía datos nuevos cuando se apagó el streaming | Prendan el streaming otra vez (`demo/1_streaming.sh`), corran `demo/4_verificar.sh` y repitan `demo/5_gold.sh`. |
| Una tarea de Airflow falla | Falta de memoria o un error de Spark | Abran el log de la tarea en Airflow. En A, `free -h`: si no hay swap, `sudo swapon /swapfile`. Airflow reintenta hasta 3 veces. |
| Athena: "TABLE_NOT_FOUND" o pide la ubicación de resultados | Otro *Data source* o workgroup | *Data source* `AwsDataCatalog` y workgroup `primary`. Si pide la ubicación: **Settings → Manage** → `s3://breb-414490329576-athena-resultados/consultas/`. |
| Metabase no deja entrar | Clave distinta | `demo/credenciales.sh` (sin grabar). |
| Todo va muy lento | Corren el streaming y el consumo a la vez | Con 2 vCPU, uno a la vez: `demo/1_streaming.sh` apaga el consumo, y `demo/5_gold.sh` apaga el streaming. |

---

## 10. Apagar todo

1. En A: `demo/9_apagar.sh`.
2. En E-tunel: Ctrl+C. Cierren las pestañas.
3. En la consola: **EC2** → `breb-mvp` → **Instance state** → **Stop**, o **End Lab** en el laboratorio, que también la detiene.

El disco y todo lo que está en S3, DynamoDB, SNS y SQS se conservan. Apagada, la instancia solo cobra el disco: unos 0,27 USD al día.
