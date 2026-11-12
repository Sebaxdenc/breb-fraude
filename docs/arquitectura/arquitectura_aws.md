# Arquitectura de Bre-B en AWS y recorrido del pipeline

Este documento explica qué corre en AWS, para qué sirve cada pieza y qué le pasa a una transferencia desde que se publica hasta que un analista la ve en el tablero. El diagrama es [arquitectura_aws.png](arquitectura_aws.png); los números entre paréntesis, como (3), son los círculos rojos del diagrama. Los pasos para desplegarlo están en [infra/aws/README.md](../../infra/aws/README.md).

## 1. En una frase

Una transferencia entra a Kafka. El Job 2 la valida, calcula 58 variables con el historial de las cuentas en MongoDB, la puntúa con un XGBoost y, si el score supera el umbral, avisa por SNS y SQS a las entidades para que retengan los fondos. En paralelo, la transferencia se guarda en un lakehouse Delta en S3 (Bronze, Silver y Gold), y desde ahí la consultan Trino, Athena y Metabase.

## 2. Dónde corre cada pieza

Todo está en una cuenta de AWS Academy, región us-east-1.

**Servicios gestionados de AWS**

| Servicio | Para qué | Nombre en la cuenta |
|---|---|---|
| Amazon S3 | El lakehouse: tablas Delta de Bronze, Silver y Gold | `breb-414490329576-lakehouse-bronze`, `-silver` y `-gold` |
| Amazon DynamoDB | El log de commits de Delta: coordina a varios escritores de la misma tabla | tabla `delta_log` |
| Amazon SNS | El tópico de alertas de fraude | `fraud-alerts-critical` |
| Amazon SQS | Una cola por entidad financiera, con filtro, y una para los analistas | `breb-entidad-<id>`, `breb-analistas-fraude` |
| Amazon Athena | Consultas SQL sin servidor sobre las tres capas | workgroup `primary`, resultados en `breb-414490329576-athena-resultados` |
| AWS Glue Data Catalog | El catálogo de tablas que usa Athena | bases `bronze`, `silver` y `gold` |
| IAM | El rol de la instancia: ningún contenedor tiene llaves | `LabRole` (perfil `LabInstanceProfile`) |

**Una EC2 `r5.large`** (2 vCPU, 16 GiB, 100 GiB de disco, Ubuntu 24.04) corre con Docker Compose:

| Contenedor | Para qué |
|---|---|
| Kafka 4.3.2 (KRaft, 1 bróker) | El log de eventos: tópico `breb.transacciones.raw`, 6 particiones, 7 días de retención |
| MongoDB 7.0 (replica set de 1 nodo) | El estado operacional: historial de cada cuenta, par y banco, ventanas del RF3 y acciones de las entidades |
| Spark 3.5.9 (master y worker) | Los jobs de streaming 1, 2 y 3 |
| Airflow 3.3.2 | El batch: arma Gold (Job 4), carga el estado inicial, reprocesa la cuarentena y hace el mantenimiento |
| Trino 484 y Hive Metastore 4.2.1 | SQL sobre las tres capas, con permisos por rol |
| Metabase 0.65 | El tablero de los analistas, con dos bases de datos: Trino y Athena |
| Productor y servicio de las entidades | Simulan el switch de Bre-B y a los bancos que reciben las alertas |

Los contenedores toman las credenciales de AWS del rol de la instancia, a través del servicio de metadatos (IMDSv2, con límite de saltos 2 para que lleguen desde Docker). Todo el tráfico hacia S3, DynamoDB, SNS, SQS y Athena va por HTTPS. Las interfaces web solo escuchan en `127.0.0.1` de la instancia y se ven con un túnel SSH; el security group solo abre el puerto 22 a una IP.

## 3. Recorrido de una transferencia

Ejemplo real de la corrida en AWS: la transacción `d2d22203-f703-52ea-949a-2f472b07cf67`. Es un pago ACH de 6.485,49 USD de la cuenta `23726_80186AE40` a la `1422_8012FC120`, y en el dataset está marcado como lavado real. Las horas son UTC.

### (1) Productor
`src/breb/productor/` lee el CSV de IBM AML (LI-Small) en orden de `seq` y publica cada transferencia como JSON (contrato 1.0, `contratos/`). Tiene dos garantías:
- **Sin duplicados del productor:** `acks=all` y producción idempotente (`publicador.py:143-144`), así que un reintento no duplica el mensaje.
- **Identificador estable:** el `transaction_uuid` es un UUID v5 calculado a partir del `seq`; un reenvío conserva el mismo identificador.

La clave del mensaje es la cuenta ordenante (`publicador.py:126`), así que todas las transferencias de una cuenta caen en la misma partición y en orden. En AWS, del productor a Kafka hubo un p99 de 23 ms.

### (2) Kafka
El tópico `breb.transacciones.raw` guarda el log 7 días. El bróker pone la hora de escritura (`LogAppendTime`), que sirve para medir la latencia con un reloj que el productor no controla. Tres aplicaciones de Spark leen el mismo tópico de forma independiente, cada una con sus offsets en su checkpoint (`comun.py:76-83`). La transacción del ejemplo quedó en la partición 1, offset 172, a las 22:01:28.

### (3) Job 2 · decisión de riesgo
`src/breb/streaming/job2_riesgo.py` corre con un trigger de 1 s. En cada micro-batch:
1. **Valida** cada evento contra el contrato, con la misma función que usan el productor y las pruebas.
2. **Quita duplicados** por `transaction_uuid`, con un watermark de 1 hora.
3. **Ordena** el micro-batch por `seq` (`microbatch.py:47`), porque las variables dependen del orden.
4. **Lee de MongoDB (4)** el estado de las cuentas, pares y bancos del lote, por `_id` y en una sola consulta por colección.
5. **Calcula las 58 variables** (`src/breb/features/motor.py`), idénticas a las del notebook de entrenamiento.
6. **Puntúa** con el XGBoost y **decide**: desde 0,172, alerta.
7. **Publica las alertas en SNS (5)**, con el atributo `entidades` para el filtro.
8. **Guarda el nuevo estado en MongoDB**, de forma idempotente (ver la sección 6).
9. **Deja las decisiones en un archivo** `batch_<id>.parquet`. Otra consulta las agrega a la tabla Delta `risk_decisions` cada 10 s, para no frenar la decisión.

La transacción del ejemplo se decidió a las 22:02:54, en el micro-batch 4: score de 0,9953, alerta. Pasaron 86,2 s desde Kafka.

Otra aplicación de Spark del mismo archivo, `job2-ventanas`, calcula las ventanas deslizantes del RF3: destinatarios distintos por ordenante en 5 minutos y ordenantes distintos por receptora en 10 minutos. Las guarda en MongoDB. Corre aparte para no quitarle núcleos a la decisión.

### (5) SNS y SQS → (6) la entidad
SNS entrega la alerta a la cola de cada entidad involucrada, gracias al filtro por entidad de cada suscripción, y a la cola de los analistas. Cada cola solo acepta mensajes de ese tópico. El servicio de la entidad (`src/breb/entidad/servicio.py`) lee su cola y actúa:
- La entidad de destino retiene los fondos.
- La entidad de origen bloquea la cuenta ordenante.

Antes de actuar, registra la alerta en `alertas_recibidas` con `_id = entidad:transaction_uuid`; si SNS la entrega dos veces, la segunda se descarta.

En el ejemplo, la entidad 1422 retuvo los fondos a las 22:02:58 y la 23726 bloqueó la cuenta a las 22:02:59, 4 s después de la decisión.

### (7) Job 1 · Bronze
`src/breb/streaming/job1_bronze.py`, con un trigger de 30 s, guarda el 100 % de los eventos en la tabla Delta de Bronze en S3. Cada evento va con sus coordenadas de Kafka (tópico, partición, offset y hora) y la hora de ingesta. Los inválidos no se descartan: quedan con `is_valid = false` y su lista de errores, y esa es la cuarentena. Bronze es append-only y está particionado por la fecha de ingesta. La transacción del ejemplo entró a Bronze a las 22:03:08: la decisión no espera a Bronze (arquitectura Kappa).

### (8) Job 3 · Silver
`src/breb/streaming/job3_silver.py`, con un trigger de 30 s, lee Bronze (solo `is_valid = true`) y escribe Silver tipado, en UTC y sin duplicados, con un MERGE por `transaction_uuid` que solo inserta (`job3_silver.py:47-56`). Un reintento o un duplicado tardío no cambian Silver. La transacción del ejemplo llegó a Silver a las 22:05:54.

### (9) Airflow · Job 4 · Gold
El DAG `breb_lakehouse_pipeline` corre cada 15 minutos (`dags/breb_lakehouse_pipeline.py`):
1. `check_streaming_health`: no arma Gold si el streaming va muy atrasado.
2. `check_new_silver_data`: fija la versión de Silver que se va a procesar. Si no hay datos nuevos, se salta el resto.
3. `silver_to_gold_dims`: dimensiones de tiempo, canal, entidad financiera y cuenta. `dim_cuenta` es SCD 2: cuando cambia el nivel de riesgo de una cuenta, se cierra la versión vigente y se abre otra.
4. `silver_to_gold_facts`: `fact_transferencias`, con la decisión del Job 2 y la etiqueta real de lavado.
5. `data_quality_checks`: los hechos cuadran con Silver, hay integridad referencial y una sola versión vigente por cuenta. Si falla, no sigue.
6. `sync_profiles_to_mongo`: actualiza el subdocumento `historico` de cada cuenta en MongoDB.

Las etapas 3 a 5 leen Silver en la versión que fijó la etapa 2 (time travel), así que un reintento no mezcla datos nuevos. En AWS, la corrida completa tardó 5 min 4 s.

Los otros DAG:

| DAG | Cuándo | Qué hace |
|---|---|---|
| `bootstrap_state` | Una vez, antes de reproducir | Carga en MongoDB el historial de las cuentas, calculado con las transacciones anteriores al tramo. En AWS tardó 3 min 44 s |
| `reprocess_quarantine` | Bajo demanda | Corrige lo que se puede de la cuarentena sin inventar datos, lo pasa a Silver con MERGE y lanza el pipeline |
| `breb_lakehouse_mantenimiento` | Una vez al día | `OPTIMIZE` compacta los archivos pequeños y `VACUUM` borra los viejos, con 7 días de retención |

### (10) Trino y Athena
Spark escribe las tablas Delta en S3; nadie las copia. Dos motores las leen directamente del `_delta_log`:
- **Trino**, con el Hive Metastore como catálogo, aplica los permisos por rol de `infra/trino/etc/reglas.json`:
  - `analista` y `metabase` solo ven Gold.
  - `cumplimiento` lee las tres capas sin poder modificar.
  - Cualquier otro usuario no ve nada.

  En AWS pasaron 10 de 10 casos.
- **Athena**, con el Glue Data Catalog, responde las mismas 9 consultas del tablero sin cambiar una línea, porque usa el mismo SQL de Trino. Cada consulta tardó menos de 3 s y escaneó menos de 2 MB (`herramientas/aws/athena.py`).

### (11) Metabase y los analistas
El tablero "Bre-B · detección de fraude" responde las cuatro preguntas de negocio:
1. Ordenantes con muchos destinatarios nuevos.
2. Concentración hacia cuentas mula.
3. Reenvío rápido.
4. Tasa de alertas por canal y por entidad.

Muestra además el desempeño del modelo contra las etiquetas reales, la latencia por minuto y la salud del streaming. Hay una copia del tablero que consulta por Athena.

### (12) Cumplimiento y el linaje
`python -m breb.consumo linaje` sigue el `transaction_uuid` por todas las capas:
- Bronze, con sus coordenadas de Kafka.
- Silver.
- `risk_decisions`, con el score, el umbral, la versión del modelo y la latencia.
- Los hechos de Gold, con el nivel de riesgo de las cuentas.
- La acción de cada entidad en MongoDB.

En AWS el linaje del ejemplo se reconstruyó en 19,5 s; el requisito es menos de 5 minutos.

## 4. El modelo

El modelo es un XGBoost preentrenado en Kaggle con IBM AML, con partición cronológica.
- **Entrada:** 58 variables de comportamiento:
  - **Monto:** montos redondos o justo por debajo de 10.000, y moneda cruzada.
  - **Tiempo:** hora, noche y fin de semana.
  - **Historia de la cuenta:** conteos, promedio y z-score del monto.
  - **Contraparte:** primer envío a ese destino y destinatarios distintos.
  - **Reenvío rápido:** segundos desde la última entrada y fracción del dinero reenviada.
- **Salida:** una probabilidad de lavado; desde 0,172 es alerta.
- **Desempeño en el test** (1.038.880 transacciones, 617 de lavado):
  - PR-AUC de 0,267, unas 450 veces el azar.
  - 264 alertas, de las cuales 141 son lavado real.
- **Paridad:** el Job 2 calcula exactamente lo mismo que el notebook: 58 de 58 variables y 20.000 de 20.000 decisiones iguales. En AWS, el score en línea fue idéntico al de fuera de línea en las 5.000 transacciones.

## 5. Dónde vive cada dato

| Dato | Dónde | Clave |
|---|---|---|
| Eventos crudos | Kafka, `breb.transacciones.raw` | Cuenta ordenante |
| Estado por cuenta | MongoDB, `cuentas` | `_id` = cuenta. Subdocumentos: `streaming` y `streaming_prev` (Job 2), `ventanas` (RF3), `historico` (Airflow), `estado_retencion` (entidad) |
| Estado por par | MongoDB, `pares` | `_id` = `origen\|destino` (dirigido) |
| Estado por banco | MongoDB, `bancos` | `_id` = banco |
| Alertas procesadas | MongoDB, `alertas_recibidas` | `_id` = `entidad:transaction_uuid` |
| Marcas de agua | MongoDB, `sincronizacion` | `gold` (versión de Silver procesada) y `perfiles` |
| Bronze | S3, `breb-…-lakehouse-bronze/transacciones` | Partición por fecha de ingesta |
| Silver | S3, `breb-…-lakehouse-silver/transacciones` | Partición por fecha del evento |
| Gold | S3, `breb-…-lakehouse-gold/analytics/*` | Estrella: `fact_transferencias`, `dim_cuenta` (SCD 2), `dim_entidad_financiera`, `dim_canal`, `dim_tiempo` y `etiquetas` |
| Decisiones | S3, `breb-…-lakehouse-gold/risk_decisions` | Una fila por transacción, con las variables, el score y la latencia |
| Métricas del streaming | S3, `breb-…-lakehouse-gold/metricas/progreso_streaming` | Una fila por micro-batch (StreamingQueryListener) |
| Commits de Delta | DynamoDB, `delta_log` | `tablePath` + `fileName` |

## 6. Garantías

- **Exactly-once en el efecto, no en el transporte.** Kafka entrega al menos una vez y Spark, si falla, repite el micro-batch con los mismos offsets. Por eso cada escritura es idempotente:
  - **Bronze:** el sink de Delta hace un solo commit por micro-batch.
  - **MongoDB:** cada documento guarda el micro-batch que lo escribió (`batch_id`) y el estado anterior (`streaming_prev`). Si el micro-batch se repite, se parte del estado anterior (`almacen.py:34-80`).
  - **Silver y Gold:** MERGE de solo inserción.
  - **Entidad:** descarta las alertas repetidas.

  En AWS: 5.000 de 5.000 transacciones en Bronze, Silver y `risk_decisions`, sin duplicados.
- **MongoDB es CP:** write concern y read concern majority (`almacen.py:30`). Si MongoDB no responde, el Job 2 espera en vez de decidir con datos viejos. En la EC2 hay un nodo; con 3 nodos, esa espera es la elección del primario, unos 2 s (`iniciar_replica.js:34`).
- **Varios escritores en una tabla Delta:** el `S3DynamoDBLogStore` coordina el commit N con una escritura condicional en DynamoDB, así que dos escritores no se pisan.

## 7. Resultados medidos en AWS

EC2 `r5.large`, 2 vCPU, 5.000 transacciones a 20 ev/s:

| Requisito | Objetivo | Medido | Estado |
|---|---|---|---|
| RF1 · fuente → Kafka | p99 < 300 ms | p99 de 23 ms, 0 errores | Cumple |
| RNF-01 · tasa del productor | 1.000 ev/s | Probado a 20 ev/s, sin errores | Parcial |
| RNF-02 · Kafka → decisión | p99 < 3 s | p99 de 86 s (p50 de 40 s) | No cumple |
| RF5 y RF6 · completitud | 100 %, sin duplicados | 5.000 de 5.000 en Bronze, Silver y `risk_decisions` | Cumple |
| R8 · paridad con el notebook | Idéntica | Score igual en 5.000 de 5.000; 264 alertas del test iguales al notebook | Cumple |
| Alertas a la entidad | Por SNS y SQS | 8 alertas; la entidad retiene los fondos y bloquea la cuenta | Cumple |
| RF7 y RF9 · Gold con Airflow | Cada 15 min | Corrida en 5 min 4 s; precisión de 0,50 y recall de 0,40 | Cumple |
| RNF-06 · linaje | < 5 min | 19,5 s | Cumple |
| RF11 · acceso por rol | Por rol | 10 de 10 casos | Cumple |

**Por qué no se cumple la latencia.** Desglose de un micro-batch del Job 2 de 480 transacciones:

| Tramo | Tiempo |
|---|---|
| Leer el micro-batch de Kafka y validar el contrato en Python | 29,2 s |
| MongoDB (leer y escribir) | 1,1 s |
| Variables y modelo | 0,25 s |
| SNS | 0,2 s |

El 95 % se va en leer y validar. Con 2 vCPU, los cuatro jobs compiten por el procesador, y un micro-batch que debería durar 1 s dura de 22 a 34 s.
