# Plan de implementación · Entregable 2

Plan para construir la arquitectura del Entregable 1 v2.0 (`documento/Entregable1_BreB_v2.pdf`). Las fases siguen los hitos S13 a S16 de la sección 5.2.

**Cómo usarlo:** cada tarea se marca `[x]` en el mismo pull request que la completa, con el enlace del PR al final de la línea. Si una tarea cambia de alcance, se edita aquí.

## Estado general
| Fase | Hito | Avance |
|---|---|---|
| 0. Repositorio y contratos | S13 | 8 de 10 |
| 1. Modelo dentro del Job 2 | S13 | 10 de 10 |
| 2. Infraestructura local | S13 | 16 de 17 |
| 3. Ingesta | S13–S14 | 7 de 7 |
| 4. Streaming | S14 | 9 de 11 |
| 5. Batch y orquestación | S15 | 0 de 6 |
| 6. Consumo y gobernanza | S15 | 0 de 6 |
| 7. Pruebas y endurecimiento | S16 | 0 de 6 |
| 8. Documento y sustentación | S16 | 0 de 6 |

---

## Fase 0 · Repositorio y contratos (S13)
- [x] Estructura del repositorio y paquete `breb` (`src/breb`), con pruebas en `tests/`
- [x] Contrato del evento 1.0 en JSON Schema y validación `errores_evento` (RF2)
- [x] Contrato de la alerta 1.0 (RF10)
- [x] Contrato de variables del modelo: `modelo/aml_xgb/model_meta.json`
- [x] Documentos de MongoDB documentados en `contratos/README.md` (RF8)
- [x] Notebook de entrenamiento v1 versionado en `modelo/entrenamiento/`
- [x] Subir el repositorio a GitHub: [Sebaxdenc/breb-fraude](https://github.com/Sebaxdenc/breb-fraude) (privado)
- [ ] Agregar al resto del equipo como colaboradores y proteger `main` (pull request con una aprobación)
- [x] Copiar `aml_xgb.json` y `golden_sample.parquet` de la salida de Kaggle a `modelo/aml_xgb/`
- [ ] Congelar los cuatro contratos: PR aprobado por el equipo

## Fase 1 · Modelo dentro del Job 2 (S13, riesgo R8)
- [x] Motor de las 58 variables con estado por cuenta, par y banco (`src/breb/features/motor.py`)
- [x] Paridad exacta con el notebook en datos sintéticos: 58 de 58 variables idénticas y score idéntico, entre el entorno de Kaggle (NumPy 2) y el de Spark (NumPy 1.26), con la aritmética de numexpr que usa pandas en Kaggle
- [x] Puntuador sin scikit-learn ni `ohe.joblib` (`src/breb/modelo/puntuador.py`, RF14)
- [x] Estado en MongoDB idempotente por micro-batch, con prueba de reintento tras falla parcial (`src/breb/estado/almacen.py`, RNF-03)
- [x] Lógica completa del micro-batch sin Spark: variables, score, estado, decisiones y alertas (`src/breb/job2/microbatch.py`)
- [x] Cálculo del estado inicial con el mismo motor (`src/breb/estado/inicial.py`)
- [x] Notebook de verificación con datos reales para Kaggle (`herramientas/kaggle/verificar_paridad_kaggle.ipynb`)
- [x] Notebook de demo del modelo (`herramientas/kaggle/demo_modelo_breb.ipynb`)
- [x] Ejecutar la verificación en Kaggle con LI-Small real y obtener **PARIDAD OK**: 58 de 58 variables idénticas, diferencia de score 0 y 20.000 de 20.000 decisiones iguales ([#3](https://github.com/Sebaxdenc/breb-fraude/pull/3))
- [x] Registrar tiempo y RAM del estado inicial real: 6.923.826 filas (5.884.946 de historia) cargadas en 44 s y estado de 704.707 cuentas, 1.349.332 pares y 41.815 bancos calculado en 121 s. En MongoDB ocupa unos 390 MB de BSON, hasta 655 MB cuando todos los documentos tienen `streaming_prev`. El cálculo en Python necesita unos 5,5 GB (DataFrame 3,4 GB, acumulados 0,9 GB y estado 1,2 GB): ver decisión D4

## Fase 2 · Infraestructura local (S13)
Criterio del hito S13: `docker compose up` levanta todo con el perfil de desarrollo y el productor publica en Kafka.
- [x] Decisión D1: SeaweedFS 4.52 (Apache 2.0) reemplaza a MinIO. `herramientas/infra/prueba_delta_s3.py` pasó con los binarios nativos: escritura, MERGE, OPTIMIZE, VACUUM y dos escritores concurrentes sin perder commits (riesgo R10)
- [x] `S3DynamoDBLogStore` con endpoint de DynamoDB configurable, para usar DynamoDB Local: parche de 5 líneas en `infra/spark/logstore/` (riesgo R9)
- [x] Núcleo de `infra/docker-compose.yml` con perfiles `dev` y `completo`: Kafka 4.3.2 KRaft (1 o 3 brókers), SeaweedFS, DynamoDB Local, Spark 3.5.9 (master y 1 o 2 workers), MongoDB 7.0 (replica set de 1 o 3) y moto
- [x] Perfil `consumo` (fase 2b): PostgreSQL 15.19, Hive Metastore 4.2.1, Trino 484 con el catálogo `lakehouse` (conector Delta) y Metabase 0.65.3.6. Se combina con `dev` o con `completo`
- [x] `herramientas/infra/prueba_consulta.py` pasó sin Docker: Spark escribe en los tres buckets, Trino crea el esquema y registra las tablas en el metastore, las consultas coinciden y Trino ve un append de Spark sin volver a registrar la tabla (RF11)
- [x] Perfil `consumo` en Docker en el equipo de la demo: **PRUEBA CONSULTA OK** en 142 s y Metabase conectado a Trino con el driver Starburst. `dev` + `consumo` usan 4,03 GiB; Metabase llegó a 1,46 GiB de 1,5 GiB, así que su tope sube a 2 GiB
- [ ] Airflow 3 (fase 2c): imagen con el cliente de Spark para los DAG de la fase 5
- [x] Imagen de Spark (`infra/spark/Dockerfile`): Python 3.11, numpy 1.26.4, pandas 2.2.3, xgboost-cpu 3.2.0, Delta 3.3.3, JARs verificados por SHA-1, paquete `breb` y artefactos del modelo copiados con `COPY`
- [x] Creación de tópicos (`infra/kafka/crear_topicos.sh`): `breb.transacciones.raw` con 6 particiones, `replication.factor=3`, `min.insync.replicas=2`, `message.timestamp.type=LogAppendTime` y 7 días de retención en `completo` (1 y 1 en `dev`); probado con un clúster nativo de 3 nodos
- [x] Inicialización del replica set (`infra/mongo/iniciar_replica.js`): `electionTimeoutMillis=2000`, keyFile, usuario root y usuario `breb_app` con readWrite solo sobre `breb`; probado con 1 y 3 miembros, y es idempotente
- [x] Buckets `lakehouse-bronze`, `lakehouse-silver`, `lakehouse-gold` y tabla `delta_log` en DynamoDB Local (`infra/init/crear_recursos.py`)
- [x] Límites de CPU y memoria por servicio
- [x] `spark.cores.max` por aplicación: `JOB1_NUCLEOS`, `JOB2_NUCLEOS` y `JOB3_NUCLEOS` en `infra/.env` (1, 2 y 1)
- [x] Perfil `dev` en Docker en el equipo de la demo (Windows 11, WSL 2): los tres servicios de inicio terminan en `Exited (0)`, `verificar_stack.py` da **STACK OK** y `prueba_delta_s3.py` da **PRUEBA DELTA OK** en 182 s. En reposo, los 7 contenedores usan 1,04 GiB ([#7](https://github.com/Sebaxdenc/breb-fraude/pull/7), [#8](https://github.com/Sebaxdenc/breb-fraude/pull/8))
- [x] Perfil `completo` en Docker en el equipo de la demo: 3 brókers, replica set de 3 y 2 workers sanos; **STACK OK** y **PRUEBA DELTA OK** en 222 s
- [x] RAM del perfil `completo` con la prueba de Delta corriendo: 4,59 GiB en total (Kafka 1,02, MongoDB 0,42, workers 1,62, driver 1,06 y el resto 0,46) sobre 11,68 GiB de WSL (riesgo R5). Falta medirla con los Jobs 1 a 3 en la fase 7
- [x] README: levantar el stack desde cero en Windows 11

## Fase 3 · Ingesta (S13–S14)
Decisión D3: el productor publica directo en Kafka, sin API de ingesta.
- [x] Conversión de filas de IBM AML a eventos: `seq`, UUID v5, sin etiqueta (`src/breb/productor/eventos.py`)
- [x] Generador de eventos inválidos para demostrar la cuarentena
- [x] `python -m breb.productor preparar`: lee LI-Small una vez y guarda en `datos/productor/` las transacciones posteriores al corte de validación (`eventos_test.parquet`) y sus etiquetas para Gold (`etiquetas_test.parquet`)
- [x] `python -m breb.productor publicar`: reproduce en orden de `seq` por escalones (por defecto 100, 250, 500, 750 y 1.000 ev/s, 60 s cada uno), con clave `source_account`, `acks=all`, idempotencia, fracción opcional de inválidos y `--continuar` desde el último `seq` publicado
- [x] Medición fuente → Kafka (RF1): de `produced_at` a la hora de escritura en el bróker (LogAppendTime), con p50, p95 y p99 por escalón e informe JSON en `datos/mediciones/`. Sin Docker, con datos sintéticos: p99 de 6 a 7 ms entre 200 y 2.000 ev/s; el tópico tiene todos los mensajes, en orden de `seq` por partición y sin duplicados
- [x] Imagen y servicio `productor` (`infra/productor/`, perfil `herramientas`), sin Spark
- [x] En Docker en el equipo de la demo con LI-Small real: `preparar` dejó 1.038.880 transacciones a reproducir (617 de lavado) en 72 s; los cinco escalones lograron la tasa pedida sin errores, con p50 de 5 a 6 ms y p99 de 49 a 235 ms (RF1: p99 < 300 ms)

## Fase 4 · Streaming (S14)
Criterio del hito S14: un evento llega a Bronze, Silver y MongoDB; el Job 2 publica alertas; p99 < 3 s.
- [x] Carga del estado inicial en MongoDB: `python -m breb.estado cargar` (lo que hará el DAG `bootstrap_state`)
- [x] Job 1 (`src/breb/streaming/job1_bronze.py`): Kafka → validación del contrato → Bronze append-only con `raw_payload`, `is_valid`, `validation_errors` y coordenadas de Kafka, particionado por fecha de ingesta; trigger de 30 s (RF2, RF5)
- [x] Job 2 (`src/breb/streaming/job2_riesgo.py`): Kafka → validación → `dropDuplicatesWithinWatermark` → `foreachBatch` con `procesar_microbatch` → MongoDB, SNS (moto) y `risk_decisions` con `txnAppId`/`txnVersion`, `decision_ts` y `latencia_ms`; trigger de 1 s (RF4, RF10, RF13, RF14)
- [x] Ventanas del RF3 en el Job 2: 5 min por ordenante y 10 min por receptora, salto de 1 min, modo update y watermark de 2 min; la ventana más reciente queda en `cuentas.ventanas` de MongoDB. Planificador FAIR para que no le quiten núcleos a las decisiones
- [x] RocksDB como state store y `spark.sql.shuffle.partitions` igual a los núcleos de cada job (ADR 02)
- [x] `StreamingQueryListener` → tabla Delta de métricas `lakehouse-gold/metricas/progreso_streaming` (duración, filas por segundo, `maxOffsetsBehindLatest`, estado)
- [x] Job 3 (`src/breb/streaming/job3_silver.py`): Bronze válidos → Silver en UTC, sin duplicados, con MERGE por `transaction_uuid` que solo inserta (RF6). Sin `dropDuplicatesWithinWatermark`: la reproducción comprime el tiempo unas 155 veces y descartaría eventos de una partición demorada
- [x] `herramientas/streaming/verificar_fase4.py`: Kafka = Bronze, Silver y `risk_decisions` sin duplicados, paridad en línea contra el notebook, latencia del RNF-02 (total y por minuto), ventanas en MongoDB y tabla de métricas
- [x] Sin Docker, con datos sintéticos y Spark local, a 200 ev/s: Bronze, Silver y `risk_decisions` completos; 7.920 de 7.920 transacciones con variables y score idénticos al notebook; ventanas y métricas escritas. **La latencia no cumplió** (p99 de 12,9 s): en ese entorno, escribir en `risk_decisions` tardaba de 1,5 a 8 s por micro-batch
- [ ] En Docker en el equipo de la demo con LI-Small real: `verificar_fase4.py` con **FASE 4 OK** y la latencia por escalón
- [ ] Si la latencia no cumple en Docker: medir cada tramo del micro-batch (el Job 2 lo imprime) y ajustar (D6)

## Fase 5 · Batch y orquestación (S15)
- [ ] Job 4: Gold en estrella (`Fact_Transferencias`, `Dim_Cuenta` SCD 2, `Dim_Entidad_Financiera`, `Dim_Canal`, `Dim_Tiempo`) con MERGE (RF7)
- [ ] Carga de las etiquetas `Is Laundering` a Gold para medir precisión y recall
- [ ] DAG `breb_lakehouse_pipeline` cada 15 min: sensores → dimensiones → hechos → calidad → sincronización a MongoDB → OPTIMIZE y VACUUM (RF9)
- [ ] DAG `reprocess_quarantine` bajo demanda (RF12)
- [ ] DAG `bootstrap_state`: `breb.estado.inicial.calcular` + `AlmacenMongo.cargar_estado_inicial` (RF8)
- [ ] Los tres DAG en verde de punta a punta

## Fase 6 · Consumo y gobernanza (S15)
- [ ] SNS y SQS en moto: tópico `fraud-alerts-critical`, una cola por entidad y suscripción de los analistas (RF15)
- [ ] Servicio de la entidad: lee su cola SQS, descarta `transaction_uuid` repetidos y escribe `estado_retencion` en MongoDB
- [ ] Hive Metastore y Trino con el conector de Delta; tablas de las tres capas registradas (RF11)
- [ ] Reglas de acceso de Trino: analistas solo Gold, cumplimiento las tres capas en solo lectura
- [ ] Metabase: tableros de las cuatro preguntas de la sección 1.3, más alertas y latencia
- [ ] Linaje: reconstruir una transacción por `transaction_uuid` en las tres capas en menos de 5 min (RNF-06)

## Fase 7 · Pruebas y endurecimiento (S16)
- [ ] Prueba de carga por escalones en el equipo de la demo; declarar la tasa con 30 % de margen (RNF-01, RNF-02)
- [ ] Falla de un bróker durante la carga (RNF-01, RNF-03)
- [ ] Falla de un ejecutor de Spark (RNF-05)
- [ ] Aislar el primario de MongoDB y medir el tiempo de elección (RNF-04)
- [ ] README validado desde cero en una máquina limpia
- [ ] Ensayo completo de la demo de 8 minutos el día anterior

## Fase 8 · Documento y sustentación (S16)
- [ ] Completar en el documento la tasa sostenida y el p99 medidos
- [ ] Publicar el `.drawio` y poner su enlace en la sección 3
- [ ] Actualizar el documento: `transaction_uuid` es UUID v5 (no v4) y resultado de la decisión D2
- [ ] Actualizar el documento: el productor publica directo en Kafka, sin API de ingesta (D3); Kafka 4.3 en lugar de 3.6, SeaweedFS en lugar de MinIO, el parche de `S3DynamoDBLogStore`, Hive Metastore 4.2.1, Trino 484 con `register_table` y el perfil `consumo`
- [ ] Guion de la demo con los pasos del diagrama
- [ ] Ensayo de preguntas: explicar cualquier componente del diagrama

---

## Decisiones pendientes
| # | Decisión | Opciones | Hito |
|---|---|---|---|
| D2 | Particiones del tópico frente al orden de las variables | Sin Docker: con `maxOffsetsPerTrigger` de 5.000, 226 de 9.204 transacciones tuvieron variables distintas (sobre todo `from_bank_freq` y `to_bank_freq`), con las mismas decisiones; sin tope, 0 de 7.920. El Job 2 queda sin tope por defecto. Falta confirmarlo con LI-Small real en Docker; si hay diferencias que cambien decisiones, usar 1 partición para la demo | S14 |
| D6 | Latencia del Job 2 (RNF-02) | Sin Docker, la escritura en `risk_decisions` (Delta en SeaweedFS) dominó el micro-batch. Si en Docker el p99 pasa de 3 s: escribir `risk_decisions` cada N micro-batches o en una consulta aparte, y declarar la tasa con la que sí se cumple (RNF-01) | S14 |
| D4 | Estado inicial en el equipo de 16 GB | Calcularlo en Python pide unos 5,5 GB, que no caben junto al stack completo. Opciones: calcularlo en Kaggle y exportar los documentos (unos 390 MB sin comprimir) para que el DAG `bootstrap_state` solo los cargue; correr el DAG con el resto del stack apagado; o bajar la memoria de `cargar` y `procesar` (tipos categóricos, sin guardar las variables crudas) | S15 |
| D5 | Perfiles para la demo | Propuesta: la demo de punta a punta corre `dev` + `consumo` (y Airflow); las pruebas de falla corren `completo` sin `consumo`. Juntos, los topes de memoria suman 16,7 GiB frente a 11,68 GiB de WSL; `dev` + `consumo` usaron 4,03 GiB en reposo. Se decide con la RAM medida en la fase 7 | S16 |

## Hallazgos que el equipo debe conocer
- **El motor replica el notebook exactamente, incluidas sus particularidades.** `is_reciprocal_loop` vale 1 cuando el mismo par ordenante→destino ya transó antes, no cuando el destino le había enviado al ordenante: el notebook agrega y busca la clave invertida. `net_flow` combina acumulados de dos cuentas distintas. No se corrigen porque el modelo se entrenó así.
- **Las sumas usan compensación de Kahan** porque `groupby().cumsum()` de pandas la usa. Sin ella, algunas variables cambian en el último decimal.
- **El motor replica la aritmética de numexpr.** En Kaggle el notebook procesa unos 5 millones de filas y, con más de 1.000.000, pandas calcula con numexpr: float32 operado con un escalar da float64 y `x ** 2` usa pow() de la libm (en el 0,08 % de los montos no es igual a `x * x`). El Job 2 trabaja con lotes chicos, donde pandas usa NumPy, así que `derivar` hace esas conversiones a mano. Afecta `sender_std_amt`, `z_score_amount_sender`, `pass_through_velocity` y `passthrough_velocity_true`. Las pruebas son bit a bit y suponen Linux x86-64, como Kaggle y la imagen de Spark.
- **`S3DynamoDBLogStore` no deja configurar el endpoint de DynamoDB** (Delta 3.3.3): siempre usa el de la región de AWS. `infra/spark/logstore/` toma la fuente oficial, le agrega la opción `ddb.endpoint` y reemplaza la clase en el JAR. En AWS se usa el JAR original, sin esa opción.
- **Kafka 3.6 ya no tiene soporte ni imagen oficial** (`apache/kafka` empieza en 3.7). El stack usa 4.3.2; el conector de Spark 3.5 trae kafka-clients 3.4.1, que funciona con brókers 4.x.
- **Delta avisa `Found Delta commit N with a timestamp ... greater than the next commit timestamp`.** S3 guarda la hora de modificación al segundo, así que dos commits en el mismo segundo quedan con la misma hora, y Delta suma 1 ms al segundo. Solo afecta la consulta por hora (`TIMESTAMP AS OF`); la consulta por versión no cambia.
- **SeaweedFS reserva 7 volúmenes por bucket.** Con los tres buckets y poco disco libre falla con `No writable volumes`, y S3A reintenta varios minutos antes de rendirse. `WEED_MASTER_VOLUME_GROWTH_COPY_1=1` baja la reserva a 1.
- **El Hive Metastore 4.2.1 no aplicó las claves JDO de `hive-site.xml`** y siguió usando Derby. `infra/hms/iniciar_hms.sh` las pasa con `-D`, como en la documentación de la imagen, y tampoco imprime la clave en el log.
- **Spark no habla con el metastore.** Spark escribe Delta en S3 y la tabla se registra una vez con `CALL lakehouse.system.register_table`; Trino lee el log de Delta en cada consulta, así que ve los datos nuevos sin volver a registrarla.
- **El servicio IAM del filer de SeaweedFS no tiene autenticación.** Por eso solo se publica el puerto S3 (8333), y únicamente en 127.0.0.1.
- **El primer mensaje del productor tarda cientos de ms** mientras obtiene la metadata y el PID de idempotencia. El productor lo envía antes de medir y lo reporta aparte como calentamiento.
- **El orden importa.** Las transacciones del mismo minuto se procesan en el orden del CSV (`seq`). Por eso el productor debe emitir en ese orden y el Job 2 ordena cada micro-batch por `seq`.

## Verificación rápida
```bash
uv venv -p 3.11 .venv && uv pip install -p .venv/bin/python -e ".[dev]"
.venv/bin/python -m pytest -q        # 14 pruebas, unos 15 s
```
