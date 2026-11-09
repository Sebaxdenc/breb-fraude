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
| 4. Streaming | S14 | 12 de 13 |
| 5. Batch y orquestación | S15 | 3 de 7 |
| 6. Consumo y gobernanza | S15 | 7 de 8 |
| 7. Pruebas y endurecimiento | S16 | 3 de 9 |
| 8. Documento y sustentación | S16 | 0 de 6 |

## Camino a la demo de punta a punta
Prioridad del equipo: mostrar el flujo completo funcionando. La latencia (D6) queda pospuesta.
1. Alertas a las entidades y a los analistas: SNS → SQS → servicio de retención → MongoDB (fase 6) · hecho
2. Tramo de la demo con alertas: puntuación fuera de línea del test y estado hasta ese punto · hecho
3. Gold: Job 4 con el modelo en estrella y las etiquetas (fase 5) · hecho
4. Trino con las tablas registradas, permisos, linaje y tablero de Metabase (fase 6) · hecho
5. Airflow 3 con los tres DAG (fases 2 y 5)
6. Guion de la demo con perfiles escalonados para que quepa en 11,7 GB de WSL (D5)

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
- [x] Primera corrida en Docker con LI-Small real (100, 250 y 500 ev/s, [#13](https://github.com/Sebaxdenc/breb-fraude/pull/13)): estado inicial de 2.095.854 documentos cargado en 4,4 min; Bronze 50.998 de 50.998 y Silver 50.998 sin duplicados; **paridad en línea: 20.000 de 20.000 transacciones con variables y score idénticos al notebook**. **La latencia no cumplió**: p99 de 285 s. Los micro-batches del Job 2 crecieron hasta 29.549 transacciones y tardaron hasta 166 s; cada uno pasó de 6 a 19 s escribiendo en `risk_decisions` y hasta 107 s entre MongoDB, variables y modelo, sin desglose
- [x] Cambios para la latencia (D6): `risk_decisions` sale del camino de la decisión (archivo por micro-batch y consulta aparte que hace un commit cada 10 s); las ventanas del RF3 pasan a su propia aplicación (`job2-ventanas`); Delta reconstruye el estado de la tabla con 4 tareas en vez de 50; Kafka y MongoDB con hasta 2 CPU; el Job 2 imprime el tiempo de cada tramo (lectura de Kafka, lectura de MongoDB, variables, modelo, escritura en MongoDB, SNS, archivo)
- [x] Segunda corrida en Docker ([#14](https://github.com/Sebaxdenc/breb-fraude/pull/14)): Bronze, Silver y `risk_decisions` con 71.910 de 71.910, sin duplicados; paridad de 20.000 de 20.000; ventanas en 35.258 cuentas. Latencia: p99 de 99 s a 250 ev/s (antes 285 s)
- [ ] Latencia del RNF-02 en Docker (pospuesta, ver D6)

## Fase 5 · Batch y orquestación (S15)
- [x] Job 4 (`src/breb/batch/job4_gold.py`): `fact_transferencias`, `dim_cuenta` (SCD 2 sobre entidad y nivel de riesgo), `dim_entidad_financiera`, `dim_canal` y `dim_tiempo` en `lakehouse-gold/analytics/`, incremental con MERGE e idempotente; completa las decisiones que llegan tarde. Probado sin Docker con Spark local (RF7)
- [x] Job 4 en Docker con el tramo de la demo: **JOB 4 OK** en 65 s; 15.000 hechos, 13.935 cuentas, etiquetas de las 1.038.880 transacciones del test; 25 alertas con 13 verdaderos positivos, 12 falsos positivos y 12 falsos negativos (precisión 0,52, recall 0,52) ([#17](https://github.com/Sebaxdenc/breb-fraude/pull/17))
- [x] Carga de las etiquetas `Is Laundering` a Gold (`etiquetas`) y controles de calidad en el Job 4: Silver completo en los hechos, claves no nulas, integridad con `dim_tiempo`, una versión vigente por cuenta, y precisión y recall de las alertas; cada ejecución queda en `ejecuciones`
- [ ] DAG `breb_lakehouse_pipeline` cada 15 min: sensores → dimensiones → hechos → calidad → sincronización a MongoDB → OPTIMIZE y VACUUM (RF9)
- [ ] DAG `reprocess_quarantine` bajo demanda (RF12)
- [ ] DAG `bootstrap_state`: `breb.estado.inicial.calcular` + `AlmacenMongo.cargar_estado_inicial` (RF8)
- [ ] Los tres DAG en verde de punta a punta

## Fase 6 · Consumo y gobernanza (S15)
- [x] SNS y SQS en moto: tópico `fraud-alerts-critical`, cola `breb-entidad-<id>` por entidad con filtro por el atributo `entidades` (origen y destino), y cola `breb-analistas-fraude` sin filtro en lugar del correo (RF15)
- [x] Servicio de la entidad (`python -m breb.entidad --entidades 70,12`): retiene fondos en la cuenta destino o bloquea la ordenante, lo registra en `estado_retencion` y descarta las reentregas por `transaction_uuid`; bandeja de los analistas con `--analistas`. Probado sin Docker con moto y MongoDB
- [ ] El Job 2 usa `estado_retencion` en las evaluaciones siguientes (sección 1.4)
- [x] Tablas de las tres capas registradas en el Hive Metastore por Trino (`python -m breb.consumo registrar`): `lakehouse.bronze.transacciones`, `lakehouse.silver.transacciones` y en `lakehouse.gold` las 9 de Gold, `risk_decisions` y `progreso_streaming`; idempotente (RF11)
- [x] Reglas de acceso de Trino (`infra/trino/etc/reglas.json`): `breb_admin` registra tablas; `cumplimiento` lee las tres capas; `analista` y `metabase` solo Gold; cualquier otro usuario, nada. `python -m breb.consumo permisos` comprueba 10 casos
- [x] Tablero de Metabase creado por la API (`python -m breb.consumo tablero`): las cuatro preguntas de la sección 1.3 (en 5 preguntas), alertas emitidas, desempeño frente a las etiquetas, latencia por minuto y salud del streaming
- [x] Linaje (`python -m breb.consumo linaje [uuid]`), como `cumplimiento`: Bronze con coordenadas de Kafka, Silver, decisión, hechos con nivel de riesgo de las cuentas y acciones de las entidades en MongoDB; 1,1 s (RNF-06)
- [x] Capa de consumo en Docker con el tramo ([#18](https://github.com/Sebaxdenc/breb-fraude/pull/18)): 11 tablas registradas (15.000 en Bronze, Silver, `risk_decisions` y hechos; 1.038.880 etiquetas), **PERMISOS OK** en los 10 casos, linaje de la alerta con score 0,9953 en 6 s y tablero de Metabase con las 9 preguntas

## Fase 7 · Pruebas y endurecimiento (S16)
- [x] Tramo de la demo (`python -m breb.estado tramo`): puntúa todo el test con el motor del Job 2, compara PR-AUC, verdaderos y falsos positivos con el notebook, y elige las N transacciones consecutivas con más alertas. `cargar --desde-seq` deja el estado justo antes de ese tramo. Con los datos sintéticos, el PR-AUC salió idéntico al del notebook
- [x] Tramo de la demo con LI-Small real: en el test completo, el motor del Job 2 da PR-AUC de 0,26744793163262176 (el notebook, 0,2674479316326217), 264 alertas, 141 verdaderos positivos, 123 falsos positivos y 476 falsos negativos, igual que en Kaggle. Tramo: 15.000 transacciones desde el seq 6.814.261, con 25 alertas (13 de lavado real)
- [x] Corrida de punta a punta del tramo en Docker: Bronze, Silver y `risk_decisions` con 15.000 de 15.000; score idéntico a la puntuación fuera de línea en 15.000 de 15.000; los analistas recibieron las 25 alertas y la entidad 12 bloqueó las dos cuentas ordenantes de sus alertas. Latencia: p99 de 53 s a 50 ev/s; fuente → Kafka con p99 de 5,5 s mientras corrían los jobs (D6)
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
| D6 | Latencia del Job 2 (RNF-02) · **pospuesta** | Segunda corrida: p99 de 99 s. En micro-batches de 1.400 a 4.800 transacciones, variables y modelo tardan menos de 0,5 s; el tiempo se va en leer Kafka con la validación en Python (de 2,7 a 62 s) y en MongoDB (de 0,9 a 6,9 s para leer y de 2,5 a 12,4 s para escribir). En la corrida del tramo, a 50 ev/s, el p99 fue de 53 s y el productor vio p99 de 5,5 s hacia Kafka mientras corrían los jobs: el equipo está saturado. Ideas, en orden: (1) validar el contrato con expresiones de Spark en lugar de la pandas UDF con jsonschema, en los Jobs 1 y 2; (2) MongoDB con 2 GB de caché, escribir solo los campos que cambian en vez de `streaming` y `streaming_prev` completos, y revisar con `mongostat` si el cuello es el disco de WSL; (3) triggers de 60 s para los Jobs 1 y 3 durante la demo; (4) declarar una tasa de demo más baja o medir en Linux nativo con más núcleos | S16 |
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
- **Decisión D2 resuelta: el orden entre particiones no altera las variables.** Con LI-Small real en Docker, 6 particiones y el Job 2 sin `maxOffsetsPerTrigger`, las 20.000 transacciones de `golden_sample.parquet` salieron idénticas al notebook. Con tope, Spark lo reparte entre particiones y sí aparecen diferencias.
- **El orden importa.** Las transacciones del mismo minuto se procesan en el orden del CSV (`seq`). Por eso el productor debe emitir en ese orden y el Job 2 ordena cada micro-batch por `seq`.

## Verificación rápida
```bash
uv venv -p 3.11 .venv && uv pip install -p .venv/bin/python -e ".[dev]"
.venv/bin/python -m pytest -q        # 14 pruebas, unos 15 s
```
