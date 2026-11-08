# Plan de implementación · Entregable 2

Plan para construir la arquitectura del Entregable 1 v2.0 (`documento/Entregable1_BreB_v2.pdf`). Las fases siguen los hitos S13 a S16 de la sección 5.2.

**Cómo usarlo:** cada tarea se marca `[x]` en el mismo pull request que la completa, con el enlace del PR al final de la línea. Si una tarea cambia de alcance, se edita aquí.

## Estado general
| Fase | Hito | Avance |
|---|---|---|
| 0. Repositorio y contratos | S13 | 8 de 10 |
| 1. Modelo dentro del Job 2 | S13 | 10 de 10 |
| 2. Infraestructura local | S13 | 9 de 13 |
| 3. Ingesta | S13–S14 | 2 de 5 |
| 4. Streaming | S14 | 0 de 8 |
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
- [ ] Resto del perfil `completo`: PostgreSQL 15, Hive Metastore, Trino, Metabase y Airflow 3 (fase 2b)
- [x] Imagen de Spark (`infra/spark/Dockerfile`): Python 3.11, numpy 1.26.4, pandas 2.2.3, xgboost-cpu 3.2.0, Delta 3.3.3, JARs verificados por SHA-1, paquete `breb` y artefactos del modelo copiados con `COPY`
- [x] Creación de tópicos (`infra/kafka/crear_topicos.sh`): `breb.transacciones.raw` con 6 particiones, `replication.factor=3`, `min.insync.replicas=2`, `message.timestamp.type=LogAppendTime` y 7 días de retención en `completo` (1 y 1 en `dev`); probado con un clúster nativo de 3 nodos
- [x] Inicialización del replica set (`infra/mongo/iniciar_replica.js`): `electionTimeoutMillis=2000`, keyFile, usuario root y usuario `breb_app` con readWrite solo sobre `breb`; probado con 1 y 3 miembros, y es idempotente
- [x] Buckets `lakehouse-bronze`, `lakehouse-silver`, `lakehouse-gold` y tabla `delta_log` en DynamoDB Local (`infra/init/crear_recursos.py`)
- [x] Límites de CPU y memoria por servicio
- [ ] `spark.cores.max` por aplicación (se fija al crear cada job, fase 4)
- [ ] Construir la imagen y correr `verificar_stack.py` y `prueba_delta_s3.py` en Docker en el equipo de la demo, en `dev` y en `completo`
- [ ] Medir la RAM del perfil `completo` (riesgo R5)
- [x] README: levantar el stack desde cero en Windows 11

## Fase 3 · Ingesta (S13–S14)
- [x] Conversión de filas de IBM AML a eventos: `seq`, UUID v5, sin etiqueta (`src/breb/productor/eventos.py`)
- [x] Generador de eventos inválidos para demostrar la cuarentena
- [ ] Productor: lee LI-Small, reproduce desde el corte de validación a una tasa configurable, por escalones (100, 250, 500, 750 y 1.000 ev/s), con un porcentaje opcional de inválidos
- [ ] API de ingesta: recibe lotes por HTTPS y publica en Kafka con `acks=all` y `enable.idempotence=true` (decisión D3)
- [ ] Medir fuente → Kafka con p99 < 300 ms (RF1)

## Fase 4 · Streaming (S14)
Criterio del hito S14: un evento llega a Bronze, Silver y MongoDB; el Job 2 publica alertas; p99 < 3 s.
- [ ] Job 1: Kafka → validación → Bronze con `is_valid`, `validation_errors`, `raw_payload` y metadatos de Kafka; trigger de 30 s (RF2, RF5)
- [ ] Job 2: Kafka → validación → `dropDuplicatesWithinWatermark` → ventanas del RF3 (5 y 10 min, modo update) → `foreachBatch` que llama a `procesar_microbatch` → MongoDB, SNS y `risk_decisions` con `txnAppId`/`txnVersion` (RF3, RF4, RF10, RF13, RF14)
- [ ] Trigger de 1 s, RocksDB como state store y `spark.sql.shuffle.partitions` igual a los núcleos (ADR 02)
- [ ] `StreamingQueryListener` → tabla de métricas (duración del micro-batch, filas por segundo, `maxOffsetsBehindLatest`)
- [ ] Latencia por evento: `decision_ts − kafka_ts` guardada en la tabla de métricas (RNF-02)
- [ ] Job 3: Bronze válidos → Silver, deduplicado con watermark de 10 min y MERGE por `transaction_uuid` (RF6)
- [ ] Delta en modo multi-cluster con `S3DynamoDBLogStore` (RNF-05, riesgo R9)
- [ ] Paridad en línea: comparar las variables guardadas en `risk_decisions` con `golden_sample.parquet` (decisión D2)

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
- [ ] Actualizar el documento: Kafka 4.3 en lugar de 3.6, SeaweedFS en lugar de MinIO y el parche de `S3DynamoDBLogStore`
- [ ] Guion de la demo con los pasos del diagrama
- [ ] Ensayo de preguntas: explicar cualquier componente del diagrama

---

## Decisiones pendientes
| # | Decisión | Opciones | Hito |
|---|---|---|---|
| D2 | Particiones del tópico frente al orden de las variables | Con varias particiones, dos eventos muy cercanos pueden caer en micro-batches distintos y procesarse en otro orden, lo que cambia un poco los acumulados (sobre todo los de banco). Medirlo con la paridad en línea; si la diferencia afecta decisiones, usar 1 partición para la demo | S14 |
| D3 | Tecnología de la API de ingesta | Contenedor Python mínimo. Si usan FastAPI, aclarar que su consumidor es Kafka, no el motor de autorización que se eliminó | S13 |
| D4 | Estado inicial en el equipo de 16 GB | Calcularlo en Python pide unos 5,5 GB, que no caben junto al stack completo. Opciones: calcularlo en Kaggle y exportar los documentos (unos 390 MB sin comprimir) para que el DAG `bootstrap_state` solo los cargue; correr el DAG con el resto del stack apagado; o bajar la memoria de `cargar` y `procesar` (tipos categóricos, sin guardar las variables crudas) | S15 |

## Hallazgos que el equipo debe conocer
- **El motor replica el notebook exactamente, incluidas sus particularidades.** `is_reciprocal_loop` vale 1 cuando el mismo par ordenante→destino ya transó antes, no cuando el destino le había enviado al ordenante: el notebook agrega y busca la clave invertida. `net_flow` combina acumulados de dos cuentas distintas. No se corrigen porque el modelo se entrenó así.
- **Las sumas usan compensación de Kahan** porque `groupby().cumsum()` de pandas la usa. Sin ella, algunas variables cambian en el último decimal.
- **El motor replica la aritmética de numexpr.** En Kaggle el notebook procesa unos 5 millones de filas y, con más de 1.000.000, pandas calcula con numexpr: float32 operado con un escalar da float64 y `x ** 2` usa pow() de la libm (en el 0,08 % de los montos no es igual a `x * x`). El Job 2 trabaja con lotes chicos, donde pandas usa NumPy, así que `derivar` hace esas conversiones a mano. Afecta `sender_std_amt`, `z_score_amount_sender`, `pass_through_velocity` y `passthrough_velocity_true`. Las pruebas son bit a bit y suponen Linux x86-64, como Kaggle y la imagen de Spark.
- **`S3DynamoDBLogStore` no deja configurar el endpoint de DynamoDB** (Delta 3.3.3): siempre usa el de la región de AWS. `infra/spark/logstore/` toma la fuente oficial, le agrega la opción `ddb.endpoint` y reemplaza la clase en el JAR. En AWS se usa el JAR original, sin esa opción.
- **Kafka 3.6 ya no tiene soporte ni imagen oficial** (`apache/kafka` empieza en 3.7). El stack usa 4.3.2; el conector de Spark 3.5 trae kafka-clients 3.4.1, que funciona con brókers 4.x.
- **El servicio IAM del filer de SeaweedFS no tiene autenticación.** Por eso solo se publica el puerto S3 (8333), y únicamente en 127.0.0.1.
- **El orden importa.** Las transacciones del mismo minuto se procesan en el orden del CSV (`seq`). Por eso el productor debe emitir en ese orden y el Job 2 ordena cada micro-batch por `seq`.

## Verificación rápida
```bash
uv venv -p 3.11 .venv && uv pip install -p .venv/bin/python -e ".[dev]"
.venv/bin/python -m pytest -q        # 14 pruebas, unos 15 s
```
