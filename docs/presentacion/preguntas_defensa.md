# Preguntas de la defensa: respuestas y dónde está cada cosa

El profesor pregunta a un integrante por una parte que no implementó, y los demás no pueden ayudar. Todos estudian este archivo completo, junto con [la arquitectura y el recorrido del pipeline](../arquitectura/arquitectura_aws.md).

Todo se refiere al sistema desplegado en AWS Academy (us-east-1). Las cifras son de la corrida en AWS: EC2 `r5.large` de 2 vCPU, 5.000 transacciones a 20 ev/s.

Cada respuesta tiene:
- **Respuesta corta:** lo que se dice en 30 a 45 segundos. Primero la decisión, después la evidencia (un número o un archivo), después lo que se sacrificó.
- **Si piden más:** detalles y la parte del código que se abre.

Los enlaces apuntan a `main`. La sección C, "Dónde está cada cosa", reúne los enlaces al código.

---

## A. Las preguntas típicas del profesor

### A1. "¿Por qué eligieron [motor X] y no [motor Y]?"
Fórmula: "Lo decidimos en el ADR N del Entregable 1 por [razón]; al implementarlo en AWS confirmamos o aprendimos [hecho medido]".

| Elegimos | En vez de | Razón del ADR (E1) | Lo que aprendimos al implementarlo |
|---|---|---|---|
| **Kafka** 4.3.2 (KRaft) | Kinesis, RabbitMQ | ADR 01. Spark open source no trae una fuente de Kinesis para Structured Streaming, y Kinesis ata el procesamiento a un servicio. El límite de 1 MB/s es por shard, no por stream, así que no fue motivo de descarte (corrección 4). RabbitMQ no está hecho para retener días en disco ni para varios consumidores independientes del mismo flujo. | Kafka no es el cuello: del productor a Kafka, p99 de 23 ms en AWS. Tres aplicaciones de Spark leen el mismo tópico, cada una con sus offsets. Usamos 4.3.2 porque 3.6 ya no tiene soporte. |
| **Spark Structured Streaming** 3.5.9 | Flink | ADR 02. Un solo motor para streaming y batch (Jobs 1 a 4), integración nativa con Delta y curva de aprendizaje; Flink obligaba a mantener dos motores. | El motor no es el cuello: en un micro-batch de 31 s, variables y modelo tardan 0,25 s. Lo que cuesta es leer y validar (29,2 s). Con Flink el estado viviría en el operador, lo que ataca el cuello real (ver A2). |
| **MongoDB** 7.0 | Cassandra, Neo4j, Redis, DynamoDB | ADR 04. El Job 2 lee y escribe por clave en cada micro-batch; escanear Silver en S3 tarda segundos. Cassandra: leer-modificar-escribir necesita transacciones ligeras o contadores, que no son idempotentes ante reintentos. Neo4j no es un almacén operacional de alta concurrencia. | En AWS, leer y escribir el estado de un micro-batch de 480 transacciones tomó 1,1 s: no es el cuello. El patrón de `streaming` y `streaming_prev` hace idempotente cada escritura (A3). |
| **Delta Lake** 3.3.3 | Iceberg, Parquet sin tabla | ADR 03. ACID, MERGE, time travel, OPTIMIZE y VACUUM; Iceberg era más complejo con Structured Streaming. Parquet solo no tiene transacciones. | En S3, varios escritores por tabla necesitan el log de commits en DynamoDB (`S3DynamoDBLogStore`). Athena y Trino leen el mismo `_delta_log` sin copiar datos. |
| **Airflow** 3.3.2 | cron, Prefect, Dagster | ADR 05. Dependencias entre tareas, reintentos con retroceso e historial; Airflow 2 dejó de recibir soporte el 22 de abril de 2026. | La corrida de Gold tarda 5 min 4 s en AWS. Las tareas de Spark corren dentro del contenedor de Airflow (`local[2]`); con MWAA se cambiarían por operadores que lanzan trabajos en EMR. |
| **Trino** 484 + **Athena** | Spark Thrift Server, DuckDB | ADR 08 (correcciones 5 y 7). Una capa SQL sobre las tres capas, con catálogo y permisos por rol. El Thrift Server compite por los núcleos del streaming; DuckDB no tiene catálogo compartido ni control de acceso. | Athena responde las 9 consultas del tablero sin cambiar una línea, porque usa el SQL de Trino. Trino se queda para los permisos por rol (10 de 10 casos), porque Lake Formation no está en el laboratorio. |
| **SNS + SQS** | FastAPI, un tópico de Kafka de alertas | ADR 09 (corrección 6). Push a una cola por entidad, con filtro por el atributo `entidades`; la API no tenía consumidor; con Kafka, cada entidad tendría que operar un consumidor de Kafka. | SNS entrega al menos una vez: la entidad descarta las repetidas con `alertas_recibidas`. En AWS cada cola solo acepta mensajes del tópico (política de la cola). |
| Cuarentena **en Bronze** | DLQ en Kafka o en SQS | ADR 10 (corrección 1). La DLQ duplica Bronze, y los eventos reinyectados llegan tarde y el watermark los descarta. | `reprocess_quarantine` corrige lo que puede sin inventar datos y es idempotente: correrlo dos veces no cambia nada. |
| **XGBoost de Kaggle** | Reglas propias, otros modelos | ADR 07 (corrección 2). Notebook público con IBM AML, partición cronológica y variables causales; los otros candidatos tenían fugas de información o datos sin cuentas. | Paridad exacta: 58 de 58 variables y 20.000 de 20.000 decisiones. En AWS, el score en línea fue idéntico al de fuera de línea en las 5.000. |
| **EC2 con Docker Compose** para Kafka, Spark y Airflow | MSK, EMR, MWAA | ADR 06. El stack gestionado completo supera 300 USD al mes, y MSK y MWAA no tienen capa gratuita. | AWS Academy no ofrece esos servicios ni instancias de más de 2 vCPU. Lo gestionado quedó en S3, DynamoDB, SNS, SQS, Athena y Glue, con el mismo código. |

### A2. "Si el volumen de datos se multiplicara por 100, ¿qué cambiarían primero?"
**Respuesta corta:** "Lo primero es el camino crítico del Job 2, porque ahí está el cuello medido, no en Kafka. En AWS, de 31 segundos por micro-batch, 29 son leer y validar el contrato con una función de Python. Con cien veces más eventos, validaríamos con expresiones de Spark y llevaríamos el estado al state store de Spark, particionado por cuenta, para que viva donde se calcula. Después, separar los componentes: Kafka en MSK con más particiones, cada job en su propio clúster de EMR con autoescalado, y MongoDB fragmentado por hash de la cuenta."

**Si piden más:**
- **Referencia:** el promedio del día récord son 120 ev/s; por 100, 12.000 ev/s, unos 12 MB/s con eventos de 1 KB. Kafka maneja eso sin problema; lo que importa es el paralelismo.
- **Kafka:** de 6 a 48 o 96 particiones, en MSK con 3 zonas. Hay que definirlas antes de producción: cambiar el número de particiones cambia a qué partición va cada clave y rompe el orden por cuenta durante la transición.
- **Spark:** un clúster de EMR por job, con autoescalado. `spark.sql.shuffle.partitions` queda fijo en el checkpoint de una consulta con estado; hay que planearlo antes, porque cambiarlo obliga a un checkpoint nuevo. Los checkpoints pasarían a S3.
- **Lakehouse:** hoy `risk_decisions` y Gold no están particionados. Con 100 veces más datos, particionar por fecha o usar liquid clustering, y OPTIMIZE por partición.
- **Job 4:** hoy busca los pendientes con un anti-join de Silver contra todos los hechos; con el Change Data Feed de Silver procesaría solo lo nuevo.
- **Consumo:** Athena ya escala solo y cobra por lo que escanea.
- **La tensión de diseño que hay que mencionar:** cada transacción toca el estado de dos cuentas (ordenante y destino), un par y dos bancos. Si se particiona el cálculo por cuenta ordenante, las variables de la cuenta destino quedan en otra partición. Habría que propagar ese estado con mensajes (consistencia eventual de milisegundos) y probablemente reentrenar el modelo con variables que se puedan calcular de forma distribuida.

### A3. "¿Cómo garantizan que un evento no se procese dos veces si el job de Spark falla a mitad?"
**Respuesta corta:** "No prometemos exactly-once en el transporte: Kafka entrega al menos una vez y, si Spark falla, repite el micro-batch con los mismos offsets del checkpoint. Lo que garantizamos es que repetirlo no cambia el resultado. En MongoDB, cada documento guarda el número de micro-batch que lo escribió y una copia del estado anterior, `streaming_prev`. Si el micro-batch B se repite, los documentos que ya tienen `batch_id = B` se leen desde `streaming_prev`, así que el cálculo parte del mismo estado y da lo mismo. Lo probamos cortando la escritura a la mitad. En AWS: 5.000 de 5.000 transacciones, sin duplicados."

**Si piden el código** (abrir en este orden):
1. `src/breb/estado/almacen.py`, líneas 34 a 57 (`cargar`): si `streaming.batch_id == B`, usa `streaming_prev`; si es menor, usa `streaming`; si es mayor, falla, porque significa que se borraron los checkpoints con el estado intacto.
2. Líneas 59 a 80 (`guardar`): `$set` de `streaming` con `batch_id = B` y de `streaming_prev` con el estado anterior, en lotes de 5.000 sin orden.
3. `tests/test_job2_microbatch.py`, línea 96: la escritura falla después de 300 documentos, y el reintento deja exactamente el mismo estado que sin falla.

El resto de los efectos:

| Efecto | Mecanismo | Código |
|---|---|---|
| Duplicados del productor | `acks=all` y `enable.idempotence` | `publicador.py:143-144` |
| Duplicados del mismo evento | `dropDuplicatesWithinWatermark` por `transaction_uuid` (watermark de 1 h) y `drop_duplicates` dentro del lote | `job2_riesgo.py:168-169`, `microbatch.py:47` |
| Bronze | Sink de Delta con checkpoint: un commit por batchId | `job1_bronze.py:45-49` |
| `risk_decisions` | `batch_<id>.parquet` con nombre fijo (escritura atómica con `os.replace`); otra consulta lo agrega a Delta con su propio checkpoint | `job2_riesgo.py:81-103` |
| Alertas | SNS puede repetir; la entidad registra `{entidad}:{transaction_uuid}` antes de borrar el mensaje y descarta las repetidas | `servicio.py:78-101` |
| Silver | MERGE por `transaction_uuid` que solo inserta | `job3_silver.py:47-56` |
| Gold | MERGE de solo inserción por clave; las tres etapas leen Silver en la versión que fijó el sensor | `job4_gold.py:59-65`, `job4_gold.py:260-261` |

**Aclaración:** el Entregable 1 decía que `risk_decisions` usaría `txnAppId` y `txnVersion` de Delta. Lo cambiamos: escribir en Delta dentro de la decisión tardaba de 6 a 19 s por micro-batch, así que la decisión escribe un archivo y otra consulta hace el commit cada 10 s. La garantía es la misma (fuente de archivos y sink de Delta con checkpoint).

### A4. "¿Cuál es la posición CAP del componente más crítico de su sistema y qué sacrificaron?"
**Respuesta corta:** "El componente más crítico es MongoDB, porque guarda el estado con el que el Job 2 calcula las variables. Es CP: write concern majority y read concern majority. Una escritura confirmada no se pierde y no leemos estado que podría revertirse. Sacrificamos disponibilidad: si MongoDB no responde, el Job 2 espera en vez de decidir con datos viejos. En la EC2 hay un nodo; con tres nodos, esa espera es la elección de un primario nuevo, que bajamos a 2 segundos."

**Si piden más:**
- **Por qué no AP:** con `w:1`, una escritura confirmada podría perderse si el primario cae antes de replicarla. El estado son acumulados (conteos, sumas, contrapartes) que quedarían desalineados con los eventos ya procesados, y las variables del modelo cambiarían.
- **Código:** `src/breb/estado/almacen.py:30` (`w="majority", read_concern="majority"`) e `infra/mongo/iniciar_replica.js:34` (`electionTimeoutMillis: 2000`). La lista de miembros sale de `MONGO_MIEMBROS` en `infra/.env`; en la EC2 es uno solo.
- **Kafka**, del lado de la consistencia: `acks=all`. En la EC2 hay un bróker (factor de replicación 1). Con tres brókers serían factor 3 y `min.insync.replicas=2`: si caen dos, el productor no puede escribir, porque prefiere fallar a confirmar algo que podría perderse. Está parametrizado en `infra/kafka/crear_topicos.sh:10-14`.
- **S3 y Delta:** S3 tiene consistencia fuerte de lectura después de escritura. El log de commits en DynamoDB serializa los commits de varios escritores con una escritura condicional; sin él, dos escritores podrían pisarse el mismo `N.json`.
- **Lo que hay que reconocer:** con un solo nodo de MongoDB y un solo bróker en la EC2, no hay tolerancia a fallas de nodo. La configuración de tres nodos está parametrizada, pero en AWS Academy no la desplegamos.

### A5. "¿Qué pasa con el pipeline si el broker de Kafka cae? Muéstrenme en el código cómo lo manejan."
**Respuesta corta:** "En la EC2 hay un solo bróker. Si cae, el productor reintenta hasta 5 minutos sin dar por enviado nada que el bróker no confirmó. Las consultas de Spark fallan, Docker reinicia los contenedores, y cada job sigue desde los offsets de su checkpoint cuando Kafka vuelve. No se pierde nada mientras el tópico retenga los mensajes, que son 7 días. Con tres brókers, factor de replicación 3 y `min.insync.replicas=2`, la caída de uno no detendría nada."

**Código, en este orden:**
1. **Productor**, `src/breb/productor/publicador.py:143-144`: `acks: all` y `enable.idempotence: True`. No hay `retries` explícito: con idempotencia, librdkafka reintenta hasta `message.timeout.ms` (300 s por defecto). El progreso (`progreso.json`) solo avanza si no quedan mensajes sin confirmar (`__main__.py:65-66`).
2. **Tópico**, `infra/kafka/crear_topicos.sh:10-14` y `:27-28`: factor de replicación, `min.insync.replicas` y 7 días de retención. En la EC2 valen 1 y 1 (`infra/.env`).
3. **Lectura en Spark**, `src/breb/streaming/comun.py:76-83`: `startingOffsets=earliest` solo la primera vez; después mandan los offsets del checkpoint (`CHECKPOINTS`, línea 30). `failOnDataLoss` queda en su valor por defecto (true): si Kafka hubiera borrado offsets que el job no leyó, el job falla en lugar de saltarse datos en silencio.
4. **Reinicio**, `infra/docker-compose.yml:79`: `restart: unless-stopped` en los jobs. `job2_riesgo.py:250` (`awaitAnyTermination`) hace que el proceso termine si una consulta falla.

**Lo que hay que reconocer:** la prueba de tumbar el bróker en plena carga no se corrió. No hay lógica de reintentos propia: se usan los mecanismos de librdkafka, Spark y Docker.

### A6. "¿Por qué la partition key de [tabla NoSQL] es [campo X] y no [campo Y]?"
**MongoDB** (`_id` de cada colección):

| Colección | `_id` | Por qué no otra cosa |
|---|---|---|
| `cuentas` | El identificador de la cuenta (banco + cuenta en IBM AML) | El Job 2 lee por cuenta, en lote, con `{_id: {$in: [...]}}`, que usa el índice por defecto. Para fragmentar, la clave sería el **hash** de la cuenta: reparte uniformemente las escrituras. Una clave por rango o por fecha concentraría las escrituras en un fragmento; una por banco tiene muy pocos valores y deja fragmentos enormes. |
| `pares` | `"origen\|destino"` (`motor.py:67`) | El par es dirigido: A→B y B→A son estados distintos (primer envío, montos acumulados del par). Una sola cadena determinista permite leerlo por `_id`. |
| `bancos` | El código del banco | Hay pocos bancos y se leen todos los del lote. |
| `alertas_recibidas` | `"entidad:transaction_uuid"` (`servicio.py:81`) | La misma alerta llega a la entidad de origen y a la de destino, y cada una la registra una vez. El `_id` único hace que una reentrega choque con `DuplicateKeyError`. |
| `sincronizacion` | `"gold"` y `"perfiles"` | Un documento por marca de agua: la última versión de Silver procesada y la última fecha sincronizada. |

**Kafka** (clave del mensaje = `source_account`, `publicador.py:126`): conserva el orden de las transacciones de cada ordenante en su partición y reparte la carga. Con `destination_account`, las cuentas mula (muchos ordenantes hacia una receptora) harían una partición caliente. Con `transaction_uuid` se pierde el orden por cuenta. Además, el Job 2 ordena cada micro-batch por `seq`, porque las variables dependen del orden global.

**DynamoDB `delta_log`** (`infra/init/crear_recursos.py:50-56`): clave de partición `tablePath` y de orden `fileName`, el esquema que exige `S3DynamoDBLogStore`. Una escritura condicional sobre (tabla, archivo de commit) garantiza que solo un escritor cree el commit N.

**Delta en S3:** Bronze se particiona por la fecha de **ingesta**, porque llega en orden de escritura y es append-only. Silver se particiona por la fecha del **evento**, porque las consultas filtran por cuándo ocurrió la transacción y el MERGE incluye la partición para no escanear todo.

---

## B. Otras preguntas probables, por componente

### Despliegue en AWS
**¿Qué corre en AWS y qué no?** Gestionado: S3 (lakehouse), DynamoDB (log de commits de Delta), SNS y SQS (alertas), Athena con el Glue Data Catalog (consultas) e IAM (`LabRole`). En una EC2 `r5.large`, con Docker Compose: Kafka, MongoDB, Spark, Airflow, Trino y Metabase.

**¿Por qué Kafka, Spark y Airflow en una EC2 y no en MSK, EMR y MWAA?** Por tres razones:
- **Costo:** el ADR 06 estimó el stack gestionado completo en más de 300 USD al mes. Con la carga real de Bre-B y alta disponibilidad serían unos 2.550 USD al mes, contra 0,14 USD por hora del MVP (ver "¿Cuánto cuesta el MVP?").
- **El laboratorio:** AWS Academy no ofrece esos servicios ni instancias de más de 2 vCPU.
- **Código:** moverlos exige cambios. Los checkpoints y los archivos de decisiones pendientes del Job 2 están en un volumen que comparten el driver y los ejecutores (en EMR tendrían que ir a S3), y las tareas de Airflow corren `spark-submit` dentro de su propio contenedor (en MWAA serían operadores de EMR).

Es el punto 2 de "qué cambiaríamos".

**¿Cómo se despliega?** `infra/aws/README.md`. `infra/docker-compose.aws.yml` se combina con el compose de siempre:
- **Endpoints:** apunta S3, DynamoDB, SNS y SQS a sus endpoints de AWS.
- **Configuración:** monta las configuraciones de `infra/aws/` en Spark, el Hive Metastore y Trino.
- **Credenciales:** cambia las credenciales por el rol de la instancia.

`infra/aws/preparar_ec2.sh` comprueba el rol y el servicio de metadatos, y escribe el prefijo de los buckets. Los jobs no cambiaron.

**¿Cómo manejan las credenciales?** Ningún contenedor tiene llaves:
- **Rol:** la instancia tiene el perfil `LabInstanceProfile` (rol `LabRole`).
- **Llegada a los contenedores:** el servicio de metadatos (IMDSv2) entrega las credenciales; el límite de saltos está en 2 para que lleguen desde dentro de Docker.
- **Lectura en cada componente:** Spark usa `DefaultAWSCredentialsProviderChain` (`infra/aws/spark-defaults.conf:14` y `:24`); Trino, boto3 y Metabase, la cadena por defecto de su SDK.

Un error que encontramos: AWS Academy no deja crear roles (`iam:CreateRole` denegado), por eso usamos `LabRole`.

**¿Por qué los buckets se llaman `breb-<cuenta>-lakehouse-*`?** En S3 los nombres de bucket son únicos en todo AWS, así que `lakehouse-bronze` ya podía tener dueño. `src/breb/buckets.py` les pone el prefijo `BREB_BUCKET_PREFIJO`, que `preparar_ec2.sh` arma con el número de la cuenta.

**¿Por qué una `r5.large`?** El laboratorio permite instancias hasta tamaño large (2 vCPU). De esas, la `r5.large` es la que tiene 16 GiB; con 8 GiB el stack no cabe. Le agregamos 8 GB de swap en el disco. Con 2 vCPU no corremos el streaming y el consumo al mismo tiempo: para armar Gold, se apaga el streaming.

**¿Cuánto cuesta el MVP?** Unos **0,14 USD por hora prendido**. Prendido todo el mes serían unos 105 USD; apagado, solo el disco, 8 USD al mes. Son precios de lista bajo demanda en us-east-1, y el laboratorio los descuenta de su crédito. El gasto real está en el panel del Learner Lab.

| Servicio | Qué usamos | Precio | Por hora prendido | Mes 24/7 |
|---|---|---|---|---|
| EC2 `r5.large` | 1 instancia Linux (2 vCPU, 16 GiB) | 0,126 USD/h | 0,126 | 91,98 |
| EBS gp3 | Disco de 100 GiB | 0,08 USD/GB-mes | 0,011 (se cobra aunque esté apagada) | 8,00 |
| IPv4 pública | 1 dirección | 0,005 USD/h | 0,005 | 3,65 |
| S3 Standard | Bronze, Silver, Gold y resultados de Athena | 0,023 USD/GB-mes; 0,005 USD por 1.000 PUT o LIST; 0,0004 USD por 1.000 GET | Centavos | < 1 |
| DynamoDB bajo demanda | `delta_log`: una escritura por commit de Delta | 0,625 USD por millón de escrituras | ≈ 0 | < 0,10 |
| SNS | 8 alertas en la corrida | 0,50 USD por millón (el primer millón es gratis) | 0 | 0 |
| SQS | Colas de las entidades | 0,40 USD por millón (el primer millón es gratis) | 0 | 0 |
| Athena | Las 9 consultas del tablero (menos de 2 MB cada una) | 5 USD/TB, mínimo 10 MB por consulta | ≈ 0,0005 USD por corrida del tablero | ≈ 0 |
| Glue Data Catalog | Las tablas de las tres capas | Gratis hasta 1 millón de objetos | 0 | 0 |
| **Total** | | | **≈ 0,14 USD/h** | **≈ 105 USD/mes** |

En la práctica: apagada cuesta 0,27 USD al día, y una sesión de 4 horas, como la de grabar el video, unos 0,57 USD.

**¿Y cuánto costaría la arquitectura ideal, en producción?** Unos **2.550 USD al mes** (unos 30.500 al año), unas 24 veces el MVP prendido 24/7, o unos 18 USD por millón de transacciones. Es el diseño del E1 en servicios gestionados, en 3 zonas de disponibilidad. La carga es la del enunciado: 1.000 ev/s de pico y el volumen real de Bre-B, unos 1.700 millones de transacciones al año (unos 140 millones al mes; unos 140 GB al mes de eventos de 1 KB).

| Capa | Servicio y configuración | Supuesto | USD/mes |
|---|---|---|---|
| Ingesta | **Amazon MSK**: 3 brókers `kafka.m5.large` (0,21 USD/h c/u), factor de replicación 3, `min.insync.replicas=2` | 7 días de retención × 3 réplicas ≈ 300 GB de disco a 0,10 USD/GB-mes | ≈ 490 |
| Streaming (Jobs 1, 2, ventanas y 3) | **EMR en EC2** 24/7: 1 primario `m5.xlarge` (0,24 USD/h con la tarifa de EMR) y 3 nodos `r5.xlarge` (0,315 USD/h c/u) | Checkpoints en S3; discos gp3 de 64 GB por nodo | ≈ 885 |
| Batch (Job 4, calidad y mantenimiento) | **EMR Serverless** (0,0526 USD por vCPU-h y 0,0058 USD por GB-h) | 96 corridas al día de unos 5 min con 4 vCPU y 16 GB, más OPTIMIZE y VACUUM diarios | ≈ 80 |
| Estado | **MongoDB Atlas M30**: replica set de 3 nodos, 8 GB de RAM, 40 GB de disco | 0,54 USD/h; backups aparte | ≈ 388 |
| Orquestación | **Amazon MWAA**, ambiente pequeño | 0,49 USD/h | ≈ 358 |
| Lakehouse | **S3**: Bronze, Silver, Gold y `risk_decisions` | Unos 100 GB comprimidos nuevos al mes; unos 14.000 commits de Delta al día, más un archivo de decisiones por segundo | ≈ 70 |
| Commits de Delta | **DynamoDB** bajo demanda | Menos de 1 millón de escrituras al mes | ≈ 1 |
| Alertas | **SNS + SQS** | Unas 35.000 alertas al mes (0,025 %, cabe en la capa gratuita) y unas 100 colas con long polling | ≈ 5 |
| Consumo | **Athena + Glue + Lake Formation** (Lake Formation no cobra aparte; sus permisos reemplazan las reglas de Trino) | Unos 2 TB escaneados al mes, con el tablero en caché y particiones por fecha | ≈ 10 |
| Tablero | **Metabase en ECS Fargate** (1 tarea de 2 vCPU y 4 GB), RDS PostgreSQL `db.t4g.small` multi-AZ y un ALB | | ≈ 140 |
| Red | 2 NAT Gateway (0,045 USD/h c/u) y tráfico entre zonas | Gateway endpoints de S3 y DynamoDB, que son gratis | ≈ 80 |
| Observabilidad | **CloudWatch** | 30 GB de logs a 0,50 USD/GB, unas 30 métricas y 20 alarmas | ≈ 25 |
| Seguridad | **KMS** (1 USD por llave al mes) y **Secrets Manager** (0,40 USD por secreto) | 4 llaves y 5 secretos | ≈ 10 |
| **Total** | | | **≈ 2.550** |

- **Dónde se va la plata:** el 83 % en EMR del streaming (35 %), MSK (19 %), Atlas (15 %) y MWAA (14 %). S3, DynamoDB, SNS, SQS y Athena juntos no llegan a 100 USD: lo caro es lo que corre 24/7 con alta disponibilidad, no los datos.
- **Cómo bajarlo sin cambiar el diseño:** un Savings Plan de un año para los nodos de EMR (30 a 40 % menos en la parte de EC2), instancias Graviton en EMR y MSK (alrededor de 20 % menos) y mover la validación del Job 2 a expresiones de Spark, con lo que 2 nodos probablemente alcanzan. Con las tres, unos 1.800 a 1.900 USD al mes.
- **Supuestos que pueden mover el número:** con decenas de millones de cuentas reales, el estado de MongoDB probablemente no cabe en 8 GB de RAM, y Atlas M40 suma unos 360 USD al mes. Si los analistas consultan mucho más que el tablero, Athena crece en proporción. La tarifa de EMR y el precio de Atlas salen de fuentes no oficiales: hay que confirmarlos en la calculadora de AWS y en la página de MongoDB.

Precios consultados el 11 de noviembre de 2026: [MSK](https://aws.amazon.com/msk/pricing/), [MWAA](https://www.aws.amazon.com/managed-workflows-for-apache-airflow/pricing/), [EMR y EMR Serverless](https://aws.amazon.com/emr/pricing/), [tarifa de EMR por instancia](https://wring.co/blog/aws-emr-pricing-guide), [r5.large](https://calculator.holori.com/aws/ec2/r5.large/us-east-1), [r5.xlarge](https://instances.vantage.sh/aws/ec2/r5.xlarge), [m5.xlarge](https://instances.vantage.sh/aws/ec2/m5.xlarge), [MongoDB Atlas](https://managed-db-pricing.pages.dev/db/mongodb-atlas/), [Fargate](https://aws.amazon.com/fargate/pricing/), [RDS db.t4g.small](https://calculator.holori.com/aws/rds/db.t4g.small), [ALB](https://aws.amazon.com/elasticloadbalancing/classicloadbalancer/pricing), [NAT Gateway](https://cloudburn.io/blog/aws-nat-gateway-pricing), [CloudWatch](https://aws.amazon.com/cloudwatch/pricing/), [Secrets Manager y KMS](https://cloudburn.io/blog/aws-secrets-manager-pricing), [EBS](https://cloudburn.io/blog/amazon-ebs-pricing), [S3](https://cloudburn.io/blog/amazon-s3-pricing), [DynamoDB](https://aws.amazon.com/about-aws/whats-new/2024/11/amazon-dynamo-db-reduces-prices-on-demand-throughput-global-tables), [SNS y SQS](https://cloudburn.io/blog/amazon-sqs-pricing), [Athena](https://cloudburn.io/blog/amazon-athena-pricing), [IPv4 pública](https://aws.amazon.com/blogs/aws/new-aws-public-ipv4-address-charge-public-ip-insights).

**¿Qué pasa cuando se acaba la sesión del laboratorio?** La instancia se detiene y el disco se conserva. Al prenderla, `docker compose up -d` levanta los contenedores; los jobs siguen desde sus checkpoints, y lo que está en S3, DynamoDB, SNS y SQS no se toca. Lo único que cambia es la IP pública.

**¿Por qué Athena si ya tenían Trino?**
- **Athena:** es sin servidor; no ocupa la EC2 y cobra por lo que escanea. Lee el mismo Delta de S3 a través del Glue Data Catalog, y responde las 9 consultas del tablero sin cambiar una línea (`herramientas/aws/athena.py`).
- **Trino:** se queda para los permisos por rol (`reglas.json`). En Athena eso lo haría Lake Formation, que el laboratorio no ofrece.

**¿Qué monitoreo tienen?**
- Un `StreamingQueryListener` escribe una fila por micro-batch en `gold.progreso_streaming`, con su duración, filas y atraso de offsets.
- El tablero tiene la tarjeta "Salud del streaming".
- `check_streaming_health` no arma Gold si el streaming va muy atrasado.
- Airflow guarda el historial de cada corrida.

No hay CloudWatch: sería el siguiente paso, con el driver `awslogs` de Docker para los logs.

**¿Por qué el PR-AUC en la EC2 sale distinto en la sexta cifra decimal?** NumPy elige al importar la implementación de `log1p` en float32 según la CPU. La EC2 tiene AVX-512 y usa la versión vectorizada, que difiere en hasta 2 ULP en cerca del 3 % de las filas de algunas variables. El score cambia menos de 1e-6 y las decisiones no cambian: las mismas 264 alertas del test que el notebook (diapositiva 11). Está documentado en `src/breb/features/motor.py:28-32`.

### Ingesta y Kafka
**¿Por qué RNF-01 sale "parcial"?** El objetivo es 1.000 ev/s nominales. En AWS probamos a 20 ev/s, sin errores y con un p99 de 23 ms hacia Kafka, porque con 2 vCPU los jobs no procesan más rápido que eso. El productor no es el límite. Probar la tasa nominal es el punto 3 de "qué cambiaríamos".

**¿Por qué no hay API de ingesta, si el E1 la tenía?** La API no tenía un consumidor real dentro del alcance (el switch de Bre-B no existe en el proyecto). El productor publica directo en Kafka con la misma clave y el mismo contrato; en producción, el switch o un conector harían ese papel.

**¿Cómo conservan el orden de las transacciones?** El productor publica en orden de `seq` con clave `source_account`, y la idempotencia mantiene el orden por partición en los reintentos. Entre particiones no hay orden, así que el Job 2 ordena cada micro-batch por `seq` (`microbatch.py:47`). No limitamos los offsets por micro-batch: con `maxOffsetsPerTrigger`, Spark reparte el tope entre particiones y algunas transacciones quedaban en otro micro-batch; en la prueba cambiaron variables en 226 de 9.204 transacciones.

**¿Por qué `message.timestamp.type=LogAppendTime`?** Para medir la latencia desde que el bróker escribió el mensaje, con un reloj que el productor no controla. RF1 = LogAppendTime − `produced_at`; RNF-02 = `decision_ts` − LogAppendTime.

**¿Por qué el `transaction_uuid` es UUID v5?** Es determinista (se calcula a partir del `seq`): el mismo evento siempre tiene el mismo identificador, así que un reenvío o un reproceso se deduplica. Con v4, aleatorio, publicar dos veces el mismo evento daría dos identificadores.

### Streaming
**¿Por qué el Job 2 lee Kafka y no Bronze?** Leer Bronze sumaría a la latencia el trigger de 30 s del Job 1 y la escritura en Delta. Es una arquitectura Kappa: los dos jobs leen el mismo log de forma independiente, y la detección vive una sola vez, en el Job 2. En el linaje del ejemplo se ve: la decisión fue a las 22:02:54 y el evento entró a Bronze a las 22:03:08.

**¿Por qué no se cumple la latencia?** Desglose de un micro-batch del Job 2 en AWS, de 480 transacciones:

| Tramo | Tiempo |
|---|---|
| Leer el micro-batch de Kafka y validar el contrato en Python | 29,2 s |
| MongoDB (leer y escribir) | 1,1 s |
| Variables y modelo | 0,25 s |
| SNS | 0,2 s |

Con 2 vCPU, los cuatro jobs compiten por el procesador, y un micro-batch que debería durar 1 s dura de 22 a 34 s. El p99 de Kafka a la decisión fue de 86 s. Lo primero que cambiaríamos es la validación.

**¿Por qué la validación es una pandas UDF con jsonschema?** Es la misma función del contrato que usan el productor y las pruebas (`breb.contratos.errores_evento`), así que hay una sola definición de "evento válido". El costo es el rendimiento: es el tramo más lento.

**¿Cómo midieron la latencia?** Por evento: `decision_ts − kafka_timestamp`, donde `decision_ts` se toma después de publicar las alertas en SNS (`job2_riesgo.py:147-153`). Se guarda en `risk_decisions.latencia_ms`, y el tablero la muestra por minuto.

**¿Por qué el Job 3 no usa `dropDuplicatesWithinWatermark`, como dice el RF6?** La reproducción comprime el tiempo unas 155 veces: un watermark de 10 minutos de tiempo del evento equivale a segundos reales, y descartaría eventos de una partición demorada. El MERGE por `transaction_uuid` ya garantiza que no haya duplicados (`job3_silver.py:6-10`).

**¿Para qué sirve RocksDB?** Es el state store de las consultas con estado: la deduplicación del Job 2 y las ventanas del RF3. Guarda el estado en disco y lo respalda en el checkpoint, así que no depende de la memoria del ejecutor (`infra/aws/spark-defaults.conf:41`).

**¿Por qué las ventanas están en otra aplicación?** Para que no compitan por núcleos con la decisión. Además, la decisión usa el planificador FAIR con un pool de peso 10.

### Modelo
**¿Por qué un modelo de Kaggle y no uno propio?** Es la corrección 2 del profesor. Elegimos el notebook de Artur Markosyan (XGBoost con IBM AML) porque usa una partición cronológica y variables causales. Descartamos otros por fugas de información (SMOTE antes de dividir, identificadores de cuenta como variables) o por datos sin cuentas (tarjetas con PCA, PaySim).

**¿Por qué PR-AUC y no accuracy?** Hay 617 casos de lavado en 1.038.880 transacciones del test (0,06 %). Un modelo que dice "nunca es lavado" tiene 99,94 % de accuracy. El PR-AUC de un modelo al azar es 0,0006; el nuestro, 0,267: unas 450 veces mejor.

**¿Precisión de 0,50 y recall de 0,40 es bueno?** En las 5.000 de AWS hubo 8 alertas, 4 de lavado real, y se escaparon 6 casos. En el test completo, 141 de 264 alertas son lavado real (precisión de 0,53, recall de 0,23). Para el analista, que revisa cada alerta, es mucho mejor que revisar al azar. El umbral (0,172, el que maximiza F1 en validación) se calibra según cuántas alertas pueden atender.

**¿Cómo saben que el Job 2 calcula lo mismo que el notebook?** Por la prueba de paridad (riesgo R8): 58 de 58 variables y el score idénticos, y 20.000 de 20.000 decisiones. Hubo que replicar las sumas compensadas de Kahan de pandas, la aritmética de numexpr y el orden por `seq`. En AWS, el score en línea fue idéntico al de fuera de línea en las 5.000.

**¿El modelo ve la etiqueta de lavado?** No. `Is Laundering` no viaja en el evento: se carga aparte en Gold (`etiquetas`) solo para medir precisión y recall.

**¿Cómo se compara con la industria?** Stripe Radar usa una red neuronal con más de 1.000 características, entrenada con miles de millones de pagos, y decide en menos de 100 ms durante la autorización; Visa da un score de 0 a 99 en milisegundos. Ellos bloquean el pago; en lavado de activos la alerta va a analistas, que confirman y reportan a la UIAF. Nuestro modelo es pequeño (58 variables, datos sintéticos, umbral fijo, sin reentrenamiento), y por eso prioriza en lugar de bloquear.

### Almacenamiento
**¿Para qué tres capas?** Bronze guarda el 100 % de lo recibido, sin cambios y con sus coordenadas de Kafka: auditoría y reproceso. Silver tiene los eventos válidos, tipados, en UTC y sin duplicados: la fuente para analítica. Gold tiene el modelo en estrella para consultas, y `risk_decisions`.

**¿Qué es la SCD 2 de `dim_cuenta`?** Cada vez que cambia la entidad o el nivel de riesgo de una cuenta, se cierra la versión vigente (`valido_hasta`, `vigente = false`) y se abre otra. Así, un hecho de ayer se une con el nivel de riesgo que la cuenta tenía ayer. El control de calidad verifica que haya una sola versión vigente por cuenta (`job4_gold.py:114-166`).

**¿Por qué DynamoDB para el log de Delta?** En S3, Delta solo garantiza transacciones si todas las escrituras de una tabla salen del mismo proceso. Aquí varios procesos escriben la misma tabla: el Job 3 y `reprocess_quarantine` en Silver, y OPTIMIZE sobre tablas que el streaming escribe. El `S3DynamoDBLogStore` serializa el commit N con una escritura condicional en DynamoDB. S3 agregó escrituras condicionales en 2024, pero con Delta 3.3.3 el modo multi-clúster documentado es el de DynamoDB.

**¿Cómo manejan los archivos pequeños?** Con OPTIMIZE en el DAG diario `breb_lakehouse_mantenimiento`, y VACUUM con retención de 168 horas. Además, el Job 1 y el Job 3 usan un trigger de 30 s para no crear un archivo por segundo.

**¿Cuánto dura el time travel y cómo auditan después?** 7 días, la retención de VACUUM. Para auditorías posteriores no se depende del time travel: Bronze es append-only, y `risk_decisions` guarda las variables, el score, el umbral y la versión del modelo de cada decisión.

**¿Por qué MongoDB y no DynamoDB, si están en AWS?** DynamoDB sería una opción válida. Elegimos MongoDB en el ADR 04 por tres cosas que el Job 2 usa en cada micro-batch:
- lecturas en lote con `$in`;
- un subdocumento por escritor (`$set` de `streaming`, `ventanas`, `historico` y `estado_retencion`, sin pisarse);
- write y read concern majority.

Con DynamoDB habría que reescribir el almacén (`almacen.py`) con `BatchGetItem` y `UpdateItem` por atributo.

**Varios procesos escriben el mismo documento de cuenta, ¿no se pisan?** No. Cada uno actualiza solo su subdocumento con `$set`: el Job 2, `streaming`; `job2-ventanas`, `ventanas`; Airflow, `historico`; y la entidad, `estado_retencion`. Un `$set` de un campo no toca los demás (riesgo R6).

### Orquestación
**¿Por qué las tareas de Spark de Airflow corren dentro de su contenedor y no en el clúster?** Los jobs de streaming ocupan los núcleos del worker, así que un batch enviado al clúster esperaría núcleos libres. `local[2]` corre dentro del contenedor de Airflow (`src/breb/orquestacion.py:11`). En MWAA se cambiarían por operadores que lanzan trabajos en EMR.

**¿Por qué OPTIMIZE y VACUUM van en un DAG aparte?** VACUUM recorre todos los archivos de todas las tablas en S3, y con los micro-batches del streaming son muchos. Dentro del pipeline de cada 15 minutos eran la mayor parte de la corrida. Ahora `breb_lakehouse_mantenimiento` los corre una vez al día, y la corrida de Gold en AWS tarda 5 min 4 s. Lo que se sacrifica: durante el día se acumulan archivos chicos, y las consultas leen más archivos.

**¿Qué pasa si el pipeline falla a mitad?** Cada tarea tiene hasta 3 reintentos con retroceso exponencial. Las etapas leen Silver en la versión que fijó `check_new_silver_data` (time travel), así que el reintento procesa los mismos datos, y los MERGE de solo inserción no duplican. La versión procesada se registra en MongoDB solo cuando la calidad pasa, así que la siguiente corrida vuelve a intentar lo que faltó. Si la calidad falla, no se sincronizan perfiles.

**¿Qué hace `check_streaming_health`?** Evita armar Gold sobre un streaming atrasado: falla solo si una consulta está activa (con micro-batch en los últimos 5 minutos) y va más de 50.000 offsets atrasada (`salud.py:38-71`). Con el streaming apagado, Silver no cambia y Gold se puede calcular.

**¿Por qué `catchup=False` y `max_active_runs=1`?** Cada corrida procesa todo lo pendiente de Silver, así que no hace falta una corrida por cada intervalo perdido; y dos corridas a la vez competirían por los mismos MERGE.

**¿Qué corrige `reprocess_quarantine` y por qué "no inventa datos"?** Corrige lo que se puede deducir del propio evento:
- quita campos extra y espacios;
- normaliza mayúsculas de cuentas, monedas y canales;
- convierte montos escritos como texto;
- acepta otros formatos de fecha;
- deriva un UUID v5 de las coordenadas de Kafka si falta.

No corrige montos negativos, monedas desconocidas ni cuentas sin banco: eso queda en cuarentena. Los recuperados llegan a Gold sin decisión del modelo, porque no pasaron por el Job 2 (`cuarentena.py:49-100`).

**¿Para qué sirve `bootstrap_state`?** Las variables dependen de toda la historia de cada cuenta. Antes de reproducir el test, hay que cargar en MongoDB el estado de 704.707 cuentas, 1.349.332 pares y 41.815 bancos, calculado con las transacciones anteriores. En AWS tardó 3 min 44 s. El DAG falla si encuentra el streaming corriendo, para no mezclar estados.

### Consumo y gobernanza
**¿Cómo funciona el control de acceso?** Con las reglas de `infra/trino/etc/reglas.json`:
- `analista` y `metabase` leen solo Gold.
- `cumplimiento` lee las tres capas sin poder modificar.
- `breb_admin` registra tablas.
- Cualquier otro usuario no ve nada.

`python -m breb.consumo permisos` prueba 10 casos, y en AWS pasaron los 10.

**Hay que reconocer** que Trino no tiene autenticación: el usuario va en la cabecera de la petición. Lo que lo protege es que solo escucha en `127.0.0.1` de la EC2, y se entra por un túnel SSH abierto a una sola IP. En producción serían autenticación en Trino o Lake Formation con IAM.

**¿Cómo reconstruyen el linaje?** Con el `transaction_uuid`, que pasa sin cambios por todas las capas:
- Bronze, con el tópico, la partición, el offset y la hora de ingesta.
- Silver.
- `risk_decisions`, con el score, el umbral, la versión del modelo y la latencia.
- Los hechos, con el nivel de riesgo de las cuentas.
- La acción de cada entidad en MongoDB.

En AWS tomó 19,5 s; el requisito es menos de 5 minutos (RNF-06).

**¿Qué pasa si la entidad está caída cuando llega una alerta?** El mensaje queda en su cola SQS, que lo retiene 4 días por defecto. La entidad lo borra solo después de registrar la acción. Si falla antes, el mensaje vuelve a aparecer al vencer el tiempo de visibilidad (30 s por defecto) y se procesa de nuevo, sin duplicar la acción.

**¿Cómo se responde cada pregunta de negocio?** Con el tablero de Metabase "Bre-B · detección de fraude" (`tablero.py:95-167`):
- Preguntas 1 a 3: sobre los hechos y las variables de `risk_decisions`.
- Pregunta 4: con `dim_canal` y `dim_entidad_financiera`.
- Además: el desempeño contra las etiquetas, la latencia por minuto y la salud del streaming.

Hay una copia del tablero sobre Athena.

### Seguridad
**¿Qué pasa con la seguridad?**
- **Credenciales:** ninguna llave en archivos; los contenedores usan el rol de la instancia por IMDSv2.
- **En tránsito:** todo el tráfico a S3, DynamoDB, SNS, SQS y Athena va por HTTPS.
- **En reposo:** S3 y DynamoDB cifran por defecto.
- **Red:** el security group solo abre SSH a una IP, y las interfaces web escuchan en `127.0.0.1`.
- **MongoDB:** el usuario de la aplicación solo tiene permisos sobre la base `breb`.
- **Consultas:** las reglas por rol de Trino.

Faltan KMS con llave propia, Secrets Manager para las claves de MongoDB, Airflow y Metabase, y autenticación en Trino. Es el punto 6 de "qué cambiaríamos".

### Uso de agentes de IA
**¿Cómo usaron la IA?** Ver `docs/bitacora_ia.md`. Se delegó a agentes (Kiro y Claude) la escritura de la mayor parte del código, la documentación y la configuración de AWS. El equipo:
- decidió el alcance y las alternativas;
- creó la instancia y corrió todo en AWS;
- validó con pruebas medidas.

Varias decisiones salieron de fallas que el equipo encontró al correr y reportó con la salida exacta, como el `iam:CreateRole` denegado o los checkpoints viejos.

**¿Cómo saben que el código del agente es correcto?** Por lo que se midió, no por quién lo escribió:
- 14 pruebas automáticas (paridad, MongoDB con reintentos, contratos).
- La verificación en AWS: Kafka = Bronze = Silver = `risk_decisions` y el score idéntico en las 5.000.
- `STACK OK`, `PERMISOS OK`, `TABLERO OK` y `ATHENA OK`.
- Los controles de calidad del Job 4.

---

## C. Dónde está cada cosa
Base: https://github.com/Sebaxdenc/breb-fraude/blob/main/

| Qué | Archivo y líneas |
|---|---|
| Configuración del productor (acks, idempotencia) | [src/breb/productor/publicador.py#L143-L144](https://github.com/Sebaxdenc/breb-fraude/blob/main/src/breb/productor/publicador.py#L143-L144) |
| Clave del mensaje (cuenta ordenante) | [src/breb/productor/publicador.py#L126](https://github.com/Sebaxdenc/breb-fraude/blob/main/src/breb/productor/publicador.py#L126) |
| Progreso del productor | [src/breb/productor/__main__.py#L48-L70](https://github.com/Sebaxdenc/breb-fraude/blob/main/src/breb/productor/__main__.py#L48-L70) |
| Creación del tópico | [infra/kafka/crear_topicos.sh#L8-L28](https://github.com/Sebaxdenc/breb-fraude/blob/main/infra/kafka/crear_topicos.sh#L8-L28) |
| Lectura de Kafka y checkpoints | [src/breb/streaming/comun.py#L76-L83](https://github.com/Sebaxdenc/breb-fraude/blob/main/src/breb/streaming/comun.py#L76-L83) |
| Validación del contrato (pandas UDF) | [src/breb/streaming/comun.py#L86-L112](https://github.com/Sebaxdenc/breb-fraude/blob/main/src/breb/streaming/comun.py#L86-L112) |
| Job 1 → Bronze | [src/breb/streaming/job1_bronze.py#L32-L54](https://github.com/Sebaxdenc/breb-fraude/blob/main/src/breb/streaming/job1_bronze.py#L32-L54) |
| Job 2: archivo por micro-batch y consulta de `risk_decisions` | [src/breb/streaming/job2_riesgo.py#L81-L103](https://github.com/Sebaxdenc/breb-fraude/blob/main/src/breb/streaming/job2_riesgo.py#L81-L103) |
| Job 2: publicación en SNS | [src/breb/streaming/job2_riesgo.py#L112-L128](https://github.com/Sebaxdenc/breb-fraude/blob/main/src/breb/streaming/job2_riesgo.py#L112-L128) |
| Job 2: micro-batch, tiempos y deduplicación | [src/breb/streaming/job2_riesgo.py#L138-L174](https://github.com/Sebaxdenc/breb-fraude/blob/main/src/breb/streaming/job2_riesgo.py#L138-L174) |
| Lógica del micro-batch | [src/breb/job2/microbatch.py#L46-L134](https://github.com/Sebaxdenc/breb-fraude/blob/main/src/breb/job2/microbatch.py#L46-L134) |
| Estado idempotente en MongoDB | [src/breb/estado/almacen.py#L29-L80](https://github.com/Sebaxdenc/breb-fraude/blob/main/src/breb/estado/almacen.py#L29-L80) |
| Replica set y timeout de elección | [infra/mongo/iniciar_replica.js#L31-L38](https://github.com/Sebaxdenc/breb-fraude/blob/main/infra/mongo/iniciar_replica.js#L31-L38) |
| Motor de las 58 variables (y la nota de AVX-512) | [src/breb/features/motor.py](https://github.com/Sebaxdenc/breb-fraude/blob/main/src/breb/features/motor.py) |
| Job 3 → Silver (MERGE) | [src/breb/streaming/job3_silver.py#L40-L67](https://github.com/Sebaxdenc/breb-fraude/blob/main/src/breb/streaming/job3_silver.py#L40-L67) |
| Servicio de la entidad (cola, filtro y deduplicación) | [src/breb/entidad/servicio.py#L46-L101](https://github.com/Sebaxdenc/breb-fraude/blob/main/src/breb/entidad/servicio.py#L46-L101) |
| Job 4: SCD 2 de `dim_cuenta` | [src/breb/batch/job4_gold.py#L114-L166](https://github.com/Sebaxdenc/breb-fraude/blob/main/src/breb/batch/job4_gold.py#L114-L166) |
| Job 4: controles de calidad | [src/breb/batch/job4_gold.py#L220-L253](https://github.com/Sebaxdenc/breb-fraude/blob/main/src/breb/batch/job4_gold.py#L220-L253) |
| Job 4: versión fija de Silver | [src/breb/batch/job4_gold.py#L256-L332](https://github.com/Sebaxdenc/breb-fraude/blob/main/src/breb/batch/job4_gold.py#L256-L332) |
| Sensor de versión de Silver | [src/breb/batch/version.py#L13-L36](https://github.com/Sebaxdenc/breb-fraude/blob/main/src/breb/batch/version.py#L13-L36) |
| Salud del streaming | [src/breb/batch/salud.py#L38-L71](https://github.com/Sebaxdenc/breb-fraude/blob/main/src/breb/batch/salud.py#L38-L71) |
| Cuarentena: correcciones y MERGE | [src/breb/batch/cuarentena.py#L49-L168](https://github.com/Sebaxdenc/breb-fraude/blob/main/src/breb/batch/cuarentena.py#L49-L168) |
| OPTIMIZE y VACUUM | [src/breb/batch/mantenimiento.py#L42-L64](https://github.com/Sebaxdenc/breb-fraude/blob/main/src/breb/batch/mantenimiento.py#L42-L64) |
| DAG principal | [dags/breb_lakehouse_pipeline.py](https://github.com/Sebaxdenc/breb-fraude/blob/main/dags/breb_lakehouse_pipeline.py) |
| DAG de mantenimiento | [dags/breb_lakehouse_mantenimiento.py](https://github.com/Sebaxdenc/breb-fraude/blob/main/dags/breb_lakehouse_mantenimiento.py) |
| DAG de cuarentena y de estado inicial | [dags/reprocess_quarantine.py](https://github.com/Sebaxdenc/breb-fraude/blob/main/dags/reprocess_quarantine.py), [dags/bootstrap_state.py](https://github.com/Sebaxdenc/breb-fraude/blob/main/dags/bootstrap_state.py) |
| Modo AWS: compose, endpoints y credenciales | [infra/docker-compose.aws.yml](https://github.com/Sebaxdenc/breb-fraude/blob/main/infra/docker-compose.aws.yml) |
| Preparación de la EC2 (rol, IMDS, prefijo) | [infra/aws/preparar_ec2.sh](https://github.com/Sebaxdenc/breb-fraude/blob/main/infra/aws/preparar_ec2.sh) |
| Spark en AWS: S3 y log de Delta en DynamoDB | [infra/aws/spark-defaults.conf#L10-L24](https://github.com/Sebaxdenc/breb-fraude/blob/main/infra/aws/spark-defaults.conf#L10-L24) |
| Tabla `delta_log` en DynamoDB | [infra/init/crear_recursos.py#L44-L60](https://github.com/Sebaxdenc/breb-fraude/blob/main/infra/init/crear_recursos.py#L44-L60) |
| Nombres de los buckets | [src/breb/buckets.py](https://github.com/Sebaxdenc/breb-fraude/blob/main/src/breb/buckets.py) |
| Athena y Glue | [herramientas/aws/athena.py](https://github.com/Sebaxdenc/breb-fraude/blob/main/herramientas/aws/athena.py) |
| Reglas de acceso de Trino | [infra/trino/etc/reglas.json](https://github.com/Sebaxdenc/breb-fraude/blob/main/infra/trino/etc/reglas.json) |
| Linaje | [src/breb/consumo/linaje.py#L14-L92](https://github.com/Sebaxdenc/breb-fraude/blob/main/src/breb/consumo/linaje.py#L14-L92) |
| Preguntas del tablero | [src/breb/consumo/tablero.py#L95-L167](https://github.com/Sebaxdenc/breb-fraude/blob/main/src/breb/consumo/tablero.py#L95-L167) |
| Prueba de reintento tras falla parcial | [tests/test_job2_microbatch.py#L96-L112](https://github.com/Sebaxdenc/breb-fraude/blob/main/tests/test_job2_microbatch.py#L96-L112) |
| Reinicio y topes de los jobs | [infra/docker-compose.yml#L75-L90](https://github.com/Sebaxdenc/breb-fraude/blob/main/infra/docker-compose.yml#L75-L90) |
