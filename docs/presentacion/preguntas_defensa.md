# Preguntas de la defensa: respuestas y dónde está cada cosa

El profesor pregunta a un integrante por una parte que no implementó, y los demás no pueden ayudar. Todos estudian este archivo completo.

Cada respuesta tiene:
- **Respuesta corta:** lo que se dice en 30 a 45 segundos. Primero la decisión, después la evidencia (un número o un archivo), después lo que se sacrificó.
- **Si piden más:** detalles y la parte del código que se abre.

Los enlaces apuntan a `main` y funcionan cuando se fusionen los PR #20 (Airflow) y #21 (este material). La sección final, "Dónde está cada cosa", reúne los enlaces al código.

---

## A. Las preguntas típicas del profesor

### A1. "¿Por qué eligieron [motor X] y no [motor Y]?"
Respuesta general: "Lo decidimos en el ADR N del Entregable 1 por [razón]; al implementarlo confirmamos o aprendimos [hecho medido]".

| Elegimos | En vez de | Razón del ADR (E1) | Lo que aprendimos al implementarlo |
|---|---|---|---|
| **Kafka** 4.3.2 (KRaft) | Kinesis, RabbitMQ | ADR 01. Spark open source no trae una fuente de Kinesis para Structured Streaming, Kinesis no corre sin internet y ata a AWS. El límite de 1 MB/s es por shard, no por stream, así que no es motivo de descarte (corrección 4). RabbitMQ no está hecho para retener días en disco ni para varios consumidores independientes del mismo flujo. | A 5 MB/s Kafka no es el cuello; el productor logró 1.000 ev/s con p99 de 49 a 235 ms. Cambiamos 3.6 por 4.3.2 porque 3.6 ya no tiene soporte ni imagen oficial. |
| **Spark Structured Streaming** 3.5.9 | Flink | ADR 02. Un solo motor para streaming y batch (Jobs 1 a 4), integración nativa con Delta y curva de aprendizaje; Flink obligaba a mantener dos motores. | Lo que cuesta es el acceso a datos dentro del micro-batch, no el motor: variables y modelo tardan menos de 0,5 s. Con Flink el estado viviría en el operador, lo que ataca el cuello real (ver A2). |
| **MongoDB** 7.0 | Cassandra, Neo4j, (Redis, DynamoDB) | ADR 04. El Job 2 necesita leer y escribir por clave en cada micro-batch; escanear Silver en S3 tarda de 1 a 4 s. Cassandra: leer-modificar-escribir necesita transacciones ligeras o contadores, que no son idempotentes ante reintentos. Neo4j: no es un almacén operacional de alta concurrencia. | Leer de 1.400 a 4.800 transacciones (unos 5 documentos cada una) toma de 0,9 a 6,9 s en el portátil; escribir, de 2,5 a 12,4 s. Es parte del cuello medido. |
| **Delta Lake** 3.3.3 | Iceberg, Parquet sin tabla | ADR 03. ACID, MERGE, time travel, OPTIMIZE y VACUUM; Iceberg era más complejo con Structured Streaming en contenedores locales. Parquet solo no tiene transacciones. | En S3, varios escritores por tabla necesitan el log de commits en DynamoDB, y tuvimos que parchear `S3DynamoDBLogStore` (5 líneas) porque no dejaba configurar el endpoint. |
| **Airflow** 3.3.2 | cron, Prefect, Dagster | ADR 05. Dependencias entre tareas, reintentos con retroceso, historial en un metastore; Airflow 2 dejó de recibir soporte el 22 de abril de 2026. | Las tareas Spark corren con `spark-submit --master local[2]` dentro del contenedor de Airflow: el streaming ocupa los núcleos del worker y así no hay que exponer el socket de Docker. |
| **Trino** 484 + Hive Metastore 4.2.1 | Spark SQL / Thrift Server, DuckDB | ADR 08 (correcciones 5 y 7). Una capa SQL sobre las tres capas con catálogo y permisos por rol; el Thrift Server compite por los núcleos del streaming; DuckDB no tiene catálogo compartido ni control de acceso. | Spark no habla con el metastore: escribe Delta en S3 y Trino registra la tabla una vez con `register_table`; después lee el `_delta_log` en cada consulta y ve los datos nuevos sin volver a registrar. |
| **SNS + SQS** (moto) | FastAPI, un tópico Kafka de alertas | ADR 09 (corrección 6). Push a una cola por entidad, con filtro por el atributo `entidades`; la API no tenía consumidor; con Kafka cada entidad tendría que operar un consumidor de Kafka. | SNS entrega al menos una vez: la entidad descarta repetidas con `alertas_recibidas`. Los analistas usan una cola sin filtro en lugar del correo. |
| Cuarentena **en Bronze** | DLQ en Kafka o en SQS | ADR 10 (corrección 1). La DLQ duplica Bronze y los eventos reinyectados llegan tarde y el watermark los descarta. | `reprocess_quarantine` recuperó 10 de 41 eventos inválidos de la prueba y la segunda corrida no cambió nada. |
| **SeaweedFS** 4.52 | MinIO | D1. MinIO dejó de publicar imágenes de la edición comunitaria. | Pasó la prueba de Delta con dos escritores concurrentes sin perder commits. Hubo que limitar la reserva de volúmenes (`WEED_MASTER_VOLUME_GROWTH_COPY_1=1`). |
| **XGBoost de Kaggle** | Reglas propias, otros modelos | ADR 07 (corrección 2). Notebook público con IBM AML, partición cronológica y variables causales; los demás candidatos tenían fugas de información o datos sin cuentas. | Paridad exacta: 58 de 58 variables y 20.000 de 20.000 decisiones entre el notebook y el Job 2. |

### A2. "Si el volumen de datos se multiplicara por 100, ¿qué cambiarían primero?"
**Respuesta corta:** "Lo primero es el camino crítico del Job 2, porque es el cuello medido, no Kafka. Hoy cada micro-batch valida el contrato con una función de Python y va a MongoDB a leer y escribir el estado. Con cien veces más eventos, validaríamos con expresiones de Spark y moveríamos el estado al state store de Spark, particionado por cuenta, para que el estado viva donde se calcula. MongoDB quedaría para los perfiles y las consultas, fragmentado por hash de la cuenta. Después, más particiones de Kafka."

**Si piden más** (lámina 14):
- Referencia: el promedio del día récord son 120 ev/s; por 100, 12.000 ev/s, unos 12 MB/s con eventos de 1 KB. Kafka no tiene problema con 12 MB/s; lo que importa es el paralelismo.
- Kafka: de 6 a 48 o 96 particiones. Hay que definirlas antes de producción: cambiar el número de particiones cambia a qué partición va cada clave y rompe el orden por cuenta durante la transición.
- Spark: un clúster por job con autoescalado (EMR o Kubernetes). `spark.sql.shuffle.partitions` queda fijo en el checkpoint de una consulta con estado; hay que planearlo antes, porque cambiarlo obliga a un checkpoint nuevo.
- Lakehouse: hoy `risk_decisions` y Gold no están particionados. Con 100 veces más datos: particionar por fecha o usar liquid clustering, y OPTIMIZE por partición.
- Job 4: hoy busca los pendientes con un anti-join de Silver contra todos los hechos; con el Change Data Feed de Silver procesaría solo lo nuevo.
- Consumo: Athena o un clúster de Trino.
- **La tensión de diseño que hay que mencionar:** cada transacción toca el estado de dos cuentas (ordenante y destino), un par y dos bancos. Si se particiona el cálculo por cuenta ordenante, las variables de la cuenta destino quedan en otra partición. Habría que propagar ese estado con mensajes (consistencia eventual de milisegundos) y probablemente reentrenar el modelo con variables que se puedan calcular de forma distribuida. El Entregable 1 ya lo anticipaba: "a escala de producción habría que particionarlo por cuenta".

### A3. "¿Cómo garantizan que un evento no se procese dos veces si el job de Spark falla a mitad?"
**Respuesta corta:** "No prometemos exactly-once en el transporte: Kafka entrega al menos una vez y, si Spark falla, repite el micro-batch con los mismos offsets del checkpoint. Lo que garantizamos es que repetirlo no cambia el resultado. En MongoDB, cada documento guarda el número de micro-batch que lo escribió y una copia del estado anterior, `streaming_prev`. Si el micro-batch B se repite, los documentos que ya tienen batch_id = B se leen desde `streaming_prev`, así que el cálculo parte del mismo estado y da lo mismo. Lo probamos cortando la escritura a la mitad."

**Si piden el código** (abrir en este orden):
1. `src/breb/estado/almacen.py`, líneas 34 a 57 (`cargar`): si `streaming.batch_id == B`, usa `streaming_prev`; si es menor, usa `streaming`; si es mayor, falla (checkpoints borrados con el estado intacto).
2. Líneas 59 a 80 (`guardar`): `$set` de `streaming` con `batch_id = B` y de `streaming_prev` con el estado anterior, en lotes de 5.000 sin orden.
3. `tests/test_job2_microbatch.py`, línea 96: la escritura falla después de 300 documentos y el reintento deja exactamente el mismo estado que sin falla.

El resto de los efectos:

| Efecto | Mecanismo | Código |
|---|---|---|
| Duplicados del productor | `acks=all` y `enable.idempotence` | `publicador.py:143-144` |
| Duplicados del mismo evento | `dropDuplicatesWithinWatermark` por transaction_uuid (watermark de 1 h) y `drop_duplicates` dentro del lote | `job2_riesgo.py:168-169` |
| Bronze | Sink de Delta con checkpoint: un commit por batchId | `job1_bronze.py:44-49` |
| risk_decisions | `batch_<id>.parquet` con nombre fijo (escritura atómica con `os.replace`); otra consulta lo agrega a Delta con su checkpoint | `job2_riesgo.py:81-103` |
| Alertas | SNS puede repetir; la entidad registra `{entidad}:{transaction_uuid}` antes de borrar el mensaje y descarta las repetidas | `servicio.py:78-101` |
| Silver | MERGE por transaction_uuid que solo inserta | `job3_silver.py:47-56` |
| Gold | MERGE de solo inserción por clave; las tres etapas leen Silver en la versión que fijó el sensor | `job4_gold.py:58-64`, `job4_gold.py:259-260` |

**Aclaración:** el Entregable 1 decía que `risk_decisions` usaría `txnAppId` y `txnVersion` de Delta. Lo cambiamos en la fase 4: escribir en Delta dentro de la decisión tardaba de 6 a 19 s por micro-batch, así que la decisión escribe un archivo local y otra consulta hace el commit cada 10 s. La garantía es la misma (fuente de archivos y sink de Delta con checkpoint).

### A4. "¿Cuál es la posición CAP del componente más crítico de su sistema y qué sacrificaron?"
**Respuesta corta:** "El componente más crítico es MongoDB, porque guarda el estado con el que el Job 2 calcula las variables. Es CP: write concern majority y read concern majority. Una escritura confirmada sobrevive a la caída del primario, y no leemos estado que podría revertirse. Sacrificamos disponibilidad: durante la elección de un nuevo primario, el Job 2 no puede escribir y la latencia sube. Bajamos el timeout de elección a 2 segundos."

**Si piden más:**
- Por qué no AP: con `w:1`, una escritura confirmada puede perderse si el primario cae antes de replicarla. El estado son acumulados (conteos, sumas, contrapartes); quedarían desalineados con los eventos ya procesados, y las variables del modelo cambiarían.
- Código: `src/breb/estado/almacen.py:30` (`w="majority", read_concern="majority"`) e `infra/mongo/iniciar_replica.js:34` (`electionTimeoutMillis: 2000`).
- Kafka también está configurado hacia la consistencia: en el perfil completo, `replication.factor=3`, `min.insync.replicas=2` y `acks=all`. Con dos brókers caídos, el productor no puede escribir: prefiere fallar a confirmar algo que podría perderse. La elección de un líder no limpio está apagada (valor por defecto de Kafka).
- Delta en S3: el log de commits en DynamoDB serializa los commits de varios escritores; sin él, dos escritores podrían pisarse el mismo `N.json`.
- **Lo que hay que reconocer:** en el perfil dev, MongoDB y Kafka tienen un solo nodo, así que no hay tolerancia a particiones; el perfil completo tiene 3 de cada uno. La prueba de aislar el primario y medir la elección (RNF-04) está pendiente. La consulta de ventanas (`job2-ventanas`) usa los valores por defecto del driver.

### A5. "¿Qué pasa con el pipeline si el broker de Kafka cae? Muéstrenme en el código cómo lo manejan."
**Respuesta corta:** "Depende del perfil. En el completo hay tres brókers con factor de replicación 3 y `min.insync.replicas=2`: si cae uno, el productor con `acks=all` sigue escribiendo y los jobs siguen leyendo de los líderes nuevos. En dev hay un solo bróker: el productor reintenta hasta 5 minutos sin dar por enviado nada que el bróker no confirmó, la consulta de Spark falla, Docker reinicia el contenedor y el job sigue desde los offsets de su checkpoint cuando Kafka vuelve. No se pierde nada mientras el tópico retenga los mensajes: 7 días."

**Código, en este orden:**
1. Productor, `src/breb/productor/publicador.py:143-144`: `acks: all`, `enable.idempotence: True`. Sin `retries` explícito: con idempotencia, librdkafka reintenta hasta `message.timeout.ms` (300 s por defecto). El progreso (`progreso.json`) solo avanza si no quedan mensajes sin confirmar (`__main__.py:65-66`).
2. Tópico, `infra/kafka/crear_topicos.sh:10-14` y `:28`: factor de replicación, `min.insync.replicas` y 7 días de retención, con valores de `infra/.env` (1 y 1) o `infra/completo.env` (3 y 2).
3. Lectura en Spark, `src/breb/streaming/comun.py:74-81`: `startingOffsets=earliest` solo la primera vez; después mandan los offsets del checkpoint (`CHECKPOINTS`, línea 28). `failOnDataLoss` queda en su valor por defecto (true): si Kafka hubiera borrado offsets que el job no leyó, el job falla en lugar de saltarse datos en silencio.
4. Reinicio, `infra/docker-compose.yml:79`: `restart: unless-stopped` en los jobs; `job2_riesgo.py:250` (`awaitAnyTermination`) hace que el proceso termine si una consulta falla.

**Lo que hay que reconocer:** la prueba de tumbar un bróker durante la carga (RNF-01 y RNF-03) está en el plan de la fase 7 y no se ha corrido. No hay lógica de reintentos propia: se usan los mecanismos de librdkafka, Spark y Docker.

### A6. "¿Por qué la partition key de [tabla NoSQL] es [campo X] y no [campo Y]?"
**MongoDB** (`_id` de cada colección):

| Colección | `_id` | Por qué no otra cosa |
|---|---|---|
| `cuentas` | El identificador de la cuenta (banco + cuenta en IBM AML) | El Job 2 lee por cuenta, en lote, con `{_id: {$in: [...]}}`, que usa el índice por defecto. En producción se fragmenta por **hash** de la cuenta: reparte uniformemente las escrituras. Una clave por rango o por fecha concentraría las escrituras en un fragmento; una clave por banco tiene muy pocos valores y deja fragmentos enormes. |
| `pares` | `"origen\|destino"` (`motor.py:66`) | El par es dirigido: A→B y B→A son estados distintos (primer envío, montos acumulados del par). Una sola cadena determinista permite leerlo por `_id`. |
| `bancos` | El código del banco | Hay pocos bancos y se leen todos los del lote. |
| `alertas_recibidas` | `"entidad:transaction_uuid"` (`servicio.py:81`) | La misma alerta llega a la entidad de origen y a la de destino; cada una la registra una vez. El `_id` único hace que una reentrega choque con `DuplicateKeyError`. |
| `sincronizacion` | `"gold"` y `"perfiles"` | Un documento por marca de agua: la última versión de Silver procesada y la última fecha sincronizada. |

**Kafka** (clave del mensaje = `source_account`, `publicador.py:126`): conserva el orden de las transacciones de cada ordenante en su partición y reparte la carga entre particiones. Con `destination_account`, las cuentas mula (muchos ordenantes hacia una receptora) harían una partición caliente. Con `transaction_uuid` se pierde el orden por cuenta. El Job 2 además ordena cada micro-batch por `seq`, porque las variables dependen del orden global.

**DynamoDB `delta_log`** (`crear_recursos.py:47-51`): clave de partición `tablePath` y de orden `fileName`, el esquema que exige `S3DynamoDBLogStore`. Una escritura condicional sobre (tabla, archivo de commit) garantiza que solo un escritor cree el commit N.

**Delta:** Bronze se particiona por la fecha de **ingesta** (llega en orden de escritura y es append-only); Silver, por la fecha del **evento** (las consultas filtran por cuándo ocurrió la transacción, y el MERGE incluye la partición para no escanear todo).

---

## B. Otras preguntas probables, por componente

### Ingesta y Kafka
**¿Por qué no hay API de ingesta, si el E1 la tenía?** Decisión D3: la API no tenía un consumidor real dentro del alcance (el switch de Bre-B no existe en el proyecto). El productor publica directo en Kafka con la misma clave y el mismo contrato; en producción, el switch o un conector harían ese papel.

**¿Cómo conservan el orden de las transacciones?** El productor publica en orden de `seq` (número de fila del CSV) con clave `source_account`, y la idempotencia mantiene el orden por partición en los reintentos. Entre particiones no hay orden, así que el Job 2 ordena cada micro-batch por `seq` (`microbatch.py:46`). Decisión D2: con `maxOffsetsPerTrigger`, Spark reparte el tope entre particiones y algunas transacciones quedaban en otro micro-batch; en la prueba cambiaron variables en 226 de 9.204 transacciones. Sin el tope, las 20.000 del golden sample salieron idénticas.

**¿Por qué `message.timestamp.type=LogAppendTime`?** Para medir la latencia desde que el bróker escribió el mensaje, con un reloj que el productor no controla. RF1 = LogAppendTime − `produced_at`; RNF-02 = `decision_ts` − LogAppendTime.

**¿Por qué el `transaction_uuid` es UUID v5?** Es determinista (se calcula a partir del `seq`): el mismo evento siempre tiene el mismo identificador, así que un reenvío o un reproceso se deduplica. Con v4 (aleatorio), publicar dos veces el mismo evento daría dos identificadores.

### Streaming
**¿Por qué el Job 2 lee Kafka y no Bronze?** Leer Bronze sumaría el trigger de 30 s del Job 1 y la escritura en Delta a la latencia. Es una arquitectura Kappa: los dos jobs leen el mismo log de forma independiente, y la lógica de detección vive una sola vez, en el Job 2.

**¿Por qué el Job 3 no usa `dropDuplicatesWithinWatermark` como dice el RF6?** La reproducción comprime el tiempo unas 155 veces: un watermark de 10 minutos de tiempo del evento equivale a segundos reales, y descartaría eventos de una partición demorada. El MERGE por transaction_uuid ya garantiza que no haya duplicados (`job3_silver.py:7-10`).

**¿Para qué sirve RocksDB?** Es el state store de las consultas con estado: la deduplicación del Job 2 y las ventanas del RF3. Guarda el estado en disco y lo respalda en el checkpoint, así que no depende de la memoria del ejecutor (`spark-defaults.conf:41`).

**¿Por qué las ventanas están en otra aplicación?** Para que no compitan por núcleos con la decisión: el Job 2 de decisiones tiene 2 núcleos propios, y además usa el planificador FAIR con un pool de peso 10 para las decisiones.

**¿Por qué no se cumple la latencia?** Ver lámina 11: variables y modelo tardan menos de 0,5 s; leer Kafka y validar en Python, de 2,7 a 62 s; MongoDB, hasta 12,4 s. El portátil está saturado (el productor vio 5,5 s de p99 hacia Kafka con los jobs corriendo). Bajamos de 285 a 99 s sacando `risk_decisions` y las ventanas del camino crítico; a 50 ev/s, 53 s. Cambios en orden: validación con expresiones de Spark, escribir solo los campos que cambian en MongoDB, medir en un equipo con más núcleos.

**¿Por qué la validación es una pandas UDF con jsonschema?** Es la misma función del contrato que usan el productor y las pruebas (`breb.contratos.errores_evento`), así que hay una sola definición de "evento válido". El costo es el rendimiento: es el tramo más lento, y es lo primero que cambiaríamos.

**¿Cómo midieron la latencia?** Por evento: `decision_ts − kafka_timestamp`, donde `decision_ts` se toma después de publicar las alertas en SNS (`job2_riesgo.py:149-154`). Se guarda en `risk_decisions.latencia_ms` y el tablero la muestra por minuto.

### Modelo
**¿Por qué un modelo de Kaggle y no uno propio?** Corrección 2 del profesor. Elegimos el notebook de Artur Markosyan (XGBoost con IBM AML) porque usa una partición cronológica y variables causales; descartamos otros por fugas de información (SMOTE antes de dividir, identificadores de cuenta como variables) o por datos sin cuentas (tarjetas con PCA, PaySim).

**¿Por qué PR-AUC y no accuracy?** Hay 617 casos de lavado en 1.038.880 transacciones del test (0,06 %). Un modelo que dice "nunca es lavado" tiene 99,94 % de accuracy. El PR-AUC de un modelo al azar es 0,0006; el nuestro, 0,267: unas 450 veces mejor.

**¿Precisión y recall de 0,52 es bueno?** En el tramo de la demo, 13 de las 25 alertas son lavado real. En el test completo, 141 de 264 alertas (precisión 0,53, recall 0,23). Para el analista, que revisa cada alerta, es mucho mejor que revisar al azar; el umbral (0,172, el que maximiza F1 en validación) se calibra según cuántas alertas puede atender.

**¿Cómo saben que el Job 2 calcula lo mismo que el notebook?** Prueba de paridad (riesgo R8): 58 de 58 variables y el score idénticos bit a bit, en Kaggle con NumPy 2 y en Docker con NumPy 1.26. Hubo que replicar las sumas compensadas de Kahan de pandas, la aritmética de numexpr (con más de un millón de filas, pandas promueve a float64 y calcula `x ** 2` con `pow()`) y el orden por `seq`. En una CPU con AVX-512, `np.log1p` en float32 difiere en el último bit en dos variables; el score no cambia.

**¿El modelo ve la etiqueta de lavado?** No. `Is Laundering` no viaja en el evento: se carga aparte en Gold (`etiquetas`) solo para medir precisión y recall.

### Almacenamiento
**¿Para qué tres capas?** Bronze guarda el 100 % de lo recibido, sin cambios y con sus coordenadas de Kafka: auditoría y reproceso. Silver tiene los eventos válidos, tipados, en UTC y sin duplicados: la fuente para analítica. Gold tiene el modelo en estrella para consultas y `risk_decisions`.

**¿Qué es la SCD 2 de `dim_cuenta`?** Cada vez que cambia la entidad o el nivel de riesgo de una cuenta, se cierra la versión vigente (`valido_hasta`, `vigente = false`) y se abre otra. Así un hecho de ayer se une con el nivel de riesgo que la cuenta tenía ayer. El control de calidad verifica que haya una sola versión vigente por cuenta (`job4_gold.py:113-165`).

**¿Por qué DynamoDB para el log de Delta?** En S3, Delta solo garantiza transacciones si todas las escrituras de una tabla salen del mismo driver: S3 no tenía una escritura "solo si no existe" para `_delta_log/N.json`. Aquí varios procesos escriben la misma tabla (Job 3 y `reprocess_quarantine` en Silver; OPTIMIZE sobre tablas que el streaming escribe), así que el `S3DynamoDBLogStore` serializa el commit N con una escritura condicional en DynamoDB. S3 agregó escrituras condicionales en 2024; con Delta 3.3.3 usamos el LogStore de DynamoDB, que es el modo multi-clúster que documenta Delta.

**¿Cómo manejan los archivos pequeños?** OPTIMIZE en el DAG (en la prueba, Silver pasó de 33 a 10 archivos) y VACUUM con retención de 168 horas. El Job 1 usa un trigger de 30 s para no crear un archivo por segundo.

**¿Cuánto dura el time travel y cómo auditan después?** 7 días, la retención de VACUUM. Para auditorías posteriores no se depende del time travel: Bronze es append-only y `risk_decisions` guarda las variables, el score, el umbral y la versión del modelo de cada decisión.

**¿Por qué MongoDB y no Redis o DynamoDB?** Redis vive en memoria y su durabilidad depende de la configuración de persistencia; necesitábamos confirmación por mayoría. DynamoDB sería una opción válida en AWS, pero no tiene un motor igual para correr en local (DynamoDB Local es para desarrollo). MongoDB da documentos con un subdocumento por escritor (`$set` sin pisarse), lecturas en lote con `$in` y el mismo motor en local y en Atlas.

**Tres procesos escriben el mismo documento de cuenta, ¿no se pisan?** Cada uno actualiza solo su subdocumento con `$set`: el Job 2 `streaming`, job2-ventanas `ventanas`, Airflow `historico` y la entidad `estado_retencion`. Un `$set` de un campo no toca los demás (riesgo R6).

### Orquestación
**¿Por qué las tareas Spark de Airflow corren en local y no en el clúster?** Los jobs de streaming ocupan todos los núcleos del worker; un batch enviado al clúster esperaría núcleos libres. `local[2]` con 2 GB de driver corre dentro del contenedor de Airflow. Con más recursos se cambia `BREB_SPARK_MASTER=spark://spark-master:7077` (`orquestacion.py:11`).

**¿Qué pasa si el pipeline falla a mitad?** Cada tarea tiene hasta 3 reintentos con retroceso exponencial. Las etapas leen Silver en la versión que fijó `check_new_silver_data` (time travel), así que el reintento procesa los mismos datos; los MERGE de solo inserción no duplican. La versión procesada se registra en MongoDB solo cuando la calidad pasa, así que la siguiente corrida vuelve a intentar lo que faltó. Si la calidad falla, no se sincronizan perfiles.

**¿Por qué `check_streaming_health` no bloquea si el streaming está apagado?** Su objetivo es no armar Gold sobre un streaming atrasado: falla solo si una consulta está activa (micro-batch en los últimos 5 minutos) y va más de 50.000 offsets atrasada (`salud.py:38-71`). Con el streaming apagado, Silver no cambia y Gold se puede calcular.

**¿Por qué `catchup=False` y `max_active_runs=1`?** Cada corrida procesa todo lo pendiente de Silver, así que no hace falta una corrida por cada intervalo perdido; y dos corridas a la vez competirían por los mismos MERGE.

**¿Qué corrige `reprocess_quarantine` y por qué "no inventa datos"?** Quita campos extra y espacios, normaliza mayúsculas de cuentas, monedas y canales, convierte montos escritos como texto, acepta otros formatos de fecha y deriva un UUID v5 de las coordenadas de Kafka si falta. No corrige montos negativos, monedas desconocidas ni cuentas sin banco: eso queda en cuarentena. Los recuperados llegan a Gold sin decisión del modelo, porque no pasaron por el Job 2 (`cuarentena.py:49-100`).

**¿Para qué sirve `bootstrap_state`?** Las variables dependen de toda la historia de cada cuenta. Antes de reproducir el test, hay que cargar en MongoDB el estado de 704.707 cuentas, 1.349.332 pares y 41.815 bancos calculado con las transacciones anteriores. Calcularlo pide unos 5,5 GB (decisión D4), por eso el DAG falla si encuentra el streaming corriendo.

### Consumo y gobernanza
**¿Cómo funciona el control de acceso en Trino?** Reglas en `infra/trino/etc/reglas.json`: `analista` y `metabase` leen solo Gold; `cumplimiento` lee las tres capas sin poder modificar; `breb_admin` registra tablas; cualquier otro usuario no ve nada. `python -m breb.consumo permisos` prueba 10 casos. **Hay que reconocer** que en local no hay autenticación (el usuario va en la cabecera de la petición); en AWS lo harían IAM y Lake Formation.

**¿Cómo reconstruyen el linaje?** Con el `transaction_uuid`, que pasa sin cambios por todas las capas: Bronze (tópico, partición, offset, hora de ingesta), Silver, `risk_decisions` (score, umbral, versión del modelo, latencia), los hechos con el nivel de riesgo de las cuentas, y la acción de cada entidad en MongoDB. Toma 6 s; el requisito era menos de 5 minutos (RNF-06).

**¿Qué pasa si la entidad está caída cuando llega una alerta?** El mensaje queda en su cola SQS (retención por defecto de 4 días). La entidad lo borra solo después de registrar la acción; si falla antes, el mensaje vuelve a aparecer al vencer el tiempo de visibilidad (30 s por defecto) y se procesa de nuevo, sin duplicar la acción.

**¿Cómo se responde cada pregunta de negocio?** Con el tablero de Metabase "Bre-B · detección de fraude": preguntas 1 a 3 sobre los hechos y las variables de `risk_decisions`, pregunta 4 con `dim_canal` y `dim_entidad_financiera`, más el desempeño contra las etiquetas, la latencia por minuto y la salud del streaming (`tablero.py:95-167`).

### Infraestructura y DataOps
**¿Por qué Docker Compose y no AWS?** ADR 06: el stack gestionado en AWS supera 300 USD al mes, MSK y MWAA no tienen capa gratuita, y la demo debía funcionar sin depender de la red. Usamos el mismo motor open source de cada servicio, así que migrar es cambiar endpoints y credenciales. La lámina 4 muestra el equivalente de cada caja.

**¿Cuánta memoria usa?** WSL tiene 11,68 GiB. Medido: `dev` + `consumo` en reposo, 4,03 GiB; `completo` con la prueba de Delta, 4,59 GiB. Los topes suman más que eso, por eso la demo tiene un plan B que apaga piezas. Falta medir todo junto con los Jobs 1 a 3 (riesgo R5).

**¿Cómo se corre desde cero?** `README.md`, sección "Correr el pipeline desde cero": clonar, construir las imágenes, levantar el stack, cargar el estado, levantar el streaming, publicar y correr el pipeline. El `docker-compose.yml` de la raíz incluye `infra/docker-compose.yml`.

**¿Qué pasa con la seguridad?** Credenciales locales en `infra/credenciales/` (solo para este entorno), puertos publicados únicamente en 127.0.0.1, usuario de MongoDB con permisos solo sobre la base `breb` y reglas por rol en Trino. TLS, KMS y Secrets Manager (RNF-08) son parte del diseño en AWS y no están implementados en local.

### Uso de agentes de IA
**¿Cómo usaron la IA?** Ver `docs/bitacora_ia.md`. Se delegó la escritura de la mayor parte del código y de la documentación a un agente (Kiro). El equipo decidió el alcance y las alternativas, corrió todo en Docker en el portátil de la demo y en Kaggle, y validó con pruebas medidas: la paridad bit a bit con el notebook, el conteo sin duplicados y las corridas en Docker. Varias decisiones salieron de fallas que el equipo encontró al correr (DNS, memoria, el apagado de perfiles) y reportó con la salida exacta.

**¿Cómo saben que el código del agente es correcto?** Por lo que se midió, no por quién lo escribió: 14 pruebas automáticas (paridad, MongoDB con reintentos, contratos), la verificación de la fase 4 en Docker (Kafka = Bronze = Silver = `risk_decisions`, paridad en línea de 20.000 transacciones) y los controles de calidad del Job 4.

---

## C. Dónde está cada cosa
Base: https://github.com/Sebaxdenc/breb-fraude/blob/main/

| Qué | Archivo y líneas |
|---|---|
| Configuración del productor (acks, idempotencia) | [src/breb/productor/publicador.py#L143-L144](https://github.com/Sebaxdenc/breb-fraude/blob/main/src/breb/productor/publicador.py#L143-L144) |
| Combinación de `--tramo`, `--continuar` y `--max` | [src/breb/productor/__main__.py#L48-L66](https://github.com/Sebaxdenc/breb-fraude/blob/main/src/breb/productor/__main__.py#L48-L66) |
| Creación del tópico | [infra/kafka/crear_topicos.sh#L8-L28](https://github.com/Sebaxdenc/breb-fraude/blob/main/infra/kafka/crear_topicos.sh#L8-L28) |
| Lectura de Kafka y checkpoints | [src/breb/streaming/comun.py#L74-L81](https://github.com/Sebaxdenc/breb-fraude/blob/main/src/breb/streaming/comun.py#L74-L81) |
| Validación del contrato (pandas UDF) | [src/breb/streaming/comun.py#L84-L110](https://github.com/Sebaxdenc/breb-fraude/blob/main/src/breb/streaming/comun.py#L84-L110) |
| Job 1 → Bronze | [src/breb/streaming/job1_bronze.py#L32-L54](https://github.com/Sebaxdenc/breb-fraude/blob/main/src/breb/streaming/job1_bronze.py#L32-L54) |
| Job 2: archivo por micro-batch y consulta de risk_decisions | [src/breb/streaming/job2_riesgo.py#L81-L103](https://github.com/Sebaxdenc/breb-fraude/blob/main/src/breb/streaming/job2_riesgo.py#L81-L103) |
| Job 2: publicación en SNS | [src/breb/streaming/job2_riesgo.py#L112-L128](https://github.com/Sebaxdenc/breb-fraude/blob/main/src/breb/streaming/job2_riesgo.py#L112-L128) |
| Job 2: micro-batch, tiempos y deduplicación | [src/breb/streaming/job2_riesgo.py#L138-L173](https://github.com/Sebaxdenc/breb-fraude/blob/main/src/breb/streaming/job2_riesgo.py#L138-L173) |
| Lógica del micro-batch sin Spark | [src/breb/job2/microbatch.py#L46-L134](https://github.com/Sebaxdenc/breb-fraude/blob/main/src/breb/job2/microbatch.py#L46-L134) |
| Estado idempotente en MongoDB | [src/breb/estado/almacen.py#L29-L80](https://github.com/Sebaxdenc/breb-fraude/blob/main/src/breb/estado/almacen.py#L29-L80) |
| Replica set y timeout de elección | [infra/mongo/iniciar_replica.js#L31-L38](https://github.com/Sebaxdenc/breb-fraude/blob/main/infra/mongo/iniciar_replica.js#L31-L38) |
| Motor de las 58 variables | [src/breb/features/motor.py](https://github.com/Sebaxdenc/breb-fraude/blob/main/src/breb/features/motor.py) |
| Job 3 → Silver (MERGE) | [src/breb/streaming/job3_silver.py#L40-L67](https://github.com/Sebaxdenc/breb-fraude/blob/main/src/breb/streaming/job3_silver.py#L40-L67) |
| Servicio de la entidad (deduplicación) | [src/breb/entidad/servicio.py#L78-L101](https://github.com/Sebaxdenc/breb-fraude/blob/main/src/breb/entidad/servicio.py#L78-L101) |
| Job 4: SCD 2 de dim_cuenta | [src/breb/batch/job4_gold.py#L113-L165](https://github.com/Sebaxdenc/breb-fraude/blob/main/src/breb/batch/job4_gold.py#L113-L165) |
| Job 4: controles de calidad | [src/breb/batch/job4_gold.py#L219-L252](https://github.com/Sebaxdenc/breb-fraude/blob/main/src/breb/batch/job4_gold.py#L219-L252) |
| Job 4: versión fija de Silver | [src/breb/batch/job4_gold.py#L255-L331](https://github.com/Sebaxdenc/breb-fraude/blob/main/src/breb/batch/job4_gold.py#L255-L331) |
| Sensor de versión de Silver | [src/breb/batch/version.py#L13-L36](https://github.com/Sebaxdenc/breb-fraude/blob/main/src/breb/batch/version.py#L13-L36) |
| Salud del streaming | [src/breb/batch/salud.py#L38-L71](https://github.com/Sebaxdenc/breb-fraude/blob/main/src/breb/batch/salud.py#L38-L71) |
| Cuarentena: correcciones y MERGE | [src/breb/batch/cuarentena.py#L49-L168](https://github.com/Sebaxdenc/breb-fraude/blob/main/src/breb/batch/cuarentena.py#L49-L168) |
| OPTIMIZE y VACUUM | [src/breb/batch/mantenimiento.py#L42-L64](https://github.com/Sebaxdenc/breb-fraude/blob/main/src/breb/batch/mantenimiento.py#L42-L64) |
| DAG principal | [dags/breb_lakehouse_pipeline.py](https://github.com/Sebaxdenc/breb-fraude/blob/main/dags/breb_lakehouse_pipeline.py) |
| DAG de cuarentena y de estado inicial | [dags/reprocess_quarantine.py](https://github.com/Sebaxdenc/breb-fraude/blob/main/dags/reprocess_quarantine.py), [dags/bootstrap_state.py](https://github.com/Sebaxdenc/breb-fraude/blob/main/dags/bootstrap_state.py) |
| Log de Delta en DynamoDB | [infra/spark/conf/spark-defaults.conf#L18-L24](https://github.com/Sebaxdenc/breb-fraude/blob/main/infra/spark/conf/spark-defaults.conf#L18-L24), [infra/init/crear_recursos.py#L39-L58](https://github.com/Sebaxdenc/breb-fraude/blob/main/infra/init/crear_recursos.py#L39-L58) |
| Reglas de acceso de Trino | [infra/trino/etc/reglas.json](https://github.com/Sebaxdenc/breb-fraude/blob/main/infra/trino/etc/reglas.json) |
| Linaje | [src/breb/consumo/linaje.py#L14-L92](https://github.com/Sebaxdenc/breb-fraude/blob/main/src/breb/consumo/linaje.py#L14-L92) |
| Preguntas del tablero | [src/breb/consumo/tablero.py#L95-L167](https://github.com/Sebaxdenc/breb-fraude/blob/main/src/breb/consumo/tablero.py#L95-L167) |
| Prueba de reintento tras falla parcial | [tests/test_job2_microbatch.py#L96-L112](https://github.com/Sebaxdenc/breb-fraude/blob/main/tests/test_job2_microbatch.py#L96-L112) |
| Topes de memoria y reinicio de los jobs | [infra/docker-compose.yml#L75-L90](https://github.com/Sebaxdenc/breb-fraude/blob/main/infra/docker-compose.yml#L75-L90) |
