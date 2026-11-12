# Guion de la sustentación final · Bre-B en AWS

Sesión de 25 minutos: **10 min de presentación + 8 min de demo + 7 min de defensa**. Lo que se evalúa es justificar cada decisión de diseño, así que el guion repite una misma estructura: la decisión, la alternativa descartada, la evidencia medida y lo que se sacrificó.

Todo el guion habla del sistema desplegado en AWS (AWS Academy, us-east-1). Las cifras son de la corrida en AWS: EC2 `r5.large` de 2 vCPU, 5.000 transacciones a 20 ev/s.

Materiales:
- `Bre-B_sustentacion_final.pdf`: las 15 diapositivas.
- `preguntas_defensa.md`: respuestas a las preguntas del profesor, con el archivo y la línea del código.
- `../arquitectura/arquitectura_aws.md`: la arquitectura y el recorrido completo del pipeline, para estudiar.
- `../../infra/aws/README.md`: cómo se despliega en AWS.

## Reparto

| Integrante | Rol | Diapositivas | Tiempo | En la demo |
|---|---|---|---|---|
| Sebastian Medina | Consumo y gobernanza | 1, 2, 3 y 11 | 2:25 | Alertas, Gold en Athena y Metabase, linaje |
| Samuel Arango | Ingesta y streaming | 4, 6 y 12 | 2:40 | Productor, Spark y Job 2 |
| Nathalia Cardoza | DataOps | 5, 7 y 10 | 2:25 | Opera la terminal y la consola de AWS durante toda la demo |
| Mateo Sanz | Almacenamiento | 8, 9 y 13 | 2:10 | Airflow y Gold |

Total de la presentación: 9:40, a un ritmo de unas 150 palabras por minuto. Cronométrenlo en el ensayo. El profesor pregunta a cada integrante por partes que **no** implementó, así que todos estudian `preguntas_defensa.md` completo.

Reglas para hablar:
- Decir el número, no el adjetivo: "86 segundos de p99", no "la latencia es alta".
- Señalar la diapositiva mientras se habla del diagrama o de la tabla.
- No leer. El texto de abajo es una guía: digan las mismas ideas con sus palabras.
- Cada uno termina su parte nombrando a quien sigue.

---

## Bloque 1 · Presentación (10 min)

### Diapositiva 1 · Portada — Sebastian (0:25)
> Buenos días, profe. Somos Samuel, Mateo, Nathalia y yo, Sebastian. Nuestro caso es Bre-B, el sistema de pagos inmediatos del Banco de la República. La pregunta que guía el proyecto es esta: dado el fraude en pagos inmediatos, ¿qué pipeline construiríamos y por qué exactamente así? En diez minutos les mostramos qué construimos en AWS, qué cambió desde el Entregable 1 y qué no logramos.

### Diapositiva 2 · En Bre-B el fraude se consuma en minutos — Sebastian (0:40)
> Bre-B acredita una transferencia en unos 20 segundos, todos los días del año. En su primer año pasó de 1.700 millones de transacciones, y el día récord tuvo 10,4 millones, unas 120 por segundo. El problema es que las redes de mulas reparten el dinero entre varias entidades en minutos, y un pago inmediato no se puede reversar. La banca analiza por lotes, con cortes de 12 a 24 horas: llega tarde. Por eso nuestro sistema decide en segundos y avisa a la entidad para que retenga los fondos. Tiene tres usuarios: la entidad financiera, los analistas de fraude y cumplimiento.

### Diapositiva 3 · Cuatro preguntas de negocio; la decisión la toma el modelo — Sebastian (0:35)
> Estas son las cuatro preguntas del Entregable 1, y cada tarjeta dice dónde se responde. Las dos primeras salen de ventanas de 5 y 10 minutos que calcula el Job 2. La tercera, el reenvío rápido, son variables del modelo calculadas con el estado de cada cuenta. La cuarta es la tasa de alertas por entidad y canal, en Gold. Estas preguntas no son reglas: la decisión la toma un modelo, que les explicamos en un momento. Samuel les muestra la arquitectura.

### Diapositiva 4 · Arquitectura desplegada en AWS — Samuel (1:10)
**En pantalla:** el diagrama completo. Recorrer los círculos rojos con el puntero, en orden.

> Este es el sistema desplegado en AWS. Lo gestionado: S3 para el lakehouse, DynamoDB para los commits de Delta, SNS y SQS para las alertas, y Athena con Glue. Lo demás corre en contenedores en una EC2, con el rol del laboratorio y sin llaves en ningún archivo. Sigan los números. El productor (1) publica en Kafka (2), con la cuenta ordenante como clave. El Job 2 (3) valida, quita duplicados, lee el estado en MongoDB (4), puntúa con el modelo y, si supera el umbral, publica en SNS (5); SQS se la entrega a la entidad (6), que retiene los fondos. El Job 1 (7) guarda todo en Bronze y aparta los inválidos; el Job 3 (8) arma Silver sin duplicados. Cada 15 minutos, Airflow (9) arma Gold en estrella. Trino y Athena (10) lo consultan; Metabase (11) lo muestra a los analistas y cumplimiento (12) sigue el linaje. Es Kappa: la detección vive una sola vez, en el Job 2. Nathalia les cuenta qué cambió.

### Diapositiva 5 · Del Entregable 1 al 2 — Nathalia (0:55)
> Esto es lo que cambió frente al diseño del Entregable 1. La base se mantuvo: Kafka, Spark Structured Streaming, MongoDB, Delta en S3, SNS y SQS, Athena y Airflow. La API de ingesta no tenía un consumidor real, así que el productor publica directo en Kafka. Kafka, Spark y Airflow corren en una EC2 en vez de MSK, EMR y MWAA: AWS Academy limita los servicios y las instancias a 2 vCPU, y pasarlos a gestionados exige cambios de código. Lake Formation no está en el laboratorio, así que los permisos por rol los aplica Trino. Escribir risk_decisions dentro de la decisión costaba de 6 a 19 segundos por micro-batch, así que lo sacamos del camino crítico. Y lo que falló es la latencia: el objetivo de 3 segundos no se cumplió. Samuel explica las decisiones del streaming.

### Diapositiva 6 · Decisión: Kafka y Spark, con el modelo dentro del Job 2 — Samuel (0:50)
> ¿Por qué Spark Structured Streaming y no Flink? Porque un solo motor nos sirve para los cuatro jobs, en streaming y en batch, se integra con Delta y el equipo ya lo conocía. El costo es que la latencia es la espera del micro-batch más su duración. ¿Por qué Kafka y no Kinesis? Spark open source no trae una fuente de Kinesis para Structured Streaming, y queríamos varios consumidores independientes del mismo log con 7 días de retención. El riesgo más grande era que el Job 2 calculara las variables distinto al notebook de entrenamiento. Lo resolvimos con paridad exacta: 58 de 58 variables, 20.000 de 20.000 decisiones y las 264 alertas del test, iguales al notebook. Nathalia explica qué hace el modelo.

### Diapositiva 7 · El modelo de IA — Nathalia (0:50)
> Es un XGBoost preentrenado con IBM AML, un dataset público y etiquetado. Recibe 58 variables de comportamiento, como si la cuenta le envía por primera vez a ese destino o qué tan rápido reenvía lo que recibe, y devuelve una probabilidad de lavado. Elegimos árboles con gradiente porque son el estándar para datos tabulares y puntúan en milisegundos. En el test, algo más de la mitad de las alertas eran lavado real. Frente a la industria es pequeño: Stripe Radar usa una red neuronal con más de mil características y decide en menos de 100 milisegundos. La diferencia de fondo: ellos bloquean el pago; en lavado de activos la alerta va a analistas, que confirman y reportan a la UIAF. Mateo explica las garantías.

### Diapositiva 8 · Exactly-once en el efecto, no en el transporte — Mateo (0:45)
> Kafka entrega al menos una vez y Spark, si falla, repite el micro-batch con los mismos offsets del checkpoint. Entonces cada efecto tiene que ser idempotente. Bronze usa el sink de Delta, que hace un solo commit por micro-batch. En MongoDB, cada documento guarda el número de micro-batch que lo escribió y una copia del estado anterior: si el micro-batch se repite, se parte de esa copia. Lo probamos cortando la escritura a la mitad, y el reintento da el mismo estado. Silver y Gold usan un MERGE por transaction_uuid que solo inserta. SNS puede repetir alertas, y la entidad las descarta. El resultado en AWS: 5.000 de 5.000 transacciones, sin duplicados.

### Diapositiva 9 · Decisión: MongoDB CP y lakehouse en Delta — Mateo (0:50)
> El estado del Job 2 vive en MongoDB porque leer Silver en S3 toma segundos y no cabe en un micro-batch de 1 segundo. Lo configuramos CP: write concern y read concern majority. Una escritura confirmada no se pierde. Lo que sacrificamos es disponibilidad: si MongoDB no responde, el Job 2 espera en vez de decidir con datos viejos. En la EC2 hay un nodo; con tres nodos, esa espera es la elección del primario, unos 2 segundos. Descartamos Cassandra porque sus contadores no son idempotentes ante reintentos. Del lado analítico usamos Delta en tres capas en S3, y como varios procesos escriben la misma tabla, el log de commits va en DynamoDB. Nathalia sigue con la orquestación.

### Diapositiva 10 · Decisión: Airflow 3; Trino y Athena para consultar — Nathalia (0:40)
> Airflow 3 orquesta cuatro DAG. El principal corre cada 15 minutos: fija la versión de Silver, y las tres etapas del Job 4 leen esa versión con time travel, así un reintento no mezcla datos nuevos. La calidad corre antes de tocar MongoDB. En AWS, la corrida completa tarda 5 minutos; OPTIMIZE y VACUUM van en un DAG diario aparte. Trino aplica los permisos por rol: analistas y Metabase solo ven Gold, cumplimiento lee todo sin modificar, y pasaron 10 de 10 casos. Athena responde las mismas 9 consultas del tablero sin cambiar una línea. Sebastian presenta los resultados.

### Diapositiva 11 · Resultados medidos en AWS — Sebastian (0:45)
> Estos números son de la corrida en AWS, en una EC2 de 2 vCPU con 5.000 transacciones a 20 por segundo. Del productor a Kafka el p99 fue de 23 milisegundos, sin errores. No se perdió ni se duplicó nada: 5.000 de 5.000 en las tres capas, y el score en línea es idéntico al de fuera de línea. Airflow arma Gold en 5 minutos, con precisión de 0,50 y recall de 0,40, y el linaje de una alerta se reconstruye en 19,5 segundos. Lo que no cumple es la latencia de la decisión: 86 segundos de p99. La tasa nominal de 1.000 eventos por segundo no la probamos en AWS. Samuel explica la latencia.

### Diapositiva 12 · Qué falló: la latencia, y por qué — Samuel (0:40)
> Medimos cada tramo del micro-batch del Job 2 en AWS. De 31 segundos, 29 se van en leer el micro-batch de Kafka y validar el contrato con una función de Python. MongoDB tarda un segundo, y las variables y el modelo, un cuarto de segundo. Con 2 vCPU, los cuatro jobs compiten por el procesador, y un micro-batch que debería durar un segundo dura de 22 a 34. También encontramos errores al desplegar: AWS Academy no deja crear roles, así que usamos LabRole; reiniciar sin perfiles dejaba checkpoints viejos; y limitar los offsets cambiaba las variables. Mateo cierra con lo que cambiaríamos.

### Diapositiva 13 · Qué cambiaríamos con más tiempo — Mateo (0:35)
> En orden. Primero, validar el contrato con expresiones de Spark: es el 95 % del micro-batch. Segundo, pasar Kafka, Spark y Airflow a MSK, EMR y MWAA, para que cada uno escale por su cuenta. Tercero, probar los 1.000 eventos por segundo en AWS y declarar la tasa con 30 % de margen. Cuarto, las pruebas de falla del bróker, del ejecutor y de MongoDB. Quinto, Gold incremental con el Change Data Feed. Y sexto, Secrets Manager, KMS y autenticación en Trino. Ahora pasamos a la demo.

---

## Bloque 2 · Demo en AWS (8 min)

### Diapositiva 14 · Demo en AWS — todo el equipo
> En la demo van a ver el pipeline corriendo en AWS: el productor publica, Spark decide y alerta por SNS y SQS, Airflow lleva los datos a Gold, consultamos Gold en Athena y en Metabase, y al final seguimos el linaje de una alerta. El estado inicial de las cuentas ya está cargado en MongoDB.

Si la demo va en video, se graba siguiendo esta misma tabla, cortando las esperas, y cada integrante narra su parte en vivo mientras corre el video. El paso a paso para prepararla y grabarla, con las pestañas, los scripts y las consultas de Athena, está en [demo_en_vivo.md](demo_en_vivo.md).

### Preparación (40 minutos antes)
1. **Laboratorio y EC2:**
   - En el Learner Lab, **Start Lab**.
   - En EC2, la instancia `breb-mvp` → **Instance state → Start**.
   - Copiar la IP pública nueva. Si cambiaron de red, actualizar la regla de SSH del security group con **My IP**.
2. **Cuatro ventanas de PowerShell** conectadas con `ssh -i $HOME\Downloads\labsuser.pem ubuntu@<IP>`: A (comandos), B (log del Job 2), C (entidad) y D (analistas). Una quinta, E, es para el túnel.
3. **Ventana A:**
   ```bash
   sudo swapon /swapfile
   docker compose up -d
   docker compose --profile orquestacion --profile consumo up -d
   docker compose ps
   ```
   Esperen a que `airflow` y `trino` salgan como `healthy`. Luego revisen Metabase y Athena (paso 6), y apaguen el consumo para dejarle memoria al streaming:
   ```bash
   docker compose --profile consumo stop
   docker compose --profile streaming up -d
   ```
4. **Ventana E, el túnel**, que se deja abierta:
   ```powershell
   ssh -i $HOME\Downloads\labsuser.pem -N -L 8085:localhost:8085 -L 8080:localhost:8080 -L 8090:localhost:8090 -L 3000:localhost:3000 ubuntu@<IP>
   ```
5. **Ventanas B, C y D:**
   - B: `docker compose --profile streaming logs -f job2-riesgo | grep "\[job2\] batch"`
   - C: `docker compose run --rm entidad --tramo`
   - D: `docker compose run --rm entidad --analistas`
6. **Pestañas del navegador:**
   - **Consola de AWS:**
     - EC2, con la instancia y la pestaña *Security*, donde se ve `LabRole`.
     - S3, en `breb-414490329576-lakehouse-bronze/transacciones/_delta_log/`.
     - DynamoDB, en `delta_log` → *Explore table items*.
     - SNS, en `fraud-alerts-critical` → *Subscriptions*.
     - SQS, con la lista de colas.
     - Athena, en *Query editor*, con la consulta de alertas por canal ya escrita.
   - **Por el túnel:** Spark (http://localhost:8080), Airflow (http://localhost:8085), Metabase (http://localhost:3000/dashboard/2 y `/dashboard/3`) y Trino (http://localhost:8090/ui/).

### Durante la demo

| Minuto | Quién habla | Qué se ve | Qué decir |
|---|---|---|---|
| 0:00 | Samuel | Consola de AWS: la EC2 con `LabRole`, el bucket de Bronze con `_delta_log/`, la tabla `delta_log` en DynamoDB | "Todo esto es AWS: el lakehouse en S3, cada commit de Delta coordinado en DynamoDB, y la instancia toma sus credenciales del rol, sin llaves." |
| 0:40 | Samuel | Ventana A: `docker compose run --rm productor publicar --continuar --max 1500 --escalones "25:70"` | "El productor publica en Kafka el siguiente pedazo del tramo, en orden, con acks=all. Cada 60 s imprime cuántos confirmó." |
| 1:00 | Samuel | Spark UI con las cuatro aplicaciones; ventana B con las líneas `[job2] batch` | "Cada línea es un micro-batch: transacciones, alertas, latencia y el tiempo de cada tramo. Fíjense: variables y modelo, milisegundos; leer y validar, segundos. Ese es el cuello que les mostramos." |
| 2:00 | Sebastian | Ventanas C y D; en la consola, S3 con objetos nuevos y SQS con mensajes | "Esta alerta salió por SNS y llegó por SQS a la cola de la entidad, que retuvo los fondos. Si SNS la repite, la entidad la descarta." |
| 3:15 | Samuel | El productor termina: tabla del RF1 | "Confirmados todos, cero errores. El p99 hacia Kafka está en milisegundos." |
| 3:45 | Mateo | Ventana A: se apaga el streaming, se prende el consumo y se quita la pausa del pipeline (ver los comandos abajo); Airflow en la vista Graph | "Con 2 vCPU apagamos el streaming para que el batch tenga procesador. check_new_silver_data fija la versión de Silver, y dimensiones, hechos y calidad leen esa versión." |
| 6:15 | Sebastian | Athena: la consulta de alertas por canal, y el *Data scanned*; Metabase, tablero sobre Athena | "Pregunta 4 del E1, respondida sin servidor: Athena lee el mismo Delta de S3 por el catálogo de Glue y cobra por lo que escanea, menos de 2 MB. El tablero muestra lo mismo para los analistas." |
| 7:15 | Sebastian | Ventana A: `docker compose --profile consumo run --rm spark-cliente python -m breb.consumo linaje` | "Como cumplimiento: de la partición y el offset en Kafka hasta la acción de cada entidad, reconstruido en segundos." |

Comandos del minuto 3:45, en la ventana A:
```bash
docker compose stop job1-bronze job2-riesgo job2-ventanas job3-silver
docker compose --profile consumo up -d
docker compose exec airflow airflow dags unpause breb_lakehouse_pipeline
```
Al terminar la demo: `docker compose exec airflow airflow dags pause breb_lakehouse_pipeline`.

**Si a las 6:15 Gold todavía no termina:** con 2 vCPU la corrida tarda unos 5 minutos. Consulten Gold igual en Athena: muestra las 5.000 transacciones anteriores. Digan: "la corrida que lanzamos está en dims y hechos; cuando termine, estas consultas ya incluyen lo de hoy, porque Athena y Trino leen el `_delta_log` en cada consulta".

**Si no llega ninguna alerta nueva** en las 1.500 transacciones, muestren las 8 de la corrida anterior en la tarjeta "Alertas emitidas" del tablero o en Athena.

**Si algo falla:** quien esté hablando dice qué falló, por qué (con la evidencia en pantalla) y cómo se arregla. Un fallo bien explicado mantiene la nota; uno sin explicar la baja.

---

## Bloque 3 · Defensa (7 min)

El profesor pregunta a una persona y los demás no pueden intervenir. Cada respuesta dura de 30 a 45 segundos y sigue este orden:
1. **La decisión, en una frase.** "MongoDB con write y read concern majority: es CP."
2. **La evidencia:** un número medido o el archivo donde está. "Está en `src/breb/estado/almacen.py`, línea 30."
3. **Lo que se sacrificó.** "Si MongoDB no responde, el Job 2 espera."
4. **Qué haríamos distinto**, solo si aplica.

Si no saben algo, digan lo que sí saben, a qué componente pertenece y dónde está en el código. No inventen cifras. Tengan abierta una pestaña con el repositorio en GitHub.

### Diapositiva 15 · Respaldo: qué pasa si algo falla
Se muestra solo si preguntan por fallas.

> Cada fila dice qué pasa. En la EC2 hay un solo bróker: si se cae, los jobs fallan, Docker los reinicia y siguen desde el checkpoint, sin perder ni duplicar eventos, porque Kafka guarda 7 días. Con MongoDB pasa lo mismo: en la EC2 hay un nodo y el Job 2 espera; con tres nodos se elegiría otro primario en unos 2 segundos.

### Números que todos deben saber

| Qué | Valor |
|---|---|
| Bre-B | 20 s para acreditar; 1.700 millones de transacciones el primer año; récord de 10,4 millones en un día (unas 120 por segundo) |
| Despliegue | AWS Academy, us-east-1. Gestionado: S3, DynamoDB, SNS, SQS, Athena, Glue e IAM (`LabRole`). En una EC2 `r5.large` (2 vCPU, 16 GiB): Kafka, MongoDB, Spark, Airflow, Trino y Metabase |
| Modelo | XGBoost preentrenado con IBM AML; PR-AUC de 0,267 (unas 450 veces el azar); umbral de 0,172; test de 1.038.880 transacciones con 617 de lavado; 264 alertas, 141 de lavado real |
| Paridad | 58 de 58 variables; 20.000 de 20.000 decisiones; en AWS, score idéntico en 5.000 de 5.000 |
| Corrida en AWS | 5.000 transacciones a 20 ev/s; 8 alertas por SNS; 4.728 cuentas actualizadas en MongoDB |
| RF1 · fuente → Kafka | p99 de 23 ms, 0 errores (objetivo: < 300 ms) |
| RNF-02 · Kafka → decisión | p99 de 86 s, p50 de 40 s (objetivo: p99 < 3 s) |
| Desglose del Job 2 | De 31 s por micro-batch: 29,2 s leer y validar; 1,1 s MongoDB; 0,25 s variables y modelo; 0,2 s SNS |
| Completitud | 5.000 de 5.000 en Bronze, Silver y `risk_decisions`, sin duplicados ni cuarentena |
| Airflow | Pipeline de Gold en 5 min 4 s; `bootstrap_state` en 3 min 44 s |
| Gold | Precisión de 0,50 y recall de 0,40 en las 5.000 (4 de 8 alertas eran lavado real) |
| Consumo | 10 de 10 casos de permisos en Trino; 9 de 9 consultas en Athena, menos de 3 s y menos de 2 MB cada una; linaje en 19,5 s (objetivo: < 5 min) |
| Ejemplo de linaje | Transacción `d2d22203…`, lavado real: llegó a Kafka a las 22:01:28; decidida a las 22:02:54, con score de 0,995 (86 s); la entidad 1422 retuvo los fondos a las 22:02:58 |
