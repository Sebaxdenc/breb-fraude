# Guion de la sustentación final · Bre-B

Sesión de 25 minutos: **10 min de presentación + 8 min de demo en vivo + 7 min de defensa**. El objetivo de la evaluación es justificar cada decisión de diseño, así que el guion repite una misma estructura: la decisión, la alternativa descartada, la evidencia medida y lo que se sacrificó.

Materiales de esta carpeta:
- `slides.pptx`: 15 láminas editables, con este guion en las notas del orador. `slides.pdf` es la copia para presentar sin conexión.
- `demo_en_vivo.md`: preparación, comandos de la demo, plan B y qué decir si algo falla.
- `preguntas_defensa.md`: respuestas a las preguntas típicas del profesor y a otras probables, con el archivo y la línea del código.
- `../arquitectura/arquitectura_aws.png`: el diagrama de la lámina 4 (despliegue en AWS). El del modo local es `../arquitectura/arquitectura_e2.png`.

Las láminas se generan con `python docs/presentacion/construir_slides.py` (ver el encabezado del script). Si cambian el texto hablado, editen este archivo y vuelvan a generar las láminas: las notas salen de aquí.

## Reparto
El reparto sigue los roles del Entregable 1. Es una propuesta: cámbienlo si quieren, pero mantengan los tiempos.

| Integrante | Rol en el E1 | Láminas | Tiempo | En la demo |
|---|---|---|---|---|
| Sebastian Medina | Consumo y gobernanza | 1, 2, 3 y 10 | 2:45 | Alertas, consulta a Gold y linaje |
| Samuel Arango | Ingesta y streaming | 4, 6 y 11 | 2:50 | Productor, Spark y Job 2 |
| Mateo Sanz | Almacenamiento | 7, 8 y 12 | 2:35 | Airflow y Gold |
| Nathalia Cardoza | DataOps | 5 y 9 | 1:30 | Opera la terminal y el navegador durante toda la demo |

Total de la presentación: 9:40, con 20 s de margen. En la defensa, el profesor pregunta a cada integrante por partes que **no** implementó: todos estudian `preguntas_defensa.md` completo.

Reglas para hablar:
- Decir el número, no el adjetivo: "53 segundos de p99", no "la latencia es alta".
- Señalar la lámina mientras se habla del diagrama o de la tabla.
- No leer las notas. El texto de abajo es una guía: digan las mismas ideas con sus palabras.
- Cada uno termina su parte nombrando a quien sigue, para que el cambio no gaste tiempo.

---

## Bloque 1 · Presentación (10 min)

### Lámina 1 · Portada — Sebastian (0:25)
**En pantalla:** título, equipo y la pregunta central del proyecto.

> Buenos días, profe. Somos Samuel, Mateo, Nathalia y yo, Sebastian. Nuestro caso es Bre-B, el sistema de pagos inmediatos del Banco de la República. La pregunta que guía el proyecto es esta: dado el fraude en pagos inmediatos, ¿qué pipeline construiríamos y por qué exactamente así? En diez minutos les mostramos qué construimos, qué cambió desde el Entregable 1 y qué no logramos.

### Lámina 2 · El problema de negocio — Sebastian (0:45)
**En pantalla:** cifras de Bre-B a la izquierda y la tabla de usuarios a la derecha.

> Bre-B acredita una transferencia en unos 20 segundos, todos los días del año. En su primer año pasó de 1.700 millones de transacciones, y el día récord tuvo 10,4 millones, unas 120 por segundo en promedio. El problema es que las redes de mulas reparten el dinero entre varias entidades en minutos, y un pago inmediato no se puede reversar. La banca analiza por lotes, con cortes de 12 a 24 horas: llega tarde. Por eso nuestro sistema decide en segundos y avisa a la entidad para que retenga los fondos. No bloquea la autorización del pago: detecta y notifica. Tiene tres usuarios: la entidad financiera, los analistas de fraude y cumplimiento.

### Lámina 3 · Las preguntas que responde el sistema — Sebastian (0:45)
**En pantalla:** tabla de las cuatro preguntas y el recuadro del modelo.

> Estas son las cuatro preguntas del Entregable 1, y la tabla dice dónde se responde cada una. Las dos primeras salen de ventanas de 5 y 10 minutos que calcula el Job 2. La tercera, el reenvío rápido, son variables del modelo calculadas con el estado de cada cuenta. La cuarta es la tasa de alertas por entidad y canal, en Gold. Algo importante: estas preguntas no son reglas. La decisión la toma un XGBoost preentrenado de Kaggle, entrenado con el dataset IBM AML. En el test fuera de tiempo tiene un PR-AUC de 0,267, unas 450 veces el de un modelo al azar. Samuel les muestra la arquitectura.

### Lámina 4 · Arquitectura final — Samuel (1:05)
**En pantalla:** el diagrama completo. Recorrer los números rojos con el puntero, en orden.

> Este es el sistema desplegado en AWS. El lakehouse está en S3, el log de commits de Delta en DynamoDB, las alertas en SNS y SQS, y las consultas sin servidor en Athena con el catálogo de Glue. Kafka, Spark, MongoDB, Airflow, Trino y Metabase corren como contenedores en una EC2, que toma el rol del laboratorio, sin llaves en ningún archivo. El mismo código corre en local con emuladores; la caja de la izquierda dice qué cambia. Sigan los números. El productor (1) publica en Kafka (2) las transacciones del test de IBM, en orden y con la cuenta ordenante como clave. Tres aplicaciones de Spark leen el mismo tópico por separado. El Job 2 (3) valida, quita duplicados, lee el estado de las cuentas en MongoDB (4), calcula 58 variables, puntúa con el modelo y, si el score supera el umbral, publica la alerta en SNS (5); su otra aplicación calcula las ventanas de 5 y 10 minutos y también las guarda en MongoDB. SQS le entrega la alerta a la entidad (6), que retiene los fondos. El Job 1 (7) guarda el 100 % de los eventos en Bronze y deja los inválidos en cuarentena. El Job 3 (8) pasa Bronze a Silver sin duplicados. Cada 15 minutos, Airflow corre el Job 4 (9), que arma Gold en estrella. Trino (10) expone las tres capas con permisos por rol, y Athena (10) responde las mismas consultas sin servidor. Metabase (11) muestra el tablero a los analistas, y cumplimiento (12) sigue el linaje de cada alerta. Es una arquitectura Kappa: la lógica de detección vive una sola vez, en el Job 2. Nathalia les cuenta qué cambió.

### Lámina 5 · Del Entregable 1 al 2 — Nathalia (0:45)
**En pantalla:** tabla de cambios; abajo, lo que se mantuvo y lo que falló.

> Esto es lo que cambió frente al Entregable 1. La base se mantuvo: Kafka, Spark Structured Streaming, MongoDB, Delta, Trino, SNS y SQS. Cada cambio tiene una razón práctica o medida. La API de ingesta no tenía un consumidor real, así que el productor publica directo en Kafka. MinIO dejó de publicar imágenes y lo reemplazamos por SeaweedFS, después de probar Delta con dos escritores concurrentes. Escribir risk_decisions dentro de la decisión costaba de 6 a 19 segundos por micro-batch, así que lo sacamos del camino crítico. Y lo que falló es la latencia: el objetivo de 3 segundos no se cumplió. Samuel explica las decisiones del streaming.

### Lámina 6 · Decisión: Kafka y Spark, con el modelo dentro del Job 2 — Samuel (0:55)
**En pantalla:** a la izquierda, por qué Spark y por qué Kafka; a la derecha, la paridad con el notebook.

> ¿Por qué Spark Structured Streaming y no Flink? Porque un solo motor nos sirve para los cuatro jobs, en streaming y en batch, se integra con Delta y el equipo ya lo conocía. El costo es que la latencia es la espera del micro-batch más su duración. ¿Por qué Kafka y no Kinesis? Spark open source no trae una fuente de Kinesis para Structured Streaming, necesitábamos correr sin internet y queríamos dos consumidores independientes con 7 días de retención. El riesgo más grande era que el Job 2 calculara las variables distinto al notebook de entrenamiento. Lo resolvimos con paridad exacta: 58 de 58 variables y 20.000 de 20.000 decisiones iguales, en Kaggle y en Docker. Para lograrlo replicamos las sumas de Kahan de pandas y la aritmética de numexpr, y procesamos cada micro-batch en el orden original. Mateo explica las garantías.

### Lámina 7 · ¿Cómo evitamos procesar dos veces un evento? — Mateo (0:55)
**En pantalla:** tabla de mecanismos por componente.

> Kafka entrega al menos una vez y Spark, si falla, repite el micro-batch con los mismos offsets del checkpoint. Entonces cada efecto tiene que ser idempotente. Bronze usa el sink de Delta, que hace un solo commit por micro-batch. En MongoDB, cada documento guarda el número de micro-batch que lo escribió y una copia del estado anterior: si el micro-batch se repite, se parte de esa copia. Lo probamos cortando la escritura a la mitad, y el reintento da el mismo estado. Silver y Gold usan un MERGE por transaction_uuid que solo inserta. SNS puede repetir alertas, y la entidad las descarta por entidad y transaction_uuid. El resultado es exactly-once en el efecto, no en el transporte: 71.910 de 71.910 eventos, sin duplicados.

### Lámina 8 · Decisión: estado en MongoDB y lakehouse en Delta — Mateo (0:55)
**En pantalla:** dos columnas, MongoDB y Delta Lake.

> El estado del Job 2 vive en MongoDB porque leer Silver en S3 tarda de 1 a 4 segundos y no cabe en un micro-batch de 1 segundo. Lo configuramos CP: write concern y read concern majority. Una escritura confirmada sobrevive a la caída del primario. Lo que sacrificamos es disponibilidad: durante la elección, que ajustamos a 2 segundos de timeout, el Job 2 no puede escribir. Las claves son la cuenta, el par ordenante–destino y el banco, porque así lee el Job 2: por clave y en lote. En producción se fragmenta por hash de la cuenta. Descartamos Cassandra porque sus contadores no son idempotentes ante reintentos. Del lado analítico usamos Delta en tres capas, y como varios procesos escriben la misma tabla, el log de commits va en DynamoDB. Nathalia sigue con la orquestación.

### Lámina 9 · Decisión: Airflow 3 y Trino con catálogo — Nathalia (0:45)
**En pantalla:** los cuatro DAG arriba; Airflow y consumo abajo.

> Airflow 3 orquesta cuatro DAG. El principal corre cada 15 minutos. Primero fija la versión de Silver que va a procesar, y las tres etapas del Job 4 leen esa versión con time travel; así un reintento no mezcla datos nuevos. Los controles de calidad corren antes de tocar MongoDB. OPTIMIZE y VACUUM van en un DAG diario aparte: en la primera corrida en Docker eran 8 de los 11 minutos, y sin ellos la corrida bajó a 3 minutos y medio. Los otros dos DAG son bajo demanda: reprocesar la cuarentena y cargar el estado inicial. Para consultar, Trino lee las tres capas a través del Hive Metastore, con reglas por rol: analistas y Metabase solo ven Gold, y cumplimiento lee todo sin poder modificar nada. Probamos diez casos y pasaron los diez. Sebastian presenta los resultados.

### Lámina 10 · Resultados medidos — Sebastian (0:50)
**En pantalla:** tabla de requisitos con su estado.

> Estos números se midieron en el portátil de la demo, de 16 GB. Del productor a Kafka, el p99 estuvo entre 49 y 235 milisegundos hasta 1.000 eventos por segundo: cumple. No perdimos ni duplicamos eventos: 71.910 de 71.910. La paridad con el notebook es exacta. Gold se arma en 65 segundos para el tramo de la demo, con precisión y recall de 0,52 contra las etiquetas reales, y Airflow corre el pipeline completo en Docker en 3 minutos y medio; eran 11 y medio hasta que sacamos el mantenimiento a un DAG diario. El linaje de una alerta se reconstruye en 6 segundos, y el requisito era menos de 5 minutos. Lo que no cumple es la latencia de la decisión: 53 segundos de p99 a 50 eventos por segundo. Y faltan las pruebas de falla. Samuel explica la latencia.

### Lámina 11 · Qué falló: la latencia — Samuel (0:50)
**En pantalla:** el gráfico de latencia y la lista de errores encontrados.

> Medimos cada tramo del micro-batch. Las variables y el modelo tardan menos de medio segundo. El tiempo se va en leer Kafka y validar el contrato con una función de Python, de 2,7 a 62 segundos, y en leer y escribir el estado en MongoDB, hasta 12 segundos. Además el equipo está saturado: con los jobs corriendo, hasta el productor ve 5,5 segundos hacia Kafka. Bajamos de 285 a 99 segundos sacando risk_decisions y las ventanas del camino crítico; a 50 eventos por segundo quedó en 53. Encontramos otros errores: limitar los offsets por micro-batch cambiaba las variables, y apagar un perfil de Compose apagaba toda la infraestructura. Cada uno quedó documentado con su arreglo. Mateo cierra con lo que cambiaríamos.

### Lámina 12 · Qué cambiaríamos con más tiempo — Mateo (0:45)
**En pantalla:** tabla de cambios en orden de prioridad.

> Con más tiempo haríamos esto, en orden. Primero, validar el contrato con expresiones de Spark en lugar de Python, porque es el tramo más lento. Segundo, acercar el estado al cómputo: escribir en MongoDB solo los campos que cambian y, a escala, mover el estado al state store de Spark, particionado por cuenta. Tercero, medir en Linux nativo o en la nube y declarar la tasa con 30 % de margen. Cuarto, correr las pruebas de falla del bróker, del ejecutor y de la elección de MongoDB. Y quinto, Gold incremental con el Change Data Feed de Delta. Ahora pasamos a la demo.

---

## Bloque 2 · Demo en vivo (8 min)

### Lámina 13 · Demo en vivo — todo el equipo
**En pantalla:** la línea de tiempo de la demo, mientras Nathalia cambia a las ventanas.

> En la demo van a ver el pipeline procesando en vivo la segunda parte del tramo de la demo: el productor publica, Spark decide y alerta, Airflow lleva los datos a Gold y al final consultamos Gold y el linaje de una alerta. El estado inicial y la primera parte del tramo los cargamos antes de la clase.

Nathalia ejecuta los comandos de `demo_en_vivo.md` (pasos D1 a D8). Los demás narran. Tiempos objetivo:

| Minuto | Quién habla | Qué se ve (paso de la guía) | Qué decir |
|---|---|---|---|
| 0:00 | Samuel | D1. `docker compose ps`, Spark UI con 4 aplicaciones, Airflow con `bootstrap_state` y una corrida del pipeline en verde | "Arrancamos con el estado de MongoDB cargado y la primera parte del tramo ya procesada. Lo que sigue pasa en vivo." |
| 0:40 | Samuel | D2. Nathalia lanza el productor con el resto del tramo a 50 ev/s | "El productor publica en Kafka en orden de seq, con acks=all. Cada 60 s imprime cuántos confirmó." |
| 1:00 | Samuel | D3. Spark UI → `job2-riesgo` → Structured Streaming; log `[job2] batch` | "Cada línea es un micro-batch: cuántas transacciones, cuántas alertas, la latencia y cuánto tardó cada tramo. Fíjense que variables y modelo son milisegundos; lectura de Kafka y MongoDB, segundos." |
| 2:00 | Sebastian | D4. Ventanas de la entidad y de los analistas | "Esta alerta llegó por SNS a la cola de la entidad, que retuvo los fondos de la cuenta destino. Si SNS la repite, la entidad la descarta." |
| 3:15 | Samuel | Termina el productor: tabla del RF1 | "Confirmados todos y cero errores: no se perdió nada. Con los cuatro jobs en el mismo portátil, la latencia hacia Kafka sube a segundos y la tabla puede decir NO cumple; el RF1 lo medimos con el productor solo: p99 de 49 a 235 ms." |
| 3:30 | Mateo | D5. Jobs al día (y, en el plan B, se apagan los jobs de streaming) | "Silver ya tiene todo. En el plan B: apagamos el streaming para liberar memoria del portátil; en AWS corren en clústeres separados." |
| 3:45 | Mateo | D6. Airflow: quitar la pausa de `breb_lakehouse_pipeline`, vista Graph | "check_new_silver_data fija la versión de Silver. Dims y hechos leen esa versión; si una tarea falla, se reintenta sobre los mismos datos." |
| 5:30 | Mateo | Vista Graph: `silver_to_gold_dims` en verde y `silver_to_gold_facts` corriendo (en la prueba del DAG nuevo, los hechos terminaron a los 2 min 21 s de arrancar la corrida) | "Dimensiones y hechos leen la misma versión de Silver. Cuando los hechos estén en verde, Gold ya se puede consultar." |
| 6:15 | Sebastian | D7. Consulta a Gold (Trino o Metabase) | "Pregunta 4 del E1: tasa de alertas por canal. Y el desempeño del modelo con las etiquetas, que nunca viajaron en el evento." |
| 6:55 | Mateo | Log de `data_quality_checks` (termina hacia el 6:30) | "Hechos igual a Silver, integridad con dim_tiempo, una versión vigente por cuenta, y precisión y recall contra las etiquetas reales." |
| 7:15 | Sebastian | D8. `python -m breb.consumo linaje` | "Como cumplimiento: de la coordenada en Kafka hasta la acción de la entidad, en segundos." |

**Orden B:** si en el ensayo `silver_to_gold_facts` tardó más de 2 min 30 s desde que arranca la corrida, D8 (linaje) va antes y D7 (consulta a Gold) al final, cuando los hechos estén en verde. En la prueba del DAG nuevo, los hechos estuvieron en verde a los 2 min 21 s, así que el orden normal alcanza, pero con poco margen; `demo_en_vivo.md` ("Tiempos de Airflow") tiene la duración de cada tarea y qué decir si no alcanza.

Si algo falla, quien está hablando dice qué falló, por qué (con la evidencia en pantalla) y cómo se arregla. `demo_en_vivo.md` tiene la tabla de fallas probables con esas tres respuestas. Un fallo bien explicado mantiene la nota de defensa; uno sin explicar la baja.

---

## Bloque 3 · Defensa con preguntas (7 min)

El profesor pregunta a una persona y los demás no pueden intervenir. Estructura de cada respuesta, en 30 a 45 segundos:
1. **La decisión, en una frase.** "MongoDB con write y read concern majority: es CP."
2. **La evidencia:** un número medido o el archivo donde está. "Está en `src/breb/estado/almacen.py`, línea 30."
3. **Lo que se sacrificó.** "Durante una elección, el Job 2 no escribe."
4. **Qué haríamos distinto**, solo si aplica.

Si no saben algo, digan lo que sí saben, a qué componente pertenece y dónde está en el código. No inventen cifras.

Si piden el código, abran el enlace de GitHub de `preguntas_defensa.md` (sección "Dónde está cada cosa"). Tengan una pestaña con el repositorio abierta.

### Lámina 14 · Respaldo: volumen × 100
Se muestra solo si preguntan por escalabilidad.

> El cuello medido es el camino crítico del Job 2, no Kafka. Con cien veces el volumen, lo primero que cambiaríamos es la validación en Python y el acceso al estado por micro-batch; después, más particiones de Kafka y MongoDB fragmentado por hash de la cuenta.

### Lámina 15 · Respaldo: qué pasa si algo falla
Se muestra solo si preguntan por fallas.

> Cada fila dice qué pasa y dónde está en el código. Si cae el bróker en el perfil completo, acks=all con min.insync.replicas=2 sigue escribiendo; con un solo bróker, los jobs se detienen, Docker los reinicia y siguen desde el checkpoint.

### Números que todos deben saber
| Qué | Valor |
|---|---|
| Bre-B | 20 s para acreditar; 1.700 millones de transacciones el primer año; récord de 10,4 millones en un día (≈ 120 tx/s) |
| Objetivos de diseño (RNF-01) | 1.000 ev/s nominales, 5.000 en pico; tópico de 6 particiones en local (12 en el diseño) |
| Modelo | XGBoost de Kaggle; PR-AUC 0,267 en el test (≈ 450 veces el azar); umbral 0,172; test de 1.038.880 transacciones con 617 de lavado |
| Paridad | 58 de 58 variables; 20.000 de 20.000 decisiones; PR-AUC del motor = notebook |
| RF1 | p99 de 49 a 235 ms, de 100 a 1.000 ev/s (objetivo < 300 ms) |
| RNF-02 | objetivo p99 < 3 s; medido 285 s → 99 s → 53 s |
| Completitud | 71.910 de 71.910 en Bronze, Silver y risk_decisions, sin duplicados |
| Tramo de la demo | 15.000 transacciones desde el seq 6.814.261; 25 alertas, 13 de lavado real; precisión 0,52 y recall 0,52 |
| Gold | Job 4 en 65 s; 15.000 hechos; 13.935 cuentas en dim_cuenta |
| Airflow en Docker | DAG nuevo: corrida completa en 3 min 33 s y hechos en Gold a los 2 min 21 s (2026-11-10). Con OPTIMIZE y VACUUM adentro eran 11 min 39 s (VACUUM, 6 min 46 s), por eso pasaron al DAG diario `breb_lakehouse_mantenimiento` |
| Estado inicial | 704.707 cuentas, 1.349.332 pares y 41.815 bancos; unos 5,5 GB para calcularlo |
| Consumo | 11 tablas registradas en Trino (12 con `silver.reprocesos_cuarentena`); 10 de 10 casos de permisos; linaje en 6 s (objetivo < 5 min) |
| Memoria | WSL con 11,68 GiB; dev + consumo en reposo 4,03 GiB; completo con la prueba de Delta 4,59 GiB |

---

## Ensayo
1. **Dos días antes:** ensayo completo con cronómetro, incluida la demo con el plan A y con el plan B. Anotar la memoria de `docker stats` en cada paso.
2. **El día anterior:** ensayo final (`docs/plan_e2.md`, fase 7). Cada integrante responde en voz alta cinco preguntas al azar de `preguntas_defensa.md` sobre partes que no implementó.
3. **El día de la sustentación:** preparación de `demo_en_vivo.md` 60 minutos antes. Llevar `slides.pdf` en una memoria USB.
