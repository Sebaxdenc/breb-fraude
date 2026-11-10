# Bre-B · Detección de fraude en tiempo real

Proyecto final de ST1630 Sistemas Intensivos en Datos (EAFIT, 2026-2). Implementación de la arquitectura del Entregable 1 v2.0: Kafka, Spark Structured Streaming, Delta Lake, MongoDB, Airflow, Trino, Metabase y alertas por SNS/SQS, con un modelo XGBoost de Kaggle dentro del Job 2.

- **Plan y avance:** [docs/plan_e2.md](docs/plan_e2.md)
- **Contratos de integración:** [contratos/README.md](contratos/README.md)
- **Arquitectura implementada:** [docs/arquitectura/arquitectura_e2.png](docs/arquitectura/arquitectura_e2.png) (editable en draw.io: [arquitectura_e2.drawio](docs/arquitectura/arquitectura_e2.drawio))
- **Sustentación:** [docs/presentacion/](docs/presentacion/) (guion, láminas, guía de la demo y preguntas de la defensa)
- **Bitácora de delegación a agentes de IA:** [docs/bitacora_ia.md](docs/bitacora_ia.md)

## Estructura
| Carpeta | Contenido |
|---|---|
| `contratos/` | Documentación de los contratos de integración: evento, alerta, variables del modelo y documentos de MongoDB |
| `src/breb/contratos/` | Esquemas JSON del evento y de la alerta, validación |
| `src/breb/datos/` | Carga de IBM AML en el mismo orden del entrenamiento |
| `src/breb/features/` | Motor de las 58 variables del modelo (Job 2) |
| `src/breb/modelo/` | Carga del modelo y puntuación |
| `src/breb/estado/` | Estado en MongoDB, idempotente por micro-batch, y estado inicial |
| `src/breb/job2/` | Lógica del micro-batch del Job 2, sin Spark |
| `src/breb/productor/` | Productor de la demo: conversión de filas a eventos y publicación en Kafka por escalones |
| `modelo/` | Notebook de entrenamiento (versión 1 de Kaggle) y artefactos del modelo |
| `herramientas/referencia/` | Dataset sintético y ejecución del notebook original para la prueba de paridad |
| `herramientas/kaggle/` | Notebooks para Kaggle: verificación de paridad con datos reales y demo del modelo |
| `docker-compose.yml` | Punto de entrada del stack en la raíz: incluye `infra/docker-compose.yml` |
| `infra/` | Stack local: `docker-compose.yml`, imagen de Spark, inicialización de Kafka, MongoDB, S3 y DynamoDB |
| `docs/` | Diagramas de la arquitectura implementada, material de la sustentación y bitácora de IA |
| `src/breb/streaming/` | Jobs 1, 2 y 3 de Spark Structured Streaming (el Job 2 corre en dos aplicaciones: riesgo y ventanas) |
| `jobs/` | Puntos de entrada de `spark-submit`: Jobs 1 a 4 y `lakehouse.py`, que corre los procesos batch de los DAG |
| `src/breb/batch/` | Job 4 (Silver → Gold), cuarentena, perfiles en MongoDB, salud del streaming, OPTIMIZE y VACUUM |
| `dags/`, `src/breb/orquestacion.py`, `infra/airflow/` | DAG de Airflow 3 y su imagen |
| `src/breb/entidad/` | Consumidores de las alertas (entidades y analistas) |
| `src/breb/consumo/` | Registro en Trino, permisos, linaje y tablero de Metabase |
| `herramientas/infra/` | Verificación del stack y prueba de Delta Lake sobre el S3 local |
| `herramientas/streaming/` | Verificación de la fase 4 |
| `tests/` | Pruebas (paridad, Job 2 con MongoDB, contratos) |

## Entorno de desarrollo
La versión de Python y de las librerías es la de la imagen de Spark: PySpark 3.5 admite hasta Python 3.11 y exige numpy < 2.
```bash
uv venv -p 3.11 .venv
uv pip install -p .venv/bin/python -e ".[dev]"
.venv/bin/python -m pytest -q
```

## Correr el pipeline desde cero
Resumen de punta a punta con el tramo de la demo. Los comandos son de PowerShell y se corren en la raíz del repositorio: el `docker-compose.yml` de la raíz incluye `infra/docker-compose.yml` (Docker Compose 2.20 o posterior) y da el mismo proyecto que correrlos dentro de `infra/`. Las secciones siguientes explican cada paso y sus opciones.

**Requisitos:** Docker Desktop con WSL 2 y 12 GB para WSL (ver "Preparación"), y `LI-Small_Trans.csv` de [IBM Transactions for Anti Money Laundering](https://www.kaggle.com/datasets/ealtman2019/ibm-transactions-for-anti-money-laundering-aml) en la carpeta `datos/` de la raíz. Los artefactos del modelo ya están en `modelo/aml_xgb/`.

```powershell
# 1. Imágenes (una vez, con internet; unos 20 minutos): Spark, Airflow, Hive Metastore y productor se construyen
docker compose --profile consumo --profile orquestacion --profile herramientas build
docker compose --profile consumo --profile orquestacion --profile herramientas --profile streaming pull

# 2. Infraestructura y Airflow, desde cero
docker compose --profile "*" down -v                    # todos los perfiles: así también se borran los checkpoints
docker compose up -d                                    # Kafka, MongoDB, SeaweedFS, DynamoDB Local, Spark, moto
docker compose --profile orquestacion up -d             # PostgreSQL y Airflow (http://localhost:8085)
docker compose ps -a                                    # los servicios de inicio en "Exited (0)" y airflow "healthy"

# 3. Datos de la reproducción (una vez: quedan en datos/productor/)
docker compose run --rm productor preparar              # transacciones posteriores al corte y sus etiquetas
docker compose run --rm spark-cliente python -m breb.estado tramo   # puntuación del test y tramo de la demo (6 GB, 5 min)

# 4. Estado inicial del Job 2 en MongoDB, orquestado (equivale a: python -m breb.estado cargar --tramo --reemplazar)
docker compose exec airflow airflow dags trigger bootstrap_state
docker compose exec airflow airflow dags list-runs bootstrap_state  # repetir hasta "success"

# 5. Streaming: Jobs 1, 2 y 3, y los consumidores de alertas (cada uno en su ventana)
docker compose --profile streaming up -d
docker compose run --rm entidad --tramo
docker compose run --rm entidad --analistas

# 6. Publicar el tramo y verificar Kafka, Bronze, Silver, risk_decisions y MongoDB
docker compose run --rm productor publicar --tramo --escalones "50:310"
docker compose run --rm spark-cliente spark-submit /opt/breb/herramientas/streaming/verificar_fase4.py --esperar 900

# 7. Gold con Airflow, con el streaming apagado para liberar memoria
docker compose stop job1-bronze job2-riesgo job2-ventanas job3-silver
docker compose exec airflow airflow dags unpause breb_lakehouse_pipeline
docker compose exec airflow airflow dags list-runs breb_lakehouse_pipeline   # repetir hasta "success"

# 8. Consumo: Trino, permisos, tablero de Metabase (http://localhost:3000) y linaje
docker compose --profile consumo up -d
docker compose --profile consumo run --rm spark-cliente python -m breb.consumo registrar
docker compose --profile consumo run --rm spark-cliente python -m breb.consumo permisos
docker compose --profile consumo run --rm spark-cliente python -m breb.consumo tablero
docker compose --profile consumo run --rm spark-cliente python -m breb.consumo linaje
```
Resultados esperados: en el paso 6, `confirmados 14,999 de 14,999` (más el mensaje de calentamiento), `errores 0` y `no quedan transacciones por publicar`. Con los jobs prendidos, el RF1 sale `NO cumple` y el cierre `PUBLICACIÓN CON PROBLEMAS`: sube la latencia fuente → Kafka, pero no se pierde nada. En la verificación, todo en `[ok]` salvo la latencia del RNF-02 (`HAY FALLAS: 1`), que en el portátil no se cumple. Después, la corrida de `breb_lakehouse_pipeline` en `success`, `PERMISOS OK` y el enlace del tablero. En el portátil de 16 GB no conviene tener `streaming`, `consumo` y `orquestacion` arriba a la vez: por eso el paso 7 apaga los jobs. La guía de la demo en vivo ([docs/presentacion/demo_en_vivo.md](docs/presentacion/demo_en_vivo.md)) divide el paso 6 en una parte antes de la clase y otra en vivo.

## Levantar el stack
Pensado para el equipo de la demo: Windows 11, 16 GB de RAM, Docker Desktop con WSL 2.

### Preparación (una vez, con internet)
1. Instalar Docker Desktop con el motor WSL 2.
2. Darle memoria a WSL. En PowerShell:
   ```powershell
   Set-Content -Path "$env:USERPROFILE\.wslconfig" -Encoding ascii -Value "[wsl2]","memory=12GB","processors=12","swap=4GB"
   wsl --shutdown
   ```
   Abrir Docker Desktop de nuevo y comprobar con `docker info | Select-String "CPUs|Total Memory"`: debe mostrar 12 CPUs y unos 11,7 GiB. Si muestra 16 CPUs y unos 7,7 GiB, WSL no leyó el archivo (los valores por defecto son todos los núcleos y la mitad de la RAM). Lo más común es que el Bloc de notas lo haya guardado como `.wslconfig.txt`: `Get-ChildItem $env:USERPROFILE -Force -Filter ".wslconfig*"` lo muestra.
3. Clonar el repositorio. `.gitattributes` deja los scripts con finales de línea LF, que es lo que esperan los contenedores. Si el repositorio ya estaba clonado desde antes de este cambio, es más fácil clonarlo otra vez.
4. Construir la imagen de Spark y descargar las demás (unos 3 GB; la de Spark tarda unos 10 minutos):
   ```powershell
   cd breb-fraude\infra
   docker compose --env-file completo.env build
   docker compose --env-file completo.env pull
   ```
   `pull` omite la imagen de Spark, que es local. Si una descarga falla, se puede repetir: las capas ya descargadas se conservan. Después de esto, el stack arranca sin internet.

### Perfiles
| Perfil | Comando | Contenido |
|---|---|---|
| `dev` (por defecto, `infra/.env`) | `docker compose up -d` | 1 bróker de Kafka, MongoDB con 1 miembro, 1 worker de Spark |
| `completo` | `docker compose --env-file completo.env up -d` | 3 brókers, replica set de 3 y 2 workers, para las pruebas de falla |
| `streaming` (se suma a otro) | `docker compose --profile streaming up -d` | Jobs de streaming: `job1-bronze`, `job2-riesgo`, `job2-ventanas` y `job3-silver` |
| `consumo` (se suma a otro) | `docker compose --profile consumo up -d` | PostgreSQL, Hive Metastore, Trino y Metabase |
| `orquestacion` (se suma a otro) | `docker compose --profile orquestacion up -d` | PostgreSQL y Airflow 3 |
| `herramientas` (con `run`) | `docker compose run --rm <servicio> ...` | `spark-cliente`, `productor` y `entidad`: se corren a demanda, no quedan arriba |

Para sumar `consumo` a `completo` hay que nombrar los dos perfiles: `docker compose --env-file completo.env --profile completo --profile consumo up -d`. Con un `--profile` en la línea de comandos, Compose ignora el `COMPOSE_PROFILES` de `completo.env`: con solo `--profile consumo` quedaría 1 bróker con la configuración de 3, y `kafka-init` fallaría.

Los topes de memoria de los contenedores suman 8,0 GiB en `dev`, 12,1 GiB en `completo` y 4,6 GiB más con `consumo`. Son máximos, no reservas: el consumo real hay que medirlo con `docker stats` (riesgo R5). Con la prueba de Delta corriendo, el perfil `completo` usó 4,59 GiB de los 11,68 GiB de WSL. Con `dev` + `consumo`, después de `prueba_consulta.py`, el total fue 4,03 GiB. `completo` y `consumo` juntos suman 16,7 GiB de topes; la decisión D5 del plan propone usar `dev` + `consumo` para la demo.

En el perfil completo, **todos** los comandos de `docker compose` llevan `--env-file completo.env`. Para cambiar de perfil hay que borrar los volúmenes: el quórum de Kafka y el replica set quedan guardados en ellos.
```powershell
docker compose --profile "*" down -v
```

### Verificar
```powershell
docker compose ps -a                      # kafka-init, mongo-init e init-recursos terminan con "Exited (0)"
docker compose run --rm spark-cliente python /opt/breb/herramientas/infra/verificar_stack.py   # STACK OK
docker compose run --rm spark-cliente spark-submit /opt/breb/herramientas/infra/prueba_delta_s3.py   # PRUEBA DELTA OK
docker stats --no-stream                  # RAM por contenedor (riesgo R5)
```
Con el perfil `consumo` (en `completo`, con los dos `--profile` de arriba):
```powershell
docker compose --profile consumo run --rm spark-cliente spark-submit /opt/breb/herramientas/infra/prueba_consulta.py   # PRUEBA CONSULTA OK
```
Metabase no se configura a mano: `python -m breb.consumo tablero` (fase 6) crea el administrador con las credenciales de `infra/credenciales/metabase-admin.env` y la conexión a Trino (driver **Starburst**, host `trino`, puerto `8080`, catálogo `lakehouse`, usuario `metabase`, sin contraseña ni SSL). Si alguien crea el administrador desde la interfaz con otras credenciales, `tablero` no puede iniciar sesión. Trino también responde en http://localhost:8090 para clientes del host.
Interfaces: Spark en http://localhost:8080 (master) y 8081/8082 (workers); con `consumo`, Trino en http://localhost:8090 y Metabase en http://localhost:3000. Puertos para clientes del host: Kafka `localhost:29092` (29093 y 29094 en completo), MongoDB `localhost:27017` (con `directConnection=true`), S3 `localhost:8333`, DynamoDB `localhost:8000` y moto `localhost:5000`. Todos escuchan solo en 127.0.0.1. Las credenciales de `infra/credenciales/` son solo para este entorno local.

### Productor (fase 3)
Publica las transacciones de LI-Small en `breb.transacciones.raw`, directo en Kafka (decisión D3). Primero hay que descargar `LI-Small_Trans.csv` del dataset [IBM Transactions for Anti Money Laundering](https://www.kaggle.com/datasets/ealtman2019/ibm-transactions-for-anti-money-laundering-aml) en la carpeta `datos/` del repositorio, que git ignora.
```powershell
docker compose --profile herramientas build productor
docker compose run --rm productor preparar      # una vez: unos 4 GB de RAM y 1 a 2 minutos
docker compose run --rm productor publicar      # 100, 250, 500, 750 y 1.000 ev/s, 60 s cada uno
```
Opciones de `publicar`:
- `--escalones "500:120"` cambia las tasas y duraciones (`tasa:segundos`, separadas por coma).
- `--invalidos 0.01` mezcla un 1 % de eventos inválidos para mostrar la cuarentena.
- `--continuar` sigue después del último `seq` publicado.
- `--max` limita la cantidad de transacciones.
- `--tramo` publica solo el tramo de la demo (`datos/productor/tramo_demo.json`).
- `--solo-invalidos N` publica N eventos inválidos y nada más (para la cuarentena); no mueve el progreso del productor.

Al final imprime, por escalón, la tasa lograda y la latencia fuente → Kafka (p50 y p99, RF1: p99 < 300 ms), y guarda el informe en `datos/mediciones/`. Después de `docker compose --profile "*" down -v`, el tópico queda vacío: la siguiente corrida va sin `--continuar`.

### Streaming (fase 4)
Los Jobs 1, 2 y 3 corren en el perfil `streaming`. El Job 2 necesita el estado de MongoDB al corte de validación, así que se reproduce desde cero:
```powershell
docker compose --profile "*" down -v                               # tópico, MongoDB, lakehouse y checkpoints vacíos
docker compose build
docker compose up -d
docker compose run --rm spark-cliente python -m breb.estado cargar  # unos 5,5 GB de RAM y de 3 a 5 minutos
docker compose --profile streaming up -d                           # job1-bronze, job2-riesgo, job2-ventanas y job3-silver
docker compose run --rm productor publicar --escalones "100:60,250:60,500:60"
docker compose run --rm spark-cliente spark-submit /opt/breb/herramientas/streaming/verificar_fase4.py --esperar 600
```
`verificar_fase4.py` compara Kafka, Bronze, Silver, `risk_decisions` y MongoDB. También mide la paridad en línea con el notebook y la latencia del RNF-02, total y por minuto de publicación. Termina en `FASE 4 OK` o en `HAY FALLAS` con lo que no cuadra; en el portátil, con los cuatro jobs y el productor a la vez, la latencia del RNF-02 no se cumple. Cada micro-batch del Job 2 imprime cuánto tardó cada tramo: `docker compose logs job2-riesgo | Select-String "batch"`.

Para seguir publicando sin volver a cargar el estado, se deja el stack arriba y se publica con `--continuar`: los jobs siguen desde sus checkpoints y el productor desde el último `seq`.

### Demo con alertas (tramo)
Con el umbral del modelo hay en promedio una alerta cada 3.900 transacciones. La demo arranca en el tramo de 15.000 transacciones con más alertas (25 en LI-Small), que `tramo` elige y guarda en `datos/productor/tramo_demo.json`; `cargar`, `entidad` y `publicar` lo leen con `--tramo`.
```powershell
docker compose --profile "*" down -v                                # todo vacío: la demo empieza en el tramo
docker compose build
docker compose up -d
docker compose run --rm spark-cliente python -m breb.estado tramo    # solo la primera vez: 6 GB y 5 min
docker compose run --rm spark-cliente python -m breb.estado cargar --tramo
docker compose --profile streaming up -d
```
En otras dos ventanas de PowerShell (dentro de `infra`), los consumidores de las alertas:
```powershell
docker compose run --rm entidad --tramo        # todas las entidades que participan en alertas del tramo
docker compose run --rm entidad --analistas    # todas las alertas
```
Y en la primera, después de un minuto:
```powershell
docker compose run --rm productor publicar --tramo --escalones "50:310"
docker compose run --rm spark-cliente spark-submit /opt/breb/herramientas/streaming/verificar_fase4.py --esperar 900
```
Gold (Job 4), con los jobs de streaming apagados para liberar memoria:
```powershell
docker compose stop job1-bronze job2-riesgo job2-ventanas job3-silver
docker compose run --rm spark-cliente spark-submit --master local[2] --driver-memory 2g /opt/breb/jobs/job4_gold.py
```
Termina en `JOB 4 OK`, con los controles de calidad y la precisión y el recall de las alertas contra las etiquetas reales.

### Orquestación: Airflow 3 (fase 5)
Airflow 3.3.2 corre en un solo contenedor (`airflow standalone`, LocalExecutor) con el metastore en PostgreSQL. Las tareas de Spark corren dentro de ese contenedor en modo local (`local[2]`, 2 GB de driver), porque los jobs de streaming ocupan los núcleos del worker.
```powershell
docker compose build
docker compose --profile orquestacion build airflow   # la primera vez instala Airflow sobre la imagen de Spark
docker compose --profile orquestacion up -d           # PostgreSQL y Airflow
docker compose ps airflow                             # esperar "healthy" (de 1 a 3 minutos)
```
Interfaz: http://localhost:8085, usuario `admin` y la clave `AIRFLOW_ADMIN_CLAVE` de `infra/credenciales/airflow.env`.

| DAG | Cuándo | Qué hace |
|---|---|---|
| `breb_lakehouse_pipeline` | cada 15 minutos; arranca pausado | Silver → Gold (dimensiones, hechos), controles de calidad y perfiles en MongoDB |
| `breb_lakehouse_mantenimiento` | todos los días a las 08:00 UTC (03:00 en Bogotá); arranca pausado | OPTIMIZE y VACUUM de las tablas del lakehouse |
| `reprocess_quarantine` | bajo demanda | Corrige la cuarentena de Bronze, hace MERGE a Silver y dispara el pipeline |
| `bootstrap_state` | bajo demanda | Estado inicial del Job 2 en MongoDB. Parámetro `inicio`: `tramo` (por defecto) o `corte` |

El pipeline procesa Silver en una versión fija de Delta (`check_new_silver_data`) y se salta el resto si no hay datos nuevos. `check_streaming_health` solo falla si un job de streaming está activo y va más de 50.000 mensajes atrasado; con el streaming apagado, Gold se calcula igual.
```powershell
docker compose exec airflow airflow dags unpause breb_lakehouse_pipeline   # corre ahora y después cada 15 minutos
docker compose exec airflow airflow dags trigger breb_lakehouse_pipeline   # una corrida más, a mano
docker compose exec airflow airflow dags list-runs breb_lakehouse_pipeline
```
Si el pipeline está pausado, las corridas que se disparen (también las de `reprocess_quarantine`) quedan en cola hasta quitarle la pausa.

OPTIMIZE y VACUUM van en su propio DAG porque son lentos y no hace falta correrlos cada 15 minutos: en la primera corrida en Docker, con los dos dentro del pipeline, sumaron 8 de 11 min 39 s (VACUUM solo, 6 min 46 s); sin ellos, la corrida tardó 3 min 33 s. El DAG de mantenimiento arranca en pausa para que no corra durante la demo:
```powershell
docker compose exec airflow airflow dags unpause breb_lakehouse_mantenimiento   # queda programado para las 08:00 UTC
docker compose exec airflow airflow dags trigger breb_lakehouse_mantenimiento   # una corrida ahora (sin pausa)
```

Cuarentena: el productor publica eventos inválidos, el Job 1 los deja en la cuarentena de Bronze y el DAG recupera los que se pueden corregir sin inventar datos. Los recuperados llegan a Gold sin decisión del modelo, porque el Job 2 no los puntúa.
```powershell
docker compose up -d job1-bronze                                  # si estaba apagado
docker compose run --rm productor publicar --solo-invalidos 50    # no mueve el progreso del productor
docker compose exec airflow airflow dags trigger reprocess_quarantine
```
El resultado de cada evento queda en `silver.reprocesos_cuarentena`.

Estado inicial: `bootstrap_state` hace lo mismo que `python -m breb.estado tramo` y `cargar --tramo --reemplazar` (y antes `python -m breb.productor preparar`, si falta `datos/productor/eventos_test.parquet`). Necesita unos 5,5 GB (decisión D4) y falla si encuentra jobs de streaming corriendo:
```powershell
docker compose stop job1-bronze job2-riesgo job2-ventanas job3-silver
docker compose exec airflow airflow dags trigger bootstrap_state
```
Para empezar en el corte de validación, se dispara desde la interfaz (botón Trigger) con `inicio` = `corte`.

Memoria: `orquestacion` suma 8,4 GiB de topes (Airflow 8 GiB, porque `bootstrap_state` corre dentro del contenedor). No conviene tener `streaming`, `consumo` y `orquestacion` arriba a la vez en el equipo de 16 GB: para consultar en Trino y Metabase, primero se apagan los jobs de streaming.

### Consumo: Trino, permisos, linaje y tablero (fase 6)
Después del Job 4, con los jobs de streaming apagados:
```powershell
docker compose --profile consumo up -d                                         # PostgreSQL, Hive Metastore, Trino y Metabase
docker compose --profile consumo run --rm spark-cliente python -m breb.consumo registrar
docker compose --profile consumo run --rm spark-cliente python -m breb.consumo permisos
docker compose --profile consumo run --rm spark-cliente python -m breb.consumo linaje
docker compose --profile consumo run --rm spark-cliente python -m breb.consumo tablero
```
- `registrar` registra en Trino las tablas de las tres capas (`lakehouse.bronze`, `lakehouse.silver` y `lakehouse.gold`) y muestra cuántas filas tiene cada una.
- `permisos` comprueba las reglas de `infra/trino/etc/reglas.json`: `analista` y `metabase` solo leen Gold; `cumplimiento` lee las tres capas sin poder modificar nada.
- `linaje` reconstruye, como `cumplimiento`, la alerta con mayor score desde Kafka hasta Gold y lo que hicieron las entidades (RNF-06). Con un `transaction_uuid` al final, reconstruye esa transacción.
- `tablero` configura Metabase (administrador de `infra/credenciales/metabase-admin.env`), lo conecta a Trino y crea el tablero "Bre-B · detección de fraude". Imprime el enlace, p. ej. http://localhost:3000/dashboard/1.

Para consultar a mano: `docker compose --profile consumo exec trino trino --user cumplimiento --catalog lakehouse`.

### Apagar
`docker compose --profile "*" down` detiene todo y conserva los datos; con `-v` también borra los volúmenes.

Sin `--profile`, `down` solo quita los servicios que no tienen perfil: los jobs de streaming, Airflow, Trino y Metabase siguen ahí, y Compose no puede borrar un volumen que un contenedor todavía usa. Así sobreviven los checkpoints de los jobs (volumen `breb_checkpoints`) aunque Kafka y el lakehouse queden vacíos. `--profile "*"` activa todos los perfiles.

### Problemas comunes
- **`lookup production.cloudfront.docker.com: no such host`** al descargar imágenes: Windows no resolvió el dominio del que Docker Hub sirve las capas. Repetir `docker compose pull`. Si sigue fallando, comparar `Resolve-DnsName production.cloudfront.docker.com` con `Resolve-DnsName production.cloudfront.docker.com -Server 1.1.1.1`. Si solo el segundo responde, el DNS de la red falla: cambiar el DNS del adaptador a `1.1.1.1` y `8.8.8.8` (Configuración → Red e Internet → Wi-Fi → Propiedades de hardware → Asignación de servidor DNS) o usar otra red, y reiniciar Docker Desktop.
- **`No writable volumes and no free volumes left`** en el log de SeaweedFS, o Spark reintentando escrituras durante minutos: SeaweedFS se quedó sin volúmenes para un bucket. `WEED_MASTER_VOLUME_GROWTH_COPY_1=1` en `docker-compose.yml` lo evita; si pasa, liberar disco en Windows o borrar los datos de prueba con `docker compose --profile "*" down -v`.
- **`verificar_fase4.py` se queda en `esperando: Bronze 0, Silver 0, risk_decisions 0` con Kafka lleno**: los jobs arrancaron con los checkpoints de una corrida anterior, casi siempre porque se reinició con `docker compose down -v` sin `--profile "*"`. Si la corrida anterior publicó el mismo tramo, los offsets guardados coinciden con los del tópico nuevo y los jobs creen que ya lo leyeron todo; si publicó más, fallan por datos perdidos y se reinician sin parar. `docker volume inspect breb_checkpoints --format "{{.CreatedAt}}"` muestra una fecha anterior al reinicio. Se arregla reiniciando desde cero con `docker compose --profile "*" down -v`.
- **`seaweedfs: Name or service not known`** (o `kafka-1`, `mongo-1`): la infraestructura está apagada. `docker compose --profile streaming stop` apaga **todos** los servicios, no solo los jobs; para apagar solo los jobs: `docker compose stop job1-bronze job2-riesgo job2-ventanas job3-silver`. Se recupera con `docker compose up -d`; los datos se conservan.
- **`image "docker.io/library/breb-spark:local": already exists`** al construir: pasaba cuando varios servicios tenían la misma sección `build`. Ahora solo `spark-master` construye la imagen y los demás la reutilizan.

## Artefactos del modelo
`modelo/aml_xgb/` ya trae los tres artefactos que usa el Job 2: `model_meta.json`, `aml_xgb.json` y `golden_sample.parquet`. Salen de la pestaña Output de la versión 1 de [AML XGBoost Bre-B Fraud](https://www.kaggle.com/code/sebastianmedinacab/aml-xgboost-bre-b-fraud); solo hay que volver a descargarlos si se reentrena el modelo. No se usa `ohe.joblib`.

## Atribución
- Modelo adaptado de [XGBoost + Leiden community detection](https://www.kaggle.com/code/arturmarkosyan2/xgboost-leiden-community-detection-pr-auc-0-39) (Artur Markosyan).
- Datos: [IBM Transactions for Anti Money Laundering](https://www.kaggle.com/datasets/ealtman2019/ibm-transactions-for-anti-money-laundering-aml), licencia CDLA-Sharing-1.0 (Altman et al., NeurIPS 2023).
