# Bre-B · Detección de fraude en tiempo real

Proyecto final de ST1630 Sistemas Intensivos en Datos (EAFIT, 2026-2). Implementación de la arquitectura del Entregable 1 v2.0: Kafka, Spark Structured Streaming, Delta Lake, MongoDB, Airflow, Trino, Metabase y alertas por SNS/SQS, con un modelo XGBoost de Kaggle dentro del Job 2.

- **Plan y avance:** [PLAN.md](PLAN.md)
- **Contratos de integración:** [contratos/README.md](contratos/README.md)

## Estructura
| Carpeta | Contenido |
|---|---|
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
| `infra/` | Stack local: `docker-compose.yml`, imagen de Spark, inicialización de Kafka, MongoDB, S3 y DynamoDB |
| `src/breb/streaming/`, `jobs/` | Jobs 1, 2 y 3 de Spark Structured Streaming |
| `herramientas/infra/` | Verificación del stack y prueba de Delta Lake sobre el S3 local |
| `herramientas/streaming/` | Verificación de la fase 4 |
| `tests/` | Pruebas (paridad, Job 2 con MongoDB, contratos) |

Pendiente según el plan: `dags/` (Airflow) y `servicios/` (servicio de la entidad).

## Entorno de desarrollo
La versión de Python y de las librerías es la de la imagen de Spark: PySpark 3.5 admite hasta Python 3.11 y exige numpy < 2.
```bash
uv venv -p 3.11 .venv
uv pip install -p .venv/bin/python -e ".[dev]"
.venv/bin/python -m pytest -q
```

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
| `consumo` (se suma a otro) | `docker compose --profile consumo up -d` | PostgreSQL, Hive Metastore, Trino y Metabase |

Para sumar `consumo` a `completo` hay que nombrar los dos perfiles: `docker compose --env-file completo.env --profile completo --profile consumo up -d`. Con un `--profile` en la línea de comandos, Compose ignora el `COMPOSE_PROFILES` de `completo.env`: con solo `--profile consumo` quedaría 1 bróker con la configuración de 3, y `kafka-init` fallaría.

Los topes de memoria de los contenedores suman 8,0 GiB en `dev`, 12,1 GiB en `completo` y 4,6 GiB más con `consumo`. Son máximos, no reservas: el consumo real hay que medirlo con `docker stats` (riesgo R5). Con la prueba de Delta corriendo, el perfil `completo` usó 4,59 GiB de los 11,68 GiB de WSL. Con `dev` + `consumo`, después de `prueba_consulta.py`, el total fue 4,03 GiB. `completo` y `consumo` juntos suman 16,7 GiB de topes; la decisión D5 del plan propone usar `dev` + `consumo` para la demo.

En el perfil completo, **todos** los comandos de `docker compose` llevan `--env-file completo.env`. Para cambiar de perfil hay que borrar los volúmenes: el quórum de Kafka y el replica set quedan guardados en ellos.
```powershell
docker compose down -v
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
La primera vez, Metabase se configura en http://localhost:3000: crear el usuario administrador y agregar una base de datos de tipo **Starburst** (sirve para Trino) con host `trino`, puerto `8080`, catálogo `lakehouse`, usuario `metabase` y sin contraseña ni SSL. Trino también responde en http://localhost:8090 para clientes del host.
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

Al final imprime, por escalón, la tasa lograda y la latencia fuente → Kafka (p50 y p99, RF1: p99 < 300 ms), y guarda el informe en `datos/mediciones/`. Después de `docker compose down -v`, el tópico queda vacío: la siguiente corrida va sin `--continuar`.

### Streaming (fase 4)
Los Jobs 1, 2 y 3 corren en el perfil `streaming`. El Job 2 necesita el estado de MongoDB al corte de validación, así que se reproduce desde cero:
```powershell
docker compose down -v                                             # tópico, MongoDB y lakehouse vacíos
docker compose build
docker compose up -d
docker compose run --rm spark-cliente python -m breb.estado cargar  # unos 5,5 GB de RAM y de 3 a 5 minutos
docker compose --profile streaming up -d                           # job1-bronze, job2-riesgo y job3-silver
docker compose run --rm productor publicar --escalones "100:60,250:60,500:60"
docker compose run --rm spark-cliente spark-submit /opt/breb/herramientas/streaming/verificar_fase4.py --esperar 600
```
`verificar_fase4.py` compara Kafka, Bronze, Silver, `risk_decisions` y MongoDB. También mide la paridad en línea con el notebook y la latencia del RNF-02, total y por minuto de publicación. Termina en `FASE 4 OK`. Cada micro-batch del Job 2 imprime cuánto tardó cada tramo: `docker compose logs job2-riesgo | Select-String "batch"`.

### Apagar
`docker compose down` detiene todo y conserva los datos; `docker compose down -v` también borra los volúmenes.

### Problemas comunes
- **`lookup production.cloudfront.docker.com: no such host`** al descargar imágenes: Windows no resolvió el dominio del que Docker Hub sirve las capas. Repetir `docker compose pull`. Si sigue fallando, comparar `Resolve-DnsName production.cloudfront.docker.com` con `Resolve-DnsName production.cloudfront.docker.com -Server 1.1.1.1`. Si solo el segundo responde, el DNS de la red falla: cambiar el DNS del adaptador a `1.1.1.1` y `8.8.8.8` (Configuración → Red e Internet → Wi-Fi → Propiedades de hardware → Asignación de servidor DNS) o usar otra red, y reiniciar Docker Desktop.
- **`No writable volumes and no free volumes left`** en el log de SeaweedFS, o Spark reintentando escrituras durante minutos: SeaweedFS se quedó sin volúmenes para un bucket. `WEED_MASTER_VOLUME_GROWTH_COPY_1=1` en `docker-compose.yml` lo evita; si pasa, liberar disco en Windows o borrar los datos de prueba con `docker compose down -v`.
- **`image "docker.io/library/breb-spark:local": already exists`** al construir: pasaba cuando varios servicios tenían la misma sección `build`. Ahora solo `spark-master` construye la imagen y los demás la reutilizan.

## Artefactos del modelo
`modelo/aml_xgb/` debe contener `model_meta.json` (incluido), `aml_xgb.json` y `golden_sample.parquet`, descargados de la pestaña Output de la versión 1 de [AML XGBoost Bre-B Fraud](https://www.kaggle.com/code/sebastianmedinacab/aml-xgboost-bre-b-fraud). No se usa `ohe.joblib`.

## Atribución
- Modelo adaptado de [XGBoost + Leiden community detection](https://www.kaggle.com/code/arturmarkosyan2/xgboost-leiden-community-detection-pr-auc-0-39) (Artur Markosyan).
- Datos: [IBM Transactions for Anti Money Laundering](https://www.kaggle.com/datasets/ealtman2019/ibm-transactions-for-anti-money-laundering-aml), licencia CDLA-Sharing-1.0 (Altman et al., NeurIPS 2023).
