# Bre-B en AWS · MVP en una EC2

El mismo pipeline del Entregable 2, con el almacenamiento y la mensajería en servicios gestionados de AWS y el resto del stack en una sola EC2.

| Pieza | Modo local | Modo AWS |
|---|---|---|
| Lakehouse Bronze, Silver y Gold | SeaweedFS | **Amazon S3** (`breb-<cuenta>-lakehouse-*`) |
| Log de commits de Delta (RNF-05) | DynamoDB Local | **Amazon DynamoDB** (tabla `delta_log`) |
| Alertas (RF15) | moto | **Amazon SNS** (`fraud-alerts-critical`) y **Amazon SQS** (una cola por entidad y la de analistas) |
| Consultas sobre el lakehouse | Trino + Hive Metastore | **Amazon Athena** + **AWS Glue Data Catalog** (paso 11), además de Trino |
| Credenciales | `credenciales/s3.env` | **Rol de IAM** de la instancia (`LabInstanceProfile` en AWS Academy), sin llaves en archivos |
| Kafka, MongoDB, Spark, Airflow, Hive Metastore, Trino y Metabase | Docker en el portátil | Los mismos contenedores, en **Amazon EC2** |

Lo que no cambia de lugar, y por qué, está al final: "Lo que sigue en EC2".

## Límites de AWS Academy (Learner Lab)
- No se pueden crear roles de IAM. Ya existen el rol `LabRole` y el perfil de instancia `LabInstanceProfile`, con permisos sobre S3, DynamoDB, SNS y SQS.
- EC2 permite tamaños hasta **large** (2 vCPU), un máximo de 9 instancias y discos de hasta **100 GB**. Solo funcionan us-east-1 y us-west-2; esta configuración usa **us-east-1**.
- La sesión del laboratorio dura unas horas. Al terminar, el laboratorio detiene las instancias y el disco se conserva. Con **Start Lab** empieza otra sesión.
- El costo sale del presupuesto del laboratorio: una `r5.large` cuesta unos 0,13 USD por hora. S3, DynamoDB, SNS y SQS cuestan centavos con este volumen.

Con 2 vCPU el stack corre igual que en el portátil, pero más lento. `docker-compose.aws.yml` limita a 2 CPU el worker de Spark y Airflow, porque Docker no acepta un tope mayor que los núcleos de la máquina. Si la `r5.large` no se deja lanzar, el plan B (al final) deja el cómputo en el portátil y usa igual S3, DynamoDB, SNS y SQS reales.

## 1. Abrir el laboratorio
En AWS Academy → el curso → **Modules** → **Launch AWS Academy Learner Lab** → **Start Lab**. Cuando el círculo quede en verde, **AWS** abre la consola. Revisen arriba a la derecha que la región sea **N. Virginia (us-east-1)**.

En **AWS Details** → **Download PEM** se descarga `labsuser.pem`, la llave del par `vockey`, que sirve para entrar por SSH.

## 2. Lanzar la instancia
Consola → **EC2** → **Launch instance**.

| Campo | Valor |
|---|---|
| Name | `breb-mvp` |
| AMI | **Ubuntu Server 24.04 LTS**, arquitectura **64-bit (x86)** |
| Instance type | **r5.large** (2 vCPU, 16 GiB). Con 8 GiB (`t3.large`, `m5.large`) el stack no cabe |
| Key pair | `vockey` |
| Network settings | VPC por defecto, **Auto-assign public IP: Enable**. Security group nuevo con una sola regla: **SSH** desde **My IP** |
| Configure storage | **100 GiB gp3**, el máximo del laboratorio. Ignoren el aviso de la capa gratuita |
| Advanced details → IAM instance profile | **LabInstanceProfile** |
| Advanced details → Metadata version | **V2 only (token required)** |
| Advanced details → Metadata response hop limit | **2** (con 1, los contenedores no reciben las credenciales del rol) |

**Launch instance** y copien la **Public IPv4 address**. Si la consola rechaza el tipo de instancia, prueben `r6i.large`; si tampoco, vayan al plan B.

Las interfaces web (Airflow, Spark, Trino y Metabase) quedan en `127.0.0.1` de la instancia y se ven con un túnel SSH (paso 8). No hay que abrir más puertos.

## 3. Conectarse
En PowerShell:
```powershell
ssh -i $HOME\Downloads\labsuser.pem ubuntu@<IP pública>
```
Si responde `UNPROTECTED PRIVATE KEY FILE`:
```powershell
icacls $HOME\Downloads\labsuser.pem /inheritance:r /grant:r "$($env:USERNAME):(R)"
```
Para tener varias terminales en la instancia (jobs, entidades, comandos), abran varias ventanas de PowerShell con el mismo `ssh`.

## 4. Docker, código y datos
En la instancia:
```bash
curl -fsSL https://get.docker.com | sudo sh
sudo usermod -aG docker ubuntu
exit
```
Vuelvan a conectarse (paso 3) para que el grupo `docker` aplique, y luego:
```bash
docker compose version                      # v2.24 o posterior
git clone https://github.com/Sebaxdenc/breb-fraude.git
mkdir -p ~/breb-fraude/datos
```
Desde PowerShell en el portátil, en la carpeta del repositorio, suban el CSV de Kaggle:
```powershell
scp -i $HOME\Downloads\labsuser.pem .\datos\LI-Small_Trans.csv ubuntu@<IP pública>:~/breb-fraude/datos/
```

## 5. Preparar el modo AWS
```bash
bash ~/breb-fraude/infra/aws/preparar_ec2.sh
source ~/.bashrc
```
Debe terminar en `[aws] listo` después de imprimir la cuenta, el rol `LabRole` y los nombres de los buckets. Si dice que un contenedor no llega al servicio de metadatos, el límite de saltos quedó en 1: **EC2** → la instancia → **Actions** → **Instance settings** → **Modify instance metadata options** → hop limit **2**, y repetir el script.

Desde aquí, `docker compose` usa `infra/docker-compose.yml` más `infra/docker-compose.aws.yml` en cualquier carpeta.

## 6. Imágenes (una vez, de 30 a 40 minutos con 2 vCPU)
```bash
docker compose --profile consumo --profile orquestacion --profile herramientas build
docker compose --profile consumo --profile orquestacion --profile herramientas --profile streaming pull
```

## 7. Pipeline de punta a punta
Son los pasos del README de la raíz, con `grep` en vez de `Select-String`. Con 2 vCPU cada paso tarda más que en el portátil.
```bash
# Infraestructura: init-recursos crea los buckets en S3, la tabla delta_log en DynamoDB, el tópico en SNS y la cola de analistas en SQS
docker compose up -d
docker compose --profile orquestacion up -d
docker compose ps -a                         # init-recursos, kafka-init y mongo-init en "Exited (0)"; no aparecen seaweedfs, dynamodb-local ni moto
docker compose logs init-recursos
docker compose run --rm spark-cliente python /opt/breb/herramientas/infra/verificar_stack.py   # STACK OK

# Datos de la reproducción (una vez)
docker compose run --rm productor preparar
docker compose run --rm spark-cliente python -m breb.estado tramo

# Estado inicial (una sola vez el trigger; con 2 vCPU, de 15 a 25 minutos)
docker compose exec airflow airflow dags trigger bootstrap_state
docker compose exec airflow airflow dags list-runs bootstrap_state          # repetir hasta success

# Streaming y consumidores de alertas (las dos entidades, cada una en su propia terminal)
docker compose --profile streaming up -d
docker compose run --rm entidad --tramo
docker compose run --rm entidad --analistas

# Publicar parte del tramo y verificar. Con 2 vCPU los jobs procesan muchas menos transacciones por segundo que en
# el portátil: 5.000 a 20 por segundo alcanzan para el video. Para el tramo completo: --escalones "50:310" sin --max
docker compose run --rm productor publicar --tramo --max 5000 --escalones "20:260"
docker compose logs -f job2-riesgo | grep batch                             # en otra terminal, para ver los micro-batches
docker compose run --rm spark-cliente spark-submit /opt/breb/herramientas/streaming/verificar_fase4.py --esperar 1800

# Gold con Airflow
docker compose stop job1-bronze job2-riesgo job2-ventanas job3-silver
docker compose exec airflow airflow dags unpause breb_lakehouse_pipeline
docker compose exec airflow airflow dags list-runs breb_lakehouse_pipeline  # repetir hasta success
docker compose exec airflow airflow dags pause breb_lakehouse_pipeline

# Consumo: Trino, permisos, tablero de Metabase y linaje
docker compose --profile consumo up -d
docker compose --profile consumo run --rm spark-cliente python -m breb.consumo registrar
docker compose --profile consumo run --rm spark-cliente python -m breb.consumo permisos
docker compose --profile consumo run --rm spark-cliente python -m breb.consumo tablero
docker compose --profile consumo run --rm spark-cliente python -m breb.consumo linaje
```
Los resultados esperados son los del README de la raíz, con 5.000 transacciones en vez de 15.000: el RF1 puede salir `NO cumple` con los jobs prendidos, y la latencia del RNF-02 sigue siendo la única falla de la verificación, más alta que en el portátil.

Para empezar de cero en la instancia: `docker compose --profile "*" down -v`. Eso borra los volúmenes de la instancia, **no** lo que quedó en S3, DynamoDB, SNS y SQS. Para vaciar también el lakehouse:
```bash
docker compose run --rm spark-cliente python -c "import boto3; from breb.buckets import CAPAS, bucket; [boto3.resource('s3').Bucket(bucket(c)).objects.all().delete() for c in CAPAS]"
```
La tabla `delta_log` se puede dejar: cada tabla Delta usa su propia ruta como clave.

## 8. Ver las interfaces
En una ventana de PowerShell que se deja abierta. Si el stack local del portátil está prendido, primero apáguenlo, porque usa los mismos puertos:
```powershell
ssh -i $HOME\Downloads\labsuser.pem -N -L 8085:localhost:8085 -L 8080:localhost:8080 -L 8090:localhost:8090 -L 3000:localhost:3000 ubuntu@<IP pública>
```
Airflow en http://localhost:8085, Spark en http://localhost:8080, Trino en http://localhost:8090/ui/ y Metabase en http://localhost:3000. Las credenciales son las mismas del README.

## 9. Evidencia para el video, en la consola de AWS
- **EC2**: la instancia `breb-mvp` en *Running*, con el rol `LabRole` en la pestaña *Security*.
- **S3**: los buckets `breb-<cuenta>-lakehouse-bronze`, `-silver` y `-gold`. Dentro de `transacciones/`, los Parquet y la carpeta `_delta_log/` con un JSON por commit.
- **DynamoDB** → *Tables* → `delta_log` → *Explore table items*: una entrada por commit de Delta.
- **SNS** → *Topics* → `fraud-alerts-critical` → *Subscriptions*: una por cola, con la política de filtro por entidad.
- **SQS** → *Queues*: `breb-entidad-*` y `breb-analistas-fraude`, con mensajes enviados y recibidos en *Monitoring*. No usen *Poll for messages* mientras los consumidores están abiertos: se llevaría los mensajes.
- En la terminal, `docker compose ps`: no corre ningún emulador.
- **Athena** (después del paso 11) → *Query editor*: las bases `bronze`, `silver` y `gold` en *AwsDataCatalog*, y una de las preguntas de negocio corriendo ahí mismo.
- **Glue** → *Data Catalog* → *Tables*: las tablas con *Table type* `DELTA` y su ubicación en S3.

## 10. Apagar y borrar
- Entre sesiones: **EC2** → la instancia → **Instance state** → **Stop**, o **End Lab**, que también la detiene. El disco se conserva y la IP pública cambia al volver a prenderla. Al prenderla, `docker compose up -d` y `docker compose --profile orquestacion up -d` levantan lo que falte.
- Al terminar el curso: **Terminate** la instancia (borra el disco), vaciar y borrar los tres buckets, y borrar la tabla `delta_log`, el tópico y las colas.

## 11. Athena y el Glue Data Catalog
Después del paso 7, con Gold ya armado:
```bash
cd ~/breb-fraude && git pull
docker compose --profile consumo run --rm spark-cliente python /opt/breb/herramientas/aws/athena.py --metabase
```
`herramientas/aws/athena.py`:
1. Crea el bucket `breb-<cuenta>-athena-resultados` y lo deja como ubicación de resultados del workgroup `primary`. Con eso, el editor de la consola deja de pedirla.
2. Registra en el Glue Data Catalog las mismas tablas que `breb.consumo registrar` registra en Trino, en las bases `bronze`, `silver` y `gold`. Athena lee el esquema del `_delta_log`, así que no hay que declarar columnas.
3. Corre en Athena las 9 consultas del tablero de Metabase sin cambiarlas: Athena versión 3 usa el SQL de Trino. Imprime cuánto escaneó cada una.
4. Con `--metabase`, agrega Athena como segunda base de datos de Metabase y arma el mismo tablero sobre ella, en la colección *Bre-B (Athena)*. El driver toma el rol de la instancia, sin llaves.

Termina en `ATHENA OK`. Si Gold cambia (otra corrida de Airflow), Athena lo ve en la siguiente consulta; no hay que volver a registrar. Si una consulta falla, el script sigue con las demás y dice cuál falló.

Trino se queda: los permisos por rol (`breb.consumo permisos`) necesitarían Lake Formation o roles de IAM, y el laboratorio no permite ninguno de los dos.

## Plan B: cómputo en el portátil, servicios de AWS reales
Si el laboratorio no deja lanzar una instancia de 16 GiB, el mismo stack corre en el portátil contra S3, DynamoDB, SNS y SQS de la cuenta del laboratorio. En AWS quedan el almacenamiento y la mensajería; Kafka, Spark, Airflow, Trino y Metabase quedan en Docker local.

1. **AWS Details** → **AWS CLI: Show**. Con esos tres valores, creen `infra\aws\aws.env` (el repositorio lo ignora y no se sube):
   ```
   AWS_REGION=us-east-1
   AWS_DEFAULT_REGION=us-east-1
   BREB_BUCKET_PREFIJO=breb-<número de la cuenta>-
   AWS_ACCESS_KEY_ID=<aws_access_key_id>
   AWS_SECRET_ACCESS_KEY=<aws_secret_access_key>
   AWS_SESSION_TOKEN=<aws_session_token>
   ```
   El número de la cuenta sale en **AWS Details** y en el menú de arriba a la derecha de la consola.
2. En PowerShell, en la raíz del repositorio:
   ```powershell
   git pull
   $env:COMPOSE_FILE = "infra\docker-compose.yml;infra\docker-compose.aws.yml"
   $env:BREB_CPUS_MAX = "5"
   docker compose --profile "*" down -v
   ```
   Desde ahí, los pasos 6 y 7 de esta guía, con `Select-String` en vez de `grep` y `--escalones "50:310"` sin `--max`, porque el portátil sí alcanza.
3. Las credenciales del laboratorio vencen cuando termina la sesión. Empiecen con una sesión nueva. Si vencen a mitad de camino, actualicen `aws.env` y recreen los contenedores que estén corriendo con `docker compose up -d --force-recreate`.
4. Para volver a la demo local: `docker compose --profile "*" down -v`, luego `Remove-Item Env:COMPOSE_FILE` y `Remove-Item Env:BREB_CPUS_MAX`. Esto también borra los datos locales; la preparación de la demo empieza en P2 de todos modos.

## Lo que sigue en EC2
| Pieza | Servicio gestionado | Qué falta para moverla |
|---|---|---|
| Kafka | Amazon MSK | MSK vive dentro de la VPC y, con autenticación de IAM, Spark y el productor necesitan otra librería |
| Spark | Amazon EMR | Los checkpoints y los archivos de decisiones pendientes del Job 2 están en un volumen que comparten el driver y los ejecutores; en EMR tienen que ir a S3. Además hay que empaquetar el código, el modelo y las librerías |
| Airflow | Amazon MWAA | Las tareas corren `spark-submit` dentro del contenedor de Airflow, y `bootstrap_state` usa unos 5,5 GB ahí. En MWAA se reemplazan por operadores que lanzan trabajos en EMR |
| Trino y las reglas por rol | Amazon Athena y Lake Formation | Athena ya consulta las tres capas (paso 11). Faltan los permisos: las reglas de `reglas.json` pasan a Lake Formation, que el laboratorio no ofrece, y `permisos` y `linaje` le hablan a Trino |
| MongoDB | MongoDB Atlas | Solo cambia la conexión; el plan gratuito (512 MB) puede quedar corto para el estado inicial |
| Monitoreo y secretos | CloudWatch, KMS y Secrets Manager | Fuera del MVP |
