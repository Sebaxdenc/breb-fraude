# Bre-B en AWS · MVP en una EC2

Rama `aws/mvp-ec2`. El mismo pipeline del Entregable 2, con el almacenamiento y la mensajería en servicios gestionados de AWS y el resto del stack en una sola EC2.

| Pieza | Local (`main`) | En esta rama |
|---|---|---|
| Lakehouse Bronze, Silver y Gold | SeaweedFS | **Amazon S3** (`breb-<cuenta>-lakehouse-*`) |
| Log de commits de Delta (RNF-05) | DynamoDB Local | **Amazon DynamoDB** (tabla `delta_log`) |
| Alertas (RF15) | moto | **Amazon SNS** (`fraud-alerts-critical`) y **Amazon SQS** (una cola por entidad y la de analistas) |
| Credenciales | `credenciales/s3.env` | **Rol de IAM** de la instancia, sin llaves en archivos |
| Kafka, MongoDB, Spark, Airflow, Hive Metastore, Trino y Metabase | Docker en el portátil | Los mismos contenedores, en **Amazon EC2** |

Lo que no cambia de lugar, y por qué, está al final: "Lo que sigue en EC2".

Costo aproximado en us-east-1: la instancia `m6i.2xlarge` cuesta unos 0,38 USD por hora, y el disco de 100 GiB unos 0,27 USD por día. S3, DynamoDB, SNS y SQS cuestan centavos con este volumen. Detengan la instancia cuando no la usen.

## 1. Rol de IAM para la instancia
Consola → **IAM** → **Roles** → **Create role**.
1. Trusted entity type: **AWS service**. Use case: **EC2**. Next.
2. Permisos: `AmazonS3FullAccess`, `AmazonDynamoDBFullAccess`, `AmazonSNSFullAccess` y `AmazonSQSFullAccess`. Next.
3. Role name: `breb-ec2`. **Create role**.

Para un MVP bastan las políticas administradas; en producción se limitarían a los buckets, la tabla, el tópico y las colas del proyecto.

## 2. Lanzar la instancia
Consola → arriba a la derecha, región **US East (N. Virginia) us-east-1** → **EC2** → **Launch instance**.

| Campo | Valor |
|---|---|
| Name | `breb-mvp` |
| AMI | **Ubuntu Server 24.04 LTS**, arquitectura **64-bit (x86)** |
| Instance type | **m6i.2xlarge** (8 vCPU, 32 GiB). Ver "Si no deja lanzar la m6i.2xlarge" |
| Key pair | **Create new key pair** → nombre `breb`, tipo RSA, formato `.pem`. El navegador lo descarga en `Descargas` como `breb.pem` |
| Network settings | VPC por defecto, **Auto-assign public IP: Enable**. Security group nuevo con una sola regla: **SSH** desde **My IP** |
| Configure storage | **100 GiB gp3** (32 no alcanzan: imágenes, datos y volúmenes). Ignoren el aviso de la capa gratuita |
| Advanced details → IAM instance profile | `breb-ec2` |
| Advanced details → Metadata version | **V2 only (token required)** |
| Advanced details → Metadata response hop limit | **2** (con 1, los contenedores no reciben las credenciales del rol) |

**Launch instance** y copien la **Public IPv4 address**.

Las interfaces web (Airflow, Spark, Trino y Metabase) quedan en `127.0.0.1` de la instancia y se ven con un túnel SSH (paso 8). No hay que abrir más puertos.

### Si no deja lanzar la m6i.2xlarge
Una cuenta nueva puede tener un límite de vCPU bajo y responder `You have requested more vCPU capacity than your current vCPU limit`. Usen **r6i.xlarge** (4 vCPU, 32 GiB) y, en la instancia, antes del paso 6:
```bash
echo 'export SPARK_WORKER_CORES=4 JOB2_NUCLEOS=1' >> ~/.bashrc && source ~/.bashrc
```
Docker no acepta un tope de CPU mayor que los núcleos de la máquina, y el worker de Spark pide 5.

## 3. Conectarse
En PowerShell:
```powershell
ssh -i $HOME\Downloads\breb.pem ubuntu@<IP pública>
```
Si responde `UNPROTECTED PRIVATE KEY FILE`:
```powershell
icacls $HOME\Downloads\breb.pem /inheritance:r /grant:r "$($env:USERNAME):(R)"
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
git clone -b aws/mvp-ec2 https://github.com/Sebaxdenc/breb-fraude.git
mkdir -p ~/breb-fraude/datos
```
Desde PowerShell en el portátil, en la carpeta del repositorio, suban el CSV de Kaggle:
```powershell
scp -i $HOME\Downloads\breb.pem .\datos\LI-Small_Trans.csv ubuntu@<IP pública>:~/breb-fraude/datos/
```

## 5. Preparar el modo AWS
```bash
bash ~/breb-fraude/infra/aws/preparar_ec2.sh
source ~/.bashrc
```
Debe terminar en `[aws] listo` después de imprimir la cuenta, el rol `breb-ec2` y los nombres de los buckets. Si dice que un contenedor no llega al servicio de metadatos, el límite de saltos quedó en 1: **EC2** → la instancia → **Actions** → **Instance settings** → **Modify instance metadata options** → hop limit **2**, y repetir el script.

Desde aquí, `docker compose` usa `infra/docker-compose.yml` más `infra/docker-compose.aws.yml` en cualquier carpeta.

## 6. Imágenes (una vez, unos 20 minutos)
```bash
docker compose --profile consumo --profile orquestacion --profile herramientas build
docker compose --profile consumo --profile orquestacion --profile herramientas --profile streaming pull
```

## 7. Pipeline de punta a punta
Son los pasos del README de la raíz, con `grep` en vez de `Select-String`.
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

# Estado inicial (una sola vez el trigger)
docker compose exec airflow airflow dags trigger bootstrap_state
docker compose exec airflow airflow dags list-runs bootstrap_state          # repetir hasta success

# Streaming y consumidores de alertas (las dos entidades, cada una en su propia terminal)
docker compose --profile streaming up -d
docker compose run --rm entidad --tramo
docker compose run --rm entidad --analistas

# Publicar el tramo y verificar
docker compose run --rm productor publicar --tramo --escalones "50:310"
docker compose logs -f job2-riesgo | grep batch                             # en otra terminal, para ver los micro-batches
docker compose run --rm spark-cliente spark-submit /opt/breb/herramientas/streaming/verificar_fase4.py --esperar 900

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
Los resultados esperados son los mismos del README de la raíz: el RF1 puede salir `NO cumple` con los jobs prendidos, y la latencia del RNF-02 sigue siendo la única falla de la verificación.

Para empezar de cero en la instancia: `docker compose --profile "*" down -v`. Eso borra los volúmenes de la instancia, **no** lo que quedó en S3, DynamoDB, SNS y SQS. Para vaciar también el lakehouse:
```bash
docker compose run --rm spark-cliente python -c "import boto3; from breb.buckets import CAPAS, bucket; [boto3.resource('s3').Bucket(bucket(c)).objects.all().delete() for c in CAPAS]"
```
La tabla `delta_log` se puede dejar: cada tabla Delta usa su propia ruta como clave.

## 8. Ver las interfaces
En una ventana de PowerShell que se deja abierta. Si el stack local del portátil está prendido, primero apáguenlo, porque usa los mismos puertos:
```powershell
ssh -i $HOME\Downloads\breb.pem -N -L 8085:localhost:8085 -L 8080:localhost:8080 -L 8090:localhost:8090 -L 3000:localhost:3000 ubuntu@<IP pública>
```
Airflow en http://localhost:8085, Spark en http://localhost:8080, Trino en http://localhost:8090/ui/ y Metabase en http://localhost:3000. Las credenciales son las mismas del README.

## 9. Evidencia para el video, en la consola de AWS
- **EC2**: la instancia `breb-mvp` en *Running*, con el rol `breb-ec2` en la pestaña *Security*.
- **S3**: los buckets `breb-<cuenta>-lakehouse-bronze`, `-silver` y `-gold`. Dentro de `transacciones/`, los Parquet y la carpeta `_delta_log/` con un JSON por commit.
- **DynamoDB** → *Tables* → `delta_log` → *Explore table items*: una entrada por commit de Delta.
- **SNS** → *Topics* → `fraud-alerts-critical` → *Subscriptions*: una por cola, con la política de filtro por entidad.
- **SQS** → *Queues*: `breb-entidad-*` y `breb-analistas-fraude`, con mensajes enviados y recibidos en *Monitoring*. No usen *Poll for messages* mientras los consumidores están abiertos: se llevaría los mensajes.
- En la terminal, `docker compose ps`: no corre ningún emulador.

## 10. Apagar y borrar
- Entre sesiones: **EC2** → la instancia → **Instance state** → **Stop**. El disco se conserva y la IP pública cambia al volver a prenderla.
- Al terminar el curso: **Terminate** la instancia (borra el disco), vaciar y borrar los tres buckets, borrar la tabla `delta_log`, el tópico, las colas, el rol `breb-ec2` y el par de llaves.
- Recomendado: **Billing** → **Budgets** → un presupuesto de 10 USD con alerta por correo.

## Lo que sigue en EC2
| Pieza | Servicio gestionado | Qué falta para moverla |
|---|---|---|
| Kafka | Amazon MSK | MSK vive dentro de la VPC y, con autenticación de IAM, Spark y el productor necesitan otra librería |
| Spark | Amazon EMR | Los checkpoints y los archivos de decisiones pendientes del Job 2 están en un volumen que comparten el driver y los ejecutores; en EMR tienen que ir a S3. Además hay que empaquetar el código, el modelo y las librerías |
| Airflow | Amazon MWAA | Las tareas corren `spark-submit` dentro del contenedor de Airflow, y `bootstrap_state` usa unos 5,5 GB ahí. En MWAA se reemplazan por operadores que lanzan trabajos en EMR |
| Trino y las reglas por rol | Amazon Athena y Lake Formation | `registrar`, `permisos` y `linaje` le hablan a Trino; las reglas de `reglas.json` pasan a permisos de Lake Formation |
| MongoDB | MongoDB Atlas | Solo cambia la conexión; el plan gratuito (512 MB) puede quedar corto para el estado inicial |
| Monitoreo y secretos | CloudWatch, KMS y Secrets Manager | Fuera del MVP |
