<#
Bre-B · demo en AWS: abre las pestañas de PowerShell conectadas a la EC2 y deja los scripts de la demo en la instancia.
Guía completa: docs/presentacion/demo_en_vivo.md

Uso, en PowerShell, con la instancia breb-mvp en Running:
    powershell -ExecutionPolicy Bypass -File $HOME\Downloads\abrir_demo.ps1
Para volver a abrir solo B, C y D (por ejemplo, si cerraron una):
    powershell -ExecutionPolicy Bypass -File $HOME\Downloads\abrir_demo.ps1 -SoloBCD

Qué hace:
1. Pide la IP pública de la EC2 y escribe C:\breb\ssh.conf con dos nombres: "breb" (una terminal en la EC2) y
   "breb-tunel" (el túnel a Spark, Airflow, Trino y Metabase).
2. Copia en la EC2 los scripts de la demo (~/demo/*.sh). Cada uno muestra en cian el comando de docker compose antes
   de correrlo, para que en el video se vea qué se ejecuta.
3. Abre en Windows Terminal las pestañas A-comandos y E-tunel. Cuando el stack esté arriba (demo/0_prender.sh),
   abre B-job2, C-entidad y D-analistas.
#>
param(
    [string]$IP,
    [string]$Llave = (Join-Path $HOME "Downloads\labsuser.pem"),
    [switch]$SoloBCD
)

$OutputEncoding = [System.Text.UTF8Encoding]::new($false)
$Dir = "C:\breb"
$Conf = "$Dir\ssh.conf"
$Ssh = @("ssh", "-F", $Conf)

function Abrir-Pestanas([array]$pestanas) {
    # Windows Terminal: una pestaña por entrada. Sin Windows Terminal: una ventana de PowerShell por entrada.
    if (Get-Command wt.exe -ErrorAction SilentlyContinue) {
        $argumentos = @("-w", "0")
        for ($i = 0; $i -lt $pestanas.Count; $i++) {
            if ($i -gt 0) { $argumentos += ";" }
            $argumentos += @("new-tab", "--title", $pestanas[$i].Titulo, "powershell", "-NoExit", "-Command") + $pestanas[$i].Comando
        }
        & wt.exe @argumentos
    } else {
        foreach ($p in $pestanas) {
            Start-Process powershell -ArgumentList (@("-NoExit", "-Command") + $p.Comando)
        }
    }
}

function Abrir-BCD {
    Abrir-Pestanas @(
        @{ Titulo = "B-job2"; Comando = $Ssh + @("-t", "breb", "demo/b_job2.sh") },
        @{ Titulo = "C-entidad"; Comando = $Ssh + @("-t", "breb", "demo/c_entidad.sh") },
        @{ Titulo = "D-analistas"; Comando = $Ssh + @("-t", "breb", "demo/d_analistas.sh") }
    )
}

if ($SoloBCD) {
    if (-not (Test-Path $Conf)) { Write-Host "No existe ${Conf}. Corran primero el script sin -SoloBCD." -ForegroundColor Red; exit 1 }
    Abrir-BCD
    exit 0
}

if (-not (Test-Path $Llave)) {
    Write-Host "No encuentro $Llave. En el Learner Lab: AWS Details -> Download PEM, y guárdenla en Descargas." -ForegroundColor Red
    exit 1
}
if (-not $IP) { $IP = Read-Host "IP pública de la EC2 (EC2 -> breb-mvp -> Public IPv4 address)" }
$IP = $IP.Trim()

# 1. Configuración de SSH: con esto, "ssh -F C:\breb\ssh.conf breb" entra a la EC2 sin repetir IP, usuario ni llave
New-Item -ItemType Directory -Force $Dir | Out-Null
$LlaveSsh = $Llave -replace '\\', '/'
$config = @"
Host breb breb-tunel
    HostName $IP
    User ubuntu
    IdentityFile "$LlaveSsh"
    StrictHostKeyChecking accept-new
    ServerAliveInterval 30

Host breb-tunel
    LocalForward 8080 localhost:8080
    LocalForward 8085 localhost:8085
    LocalForward 8090 localhost:8090
    LocalForward 3000 localhost:3000
    ExitOnForwardFailure yes
"@
[System.IO.File]::WriteAllText($Conf, $config, [System.Text.UTF8Encoding]::new($false))

Write-Host "Conectando con $IP ..."
& ssh -F $Conf -o ConnectTimeout=15 breb "echo conectado a la EC2"
if ($LASTEXITCODE -ne 0) {
    Write-Host "No hubo conexión. Revisen que la instancia esté Running con 2/2 checks y que la regla SSH del security group tenga su IP actual (Edit inbound rules -> My IP)." -ForegroundColor Red
    exit 1
}

# 2. Scripts de la demo en la EC2 (~/demo). Se reescriben cada vez: correr esto dos veces no daña nada.
$instalar = @'
mkdir -p ~/demo

cat > ~/demo/comun.sh <<'EOF'
# Lo cargan los demas scripts de la demo.
# El modo AWS de docker compose: la linea COMPOSE_FILE que dejo infra/aws/preparar_ec2.sh en ~/.bashrc.
# Un ssh con comando no lee ~/.bashrc completo, por eso se toma de ahi.
eval "$(grep '^export COMPOSE_FILE=' ~/.bashrc)"
cd ~/breb-fraude || exit 1
# paso: muestra el comando en cian, como si se hubiera escrito, y lo corre
paso() { printf '\n\033[1;36m$ %s\033[0m\n' "$*"; eval "$*"; }
# esperar_sano: espera hasta 10 minutos a que cada servicio quede healthy
esperar_sano() {
  local s id estado i
  for s in "$@"; do
    printf 'esperando a que %s quede healthy ' "$s"
    for i in $(seq 120); do
      id=$(docker compose --profile '*' ps -q "$s" 2>/dev/null)
      estado=$([ -n "$id" ] && docker inspect -f '{{if .State.Health}}{{.State.Health.Status}}{{end}}' "$id" 2>/dev/null)
      [ "$estado" = healthy ] && break
      printf '.'; sleep 5
    done
    echo " ${estado:-sin estado}"
  done
}
EOF

cat > ~/demo/0_prender.sh <<'EOF'
#!/usr/bin/env bash
# Despues de prender la instancia: swap, servicios base, Airflow y consumo (para revisar Metabase antes de grabar)
source ~/demo/comun.sh
swapon --show | grep -q /swapfile || paso 'sudo swapon /swapfile'
paso 'free -h'
paso 'docker compose up -d'
paso 'docker compose --profile orquestacion up -d'
paso 'docker compose --profile consumo up -d'
esperar_sano airflow trino metabase
paso 'docker compose --profile "*" ps --format "table {{.Service}}\t{{.Status}}"'
echo
echo "Listo. Siguiente: revisar Airflow (8085) y Metabase (3000) en el navegador; luego demo/1_streaming.sh"
EOF

cat > ~/demo/1_streaming.sh <<'EOF'
#!/usr/bin/env bash
# Apaga el consumo (para dejarle memoria y CPU al streaming) y prende los cuatro jobs de Spark Structured Streaming
source ~/demo/comun.sh
paso 'docker compose --profile consumo stop trino metabase hive-metastore'
paso 'docker compose --profile streaming up -d'
paso 'docker compose --profile "*" ps --format "table {{.Service}}\t{{.Status}}"'
echo
echo "Los cuatro jobs tardan 1 o 2 minutos en aparecer en Running Applications de http://localhost:8080"
EOF

cat > ~/demo/2_alertas_esperadas.sh <<'EOF'
#!/usr/bin/env bash
# Cuantas alertas va a producir la demo segun cuantas transacciones se publiquen (scores del notebook >= umbral).
# Uso: demo/2_alertas_esperadas.sh [N ...]   (por defecto 1000 1500 2000 3000)
source ~/demo/comun.sh
docker compose run --rm --no-deps -T --entrypoint python productor - "$@" <<'PY'
import json, sys
import pandas as pd
d = "/datos/productor"
t = json.load(open(f"{d}/tramo_demo.json"))
umbral = t["metricas_test"]["umbral"]
try:
    desde = json.load(open(f"{d}/progreso.json"))["ultimo_seq_valido"] + 1
except FileNotFoundError:
    desde = t["tramo"]["desde_seq"]
s = pd.read_parquet(f"{d}/scores_test.parquet", columns=["seq", "score"])
print(f"El productor sigue desde seq {desde} (umbral {umbral:.3f})")
for n in [int(x) for x in sys.argv[1:]] or [1000, 1500, 2000, 3000]:
    v = s[(s.seq >= desde) & (s.seq < desde + n)]
    print(f"  publicar {n:>5}: unas {int((v.score >= umbral).sum())} alertas")
PY
EOF

cat > ~/demo/3_productor.sh <<'EOF'
#!/usr/bin/env bash
# Publica en Kafka las siguientes N transacciones del tramo (por defecto 1500), a 25 por segundo.
# Uso: demo/3_productor.sh [N]
source ~/demo/comun.sh
N=${1:-1500}
SEG=$(( N / 25 + 10 ))
paso "docker compose run --rm productor publicar --continuar --max $N --escalones \"25:$SEG\""
EOF

cat > ~/demo/4_verificar.sh <<'EOF'
#!/usr/bin/env bash
# Espera a que los jobs se pongan al dia y compara Kafka, Bronze, Silver, risk_decisions y MongoDB
source ~/demo/comun.sh
paso 'docker compose run --rm spark-cliente spark-submit /opt/breb/herramientas/streaming/verificar_fase4.py --esperar 900'
EOF

cat > ~/demo/5_gold.sh <<'EOF'
#!/usr/bin/env bash
# Apaga el streaming (con 2 vCPU no caben los dos) y deja que Airflow arme Gold
source ~/demo/comun.sh
paso 'docker compose --profile streaming stop job1-bronze job2-riesgo job2-ventanas job3-silver'
paso 'docker compose exec airflow airflow dags unpause breb_lakehouse_pipeline'
sleep 20
paso 'docker compose exec airflow airflow dags list-runs breb_lakehouse_pipeline'
echo
echo "Sigan la corrida en http://localhost:8085 -> breb_lakehouse_pipeline -> Graph (unos 5 minutos)."
echo "Si no aparece una corrida nueva: docker compose exec airflow airflow dags trigger breb_lakehouse_pipeline"
EOF

cat > ~/demo/6_consumo.sh <<'EOF'
#!/usr/bin/env bash
# Prende Trino, el Hive Metastore y Metabase, despues de que Gold termino
source ~/demo/comun.sh
paso 'docker compose --profile consumo up -d'
esperar_sano trino metabase
echo
echo "Metabase: http://localhost:3000/dashboard/2 (sobre Trino) y http://localhost:3000/dashboard/3 (sobre Athena)"
EOF

cat > ~/demo/7_linaje.sh <<'EOF'
#!/usr/bin/env bash
# Linaje de una alerta como el usuario cumplimiento (por defecto, la de score mas alto que atendio una entidad)
# Uso: demo/7_linaje.sh [transaction_uuid]
source ~/demo/comun.sh
paso "docker compose --profile consumo run --rm spark-cliente python -m breb.consumo linaje $*"
EOF

cat > ~/demo/9_apagar.sh <<'EOF'
#!/usr/bin/env bash
# Antes de detener la instancia: pausa el pipeline y detiene todos los contenedores (asi no arrancan solos al prenderla)
source ~/demo/comun.sh
paso 'docker compose exec airflow airflow dags pause breb_lakehouse_pipeline'
paso 'docker compose --profile "*" stop'
echo
echo "Listo. Ahora: Ctrl+C en E-tunel y, en la consola, EC2 -> Instance state -> Stop (o End Lab)."
EOF

cat > ~/demo/estado.sh <<'EOF'
#!/usr/bin/env bash
# Que contenedores corren en la EC2 (no aparece ningun emulador: S3, DynamoDB, SNS y SQS son los de AWS)
source ~/demo/comun.sh
paso 'docker compose --profile "*" ps --format "table {{.Service}}\t{{.Status}}"'
EOF

cat > ~/demo/credenciales.sh <<'EOF'
#!/usr/bin/env bash
# Usuario y clave de Airflow y Metabase. NO grabar esta pantalla.
source ~/demo/comun.sh
echo "Airflow (http://localhost:8085): usuario admin"
grep -h 'AIRFLOW_ADMIN_CLAVE\|MB_ADMIN_' infra/credenciales/airflow.env infra/credenciales/metabase-admin.env
EOF

cat > ~/demo/b_job2.sh <<'EOF'
#!/usr/bin/env bash
# Pestana B: una linea por micro-batch del Job 2. Espera a que el job este corriendo y se reengancha si se reinicia.
source ~/demo/comun.sh
while true; do
  until [ -n "$(docker compose --profile streaming ps -q --status running job2-riesgo 2>/dev/null)" ]; do
    echo "esperando a que job2-riesgo este corriendo (demo/1_streaming.sh en la pestana A)..."; sleep 10
  done
  printf '\033[1;36m$ docker compose logs -f job2-riesgo | grep "[job2] batch"\033[0m\n'
  docker compose --profile streaming logs -f --since 1m --no-log-prefix job2-riesgo | grep --line-buffered '\[job2\] batch'
done
EOF

cat > ~/demo/c_entidad.sh <<'EOF'
#!/usr/bin/env bash
# Pestana C: el servicio de las entidades que tienen alertas en el tramo (lee sus colas SQS y retiene o bloquea)
source ~/demo/comun.sh
while true; do
  paso 'docker compose run --rm entidad --tramo'
  echo "La entidad se detuvo; arranca de nuevo en 10 s (Ctrl+C para salir)"; sleep 10
done
EOF

cat > ~/demo/d_analistas.sh <<'EOF'
#!/usr/bin/env bash
# Pestana D: la bandeja de los analistas de fraude (cola breb-analistas-fraude, recibe todas las alertas)
source ~/demo/comun.sh
while true; do
  paso 'docker compose run --rm entidad --analistas'
  echo "La bandeja se detuvo; arranca de nuevo en 10 s (Ctrl+C para salir)"; sleep 10
done
EOF

chmod +x ~/demo/*.sh
echo "Scripts de la demo en ~/demo:"
ls ~/demo
'@
$instalar | & ssh -F $Conf breb "tr -d '\r' | bash -s"
if ($LASTEXITCODE -ne 0) { Write-Host "No se pudieron copiar los scripts a la EC2." -ForegroundColor Red; exit 1 }

# 3. Pestañas
Abrir-Pestanas @(
    @{ Titulo = "A-comandos"; Comando = $Ssh + @("-t", "breb") },
    @{ Titulo = "E-tunel"; Comando = $Ssh + @("-N", "breb-tunel") }
)

Write-Host ""
Write-Host "1. En la pestaña A-comandos corran:  demo/0_prender.sh   (de 5 a 10 minutos)" -ForegroundColor Cyan
Read-Host "2. Cuando termine, presionen Enter aquí para abrir B-job2, C-entidad y D-analistas"
Abrir-BCD
Write-Host ""
Write-Host "Por el túnel (pestaña E-tunel abierta):"
Write-Host "  Spark     http://localhost:8080"
Write-Host "  Airflow   http://localhost:8085"
Write-Host "  Trino     http://localhost:8090/ui/"
Write-Host "  Metabase  http://localhost:3000/dashboard/2 (Trino) y /dashboard/3 (Athena)"
