#!/usr/bin/env bash
# Prepara el modo AWS en la EC2 (guía: infra/aws/README.md). Se corre una vez, desde cualquier carpeta:
#   bash infra/aws/preparar_ec2.sh
# 1. Comprueba que corre en una EC2 de us-east-1 con un rol de IAM.
# 2. Escribe infra/aws/aws.env: región y BREB_BUCKET_PREFIJO=breb-<cuenta>- (los buckets de S3 tienen nombres
#    únicos en todo AWS; con el número de la cuenta no chocan con los de nadie).
# 3. Deja COMPOSE_FILE en ~/.bashrc para que docker compose use también docker-compose.aws.yml.
# 4. Comprueba que un contenedor llega al servicio de metadatos (IMDS): sin eso, los SDK no ven el rol.
set -euo pipefail

AQUI="$(cd "$(dirname "$0")" && pwd)"
INFRA="$(cd "$AQUI/.." && pwd)"
IMDS=http://169.254.169.254

TOKEN=$(curl -sf -m 5 -X PUT "$IMDS/latest/api/token" -H "X-aws-ec2-metadata-token-ttl-seconds: 300") || {
  echo "[aws] no responde el servicio de metadatos: este script corre dentro de la EC2" >&2; exit 1; }
md() { curl -sf -m 5 -H "X-aws-ec2-metadata-token: $TOKEN" "$IMDS/latest/$1"; }

ROL=$(md meta-data/iam/security-credentials/) || {
  echo "[aws] la instancia no tiene rol de IAM. Consola de EC2 → la instancia → Actions → Security →" >&2
  echo "      Modify IAM role → breb-ec2 (paso 1 de la guía)" >&2; exit 1; }
DOC=$(md dynamic/instance-identity/document)
CUENTA=$(printf '%s' "$DOC" | python3 -c 'import json, sys; print(json.load(sys.stdin)["accountId"])')
REGION=$(printf '%s' "$DOC" | python3 -c 'import json, sys; print(json.load(sys.stdin)["region"])')
INSTANCIA=$(printf '%s' "$DOC" | python3 -c 'import json, sys; print(json.load(sys.stdin)["instanceId"])')
if [ "$REGION" != "us-east-1" ]; then
  echo "[aws] la instancia está en $REGION; la configuración de infra/aws usa us-east-1 (N. Virginia)" >&2; exit 1
fi
echo "[aws] cuenta $CUENTA, región $REGION, instancia $INSTANCIA, rol $ROL"

cat > "$AQUI/aws.env" <<EOF
# Generado por infra/aws/preparar_ec2.sh. Sin llaves: los SDK toman el rol de la EC2.
AWS_REGION=$REGION
AWS_DEFAULT_REGION=$REGION
BREB_BUCKET_PREFIJO=breb-$CUENTA-
EOF
echo "[aws] $AQUI/aws.env: buckets breb-$CUENTA-lakehouse-{bronze,silver,gold}"

LINEA="export COMPOSE_FILE=$INFRA/docker-compose.yml:$INFRA/docker-compose.aws.yml"
if ! grep -qxF "$LINEA" "$HOME/.bashrc" 2>/dev/null; then
  printf '\n# breb-fraude en modo AWS (infra/aws/preparar_ec2.sh)\n%s\n' "$LINEA" >> "$HOME/.bashrc"
fi
echo "[aws] COMPOSE_FILE quedó en ~/.bashrc; para esta terminal: source ~/.bashrc"

if ! command -v docker >/dev/null; then
  echo "[aws] falta Docker (paso 4 de la guía); después vuelvan a correr este script" >&2; exit 1
fi
# Misma imagen base de infra/spark/Dockerfile: queda en caché para el build.
PRUEBA="import urllib.request as u; u.urlopen(u.Request('$IMDS/latest/api/token', method='PUT', headers={'X-aws-ec2-metadata-token-ttl-seconds': '60'}), timeout=5)"
if docker run --rm python:3.11.18-slim-bookworm python -c "$PRUEBA"; then
  echo "[aws] los contenedores llegan al servicio de metadatos"
else
  echo "[aws] un contenedor no llega al servicio de metadatos: el límite de saltos tiene que ser 2." >&2
  echo "      Consola de EC2 → la instancia → Actions → Instance settings → Modify instance metadata options →" >&2
  echo "      Metadata response hop limit = 2. Después vuelvan a correr este script." >&2
  exit 1
fi
echo "[aws] listo"
