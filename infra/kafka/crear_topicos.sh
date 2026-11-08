#!/bin/sh
# Crea los tópicos del stack y comprueba su configuración. Idempotente.
# Variables: KAFKA_BOOTSTRAP (por defecto kafka-1:9092), KAFKA_RF y KAFKA_MIN_ISR (dev 1/1, completo 3/2).
set -eu
K="${KAFKA_HOME:-/opt/kafka}/bin"
B="${KAFKA_BOOTSTRAP:-kafka-1:9092}"

crear() {  # tópico particiones retención_ms
  i=0
  until "$K/kafka-topics.sh" --bootstrap-server "$B" --create --if-not-exists --topic "$1" \
      --partitions "$2" --replication-factor "$KAFKA_RF" \
      --config min.insync.replicas="$KAFKA_MIN_ISR" \
      --config message.timestamp.type=LogAppendTime \
      --config retention.ms="$3"; do
    i=$((i + 1))
    if [ "$i" -ge 40 ]; then echo "[kafka-init] no pude crear $1" >&2; exit 1; fi
    echo "[kafka-init] reintento $i para $1 (puede que falten brókers)"
    sleep 3
  done
  desc=$("$K/kafka-topics.sh" --bootstrap-server "$B" --describe --topic "$1")
  echo "$desc" | head -1
  echo "$desc" | grep -q "PartitionCount: $2" || { echo "[kafka-init] $1 no tiene $2 particiones" >&2; exit 1; }
  echo "$desc" | grep -q "ReplicationFactor: $KAFKA_RF" || {
    echo "[kafka-init] $1 no tiene replication.factor=$KAFKA_RF; al cambiar de perfil: docker compose down -v" >&2; exit 1; }
}

# Eventos de transferencias: el orden y la latencia se miden con LogAppendTime (RNF-02); 7 días de retención.
crear breb.transacciones.raw 6 604800000
# Tópico de humo para herramientas/infra/verificar_stack.py (1 h de retención).
crear breb.humo 1 3600000
echo "[kafka-init] tópicos listos (replication.factor=$KAFKA_RF, min.insync.replicas=$KAFKA_MIN_ISR)"
