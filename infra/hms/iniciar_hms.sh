#!/bin/sh
# Arranque del Hive Metastore: crea o actualiza el esquema en PostgreSQL y levanta el servicio Thrift.
# Reemplaza el /entrypoint.sh de la imagen por dos motivos:
# - schematool de Hive 4.2.1 no aplicó las claves JDO de hive-site.xml (siguió usando Derby); con
#   propiedades -D sí las toma, que es como lo documenta la imagen oficial.
# - el entrypoint original corre con "set -x" e imprimiría la clave de la base en el log.
# Variables: HMS_DB_URL, HMS_DB_USUARIO, HMS_DB_CLAVE y HMS_HEAP (por defecto 384m).
set -eu

export HIVE_CONF_DIR="$HIVE_HOME/conf"
export HADOOP_CONF_DIR="$HIVE_CONF_DIR"
export HADOOP_CLIENT_OPTS="-Xmx${HMS_HEAP:-384m} \
 -Djavax.jdo.option.ConnectionDriverName=org.postgresql.Driver \
 -Djavax.jdo.option.ConnectionURL=$HMS_DB_URL \
 -Djavax.jdo.option.ConnectionUserName=$HMS_DB_USUARIO \
 -Djavax.jdo.option.ConnectionPassword=$HMS_DB_CLAVE"

i=0
until "$HIVE_HOME/bin/schematool" -dbType postgres -initOrUpgradeSchema; do
  i=$((i + 1))
  if [ "$i" -ge 10 ]; then echo "[hms] no pude crear el esquema del metastore" >&2; exit 1; fi
  echo "[hms] reintento $i de schematool (¿PostgreSQL todavía arrancando?)"
  sleep 3
done
echo "[hms] esquema listo; arrancando el metastore en el puerto ${METASTORE_PORT:-9083}"
export METASTORE_PORT="${METASTORE_PORT:-9083}"
exec "$HIVE_HOME/bin/hive" --skiphadoopversion --skiphbasecp --service metastore
