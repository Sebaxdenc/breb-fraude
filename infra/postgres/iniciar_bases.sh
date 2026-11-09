#!/bin/sh
# Crea las bases y los usuarios de los servicios que guardan su estado en PostgreSQL.
# La imagen de postgres corre este script una sola vez, cuando el volumen está vacío.
# Cada servicio es dueño solo de su base. Las claves llegan por variables de entorno y psql las
# interpola como literales (:'variable'), así que no pasan por el shell.
set -eu
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname postgres \
  -v hms_clave="$HMS_DB_CLAVE" -v mb_clave="$MB_DB_PASS" <<'SQL'
CREATE ROLE metastore LOGIN PASSWORD :'hms_clave';
CREATE DATABASE metastore OWNER metastore;
REVOKE ALL ON DATABASE metastore FROM PUBLIC;
CREATE ROLE metabase LOGIN PASSWORD :'mb_clave';
CREATE DATABASE metabase OWNER metabase;
REVOKE ALL ON DATABASE metabase FROM PUBLIC;
SQL
echo "[postgres-init] bases metastore y metabase creadas"
