#!/bin/sh
# Crea las bases y los usuarios de los servicios que guardan su estado en PostgreSQL: metastore (Hive),
# metabase y airflow. Cada servicio es dueño solo de su base. Es idempotente: crea solo lo que falta.
# - La imagen de postgres lo corre al crear el volumen (docker-entrypoint-initdb.d, por socket local).
# - El servicio postgres-bases lo corre en cada arranque (PGHOST=postgres), así un volumen creado antes de
#   agregar un servicio también recibe su base.
# Las claves llegan por variables de entorno y psql las interpola como literales (:'variable').
set -eu
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname postgres \
  -v hms_clave="$HMS_DB_CLAVE" -v mb_clave="$MB_DB_PASS" -v af_clave="$AIRFLOW_DB_CLAVE" <<'SQL'
SELECT format('CREATE ROLE %I LOGIN PASSWORD %L', r.nombre, r.clave)
FROM (VALUES ('metastore', :'hms_clave'), ('metabase', :'mb_clave'), ('airflow', :'af_clave')) AS r(nombre, clave)
WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = r.nombre) \gexec
SELECT format('CREATE DATABASE %I OWNER %I', b.nombre, b.nombre)
FROM (VALUES ('metastore'), ('metabase'), ('airflow')) AS b(nombre)
WHERE NOT EXISTS (SELECT 1 FROM pg_database WHERE datname = b.nombre) \gexec
SELECT format('REVOKE ALL ON DATABASE %I FROM PUBLIC', b.nombre)
FROM (VALUES ('metastore'), ('metabase'), ('airflow')) AS b(nombre) \gexec
SQL
echo "[postgres-init] bases metastore, metabase y airflow listas"
