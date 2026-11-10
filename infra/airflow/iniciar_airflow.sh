#!/bin/sh
# Arranque de Airflow 3 en un solo contenedor (airflow standalone: api-server, scheduler, dag-processor y
# triggerer, con LocalExecutor y el metastore en PostgreSQL). Crea el archivo de claves del SimpleAuthManager
# con la clave de credenciales/airflow.env, para que el usuario admin tenga una clave conocida.
set -eu
umask 077
python - <<'EOF'
import json, os
ruta = os.environ["AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_PASSWORDS_FILE"]
with open(ruta, "w") as fh:
    json.dump({"admin": os.environ["AIRFLOW_ADMIN_CLAVE"]}, fh)
EOF
exec airflow standalone
