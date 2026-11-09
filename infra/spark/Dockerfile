# Imagen única de Spark para el master, los workers, los jobs y las herramientas.
# Sin línea "# syntax=": esa directiva descarga el frontend de BuildKit de Docker Hub en cada build.
# El Dockerfile no usa funciones que no traiga el frontend incluido en Docker.
# Python 3.11 + numpy 1.26 (PySpark 3.5 no admite numpy 2), Java 17, Delta 3.3.3 y conectores S3A y Kafka.
# Contexto de build: la raíz del repositorio (ver infra/docker-compose.yml).

ARG PYTHON_IMAGE=python:3.11.18-slim-bookworm
ARG JDK_IMAGE=eclipse-temurin:17.0.21_9-jdk-jammy
ARG JRE_IMAGE=eclipse-temurin:17.0.21_9-jre-jammy

FROM ${JRE_IMAGE} AS jre

# JARs de Maven Central verificados por SHA-1 y S3DynamoDBLogStore con la opción ddb.endpoint.
FROM ${JDK_IMAGE} AS jars
RUN apt-get update && apt-get install -y --no-install-recommends curl patch && rm -rf /var/lib/apt/lists/*
WORKDIR /build
COPY infra/spark/jars.txt infra/spark/descargar_jars.sh ./
COPY infra/spark/logstore/ logstore/
# Quita CRLF por si el repositorio se clonó en Windows sin .gitattributes.
RUN sed -i 's/\r$//' jars.txt descargar_jars.sh logstore/construir_logstore.sh logstore/ddb-endpoint.patch \
 && sh descargar_jars.sh jars.txt runtime compilacion && sh logstore/construir_logstore.sh runtime compilacion

FROM ${PYTHON_IMAGE}
COPY --from=jre /opt/java/openjdk /opt/java/openjdk
ENV JAVA_HOME=/opt/java/openjdk \
    PATH=/opt/java/openjdk/bin:$PATH \
    SPARK_HOME=/usr/local/lib/python3.11/site-packages/pyspark \
    SPARK_CONF_DIR=/opt/spark/conf \
    PYSPARK_PYTHON=python3 \
    PYTHONUNBUFFERED=1
RUN apt-get update && apt-get install -y --no-install-recommends procps tini && rm -rf /var/lib/apt/lists/*

COPY infra/spark/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt
COPY --from=jars /build/runtime/ ${SPARK_HOME}/jars/
COPY infra/spark/conf/ /opt/spark/conf/

# Paquete breb y artefactos del modelo dentro de la imagen: la demo no depende de internet.
COPY pyproject.toml /opt/breb/
COPY src/ /opt/breb/src/
RUN pip install --no-cache-dir --no-deps /opt/breb && rm -rf /opt/breb/build /opt/breb/src/*.egg-info
COPY modelo/aml_xgb/ /opt/breb/modelo/aml_xgb/
COPY infra/init/ /opt/breb/infra/init/
COPY herramientas/infra/ /opt/breb/herramientas/infra/

RUN useradd --uid 1000 --create-home spark && mkdir -p /opt/spark/work && chown -R spark /opt/spark
USER spark
WORKDIR /opt/breb
ENTRYPOINT ["/usr/bin/tini", "--"]
CMD ["bash"]
