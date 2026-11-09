# Hive Metastore 4.2.1 con el driver JDBC de PostgreSQL y acceso a S3 (SeaweedFS).
# Contexto de build: la raíz del repositorio (ver infra/docker-compose.yml).

ARG HIVE_IMAGE=apache/hive:4.2.1
ARG JDK_IMAGE=eclipse-temurin:17.0.21_9-jdk-jammy

# Misma etapa que en infra/spark/Dockerfile: Docker reutiliza la capa de curl y patch.
FROM ${JDK_IMAGE} AS jars
RUN apt-get update && apt-get install -y --no-install-recommends curl patch && rm -rf /var/lib/apt/lists/*
WORKDIR /build
COPY infra/hms/jars.txt infra/spark/descargar_jars.sh ./
RUN sed -i 's/\r$//' jars.txt descargar_jars.sh && sh descargar_jars.sh jars.txt runtime compilacion

FROM ${HIVE_IMAGE}
COPY --from=jars /build/runtime/ /opt/hive/lib/
# core-site.xml: acceso a SeaweedFS por S3A. HADOOP_OPTIONAL_TOOLS agrega hadoop-aws y el SDK de AWS,
# que ya vienen en la imagen (Hadoop 3.4.1), al classpath.
COPY infra/hms/conf/core-site.xml /opt/hive/conf/core-site.xml
COPY infra/hms/iniciar_hms.sh /opt/breb-hms/iniciar_hms.sh
# Quita CRLF por si el repositorio se clonó en Windows sin .gitattributes.
USER root
RUN sed -i 's/\r$//' /opt/breb-hms/iniciar_hms.sh /opt/hive/conf/core-site.xml
USER hive
ENV HADOOP_OPTIONAL_TOOLS=hadoop-aws
ENTRYPOINT ["sh", "/opt/breb-hms/iniciar_hms.sh"]
